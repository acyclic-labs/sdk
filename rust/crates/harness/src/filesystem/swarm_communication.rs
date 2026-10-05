//! Communication projects the swarm's authoritative lifecycle, never a second
//! workflow. Payload copies and inbox records retain separate stable identities.

use super::*;
use crate::{
    Outcome,
    communication::{message_endpoint_operation, publish_control_record},
    durable_mail::MailboxStore,
    runtime::{DurableTaskHost, TaskCommunicationScope},
    scheduler::InboxItem,
    swarm_budget::SwarmReservationState,
};

pub(super) struct SwarmCommunicationHost {
    swarm: OnceLock<Weak<PersistentLocalSwarm>>,
    stream: StreamClient<LocalStream>,
}

fn budget_allows_new_mutations(
    reservation_state: Option<SwarmReservationState>,
    phase: &LocalSessionPhase,
) -> bool {
    match reservation_state {
        None | Some(SwarmReservationState::Active) => true,
        Some(SwarmReservationState::Reserved | SwarmReservationState::Cancelled) => false,
        Some(SwarmReservationState::Completed) => matches!(phase, LocalSessionPhase::Completed),
    }
}

impl SwarmCommunicationHost {
    pub(super) fn new(stream: StreamClient<LocalStream>) -> Self {
        Self {
            swarm: OnceLock::new(),
            stream,
        }
    }

    pub(super) fn bind(&self, swarm: Weak<PersistentLocalSwarm>) -> Result<()> {
        self.swarm
            .set(swarm)
            .map_err(|_| Error::Conflict("swarm communication host is already bound".into()))
    }

    fn swarm(&self) -> Result<Arc<PersistentLocalSwarm>> {
        self.swarm
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| Error::Storage("swarm communication owner is unavailable".into()))
    }
}

impl DurableTaskHost for SwarmCommunicationHost {
    fn supports_admitted_message_recovery(&self) -> bool {
        true
    }

    fn supports_admitted_timer_recovery(&self) -> bool {
        true
    }

    fn observe_admission<'a>(
        &'a self,
        task: TaskId,
    ) -> BoxFuture<'a, Result<crate::runtime::TaskAdmissionRecord>> {
        Box::pin(async move { self.swarm()?.authenticated_admission(task).await })
    }

    fn communication_scope<'a>(
        &'a self,
        task: TaskId,
    ) -> BoxFuture<'a, Result<TaskCommunicationScope>> {
        Box::pin(async move {
            let swarm = self.swarm()?;
            // Communication authority comes from the immutable TaskAdmitted
            // record. Do not rebuild grants from the mutable harness bundle
            // or current swarm configuration; those values are only useful
            // after the admission has been authenticated.
            let admission = swarm.authenticated_admission(task).await?;
            let session = swarm.session(task).await?;
            // A completed task may be explicitly resumed for a new user
            // turn. Cancellation and failure are owner fences: they retain
            // reads and committed replay, but cannot admit fresh mutations.
            if session.parent != admission.parent {
                return Err(Error::Conflict(
                    "task session parent differs from its authenticated admission".into(),
                ));
            }
            // Child communication is subordinate to the single durable swarm
            // budget.  Admission alone is insufficient: the child reservation
            // must be present, bound to the exact canonical admission bytes,
            // and past the publication gate before a model or mailbox effect
            // can mutate state.  Refresh the journal here so a second owner
            // cannot leave this handle using a stale in-memory projection.
            let reservation_state = if admission.parent.is_some() {
                let parent_task = admission.parent.ok_or_else(|| {
                    Error::Conflict("child admission is missing its parent identity".into())
                })?;
                let parent_operation = swarm
                    .authenticated_admission(parent_task)
                    .await?
                    .operation_id;
                let admission_digest = crate::contract::canonical_json_digest(
                    &admission.canonical_value(),
                )?;
                let mut budget = swarm.budget_journal.lock().await;
                budget.refresh().await?;
                let reservation = budget
                    .reservation(admission.operation_id)?
                    .ok_or_else(|| {
                        Error::Unauthorized(
                            "child communication requires a canonical swarm budget reservation"
                        .into(),
                    )
                })?;
                if reservation.admission_digest != Some(admission_digest)
                    || reservation.parent_operation_id != Some(parent_operation)
                    || reservation.depth != u32::try_from(session.depth).map_err(|_| {
                        Error::Conflict("task session depth exceeds the budget contract".into())
                    })?
                {
                    return Err(Error::Conflict(
                        "child communication admission is not bound to its canonical budget ancestry"
                            .into(),
                    ));
                }
                Some(reservation.state)
            } else {
                None
            };
            let lifecycle_allows = matches!(
                &session.phase,
                LocalSessionPhase::Ready
                    | LocalSessionPhase::Activating
                    | LocalSessionPhase::Completed
            );
            let budget_allows = budget_allows_new_mutations(reservation_state, &session.phase);
            Ok(TaskCommunicationScope {
                parent: admission.parent,
                grants: admission.grants,
                limits: admission.limits,
                run_limits: admission.run_limits,
                accepts_new_mutations: lifecycle_allows && budget_allows,
            })
        })
    }

    fn outcome<'a>(&'a self, task: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async move {
            let swarm = self.swarm()?;
            match swarm.session(task).await?.phase {
                LocalSessionPhase::Ready | LocalSessionPhase::Activating => Ok(None),
                LocalSessionPhase::Cancelled => Ok(Some(Outcome::Cancelled)),
                LocalSessionPhase::Failed(message) => Ok(Some(Outcome::Failed { message })),
                LocalSessionPhase::Completed => {
                    let output = swarm.outcome(task).await?;
                    Ok(Some(Outcome::Succeeded(
                        serde_json::to_value(output)
                            .map_err(|error| Error::Storage(error.to_string()))?,
                    )))
                }
            }
        })
    }

    fn cancel<'a>(&'a self, task: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let swarm = self.swarm()?;
            if swarm.session(task).await?.phase != LocalSessionPhase::Cancelled {
                swarm.cancel(task).await?;
            }
            Ok(())
        })
    }

    fn replay_message<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            let swarm = self.swarm()?;
            let recipient_harness = swarm.open_session(recipient).await?;
            let mailbox = MailboxStore::new(
                self.stream.clone(),
                recipient_harness.storage().content_verifier(),
            );
            let Some(existing) = mailbox
                .find_message(self, sender, recipient, message)
                .await?
            else {
                return Ok(false);
            };
            if existing != payload {
                return Err(Error::Conflict(
                    "message identity was reused with another payload".into(),
                ));
            }
            Ok(true)
        })
    }

    fn replay_message_body<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        body: &'a [u8],
    ) -> BoxFuture<'a, Result<Option<FileRef>>> {
        Box::pin(async move {
            let swarm = self.swarm()?;
            let recipient_harness = swarm.open_session(recipient).await?;
            let storage = recipient_harness.storage();
            let mailbox = MailboxStore::new(self.stream.clone(), storage.content_verifier());
            let Some(existing) = mailbox
                .find_message(self, sender, recipient, message)
                .await?
            else {
                return Ok(None);
            };
            if storage.read(&existing).await? != body {
                return Err(Error::Conflict(
                    "message identity was reused with another payload".into(),
                ));
            }
            Ok(Some(existing))
        })
    }

    fn send<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if message.into_bytes() == [0; 16] {
                return Err(Error::Invalid("swarm message identity is nil".into()));
            }
            let swarm = self.swarm()?;
            let sender_scope = self.communication_scope(sender).await?;
            let recipient_scope = self.communication_scope(recipient).await?;
            if sender_scope.parent != Some(recipient) && recipient_scope.parent != Some(sender) {
                return Err(Error::Unauthorized(
                    "message endpoints are not direct parent and child".into(),
                ));
            }
            recipient_scope.limits.validate_file(&payload)?;
            // A lifecycle-fenced endpoint may still recover the exact
            // committed delivery. Probe before admitting any new mutation;
            // the normal publication path remains the sole ledger for active
            // sends.
            if (!sender_scope.accepts_new_mutations || !recipient_scope.accepts_new_mutations)
                && self
                    .replay_message(sender, recipient, message, payload.clone())
                    .await?
            {
                return Ok(());
            }
            let recipient_harness = swarm.open_session(recipient).await?;
            let storage = recipient_harness.storage();
            let sender_harness = swarm.open_session(sender).await?;
            // Validate sender read authority and content residency before the
            // lifecycle CAS. An admission must never survive a malformed or
            // inaccessible source payload.
            let bytes = sender_harness.storage().read(&payload).await?;
            swarm
                .admit_message(sender, recipient, message, payload.clone())
                .await?;
            // The endpoint operation remains the stable identity for the
            // recipient-owned staged bytes. It is deliberately not a second
            // journal: the mailbox record below is the sole durable
            // publication effect, while staged content is unpublished until
            // that record commits.
            let transfer = message_endpoint_operation(sender, recipient, message);
            let delivered = if payload.volume() == storage.volume() {
                // A recipient-owned ref still requires explicit sender read
                // authority, checked above. Identity knowledge is not a grant.
                payload
            } else {
                // Model tools can supply only their explicitly readable refs.
                storage
                    .stage(
                        transfer,
                        &format!("system/swarm/messages/{transfer}.txt"),
                        &bytes,
                        payload.descriptor().media_type(),
                        "message.txt",
                    )
                    .await?
            };
            MailboxStore::new(self.stream.clone(), storage.content_verifier())
                .send_admitted(self, sender, recipient, message, delivered)
                .await
        })
    }

    fn inbox<'a>(
        &'a self,
        task: TaskId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(async move {
            let swarm = self.swarm()?;
            let harness = swarm.open_session(task).await?;
            MailboxStore::new(self.stream.clone(), harness.storage().content_verifier())
                .inbox(self, task, after, limit)
                .await
        })
    }

    fn wait_until<'a>(
        &'a self,
        task: TaskId,
        operation: OperationId,
        deadline: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let swarm = self.swarm()?;
            swarm.admit_timer(task, operation, deadline).await?;
            let timer = self
                .stream
                .stream(format!("harness/v2/swarm-timers/{task}"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let bytes = crate::contract::canonical_json_bytes(&json!({
                "schema_version": 1, "task": task, "operation": operation, "deadline": deadline,
            }))?;
            publish_control_record(&self.stream, &timer, "swarm-timer", task, operation, &bytes)
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(
                deadline.saturating_sub(self.now_unix_millis()),
            ))
            .await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_and_cancelled_budget_states_fence_new_mutations() {
        assert!(!budget_allows_new_mutations(
            Some(SwarmReservationState::Reserved),
            &LocalSessionPhase::Activating,
        ));
        assert!(!budget_allows_new_mutations(
            Some(SwarmReservationState::Cancelled),
            &LocalSessionPhase::Ready,
        ));
    }

    #[test]
    fn active_or_root_budget_states_allow_new_mutations() {
        assert!(budget_allows_new_mutations(
            Some(SwarmReservationState::Active),
            &LocalSessionPhase::Activating,
        ));
        assert!(budget_allows_new_mutations(None, &LocalSessionPhase::Ready));
    }

    #[test]
    fn completed_budget_state_only_allows_an_explicit_completed_turn() {
        assert!(budget_allows_new_mutations(
            Some(SwarmReservationState::Completed),
            &LocalSessionPhase::Completed,
        ));
        assert!(!budget_allows_new_mutations(
            Some(SwarmReservationState::Completed),
            &LocalSessionPhase::Ready,
        ));
        assert!(!budget_allows_new_mutations(
            Some(SwarmReservationState::Completed),
            &LocalSessionPhase::Activating,
        ));
    }
}
