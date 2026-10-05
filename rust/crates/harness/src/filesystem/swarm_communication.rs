//! Communication projects the swarm's authoritative lifecycle, never a second
//! workflow. Payload copies and inbox records retain separate stable identities.

use super::*;
use crate::{
    Outcome,
    communication::{message_endpoint_operation, publish_control_record},
    durable_mail::MailboxStore,
    runtime::{DurableTaskHost, TaskCommunicationScope},
    scheduler::InboxItem,
};

pub(super) struct SwarmCommunicationHost {
    swarm: OnceLock<Weak<PersistentLocalSwarm>>,
    stream: StreamClient<LocalStream>,
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
            let accepts_new_mutations = matches!(
                &session.phase,
                LocalSessionPhase::Ready
                    | LocalSessionPhase::Activating
                    | LocalSessionPhase::Completed
            );
            Ok(TaskCommunicationScope {
                parent: admission.parent,
                grants: admission.grants,
                limits: admission.limits,
                run_limits: admission.run_limits,
                accepts_new_mutations,
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
            sender_scope.require_new_mutation()?;
            recipient_scope.require_new_mutation()?;
            if sender_scope.parent != Some(recipient) && recipient_scope.parent != Some(sender) {
                return Err(Error::Unauthorized(
                    "message endpoints are not direct parent and child".into(),
                ));
            }
            recipient_scope.limits.validate_file(&payload)?;
            let recipient_harness = swarm.open_session(recipient).await?;
            let storage = recipient_harness.storage();
            let sender_harness = swarm.open_session(sender).await?;
            let bytes = sender_harness.storage().read(&payload).await?;
            let transfer = message_endpoint_operation(sender, recipient, message);
            let transfers = self
                .stream
                .stream(format!("harness/v2/mail-transfers/{recipient}"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let admitted = crate::contract::canonical_json_bytes(&json!({
                "schema_version": 1, "sender": sender, "recipient": recipient,
                "message": message, "payload": payload,
            }))?;
            publish_control_record(
                &self.stream,
                &transfers,
                "mail-transfer",
                recipient,
                transfer,
                &admitted,
            )
            .await?;
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
                .send(self, sender, recipient, message, delivered)
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
            self.communication_scope(task)
                .await?
                .require_new_mutation()?;
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
