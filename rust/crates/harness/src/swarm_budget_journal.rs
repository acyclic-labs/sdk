//! Stream backed durability for [`crate::swarm_budget::SwarmBudget`].
//!
//! The Stream tail is the authority for admission ordering.  The in-memory
//! projection is rebuilt from that tail and is only used to validate the next
//! transition.  This keeps a process mutex from becoming a second admission
//! ledger when two local runtimes race to reserve a child.

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    contract::canonical_json_bytes,
    runtime::TaskAdmissionRecord,
    swarm_budget::{
        ForkPublication, SwarmAdmissionReceipt, SwarmBudget, SwarmBudgetEvent, SwarmBudgetLimits,
        SwarmBudgetUsage, SwarmDispatchToken, SwarmForkRequest, SwarmForkReservation,
        SwarmOwnerFence, SwarmResourceRequest, SwarmUsage, VerifiedForkPublication,
        VerifiedSwarmUsageReceipt,
    },
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, Stream, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::future::Future;

const STREAM_PREFIX: &str = "harness/v2/swarm-budget";
const BUDGET_EVENT_VERSION: u16 = 1;
const MAX_RECORDS: u64 = 1_000_000;
const MAX_ADMISSION_RETRIES: u8 = 32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetRecord {
    /// Explicitly fences records written before receipt-backed usage events.
    version: u16,
    revision: u64,
    event_digest: [u8; 32],
    event: SwarmBudgetEvent,
}

/// A durable session budget whose Stream tail is the authoritative CAS ledger.
pub struct SwarmBudgetJournal<P> {
    stream: Stream<P>,
    session_id: OperationId,
    revision: u64,
    events: Vec<SwarmBudgetEvent>,
    budget: SwarmBudget,
}

impl<P: StreamProvider> SwarmBudgetJournal<P> {
    /// Opens and replays an existing session budget.
    pub async fn open(client: &StreamClient<P>, session_id: OperationId) -> Result<Self> {
        let stream = stream_for(client, session_id)?;
        let events = read_events(&stream).await?;
        let budget = SwarmBudget::replay(events.clone())?;
        let (stored_session, _, _) = budget.descriptor()?;
        if stored_session != session_id {
            return Err(Error::Conflict(
                "swarm budget session identity differs".into(),
            ));
        }
        Ok(Self {
            stream,
            session_id,
            revision: event_count(events.len())?,
            events,
            budget,
        })
    }

    /// Creates a session with a durable root admission, or reopens an exact
    /// existing session after a restart.
    pub async fn start(
        client: &StreamClient<P>,
        session_id: OperationId,
        owner: SwarmOwnerFence,
        limits: SwarmBudgetLimits,
    ) -> Result<Self> {
        // Validate the descriptor before creating the durable stream. A
        // malformed start must never leave an unreplayable root record.
        SwarmBudget::new(session_id, owner.clone(), limits)?;
        let stream = stream_for(client, session_id)?;
        let tail = match stream.tail().await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if tail != 0 {
            let reopened = Self::open(client, session_id).await?;
            let (_, observed_owner, observed_limits) = reopened.descriptor()?;
            if observed_owner == owner && observed_limits == limits {
                return Ok(reopened);
            }
            return Err(Error::Conflict("swarm session descriptor differs".into()));
        }
        let event = SwarmBudgetEvent::Started {
            session_id,
            owner,
            limits,
        };
        match append_record(&stream, 0, &event, session_id).await {
            Ok(()) => Self::open(client, session_id).await,
            Err(Error::Conflict(_)) => {
                let reopened = Self::open(client, session_id).await?;
                let (_, observed_owner, observed_limits) = reopened.descriptor()?;
                if observed_owner == event_owner(&event)? && observed_limits == limits {
                    Ok(reopened)
                } else {
                    Err(Error::Conflict("swarm session descriptor differs".into()))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Returns the current immutable session descriptor.
    pub fn descriptor(&self) -> Result<(OperationId, SwarmOwnerFence, SwarmBudgetLimits)> {
        self.budget.descriptor()
    }

    /// Returns the current usage projection.
    pub fn usage(&self) -> Result<SwarmBudgetUsage> {
        self.budget.usage()
    }

    /// Returns a reservation snapshot for recovery and dispatch reconciliation.
    pub fn reservation(&self, operation_id: OperationId) -> Result<Option<SwarmForkReservation>> {
        self.budget.reservation(operation_id)
    }

    /// Reloads all committed records from the provider's current tail.
    pub async fn refresh(&mut self) -> Result<()> {
        let events = read_events(&self.stream).await?;
        let budget = SwarmBudget::replay(events.clone())?;
        let (stored_session, _, _) = budget.descriptor()?;
        if stored_session != self.session_id {
            return Err(Error::Conflict(
                "swarm budget session identity differs".into(),
            ));
        }
        self.revision = event_count(events.len())?;
        self.events = events;
        self.budget = budget;
        Ok(())
    }

    /// Persists a child reservation before any fork or model dispatch.
    pub async fn reserve_child(
        &mut self,
        request: SwarmForkRequest,
    ) -> Result<SwarmAdmissionReceipt> {
        let mut retries = 0;
        loop {
            let projected = SwarmBudget::replay(self.events.clone())?;
            let receipt = projected.reserve_child(request.clone())?;
            if receipt.replayed {
                return Ok(receipt);
            }
            match self
                .commit(receipt.durable_event(), receipt.reservation.operation_id)
                .await
            {
                Ok(applied) => {
                    return self
                        .budget
                        .reservation(receipt.reservation.operation_id)?
                        .map(|reservation| SwarmAdmissionReceipt {
                            reservation,
                            replayed: !applied,
                        })
                        .ok_or_else(|| {
                            Error::Storage("committed swarm reservation is missing".into())
                        });
                }
                Err(Error::Conflict(message))
                    if message == "swarm budget tail changed"
                        && retries < MAX_ADMISSION_RETRIES =>
                {
                    retries += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Admits a child from the canonical durable task admission envelope.
    ///
    /// The task operation identity and prerequisite dependencies therefore
    /// remain owned by `TaskAdmissionRecord`; this layer adds only the swarm
    /// parent/depth and session resource reservation.
    pub async fn reserve_after_admission(
        &mut self,
        admission: &TaskAdmissionRecord,
        idempotency_key: IdempotencyKey,
        parent_operation_id: Option<OperationId>,
        depth: u32,
        resources: SwarmResourceRequest,
    ) -> Result<SwarmAdmissionReceipt> {
        admission.validate()?;
        let request = SwarmForkRequest::from_task_admission(
            admission,
            idempotency_key,
            parent_operation_id,
            depth,
            resources,
        )?;
        self.reserve_child(request).await
    }

    async fn activate_raw(
        &mut self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        publication: ForkPublication,
    ) -> Result<SwarmDispatchToken> {
        let projected = SwarmBudget::replay(self.events.clone())?;
        let token = projected.activate(operation_id, owner, publication)?;
        self.commit(
            SwarmBudgetEvent::ChildActivated {
                operation_id,
                owner: token.owner().clone(),
                publication: token.publication(),
            },
            operation_id,
        )
        .await?;
        Ok(token)
    }

    /// Activates only publication evidence produced by the authoritative fork
    /// helper. Raw digest fields cannot be supplied by an external caller.
    pub async fn activate_verified(
        &mut self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        publication: VerifiedForkPublication,
    ) -> Result<SwarmDispatchToken> {
        self.activate_raw(operation_id, owner, publication.into_publication())
            .await
    }

    /// Activates verified publication evidence and invokes the production dispatcher.
    ///
    /// Activation is durably committed before `dispatch` is called. If the
    /// dispatcher rejects the token, the active reservation is cancelled and
    /// only its unconsumed allocation is released. A failed cancellation is
    /// reported as indeterminate so recovery can reconcile the durable tail.
    pub async fn dispatch_after_publication<F, Fut, T>(
        &mut self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        publication: VerifiedForkPublication,
        dispatch: F,
    ) -> Result<T>
    where
        F: FnOnce(SwarmDispatchToken) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let token = self
            .activate_verified(operation_id, owner.clone(), publication)
            .await?;
        match dispatch(token).await {
            Ok(value) => Ok(value),
            // A storage failure after activation may have happened after the
            // provider accepted the token. Keep the active reservation until
            // recovery can reconcile the durable tail instead of releasing
            // capacity based on an unproven dispatch failure.
            Err(error @ Error::Indeterminate(_)) | Err(error @ Error::Storage(_)) => Err(error),
            Err(error) => match self.cancel(operation_id, &owner).await {
                Ok(_) => Err(error),
                Err(_) => Err(Error::Indeterminate(operation_id)),
            },
        }
    }

    /// Persists cumulative usage without refunding consumed resources.
    pub async fn report_usage(
        &mut self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
    ) -> Result<SwarmForkReservation> {
        let _ = (operation_id, owner, usage);
        Err(Error::Unauthorized(
            "provider usage receipt required for durable swarm usage".into(),
        ))
    }

    /// Persists provider-measured cumulative usage with a verified receipt.
    pub async fn report_usage_with_receipt(
        &mut self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        receipt: VerifiedSwarmUsageReceipt,
    ) -> Result<SwarmForkReservation> {
        let receipt = receipt.into_receipt();
        let projected = SwarmBudget::replay(self.events.clone())?;
        if self.receipt_replayed(operation_id, owner, &receipt, false) {
            return projected
                .reservation(operation_id)?
                .ok_or_else(|| Error::Storage("replayed swarm reservation is missing".into()));
        }
        self.validate_receipt(operation_id, receipt.usage, &receipt)?;
        let reservation = projected.report_usage(operation_id, owner, receipt.usage)?;
        self.commit(
            SwarmBudgetEvent::UsageReported {
                operation_id,
                owner: owner.clone(),
                usage: receipt.usage,
                receipt: receipt.clone(),
            },
            operation_id,
        )
        .await?;
        Ok(reservation)
    }

    /// Persists cumulative root usage before admitting further descendants.
    pub async fn report_root_usage(
        &mut self,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
    ) -> Result<SwarmUsage> {
        let _ = (owner, usage);
        Err(Error::Unauthorized(
            "provider usage receipt required for durable swarm root usage".into(),
        ))
    }

    /// Persists provider-measured cumulative root usage with a verified receipt.
    pub async fn report_root_usage_with_receipt(
        &mut self,
        owner: &SwarmOwnerFence,
        receipt: VerifiedSwarmUsageReceipt,
    ) -> Result<SwarmUsage> {
        let receipt = receipt.into_receipt();
        let projected = SwarmBudget::replay(self.events.clone())?;
        if self.root_receipt_replayed(owner, &receipt) {
            return projected.report_root_usage(owner, receipt.usage);
        }
        self.validate_receipt(self.session_id, receipt.usage, &receipt)?;
        let reported = projected.report_root_usage(owner, receipt.usage)?;
        self.commit(
            SwarmBudgetEvent::RootUsageReported {
                owner: owner.clone(),
                usage: receipt.usage,
                receipt: receipt.clone(),
            },
            self.session_id,
        )
        .await?;
        Ok(reported)
    }

    /// Completes a child and releases only the unconsumed reservation.
    pub async fn complete(
        &mut self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        usage: SwarmUsage,
    ) -> Result<SwarmForkReservation> {
        let _ = (operation_id, owner, usage);
        Err(Error::Unauthorized(
            "provider usage receipt required for durable swarm completion".into(),
        ))
    }

    /// Completes a child with provider-measured cumulative usage evidence.
    pub async fn complete_with_receipt(
        &mut self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        receipt: VerifiedSwarmUsageReceipt,
    ) -> Result<SwarmForkReservation> {
        let receipt = receipt.into_receipt();
        let projected = SwarmBudget::replay(self.events.clone())?;
        if self.receipt_replayed(operation_id, owner, &receipt, true) {
            return projected
                .reservation(operation_id)?
                .ok_or_else(|| Error::Storage("replayed swarm reservation is missing".into()));
        }
        self.validate_receipt(operation_id, receipt.usage, &receipt)?;
        let reservation = projected.complete(operation_id, owner, receipt.usage)?;
        self.commit(
            SwarmBudgetEvent::ChildCompleted {
                operation_id,
                owner: owner.clone(),
                usage: receipt.usage,
                receipt: receipt.clone(),
            },
            operation_id,
        )
        .await?;
        Ok(reservation)
    }

    fn receipt_replayed(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        receipt: &crate::swarm_budget::SwarmUsageReceipt,
        completion: bool,
    ) -> bool {
        self.events.iter().any(|event| match event {
            SwarmBudgetEvent::UsageReported {
                operation_id: event_operation,
                owner: event_owner,
                receipt: event_receipt,
                ..
            } if !completion => {
                *event_operation == operation_id && event_owner == owner && event_receipt == receipt
            }
            SwarmBudgetEvent::ChildCompleted {
                operation_id: event_operation,
                owner: event_owner,
                receipt: event_receipt,
                ..
            } if completion => {
                *event_operation == operation_id && event_owner == owner && event_receipt == receipt
            }
            _ => false,
        })
    }

    fn root_receipt_replayed(
        &self,
        owner: &SwarmOwnerFence,
        receipt: &crate::swarm_budget::SwarmUsageReceipt,
    ) -> bool {
        self.events.iter().any(|event| {
            matches!(
                event,
                SwarmBudgetEvent::RootUsageReported {
                    owner: event_owner,
                    receipt: event_receipt,
                    ..
                } if event_owner == owner && event_receipt == receipt
            )
        })
    }

    fn validate_receipt(
        &self,
        operation_id: OperationId,
        usage: SwarmUsage,
        receipt: &crate::swarm_budget::SwarmUsageReceipt,
    ) -> Result<()> {
        receipt.validate()?;
        if receipt.operation_id != operation_id || receipt.usage != usage {
            return Err(Error::Conflict(
                "swarm usage receipt does not match the requested operation".into(),
            ));
        }
        let prior = self
            .events
            .iter()
            .filter(|event| match event {
                SwarmBudgetEvent::UsageReported {
                    operation_id: id, ..
                }
                | SwarmBudgetEvent::ChildCompleted {
                    operation_id: id, ..
                } => *id == operation_id,
                SwarmBudgetEvent::RootUsageReported { .. } => operation_id == self.session_id,
                _ => false,
            })
            .count();
        let expected = u64::try_from(prior)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| Error::Invalid("swarm usage receipt sequence exhausted".into()))?;
        if receipt.sequence != expected {
            return Err(Error::Conflict(
                "swarm usage receipt sequence is stale".into(),
            ));
        }
        Ok(())
    }

    /// Cancels a child while retaining consumed usage in the total budget.
    pub async fn cancel(
        &mut self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
    ) -> Result<SwarmForkReservation> {
        let projected = SwarmBudget::replay(self.events.clone())?;
        let reservation = projected.cancel(operation_id, owner)?;
        self.commit(
            SwarmBudgetEvent::ChildCancelled {
                operation_id,
                owner: owner.clone(),
            },
            operation_id,
        )
        .await?;
        Ok(reservation)
    }

    /// Advances the owner generation and fences all old callers durably.
    pub async fn takeover(
        &mut self,
        expected_owner: &SwarmOwnerFence,
        owner: impl Into<String>,
    ) -> Result<SwarmOwnerFence> {
        let projected = SwarmBudget::replay(self.events.clone())?;
        let next = projected.takeover(expected_owner, owner)?;
        self.commit(
            SwarmBudgetEvent::OwnerTakenOver {
                owner: next.clone(),
            },
            self.session_id,
        )
        .await?;
        Ok(next)
    }

    async fn commit(&mut self, event: SwarmBudgetEvent, operation_id: OperationId) -> Result<bool> {
        let digest = event_digest(&event)?;
        if let Some(existing) = self
            .events
            .iter()
            .find(|candidate| event_digest(candidate).ok() == Some(digest))
        {
            if existing == &event {
                self.refresh().await?;
                return Ok(false);
            }
            return Err(Error::Conflict("swarm event digest collision".into()));
        }
        match append_record(&self.stream, self.revision, &event, operation_id).await {
            Ok(()) => {
                self.refresh().await?;
                Ok(true)
            }
            Err(Error::Conflict(_)) => {
                self.refresh().await?;
                if self
                    .events
                    .iter()
                    .any(|candidate| event_digest(candidate).ok() == Some(digest))
                {
                    Ok(false)
                } else {
                    Err(Error::Conflict("swarm budget tail changed".into()))
                }
            }
            Err(Error::Indeterminate(operation)) => {
                if self.refresh().await.is_ok()
                    && self
                        .events
                        .iter()
                        .any(|candidate| event_digest(candidate).ok() == Some(digest))
                {
                    Ok(false)
                } else {
                    Err(Error::Indeterminate(operation))
                }
            }
            Err(error) => Err(error),
        }
    }
}

fn stream_for<P: StreamProvider>(
    client: &StreamClient<P>,
    session_id: OperationId,
) -> Result<Stream<P>> {
    client
        .stream(format!("{STREAM_PREFIX}/{session_id}"))
        .map_err(|error| Error::Storage(error.to_string()))
}

async fn read_events<P: StreamProvider>(stream: &Stream<P>) -> Result<Vec<SwarmBudgetEvent>> {
    let mut replay = stream.replay(0);
    let mut events = Vec::new();
    while let Some(page) = replay
        .next_page()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?
    {
        for record in page {
            let envelope: BudgetRecord = serde_json::from_slice(&record.value)
                .map_err(|error| Error::Storage(error.to_string()))?;
            if envelope.version != BUDGET_EVENT_VERSION {
                return Err(Error::Conflict(format!(
                    "unsupported swarm budget event version {}",
                    envelope.version
                )));
            }
            let prior = event_count(events.len())?;
            if envelope.revision != record.sequence.saturating_add(1)
                || envelope.revision != prior.saturating_add(1)
            {
                return Err(Error::Storage(
                    "swarm budget revision is not gapless".into(),
                ));
            }
            if event_digest(&envelope.event)? != envelope.event_digest {
                return Err(Error::Storage("swarm budget event digest mismatch".into()));
            }
            events.push(envelope.event);
            if event_count(events.len())? >= MAX_RECORDS {
                return Err(Error::Storage("swarm budget record limit exceeded".into()));
            }
        }
    }
    if events.is_empty() {
        return Err(Error::NotFound("swarm budget session".into()));
    }
    Ok(events)
}

async fn append_record<P: StreamProvider>(
    stream: &Stream<P>,
    expected_tail: u64,
    event: &SwarmBudgetEvent,
    operation_id: OperationId,
) -> Result<()> {
    let digest = event_digest(event)?;
    let envelope = BudgetRecord {
        version: BUDGET_EVENT_VERSION,
        revision: expected_tail.saturating_add(1),
        event_digest: digest,
        event: event.clone(),
    };
    let bytes = canonical_json_bytes(&envelope)?;
    if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Err(Error::Invalid(
            "swarm budget event exceeds Stream limit".into(),
        ));
    }
    let key = StreamKey::new(Bytes::copy_from_slice(&digest))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    match stream
        .append_batch(vec![Bytes::from(bytes)], Some(expected_tail), Some(key))
        .await
    {
        Ok(AppendOutcome::Committed(receipt))
            if receipt.start == expected_tail && receipt.end == expected_tail + 1 =>
        {
            Ok(())
        }
        Ok(AppendOutcome::Committed(_)) => {
            Err(Error::Storage("invalid swarm budget append receipt".into()))
        }
        Ok(AppendOutcome::TailConflict { .. }) => {
            Err(Error::Conflict("swarm budget tail changed".into()))
        }
        Err(StreamError::IdempotencyMismatch) => {
            Err(Error::Conflict("swarm budget retry identity reused".into()))
        }
        Err(StreamError::Unavailable | StreamError::DeadlineElapsed) => {
            Err(Error::Indeterminate(operation_id))
        }
        Err(error) => Err(Error::Storage(error.to_string())),
    }
}

fn event_digest(event: &SwarmBudgetEvent) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&canonical_json_bytes(event)?).as_bytes())
}

fn event_count(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Storage("swarm budget record count overflow".into()))
}

fn event_owner(event: &SwarmBudgetEvent) -> Result<SwarmOwnerFence> {
    match event {
        SwarmBudgetEvent::Started { owner, .. } => Ok(owner.clone()),
        _ => Err(Error::Invalid(
            "swarm session start event is invalid".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm_budget::{
        SwarmResourceRequest, SwarmUsage, SwarmUsageReceiptIssuer, SwarmUsageSource,
    };
    use acyclic_stream::{MemoryStream, StreamClient};
    use std::sync::Arc;

    fn limits() -> SwarmBudgetLimits {
        SwarmBudgetLimits {
            max_active_agents: 2,
            max_total_agents: 3,
            max_recursion_depth: 1,
            max_model_steps: 8,
            max_output_bytes: 128,
            max_execution_time_ms: 200,
        }
    }

    struct ZeroSource;

    impl SwarmUsageSource for ZeroSource {
        fn provider_identity(&self) -> &str {
            "journal-test-provider"
        }

        fn cumulative_usage(
            &self,
            _operation_id: OperationId,
            _dispatch_id: &IdempotencyKey,
        ) -> Result<SwarmUsage> {
            Ok(SwarmUsage::default())
        }
    }

    #[tokio::test]
    async fn verified_dispatch_commits_activation_before_releasing_rejected_child() {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0).expect("owner");
        let mut journal = SwarmBudgetJournal::start(&client, session_id, owner.clone(), limits())
            .await
            .expect("start");
        let child = OperationId::new();
        journal
            .reserve_child(SwarmForkRequest {
                operation_id: child,
                idempotency_key: IdempotencyKey::new("child").expect("key"),
                parent_operation_id: None,
                depth: 1,
                resources: SwarmResourceRequest {
                    model_steps: 4,
                    output_bytes: 64,
                    execution_time_ms: 100,
                },
                admission_digest: None,
            })
            .await
            .expect("reserve");
        assert!(matches!(
            journal
                .report_usage(child, &owner, SwarmUsage::default())
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            journal
                .report_root_usage(&owner, SwarmUsage::default())
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            journal.complete(child, &owner, SwarmUsage::default()).await,
            Err(Error::Unauthorized(_))
        ));
        let retry = journal
            .reserve_child(SwarmForkRequest {
                operation_id: child,
                idempotency_key: IdempotencyKey::new("child").expect("key"),
                parent_operation_id: None,
                depth: 1,
                resources: SwarmResourceRequest {
                    model_steps: 4,
                    output_bytes: 64,
                    execution_time_ms: 100,
                },
                admission_digest: None,
            })
            .await
            .expect("retry reserve");
        assert!(retry.replayed);
        let publication = VerifiedForkPublication::from_verified(ForkPublication {
            operation_id: child,
            parent_operation_id: None,
            completed_boundary_digest: [1; 32],
            workspace_generation_digest: [2; 32],
        })
        .expect("verified publication");
        let rejected = journal
            .dispatch_after_publication(child, owner.clone(), publication, |_token| async {
                Err::<(), _>(Error::Conflict("dispatcher rejected".into()))
            })
            .await;
        assert!(rejected.is_err());
        let usage = journal.usage().expect("usage");
        assert_eq!(usage.active_agents, 1);
        assert_eq!(usage.reserved, SwarmUsage::default());

        let unknown_child = OperationId::new();
        journal
            .reserve_child(SwarmForkRequest {
                operation_id: unknown_child,
                idempotency_key: IdempotencyKey::new("unknown-child").expect("key"),
                parent_operation_id: None,
                depth: 1,
                resources: SwarmResourceRequest {
                    model_steps: 4,
                    output_bytes: 64,
                    execution_time_ms: 100,
                },
                admission_digest: None,
            })
            .await
            .expect("reserve unknown child");
        let unknown_publication = VerifiedForkPublication::from_verified(ForkPublication {
            operation_id: unknown_child,
            parent_operation_id: None,
            completed_boundary_digest: [3; 32],
            workspace_generation_digest: [4; 32],
        })
        .expect("verified unknown publication");
        let unknown = journal
            .dispatch_after_publication(unknown_child, owner, unknown_publication, |_token| async {
                Err::<(), _>(Error::Indeterminate(unknown_child))
            })
            .await;
        assert!(matches!(unknown, Err(Error::Indeterminate(_))));
        let usage = journal.usage().expect("unknown usage");
        assert_eq!(usage.active_agents, 2);
        assert_eq!(usage.reserved.model_steps, 4);
    }

    #[tokio::test]
    async fn exact_usage_receipt_retry_is_replayed_after_restart_projection() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let mut journal =
            SwarmBudgetJournal::start(&client, session_id, owner.clone(), limits()).await?;
        let child = OperationId::new();
        journal
            .reserve_child(SwarmForkRequest {
                operation_id: child,
                idempotency_key: IdempotencyKey::new("receipt-child")?,
                parent_operation_id: None,
                depth: 1,
                resources: SwarmResourceRequest {
                    model_steps: 4,
                    output_bytes: 64,
                    execution_time_ms: 100,
                },
                admission_digest: None,
            })
            .await?;
        let publication = VerifiedForkPublication::from_verified(ForkPublication {
            operation_id: child,
            parent_operation_id: None,
            completed_boundary_digest: [9; 32],
            workspace_generation_digest: [8; 32],
        })?;
        journal
            .activate_verified(child, owner.clone(), publication)
            .await?;
        let mut issuer = SwarmUsageReceiptIssuer::new(
            ZeroSource,
            child,
            IdempotencyKey::new("receipt-dispatch")?,
        )?;
        let receipt = issuer.issue()?;
        journal
            .report_usage_with_receipt(child, &owner, receipt.clone())
            .await?;
        let retry = journal
            .report_usage_with_receipt(child, &owner, receipt)
            .await?;
        assert_eq!(retry.usage, SwarmUsage::default());
        assert_eq!(journal.usage()?.active_agents, 2);
        Ok(())
    }
}
