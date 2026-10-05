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
        SwarmBudgetUsage, SwarmDispatchContext, SwarmDispatchToken, SwarmForkRequest,
        SwarmForkReservation,
        SwarmOwnerFence, SwarmResourceRequest, SwarmRootDispatchContext, SwarmUsage,
        SwarmUsageReceiptCursor,
        VerifiedForkPublication, VerifiedSwarmUsageReceipt,
    },
};
use acyclic_stream::{
    AppendOutcome, Stream, StreamClient, StreamError, StreamProvider,
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
    #[serde(default)]
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
        Self::start_inner(client, session_id, owner, limits, None).await
    }

    /// Creates a session and binds root usage to the host-issued scheduler
    /// lease.  A durable root receipt cannot select its own dispatch identity.
    pub async fn start_with_root_dispatch(
        client: &StreamClient<P>,
        session_id: OperationId,
        owner: SwarmOwnerFence,
        limits: SwarmBudgetLimits,
        root_dispatch_id: IdempotencyKey,
    ) -> Result<Self> {
        Self::start_inner(
            client,
            session_id,
            owner,
            limits,
            Some(root_dispatch_id),
        )
        .await
    }

    async fn start_inner(
        client: &StreamClient<P>,
        session_id: OperationId,
        owner: SwarmOwnerFence,
        limits: SwarmBudgetLimits,
        root_dispatch_id: Option<IdempotencyKey>,
    ) -> Result<Self> {
        // Validate the descriptor before creating the durable stream. A
        // malformed start must never leave an unreplayable root record.
        SwarmBudget::new_with_root_dispatch(
            session_id,
            owner.clone(),
            limits,
            root_dispatch_id.clone(),
        )?;
        let stream = stream_for(client, session_id)?;
        let tail = match stream.tail().await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if tail != 0 {
            let reopened = Self::open(client, session_id).await?;
            let (_, observed_owner, observed_limits) = reopened.descriptor()?;
            if observed_owner == owner
                && observed_limits == limits
                && reopened.budget.root_dispatch_id()? == root_dispatch_id
            {
                return Ok(reopened);
            }
            return Err(Error::Conflict("swarm session descriptor differs".into()));
        }
        let event = SwarmBudgetEvent::Started {
            session_id,
            owner,
            limits,
            root_dispatch_id,
        };
        match append_record(&stream, 0, &event, session_id).await {
            Ok(()) => Self::open(client, session_id).await,
            Err(Error::Conflict(_)) => {
                let reopened = Self::open(client, session_id).await?;
                let (_, observed_owner, observed_limits) = reopened.descriptor()?;
                if observed_owner == event_owner(&event)?
                    && observed_limits == limits
                    && reopened.budget.root_dispatch_id()?
                        == event_root_dispatch_id(&event)?
                {
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

    /// Returns the durable provider receipt cursor for one child.
    pub fn usage_cursor(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<SwarmUsageReceiptCursor>> {
        self.reservation(operation_id)?
            .map(|reservation| reservation.usage_cursor())
            .transpose()
    }

    /// Returns the durable provider receipt cursor for root usage.
    pub fn root_usage_cursor(&self) -> Result<SwarmUsageReceiptCursor> {
        self.budget.root_usage_cursor()
    }

    /// Returns the root's current cumulative ceiling after descendant
    /// reservations and measured descendant consumption are accounted for.
    pub fn root_resource_limits(&self) -> Result<SwarmResourceRequest> {
        self.budget.root_resource_limits()
    }

    /// Creates the provider guard that root model work must use before it
    /// consumes another session resource slice.
    pub fn root_usage_limiter(
        &self,
    ) -> Result<crate::swarm_budget::SwarmUsageLimiter> {
        self.budget.root_usage_limiter()
    }

    /// Binds a provider measurement source to the canonical root dispatch
    /// lease and current durable root cursor.
    pub fn root_usage_receipt_issuer<S: crate::swarm_budget::SwarmUsageSource>(
        &self,
        source: S,
    ) -> Result<crate::swarm_budget::SwarmUsageReceiptIssuer<S>> {
        self.budget.root_usage_receipt_issuer(source)
    }

    /// Creates the complete provider boundary for root work from the current
    /// durable projection. The returned limiter starts from accepted root
    /// usage and excludes descendant capacity already held in the journal.
    pub fn root_usage_context<S: crate::swarm_budget::SwarmUsageSource>(
        &self,
        source: S,
    ) -> Result<SwarmRootDispatchContext<S>> {
        self.budget.root_usage_context(source)
    }

    /// Returns the host-issued lease identity bound to root usage receipts.
    /// A missing value means root model work has no verified scheduler lease.
    pub fn root_dispatch_id(&self) -> Result<Option<IdempotencyKey>> {
        self.budget.root_dispatch_id()
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
        dispatch_id: IdempotencyKey,
        publication: ForkPublication,
    ) -> Result<SwarmDispatchToken> {
        let projected = SwarmBudget::replay(self.events.clone())?;
        let token = projected.activate_with_dispatch(
            operation_id,
            owner,
            publication,
            Some(dispatch_id),
        )?;
        self.commit(
            SwarmBudgetEvent::ChildActivated {
                operation_id,
                owner: token.owner().clone(),
                publication: token.publication(),
                dispatch_id: token.dispatch_id().cloned(),
            },
            operation_id,
        )
        .await?;
        Ok(token)
    }

    /// Activates only publication evidence produced by the authoritative fork
    /// helper. Raw digest fields cannot be supplied by an external caller.
    pub async fn activate_verified_with_dispatch(
        &mut self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        dispatch_id: IdempotencyKey,
        publication: VerifiedForkPublication,
    ) -> Result<SwarmDispatchToken> {
        self.activate_raw(
            operation_id,
            owner,
            dispatch_id,
            publication.into_publication(),
        )
        .await
    }

    /// Reopens a child provider context from the receipt cursor currently
    /// retained in this journal. This is the restart-safe entry point for
    /// both a fresh dispatch and a resumed dispatch; callers cannot silently
    /// reset a child limiter to zero after a committed provider receipt.
    pub fn usage_context<S: crate::swarm_budget::SwarmUsageSource>(
        &self,
        token: &SwarmDispatchToken,
        source: S,
    ) -> Result<SwarmDispatchContext<S>> {
        let cursor = self
            .usage_cursor(token.operation_id())?
            .ok_or_else(|| Error::NotFound(format!("swarm reservation {}", token.operation_id())))?;
        token.resume_usage_context(source, cursor)
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
        dispatch_id: IdempotencyKey,
        publication: VerifiedForkPublication,
        dispatch: F,
    ) -> Result<T>
    where
        F: FnOnce(SwarmDispatchToken) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let token = self
            .activate_verified_with_dispatch(operation_id, owner.clone(), dispatch_id, publication)
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

    /// Activates a child and constructs its complete provider dispatch
    /// boundary before invoking model work.
    ///
    /// The closure receives the admitted token, a limiter initialized from
    /// the reservation ceiling, and a measurement issuer bound to the same
    /// provider dispatch identity. It should call `issue_usage_receipt` and
    /// persist the returned receipt through this journal before completion.
    pub async fn dispatch_after_publication_with_usage<S, F, Fut, T>(
        &mut self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        dispatch_id: IdempotencyKey,
        publication: VerifiedForkPublication,
        source: S,
        dispatch: F,
    ) -> Result<T>
    where
        S: crate::swarm_budget::SwarmUsageSource,
        F: FnOnce(SwarmDispatchContext<S>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.dispatch_after_publication(
            operation_id,
            owner,
            dispatch_id,
            publication,
            move |token| async move {
                let context = token.usage_context(source)?;
                dispatch(context).await
            },
        )
        .await
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
        let event = SwarmBudgetEvent::UsageReported {
            operation_id,
            owner: owner.clone(),
            usage: receipt.usage,
            receipt: receipt.clone(),
        };
        // Apply the exact durable event to an isolated projection before
        // appending it.  Calling the receipt-free mutator here would allow a
        // mismatched dispatch to pass preflight and poison the stream.
        projected.apply_event(event.clone())?;
        let reservation = projected
            .reservation(operation_id)?
            .ok_or_else(|| Error::Storage("projected swarm reservation is missing".into()))?;
        self.commit(event, operation_id).await?;
        Ok(reservation)
    }

    /// Reads authenticated provider counters and persists an intermediate
    /// receipt-backed usage report.
    pub async fn report_usage_from_source<S: crate::swarm_budget::SwarmUsageSource>(
        &mut self,
        token: &SwarmDispatchToken,
        source: S,
    ) -> Result<SwarmForkReservation> {
        let owner = token.owner().clone();
        let mut context = self.usage_context(token, source)?;
        let receipt = context.issue_usage_receipt()?;
        self.report_usage_with_receipt(token.operation_id(), &owner, receipt)
            .await
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
        let root_dispatch_id = projected.root_dispatch_id()?.ok_or_else(|| {
            Error::Unauthorized("canonical root dispatch lease required".into())
        })?;
        if receipt.dispatch_id != root_dispatch_id {
            return Err(Error::Conflict(
                "swarm root usage receipt is not bound to the canonical root lease".into(),
            ));
        }
        if self.root_receipt_replayed(owner, &receipt) {
            return projected.report_root_usage(owner, receipt.usage);
        }
        self.validate_receipt(self.session_id, receipt.usage, &receipt)?;
        let event = SwarmBudgetEvent::RootUsageReported {
            owner: owner.clone(),
            usage: receipt.usage,
            receipt: receipt.clone(),
        };
        projected.apply_event(event.clone())?;
        self.commit(event, self.session_id).await?;
        Ok(receipt.usage)
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
        let event = SwarmBudgetEvent::ChildCompleted {
            operation_id,
            owner: owner.clone(),
            usage: receipt.usage,
            receipt: receipt.clone(),
        };
        projected.apply_event(event.clone())?;
        let reservation = projected
            .reservation(operation_id)?
            .ok_or_else(|| Error::Storage("projected swarm reservation is missing".into()))?;
        self.commit(event, operation_id).await?;
        Ok(reservation)
    }

    /// Reads authenticated provider counters and completes the child with one
    /// durable receipt-backed settlement.
    pub async fn complete_from_source<S: crate::swarm_budget::SwarmUsageSource>(
        &mut self,
        token: &SwarmDispatchToken,
        source: S,
    ) -> Result<SwarmForkReservation> {
        let owner = token.owner().clone();
        let mut context = self.usage_context(token, source)?;
        let receipt = context.issue_usage_receipt()?;
        self.complete_with_receipt(token.operation_id(), &owner, receipt)
            .await
    }

    /// Reads authenticated root counters and persists one root usage receipt.
    pub async fn report_root_usage_from_source<S: crate::swarm_budget::SwarmUsageSource>(
        &mut self,
        owner: &SwarmOwnerFence,
        source: S,
    ) -> Result<SwarmUsage> {
        let cursor = self.budget.root_usage_cursor()?;
        let mut context = self.budget.root_usage_context(source)?;
        if context.receipt_cursor() != cursor {
            return Err(Error::Conflict(
                "root usage cursor changed while binding provider source".into(),
            ));
        }
        let receipt = context.issue_usage_receipt()?;
        self.report_root_usage_with_receipt(owner, receipt).await
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
    // Do not use the event digest as the provider idempotency key here.  A
    // keyed replay is reported by the Stream API as the original
    // `Committed` outcome, which makes every concurrent caller appear to have
    // won the tail CAS.  The journal's event digest scan below provides the
    // durable duplicate check, while an uncached append preserves exactly one
    // observable CAS winner.
    match stream
        .append_batch(vec![Bytes::from(bytes)], Some(expected_tail), None)
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

fn event_root_dispatch_id(event: &SwarmBudgetEvent) -> Result<Option<IdempotencyKey>> {
    match event {
        SwarmBudgetEvent::Started {
            root_dispatch_id, ..
        } => Ok(root_dispatch_id.clone()),
        _ => Err(Error::Invalid(
            "swarm session start event is invalid".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm_budget::{
        SwarmReservationState, SwarmResourceRequest, SwarmUsage, SwarmUsageReceiptIssuer,
        SwarmUsageReceipt, SwarmUsageSource,
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

    struct FixedSource(SwarmUsage);

    impl SwarmUsageSource for FixedSource {
        fn provider_identity(&self) -> &str {
            "journal-fixed-provider"
        }

        fn cumulative_usage(
            &self,
            _operation_id: OperationId,
            _dispatch_id: &IdempotencyKey,
        ) -> Result<SwarmUsage> {
            Ok(self.0)
        }
    }

    struct FailingSource;

    impl SwarmUsageSource for FailingSource {
        fn provider_identity(&self) -> &str {
            "journal-failing-provider"
        }

        fn cumulative_usage(
            &self,
            operation_id: OperationId,
            _dispatch_id: &IdempotencyKey,
        ) -> Result<SwarmUsage> {
            Err(Error::Indeterminate(operation_id))
        }
    }

    fn child_request(
        operation_id: OperationId,
        key: &str,
        depth: u32,
        resources: SwarmResourceRequest,
    ) -> SwarmForkRequest {
        SwarmForkRequest {
            operation_id,
            idempotency_key: IdempotencyKey::new(key).expect("key"),
            parent_operation_id: None,
            depth,
            resources,
            admission_digest: None,
        }
    }

    #[tokio::test]
    async fn concurrent_admission_fences_remaining_dimensions_without_leaks() -> Result<()> {
        let base_limits = SwarmBudgetLimits {
            max_active_agents: 8,
            max_total_agents: 8,
            max_recursion_depth: 1,
            max_model_steps: 100,
            max_output_bytes: 100,
            max_execution_time_ms: 100,
        };
        let resources = SwarmResourceRequest {
            model_steps: 3,
            output_bytes: 3,
            execution_time_ms: 3,
        };
        let cases = [
            (
                "active",
                SwarmBudgetLimits {
                    max_active_agents: 2,
                    ..base_limits
                },
                1,
                resources,
            ),
            (
                "total",
                SwarmBudgetLimits {
                    max_active_agents: 2,
                    max_total_agents: 2,
                    ..base_limits
                },
                1,
                resources,
            ),
            (
                "depth",
                base_limits,
                2,
                resources,
            ),
            (
                "steps",
                SwarmBudgetLimits {
                    max_model_steps: 5,
                    ..base_limits
                },
                1,
                resources,
            ),
            (
                "output",
                SwarmBudgetLimits {
                    max_output_bytes: 5,
                    ..base_limits
                },
                1,
                resources,
            ),
            (
                "time",
                SwarmBudgetLimits {
                    max_execution_time_ms: 5,
                    ..base_limits
                },
                1,
                resources,
            ),
        ];

        for (index, (dimension, limits, depth, resources)) in cases.into_iter().enumerate() {
            let client = StreamClient::new(Arc::new(MemoryStream::default()));
            let session_id = OperationId::new();
            let owner = SwarmOwnerFence::new(format!("worker-{dimension}"), 0)?;
            SwarmBudgetJournal::start(&client, session_id, owner, limits).await?;
            let mut first = SwarmBudgetJournal::open(&client, session_id).await?;
            let mut second = SwarmBudgetJournal::open(&client, session_id).await?;
            let parent = if dimension == "depth" {
                let parent_id = OperationId::from_bytes([200 + index as u8; 16]);
                let parent_resources = SwarmResourceRequest {
                    model_steps: 20,
                    output_bytes: 20,
                    execution_time_ms: 20,
                };
                let parent = first
                    .reserve_child(child_request(
                        parent_id,
                        "depth-parent",
                        1,
                        parent_resources,
                    ))
                    .await?;
                drop(second);
                second = SwarmBudgetJournal::open(&client, session_id).await?;
                Some((
                    parent_id,
                    parent.reservation.owner.clone(),
                    parent_resources,
                ))
            } else {
                None
            };
            let mut first_request = child_request(
                OperationId::from_bytes([index as u8 + 1; 16]),
                &format!("{dimension}-first"),
                depth,
                resources,
            );
            let mut second_request = child_request(
                OperationId::from_bytes([index as u8 + 17; 16]),
                &format!("{dimension}-second"),
                depth,
                resources,
            );
            first_request.parent_operation_id = parent.as_ref().map(|parent| parent.0);
            second_request.parent_operation_id = parent.as_ref().map(|parent| parent.0);
            let (first, second) = tokio::join!(
                first.reserve_child(first_request),
                second.reserve_child(second_request),
            );
            let admitted = [first, second]
                .into_iter()
                .filter_map(|result| result.ok())
                .collect::<Vec<_>>();
            if dimension == "depth" {
                assert!(
                    admitted.is_empty(),
                    "depth admission unexpectedly succeeded"
                );
            } else {
                assert_eq!(
                    admitted.len(),
                    1,
                    "{dimension} race admitted too many children"
                );
            }

            let mut journal = SwarmBudgetJournal::open(&client, session_id).await?;
            let usage = journal.usage()?;
            let baseline_agents = if parent.is_some() { 2 } else { 1 };
            let baseline_reserved = parent
                .as_ref()
                .map(|parent| SwarmUsage {
                    model_steps: parent.2.model_steps,
                    output_bytes: parent.2.output_bytes,
                    execution_time_ms: parent.2.execution_time_ms,
                })
                .unwrap_or_default();
            assert_eq!(
                usage.active_agents,
                baseline_agents + admitted.len() as u64,
                "{dimension}"
            );
            assert_eq!(
                usage.total_agents,
                baseline_agents + admitted.len() as u64,
                "{dimension}"
            );
            assert_eq!(
                usage.reserved,
                if admitted.is_empty() {
                    baseline_reserved
                } else {
                    SwarmUsage {
                        model_steps: baseline_reserved.model_steps + resources.model_steps,
                        output_bytes: baseline_reserved.output_bytes + resources.output_bytes,
                        execution_time_ms: baseline_reserved.execution_time_ms
                            + resources.execution_time_ms,
                    }
                },
                "{dimension} reservation projection"
            );
            if let Some(receipt) = admitted.first() {
                journal
                    .cancel(receipt.reservation.operation_id, &receipt.reservation.owner)
                    .await?;
                let usage = journal.usage()?;
                assert_eq!(usage.active_agents, 1, "{dimension} cancellation leak");
                assert_eq!(
                    usage.reserved,
                    baseline_reserved,
                    "{dimension} resource leak"
                );
            }
            if let Some((parent_id, parent_owner, _)) = parent {
                journal.cancel(parent_id, &parent_owner).await?;
                let usage = journal.usage()?;
                assert_eq!(usage.active_agents, 1, "{dimension} parent leak");
                assert_eq!(usage.reserved, SwarmUsage::default(), "{dimension} parent budget");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn admitted_child_shares_measured_usage_context_through_settlement() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let limits = limits();
        let root_dispatch = IdempotencyKey::new("settle-root-dispatch")?;
        SwarmBudgetJournal::start_with_root_dispatch(
            &client,
            session_id,
            owner.clone(),
            limits,
            root_dispatch,
        )
        .await?;
        let child = OperationId::new();
        let resources = SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 64,
            execution_time_ms: 100,
        };
        let request = child_request(child, "settle-child", 1, resources);
        let mut journal = SwarmBudgetJournal::open(&client, session_id).await?;
        let root_usage = SwarmUsage {
            model_steps: 1,
            output_bytes: 8,
            execution_time_ms: 10,
        };
        let mut root_context = journal.root_usage_context(FixedSource(root_usage))?;
        let root_receipt = root_context.issue_usage_receipt()?;
        journal
            .report_root_usage_with_receipt(&owner, root_receipt)
            .await?;
        let admission = journal.reserve_child(request.clone()).await?;
        // The parent projection can change between a successful reservation
        // and a retry. The accepted child allocation remains stable and must
        // replay instead of being re-derived from this smaller remainder.
        let mut later_root_context = journal.root_usage_context(FixedSource(SwarmUsage {
            model_steps: 2,
            output_bytes: 16,
            execution_time_ms: 20,
        }))?;
        let later_root_receipt = later_root_context.issue_usage_receipt()?;
        journal
            .report_root_usage_with_receipt(&owner, later_root_receipt)
            .await?;
        let retry = journal.reserve_child(request).await?;
        assert!(retry.replayed);
        assert_eq!(retry.reservation, admission.reservation);

        let publication = VerifiedForkPublication::from_verified(ForkPublication {
            operation_id: child,
            parent_operation_id: None,
            completed_boundary_digest: [11; 32],
            workspace_generation_digest: [12; 32],
        })?;
        let dispatch_id = IdempotencyKey::new("settle-dispatch")?;
        let token = journal
            .activate_verified_with_dispatch(child, owner.clone(), dispatch_id, publication)
            .await?;
        assert_eq!(journal.usage()?.active_agents, 2);

        let measured = SwarmUsage {
            model_steps: 2,
            output_bytes: 20,
            execution_time_ms: 30,
        };
        let mut report_context = journal.usage_context(&token, FixedSource(measured))?;
        let report_receipt = report_context.issue_usage_receipt()?;
        let reported = journal
            .report_usage_with_receipt(child, &owner, report_receipt.clone())
            .await?;
        assert_eq!(reported.usage, measured);
        let reported_retry = journal
            .report_usage_with_receipt(child, &owner, report_receipt)
            .await?;
        assert_eq!(reported_retry.usage, measured);
        assert_eq!(
            journal.usage()?.consumed,
            SwarmUsage {
                model_steps: 3,
                output_bytes: 28,
                execution_time_ms: 40,
            }
        );
        assert_eq!(
            journal.usage()?.reserved,
            SwarmUsage {
                model_steps: 2,
                output_bytes: 44,
                execution_time_ms: 70,
            }
        );

        let mut completion_context = journal.usage_context(&token, FixedSource(measured))?;
        let completion_receipt = completion_context.issue_usage_receipt()?;
        let completed = journal
            .complete_with_receipt(child, &owner, completion_receipt.clone())
            .await?;
        assert_eq!(completed.state, SwarmReservationState::Completed);
        let usage = journal.usage()?;
        assert_eq!(usage.active_agents, 1);
        assert_eq!(
            usage.consumed,
            SwarmUsage {
                model_steps: 3,
                output_bytes: 28,
                execution_time_ms: 40,
            }
        );
        assert_eq!(usage.reserved, SwarmUsage::default());
        let settled_retry = journal
            .complete_with_receipt(child, &owner, completion_receipt)
            .await?;
        assert_eq!(settled_retry.state, SwarmReservationState::Completed);
        assert_eq!(journal.usage()?, usage);
        Ok(())
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
            .dispatch_after_publication(
                child,
                owner.clone(),
                IdempotencyKey::new("dispatch-rejected").expect("dispatch key"),
                publication,
                |_token| async { Err::<(), _>(Error::Conflict("dispatcher rejected".into())) },
            )
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
            .dispatch_after_publication(
                unknown_child,
                owner,
                IdempotencyKey::new("dispatch-unknown").expect("dispatch key"),
                unknown_publication,
                |_token| async { Err::<(), _>(Error::Indeterminate(unknown_child)) },
            )
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
            .activate_verified_with_dispatch(
                child,
                owner.clone(),
                IdempotencyKey::new("receipt-dispatch")?,
                publication,
            )
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

    #[tokio::test]
    async fn unknown_provider_usage_keeps_active_reservation_for_recovery() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let mut journal =
            SwarmBudgetJournal::start(&client, session_id, owner.clone(), limits()).await?;
        let child = OperationId::new();
        let resources = SwarmResourceRequest {
            model_steps: 2,
            output_bytes: 32,
            execution_time_ms: 40,
        };
        journal
            .reserve_child(child_request(child, "unknown-usage", 1, resources))
            .await?;
        let publication = VerifiedForkPublication::from_verified(ForkPublication {
            operation_id: child,
            parent_operation_id: None,
            completed_boundary_digest: [21; 32],
            workspace_generation_digest: [22; 32],
        })?;
        let token = journal
            .activate_verified_with_dispatch(
                child,
                owner.clone(),
                IdempotencyKey::new("unknown-usage-dispatch")?,
                publication,
            )
            .await?;
        let before = journal.usage()?;
        assert!(matches!(
            journal.complete_from_source(&token, FailingSource()).await,
            Err(Error::Indeterminate(operation)) if operation == child
        ));
        assert_eq!(journal.usage()?, before);
        assert_eq!(
            journal.reservation(child)?.expect("reservation").state,
            SwarmReservationState::Active
        );
        Ok(())
    }

    #[tokio::test]
    async fn wrong_operation_or_dispatch_receipt_is_rejected_before_projection_mutation(
    ) -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let mut journal =
            SwarmBudgetJournal::start(&client, session_id, owner.clone(), limits()).await?;
        let child = OperationId::new();
        let resources = SwarmResourceRequest {
            model_steps: 2,
            output_bytes: 32,
            execution_time_ms: 40,
        };
        journal
            .reserve_child(child_request(child, "wrong-operation", 1, resources))
            .await?;
        let publication = VerifiedForkPublication::from_verified(ForkPublication {
            operation_id: child,
            parent_operation_id: None,
            completed_boundary_digest: [23; 32],
            workspace_generation_digest: [24; 32],
        })?;
        journal
            .activate_verified_with_dispatch(
                child,
                owner.clone(),
                IdempotencyKey::new("wrong-operation-dispatch")?,
                publication,
            )
            .await?;
        let before = journal.usage()?;
        let forged_dispatch = SwarmUsageReceipt::new(
            child,
            IdempotencyKey::new("wrong-dispatch")?,
            1,
            SwarmUsage {
                model_steps: 1,
                output_bytes: 8,
                execution_time_ms: 10,
            },
            "malicious-provider",
        )?;
        let forged_dispatch = VerifiedSwarmUsageReceipt::from_verified(forged_dispatch)?;
        assert!(matches!(
            journal
                .report_usage_with_receipt(child, &owner, forged_dispatch)
                .await,
            Err(Error::Conflict(_))
        ));
        let forged_operation = SwarmUsageReceipt::new(
            OperationId::new(),
            IdempotencyKey::new("wrong-operation-dispatch")?,
            1,
            SwarmUsage {
                model_steps: 1,
                output_bytes: 8,
                execution_time_ms: 10,
            },
            "malicious-provider",
        )?;
        let forged_operation = VerifiedSwarmUsageReceipt::from_verified(forged_operation)?;
        assert!(matches!(
            journal
                .report_usage_with_receipt(child, &owner, forged_operation)
                .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(journal.usage()?, before);
        Ok(())
    }

    #[tokio::test]
    async fn recursive_remaining_reservation_survives_restart_projection() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let limits = SwarmBudgetLimits {
            max_active_agents: 4,
            max_total_agents: 4,
            max_recursion_depth: 2,
            max_model_steps: 20,
            max_output_bytes: 100,
            max_execution_time_ms: 100,
        };
        SwarmBudgetJournal::start(&client, session_id, owner.clone(), limits).await?;
        let parent = OperationId::from_bytes([31; 16]);
        let parent_resources = SwarmResourceRequest {
            model_steps: 10,
            output_bytes: 60,
            execution_time_ms: 60,
        };
        let child_resources = SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 30,
            execution_time_ms: 30,
        };
        let mut journal = SwarmBudgetJournal::open(&client, session_id).await?;
        journal
            .reserve_child(child_request(
                parent,
                "recursive-parent",
                1,
                parent_resources,
            ))
            .await?;
        let first_child = OperationId::from_bytes([32; 16]);
        journal
            .reserve_child(SwarmForkRequest {
                operation_id: first_child,
                parent_operation_id: Some(parent),
                ..child_request(first_child, "recursive-first-child", 2, child_resources)
            })
            .await?;
        drop(journal);

        let mut reopened = SwarmBudgetJournal::open(&client, session_id).await?;
        let second_child = OperationId::from_bytes([33; 16]);
        let mut oversized = child_request(second_child, "recursive-oversized", 2, child_resources);
        oversized.parent_operation_id = Some(parent);
        oversized.resources.output_bytes = 31;
        assert!(matches!(reopened.reserve_child(oversized).await, Err(Error::Conflict(_))));
        assert_eq!(reopened.usage()?.reserved.output_bytes, 90);

        reopened.cancel(first_child, &owner).await?;
        drop(reopened);
        let mut reopened = SwarmBudgetJournal::open(&client, session_id).await?;
        let replacement = OperationId::from_bytes([34; 16]);
        let mut replacement_request =
            child_request(replacement, "recursive-replacement", 2, child_resources);
        replacement_request.parent_operation_id = Some(parent);
        reopened.reserve_child(replacement_request).await?;
        assert_eq!(reopened.usage()?.reserved.output_bytes, 90);
        Ok(())
    }

    #[tokio::test]
    async fn terminal_child_rejects_late_measured_receipt_without_budget_mutation() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let mut journal =
            SwarmBudgetJournal::start(&client, session_id, owner.clone(), limits()).await?;
        let child = OperationId::new();
        let resources = SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 64,
            execution_time_ms: 100,
        };
        journal
            .reserve_child(child_request(child, "terminal-receipt", 1, resources))
            .await?;
        let publication = VerifiedForkPublication::from_verified(ForkPublication {
            operation_id: child,
            parent_operation_id: None,
            completed_boundary_digest: [17; 32],
            workspace_generation_digest: [18; 32],
        })?;
        let dispatch_id = IdempotencyKey::new("terminal-dispatch")?;
        let token = journal
            .activate_verified_with_dispatch(child, owner.clone(), dispatch_id, publication)
            .await?;
        let measured = SwarmUsage {
            model_steps: 2,
            output_bytes: 20,
            execution_time_ms: 30,
        };
        let mut completion_context = journal.usage_context(&token, FixedSource(measured))?;
        let completion_receipt = completion_context.issue_usage_receipt()?;
        journal
            .complete_with_receipt(child, &owner, completion_receipt)
            .await?;
        let settled = journal.usage()?;

        // A provider can report a final event before surfacing an error or
        // cancellation. Once the durable child terminal event won, a later
        // cumulative receipt must not reopen the reservation or charge it a
        // second time.
        let late_usage = SwarmUsage {
            model_steps: 3,
            output_bytes: 30,
            execution_time_ms: 40,
        };
        let mut late_context = journal.usage_context(&token, FixedSource(late_usage))?;
        let late_receipt = late_context.issue_usage_receipt()?;
        assert!(matches!(
            journal
                .report_usage_with_receipt(child, &owner, late_receipt)
                .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(journal.usage()?, settled);
        assert_eq!(
            journal.reservation(child)?.expect("reservation").state,
            SwarmReservationState::Completed
        );
        Ok(())
    }
}
