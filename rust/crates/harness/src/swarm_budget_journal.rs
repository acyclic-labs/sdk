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
        RootBudgetClaim, RootBudgetRefresh, SwarmOwnerFence, SwarmResourceRequest,
        SwarmRootDispatchContext, SwarmUsage,
        SwarmUsageReceiptCursor,
        VerifiedForkPublication, VerifiedSwarmUsageReceipt, DispatchConfirmation,
    },
};
use acyclic_stream::{
    AppendOutcome, Stream, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc};

/// Authenticated liveness proof used before replacing a live swarm owner.
///
/// A descriptor match is not evidence that the previous process stopped. An
/// implementation must verify the durable host lease (and its expiry) before
/// returning `true`; callers should obtain this proof from the scheduler or
/// host authority that issued the lease.
pub trait SwarmOwnerLeaseProof: Send + Sync {
    /// Returns whether the observed owner lease is durably expired.
    fn prove_expired<'a>(
        &'a self,
        owner: &'a SwarmOwnerFence,
    ) -> futures::future::BoxFuture<'a, Result<bool>>;
}

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
        Self::start_inner(client, session_id, owner, limits, None, None).await
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
            None,
        )
        .await
    }

    /// Reopens an exact owner after a restart. A different live owner is
    /// rejected until an authenticated lease-expiry proof is supplied through
    /// [`Self::start_with_root_dispatch_recovering_with_proof`].
    pub async fn start_with_root_dispatch_recovering(
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
            None,
        )
        .await
    }

    /// Reopens a root budget and takes over only after the host authority
    /// proves that the prior owner's lease expired. Matching a descriptor is
    /// insufficient because the prior process may still be dispatching.
    pub async fn start_with_root_dispatch_recovering_with_proof(
        client: &StreamClient<P>,
        session_id: OperationId,
        owner: SwarmOwnerFence,
        limits: SwarmBudgetLimits,
        root_dispatch_id: IdempotencyKey,
        proof: Arc<dyn SwarmOwnerLeaseProof>,
    ) -> Result<Self> {
        Self::start_inner(
            client,
            session_id,
            owner,
            limits,
            Some(root_dispatch_id),
            Some(proof),
        )
        .await
    }

    async fn start_inner(
        client: &StreamClient<P>,
        session_id: OperationId,
        owner: SwarmOwnerFence,
        limits: SwarmBudgetLimits,
        root_dispatch_id: Option<IdempotencyKey>,
        lease_proof: Option<Arc<dyn SwarmOwnerLeaseProof>>,
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
            let mut reopened = Self::open(client, session_id).await?;
            let (_, observed_owner, observed_limits) = reopened.descriptor()?;
            if observed_limits == limits && reopened.budget.root_dispatch_id()? == root_dispatch_id {
                if observed_owner == owner {
                    return Ok(reopened);
                }
                if let Some(proof) = lease_proof.as_ref()
                    && proof.prove_expired(&observed_owner).await?
                {
                    reopened.takeover(&observed_owner, owner.owner).await?;
                    return Ok(reopened);
                }
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
                let mut reopened = Self::open(client, session_id).await?;
                let (_, observed_owner, observed_limits) = reopened.descriptor()?;
                if observed_limits == limits
                    && reopened.budget.root_dispatch_id()? == event_root_dispatch_id(&event)?
                {
                    if observed_owner == event_owner(&event)? {
                        return Ok(reopened);
                    }
                    if let Some(proof) = lease_proof.as_ref()
                        && proof.prove_expired(&observed_owner).await?
                    {
                        reopened
                            .takeover(&observed_owner, event_owner(&event)?.owner)
                            .await?;
                        return Ok(reopened);
                    }
                }
                Err(Error::Conflict("swarm session descriptor differs".into()))
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

    /// Wraps a real provider with the complete root admission and measurement
    /// boundary derived from this durable projection.
    pub fn metered_root_provider<M, S>(
        &self,
        provider: Arc<M>,
        source: S,
    ) -> Result<(
        Arc<crate::swarm_budget::MeteredModelProvider<M, S>>,
        crate::swarm_budget::SwarmProviderMeter<S>,
    )>
    where
        M: crate::model::ModelProvider + ?Sized + 'static,
        S: crate::swarm_budget::SwarmUsageSource + Send + Sync + 'static,
    {
        let context = self.root_usage_context(source)?;
        Ok(crate::swarm_budget::MeteredModelProvider::new_root(
            provider, context,
        ))
    }

    /// Wraps root work with a durable refresh callback. The callback reloads
    /// the current journal projection immediately before each provider call,
    /// so concurrent handles cannot spend capacity held by descendants.
    pub fn metered_root_provider_with_refresh<M, S>(
        &self,
        provider: Arc<M>,
        source: S,
        refresh: RootBudgetRefresh,
    ) -> Result<(
        Arc<crate::swarm_budget::MeteredModelProvider<M, S>>,
        crate::swarm_budget::SwarmProviderMeter<S>,
    )>
    where
        M: crate::model::ModelProvider + ?Sized + 'static,
        S: crate::swarm_budget::SwarmUsageSource + Send + Sync + 'static,
    {
        let context = self.root_usage_context(source)?;
        Ok(crate::swarm_budget::MeteredModelProvider::new_root_with_refresh(
            provider, context, refresh,
        ))
    }

    /// Wraps root work with durable refresh and claim callbacks. A fresh
    /// model step claims capacity in the same journal CAS sequence as child
    /// reservations; recovery only refreshes the existing claim.
    pub fn metered_root_provider_with_refresh_and_claim<M, S>(
        &self,
        provider: Arc<M>,
        source: S,
        refresh: RootBudgetRefresh,
        claim: RootBudgetClaim,
    ) -> Result<(
        Arc<crate::swarm_budget::MeteredModelProvider<M, S>>,
        crate::swarm_budget::SwarmProviderMeter<S>,
    )>
    where
        M: crate::model::ModelProvider + ?Sized + 'static,
        S: crate::swarm_budget::SwarmUsageSource + Send + Sync + 'static,
    {
        let context = self.root_usage_context(source)?;
        Ok(
            crate::swarm_budget::MeteredModelProvider::new_root_with_refresh_and_claim(
                provider, context, refresh, claim,
            ),
        )
    }

    /// Wraps root work with durable refresh and one coordinated dispatch
    /// permit that is committed together with the execution start marker.
    pub fn metered_root_provider_with_refresh_and_permit<M, S>(
        &self,
        provider: Arc<M>,
        source: S,
        refresh: RootBudgetRefresh,
        permit: crate::swarm_budget::DispatchPermitFactory,
    ) -> Result<(
        Arc<crate::swarm_budget::MeteredModelProvider<M, S>>,
        crate::swarm_budget::SwarmProviderMeter<S>,
    )>
    where
        M: crate::model::ModelProvider + ?Sized + 'static,
        S: crate::swarm_budget::SwarmUsageSource + Send + Sync + 'static,
    {
        let context = self.root_usage_context(source)?;
        Ok(crate::swarm_budget::MeteredModelProvider::new_root_with_refresh_and_permit(
            provider,
            context,
            refresh,
            permit,
        ))
    }

    /// Wraps a real child provider with the admitted reservation boundary.
    pub fn metered_provider<M, S>(
        &self,
        token: &SwarmDispatchToken,
        provider: Arc<M>,
        source: S,
    ) -> Result<(
        Arc<crate::swarm_budget::MeteredModelProvider<M, S>>,
        crate::swarm_budget::SwarmProviderMeter<S>,
    )>
    where
        M: crate::model::ModelProvider + ?Sized + 'static,
        S: crate::swarm_budget::SwarmUsageSource + Send + Sync + 'static,
    {
        let reservation = self
            .reservation(token.operation_id())?
            .ok_or_else(|| Error::NotFound(format!("swarm reservation {}", token.operation_id())))?;
        if reservation.confirmed_dispatches.is_empty() {
            return Err(Error::Conflict(
                "unconfirmed swarm dispatch requires a permit or confirmation boundary".into(),
            ));
        }
        let context = self.usage_context(token, source)?;
        Ok(crate::swarm_budget::MeteredModelProvider::new(
            provider, context,
        ))
    }

    /// Wraps a child provider with a final journal confirmation. Admission
    /// remains active and cancellable until the execution layer has prepared
    /// its immutable request and invokes `before_model_dispatch`.
    pub(crate) fn metered_provider_with_confirmation<M, S>(
        &self,
        token: &SwarmDispatchToken,
        provider: Arc<M>,
        source: S,
        confirmation: DispatchConfirmation,
    ) -> Result<(
        Arc<crate::swarm_budget::MeteredModelProvider<M, S>>,
        crate::swarm_budget::SwarmProviderMeter<S>,
    )>
    where
        M: crate::model::ModelProvider + ?Sized + 'static,
        S: crate::swarm_budget::SwarmUsageSource + Send + Sync + 'static,
    {
        let context = self.usage_context(token, source)?;
        Ok(crate::swarm_budget::MeteredModelProvider::new_with_dispatch_confirmation(
            provider,
            context,
            confirmation,
        ))
    }

    /// Wraps a child provider with a cross-stream permit factory. The first
    /// model start commits the budget confirmation and execution marker in a
    /// single Stream transaction; later model steps on the same provider
    /// continue under that already-confirmed child lease.
    pub(crate) fn metered_provider_with_dispatch_permit<M, S>(
        &self,
        token: &SwarmDispatchToken,
        provider: Arc<M>,
        source: S,
        permit: crate::swarm_budget::DispatchPermitFactory,
    ) -> Result<(
        Arc<crate::swarm_budget::MeteredModelProvider<M, S>>,
        crate::swarm_budget::SwarmProviderMeter<S>,
    )>
    where
        M: crate::model::ModelProvider + ?Sized + 'static,
        S: crate::swarm_budget::SwarmUsageSource + Send + Sync + 'static,
    {
        let context = self.usage_context(token, source)?;
        Ok(crate::swarm_budget::MeteredModelProvider::new_with_dispatch_permit(
            provider,
            context,
            permit,
        ))
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

    /// Confirms an admitted provider lease immediately before entering the
    /// provider boundary. Activation and confirmation are separate durable
    /// records so a process dying between them cannot be mistaken for a
    /// started model attempt on restart.
    pub async fn confirm_dispatch(
        &mut self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        dispatch_id: IdempotencyKey,
    ) -> Result<SwarmDispatchToken> {
        let projected = SwarmBudget::replay(self.events.clone())?;
        let token = projected.confirm_dispatch(operation_id, owner, dispatch_id.clone())?;
        let request_digest = *blake3::hash(dispatch_id.0.as_bytes()).as_bytes();
        let applied = self.commit(
            SwarmBudgetEvent::ChildDispatchConfirmed {
                operation_id,
                owner: owner.clone(),
                dispatch_id,
                step: 0,
                request_digest,
            },
            operation_id,
        )
        .await?;
        if !applied {
            return Err(Error::Indeterminate(operation_id));
        }
        Ok(token)
    }

    /// Prepares the exact budget mutation for a coordinated execution start.
    /// The execution journal must append its `ModelStarted` observation in
    /// the same Stream commit; this method intentionally does not mutate the
    /// budget tail on its own.
    pub fn dispatch_permit(
        &self,
        operation_id: OperationId,
        owner: &SwarmOwnerFence,
        step: u32,
        request_digest: [u8; 32],
        dispatch_id: IdempotencyKey,
    ) -> Result<crate::model::ModelDispatchPermit> {
        let projected = SwarmBudget::replay(self.events.clone())?;
        let reservation = projected
            .reservation(operation_id)?
            .ok_or_else(|| Error::NotFound(format!("swarm reservation {operation_id}")))?;
        if reservation.owner != *owner
            || !matches!(
                reservation.state,
                crate::swarm_budget::SwarmReservationState::Active
            )
        {
            return Err(Error::Conflict("stale or inactive swarm dispatch permit".into()));
        }
        if reservation.dispatch_id.as_ref() != Some(&dispatch_id) {
            if reservation.dispatch_id.is_none() {
                return Err(Error::Conflict("swarm dispatch identity is missing".into()));
            }
            let expected = format!(
                "{}:model:{}:{}",
                reservation.dispatch_id.as_ref().expect("checked above").0,
                step,
                blake3::hash(&request_digest).to_hex(),
            );
            if dispatch_id.0 != expected {
                return Err(Error::Conflict(
                    "swarm dispatch identity is not bound to the admitted lease".into(),
                ));
            }
        }
        if let Some((existing_digest, existing_id)) = reservation.confirmed_dispatches.get(&step) {
            if existing_digest == &request_digest && existing_id == &dispatch_id {
                return Err(Error::Indeterminate(operation_id));
            }
            return Err(Error::Conflict("swarm model dispatch identity differs".into()));
        }
        let event = SwarmBudgetEvent::ChildDispatchConfirmed {
            operation_id,
            owner: owner.clone(),
            dispatch_id,
            step,
            request_digest,
        };
        let event_digest = event_digest(&event)?;
        let envelope = BudgetRecord {
            version: BUDGET_EVENT_VERSION,
            revision: self.revision.saturating_add(1),
            event_digest,
            event,
        };
        let budget_record = canonical_json_bytes(&envelope)?;
        let idempotency_key = format!(
            "swarm-dispatch:{}:{}",
            self.session_id,
            blake3::hash(&event_digest).to_hex(),
        )
        .into_bytes();
        Ok(crate::model::ModelDispatchPermit {
            budget_session: self.session_id,
            budget_path: self.stream.path().as_str().to_owned(),
            budget_tail: self.revision,
            budget_record,
            idempotency_key,
        })
    }

    /// Prepares the root step claim for the same coordinated commit used by
    /// child dispatches. The projected claim validates the combined session
    /// ceiling without advancing this journal's live tail.
    pub fn root_dispatch_permit(
        &self,
        owner: &SwarmOwnerFence,
        operation_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
    ) -> Result<crate::model::ModelDispatchPermit> {
        let projected = SwarmBudget::replay(self.events.clone())?;
        let dispatch_id = IdempotencyKey::new(format!("root:{operation_id}:{step}"))?;
        let usage = projected.claim_root_model_step(
            owner,
            operation_id,
            step,
            request_digest,
            dispatch_id.clone(),
        )?;
        let _ = usage;
        let event = SwarmBudgetEvent::RootModelStepClaimed {
            owner: owner.clone(),
            operation_id,
            step,
            request_digest,
            dispatch_id: Some(dispatch_id),
        };
        let event_digest = event_digest(&event)?;
        let envelope = BudgetRecord {
            version: BUDGET_EVENT_VERSION,
            revision: self.revision.saturating_add(1),
            event_digest,
            event,
        };
        Ok(crate::model::ModelDispatchPermit {
            budget_session: self.session_id,
            budget_path: self.stream.path().as_str().to_owned(),
            budget_tail: self.revision,
            budget_record: canonical_json_bytes(&envelope)?,
            idempotency_key: format!(
                "swarm-root-dispatch:{}:{}",
                self.session_id,
                blake3::hash(&event_digest).to_hex(),
            )
            .into_bytes(),
        })
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
        let reservation = self
            .reservation(token.operation_id())?
            .ok_or_else(|| Error::NotFound(format!("swarm reservation {}", token.operation_id())))?;
        if !matches!(
            reservation.state,
            crate::swarm_budget::SwarmReservationState::Active
        ) {
            return Err(Error::Conflict(
                "provider usage context requires an active swarm reservation".into(),
            ));
        }
        if reservation.owner != *token.owner() {
            return Err(Error::Conflict("stale swarm dispatch token".into()));
        }
        let cursor = reservation.usage_cursor();
        let context = token.resume_usage_context(source, cursor?)?;
        if let Some(usage) = context.receipt_cursor().usage {
            context.restore_runtime_usage(usage)?;
        }
        Ok(context)
    }

    /// Reconstructs an active child token after a process restart.
    ///
    /// The activation event already owns the reservation and publication;
    /// replaying it as a new admission would double count active and total
    /// capacity.  Recovery therefore rebuilds the token from the durable
    /// projection and resumes its receipt cursor through [`Self::usage_context`].
    pub fn resume_dispatch_token(
        &self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
    ) -> Result<SwarmDispatchToken> {
        self.budget.resume_active_with_dispatch(operation_id, owner)
    }

    /// Reconstructs a still-unconfirmed dispatch for the provider boundary.
    /// The returned token is only safe when paired with the confirmation
    /// callback installed by [`Self::metered_provider_with_confirmation`].
    pub fn resume_unconfirmed_dispatch_token(
        &self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
    ) -> Result<SwarmDispatchToken> {
        self.budget
            .resume_active_unconfirmed_with_dispatch(operation_id, owner)
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
        let dispatch_id = token.required_dispatch_id()?.clone();
        // The closure is the final provider boundary. Confirmation must be
        // ordered immediately before it is called, so cancellation can win
        // while this reservation is still merely active.
        let token = self
            .confirm_dispatch(operation_id, &owner, dispatch_id)
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

    /// Durably claims one root model step before the provider starts. The
    /// append is CAS ordered with child reservations, so a child reservation
    /// that wins first is reflected in the claim's remaining budget.
    pub async fn claim_root_model_step(
        &mut self,
        owner: &SwarmOwnerFence,
        operation_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
    ) -> Result<SwarmUsage> {
        let mut retries = 0;
        loop {
            // Refresh before preflight so a handle that survived a takeover
            // cannot authorize a claim from its stale projection.
            self.refresh().await?;
            let projected = SwarmBudget::replay(self.events.clone())?;
            // Validate the live fence before consulting the idempotency record.
            // Otherwise a stale owner could replay an old claim successfully
            // after takeover merely because its operation/step identity was
            // already present in the journal.
            if projected.owner()? != *owner {
                return Err(Error::Conflict("stale swarm owner generation".into()));
            }
            if self.events.iter().any(|event| {
                matches!(
                    event,
                    SwarmBudgetEvent::RootModelStepClaimed {
                        operation_id: claimed_operation,
                        step: claimed_step,
                        request_digest: claimed_digest,
                        ..
                    } if *claimed_operation == operation_id
                        && *claimed_step == step
                        && *claimed_digest == request_digest
                )
            }) {
                // A durable root claim is a one-shot provider right.  A
                // replayed or concurrent handle must reconcile the existing
                // attempt rather than invoke the provider again.
                return Err(Error::Indeterminate(operation_id));
            }
            let dispatch_id = IdempotencyKey::new(format!("root:{operation_id}:{step}"))?;
            let usage = projected.claim_root_model_step(
                owner,
                operation_id,
                step,
                request_digest,
                dispatch_id.clone(),
            )?;
            let event = SwarmBudgetEvent::RootModelStepClaimed {
                owner: owner.clone(),
                operation_id,
                step,
                request_digest,
                dispatch_id: Some(dispatch_id),
            };
            match self.commit(event, operation_id).await {
                Ok(true) => return Ok(usage),
                Ok(false) => return Err(Error::Indeterminate(operation_id)),
                Err(Error::Conflict(_)) if retries < MAX_ADMISSION_RETRIES => {
                    retries = retries.saturating_add(1);
                    self.refresh().await?;
                }
                Err(error) => return Err(error),
            }
        }
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

/// Verifies that a coordinated execution commit carries the exact budget
/// mutation admitted for its model operation. The permit is intentionally
/// opaque to callers; this check also binds its serialized event and stream
/// scope before the cross-stream CAS is attempted.
pub(crate) fn validate_model_dispatch_permit(
    permit: &crate::model::ModelDispatchPermit,
    operation_id: OperationId,
    step: u32,
    request_digest: [u8; 32],
) -> Result<()> {
    if permit.budget_session.into_bytes() == [0; 16]
        || permit.budget_path != format!("{STREAM_PREFIX}/{}", permit.budget_session)
        || permit.budget_tail >= MAX_RECORDS
        || permit.idempotency_key.is_empty()
        || request_digest == [0; 32]
    {
        return Err(Error::Invalid("model dispatch permit scope is invalid".into()));
    }
    let record: BudgetRecord = serde_json::from_slice(&permit.budget_record)
        .map_err(|error| Error::Invalid(format!("model dispatch permit is invalid: {error}")))?;
    if record.version != BUDGET_EVENT_VERSION || record.revision != permit.budget_tail + 1 {
        return Err(Error::Conflict(
            "model dispatch permit revision is not bound to its budget tail".into(),
        ));
    }
    if event_digest(&record.event)? != record.event_digest {
        return Err(Error::Invalid(
            "model dispatch permit event digest is invalid".into(),
        ));
    }
    match &record.event {
        SwarmBudgetEvent::ChildDispatchConfirmed {
            operation_id: event_operation,
            step: event_step,
            request_digest: event_request_digest,
            ..
        } if *event_operation == operation_id
            && *event_step == step
            && *event_request_digest == request_digest => {
            let expected = format!(
                "swarm-dispatch:{}:{}",
                permit.budget_session,
                blake3::hash(&record.event_digest).to_hex(),
            );
            if permit.idempotency_key != expected.as_bytes() {
                return Err(Error::Conflict(
                    "child model dispatch permit identity is not bound to its event".into(),
                ));
            }
            Ok(())
        }
        SwarmBudgetEvent::RootModelStepClaimed {
            operation_id: event_operation,
            step: event_step,
            request_digest: event_request_digest,
            ..
        } if *event_operation == operation_id
            && *event_step == step
            && *event_request_digest == request_digest => {
            let expected = format!(
                "swarm-root-dispatch:{}:{}",
                permit.budget_session,
                blake3::hash(&record.event_digest).to_hex(),
            );
            if permit.idempotency_key != expected.as_bytes() {
                return Err(Error::Conflict(
                    "root model dispatch permit identity is not bound to its event".into(),
                ));
            }
            Ok(())
        }
        _ => Err(Error::Conflict(
            "model dispatch permit is bound to another operation or request".into(),
        )),
    }
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
        SwarmResourceRequest, SwarmUsage, SwarmUsageReceiptIssuer, SwarmUsageSource,
    };
    use crate::{
        model::{ModelAttempt, ModelEvent, ModelProvider},
        model_input::PreparedModelInput,
    };
    use acyclic_stream::{MemoryStream, StreamClient};
    use futures::{StreamExt, future::BoxFuture, stream::BoxStream};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

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

    #[derive(Clone)]
    struct RuntimeSource(Arc<Mutex<SwarmUsage>>);

    impl SwarmUsageSource for RuntimeSource {
        fn provider_identity(&self) -> &str {
            "journal-runtime-provider"
        }

        fn cumulative_usage(
            &self,
            _operation_id: OperationId,
            _dispatch_id: &IdempotencyKey,
        ) -> Result<SwarmUsage> {
            self.0
                .lock()
                .map(|usage| *usage)
                .map_err(|_| Error::Storage("journal test usage lock poisoned".into()))
        }

        fn record_runtime_usage(
            &self,
            _operation_id: OperationId,
            _dispatch_id: &IdempotencyKey,
            usage: SwarmUsage,
        ) -> Result<()> {
            let mut current = self
                .0
                .lock()
                .map_err(|_| Error::Storage("journal test usage lock poisoned".into()))?;
            *current = SwarmUsage {
                model_steps: current.model_steps.max(usage.model_steps),
                output_bytes: current.output_bytes.max(usage.output_bytes),
                execution_time_ms: current.execution_time_ms.max(usage.execution_time_ms),
            };
            Ok(())
        }
    }

    struct InterruptedRootProvider;

    impl ModelProvider for InterruptedRootProvider {
        fn output_token_limit_for_bytes(&self, max_output_bytes: u64) -> Option<u32> {
            u32::try_from(max_output_bytes).ok().filter(|bound| *bound > 0)
        }

        fn generate<'a>(&'a self, _prepared: PreparedModelInput) -> BoxStream<'a, Result<ModelEvent>> {
            Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "before interruption".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            _attempt: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    struct ExpiredProof;

    impl SwarmOwnerLeaseProof for ExpiredProof {
        fn prove_expired<'a>(
            &'a self,
            _owner: &'a SwarmOwnerFence,
        ) -> futures::future::BoxFuture<'a, Result<bool>> {
            Box::pin(async { Ok(true) })
        }
    }

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

    struct FailingSource {
        reads: Arc<AtomicUsize>,
    }

    impl SwarmUsageSource for FailingSource {
        fn provider_identity(&self) -> &str {
            "journal-failing-provider"
        }

        fn cumulative_usage(
            &self,
            _operation_id: OperationId,
            _dispatch_id: &IdempotencyKey,
        ) -> Result<SwarmUsage> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Err(Error::Storage("provider measurement unavailable".into()))
        }
    }

    async fn prepared_child() -> Result<(
        SwarmBudgetJournal<MemoryStream>,
        SwarmOwnerFence,
        OperationId,
        VerifiedForkPublication,
    )> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let mut journal = SwarmBudgetJournal::start(&client, session_id, owner.clone(), limits())
            .await?;
        let child = OperationId::new();
        journal
            .reserve_child(SwarmForkRequest {
                operation_id: child,
                idempotency_key: IdempotencyKey::new("postdispatch-child")?,
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
            completed_boundary_digest: [11; 32],
            workspace_generation_digest: [12; 32],
        })?;
        Ok((journal, owner, child, publication))
    }

    #[tokio::test]
    async fn postdispatch_measurement_failure_is_classified_after_provider_entry() -> Result<()> {
        let (mut journal, owner, child, publication) = prepared_child().await?;
        let started = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let result = journal
            .dispatch_after_publication_with_usage(
                child,
                owner.clone(),
                IdempotencyKey::new("postdispatch-measurement")?,
                publication,
                FailingSource {
                    reads: reads.clone(),
                },
                {
                    let started = started.clone();
                    move |mut context| async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        context.issue_usage_receipt().map(|_| ())
                    }
                },
            )
            .await;

        assert!(matches!(result, Err(Error::Storage(_))));
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        let usage = journal.usage()?;
        assert_eq!(usage.active_agents, 2);
        assert_eq!(usage.reserved.model_steps, 4);
        Ok(())
    }

    #[tokio::test]
    async fn postdispatch_provider_storage_failure_retains_active_reservation() -> Result<()> {
        let (mut journal, owner, child, publication) = prepared_child().await?;
        let started = Arc::new(AtomicUsize::new(0));
        let result = journal
            .dispatch_after_publication_with_usage(
                child,
                owner,
                IdempotencyKey::new("postdispatch-storage")?,
                publication,
                ZeroSource,
                {
                    let started = started.clone();
                    move |_context| async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(Error::Storage("provider storage failed".into()))
                    }
                },
            )
            .await;

        assert!(matches!(result, Err(Error::Storage(_))));
        assert_eq!(started.load(Ordering::SeqCst), 1);
        let usage = journal.usage()?;
        assert_eq!(usage.active_agents, 2);
        assert_eq!(usage.reserved.model_steps, 4);
        Ok(())
    }

    #[tokio::test]
    async fn postdispatch_provider_denial_after_confirmation_remains_uncertain() -> Result<()> {
        let (mut journal, owner, child, publication) = prepared_child().await?;
        let started = Arc::new(AtomicUsize::new(0));
        let result = journal
            .dispatch_after_publication_with_usage(
                child,
                owner,
                IdempotencyKey::new("postdispatch-denied")?,
                publication,
                ZeroSource,
                {
                    let started = started.clone();
                    move |_context| async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(Error::Conflict("provider denied dispatch".into()))
                    }
                },
            )
            .await;

        // Confirmation is durable before entering the provider boundary. A
        // provider-side denial may race with acceptance, so the reservation
        // remains active until recovery explicitly resolves the dispatch.
        assert!(matches!(result, Err(Error::Indeterminate(_))));
        assert_eq!(started.load(Ordering::SeqCst), 1);
        let usage = journal.usage()?;
        assert_eq!(usage.active_agents, 2);
        assert_eq!(usage.reserved.model_steps, 4);
        assert_eq!(usage.reserved.output_bytes, 64);
        assert_eq!(usage.reserved.execution_time_ms, 100);
        Ok(())
    }

    #[tokio::test]
    async fn owner_recovery_requires_authenticated_expiry_proof() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let dispatch_id = IdempotencyKey::new("root-recovery")?;
        let original = SwarmOwnerFence::new("original", 0)?;
        let replacement = SwarmOwnerFence::new("replacement", 0)?;
        SwarmBudgetJournal::start_with_root_dispatch(
            &client,
            session_id,
            original,
            limits(),
            dispatch_id.clone(),
        )
        .await?;
        assert!(matches!(
            SwarmBudgetJournal::start_with_root_dispatch_recovering(
                &client,
                session_id,
                replacement.clone(),
                limits(),
                dispatch_id.clone(),
            )
            .await,
            Err(Error::Conflict(_))
        ));
        let recovered =
            SwarmBudgetJournal::start_with_root_dispatch_recovering_with_proof(
                &client,
                session_id,
                replacement,
                limits(),
                dispatch_id,
                Arc::new(ExpiredProof),
            )
            .await?;
        assert_eq!(recovered.descriptor()?.1.owner, "replacement");
        assert_eq!(recovered.descriptor()?.1.generation, 1);
        Ok(())
    }

    #[tokio::test]
    async fn child_reservation_wins_before_root_model_claim() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let dispatch_id = IdempotencyKey::new("root-cas")?;
        let limits = SwarmBudgetLimits {
            max_model_steps: 1,
            ..limits()
        };
        let mut root = SwarmBudgetJournal::start_with_root_dispatch(
            &client,
            session_id,
            owner.clone(),
            limits,
            dispatch_id,
        )
        .await?;
        let mut child_handle =
            SwarmBudgetJournal::start_with_root_dispatch_recovering(
                &client,
                session_id,
                owner.clone(),
                limits,
                IdempotencyKey::new("root-cas")?,
            )
            .await?;
        child_handle
            .reserve_child(SwarmForkRequest {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("child-wins")?,
                parent_operation_id: None,
                depth: 1,
                resources: SwarmResourceRequest {
                    model_steps: 1,
                    output_bytes: 1,
                    execution_time_ms: 1,
                },
                admission_digest: None,
            })
            .await?;

        let error = root
            .claim_root_model_step(&owner, OperationId::new(), 0, [1; 32])
            .await
            .expect_err("the child reservation must consume the only root step");
        assert!(matches!(error, Error::Conflict(_)));
        assert_eq!(root.usage()?.reserved.model_steps, 1);
        assert_eq!(root.usage()?.consumed.model_steps, 0);
        Ok(())
    }

    #[tokio::test]
    async fn stale_root_claim_is_fenced_after_durable_takeover() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let original = SwarmOwnerFence::new("original", 0)?;
        let replacement = SwarmOwnerFence::new("replacement", 1)?;
        let dispatch_id = IdempotencyKey::new("takeover-root")?;
        let mut stale = SwarmBudgetJournal::start_with_root_dispatch(
            &client,
            session_id,
            original.clone(),
            limits(),
            dispatch_id.clone(),
        )
        .await?;
        let mut current =
            SwarmBudgetJournal::start_with_root_dispatch_recovering(
                &client,
                session_id,
                original.clone(),
                limits(),
                dispatch_id,
            )
            .await?;
        current.takeover(&original, replacement.owner.clone()).await?;

        let error = stale
            .claim_root_model_step(&original, OperationId::new(), 0, [1; 32])
            .await
            .expect_err("the pre-takeover owner must be fenced");
        assert!(matches!(error, Error::Conflict(_)));
        assert_eq!(stale.refresh().await?, ());
        assert_eq!(stale.descriptor()?.1, replacement);
        Ok(())
    }

    #[tokio::test]
    async fn root_usage_cas_rejects_stale_receipt_after_child_reservation() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let dispatch_id = IdempotencyKey::new("root-usage-cas")?;
        let source = RuntimeSource(Arc::new(Mutex::new(SwarmUsage {
            model_steps: 1,
            output_bytes: 16,
            execution_time_ms: 5,
        })));
        let limits = SwarmBudgetLimits {
            max_model_steps: 2,
            ..limits()
        };
        let mut root = SwarmBudgetJournal::start_with_root_dispatch(
            &client,
            session_id,
            owner.clone(),
            limits,
            dispatch_id.clone(),
        )
        .await?;
        let mut concurrent =
            SwarmBudgetJournal::start_with_root_dispatch_recovering(
                &client,
                session_id,
                owner.clone(),
                limits,
                dispatch_id,
            )
            .await?;
        let mut issuer = root.root_usage_receipt_issuer(source.clone())?;
        let receipt = issuer.issue()?;
        concurrent
            .reserve_child(SwarmForkRequest {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("usage-child")?,
                parent_operation_id: None,
                depth: 1,
                resources: SwarmResourceRequest {
                    model_steps: 1,
                    output_bytes: 10,
                    execution_time_ms: 5,
                },
                admission_digest: None,
            })
            .await?;
        let error = root
            .report_root_usage_with_receipt(&owner, receipt)
            .await
            .expect_err("stale root usage must not overrun a child reservation");
        assert!(matches!(error, Error::Conflict(_)));
        root.refresh().await?;
        let mut issuer = root.root_usage_receipt_issuer(source)?;
        root.report_root_usage_with_receipt(&owner, issuer.issue()?)
            .await?;
        let remaining = root.root_resource_limits()?;
        assert_eq!(remaining.model_steps, 1);
        assert_eq!(remaining.output_bytes, 118);
        assert_eq!(remaining.execution_time_ms, 195);
        Ok(())
    }

    #[tokio::test]
    async fn nonzero_root_usage_survives_sequence_zero_interruption_and_reopen() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let client = StreamClient::new(stream);
        let session_id = OperationId::new();
        let operation_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0)?;
        let dispatch_id = IdempotencyKey::new("root-interrupted")?;
        let limits = SwarmBudgetLimits {
            max_model_steps: 4,
            max_output_bytes: 512,
            max_execution_time_ms: 200,
            ..limits()
        };
        let source = RuntimeSource(Arc::new(Mutex::new(SwarmUsage::default())));
        let mut journal = SwarmBudgetJournal::start_with_root_dispatch(
            &client,
            session_id,
            owner.clone(),
            limits,
            dispatch_id,
        )
        .await?;

        journal
            .claim_root_model_step(&owner, operation_id, 0, [41; 32])
            .await?;
        assert_eq!(journal.root_usage_cursor()?.sequence, 0);
        assert_eq!(journal.root_usage_cursor()?.usage, None);

        // Enter the real metered root provider and interrupt after its first
        // output. The provider has recorded nonzero usage, but no receipt has
        // reached the durable journal yet.
        let context = journal.root_usage_context(source.clone())?;
        let (provider, meter) = crate::swarm_budget::MeteredModelProvider::new_root(
            Arc::new(InterruptedRootProvider),
            context,
        );
        let request = crate::model::ModelRequest {
            model: crate::model::Model::new("mock", "interrupted", "1", serde_json::json!({}))?,
            messages: vec![crate::model::ModelMessage {
                role: crate::model::ModelRole::User,
                content: crate::model::ModelContent::Text("recover".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(64),
        };
        let prepared = PreparedModelInput::prepare(request, crate::conversation::Limits::default())?;
        let mut stream = provider.generate(prepared);
        assert!(matches!(stream.next().await, Some(Ok(ModelEvent::Content { .. }))));
        let measured = meter.usage()?;
        assert!(measured.model_steps >= 1);
        assert!(measured.output_bytes > 0);
        drop(stream);
        drop(provider);
        drop(meter);
        drop(journal);

        // Reopen with the same owner and source as recovery would. The
        // sequence-zero cursor and active claim must remain visible until the
        // host measurement is durably settled.
        let mut reopened = SwarmBudgetJournal::start_with_root_dispatch_recovering(
            &client,
            session_id,
            owner.clone(),
            limits,
            IdempotencyKey::new("root-interrupted")?,
        )
        .await?;
        assert_eq!(reopened.root_usage_cursor()?.sequence, 0);
        let mut issuer = reopened.root_usage_receipt_issuer(source.clone())?;
        let receipt = issuer.issue_at_least(measured)?;
        reopened
            .report_root_usage_with_receipt(&owner, receipt)
            .await?;
        assert_eq!(reopened.root_usage_cursor()?.sequence, 1);
        assert_eq!(reopened.root_usage_cursor()?.usage, Some(measured));
        assert_eq!(reopened.usage()?.consumed, measured);

        // The interrupted claim was released exactly once: a subsequent root
        // step can claim capacity, while the cumulative usage is unchanged.
        reopened
            .claim_root_model_step(&owner, OperationId::new(), 1, [42; 32])
            .await?;
        assert_eq!(reopened.usage()?.consumed, measured);
        Ok(())
    }

    #[tokio::test]
    async fn verified_dispatch_commits_activation_before_releasing_rejected_child() {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let session_id = OperationId::new();
        let owner = SwarmOwnerFence::new("worker", 0).expect("owner");
        let test_limits = SwarmBudgetLimits {
            max_active_agents: 3,
            ..limits()
        };
        let mut journal =
            SwarmBudgetJournal::start(&client, session_id, owner.clone(), test_limits)
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
        // Confirmation is durable before entering the provider boundary. A
        // subsequent rejection is therefore still uncertain and must retain
        // the active reservation until recovery observes a terminal outcome.
        assert_eq!(usage.active_agents, 2);
        assert_eq!(
            usage.reserved,
            SwarmUsage {
                model_steps: 4,
                output_bytes: 64,
                execution_time_ms: 100,
            }
        );

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
        assert_eq!(usage.active_agents, 3);
        assert_eq!(usage.reserved.model_steps, 8);
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
        journal
            .confirm_dispatch(
                child,
                &owner,
                IdempotencyKey::new("receipt-dispatch")?,
            )
            .await?;
        assert!(matches!(
            journal
                .confirm_dispatch(
                    child,
                    &owner,
                    IdempotencyKey::new("receipt-dispatch")?,
                )
                .await,
            Err(Error::Indeterminate(_))
        ));
        let resumed = journal.resume_dispatch_token(child, owner.clone())?;
        assert_eq!(resumed.operation_id(), child);
        assert_eq!(
            resumed.dispatch_id(),
            Some(&IdempotencyKey::new("receipt-dispatch")?)
        );
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
