//! Customer-hostable Stream-backed coordinator and pull-worker admission.

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    runtime::{MAX_CHILD_PAGE, MAX_CHILD_SLOT_BYTES},
    scheduler::{
        AssemblyKind, DurableOwner, EntrypointRef, LeaseFence, OperationSpec, OperationState,
        OrchestrationDecision, Reservation, ResourceSnapshot, Scheduler, SchedulerEvent,
        assembly_invocation_digest, reduction_invocation_digest,
    },
    wire,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::{TryStreamExt as _, future::BoxFuture};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const COORDINATOR_PATH: &str = "harness/v2/coordinator/events";
const COORDINATOR_WIRE_VERSION: &str = "2";
const COORDINATOR_WIRE_CONTRACT: &[u8] = b"acyclic.harness.coordinator.scheduler-event-envelope.v2";
const READ_PAGE_SIZE: u32 = 1_024;
fn validate_child_page_request(
    parent: OperationId,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    if parent.into_bytes() == [0; 16]
        || maximum == 0
        || maximum > MAX_CHILD_PAGE
        || after_slot.is_some_and(|slot| {
            slot.len() > MAX_CHILD_SLOT_BYTES || slot.chars().any(char::is_control)
        })
    {
        return Err(Error::Invalid(
            "child hierarchy page request is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_child_page(
    page: &ChildOperationPage,
    expected_revision: Option<u64>,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    if expected_revision.is_some_and(|revision| revision != page.revision)
        || page.entries.len() > maximum
        || page
            .next_after
            .as_ref()
            .is_some_and(|next| page.entries.last().is_none_or(|last| &last.slot != next))
    {
        return Err(Error::Invalid(
            "child hierarchy page does not match its request".into(),
        ));
    }
    let mut previous = after_slot;
    let mut ids = std::collections::BTreeSet::new();
    for entry in &page.entries {
        if entry.operation_id.into_bytes() == [0; 16]
            || entry.slot.trim().is_empty()
            || entry.slot.len() > MAX_CHILD_SLOT_BYTES
            || entry.slot.chars().any(char::is_control)
            || previous.is_some_and(|slot| entry.slot.as_str() <= slot)
            || !ids.insert(entry.operation_id)
        {
            return Err(Error::Invalid(
                "child hierarchy entries are not in stable slot order".into(),
            ));
        }
        previous = Some(&entry.slot);
    }
    Ok(())
}

/// One direct, same-owner child in stable declared-slot order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChildOperationLink {
    /// Parent-owned slot, independent of execution or completion order.
    pub slot: String,
    /// Exact child operation identity.
    pub operation_id: OperationId,
}

/// Bounded hierarchy observation at one coordinator revision. A changed
/// revision makes continuation fail explicitly instead of skipping children.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChildOperationPage {
    /// Coordinator revision this page was observed from.
    pub revision: u64,
    /// Direct same-owner children, ordered by slot.
    pub entries: Vec<ChildOperationLink>,
    /// Pass this slot with `revision` to continue, if present.
    pub next_after: Option<String>,
}

/// Cursor and bound for one direct-child observation page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildOperationPageRequest<'a> {
    /// Require the coordinator to remain at this revision while paging.
    pub expected_revision: Option<u64>,
    /// Resume strictly after this parent-local child slot.
    pub after_slot: Option<&'a str>,
    /// Maximum direct children to return.
    pub maximum: usize,
}

/// Pull worker capacity and placement identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Worker {
    /// Stable worker identity.
    pub id: String,
    /// Currently available logical capacity.
    pub available: ResourceSnapshot,
    /// Labels available for exact-match operation placement.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// Work atomically claimed from the coordinator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkLease {
    /// Complete immutable operation declaration.
    pub operation: OperationSpec,
    /// Pinned execution allocation.
    pub reservation: Reservation,
    /// Latest durable resumable checkpoint, if any.
    pub checkpoint: Option<crate::resources::CheckpointRef>,
    /// Operation revision after this lease was admitted.
    pub operation_revision: u64,
}

/// Synchronous deterministic implementation of one pinned reducer contract.
pub trait DurableReducer: Send + Sync {
    /// Exact immutable reducer identity.
    fn entrypoint(&self) -> &EntrypointRef;
    /// Reduces values in stable child-slot order.
    fn reduce(
        &self,
        values: &[(String, serde_json::Value)],
    ) -> Result<crate::Outcome<serde_json::Value>>;
}

/// Owner-bound store for staged scheduler aggregate and reducer results.
/// Implementations must reconcile an identical operation/key/bytes retry to
/// the same immutable reference and reject any conflicting retry.
pub trait SchedulerPayloadStore: Send + Sync {
    /// Stages JSON bytes before the coordinator publishes their reference.
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<FileRef>>;
}

/// Code-first registry for exact durable reducer implementations.
#[derive(Default)]
pub struct ReducerRegistry(BTreeMap<(String, String, [u8; 32]), Arc<dyn DurableReducer>>);

impl ReducerRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Registers one exact reducer without ambiguous replacement.
    pub fn register(&mut self, reducer: Arc<dyn DurableReducer>) -> Result<()> {
        let entrypoint = reducer.entrypoint();
        jsonschema::validator_for(&entrypoint.result_schema)
            .map_err(|error| Error::Invalid(format!("invalid reducer result schema: {error}")))?;
        let key = (
            entrypoint.name.clone(),
            entrypoint.version.clone(),
            entrypoint.digest,
        );
        if self.0.contains_key(&key) {
            return Err(Error::Conflict("reducer is already registered".into()));
        }
        self.0.insert(key, reducer);
        Ok(())
    }

    fn get(&self, entrypoint: &EntrypointRef) -> Option<&Arc<dyn DurableReducer>> {
        self.0.get(&(
            entrypoint.name.clone(),
            entrypoint.version.clone(),
            entrypoint.digest,
        ))
    }
}

/// Outcome of a coordinator append.
#[derive(Clone, Debug, PartialEq)]
pub enum CoordinatorApply {
    /// New state was durably appended.
    Applied,
    /// Exact retry was already durably appended.
    Replayed,
}

/// One verified durable coordinator event for an asynchronous projector.
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedSchedulerEvent {
    /// Dense one-based coordinator revision.
    pub revision: u64,
    /// Operation named by the canonical event.
    pub operation_id: OperationId,
    /// Digest of the exact canonical event bytes retained by the coordinator.
    pub event_digest: [u8; 32],
    /// Monotonic UTC time fixed by the coordinator at durable append.
    pub committed_at_ms: u64,
    /// Decoded scheduler transition.
    pub event: SchedulerEvent,
}

/// Reads one bounded page of verified coordinator events without replaying the
/// complete scheduler. A projector can resume from the last durable revision.
///
/// # Errors
///
/// Rejects an invalid bound, missing history after a nonzero cursor, a corrupt
/// envelope, a gap, or a mismatch between the event and its operation identity.
pub async fn read_coordinator_event_page<P: StreamProvider>(
    client: &StreamClient<P>,
    after_revision: u64,
    limit: u32,
) -> Result<Vec<CommittedSchedulerEvent>> {
    if limit == 0 || limit > READ_PAGE_SIZE {
        return Err(Error::Invalid(
            "coordinator page limit is out of bounds".into(),
        ));
    }
    let stream = client
        .stream(COORDINATOR_PATH)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let records = match stream.read(after_revision, limit).await {
        Ok(records) => records,
        Err(StreamError::NotFound) if after_revision == 0 => return Ok(Vec::new()),
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    let page = records
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut expected = after_revision;
    let mut events = Vec::with_capacity(page.len());
    for record in page {
        if record.sequence != expected {
            return Err(Error::Storage(
                "coordinator event page has a cursor gap".into(),
            ));
        }
        let (revision, operation_id, _, event_digest, committed_at_ms, event) =
            decode(&record.value)?;
        if revision != expected.saturating_add(1)
            || scheduler_event_operation(&event) != operation_id
        {
            return Err(Error::Storage(
                "coordinator event page has invalid identity".into(),
            ));
        }
        events.push(CommittedSchedulerEvent {
            revision,
            operation_id,
            event_digest,
            committed_at_ms,
            event,
        });
        expected = revision;
    }
    Ok(events)
}

/// One logical coordinator whose entire semantic history is a Stream.
pub struct DistributedCoordinator<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    scheduler: Scheduler,
    revision: u64,
    last_committed_at_ms: u64,
    intents: BTreeMap<String, ([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId)>,
    content_verifier: Arc<dyn ContentResidencyVerifier>,
    payload_store: Option<Arc<dyn SchedulerPayloadStore>>,
}

#[cfg(feature = "filesystem")]
#[derive(Clone, Copy)]
pub(crate) enum JournalWrite {
    Fresh,
    Model {
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
    },
    Settlement,
}

#[cfg(feature = "filesystem")]
impl JournalWrite {
    pub(crate) fn settlement(self) -> bool {
        matches!(self, Self::Settlement)
    }
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    #[cfg(feature = "filesystem")]
    pub(crate) fn journal_client(&self) -> StreamClient<P> {
        self.client.clone()
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn journal_condition(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        fence: &LeaseFence,
        write: JournalWrite,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.refresh().await?;
        let operation = self.observe_operation(owner, scope, verifier, operation_id)?;
        crate::scheduler::require_execution_owner(&operation, fence, write.settlement())?;
        if let JournalWrite::Model {
            attempt_id,
            step,
            request_digest,
        } = write
        {
            self.scheduler
                .require_model_claim(operation_id, attempt_id, step, &request_digest)?;
        }
        Ok(acyclic_stream::CommitCondition::Tail {
            path: self.stream.path().clone(),
            expected: self.revision,
        })
    }

    /// Opens and replays the public coordinator implementation.
    pub async fn open(
        client: &StreamClient<P>,
        content_verifier: Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        let stream = client.stream(COORDINATOR_PATH)?;
        let mut value = Self {
            client: client.clone(),
            stream,
            scheduler: Scheduler::new(),
            revision: 0,
            last_committed_at_ms: 0,
            intents: BTreeMap::new(),
            content_verifier,
            payload_store: None,
        };
        value.refresh().await?;
        Ok(value)
    }

    /// Replays commits made by other coordinator instances from the exact
    /// observed revision. A long-lived host must refresh before observing a
    /// remote completion or planning another event against its local reducer.
    pub async fn refresh(&mut self) -> Result<()> {
        let mut replay = self.stream.replay(self.revision);
        while let Some(page) = replay.next_page().await? {
            for record in page {
                let (revision, operation_id, key, digest, committed_at_ms, event) =
                    decode(&record.value)?;
                if revision != record.sequence + 1 {
                    return Err(Error::Storage("coordinator revision is not gapless".into()));
                }
                if let SchedulerEvent::Declared { spec } = &event {
                    self.content_verifier.verify(&spec.state).await?;
                }
                self.verify_published_result(&event).await?;
                self.apply_committed(
                    (revision, record.commit_id),
                    operation_id,
                    key,
                    digest,
                    committed_at_ms,
                    event,
                )?;
            }
        }
        Ok(())
    }

    /// Binds the owner-controlled staging boundary for joins, quorums and reducers.
    #[must_use]
    pub fn with_payload_store(mut self, store: Arc<dyn SchedulerPayloadStore>) -> Self {
        self.payload_store = Some(store);
        self
    }

    async fn verify_published_result(&self, event: &SchedulerEvent) -> Result<()> {
        if let SchedulerEvent::WorkflowResumed { input, .. } = event {
            return self.content_verifier.verify(input).await;
        }
        let (operation_id, outcome, reducer) = match event {
            SchedulerEvent::Completed {
                operation_id,
                outcome,
                ..
            } => (*operation_id, outcome, None),
            SchedulerEvent::Orchestrated {
                operation_id,
                outcome,
                reducer,
                ..
            } => (*operation_id, outcome, reducer.as_ref()),
            _ => return Ok(()),
        };
        let crate::Outcome::Succeeded(reference) = outcome else {
            return Ok(());
        };
        let value = self.load_json(reference).await?;
        if let SchedulerEvent::Orchestrated {
            operation_id,
            reducer: None,
            ..
        } = event
            && let OrchestrationDecision::Assemble {
                assembly, values, ..
            } = self.scheduler.orchestration(*operation_id)
            && value != self.assemble_values(assembly, &values).await?
        {
            return Err(Error::Invalid(
                "assembled scheduler result differs from pinned children".into(),
            ));
        }
        let operation = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
        for schema in [
            Some(&operation.spec.entrypoint.result_schema),
            reducer.map(|item| &item.result_schema),
        ]
        .into_iter()
        .flatten()
        {
            jsonschema::validator_for(schema)
                .map_err(|error| {
                    Error::Invalid(format!("invalid scheduler result schema: {error}"))
                })?
                .validate(&value)
                .map_err(|error| {
                    Error::Invalid(format!("scheduler result failed validation: {error}"))
                })?;
        }
        Ok(())
    }

    /// Returns the deterministic scheduler projection.
    #[must_use]
    pub const fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// Returns one operation only after verifying the owner's signed scope.
    pub fn observe_operation(
        &self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
    ) -> Result<OperationState> {
        self.authorize_operation(owner, scope, verifier, operation_id, "operation:observe")
            .cloned()
    }

    /// Discovers direct children without fork lineage or an arbitrary graph
    /// scan. A foreign-owner child is never disclosed by a parent grant.
    pub async fn observe_children(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        parent: OperationId,
        request: ChildOperationPageRequest<'_>,
    ) -> Result<ChildOperationPage> {
        validate_child_page_request(parent, request.after_slot, request.maximum)?;
        self.refresh().await?;
        self.authorize_operation(owner, scope, verifier, parent, "operation:observe")?;
        if request
            .expected_revision
            .is_some_and(|revision| revision != self.revision)
        {
            return Err(Error::Conflict("child hierarchy revision changed".into()));
        }
        let mut entries = Vec::with_capacity(request.maximum);
        let mut has_more = false;
        for (slot, child) in self.scheduler.children(parent) {
            if request.after_slot.is_some_and(|after| slot <= after)
                || child.spec.owner.authority() != owner
            {
                continue;
            }
            if entries.len() == request.maximum {
                has_more = true;
                break;
            }
            entries.push(ChildOperationLink {
                slot: slot.to_owned(),
                operation_id: child.spec.operation_id,
            });
        }
        let next_after = if has_more {
            entries.last().map(|entry| entry.slot.clone())
        } else {
            None
        };
        let page = ChildOperationPage {
            revision: self.revision,
            entries,
            next_after,
        };
        validate_child_page(
            &page,
            request.expected_revision,
            request.after_slot,
            request.maximum,
        )?;
        Ok(page)
    }

    /// Pins immutable session ceilings under the existing root owner's authority.
    pub async fn configure_session(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.refresh().await?;
        self.authorize_operation(owner, scope, verifier, operation_id, "operation:declare")?;
        self.apply_internal(
            operation_id,
            idempotency_key,
            SchedulerEvent::SessionConfigured {
                operation_id,
                limits,
            },
        )
        .await
    }

    /// Durably requests cancellation with exact retry and optional subtree propagation.
    pub async fn cancel_operation(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        recursive: bool,
    ) -> Result<(CoordinatorApply, OperationState)> {
        self.refresh().await?;
        self.authorize_operation(owner, scope, verifier, operation_id, "operation:cancel")?;
        let applied = self
            .apply(
                operation_id,
                idempotency_key,
                SchedulerEvent::CancellationRequested {
                    operation_id,
                    recursive,
                },
            )
            .await?;
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?
            .clone();
        Ok((applied, state))
    }

    fn authorize_operation<'a>(
        &'a self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        capability: &str,
    ) -> Result<&'a OperationState> {
        verifier.verify_audience(owner)?;
        verifier.verify(scope)?;
        if !scope.capabilities().contains(capability) {
            return Err(Error::Unauthorized(format!(
                "scope {} lacks capability {capability}",
                scope.id()
            )));
        }
        let operation = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
        let declared_owner = match &operation.spec.owner {
            DurableOwner::Attached { authority } | DurableOwner::Detached { authority } => {
                authority
            }
        };
        if declared_owner != owner {
            return Err(Error::NotFound(format!("operation {operation_id}")));
        }
        Ok(operation)
    }

    /// Durably applies one scheduler event with exact retry semantics.
    pub async fn apply(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
    ) -> Result<CoordinatorApply> {
        if matches!(&event, SchedulerEvent::Declared { .. }) {
            return Err(Error::Unauthorized(
                "declarations require owner-scoped admission".into(),
            ));
        }
        if matches!(&event, SchedulerEvent::SessionConfigured { .. }) {
            return Err(Error::Unauthorized(
                "session configuration requires owner authority".into(),
            ));
        }
        if matches!(&event, SchedulerEvent::WorkflowResumed { .. }) {
            return Err(Error::Unauthorized(
                "workflow wake requires owner authority".into(),
            ));
        }
        if matches!(&event, SchedulerEvent::Orchestrated { .. }) {
            return Err(Error::Unauthorized(
                "orchestration decisions require coordinator-owned materialization".into(),
            ));
        }
        self.apply_internal(operation_id, idempotency_key, event)
            .await
    }

    /// Authenticates the exact owner before filling one durable resume slot.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit signed owner and exact suspension identity"
    )]
    pub async fn resume_workflow(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.resume_workflow_guarded(
            owner,
            scope,
            verifier,
            operation_id,
            workflow_revision,
            input,
            idempotency_key,
            None,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact owner, suspension and selected passive command"
    )]
    #[cfg(any(feature = "filesystem", test))]
    pub(crate) async fn resume_command(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
        waiting_command: OperationId,
    ) -> Result<CoordinatorApply> {
        self.resume_workflow_guarded(
            owner,
            scope,
            verifier,
            operation_id,
            workflow_revision,
            input,
            idempotency_key,
            Some(waiting_command),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one authenticated wake publication path"
    )]
    async fn resume_workflow_guarded(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
        waiting_command: Option<OperationId>,
    ) -> Result<CoordinatorApply> {
        verifier.verify_audience(owner)?;
        verifier.verify(scope)?;
        if !scope.capabilities().contains("operation:wake") {
            return Err(Error::Unauthorized(
                "owner scope lacks operation:wake".into(),
            ));
        }
        self.refresh().await?;
        self.observe_operation(owner, scope, verifier, operation_id)?;
        self.apply_internal_guarded(
            operation_id,
            idempotency_key,
            SchedulerEvent::WorkflowResumed {
                operation_id,
                workflow_revision,
                input,
            },
            None,
            waiting_command,
        )
        .await
    }

    /// Authenticates the exact durable owner before publishing a declaration.
    pub async fn declare_operation(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.declare_operation_with_parent(owner, scope, verifier, spec, idempotency_key, None)
            .await
    }

    /// Publishes a child only while its parent retains this uncancelled lease.
    #[allow(
        clippy::too_many_arguments,
        reason = "signed owner and exact parent lease"
    )]
    pub async fn declare_operation_owned(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        idempotency_key: IdempotencyKey,
        parent_fence: LeaseFence,
    ) -> Result<CoordinatorApply> {
        if spec.parent.is_none() {
            return Err(Error::Invalid(
                "owned child declaration has no parent".into(),
            ));
        }
        self.declare_operation_with_parent(
            owner,
            scope,
            verifier,
            spec,
            idempotency_key,
            Some(parent_fence),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one authenticated declaration path"
    )]
    async fn declare_operation_with_parent(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        idempotency_key: IdempotencyKey,
        parent_fence: Option<LeaseFence>,
    ) -> Result<CoordinatorApply> {
        verifier.verify_audience(owner)?;
        verifier.verify(scope)?;
        if !scope.capabilities().contains("operation:declare") {
            return Err(Error::Unauthorized("scope lacks operation:declare".into()));
        }
        let declared_owner = match &spec.owner {
            DurableOwner::Attached { authority } | DurableOwner::Detached { authority } => {
                authority
            }
        };
        if declared_owner != owner {
            return Err(Error::Unauthorized(
                "declaration owner does not match authenticated owner".into(),
            ));
        }
        if let Some(parent) = &spec.parent {
            self.refresh().await?;
            self.authorize_operation(
                owner,
                scope,
                verifier,
                parent.operation_id,
                "operation:declare",
            )?;
        }
        let operation_id = spec.operation_id;
        let event = SchedulerEvent::Declared {
            spec: Box::new(spec),
        };
        self.apply_internal_guarded(operation_id, idempotency_key, event, parent_fence, None)
            .await
    }

    /// Publishes a root and its immutable shared ceilings in one atomic append.
    /// No worker can observe the root before its session budget is configured.
    #[allow(
        clippy::too_many_lines,
        reason = "validates and reconciles one atomic two-event publication"
    )]
    pub async fn declare_session(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        verifier.verify_audience(owner)?;
        verifier.verify(scope)?;
        if !scope.capabilities().contains("operation:declare") || spec.owner.authority() != owner {
            return Err(Error::Unauthorized(
                "session declaration owner is invalid".into(),
            ));
        }
        if spec.parent.is_some() {
            return Err(Error::Invalid("session declaration must be a root".into()));
        }
        limits.validate()?;
        IdempotencyKey::new(idempotency_key.0.clone())?;
        let operation_id = spec.operation_id;
        let keys = [
            idempotency_key.0.clone(),
            format!(
                "session:{}:{}",
                operation_id,
                blake3::hash(idempotency_key.as_str().as_bytes()).to_hex()
            ),
        ];
        let events = [
            SchedulerEvent::Declared {
                spec: Box::new(spec),
            },
            SchedulerEvent::SessionConfigured {
                operation_id,
                limits,
            },
        ];
        self.refresh().await?;
        if self.session_intent(&keys, &events)? {
            return Ok(CoordinatorApply::Replayed);
        }
        let mut projected = self.scheduler.clone();
        let mut records = Vec::with_capacity(2);
        let mut revision = self.revision;
        let mut committed_at_ms = self.next_committed_at_ms()?;
        for (key, event) in keys.iter().zip(&events) {
            if let SchedulerEvent::Declared { spec } = event {
                self.content_verifier.verify(&spec.state).await?;
            }
            projected.apply(event.clone())?;
            revision = revision
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("coordinator revision exhausted".into()))?;
            let canonical = crate::contract::canonical_json_bytes(event)?;
            let digest = *blake3::hash(&canonical).as_bytes();
            records.push(Bytes::from(encode(
                revision,
                operation_id,
                key,
                digest,
                canonical,
                committed_at_ms,
            )));
            committed_at_ms = committed_at_ms
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("coordinator commit time exhausted".into()))?;
        }
        let stream_key = stream_key(idempotency_key.as_str())?;
        let outcome = match self
            .stream
            .append_batch(records, Some(self.revision), Some(stream_key.clone()))
            .await
        {
            Ok(outcome) => outcome,
            Err(StreamError::Unavailable) => {
                match self.client.inspect_idempotency(stream_key).await {
                    Ok(Some(observation)) => match observation.outcome {
                        IdempotencyOutcome::Append(outcome) => outcome,
                        _ => {
                            return Err(Error::Conflict(
                                "session retry has another operation kind".into(),
                            ));
                        }
                    },
                    Ok(None) | Err(_) => return Err(Error::Indeterminate(operation_id)),
                }
            }
            Err(StreamError::IdempotencyMismatch) => {
                self.refresh().await?;
                if self.session_intent(&keys, &events)? {
                    return Ok(CoordinatorApply::Replayed);
                }
                return Err(Error::Conflict("session retry identity reused".into()));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        self.finish_session_append(outcome, &keys, &events, revision - 2)
            .await
    }

    fn session_intent(&self, keys: &[String; 2], events: &[SchedulerEvent; 2]) -> Result<bool> {
        let mut retained = 0;
        let mut previous: Option<(u64, acyclic_stream::CommitId)> = None;
        for (key, event) in keys.iter().zip(events) {
            if let Some((digest, existing, revision, commit_id)) = self.intents.get(key) {
                if existing != event
                    || digest
                        != blake3::hash(&crate::contract::canonical_json_bytes(event)?).as_bytes()
                {
                    return Err(Error::Conflict("session retry identity reused".into()));
                }
                if let Some((prior, envelope)) = previous
                    && (prior.checked_add(1) != Some(*revision) || envelope != *commit_id)
                {
                    return Err(Error::Conflict(
                        "session declaration is not one atomic envelope".into(),
                    ));
                }
                previous = Some((*revision, *commit_id));
                retained += 1;
            }
        }
        match retained {
            0 => Ok(false),
            2 => Ok(true),
            _ => Err(Error::Conflict("session declaration is not atomic".into())),
        }
    }

    async fn finish_session_append(
        &mut self,
        outcome: AppendOutcome,
        keys: &[String; 2],
        events: &[SchedulerEvent; 2],
        expected_start: u64,
    ) -> Result<CoordinatorApply> {
        let (result, receipt) = match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(2) || receipt.tail < receipt.end {
                    return Err(Error::Storage("invalid session append receipt".into()));
                }
                let result = if receipt.start == expected_start {
                    CoordinatorApply::Applied
                } else {
                    CoordinatorApply::Replayed
                };
                (result, Some((receipt.commit_id, receipt.start)))
            }
            AppendOutcome::TailConflict { .. } => (CoordinatorApply::Replayed, None),
        };
        self.refresh().await?;
        if !self.session_intent(keys, events)? {
            return Err(Error::Conflict(
                "session append conflicted without its complete intent".into(),
            ));
        }
        if let Some((envelope, start)) = receipt
            && keys
                .first()
                .and_then(|key| self.intents.get(key))
                .is_none_or(|(_, _, revision, commit_id)| {
                    *commit_id != envelope || revision.checked_sub(1) != Some(start)
                })
        {
            return Err(Error::Storage(
                "session append receipt differs from its committed envelope".into(),
            ));
        }
        Ok(result)
    }

    fn replay_intent(&self, event: &SchedulerEvent) -> Result<CoordinatorApply> {
        if let SchedulerEvent::ModelDispatchClaimed {
            operation_id,
            fence,
            ..
        } = event
        {
            let operation = self
                .scheduler
                .operation(*operation_id)
                .ok_or_else(|| Error::NotFound("model task".into()))?;
            crate::scheduler::require_model_owner(operation, fence)?;
        }
        Ok(CoordinatorApply::Replayed)
    }

    async fn apply_internal(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
    ) -> Result<CoordinatorApply> {
        self.apply_internal_guarded(operation_id, idempotency_key, event, None, None)
            .await
    }

    async fn apply_internal_guarded(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
        parent_fence: Option<LeaseFence>,
        waiting_command: Option<OperationId>,
    ) -> Result<CoordinatorApply> {
        self.refresh().await?;
        self.require_parent_owner(&event, parent_fence.as_ref())?;
        self.require_command_wait(&event, waiting_command)?;
        IdempotencyKey::new(idempotency_key.0.clone())?;
        let key = idempotency_key.as_str();
        if scheduler_event_operation(&event) != operation_id {
            return Err(Error::Invalid(
                "scheduler event belongs to another operation".into(),
            ));
        }
        let canonical = crate::contract::canonical_json_bytes(&event)?;
        let digest = *blake3::hash(&canonical).as_bytes();
        if let Some((existing_digest, existing, ..)) = self.intents.get(key) {
            return if existing_digest == &digest && existing == &event {
                // A retained accounting receipt is not an execution permit.
                // Revalidate the exact owner and cancellation state on retry.
                self.replay_intent(&event)
            } else {
                Err(Error::Conflict("coordinator retry identity reused".into()))
            };
        }
        if let SchedulerEvent::Declared { spec } = &event {
            self.content_verifier.verify(&spec.state).await?;
        }
        self.verify_published_result(&event).await?;
        let mut projected = self.scheduler.clone();
        projected.apply(event.clone())?;
        let revision = self.next_revision()?;
        let committed_at_ms = self.next_committed_at_ms()?;
        let bytes = encode(
            revision,
            operation_id,
            key,
            digest,
            canonical,
            committed_at_ms,
        );
        // Stream retains failed CAS requests too. A refreshed coordinator tail
        // needs a new physical retry identity; the encoded logical intent and
        // declaration/wake intent still prevents another logical publication.
        let stream_key = if parent_fence.is_some() {
            stream_key(&format!("{key}:owned-parent:{}", self.revision))?
        } else if waiting_command.is_some() {
            stream_key(&format!("{key}:owned-wait:{}", self.revision))?
        } else {
            stream_key(key)?
        };
        let outcome = match self
            .stream
            .append_batch(
                vec![Bytes::copy_from_slice(&bytes)],
                Some(self.revision),
                Some(stream_key.clone()),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(StreamError::Unavailable) => {
                match self.client.inspect_idempotency(stream_key).await {
                    Ok(Some(observation)) => match observation.outcome {
                        IdempotencyOutcome::Append(outcome) => outcome,
                        _ => {
                            return Err(Error::Conflict(
                                "coordinator retry identity has another operation kind".into(),
                            ));
                        }
                    },
                    Ok(None) | Err(_) => return Err(Error::Indeterminate(operation_id)),
                }
            }
            Err(StreamError::IdempotencyMismatch) => {
                return Err(Error::Conflict("coordinator retry identity reused".into()));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let applied = self
            .finish_append(outcome, &event, key, &digest, revision)
            .await?;
        self.require_parent_owner(&event, parent_fence.as_ref())?;
        Ok(applied)
    }

    fn require_command_wait(
        &self,
        event: &SchedulerEvent,
        waiting: Option<OperationId>,
    ) -> Result<()> {
        let Some(waiting) = waiting else {
            return Ok(());
        };
        let SchedulerEvent::WorkflowResumed {
            operation_id,
            workflow_revision,
            ..
        } = event
        else {
            return Err(Error::Invalid(
                "command wait guard only applies to wake".into(),
            ));
        };
        let operation = self
            .scheduler
            .operation(*operation_id)
            .ok_or_else(|| Error::NotFound("waiting task".into()))?;
        if operation.phase != crate::scheduler::OperationPhase::Suspended
            || operation.cancellation_requested
            || operation.workflow.as_ref().is_none_or(|slot| {
                slot.revision != *workflow_revision
                    || slot.waiting_command != Some(waiting)
                    || slot.input.is_some()
            })
        {
            return Err(Error::Conflict(
                "selected command wait is no longer current".into(),
            ));
        }
        Ok(())
    }

    fn require_parent_owner(
        &self,
        event: &SchedulerEvent,
        fence: Option<&LeaseFence>,
    ) -> Result<()> {
        let Some(fence) = fence else {
            return Ok(());
        };
        let SchedulerEvent::Declared { spec } = event else {
            return Err(Error::Invalid(
                "parent fence only applies to declaration".into(),
            ));
        };
        let parent = spec
            .parent
            .as_ref()
            .ok_or_else(|| Error::Invalid("child declaration has no parent".into()))?;
        let operation = self
            .scheduler
            .operation(parent.operation_id)
            .ok_or_else(|| Error::NotFound("admitting parent".into()))?;
        crate::scheduler::require_execution_owner(operation, fence, false)
    }

    async fn finish_append(
        &mut self,
        outcome: AppendOutcome,
        event: &SchedulerEvent,
        key: &str,
        digest: &[u8; 32],
        revision: u64,
    ) -> Result<CoordinatorApply> {
        match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(1) || receipt.tail < receipt.end {
                    return Err(Error::Storage("invalid coordinator append receipt".into()));
                }
                self.refresh().await?;
                match self.intents.get(key) {
                    Some((committed_digest, committed, ..))
                        if committed_digest == digest && committed == event =>
                    {
                        self.replay_intent(event)?;
                        Ok(if receipt.start == revision - 1 {
                            CoordinatorApply::Applied
                        } else {
                            CoordinatorApply::Replayed
                        })
                    }
                    _ => Err(Error::Conflict(
                        "coordinator committed identity differs from its intent".into(),
                    )),
                }
            }
            AppendOutcome::TailConflict { actual_tail } => {
                self.refresh().await?;
                match self.intents.get(key) {
                    Some((committed_digest, committed, ..))
                        if committed_digest == digest && committed == event =>
                    {
                        self.replay_intent(event)
                    }
                    Some(_) => Err(Error::Conflict("coordinator retry identity reused".into())),
                    None => Err(Error::Conflict(format!(
                        "coordinator tail is {actual_tail}"
                    ))),
                }
            }
        }
    }

    fn next_revision(&self) -> Result<u64> {
        self.revision
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("coordinator revision exhausted".into()))
    }

    fn next_committed_at_ms(&self) -> Result<u64> {
        let after_previous = self
            .last_committed_at_ms
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("coordinator commit time exhausted".into()))?;
        Ok(current_time_millis()?.max(after_previous))
    }

    /// Pulls and atomically admits one dependency- and resource-ready operation.
    pub async fn pull(&mut self, worker: &Worker) -> Result<Option<WorkLease>> {
        self.refresh().await?;
        if worker.id.trim().is_empty() {
            return Err(Error::Invalid("worker identity is empty".into()));
        }
        if let Some(operation_id) = self.scheduler.blocked_by_dependencies().first().copied() {
            self.apply(
                operation_id,
                IdempotencyKey::new(format!("dependency-rejected:{operation_id}"))?,
                SchedulerEvent::Rejected {
                    operation_id,
                    reason: "a required dependency did not succeed".into(),
                },
            )
            .await?;
            return Ok(None);
        }
        let available = self.scheduler.available_for(&worker.id, &worker.available);
        let Some(operation_id) = self
            .scheduler
            .ready_for(&available, &worker.labels)
            .first()
            .copied()
        else {
            return Ok(None);
        };
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?
            .clone();
        let operation = state.spec.clone();
        let reservation = Reservation {
            id: format!("{}:{operation_id}:{}", worker.id, self.revision + 1),
            placement: worker.id.clone(),
            admitted: operation.resources.clone(),
        };
        self.apply(
            operation_id,
            IdempotencyKey::new(format!(
                "pull:{}:{operation_id}:{}",
                worker.id,
                self.revision + 1
            ))?,
            SchedulerEvent::Admitted {
                operation_id,
                reservation: reservation.clone(),
            },
        )
        .await?;
        Ok(Some(WorkLease {
            operation,
            reservation,
            checkpoint: state.checkpoint,
            operation_revision: self
                .scheduler
                .operation(operation_id)
                .map_or(0, |value| value.revision),
        }))
    }

    /// Returns a crashed worker's exact lease to admission without losing its checkpoint.
    pub async fn release_lease(
        &mut self,
        lease: &WorkLease,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.apply(
            lease.operation.operation_id,
            idempotency_key,
            SchedulerEvent::LeaseReleased {
                operation_id: lease.operation.operation_id,
                fence: LeaseFence::from(&lease.reservation),
            },
        )
        .await
    }

    /// Atomically commits a ready structured-concurrency decision and loser cancellation.
    pub async fn orchestrate(
        &mut self,
        parent: OperationId,
        idempotency_key: IdempotencyKey,
    ) -> Result<bool> {
        self.refresh().await?;
        if self.replay_orchestration(parent, idempotency_key.as_str(), false)? {
            return Ok(true);
        }
        let expected_revision = self
            .scheduler
            .operation(parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?
            .revision;
        let (outcome, cancel, reduction_digest) = match self.scheduler.orchestration(parent) {
            OrchestrationDecision::Complete { outcome, cancel } => (outcome, cancel, None),
            OrchestrationDecision::Assemble {
                assembly,
                values,
                cancel,
            } => {
                let digest = assembly_invocation_digest(assembly, &values)?;
                let result = self
                    .stage_json(
                        parent,
                        idempotency_key.as_str(),
                        &self.assemble_values(assembly, &values).await?,
                    )
                    .await?;
                (crate::Outcome::Succeeded(result), cancel, Some(digest))
            }
            OrchestrationDecision::Wait | OrchestrationDecision::Reduce { .. } => return Ok(false),
        };
        self.apply_internal(
            parent,
            idempotency_key,
            SchedulerEvent::Orchestrated {
                operation_id: parent,
                expected_revision,
                outcome,
                cancel,
                reducer: None,
                reduction_digest,
            },
        )
        .await?;
        Ok(true)
    }

    /// Commits the result of the exact versioned reducer invocation currently planned.
    pub async fn complete_reduction(
        &mut self,
        reducers: &ReducerRegistry,
        parent: OperationId,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.refresh().await?;
        if self.replay_orchestration(parent, idempotency_key.as_str(), true)? {
            return Ok(CoordinatorApply::Replayed);
        }
        let state = self
            .scheduler
            .operation(parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?;
        let expected_revision = state.revision;
        let OrchestrationDecision::Reduce { reducer, values } =
            self.scheduler.orchestration(parent)
        else {
            return Err(Error::Conflict("no reducer invocation is ready".into()));
        };
        let implementation = reducers.get(&reducer).ok_or_else(|| {
            Error::Unsupported("exact reducer implementation is not registered".into())
        })?;
        let mut decoded = Vec::with_capacity(values.len());
        for (slot, reference) in &values {
            decoded.push((slot.clone(), self.load_json(reference).await?));
        }
        let outcome = implementation.reduce(&decoded)?;
        if matches!(outcome, crate::Outcome::Indeterminate { .. }) {
            return Err(Error::Invalid(
                "a synchronous reducer cannot return an indeterminate outcome".into(),
            ));
        }
        let outcome = match outcome {
            crate::Outcome::Succeeded(value) => crate::Outcome::Succeeded(
                self.stage_json(parent, idempotency_key.as_str(), &value)
                    .await?,
            ),
            crate::Outcome::Failed { message } => crate::Outcome::Failed { message },
            crate::Outcome::Cancelled => crate::Outcome::Cancelled,
            crate::Outcome::Indeterminate { .. } => unreachable!("indeterminate was rejected"),
        };
        let reduction_digest = reduction_invocation_digest(&reducer, &values)?;
        self.apply_internal(
            parent,
            idempotency_key,
            SchedulerEvent::Orchestrated {
                operation_id: parent,
                expected_revision,
                outcome,
                cancel: Vec::new(),
                reducer: Some(reducer),
                reduction_digest: Some(reduction_digest),
            },
        )
        .await
    }

    async fn load_json(&self, reference: &FileRef) -> Result<serde_json::Value> {
        reference.validate()?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid(
                "scheduler result must be a JSON file".into(),
            ));
        }
        let bytes = self.content_verifier.read(reference).await?;
        reference.descriptor().verify(&bytes)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
            Error::Invalid(format!("scheduler result is invalid JSON: {error}"))
        })?;
        if crate::contract::canonical_json_bytes(&value)? != bytes {
            return Err(Error::Invalid(
                "scheduler result is not canonical JSON".into(),
            ));
        }
        Ok(value)
    }

    async fn assemble_values(
        &self,
        assembly: AssemblyKind,
        values: &[(String, FileRef)],
    ) -> Result<serde_json::Value> {
        let mut assembled = Vec::with_capacity(values.len());
        for (slot, reference) in values {
            let value = self.load_json(reference).await?;
            assembled.push(match assembly {
                AssemblyKind::Join => serde_json::json!({"slot": slot, "value": value}),
                AssemblyKind::Quorum => value,
            });
        }
        Ok(serde_json::Value::Array(assembled))
    }

    fn replay_orchestration(&self, parent: OperationId, key: &str, reduced: bool) -> Result<bool> {
        let Some((_, event, ..)) = self.intents.get(key) else {
            return Ok(false);
        };
        if let SchedulerEvent::Orchestrated {
            operation_id,
            reducer,
            ..
        } = event
            && *operation_id == parent
            && reducer.is_some() == reduced
        {
            Ok(true)
        } else {
            Err(Error::Conflict("coordinator retry identity reused".into()))
        }
    }

    async fn stage_json(
        &self,
        operation_id: OperationId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<FileRef> {
        let store = self
            .payload_store
            .as_ref()
            .ok_or_else(|| Error::Unsupported("scheduler payload store is not bound".into()))?;
        let bytes = crate::contract::canonical_json_bytes(value)?;
        let reference = store.stage(operation_id, key, &bytes).await?;
        reference.descriptor().verify(&bytes)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid(
                "scheduler payload store returned a non-JSON file".into(),
            ));
        }
        Ok(reference)
    }

    fn apply_committed(
        &mut self,
        position: (u64, acyclic_stream::CommitId),
        operation_id: OperationId,
        key: String,
        digest: [u8; 32],
        committed_at_ms: u64,
        event: SchedulerEvent,
    ) -> Result<()> {
        let (revision, commit_id) = position;
        if revision != self.revision + 1 || scheduler_event_operation(&event) != operation_id {
            return Err(Error::Conflict(
                "invalid committed coordinator event".into(),
            ));
        }
        if committed_at_ms <= self.last_committed_at_ms {
            return Err(Error::Storage(
                "coordinator commit time is not increasing".into(),
            ));
        }
        IdempotencyKey::new(key.clone())?;
        if self.intents.contains_key(&key) {
            return Err(Error::Conflict(
                "coordinator history repeats an idempotency key".into(),
            ));
        }
        self.scheduler.apply(event.clone())?;
        self.revision = revision;
        self.last_committed_at_ms = committed_at_ms;
        self.intents
            .insert(key, (digest, event, revision, commit_id));
        Ok(())
    }
}

fn scheduler_event_operation(event: &SchedulerEvent) -> OperationId {
    match event {
        SchedulerEvent::Declared { spec } => spec.operation_id,
        SchedulerEvent::SessionConfigured { operation_id, .. }
        | SchedulerEvent::ModelDispatchClaimed { operation_id, .. }
        | SchedulerEvent::WaitingForCapacity { operation_id }
        | SchedulerEvent::Admitted { operation_id, .. }
        | SchedulerEvent::PartiallyAdmitted { operation_id, .. }
        | SchedulerEvent::Rejected { operation_id, .. }
        | SchedulerEvent::Started { operation_id, .. }
        | SchedulerEvent::Checkpointed { operation_id, .. }
        | SchedulerEvent::WaitingForChildren { operation_id, .. }
        | SchedulerEvent::WorkflowSuspended { operation_id, .. }
        | SchedulerEvent::WorkflowResumed { operation_id, .. }
        | SchedulerEvent::ReconciliationResumed { operation_id, .. }
        | SchedulerEvent::LeaseReleased { operation_id, .. }
        | SchedulerEvent::CancellationRequested { operation_id, .. }
        | SchedulerEvent::Completed { operation_id, .. }
        | SchedulerEvent::Orchestrated { operation_id, .. } => *operation_id,
    }
}

fn encode(
    revision: u64,
    operation_id: OperationId,
    key: &str,
    digest: [u8; 32],
    canonical: Vec<u8>,
    committed_at_ms: u64,
) -> Vec<u8> {
    wire::SchedulerEventEnvelope {
        protocol: Some(coordinator_protocol_identity()),
        revision,
        operation_id: operation_id.to_string(),
        idempotency_key: key.into(),
        canonical_event_json: canonical,
        event_digest: digest.to_vec(),
        committed_at_ms: Some(committed_at_ms),
    }
    .encode_to_vec()
}

fn decode(bytes: &[u8]) -> Result<(u64, OperationId, String, [u8; 32], u64, SchedulerEvent)> {
    let envelope = wire::SchedulerEventEnvelope::decode(bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    validate_coordinator_protocol(envelope.protocol.as_ref())?;
    let digest: [u8; 32] = envelope
        .event_digest
        .try_into()
        .map_err(|_| Error::Storage("scheduler digest must be 32 bytes".into()))?;
    if *blake3::hash(&envelope.canonical_event_json).as_bytes() != digest {
        return Err(Error::Storage("scheduler event digest mismatch".into()));
    }
    let event = serde_json::from_slice(&envelope.canonical_event_json)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let committed_at_ms = envelope
        .committed_at_ms
        .filter(|time| *time > 0)
        .ok_or_else(|| Error::Storage("coordinator event lacks commit time".into()))?;
    Ok((
        envelope.revision,
        OperationId::parse(&envelope.operation_id)?,
        envelope.idempotency_key,
        digest,
        committed_at_ms,
        event,
    ))
}

fn current_time_millis() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Invalid("coordinator clock predates Unix epoch".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| Error::Invalid("coordinator clock exceeds supported range".into()))
}

fn coordinator_protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: COORDINATOR_WIRE_VERSION.into(),
        descriptor_digest: blake3::hash(COORDINATOR_WIRE_CONTRACT).to_hex().to_string(),
    }
}

fn validate_coordinator_protocol(protocol: Option<&wire::ProtocolIdentity>) -> Result<()> {
    let actual =
        protocol.ok_or_else(|| Error::Storage("coordinator event protocol is missing".into()))?;
    let current = coordinator_protocol_identity();
    if actual == &current {
        Ok(())
    } else {
        Err(Error::Unsupported(
            "unsupported coordinator event wire version".into(),
        ))
    }
}

fn stream_key(key: &str) -> Result<StreamIdempotencyKey> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-coordinator-v2");
    hasher.update(key.as_bytes());
    StreamIdempotencyKey::new(Bytes::copy_from_slice(hasher.finalize().as_bytes()))
        .map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Capabilities,
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        core::{AggregateKind, Authority, AuthorityIssuer},
        resources::ProviderRef,
        scheduler::{
            DurableOwner, EntrypointRef, OperationPhase, Orchestration, ParentLink, ResourceRequest,
        },
    };
    use acyclic_stream::MemoryStream;
    use serde_json::Value;
    use std::{collections::BTreeSet, sync::Arc};

    struct TestReducer {
        entrypoint: EntrypointRef,
        value: Value,
    }

    impl DurableReducer for TestReducer {
        fn entrypoint(&self) -> &EntrypointRef {
            &self.entrypoint
        }

        fn reduce(&self, _: &[(String, Value)]) -> Result<crate::Outcome<Value>> {
            Ok(crate::Outcome::Succeeded(self.value.clone()))
        }
    }

    fn state_ref() -> Result<FileRef> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "project",
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
        )?;
        FileRef::new(
            volume,
            "state/initial.json",
            "generation-1",
            FileDescriptor::from_bytes(b"null", "application/json")?,
            "initial.json",
        )
    }

    struct TestContentVerifier;

    impl ContentResidencyVerifier for TestContentVerifier {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { reference.descriptor().verify(b"null") })
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send + 'a>>
        {
            Box::pin(async move {
                reference.descriptor().verify(b"null")?;
                Ok(b"null".to_vec())
            })
        }
    }

    struct PausedChildVerifier {
        armed: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl ContentResidencyVerifier for PausedChildVerifier {
        fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                TestContentVerifier.verify(reference).await
            })
        }
    }

    #[tokio::test]
    async fn owned_child_declaration_rechecks_parent_and_recovers_tail_conflict() -> Result<()> {
        for cancel in [false, true] {
            let client = StreamClient::new(Arc::new(MemoryStream::default()));
            let gate = Arc::new(PausedChildVerifier {
                armed: std::sync::atomic::AtomicBool::new(false),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let mut first = DistributedCoordinator::open(&client, gate.clone()).await?;
            let parent = OperationId::from_bytes([41; 16]);
            declare(&mut first, spec(parent, 0)?, "fenced-parent").await?;
            let lease = first
                .pull(&Worker {
                    id: "parent-worker".into(),
                    available: ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("parent lease".into()))?;
            let fence = LeaseFence::from(&lease.reservation);
            first
                .apply(
                    parent,
                    IdempotencyKey::new("parent-start")?,
                    SchedulerEvent::Started {
                        operation_id: parent,
                        fence: fence.clone(),
                    },
                )
                .await?;
            let mut second =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            };
            let issuer = AuthorityIssuer::new("owned-child-test", [9; 32], owner.clone());
            let signed = issuer.root(
                "declare",
                Capabilities::new(["operation:declare", "operation:observe"]),
            );
            let verifier = issuer.verifier();
            let child_id = OperationId::from_bytes([42; 16]);
            let mut child = spec(child_id, 0)?;
            child.parent = Some(ParentLink {
                operation_id: parent,
                slot: "child".into(),
            });
            gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
            let disturbance = async {
                gate.entered.notified().await;
                let result = if cancel {
                    second
                        .apply(
                            parent,
                            IdempotencyKey::new("parent-cancel")?,
                            SchedulerEvent::CancellationRequested {
                                operation_id: parent,
                                recursive: false,
                            },
                        )
                        .await
                } else {
                    declare(
                        &mut second,
                        spec(OperationId::from_bytes([43; 16]), 0)?,
                        "unrelated-tail",
                    )
                    .await
                };
                gate.release.notify_one();
                result
            };
            let (attempt, disturbance) = tokio::join!(
                first.declare_operation_owned(
                    &owner,
                    &signed,
                    &verifier,
                    child.clone(),
                    IdempotencyKey::new("owned-child")?,
                    fence.clone()
                ),
                disturbance
            );
            disturbance?;
            assert!(matches!(attempt, Err(Error::Conflict(_))));
            assert!(first.scheduler().operation(child_id).is_none());
            let retry = first
                .declare_operation_owned(
                    &owner,
                    &signed,
                    &verifier,
                    child.clone(),
                    IdempotencyKey::new("owned-child")?,
                    fence.clone(),
                )
                .await;
            if cancel {
                assert!(retry.is_err());
                assert!(first.scheduler().operation(child_id).is_none());
            } else {
                assert_eq!(retry?, CoordinatorApply::Applied);
                assert_eq!(
                    first
                        .declare_operation_owned(
                            &owner,
                            &signed,
                            &verifier,
                            child.clone(),
                            IdempotencyKey::new("owned-child")?,
                            fence.clone()
                        )
                        .await?,
                    CoordinatorApply::Replayed
                );
                assert_eq!(first.scheduler().children(parent).count(), 1);
                first
                    .apply(
                        parent,
                        IdempotencyKey::new("parent-cancel-after")?,
                        SchedulerEvent::CancellationRequested {
                            operation_id: parent,
                            recursive: false,
                        },
                    )
                    .await?;
                assert!(
                    first
                        .declare_operation_owned(
                            &owner,
                            &signed,
                            &verifier,
                            child,
                            IdempotencyKey::new("owned-child")?,
                            fence
                        )
                        .await
                        .is_err()
                );
            }
        }
        Ok(())
    }

    struct DenyContentVerifier;

    #[tokio::test]
    async fn selected_command_wake_rechecks_wait_and_recovers_tail_conflict() -> Result<()> {
        for cancel in [false, true] {
            let client = StreamClient::new(Arc::new(MemoryStream::default()));
            let gate = Arc::new(PausedChildVerifier {
                armed: std::sync::atomic::AtomicBool::new(false),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let mut first = DistributedCoordinator::open(&client, gate.clone()).await?;
            let task = OperationId::from_bytes([51; 16]);
            let selected = OperationId::from_bytes([52; 16]);
            declare(&mut first, spec(task, 0)?, "wake-task").await?;
            let worker = Worker {
                id: "wake-worker".into(),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            };
            let lease = first
                .pull(&worker)
                .await?
                .ok_or_else(|| Error::NotFound("wake lease".into()))?;
            let fence = LeaseFence::from(&lease.reservation);
            first
                .apply(
                    task,
                    IdempotencyKey::new("wake-start")?,
                    SchedulerEvent::Started {
                        operation_id: task,
                        fence: fence.clone(),
                    },
                )
                .await?;
            first
                .apply(
                    task,
                    IdempotencyKey::new("wake-suspend")?,
                    SchedulerEvent::WorkflowSuspended {
                        operation_id: task,
                        fence,
                        workflow_revision: 1,
                        waiting_command: Some(selected),
                    },
                )
                .await?;
            let mut second =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            };
            let issuer = AuthorityIssuer::new("selected-wake-test", [9; 32], owner.clone());
            let signed = issuer.root(
                "wake",
                Capabilities::new(["operation:wake", "operation:observe"]),
            );
            let verifier = issuer.verifier();
            let input = state_ref()?;
            assert!(
                first
                    .resume_command(
                        &owner,
                        &signed,
                        &verifier,
                        task,
                        1,
                        input.clone(),
                        IdempotencyKey::new("wrong-selected-wake")?,
                        OperationId::from_bytes([53; 16])
                    )
                    .await
                    .is_err()
            );
            gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
            let disturbance = async {
                gate.entered.notified().await;
                let result = if cancel {
                    second
                        .apply(
                            task,
                            IdempotencyKey::new("cancel-wake")?,
                            SchedulerEvent::CancellationRequested {
                                operation_id: task,
                                recursive: false,
                            },
                        )
                        .await
                } else {
                    declare(
                        &mut second,
                        spec(OperationId::from_bytes([54; 16]), 0)?,
                        "wake-unrelated-tail",
                    )
                    .await
                };
                gate.release.notify_one();
                result
            };
            let (attempt, disturbance) = tokio::join!(
                first.resume_command(
                    &owner,
                    &signed,
                    &verifier,
                    task,
                    1,
                    input.clone(),
                    IdempotencyKey::new("selected-wake")?,
                    selected
                ),
                disturbance
            );
            disturbance?;
            assert!(matches!(attempt, Err(Error::Conflict(_))));
            let retry = first
                .resume_command(
                    &owner,
                    &signed,
                    &verifier,
                    task,
                    1,
                    input.clone(),
                    IdempotencyKey::new("selected-wake")?,
                    selected,
                )
                .await;
            if cancel {
                assert!(retry.is_err());
            } else {
                assert_eq!(retry?, CoordinatorApply::Applied);
                // Releasing a replacement lease for a different command at the
                // same checkpoint must not accept a stale readiness decision.
                let lease = first
                    .pull(&worker)
                    .await?
                    .ok_or_else(|| Error::NotFound("replacement wake lease".into()))?;
                assert_eq!(lease.operation.operation_id, task);
                let fence = LeaseFence::from(&lease.reservation);
                first
                    .apply(
                        task,
                        IdempotencyKey::new("second-wait-start")?,
                        SchedulerEvent::Started {
                            operation_id: task,
                            fence: fence.clone(),
                        },
                    )
                    .await?;
                let next = OperationId::from_bytes([55; 16]);
                first
                    .apply(
                        task,
                        IdempotencyKey::new("second-wait-suspend")?,
                        SchedulerEvent::WorkflowSuspended {
                            operation_id: task,
                            fence,
                            workflow_revision: 1,
                            waiting_command: Some(next),
                        },
                    )
                    .await?;
                assert!(
                    first
                        .resume_command(
                            &owner,
                            &signed,
                            &verifier,
                            task,
                            1,
                            input.clone(),
                            IdempotencyKey::new("selected-wake")?,
                            selected
                        )
                        .await
                        .is_err()
                );
                assert_eq!(
                    first
                        .resume_command(
                            &owner,
                            &signed,
                            &verifier,
                            task,
                            1,
                            input,
                            IdempotencyKey::new("second-selected-wake")?,
                            next
                        )
                        .await?,
                    CoordinatorApply::Applied
                );
            }
        }
        Ok(())
    }

    impl ContentResidencyVerifier for DenyContentVerifier {
        fn verify<'a>(
            &'a self,
            _reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Err(Error::Unsupported("content provider unavailable".into())) })
        }
    }

    fn spec(operation_id: OperationId, cpu: u64) -> Result<OperationSpec> {
        Ok(OperationSpec {
            operation_id,
            parent: None,
            owner: DurableOwner::Detached {
                authority: Authority {
                    kind: AggregateKind::Task,
                    id: "owner".into(),
                },
            },
            entrypoint: EntrypointRef {
                name: "example.task".into(),
                version: "1".into(),
                digest: [2; 32],
                result_schema: Value::Object(Default::default()),
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest(BTreeMap::from([("cpu".into(), cpu)])),
            placement: BTreeMap::new(),
            orchestration: Orchestration::Leaf,
            state: state_ref()?,
        })
    }

    async fn declare<P: StreamProvider>(
        coordinator: &mut DistributedCoordinator<P>,
        spec: OperationSpec,
        key: &str,
    ) -> Result<CoordinatorApply> {
        let owner = match &spec.owner {
            DurableOwner::Attached { authority } | DurableOwner::Detached { authority } => {
                authority.clone()
            }
        };
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let scope = issuer.root("declare", Capabilities::new(["operation:declare"]));
        coordinator
            .declare_operation(
                &owner,
                &scope,
                &issuer.verifier(),
                spec,
                IdempotencyKey::new(key)?,
            )
            .await
    }

    #[tokio::test]
    async fn initial_state_must_reside_before_declaration_is_published() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let operation_id = OperationId::from_bytes([51; 16]);
        let mut unbound =
            DistributedCoordinator::open(&client, Arc::new(DenyContentVerifier)).await?;
        assert!(matches!(
            declare(&mut unbound, spec(operation_id, 0)?, "unbound-state").await,
            Err(Error::Unsupported(_))
        ));
        assert!(unbound.scheduler().operation(operation_id).is_none());

        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let mut corrupt = spec(operation_id, 0)?;
        corrupt.state = FileRef::new(
            corrupt.state.volume().clone(),
            "state/initial.json",
            "generation-1",
            FileDescriptor::from_bytes(b"different", "application/json")?,
            "initial.json",
        )?;
        assert!(matches!(
            declare(&mut coordinator, corrupt, "corrupt-state").await,
            Err(Error::Invalid(_))
        ));
        assert!(coordinator.scheduler().operation(operation_id).is_none());

        assert_eq!(
            declare(&mut coordinator, spec(operation_id, 0)?, "valid-state").await?,
            CoordinatorApply::Applied
        );
        assert!(matches!(
            DistributedCoordinator::open(&client, Arc::new(DenyContentVerifier)).await,
            Err(Error::Unsupported(_))
        ));
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert!(reopened.scheduler().operation(operation_id).is_some());
        assert_eq!(
            declare(&mut reopened, spec(operation_id, 0)?, "valid-state").await?,
            CoordinatorApply::Replayed
        );
        Ok(())
    }

    #[tokio::test]
    async fn declarations_require_authenticated_matching_owner_and_capability() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let operation_id = OperationId::from_bytes([52; 16]);
        let declaration = spec(operation_id, 0)?;
        assert!(matches!(
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("generic-declare")?,
                    coordinator.scheduler().declare(declaration.clone())?
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let other = Authority {
            kind: AggregateKind::Task,
            id: "other".into(),
        };
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let granted = issuer.root("declare", Capabilities::new(["operation:declare"]));
        let empty = issuer.root("empty", Capabilities::new([] as [&str; 0]));
        assert!(matches!(
            coordinator
                .declare_operation(
                    &owner,
                    &empty,
                    &issuer.verifier(),
                    declaration.clone(),
                    IdempotencyKey::new("missing-capability")?
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            coordinator
                .declare_operation(
                    &other,
                    &granted,
                    &issuer.verifier(),
                    declaration.clone(),
                    IdempotencyKey::new("wrong-audience")?
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut declaration = declaration;
        declaration.owner = DurableOwner::Detached { authority: other };
        assert!(matches!(
            coordinator
                .declare_operation(
                    &owner,
                    &granted,
                    &issuer.verifier(),
                    declaration,
                    IdempotencyKey::new("wrong-owner")?
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(coordinator.scheduler().operation(operation_id).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn another_owner_cannot_attach_a_child_to_a_foreign_parent() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let parent = OperationId::from_bytes([54; 16]);
        let child = OperationId::from_bytes([55; 16]);
        declare(&mut coordinator, spec(parent, 0)?, "owned-parent").await?;
        let mut foreign_child = spec(child, 0)?;
        foreign_child.owner = DurableOwner::Detached {
            authority: Authority {
                kind: AggregateKind::Task,
                id: "foreign".into(),
            },
        };
        foreign_child.parent = Some(ParentLink {
            operation_id: parent,
            slot: "uninvited-child".into(),
        });
        assert!(matches!(
            declare(&mut coordinator, foreign_child, "foreign-parent-link").await,
            Err(Error::NotFound(_))
        ));
        assert!(coordinator.scheduler().operation(child).is_none());
        assert_eq!(coordinator.scheduler().children(parent).count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn owner_observes_bounded_children_with_revision_checked_continuation() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let scope = issuer.root("observe", Capabilities::new(["operation:observe"]));
        let parent = OperationId::from_bytes([56; 16]);
        declare(&mut coordinator, spec(parent, 0)?, "hierarchy-parent").await?;
        for (id, slot) in [(57_u8, "a"), (58_u8, "b")] {
            let mut child = spec(OperationId::from_bytes([id; 16]), 0)?;
            child.parent = Some(ParentLink {
                operation_id: parent,
                slot: slot.into(),
            });
            declare(&mut coordinator, child, &format!("hierarchy-{slot}")).await?;
        }
        let first = coordinator
            .observe_children(
                &owner,
                &scope,
                &issuer.verifier(),
                parent,
                ChildOperationPageRequest {
                    expected_revision: None,
                    after_slot: None,
                    maximum: 1,
                },
            )
            .await?;
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].slot, "a");
        assert_eq!(first.next_after.as_deref(), Some("a"));
        let second = coordinator
            .observe_children(
                &owner,
                &scope,
                &issuer.verifier(),
                parent,
                ChildOperationPageRequest {
                    expected_revision: Some(first.revision),
                    after_slot: first.next_after.as_deref(),
                    maximum: 1,
                },
            )
            .await?;
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].slot, "b");
        assert!(second.next_after.is_none());
        let mut late = spec(OperationId::from_bytes([59; 16]), 0)?;
        late.parent = Some(ParentLink {
            operation_id: parent,
            slot: "c".into(),
        });
        let mut peer = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        declare(&mut peer, late, "hierarchy-c").await?;
        assert!(matches!(
            coordinator
                .observe_children(
                    &owner,
                    &scope,
                    &issuer.verifier(),
                    parent,
                    ChildOperationPageRequest {
                        expected_revision: Some(first.revision),
                        after_slot: first.next_after.as_deref(),
                        maximum: 1,
                    }
                )
                .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn child_page_validation_rejects_invalid_ids_and_continuations() -> Result<()> {
        let parent = OperationId::from_bytes([1; 16]);
        assert!(validate_child_page_request(OperationId::from_bytes([0; 16]), None, 1).is_err());
        assert!(validate_child_page_request(parent, None, 0).is_err());
        assert!(validate_child_page_request(parent, None, MAX_CHILD_PAGE + 1).is_err());
        assert!(validate_child_page_request(parent, Some("\u{7f}"), 1).is_err());
        assert!(
            validate_child_page_request(parent, Some(&"x".repeat(MAX_CHILD_SLOT_BYTES + 1)), 1)
                .is_err()
        );
        validate_child_page_request(parent, Some("résumé"), 1)?;

        let page = ChildOperationPage {
            revision: 4,
            entries: vec![ChildOperationLink {
                slot: "a".into(),
                operation_id: OperationId::from_bytes([2; 16]),
            }],
            next_after: Some("a".into()),
        };
        validate_child_page(&page, Some(4), None, 1)?;
        assert!(validate_child_page(&page, Some(5), None, 1).is_err());
        assert!(validate_child_page(&page, None, Some("a"), 1).is_err());
        let mut invalid = page;
        invalid.next_after = Some("b".into());
        assert!(validate_child_page(&invalid, None, None, 1).is_err());
        invalid.next_after = Some("a".into());
        invalid.entries[0].operation_id = OperationId::from_bytes([0; 16]);
        assert!(validate_child_page(&invalid, None, None, 1).is_err());
        invalid.entries[0].operation_id = OperationId::from_bytes([2; 16]);
        invalid.entries[0].slot = "".into();
        assert!(validate_child_page(&invalid, None, None, 1).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn pull_enforces_operation_placement_labels() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let operation_id = OperationId::from_bytes([53; 16]);
        let mut declaration = spec(operation_id, 0)?;
        declaration.placement.insert("region".into(), "eu".into());
        declare(&mut coordinator, declaration, "placed").await?;
        let mut worker = Worker {
            id: "worker".into(),
            available: ResourceSnapshot::default(),
            labels: BTreeMap::new(),
        };
        assert!(coordinator.pull(&worker).await?.is_none());
        worker.labels.insert("region".into(), "us".into());
        assert!(coordinator.pull(&worker).await?.is_none());
        worker.labels.insert("region".into(), "eu".into());
        assert_eq!(
            coordinator
                .pull(&worker)
                .await?
                .map(|lease| lease.operation.operation_id),
            Some(operation_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn projector_reads_bounded_verified_coordinator_pages() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        assert!(read_coordinator_event_page(&client, 0, 1).await?.is_empty());
        let operation_id = OperationId::from_bytes([41; 16]);
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        declare(
            &mut coordinator,
            spec(operation_id, 1)?,
            "projector-declare",
        )
        .await?;
        let page = read_coordinator_event_page(&client, 0, 1).await?;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].revision, 1);
        assert_eq!(page[0].operation_id, operation_id);
        assert!(page[0].committed_at_ms > 0);
        assert!(matches!(page[0].event, SchedulerEvent::Declared { .. }));
        let second = OperationId::from_bytes([42; 16]);
        declare(&mut coordinator, spec(second, 1)?, "projector-second").await?;
        let next = read_coordinator_event_page(&client, 1, 1).await?;
        assert_eq!(next.len(), 1);
        assert!(next[0].committed_at_ms > page[0].committed_at_ms);
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let third = OperationId::from_bytes([43; 16]);
        declare(&mut reopened, spec(third, 1)?, "projector-third").await?;
        let last = read_coordinator_event_page(&client, 2, 1).await?;
        assert_eq!(last.len(), 1);
        assert!(last[0].committed_at_ms > next[0].committed_at_ms);
        assert!(read_coordinator_event_page(&client, 3, 1).await?.is_empty());
        assert!(read_coordinator_event_page(&client, 0, 0).await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one linear end-to-end scenario asserting atomic, durable, exactly-once \
                  recursive cancellation; splitting it into helpers would scatter one coherent \
                  story across multiple functions without making any step clearer"
    )]
    async fn authenticated_recursive_cancel_is_atomic_durable_and_exactly_replayable() -> Result<()>
    {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("runtime", [9; 32], owner.clone());
        let scope = issuer.root(
            "operation-control",
            Capabilities::new(["operation:observe", "operation:cancel"]),
        );
        let parent = OperationId::from_bytes([31; 16]);
        let child = OperationId::from_bytes([32; 16]);
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        declare(&mut coordinator, spec(parent, 0)?, "declare-control-parent").await?;
        let mut child_spec = spec(child, 0)?;
        child_spec.parent = Some(ParentLink {
            operation_id: parent,
            slot: "child".into(),
        });
        declare(&mut coordinator, child_spec, "declare-control-child").await?;

        assert_eq!(
            coordinator
                .observe_operation(&owner, &scope, &issuer.verifier(), parent)?
                .phase,
            OperationPhase::WaitingForDependencies
        );
        let (applied, status) = coordinator
            .cancel_operation(
                &owner,
                &scope,
                &issuer.verifier(),
                parent,
                IdempotencyKey::new("cancel-control-tree")?,
                true,
            )
            .await?;
        assert_eq!(applied, CoordinatorApply::Applied);
        assert_eq!(status.outcome, Some(crate::Outcome::Cancelled));
        assert_eq!(
            coordinator
                .scheduler()
                .operation(child)
                .map(|value| value.outcome.clone()),
            Some(Some(crate::Outcome::Cancelled))
        );

        let (replayed, _) = coordinator
            .cancel_operation(
                &owner,
                &scope,
                &issuer.verifier(),
                parent,
                IdempotencyKey::new("cancel-control-tree")?,
                true,
            )
            .await?;
        assert_eq!(replayed, CoordinatorApply::Replayed);
        assert!(matches!(
            coordinator
                .cancel_operation(
                    &owner,
                    &scope,
                    &issuer.verifier(),
                    parent,
                    IdempotencyKey::new("cancel-control-tree")?,
                    false,
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let reopened = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(
            reopened
                .observe_operation(&owner, &scope, &issuer.verifier(), child)?
                .outcome,
            Some(crate::Outcome::Cancelled)
        );

        let observe_only = issuer.root("observe-only", Capabilities::new(["operation:observe"]));
        let mut reopened = reopened;
        assert!(matches!(
            reopened
                .cancel_operation(
                    &owner,
                    &observe_only,
                    &issuer.verifier(),
                    child,
                    IdempotencyKey::new("unauthorized-cancel")?,
                    false,
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn live_coordinators_refresh_foreign_commits_and_reconcile_identical_declarations()
    -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut first =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let mut second =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let operation_id = OperationId::from_bytes([42; 16]);
        let declaration = spec(operation_id, 0)?;
        assert!(second.scheduler().operation(operation_id).is_none());
        assert_eq!(
            declare(&mut first, declaration.clone(), "shared-declare").await?,
            CoordinatorApply::Applied
        );
        assert_eq!(
            declare(&mut second, declaration, "shared-declare").await?,
            CoordinatorApply::Replayed
        );
        assert!(second.scheduler().operation(operation_id).is_some());
        first
            .apply(
                operation_id,
                IdempotencyKey::new("shared-cancel")?,
                SchedulerEvent::CancellationRequested {
                    operation_id,
                    recursive: false,
                },
            )
            .await?;
        second.refresh().await?;
        assert_eq!(
            second
                .scheduler()
                .operation(operation_id)
                .and_then(|state| state.outcome.clone()),
            Some(crate::Outcome::Cancelled)
        );
        Ok(())
    }

    #[tokio::test]
    async fn coordinator_replays_and_pull_admission_is_exclusive() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let operation_id = OperationId::from_bytes([1; 16]);
        let spec = OperationSpec {
            operation_id,
            parent: None,
            owner: DurableOwner::Detached {
                authority: Authority {
                    kind: AggregateKind::Task,
                    id: "owner".into(),
                },
            },
            entrypoint: EntrypointRef {
                name: "example.task".into(),
                version: "1".into(),
                digest: [2; 32],
                result_schema: Value::Object(Default::default()),
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: BTreeMap::new(),
            orchestration: Orchestration::Leaf,
            state: state_ref()?,
        };
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        declare(&mut coordinator, spec, "declare-1").await?;
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let worker = Worker {
            id: "worker-1".into(),
            available: ResourceSnapshot::default(),
            labels: BTreeMap::new(),
        };
        let lease = reopened
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("lease".into()))?;
        assert_eq!(lease.operation.operation_id, operation_id);
        assert!(reopened.pull(&worker).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn active_worker_reservations_are_subtracted_from_capacity() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let first = OperationId::from_bytes([3; 16]);
        let second = OperationId::from_bytes([4; 16]);
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        for (index, operation_id) in [first, second].into_iter().enumerate() {
            declare(
                &mut coordinator,
                spec(operation_id, 1)?,
                &format!("declare-capacity-{index}"),
            )
            .await?;
        }
        let worker = Worker {
            id: "worker-one".into(),
            available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
            labels: BTreeMap::new(),
        };
        let first_lease = coordinator
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("first lease".into()))?;
        assert_eq!(first_lease.operation.operation_id, first);
        assert!(coordinator.pull(&worker).await?.is_none());
        coordinator
            .apply(
                first,
                IdempotencyKey::new("start-capacity-first")?,
                SchedulerEvent::Started {
                    operation_id: first,
                    fence: LeaseFence::from(&first_lease.reservation),
                },
            )
            .await?;
        coordinator
            .apply(
                first,
                IdempotencyKey::new("uncertain-capacity-first")?,
                SchedulerEvent::Completed {
                    operation_id: first,
                    outcome: crate::Outcome::Indeterminate {
                        operation_id: first,
                    },
                    fence: Some(LeaseFence::from(&first_lease.reservation)),
                    execution_duration_ns: None,
                },
            )
            .await?;
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert!(coordinator.pull(&worker).await?.is_none());
        coordinator
            .apply(
                first,
                IdempotencyKey::new("cancel-uncertain-capacity-first")?,
                SchedulerEvent::CancellationRequested {
                    operation_id: first,
                    recursive: true,
                },
            )
            .await?;
        assert!(coordinator.pull(&worker).await?.is_none());
        assert!(
            coordinator
                .apply(
                    first,
                    IdempotencyKey::new("unfenced-capacity-first")?,
                    SchedulerEvent::Completed {
                        operation_id: first,
                        outcome: crate::Outcome::Cancelled,
                        fence: None,
                        execution_duration_ns: None,
                    },
                )
                .await
                .is_err()
        );
        coordinator
            .apply(
                first,
                IdempotencyKey::new("complete-capacity-first")?,
                SchedulerEvent::Completed {
                    operation_id: first,
                    outcome: crate::Outcome::Succeeded(state_ref()?),
                    fence: Some(LeaseFence::from(&first_lease.reservation)),
                    execution_duration_ns: None,
                },
            )
            .await?;
        assert_eq!(
            coordinator
                .pull(&worker)
                .await?
                .map(|lease| lease.operation.operation_id),
            Some(second)
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_session_claims_cannot_overspend_after_reopen() -> Result<()> {
        session_claim_race(StreamClient::new(Arc::new(MemoryStream::default()))).await?;
        Ok(())
    }

    #[tokio::test]
    async fn root_declaration_and_shared_limits_are_atomic() -> Result<()> {
        atomic_session_race(StreamClient::new(Arc::new(MemoryStream::default()))).await?;
        Ok(())
    }

    #[tokio::test]
    async fn session_retry_rejects_separately_committed_lookalike_events() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let root = OperationId::from_bytes([86; 16]);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("split-session", [9; 32], owner.clone());
        let signed = issuer.root("owner", Capabilities::new(["operation:declare"]));
        let limits = crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        };
        let key = "split-root";
        coordinator
            .declare_operation(
                &owner,
                &signed,
                &issuer.verifier(),
                spec(root, 0)?,
                IdempotencyKey::new(key)?,
            )
            .await?;
        coordinator
            .configure_session(
                &owner,
                &signed,
                &issuer.verifier(),
                root,
                limits,
                IdempotencyKey::new(format!(
                    "session:{root}:{}",
                    blake3::hash(key.as_bytes()).to_hex()
                ))?,
            )
            .await?;
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert!(
            reopened
                .declare_session(
                    &owner,
                    &signed,
                    &issuer.verifier(),
                    spec(root, 0)?,
                    limits,
                    IdempotencyKey::new(key)?
                )
                .await
                .is_err()
        );
        assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 2);
        Ok(())
    }

    struct LostSessionAck {
        inner: MemoryStream,
        lose_ack: std::sync::atomic::AtomicBool,
        hide_receipt: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl StreamProvider for LostSessionAck {
        async fn inspect_idempotency(
            &self,
            key: acyclic_stream::IdempotencyKey,
        ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError>
        {
            if self
                .hide_receipt
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StreamError::Unavailable);
            }
            self.inner.inspect_idempotency(key).await
        }
        async fn tail(
            &self,
            path: acyclic_stream::StreamPath,
        ) -> std::result::Result<u64, StreamError> {
            self.inner.tail(path).await
        }
        async fn bounds(
            &self,
            path: acyclic_stream::StreamPath,
        ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
            self.inner.bounds(path).await
        }
        async fn append(
            &self,
            request: acyclic_stream::AppendRequest,
        ) -> std::result::Result<AppendOutcome, StreamError> {
            let outcome = self.inner.append(request).await?;
            if matches!(outcome, AppendOutcome::Committed(_))
                && self
                    .lose_ack
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StreamError::Unavailable);
            }
            Ok(outcome)
        }
        async fn fork(
            &self,
            request: acyclic_stream::ForkRequest,
        ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
            self.inner.fork(request).await
        }
        async fn read(
            &self,
            request: acyclic_stream::ReadRequest,
        ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
            self.inner.read(request).await
        }
        async fn follow(
            &self,
            path: acyclic_stream::StreamPath,
            from: u64,
        ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
            self.inner.follow(path, from).await
        }
        async fn children(
            &self,
            request: acyclic_stream::ChildrenRequest,
        ) -> std::result::Result<acyclic_stream::ChildStream, StreamError> {
            self.inner.children(request).await
        }
        async fn commit(
            &self,
            request: acyclic_stream::CommitRequest,
        ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
            self.inner.commit(request).await
        }
        async fn read_commit(
            &self,
            id: acyclic_stream::CommitId,
        ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
            self.inner.read_commit(id).await
        }
    }

    #[tokio::test]
    async fn root_limits_reconcile_a_lost_atomic_append_acknowledgement() -> Result<()> {
        for hide_receipt in [false, true] {
            let client = StreamClient::new(Arc::new(LostSessionAck {
                inner: MemoryStream::default(),
                lose_ack: std::sync::atomic::AtomicBool::new(true),
                hide_receipt: std::sync::atomic::AtomicBool::new(hide_receipt),
            }));
            let mut coordinator =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let root = OperationId::from_bytes([87; 16]);
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            };
            let issuer = AuthorityIssuer::new("lost-session-ack", [9; 32], owner.clone());
            let signed = issuer.root("owner", Capabilities::new(["operation:declare"]));
            let limits = crate::scheduler::SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 0,
                model_steps: 1,
            };
            let result = coordinator
                .declare_session(
                    &owner,
                    &signed,
                    &issuer.verifier(),
                    spec(root, 0)?,
                    limits,
                    IdempotencyKey::new("lost-root")?,
                )
                .await;
            if hide_receipt {
                assert!(matches!(result, Err(Error::Indeterminate(id)) if id == root));
            } else {
                assert!(result.is_ok());
            }
            drop(coordinator);
            let mut recovered =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            assert_eq!(recovered.scheduler().session_limits(root)?, limits);
            assert_eq!(
                recovered
                    .declare_session(
                        &owner,
                        &signed,
                        &issuer.verifier(),
                        spec(root, 0)?,
                        limits,
                        IdempotencyKey::new("lost-root")?
                    )
                    .await?,
                CoordinatorApply::Replayed
            );
            assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 2);
        }
        Ok(())
    }

    #[cfg(feature = "filesystem-local")]
    #[tokio::test]
    async fn atomic_root_limits_survive_local_stream_reopen() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let client = StreamClient::new(Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let (root, limits) = atomic_session_race(client).await?;
        let client = StreamClient::new(Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(coordinator.scheduler().session_limits(root)?, limits);
        assert_eq!(
            coordinator
                .scheduler()
                .operation(root)
                .map(|state| state.phase),
            Some(crate::scheduler::OperationPhase::Admitted)
        );
        Ok(())
    }

    async fn atomic_session_race<P: StreamProvider>(
        client: StreamClient<P>,
    ) -> Result<(OperationId, crate::scheduler::SessionLimits)> {
        let mut left = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let mut right =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let root = OperationId::from_bytes([89; 16]);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("atomic-budget", [9; 32], owner.clone());
        let signed = issuer.root("owner", Capabilities::new(["operation:declare"]));
        let verifier = issuer.verifier();
        let one = crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 2,
            depth: 1,
            model_steps: 1,
        };
        let two = crate::scheduler::SessionLimits {
            model_steps: 2,
            ..one
        };
        let unsigned = issuer.root("reader", Capabilities::default());
        assert!(
            left.declare_session(
                &owner,
                &unsigned,
                &verifier,
                spec(root, 0)?,
                one,
                IdempotencyKey::new("unauthorized-root")?
            )
            .await
            .is_err()
        );
        assert!(
            read_coordinator_event_page(&client, 0, 64)
                .await?
                .is_empty()
        );
        let (first, second) = tokio::join!(
            left.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(root, 0)?,
                one,
                IdempotencyKey::new("atomic-one")?
            ),
            right.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(root, 0)?,
                two,
                IdempotencyKey::new("atomic-two")?
            ),
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let (limits, key) = if first.is_ok() {
            (one, "atomic-one")
        } else {
            (two, "atomic-two")
        };
        let events = read_coordinator_event_page(&client, 0, 64).await?;
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events.first().map(|record| &record.event),
            Some(SchedulerEvent::Declared { .. })
        ));
        assert!(
            matches!(events.last().map(|record| &record.event), Some(SchedulerEvent::SessionConfigured { limits: retained, .. }) if *retained == limits)
        );
        let mut recovered =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(recovered.scheduler().session_limits(root)?, limits);
        let lease = recovered
            .pull(&Worker {
                id: "atomic-worker".into(),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            })
            .await?
            .ok_or_else(|| Error::NotFound("atomic root lease".into()))?;
        assert_eq!(lease.operation.operation_id, root);
        assert_eq!(
            recovered
                .declare_session(
                    &owner,
                    &signed,
                    &verifier,
                    spec(root, 0)?,
                    limits,
                    IdempotencyKey::new(key)?
                )
                .await?,
            CoordinatorApply::Replayed
        );
        assert!(
            recovered
                .declare_session(
                    &owner,
                    &signed,
                    &verifier,
                    spec(root, 0)?,
                    crate::scheduler::SessionLimits {
                        model_steps: limits.model_steps + 1,
                        ..limits
                    },
                    IdempotencyKey::new(key)?
                )
                .await
                .is_err()
        );
        assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 3);
        // Identical logical requests may encode different commit timestamps.
        let identical_root = OperationId::from_bytes([88; 16]);
        let mut duplicate =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let (first, second) = tokio::join!(
            recovered.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(identical_root, 0)?,
                limits,
                IdempotencyKey::new("identical-root")?
            ),
            duplicate.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(identical_root, 0)?,
                limits,
                IdempotencyKey::new("identical-root")?
            ),
        );
        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 5);
        Ok((root, limits))
    }

    #[cfg(feature = "filesystem-local")]
    #[tokio::test]
    async fn local_session_budget_survives_provider_reopen() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let loser = session_claim_race(StreamClient::new(provider.clone())).await?;
        drop(provider);
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let client = StreamClient::new(Arc::new(provider));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert!(
            coordinator
                .apply(
                    scheduler_event_operation(&loser),
                    IdempotencyKey::new("local-budget-loser")?,
                    loser
                )
                .await
                .is_err()
        );
        Ok(())
    }

    async fn session_claim_race<P: StreamProvider>(
        client: StreamClient<P>,
    ) -> Result<SchedulerEvent> {
        let mut first =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let root = OperationId::from_bytes([90; 16]);
        declare(&mut first, spec(root, 0)?, "budget-root").await?;
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("budget-test", [9; 32], owner.clone());
        let scope = issuer.root("budget", Capabilities::new(["operation:declare"]));
        let limits = crate::scheduler::SessionLimits {
            active_tasks: 2,
            total_tasks: 3,
            depth: 1,
            model_steps: 1,
        };
        assert!(
            first
                .apply(
                    root,
                    IdempotencyKey::new("unscoped-budget")?,
                    SchedulerEvent::SessionConfigured {
                        operation_id: root,
                        limits
                    }
                )
                .await
                .is_err()
        );
        first
            .configure_session(
                &owner,
                &scope,
                &issuer.verifier(),
                root,
                limits,
                IdempotencyKey::new("configure-budget")?,
            )
            .await?;
        let mut claims = Vec::new();
        for index in [91, 92] {
            let id = OperationId::from_bytes([index; 16]);
            let mut child = spec(id, 0)?;
            child.parent = Some(ParentLink {
                operation_id: root,
                slot: index.to_string(),
            });
            declare(&mut first, child, &format!("budget-child-{index}")).await?;
            let lease = first
                .pull(&Worker {
                    id: format!("worker-{index}"),
                    available: ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("budget lease".into()))?;
            // Root may be pulled first; both independently placed tasks belong to this session.
            let id = lease.operation.operation_id;
            let fence = LeaseFence::from(&lease.reservation);
            first
                .apply(
                    id,
                    IdempotencyKey::new(format!("budget-start-{index}"))?,
                    SchedulerEvent::Started {
                        operation_id: id,
                        fence: fence.clone(),
                    },
                )
                .await?;
            claims.push(SchedulerEvent::ModelDispatchClaimed {
                operation_id: id,
                attempt_id: id,
                step: 0,
                request_digest: [index; 32],
                fence,
                ceiling: 1,
            });
        }
        let mut second =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let [one, two] = claims.as_slice() else {
            return Err(Error::Invalid("two budget claims required".into()));
        };
        let (left, right) = tokio::join!(
            first.apply(
                scheduler_event_operation(one),
                IdempotencyKey::new("budget-one")?,
                one.clone()
            ),
            second.apply(
                scheduler_event_operation(two),
                IdempotencyKey::new("budget-two")?,
                two.clone()
            )
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let (winner, key, loser) = if left.is_ok() {
            (one, "budget-one", two)
        } else {
            (two, "budget-two", one)
        };
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(
            reopened
                .apply(
                    scheduler_event_operation(winner),
                    IdempotencyKey::new(key)?,
                    winner.clone()
                )
                .await?,
            CoordinatorApply::Replayed
        );
        assert!(
            reopened
                .apply(
                    scheduler_event_operation(loser),
                    IdempotencyKey::new("budget-loser-retry")?,
                    loser.clone()
                )
                .await
                .is_err()
        );
        let mut changed = winner.clone();
        if let SchedulerEvent::ModelDispatchClaimed { request_digest, .. } = &mut changed {
            *request_digest = [99; 32];
        }
        assert!(
            reopened
                .apply(
                    scheduler_event_operation(&changed),
                    IdempotencyKey::new("budget-changed")?,
                    changed
                )
                .await
                .is_err()
        );
        assert!(
            reopened
                .configure_session(
                    &owner,
                    &scope,
                    &issuer.verifier(),
                    root,
                    crate::scheduler::SessionLimits {
                        model_steps: 2,
                        ..limits
                    },
                    IdempotencyKey::new("budget-reset")?
                )
                .await
                .is_err()
        );
        let winner_id = scheduler_event_operation(winner);
        let digest = *blake3::hash(&crate::contract::canonical_json_bytes(winner)?).as_bytes();
        let receipt = client
            .inspect_idempotency(stream_key(key)?)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .ok_or_else(|| Error::NotFound("winning budget receipt".into()))?;
        let IdempotencyOutcome::Append(AppendOutcome::Committed(receipt)) = receipt.outcome else {
            return Err(Error::Invalid("winning budget append required".into()));
        };
        let revision = receipt.end;
        // Exercise the completion boundary with actual provider outcomes. A response
        // may be delayed while another coordinator commits cancellation.
        let mut delayed =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(
            delayed
                .finish_append(
                    AppendOutcome::Committed(receipt.clone()),
                    winner,
                    key,
                    &digest,
                    revision,
                )
                .await?,
            CoordinatorApply::Applied
        );
        reopened
            .apply(
                winner_id,
                IdempotencyKey::new("cancel-budget-winner")?,
                SchedulerEvent::CancellationRequested {
                    operation_id: winner_id,
                    recursive: false,
                },
            )
            .await?;
        assert!(
            reopened
                .apply(winner_id, IdempotencyKey::new(key)?, winner.clone(),)
                .await
                .is_err(),
            "an exact accounting retry cannot bypass cancellation"
        );
        assert!(
            delayed
                .finish_append(
                    AppendOutcome::Committed(receipt),
                    winner,
                    key,
                    &digest,
                    revision,
                )
                .await
                .is_err(),
            "a delayed committed response cannot bypass cancellation"
        );
        let conflict = delayed
            .stream
            .append_batch(vec![Bytes::from_static(b"not committed")], Some(0), None)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(matches!(conflict, AppendOutcome::TailConflict { .. }));
        assert!(
            delayed
                .finish_append(conflict, winner, key, &digest, revision)
                .await
                .is_err(),
            "an intent discovered after a tail conflict cannot bypass cancellation"
        );
        Ok(loser.clone())
    }

    #[tokio::test]
    async fn cancelled_work_does_not_starve_the_pull_queue() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let cancelled = OperationId::from_bytes([5; 16]);
        let runnable = OperationId::from_bytes([6; 16]);
        for (index, operation_id) in [cancelled, runnable].into_iter().enumerate() {
            declare(
                &mut coordinator,
                spec(operation_id, 0)?,
                &format!("declare-cancel-{index}"),
            )
            .await?;
        }
        coordinator
            .apply(
                cancelled,
                IdempotencyKey::new("cancel-before-pull")?,
                SchedulerEvent::CancellationRequested {
                    operation_id: cancelled,
                    recursive: false,
                },
            )
            .await?;
        assert_eq!(
            coordinator
                .scheduler()
                .operation(cancelled)
                .and_then(|state| state.outcome.as_ref()),
            Some(&crate::Outcome::Cancelled)
        );
        let worker = Worker {
            id: "worker".into(),
            available: ResourceSnapshot::default(),
            labels: BTreeMap::new(),
        };
        assert_eq!(
            coordinator
                .pull(&worker)
                .await?
                .map(|lease| lease.operation.operation_id),
            Some(runnable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn leases_are_fenced_and_recoverable() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let operation_id = OperationId::from_bytes([7; 16]);
        declare(&mut coordinator, spec(operation_id, 0)?, "declare-recovery").await?;
        let worker = Worker {
            id: "worker".into(),
            available: ResourceSnapshot::default(),
            labels: BTreeMap::new(),
        };
        let first = coordinator
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("lease".into()))?;
        let forged = LeaseFence {
            reservation_id: "foreign".into(),
            placement: worker.id.clone(),
        };
        assert!(matches!(
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("forged-start")?,
                    SchedulerEvent::Started {
                        operation_id,
                        fence: forged
                    }
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        coordinator
            .apply(
                operation_id,
                IdempotencyKey::new("valid-start")?,
                SchedulerEvent::Started {
                    operation_id,
                    fence: LeaseFence::from(&first.reservation),
                },
            )
            .await?;
        let checkpoint = crate::resources::CheckpointRef::new(
            crate::resources::ProviderRef::new("example", "machines", "1")?,
            vec![1],
            Some("checkpoint-1".into()),
        )?;
        coordinator
            .apply(
                operation_id,
                IdempotencyKey::new("checkpoint-before-crash")?,
                SchedulerEvent::Checkpointed {
                    operation_id,
                    checkpoint: checkpoint.clone(),
                    fence: LeaseFence::from(&first.reservation),
                },
            )
            .await?;
        coordinator
            .release_lease(&first, IdempotencyKey::new("release-crashed-worker")?)
            .await?;
        let second = coordinator
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("replacement lease".into()))?;
        assert_ne!(first.reservation.id, second.reservation.id);
        assert_eq!(second.checkpoint, Some(checkpoint));
        assert!(second.operation_revision > first.operation_revision);
        assert!(matches!(
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("stale-complete")?,
                    SchedulerEvent::Completed {
                        operation_id,
                        outcome: crate::Outcome::Succeeded(state_ref()?),
                        fence: Some(LeaseFence::from(&first.reservation)),
                        execution_duration_ns: None,
                    }
                )
                .await,
            Err(Error::Conflict(_) | Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[test]
    fn duplicate_reducer_registration_never_replaces_the_original() -> Result<()> {
        let entrypoint = EntrypointRef {
            name: "example.reducer".into(),
            version: "1".into(),
            digest: [3; 32],
            result_schema: serde_json::json!({"type": "number"}),
        };
        let original: Arc<dyn DurableReducer> = Arc::new(TestReducer {
            entrypoint: entrypoint.clone(),
            value: serde_json::json!(1),
        });
        let replacement: Arc<dyn DurableReducer> = Arc::new(TestReducer {
            entrypoint: entrypoint.clone(),
            value: serde_json::json!(2),
        });
        let mut registry = ReducerRegistry::new();
        registry.register(Arc::clone(&original))?;
        assert!(matches!(
            registry.register(replacement),
            Err(Error::Conflict(_))
        ));
        let retained = registry
            .get(&entrypoint)
            .ok_or_else(|| Error::NotFound("reducer".into()))?;
        assert!(Arc::ptr_eq(retained, &original));
        Ok(())
    }

    #[tokio::test]
    async fn generic_coordinator_apply_cannot_bypass_reducer_execution() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let operation_id = OperationId::from_bytes([13; 16]);
        let event = SchedulerEvent::Orchestrated {
            operation_id,
            expected_revision: 0,
            outcome: crate::Outcome::Succeeded(state_ref()?),
            cancel: Vec::new(),
            reducer: Some(EntrypointRef {
                name: "example.reducer".into(),
                version: "1".into(),
                digest: [3; 32],
                result_schema: Value::Object(Default::default()),
            }),
            reduction_digest: Some([1; 32]),
        };
        assert!(matches!(
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("forged-reduction")?,
                    event
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }
}
