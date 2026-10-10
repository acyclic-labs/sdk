//! Customer-hostable Stream-backed coordinator and pull-worker admission.

use crate::contract::capability;
use crate::contract::next_revision;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    runtime,
    scheduler::{
        AssemblyKind, EntrypointRef, LeaseFence, OperationSpec, OperationState,
        OrchestrationDecision, Reservation, ResourceSnapshot, Scheduler, SchedulerEvent,
        assembly_invocation_digest, reduction_invocation_digest,
    },
    wire,
    wire_codec::validate_protocol,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
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
const MAX_CACHED_INTENTS: usize = 64;

type RetainedIntent = ([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId);

/// Rebuildable location only; the event and all semantic facts remain in the
/// coordinator journal. Reads always verify this location against that source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentLocation {
    key: String,
    revision: u64,
    event_digest: [u8; 32],
    commit_id: [u8; 32],
}

fn intent_location_path(key: &str) -> String {
    format!(
        "harness/v2/coordinator/intent-locations/{}",
        blake3::hash(key.as_bytes()).to_hex()
    )
}
fn validate_child_page_request(
    parent: OperationId,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    runtime::validate_child_page_request(
        "child hierarchy",
        parent.into_bytes(),
        after_slot,
        maximum,
    )
}

fn validate_child_page(
    page: &ChildOperationPage,
    expected_revision: Option<u64>,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    runtime::validate_child_page(
        "child hierarchy",
        (page.revision, page.next_after.as_deref()),
        page.entries
            .iter()
            .map(|entry| (entry.slot.as_str(), entry.operation_id.into_bytes())),
        (expected_revision, after_slot, maximum),
    )
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
    /// Coordinator-observed revision associated with this reservation. Recovery
    /// may observe progress since the reservation's initial admission.
    pub operation_revision: u64,
}

/// Owner-scoped pull result. An unresolved publication retains its exact
/// attempted reservation identity without claiming that it committed.
pub enum WorkPull {
    /// No operation was admitted by this invocation.
    Idle,
    /// Coordinator publication was observed; execution must still verify it.
    Claimed(WorkLease),
    /// Publication or its observation failed. Check this exact attempt against
    /// the coordinator before resuming; do not infer quiescence or refund it.
    Unresolved {
        /// Exact attempted lease, including the reservation identity.
        lease: WorkLease,
        /// Publication/observation failure.
        error: Error,
    },
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
pub trait SchedulerPayloadStore: acyclic_stream::ProviderPlatform {
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
        crate::contract::compile_json_schema(&entrypoint.result_schema, "reducer result")?;
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
    if limit == 0 {
        return Err(Error::Invalid(
            "coordinator page limit is out of bounds".into(),
        ));
    }
    let stream = client
        .stream(COORDINATOR_PATH)
        .map_err(|error| Error::Storage(error.to_string()))?;
    let records = match stream.read(after_revision, limit.min(READ_PAGE_SIZE)).await {
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
        if revision != expected.saturating_add(1) || event.operation_id() != operation_id {
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

/// Reads immutable declaration metadata without opening or refreshing a
/// scheduler. The existing derived intent location is checked against one
/// original coordinator record, including its logical key, digest and commit.
/// Requires the owner's signed operation:observe scope. No writes are issued.
///
/// None means the location is absent; it does not prove that no declaration
/// committed. Legacy/missing locations require bootstrap through the existing
/// mutable coordinator. This is not current lifecycle, cancellation, reservation
/// or shared-accounting authority. Verify those before executing any work.
pub async fn observe_declaration<P: StreamProvider>(
    client: &StreamClient<P>,
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    operation_id: OperationId,
    declaration_key: &IdempotencyKey,
) -> Result<Option<OperationSpec>> {
    verifier.verify_audience(owner)?;
    verifier.verify(scope)?;
    if !scope.capabilities().contains(capability::OPERATION_OBSERVE) {
        return Err(Error::Unauthorized(
            "declaration observation requires operation:observe".into(),
        ));
    }
    let Some((_, event, ..)) = read_indexed_intent(client, declaration_key.as_str(), None).await?
    else {
        return Ok(None);
    };
    let SchedulerEvent::Declared { spec } = event else {
        return Err(Error::Conflict(
            "declaration key does not identify a declaration".into(),
        ));
    };
    if spec.operation_id != operation_id {
        return Err(Error::Conflict(
            "declaration operation identity differs".into(),
        ));
    }
    spec.verify_owner(owner, verifier)?;
    Ok(Some(*spec))
}

async fn read_intent_location<P: StreamProvider>(
    client: &StreamClient<P>,
    key: &str,
) -> Result<Option<IntentLocation>> {
    IdempotencyKey::new(key.to_owned())?;
    let stream = client.stream(intent_location_path(key))?;
    let records = match stream.read(0, 2).await {
        Ok(records) => records.try_collect::<Vec<_>>().await?,
        Err(StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let [record] = records.as_slice() else {
        return Err(Error::Storage(
            "intent location must contain one immutable record".into(),
        ));
    };
    let location: IntentLocation = crate::contract::json_from_slice(&record.value)
        .map_err(|error| Error::Storage(error.to_string()))?;
    if record.sequence != 0
        || location.key != key
        || location.revision == 0
        || crate::contract::canonical_json_bytes(&location)?.as_slice() != record.value.as_ref()
    {
        return Err(Error::Storage("intent location identity differs".into()));
    }
    Ok(Some(location))
}

async fn read_indexed_intent<P: StreamProvider>(
    client: &StreamClient<P>,
    key: &str,
    maximum_revision: Option<u64>,
) -> Result<Option<RetainedIntent>> {
    let Some(location) = read_intent_location(client, key).await? else {
        return Ok(None);
    };
    // A concurrent host may have indexed a later commit. It is outside this
    // projection's snapshot; the coordinator tail CAS still fences writes.
    if maximum_revision.is_some_and(|maximum| location.revision > maximum) {
        return Ok(None);
    }
    let records = client
        .stream(COORDINATOR_PATH)?
        .read(location.revision - 1, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let [record] = records.as_slice() else {
        return Err(Error::Storage("intent source record is absent".into()));
    };
    let (revision, operation, source_key, digest, _, event) = decode(&record.value)?;
    if record.sequence != location.revision - 1
        || revision != location.revision
        || record.commit_id.as_bytes() != &location.commit_id
        || source_key != key
        || digest != location.event_digest
        || event.operation_id() != operation
    {
        return Err(Error::Storage(
            "intent location differs from coordinator source".into(),
        ));
    }
    let intent = (digest, event, revision, record.commit_id);
    Ok(Some(intent))
}

/// One logical coordinator whose entire semantic history is a Stream.
pub struct DistributedCoordinator<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    scheduler: Scheduler,
    revision: u64,
    last_committed_at_ms: u64,
    intents: BTreeMap<String, RetainedIntent>,
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
                if revision != next_revision(record.sequence)? {
                    return Err(Error::Storage("coordinator revision is not gapless".into()));
                }
                if let SchedulerEvent::Declared { spec } = &event {
                    self.content_verifier.verify(&spec.state).await?;
                }
                self.verify_published_result(&event).await?;
                self.ensure_intent_location(IntentLocation {
                    key: key.clone(),
                    revision,
                    event_digest: digest,
                    commit_id: *record.commit_id.as_bytes(),
                })
                .await?;
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

    async fn read_intent_location(&self, key: &str) -> Result<Option<IntentLocation>> {
        read_intent_location(&self.client, key).await
    }

    async fn ensure_intent_location(&self, location: IntentLocation) -> Result<()> {
        if let Some(existing) = self.read_intent_location(&location.key).await? {
            return if existing == location {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "coordinator history repeats an idempotency key or index differs".into(),
                ))
            };
        }
        let bytes = crate::contract::canonical_json_bytes(&location)?;
        let physical = stream_key(&format!(
            "intent-location:{}",
            blake3::hash(&bytes).to_hex()
        ))?;
        let outcome = self
            .client
            .stream(intent_location_path(&location.key))?
            .append_batch(vec![Bytes::from(bytes)], Some(0), Some(physical))
            .await;
        match outcome {
            Ok(AppendOutcome::Committed(receipt))
                if receipt.start == 0 && receipt.end == 1 && receipt.tail == 1 =>
            {
                Ok(())
            }
            Ok(AppendOutcome::TailConflict { .. }) | Err(StreamError::Unavailable) => {
                match self.read_intent_location(&location.key).await? {
                    Some(existing) if existing == location => Ok(()),
                    Some(_) => Err(Error::Conflict(
                        "intent location publication differs".into(),
                    )),
                    None => Err(Error::Storage(
                        "intent location publication is unavailable".into(),
                    )),
                }
            }
            Ok(AppendOutcome::Committed(_)) => {
                Err(Error::Storage("intent location receipt differs".into()))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn cache_intent(&mut self, key: String, intent: RetainedIntent) {
        self.intents.insert(key, intent);
        if self.intents.len() > MAX_CACHED_INTENTS {
            let oldest = self
                .intents
                .iter()
                .min_by_key(|(_, value)| value.2)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.intents.remove(&oldest);
            }
        }
    }

    async fn retained_intent(&mut self, key: &str) -> Result<Option<RetainedIntent>> {
        if let Some(intent) = self.intents.get(key) {
            return Ok(Some(intent.clone()));
        }
        let Some(intent) = read_indexed_intent(&self.client, key, Some(self.revision)).await?
        else {
            return Ok(None);
        };
        self.cache_intent(key.to_owned(), intent.clone());
        Ok(Some(intent))
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
            crate::contract::validate_json_schema_value(schema, &value, "scheduler result")?;
        }
        Ok(())
    }

    /// Returns the deterministic scheduler projection.
    #[must_use]
    pub const fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    #[cfg(any(feature = "filesystem", test))]
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns one operation only after verifying the owner's signed scope.
    pub fn observe_operation(
        &self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
    ) -> Result<OperationState> {
        self.authorize_operation(
            owner,
            scope,
            verifier,
            operation_id,
            capability::OPERATION_OBSERVE,
        )
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
        self.authorize_operation(
            owner,
            scope,
            verifier,
            parent,
            capability::OPERATION_OBSERVE,
        )?;
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
            child.spec.verify_owner(owner, verifier)?;
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
        self.authorize_operation(
            owner,
            scope,
            verifier,
            operation_id,
            capability::OPERATION_CANCEL,
        )?;
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
        let declared_owner = operation.spec.owner.authority();
        if declared_owner != owner {
            return Err(Error::NotFound(format!("operation {operation_id}")));
        }
        operation.spec.verify_owner(owner, verifier)?;
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

    /// Compares the empty execution journal at the scheduler release boundary.
    pub(crate) async fn suspend_if_execution_idle(
        &mut self,
        key: IdempotencyKey,
        event: SchedulerEvent,
        execution: OperationId,
        expected_tail: u64,
    ) -> Result<CoordinatorApply> {
        if !matches!(event, SchedulerEvent::WorkflowSuspended { .. }) {
            return Err(Error::Invalid(
                "idle execution guard requires suspension".into(),
            ));
        }
        self.apply_internal_guarded(
            event.operation_id(),
            key,
            event,
            None,
            None,
            Some((execution, expected_tail)),
        )
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
            None,
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
        if !scope.capabilities().contains(capability::OPERATION_DECLARE) {
            return Err(Error::Unauthorized("scope lacks operation:declare".into()));
        }
        let declared_owner = spec.owner.authority();
        if declared_owner != owner {
            return Err(Error::Unauthorized(
                "declaration owner does not match authenticated owner".into(),
            ));
        }
        spec.verify_owner(owner, verifier)?;
        if let Some(parent) = &spec.parent {
            self.refresh().await?;
            self.authorize_operation(
                owner,
                scope,
                verifier,
                parent.operation_id,
                capability::OPERATION_DECLARE,
            )?;
        }
        let operation_id = spec.operation_id;
        let event = SchedulerEvent::Declared {
            spec: Box::new(spec),
        };
        self.apply_internal_guarded(
            operation_id,
            idempotency_key,
            event,
            parent_fence,
            None,
            None,
        )
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
        spec.verify_owner(owner, verifier)?;
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
        if self.session_intent(&keys, &events).await? {
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
                if self.session_intent(&keys, &events).await? {
                    return Ok(CoordinatorApply::Replayed);
                }
                return Err(Error::Conflict("session retry identity reused".into()));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        self.finish_session_append(outcome, &keys, &events, revision - 2)
            .await
    }

    async fn session_intent(
        &mut self,
        keys: &[String; 2],
        events: &[SchedulerEvent; 2],
    ) -> Result<bool> {
        let mut retained = 0;
        let mut previous: Option<(u64, acyclic_stream::CommitId)> = None;
        for (key, event) in keys.iter().zip(events) {
            if let Some((digest, existing, revision, commit_id)) = self.retained_intent(key).await?
            {
                if &existing != event
                    || &digest
                        != blake3::hash(&crate::contract::canonical_json_bytes(event)?).as_bytes()
                {
                    return Err(Error::Conflict("session retry identity reused".into()));
                }
                if let Some((prior, envelope)) = previous
                    && (prior.checked_add(1) != Some(revision) || envelope != commit_id)
                {
                    return Err(Error::Conflict(
                        "session declaration is not one atomic envelope".into(),
                    ));
                }
                previous = Some((revision, commit_id));
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
        if !self.session_intent(keys, events).await? {
            return Err(Error::Conflict(
                "session append conflicted without its complete intent".into(),
            ));
        }
        if let Some((envelope, start)) = receipt
            && self
                .retained_intent(&keys[0])
                .await?
                .is_none_or(|(_, _, revision, commit_id)| {
                    commit_id != envelope || revision.checked_sub(1) != Some(start)
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
        self.apply_internal_guarded(operation_id, idempotency_key, event, None, None, None)
            .await
    }

    async fn apply_internal_guarded(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
        parent_fence: Option<LeaseFence>,
        waiting_command: Option<OperationId>,
        idle_execution: Option<(OperationId, u64)>,
    ) -> Result<CoordinatorApply> {
        self.refresh().await?;
        self.require_parent_owner(&event, parent_fence.as_ref())?;
        IdempotencyKey::new(idempotency_key.0.clone())?;
        let key = idempotency_key.as_str();
        if event.operation_id() != operation_id {
            return Err(Error::Invalid(
                "scheduler event belongs to another operation".into(),
            ));
        }
        let canonical = crate::contract::canonical_json_bytes(&event)?;
        let digest = *blake3::hash(&canonical).as_bytes();
        if let Some((existing_digest, existing, ..)) = self.retained_intent(key).await? {
            return if existing_digest == digest && existing == event {
                // A retained accounting receipt is not an execution permit.
                // Revalidate the exact owner and cancellation state on retry.
                self.require_command_wait(&event, waiting_command, true)?;
                self.replay_intent(&event)
            } else {
                Err(Error::Conflict("coordinator retry identity reused".into()))
            };
        }
        self.require_command_wait(&event, waiting_command, false)?;
        if let SchedulerEvent::Declared { spec } = &event {
            self.content_verifier.verify(&spec.state).await?;
        }
        self.verify_published_result(&event).await?;
        let mut projected = self.scheduler.clone();
        projected.apply(event.clone())?;
        let revision = next_revision(self.revision)?;
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
        } else if let Some((execution, expected_tail)) = idle_execution {
            stream_key(&format!(
                "{key}:idle-execution:{execution}:{expected_tail}:{}",
                self.revision
            ))?
        } else {
            stream_key(key)?
        };
        let outcome = if let Some((execution, expected_tail)) = idle_execution {
            Box::pin(append_if_execution_idle(
                &self.client,
                &self.stream,
                Bytes::copy_from_slice(&bytes),
                self.revision,
                stream_key,
                operation_id,
                execution,
                expected_tail,
            ))
            .await?
        } else {
            append_keyed(
                (&self.stream, &self.client),
                Bytes::copy_from_slice(&bytes),
                Some(self.revision),
                stream_key,
                operation_id,
                "coordinator retry identity",
            )
            .await?
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
        replaying: bool,
    ) -> Result<()> {
        let Some(waiting) = waiting else {
            return Ok(());
        };
        let SchedulerEvent::WorkflowResumed {
            operation_id,
            workflow_revision,
            input,
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
        if (!replaying && operation.phase != crate::scheduler::OperationPhase::Suspended)
            || operation.phase == crate::scheduler::OperationPhase::Terminal
            || operation.cancellation_requested
            || operation.workflow.as_ref().is_none_or(|slot| {
                slot.revision != *workflow_revision
                    || slot.waiting_command != Some(waiting)
                    // A committed wake fills this exact slot. Its retained
                    // receipt may be observed again, but cannot wake a later
                    // wait or substitute another input after a lost reply.
                    || slot.input.as_ref() != replaying.then_some(input)
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
                match self.retained_intent(key).await? {
                    Some((committed_digest, committed, ..))
                        if &committed_digest == digest && &committed == event =>
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
                match self.retained_intent(key).await? {
                    Some((committed_digest, committed, ..))
                        if &committed_digest == digest && &committed == event =>
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

    fn next_committed_at_ms(&self) -> Result<u64> {
        let after_previous = self
            .last_committed_at_ms
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("coordinator commit time exhausted".into()))?;
        Ok(current_time_millis()?.max(after_previous))
    }

    /// Pulls and atomically admits one dependency- and resource-ready operation.
    pub async fn pull(&mut self, worker: &Worker) -> Result<Option<WorkLease>> {
        match self.pull_for(worker, None, None).await? {
            WorkPull::Idle => Ok(None),
            WorkPull::Claimed(lease) => Ok(Some(lease)),
            WorkPull::Unresolved { error, .. } => Err(error),
        }
    }

    /// Pulls only operations belonging to the verified owner. Shared resource
    /// accounting and tail fencing use the same scheduler and publication path.
    /// An uncertain admission returns its exact attempt instead of losing the
    /// reservation identity in an error-only result.
    pub async fn pull_owned(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        worker: &Worker,
    ) -> Result<WorkPull> {
        self.pull_owned_target(owner, scope, verifier, worker, None)
            .await
    }

    /// Claims only the exact original operation through the existing scheduler.
    /// An idle or uncertain target never falls back to another owner's task.
    pub async fn pull_owned_operation(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        worker: &Worker,
        operation_id: OperationId,
    ) -> Result<WorkPull> {
        self.pull_owned_target(owner, scope, verifier, worker, Some(operation_id))
            .await
    }

    async fn pull_owned_target(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        worker: &Worker,
        operation_id: Option<OperationId>,
    ) -> Result<WorkPull> {
        verifier.verify_audience(owner)?;
        verifier.verify(scope)?;
        if !scope.capabilities().contains("operation:observe")
            || !scope.capabilities().contains("operation:declare")
        {
            return Err(Error::Unauthorized(
                "owned pull requires operation:declare and operation:observe".into(),
            ));
        }
        self.pull_for(worker, Some((owner, verifier)), operation_id)
            .await
    }

    fn verify_pull_owner(
        &self,
        authorization: Option<(&Authority, &AuthorityVerifier)>,
        operation_id: OperationId,
    ) -> Result<()> {
        if let Some((owner, verifier)) = authorization {
            self.scheduler
                .operation(operation_id)
                .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?
                .spec
                .verify_owner(owner, verifier)?;
        }
        Ok(())
    }

    async fn pull_for(
        &mut self,
        worker: &Worker,
        authorization: Option<(&Authority, &AuthorityVerifier)>,
        target: Option<OperationId>,
    ) -> Result<WorkPull> {
        let owner = authorization.map(|(owner, _)| owner);
        self.refresh().await?;
        if worker.id.trim().is_empty() {
            return Err(Error::Invalid("worker identity is empty".into()));
        }
        if let Some(target) = target {
            let state = self
                .scheduler
                .operation(target)
                .filter(|state| owner.is_some_and(|owner| state.spec.owner.authority() == owner))
                .ok_or_else(|| Error::NotFound(format!("operation {target}")))?;
            self.verify_pull_owner(authorization, target)?;
            // A partial reservation is already an owned attempt. Neither a fresh
            // admission nor dependency rejection may replace or release it.
            if state.reservation.is_some() || state.cancellation_requested {
                return Ok(WorkPull::Idle);
            }
        }
        let blocked = match target {
            Some(target) => self
                .scheduler
                .operation_blocked_by_dependencies(target, owner)
                .then_some(target),
            None => self.scheduler.next_blocked_by_dependencies(owner),
        };
        if let Some(operation_id) = blocked {
            self.verify_pull_owner(authorization, operation_id)?;
            self.apply(
                operation_id,
                IdempotencyKey::new(format!("dependency-rejected:{operation_id}"))?,
                SchedulerEvent::Rejected {
                    operation_id,
                    reason: "a required dependency did not succeed".into(),
                },
            )
            .await?;
            return Ok(WorkPull::Idle);
        }
        let available = self.scheduler.available_for(&worker.id, &worker.available);
        let selected = match target {
            Some(target) => self
                .scheduler
                .operation_ready_for(target, &available, &worker.labels, owner)
                .then_some(target),
            None => self
                .scheduler
                .next_ready_for(&available, &worker.labels, owner),
        };
        let Some(operation_id) = selected else {
            return Ok(WorkPull::Idle);
        };
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?
            .clone();
        self.verify_pull_owner(authorization, operation_id)?;
        let operation = state.spec.clone();
        let revision = next_revision(self.revision)?;
        let reservation = Reservation {
            id: format!("{}:{operation_id}:{revision}", worker.id),
            placement: worker.id.clone(),
            admitted: operation.resources.clone(),
        };
        let mut lease = WorkLease {
            operation,
            reservation: reservation.clone(),
            checkpoint: state.checkpoint,
            operation_revision: state
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::Storage("operation revision exhausted".into()))?,
        };
        if let Err(error) = self
            .apply(
                operation_id,
                IdempotencyKey::new(format!("pull:{}:{operation_id}:{}", worker.id, revision))?,
                SchedulerEvent::Admitted {
                    operation_id,
                    reservation: reservation.clone(),
                },
            )
            .await
        {
            return Ok(WorkPull::Unresolved { lease, error });
        }
        lease.operation_revision = self
            .scheduler
            .operation(operation_id)
            .map_or(0, |value| value.revision);
        Ok(WorkPull::Claimed(lease))
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
        if self
            .replay_orchestration(parent, idempotency_key.as_str(), false)
            .await?
        {
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
        if self
            .replay_orchestration(parent, idempotency_key.as_str(), true)
            .await?
        {
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
        let value: serde_json::Value =
            crate::contract::json_from_slice(&bytes).map_err(|error| {
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

    async fn replay_orchestration(
        &mut self,
        parent: OperationId,
        key: &str,
        reduced: bool,
    ) -> Result<bool> {
        let Some((_, event, ..)) = self.retained_intent(key).await? else {
            return Ok(false);
        };
        if let SchedulerEvent::Orchestrated {
            operation_id,
            reducer,
            ..
        } = event
            && operation_id == parent
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
        if revision != next_revision(self.revision)? || event.operation_id() != operation_id {
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
        self.cache_intent(key, (digest, event, revision, commit_id));
        Ok(())
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
    validate_protocol(
        envelope.protocol.as_ref(),
        &coordinator_protocol_identity(),
        Error::Storage,
    )?;
    let digest: [u8; 32] = envelope
        .event_digest
        .try_into()
        .map_err(|_| Error::Storage("scheduler digest must be 32 bytes".into()))?;
    if *blake3::hash(&envelope.canonical_event_json).as_bytes() != digest {
        return Err(Error::Storage("scheduler event digest mismatch".into()));
    }
    let event = crate::contract::json_from_slice(&envelope.canonical_event_json)
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

/// Appends one keyed record. An unavailable reply is resolved from the
/// Stream's retained idempotency outcome, or reported as indeterminate.
pub(crate) async fn append_keyed<P: StreamProvider>(
    (stream, client): (&Stream<P>, &StreamClient<P>),
    record: Bytes,
    if_tail: Option<u64>,
    key: StreamIdempotencyKey,
    operation_id: OperationId,
    identity: &str,
) -> Result<AppendOutcome> {
    match stream
        .append_batch(vec![record], if_tail, Some(key.clone()))
        .await
    {
        Ok(outcome) => Ok(outcome),
        Err(StreamError::Unavailable) => match client.inspect_idempotency(key).await {
            Ok(Some(observation)) => match observation.outcome {
                IdempotencyOutcome::Append(outcome) => Ok(outcome),
                _ => Err(Error::Conflict(format!(
                    "{identity} has another operation kind"
                ))),
            },
            Ok(None) | Err(_) => Err(Error::Indeterminate(operation_id)),
        },
        Err(StreamError::IdempotencyMismatch) => {
            Err(Error::Conflict(format!("{identity} was reused")))
        }
        Err(error) => Err(Error::Storage(error.to_string())),
    }
}

pub(crate) fn execution_path(operation: OperationId) -> Result<acyclic_stream::StreamPath> {
    Ok(acyclic_stream::StreamPath::new(format!(
        "harness/v2/execution/{operation}"
    ))?)
}

pub(crate) async fn commit_keyed<P: StreamProvider>(
    client: &StreamClient<P>,
    request: acyclic_stream::CommitRequest,
    operation: OperationId,
) -> Result<acyclic_stream::CommitOutcome> {
    let key = request.idempotency_key.clone();
    match client.commit(request).await {
        Ok(outcome) => Ok(outcome),
        Err(StreamError::Unavailable) => match client.inspect_idempotency(key).await {
            Ok(Some(observation)) => match observation.outcome {
                IdempotencyOutcome::Commit(outcome) => Ok(outcome),
                _ => Err(Error::Conflict("commit recovery identity reused".into())),
            },
            Ok(None) | Err(_) => Err(Error::Indeterminate(operation)),
        },
        Err(error) => Err(error.into()),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one coordinator append with an exact execution condition"
)]
async fn append_if_execution_idle<P: StreamProvider>(
    client: &StreamClient<P>,
    stream: &Stream<P>,
    record: Bytes,
    tail: u64,
    key: StreamIdempotencyKey,
    operation: OperationId,
    execution: OperationId,
    expected_tail: u64,
) -> Result<AppendOutcome> {
    use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommittedMutation};
    let path = execution_path(execution)?;
    let guard = match client.bounds(path.as_str()).await {
        Ok(bounds) if bounds.tail == expected_tail => CommitCondition::Tail {
            path,
            expected: expected_tail,
        },
        Ok(_) => {
            return Err(Error::Conflict(
                "execution changed before suspension".into(),
            ));
        }
        Err(StreamError::NotFound) if expected_tail == 0 => CommitCondition::Absent { path },
        Err(error) => return Err(error.into()),
    };
    let mut digest = blake3::Hasher::new();
    digest.update(b"harness/v2/idle-execution\0");
    digest.update(key.as_bytes());
    digest.update(&[u8::from(matches!(guard, CommitCondition::Absent { .. }))]);
    let outcome = commit_keyed(
        client,
        acyclic_stream::CommitRequest {
            conditions: vec![
                CommitCondition::Tail {
                    path: stream.path().clone(),
                    expected: tail,
                },
                guard,
            ],
            mutations: vec![CommitMutation::Append {
                path: stream.path().clone(),
                records: vec![record.clone()],
            }],
            idempotency_key: StreamIdempotencyKey::new(Bytes::copy_from_slice(
                digest.finalize().as_bytes(),
            ))?,
        },
        operation,
    )
    .await?;
    let CommitOutcome::Committed(envelope) = outcome else {
        return Err(Error::Conflict(
            "execution or coordinator changed before suspension".into(),
        ));
    };
    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
        return Err(Error::Storage("invalid guarded suspension receipt".into()));
    };
    if append.path != *stream.path()
        || append.start != tail
        || append.end != next_revision(tail)?
        || append.tail != append.end
        || append.records.len() != 1
        || append.records.first().is_none_or(|entry| {
            entry.sequence != tail || entry.value != record || entry.commit_id != envelope.commit_id
        })
    {
        return Err(Error::Storage(
            "guarded suspension differs from its commit".into(),
        ));
    }
    Ok(AppendOutcome::Committed(acyclic_stream::AppendReceipt {
        start: append.start,
        end: append.end,
        tail: append.tail,
        commit_id: envelope.commit_id,
    }))
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
    type LostSessionAck = crate::test_stream::LostSessionAck<acyclic_stream::MemoryStream>;
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

    #[tokio::test]
    async fn declaration_point_observation_is_read_only_bounded_and_source_verified() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(LostSessionAck::default());
        let client = StreamClient::new(provider.clone());
        let operation = OperationId::from_bytes([1; 16]);
        let declaration = spec(operation, 0)?;
        let owner = declaration.owner.authority().clone();
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let scope = issuer.root("observe", Capabilities::new(["operation:observe"]));
        let verifier = issuer.verifier();
        let key = IdempotencyKey::new("point-declaration")?;
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        declare(&mut coordinator, declaration.clone(), key.as_str()).await?;
        coordinator
            .apply(
                operation,
                IdempotencyKey::new("point-cancel")?,
                SchedulerEvent::CancellationRequested {
                    operation_id: operation,
                    recursive: false,
                },
            )
            .await?;
        // Evict the declaration from the existing coordinator's retry cache.
        for value in 2..82 {
            declare(
                &mut coordinator,
                spec(OperationId::from_bytes([value; 16]), 0)?,
                &format!("point-other-{value}"),
            )
            .await?;
        }
        assert!(!coordinator.intents.contains_key(key.as_str()));
        let location = coordinator
            .read_intent_location(key.as_str())
            .await?
            .ok_or_else(|| Error::NotFound("point location".into()))?;
        drop(coordinator);
        provider.forbid_writes.store(true, Ordering::SeqCst);
        // No coordinator is opened, no lifecycle map is reconstructed, and the
        // immutable declaration remains metadata after cancellation.
        assert_eq!(
            observe_declaration(&client, &owner, &scope, &verifier, operation, &key).await?,
            Some(declaration)
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 2);
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        let denied = issuer.root("denied", Capabilities::default());
        assert!(matches!(
            observe_declaration(&client, &owner, &denied, &verifier, operation, &key).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        assert!(matches!(
            observe_declaration(
                &client,
                &owner,
                &scope,
                &verifier,
                OperationId::from_bytes([99; 16]),
                &key
            )
            .await,
            Err(Error::Conflict(_))
        ));
        let foreign = Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        };
        let foreign_issuer = AuthorityIssuer::new("foreign-observer", [7; 32], foreign.clone());
        let foreign_scope =
            foreign_issuer.root("observe", Capabilities::new(["operation:observe"]));
        assert!(matches!(
            observe_declaration(
                &client,
                &foreign,
                &foreign_scope,
                &foreign_issuer.verifier(),
                operation,
                &key
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        let missing = IdempotencyKey::new("point-missing")?;
        assert_eq!(
            observe_declaration(&client, &owner, &scope, &verifier, operation, &missing).await?,
            None
        );
        assert!(matches!(
            observe_declaration(
                &client,
                &owner,
                &scope,
                &verifier,
                operation,
                &IdempotencyKey::new("point-cancel")?
            )
            .await,
            Err(Error::Conflict(_))
        ));
        // Extending the immutable location is rejected, even when the original
        // coordinator declaration remains intact.
        provider.forbid_writes.store(false, Ordering::SeqCst);
        client
            .stream(intent_location_path(key.as_str()))?
            .append_batch(
                vec![Bytes::from(crate::contract::canonical_json_bytes(
                    &location,
                )?)],
                Some(1),
                None,
            )
            .await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        assert!(matches!(
            observe_declaration(&client, &owner, &scope, &verifier, operation, &key).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn intent_locations_rebuild_legacy_history_and_replay_evicted_atomic_session()
    -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let root = OperationId::from_bytes([1; 16]);
        let root_spec = spec(root, 0)?;
        let limits = crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 2,
            depth: 1,
            model_steps: 1,
        };
        let key = "legacy-atomic-root";
        let config_key = format!("session:{root}:{}", blake3::hash(key.as_bytes()).to_hex());
        let events = [
            SchedulerEvent::Declared {
                spec: Box::new(root_spec.clone()),
            },
            SchedulerEvent::SessionConfigured {
                operation_id: root,
                limits,
            },
        ];
        let mut records = Vec::new();
        for (offset, (intent_key, event)) in [key, config_key.as_str()]
            .into_iter()
            .zip(&events)
            .enumerate()
        {
            let canonical = crate::contract::canonical_json_bytes(event)?;
            records.push(Bytes::from(encode(
                offset as u64 + 1,
                root,
                intent_key,
                *blake3::hash(&canonical).as_bytes(),
                canonical,
                offset as u64 + 10,
            )));
        }
        let source = client.stream(COORDINATOR_PATH)?;
        source
            .append_batch(records, Some(0), Some(stream_key(key)?))
            .await?;
        assert!(client.bounds(&intent_location_path(key)).await.is_err());
        // Concurrent rebuilds retain one exact location without semantic writes.
        let (first, second) = tokio::join!(
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)),
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier))
        );
        let mut coordinator = first?;
        let second = second?;
        assert_eq!(coordinator.scheduler(), second.scheduler());
        assert_eq!(client.bounds(&intent_location_path(key)).await?.tail, 1);
        for identity in 2..=81u8 {
            declare(
                &mut coordinator,
                spec(OperationId::from_bytes([identity; 16]), 0)?,
                &format!("intent-eviction-{identity}"),
            )
            .await?;
            assert!(coordinator.intents.len() <= MAX_CACHED_INTENTS);
        }
        assert!(!coordinator.intents.contains_key(key));
        assert!(!coordinator.intents.contains_key(&config_key));
        assert_eq!(source.bounds().await?.tail, 82);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let scope = issuer.root(
            "declare",
            Capabilities::new(["operation:declare", "operation:observe"]),
        );
        let verifier = issuer.verifier();
        assert_eq!(
            coordinator
                .declare_session(
                    &owner,
                    &scope,
                    &verifier,
                    root_spec.clone(),
                    limits,
                    IdempotencyKey::new(key)?
                )
                .await?,
            CoordinatorApply::Replayed
        );
        assert!(
            coordinator
                .declare_session(
                    &owner,
                    &scope,
                    &verifier,
                    root_spec.clone(),
                    crate::scheduler::SessionLimits {
                        model_steps: 2,
                        ..limits
                    },
                    IdempotencyKey::new(key)?
                )
                .await
                .is_err()
        );
        assert_eq!(source.bounds().await?.tail, 82);
        drop(coordinator);
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(reopened.intents.len(), MAX_CACHED_INTENTS);
        assert_eq!(
            reopened
                .declare_session(
                    &owner,
                    &scope,
                    &verifier,
                    root_spec,
                    limits,
                    IdempotencyKey::new(key)?
                )
                .await?,
            CoordinatorApply::Replayed
        );
        assert_eq!(source.bounds().await?.tail, 82);
        // A duplicate outside the cache is still invalid durable history, even
        // when its event would merely repeat identical configured limits.
        let canonical = crate::contract::canonical_json_bytes(&events[1])?;
        source
            .append_batch(
                vec![Bytes::from(encode(
                    83,
                    root,
                    &config_key,
                    *blake3::hash(&canonical).as_bytes(),
                    canonical,
                    reopened.last_committed_at_ms + 1,
                ))],
                Some(82),
                None,
            )
            .await?;
        assert!(matches!(reopened.refresh().await, Err(Error::Conflict(_))));
        assert_eq!(reopened.revision, 82);
        Ok(())
    }

    #[tokio::test]
    async fn intent_locations_reject_forged_or_extended_projection_records() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let operation = OperationId::from_bytes([87; 16]);
        declare(&mut coordinator, spec(operation, 0)?, "projection-source").await?;
        let source = client.stream(COORDINATOR_PATH)?;
        let location = IntentLocation {
            key: "forged-location".into(),
            revision: 1,
            event_digest: [1; 32],
            commit_id: [2; 32],
        };
        client
            .stream(intent_location_path(&location.key))?
            .append_batch(
                vec![Bytes::from(crate::contract::canonical_json_bytes(
                    &location,
                )?)],
                Some(0),
                None,
            )
            .await?;
        assert!(matches!(
            coordinator.retained_intent(&location.key).await,
            Err(Error::Storage(_))
        ));
        let valid = coordinator
            .read_intent_location("projection-source")
            .await?
            .ok_or_else(|| Error::NotFound("valid intent location".into()))?;
        client
            .stream(intent_location_path(&valid.key))?
            .append_batch(
                vec![Bytes::from(crate::contract::canonical_json_bytes(&valid)?)],
                Some(1),
                None,
            )
            .await?;
        assert!(
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier))
                .await
                .is_err()
        );
        assert_eq!(source.bounds().await?.tail, 1);
        Ok(())
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
            let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
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
        for (cancel, lost_ack) in [
            (false, None),
            (true, None),
            (false, Some(false)),
            (false, Some(true)),
        ] {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
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
            let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
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
            if let Some(hidden) = lost_ack {
                provider
                    .lose_ack
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                provider
                    .hide_receipt
                    .store(hidden, std::sync::atomic::Ordering::SeqCst);
            }
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
                if lost_ack == Some(true) {
                    assert!(matches!(retry, Err(Error::Indeterminate(id)) if id == task));
                    drop(first);
                    first = DistributedCoordinator::open(&client, gate.clone()).await?;
                    let operation = first
                        .scheduler()
                        .operation(task)
                        .ok_or_else(|| Error::NotFound("published wake".into()))?;
                    assert_eq!(
                        operation.phase,
                        crate::scheduler::OperationPhase::WaitingForCapacity
                    );
                    assert!(operation.reservation.is_none());
                    assert_eq!(
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
                            .await?,
                        CoordinatorApply::Replayed
                    );
                } else {
                    assert_eq!(retry?, CoordinatorApply::Applied);
                    assert_eq!(
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
                            .await?,
                        CoordinatorApply::Replayed
                    );
                }
                assert!(
                    first
                        .resume_command(
                            &owner,
                            &signed,
                            &verifier,
                            task,
                            1,
                            input.clone(),
                            IdempotencyKey::new("another-wake")?,
                            selected
                        )
                        .await
                        .is_err()
                );
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
                            input.clone(),
                            IdempotencyKey::new("second-selected-wake")?,
                            next
                        )
                        .await?,
                    CoordinatorApply::Applied
                );
                first
                    .apply(
                        task,
                        IdempotencyKey::new("cancel-published-wake")?,
                        SchedulerEvent::CancellationRequested {
                            operation_id: task,
                            recursive: false,
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
                            input,
                            IdempotencyKey::new("second-selected-wake")?,
                            next
                        )
                        .await
                        .is_err()
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

    fn declaration_scope(owner: &Authority) -> Scope {
        AuthorityIssuer::new("test-runtime", [9; 32], owner.clone())
            .root("original-declare", Capabilities::new(["operation:declare"]))
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
            owner_scope: AuthorityIssuer::new(
                "test-runtime",
                [9; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "owner".into(),
                },
            )
            .root("original-declare", Capabilities::new(["operation:declare"])),
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
        let owner = spec.owner.authority().clone();
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
    async fn original_owner_proof_survives_reopen_and_rejects_replaced_key() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let operation_id = OperationId::from_bytes([91; 16]);
        let original = spec(operation_id, 0)?;
        let owner = original.owner.authority().clone();
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let renewed = issuer.root(
            "renewed-current-grant",
            Capabilities::new(["operation:declare", "operation:observe", "operation:cancel"]),
        );
        let key = IdempotencyKey::new("original-owner-proof")?;
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        declare(&mut coordinator, original.clone(), key.as_str()).await?;
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let replaced = AuthorityIssuer::new("test-runtime", [8; 32], owner.clone());
        let replacement = replaced.root("renewed-current-grant", renewed.capabilities().clone());
        assert!(matches!(
            observe_declaration(
                &client,
                &owner,
                &replacement,
                &replaced.verifier(),
                operation_id,
                &key
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            reopened.observe_operation(&owner, &replacement, &replaced.verifier(), operation_id),
            Err(Error::Unauthorized(_))
        ));
        let worker = Worker {
            id: "owner-proof-worker".into(),
            available: ResourceSnapshot::default(),
            labels: BTreeMap::new(),
        };
        let revision = reopened.revision();
        assert!(matches!(
            reopened
                .pull_owned_operation(
                    &owner,
                    &replacement,
                    &replaced.verifier(),
                    &worker,
                    operation_id
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            reopened
                .pull_owned(&owner, &replacement, &replaced.verifier(), &worker)
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(reopened.revision(), revision);
        assert!(
            reopened
                .scheduler()
                .operation(operation_id)
                .is_some_and(|state| state.reservation.is_none())
        );
        assert_eq!(
            observe_declaration(
                &client,
                &owner,
                &renewed,
                &issuer.verifier(),
                operation_id,
                &key
            )
            .await?,
            Some(original.clone())
        );
        assert_eq!(
            reopened
                .declare_operation(&owner, &renewed, &issuer.verifier(), original, key)
                .await?,
            CoordinatorApply::Replayed
        );
        assert!(matches!(
            reopened
                .pull_owned_operation(&owner, &renewed, &issuer.verifier(), &worker, operation_id)
                .await?,
            WorkPull::Claimed(_)
        ));
        Ok(())
    }

    #[test]
    fn original_owner_proof_is_mandatory_and_authenticated() -> Result<()> {
        let original = spec(OperationId::from_bytes([92; 16]), 0)?;
        let owner = original.owner.authority();
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        original.verify_owner(owner, &issuer.verifier())?;
        let foreign = Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        };
        let wrong_audience = AuthorityIssuer::new("test-runtime", [9; 32], foreign.clone());
        assert!(matches!(
            original.verify_owner(owner, &wrong_audience.verifier()),
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            original.verify_owner(&foreign, &issuer.verifier()),
            Err(Error::Unauthorized(_))
        ));
        let mut json =
            serde_json::to_value(&original).map_err(|error| Error::Invalid(error.to_string()))?;
        json.as_object_mut()
            .ok_or_else(|| Error::Invalid("spec object".into()))?
            .remove("owner_scope");
        assert!(serde_json::from_value::<OperationSpec>(json).is_err());
        let mut json =
            serde_json::to_value(&original).map_err(|error| Error::Invalid(error.to_string()))?;
        json["owner_scope"]["id"] = serde_json::json!("tampered-original-grant");
        let tampered: OperationSpec =
            serde_json::from_value(json).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(matches!(
            tampered.verify_owner(owner, &issuer.verifier()),
            Err(Error::Unauthorized(_))
        ));
        let mut insufficient = original.clone();
        insufficient.owner_scope =
            issuer.root("observe-only", Capabilities::new(["operation:observe"]));
        assert!(matches!(
            insufficient.verify_owner(owner, &issuer.verifier()),
            Err(Error::Unauthorized(_))
        ));
        Ok(())
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
        foreign_child.owner_scope = declaration_scope(foreign_child.owner.authority());
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
        validate_child_page_request(parent, None, usize::MAX)?;
        assert!(validate_child_page_request(parent, Some("\u{7f}"), 1).is_err());
        validate_child_page_request(parent, Some(&"x".repeat(1_024)), 1)?;
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
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
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
            owner_scope: AuthorityIssuer::new(
                "test-runtime",
                [9; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "owner".into(),
                },
            )
            .root("original-declare", Capabilities::new(["operation:declare"])),
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
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
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

    #[tokio::test]
    async fn owned_pull_filters_foreign_operations_but_shares_worker_capacity() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let foreign = OperationId::from_bytes([1; 16]);
        let owned = OperationId::from_bytes([2; 16]);
        let mut other = spec(foreign, 1)?;
        other.owner = DurableOwner::Attached {
            authority: Authority {
                kind: AggregateKind::Task,
                id: "foreign".into(),
            },
        };
        other.owner_scope = declaration_scope(other.owner.authority());
        declare(&mut coordinator, other, "foreign-pull").await?;
        declare(&mut coordinator, spec(owned, 1)?, "owned-pull").await?;
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let scope = issuer.root(
            "admit",
            Capabilities::new(["operation:observe", "operation:declare"]),
        );
        let worker = Worker {
            id: "shared-worker".into(),
            available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
            labels: BTreeMap::new(),
        };
        let WorkPull::Claimed(lease) = coordinator
            .pull_owned(&owner, &scope, &issuer.verifier(), &worker)
            .await?
        else {
            return Err(Error::NotFound("owned pull".into()));
        };
        assert_eq!(lease.operation.operation_id, owned);
        assert!(
            coordinator
                .scheduler()
                .operation(foreign)
                .is_some_and(|state| state.reservation.is_none())
        );
        assert!(matches!(
            coordinator
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker)
                .await?,
            WorkPull::Idle
        ));
        assert!(coordinator.pull(&worker).await?.is_none());
        let fence = LeaseFence::from(&lease.reservation);
        coordinator
            .apply(
                owned,
                IdempotencyKey::new("owned-start")?,
                SchedulerEvent::Started {
                    operation_id: owned,
                    fence: fence.clone(),
                },
            )
            .await?;
        coordinator
            .apply(
                owned,
                IdempotencyKey::new("owned-failed")?,
                SchedulerEvent::Completed {
                    operation_id: owned,
                    fence: Some(fence),
                    execution_duration_ns: None,
                    outcome: crate::Outcome::Failed {
                        message: "dependency failed".into(),
                    },
                },
            )
            .await?;
        let blocked = OperationId::from_bytes([3; 16]);
        let mut other = spec(blocked, 1)?;
        other.owner = DurableOwner::Attached {
            authority: Authority {
                kind: AggregateKind::Task,
                id: "foreign".into(),
            },
        };
        other.owner_scope = declaration_scope(other.owner.authority());
        other.dependencies.insert(owned);
        declare(&mut coordinator, other, "foreign-blocked").await?;
        assert!(matches!(
            coordinator
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker)
                .await?,
            WorkPull::Idle
        ));
        assert!(
            coordinator
                .scheduler()
                .operation(blocked)
                .is_some_and(|state| state.phase != crate::scheduler::OperationPhase::Terminal)
        );
        let before = client.stream(COORDINATOR_PATH)?.tail().await?;
        let denied = issuer.root("denied", Capabilities::new(["operation:observe"]));
        assert!(matches!(
            coordinator
                .pull_owned(&owner, &denied, &issuer.verifier(), &worker)
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, before);
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one exact-target admission and unrelated-task mutation control"
    )]
    async fn exact_target_pull_never_claims_or_rejects_an_unrelated_task() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let first = OperationId::from_bytes([61; 16]);
        let target = OperationId::from_bytes([62; 16]);
        declare(&mut coordinator, spec(first, 1)?, "target-first").await?;
        declare(&mut coordinator, spec(target, 1)?, "target-second").await?;
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let scope = issuer.root(
            "admit",
            Capabilities::new(["operation:observe", "operation:declare"]),
        );
        let worker = Worker {
            id: "target-worker".into(),
            available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
            labels: BTreeMap::new(),
        };
        let first_before = coordinator.scheduler().operation(first).cloned();
        let WorkPull::Claimed(lease) = coordinator
            .pull_owned_operation(&owner, &scope, &issuer.verifier(), &worker, target)
            .await?
        else {
            return Err(Error::NotFound("target admission".into()));
        };
        assert_eq!(lease.operation.operation_id, target);
        assert_eq!(
            coordinator.scheduler().operation(first),
            first_before.as_ref()
        );
        let tail = client.stream(COORDINATOR_PATH)?.tail().await?;
        assert!(matches!(
            coordinator
                .pull_owned_operation(&owner, &scope, &issuer.verifier(), &worker, target)
                .await?,
            WorkPull::Idle
        ));
        assert!(matches!(
            coordinator
                .pull_owned_operation(&owner, &scope, &issuer.verifier(), &worker, first)
                .await?,
            WorkPull::Idle
        ));
        assert!(matches!(
            coordinator
                .pull_owned_operation(
                    &owner,
                    &scope,
                    &issuer.verifier(),
                    &worker,
                    OperationId::from_bytes([63; 16])
                )
                .await,
            Err(Error::NotFound(_))
        ));
        let denied = issuer.root("denied", Capabilities::new(["operation:observe"]));
        assert!(matches!(
            coordinator
                .pull_owned_operation(&owner, &denied, &issuer.verifier(), &worker, first)
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, tail);
        let fence = LeaseFence::from(&lease.reservation);
        coordinator
            .apply(
                target,
                IdempotencyKey::new("target-start")?,
                SchedulerEvent::Started {
                    operation_id: target,
                    fence: fence.clone(),
                },
            )
            .await?;
        coordinator
            .apply(
                target,
                IdempotencyKey::new("target-fail")?,
                SchedulerEvent::Completed {
                    operation_id: target,
                    fence: Some(fence),
                    execution_duration_ns: None,
                    outcome: crate::Outcome::Failed {
                        message: "failed dependency".into(),
                    },
                },
            )
            .await?;
        let blocked = OperationId::from_bytes([64; 16]);
        let ready = OperationId::from_bytes([65; 16]);
        let mut dependent = spec(blocked, 1)?;
        dependent.dependencies.insert(target);
        declare(&mut coordinator, dependent, "target-blocked").await?;
        declare(&mut coordinator, spec(ready, 1)?, "target-ready").await?;
        let blocked_before = coordinator.scheduler().operation(blocked).cloned();
        let WorkPull::Claimed(lease) = coordinator
            .pull_owned_operation(&owner, &scope, &issuer.verifier(), &worker, ready)
            .await?
        else {
            return Err(Error::NotFound("ready target behind blocked task".into()));
        };
        assert_eq!(lease.operation.operation_id, ready);
        assert_eq!(
            coordinator.scheduler().operation(blocked),
            blocked_before.as_ref()
        );
        assert_eq!(
            coordinator.scheduler().operation(first),
            first_before.as_ref()
        );
        assert!(matches!(
            coordinator
                .pull_owned_operation(&owner, &scope, &issuer.verifier(), &worker, blocked)
                .await?,
            WorkPull::Idle
        ));
        assert_eq!(
            coordinator
                .scheduler()
                .operation(blocked)
                .expect("blocked")
                .phase,
            crate::scheduler::OperationPhase::Terminal
        );
        assert_eq!(
            coordinator.scheduler().operation(first),
            first_before.as_ref()
        );
        Ok(())
    }

    #[tokio::test]
    async fn exact_target_pull_preserves_live_partial_and_cancelled_reservations() -> Result<()> {
        for cancelled in [false, true] {
            let client = StreamClient::new(Arc::new(MemoryStream::default()));
            let mut coordinator =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let operation = OperationId::from_bytes([68; 16]);
            declare(&mut coordinator, spec(operation, 2)?, "partial-target").await?;
            let reservation = Reservation {
                id: "original-partial".into(),
                placement: "partial-worker".into(),
                admitted: crate::scheduler::ResourceRequest(BTreeMap::from([("cpu".into(), 1)])),
            };
            coordinator
                .apply(
                    operation,
                    IdempotencyKey::new("partial-admit")?,
                    SchedulerEvent::PartiallyAdmitted {
                        operation_id: operation,
                        reservation: reservation.clone(),
                    },
                )
                .await?;
            if cancelled {
                coordinator
                    .apply(
                        operation,
                        IdempotencyKey::new("partial-cancel")?,
                        SchedulerEvent::CancellationRequested {
                            operation_id: operation,
                            recursive: false,
                        },
                    )
                    .await?;
            }
            let before = coordinator.scheduler().operation(operation).cloned();
            let tail = client.stream(COORDINATOR_PATH)?.tail().await?;
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            };
            let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
            let scope = issuer.root(
                "admit",
                Capabilities::new(["operation:observe", "operation:declare"]),
            );
            let worker = Worker {
                id: "partial-worker".into(),
                available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 2)])),
                labels: BTreeMap::new(),
            };
            assert!(matches!(
                coordinator
                    .pull_owned_operation(&owner, &scope, &issuer.verifier(), &worker, operation)
                    .await?,
                WorkPull::Idle
            ));
            assert_eq!(
                coordinator.scheduler().operation(operation),
                before.as_ref()
            );
            assert_eq!(
                coordinator
                    .scheduler()
                    .operation(operation)
                    .expect("partial")
                    .reservation
                    .as_ref(),
                Some(&reservation)
            );
            assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, tail);
        }
        Ok(())
    }

    #[tokio::test]
    async fn owned_pull_retains_exact_attempt_after_admission_ack_or_index_fault() -> Result<()> {
        use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
        for mode in 0..6 {
            let fault = mode % 3;
            let targeted = mode >= 3;
            let provider = Arc::new(LostSessionAck {
                inner: MemoryStream::default(),
                lose_ack: AtomicBool::new(false),
                hide_receipt: AtomicBool::new(false),
                location_fault: AtomicU8::new(0),
                hide_location_read: AtomicBool::new(false),
                ..Default::default()
            });
            let client = StreamClient::new(provider.clone());
            let mut coordinator =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let operation = OperationId::from_bytes([31; 16]);
            declare(&mut coordinator, spec(operation, 1)?, "fault-pull-root").await?;
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            };
            let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
            let scope = issuer.root(
                "admit",
                Capabilities::new(["operation:observe", "operation:declare"]),
            );
            let worker = Worker {
                id: "fault-worker".into(),
                available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
                labels: BTreeMap::new(),
            };
            if fault == 1 {
                provider.lose_ack.store(true, Ordering::SeqCst);
                provider.hide_receipt.store(true, Ordering::SeqCst);
            } else {
                provider
                    .location_fault
                    .store(if fault == 2 { 4 } else { 1 }, Ordering::SeqCst);
            }
            let WorkPull::Unresolved { lease, error } = coordinator
                .pull_owned_target(
                    &owner,
                    &scope,
                    &issuer.verifier(),
                    &worker,
                    targeted.then_some(operation),
                )
                .await?
            else {
                return Err(Error::NotFound("unresolved admission attempt".into()));
            };
            assert!(if fault != 0 {
                matches!(error, Error::Indeterminate(id) if id == operation)
            } else {
                matches!(error, Error::Storage(_))
            });
            assert_eq!(lease.operation.operation_id, operation);
            drop(coordinator);
            let mut reopened =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let retained = reopened
                .scheduler()
                .operation(operation)
                .ok_or_else(|| Error::NotFound("retained admission".into()))?;
            if fault == 2 {
                assert!(retained.reservation.is_none());
                assert!(
                    crate::scheduler::require_execution_owner(
                        retained,
                        &LeaseFence::from(&lease.reservation),
                        false
                    )
                    .is_err()
                );
                let WorkPull::Claimed(retried) = reopened
                    .pull_owned_target(
                        &owner,
                        &scope,
                        &issuer.verifier(),
                        &worker,
                        targeted.then_some(operation),
                    )
                    .await?
                else {
                    return Err(Error::NotFound("uncommitted admission retry".into()));
                };
                assert_eq!(retried, lease);
                continue;
            }
            assert_eq!(retained.reservation.as_ref(), Some(&lease.reservation));
            assert_eq!(retained.revision, lease.operation_revision);
            assert_eq!(retained.phase, crate::scheduler::OperationPhase::Admitted);
            let tail = client.stream(COORDINATOR_PATH)?.tail().await?;
            assert!(matches!(
                reopened
                    .pull_owned_target(
                        &owner,
                        &scope,
                        &issuer.verifier(),
                        &worker,
                        targeted.then_some(operation)
                    )
                    .await?,
                WorkPull::Idle
            ));
            assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, tail);
        }
        Ok(())
    }

    #[tokio::test]
    async fn passive_suspension_atomically_fences_execution_and_recovers_commit_ack() -> Result<()>
    {
        for existing_tail in [None, Some(0), Some(70)] {
            let expected_tail = existing_tail.unwrap_or(0);
            for mode in 0..4 {
                let provider = Arc::new(LostSessionAck::default());
                let client = StreamClient::new(provider.clone());
                let mut coordinator =
                    DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
                let task = OperationId::from_bytes([91; 16]);
                let execution = OperationId::from_bytes([92; 16]);
                declare(&mut coordinator, spec(task, 0)?, "idle-task").await?;
                let lease = coordinator
                    .pull(&Worker {
                        id: "idle-worker".into(),
                        available: ResourceSnapshot::default(),
                        labels: BTreeMap::new(),
                    })
                    .await?
                    .ok_or_else(|| Error::NotFound("idle lease".into()))?;
                let fence = LeaseFence::from(&lease.reservation);
                coordinator
                    .apply(
                        task,
                        IdempotencyKey::new("idle-start")?,
                        SchedulerEvent::Started {
                            operation_id: task,
                            fence: fence.clone(),
                        },
                    )
                    .await?;
                if let Some(tail) = existing_tail {
                    client
                        .stream(COORDINATOR_PATH)?
                        .fork(execution_path(execution)?.as_str(), Some(0), None)
                        .await?;
                    if tail != 0 {
                        client
                            .stream(execution_path(execution)?.as_str())?
                            .append_batch(
                                vec![
                                    Bytes::from_static(b"settled observation");
                                    usize::try_from(tail).map_err(|_| Error::Invalid(
                                        "fixture tail is not representable".into()
                                    ))?
                                ],
                                Some(0),
                                None,
                            )
                            .await?;
                    }
                }
                provider
                    .commit_lose_ack
                    .store(mode == 1 || mode == 2, std::sync::atomic::Ordering::SeqCst);
                provider
                    .hide_receipt
                    .store(mode == 2, std::sync::atomic::Ordering::SeqCst);
                provider
                    .execution_race
                    .store(mode == 3, std::sync::atomic::Ordering::SeqCst);
                let event = SchedulerEvent::WorkflowSuspended {
                    operation_id: task,
                    fence: fence.clone(),
                    workflow_revision: 1,
                    waiting_command: Some(execution),
                };
                let result = coordinator
                    .suspend_if_execution_idle(
                        IdempotencyKey::new("idle-suspend")?,
                        event.clone(),
                        execution,
                        expected_tail,
                    )
                    .await;
                if mode == 3 {
                    assert!(matches!(result, Err(Error::Conflict(_))));
                } else if mode == 2 {
                    assert!(matches!(result, Err(Error::Indeterminate(id)) if id == task));
                } else {
                    assert_eq!(result?, CoordinatorApply::Applied);
                }
                let reopened =
                    DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
                let retained = reopened
                    .scheduler()
                    .operation(task)
                    .ok_or_else(|| Error::NotFound("idle task".into()))?;
                if mode == 3 {
                    assert_eq!(retained.phase, OperationPhase::Running);
                    assert_eq!(retained.reservation.as_ref(), Some(&lease.reservation));
                    assert_eq!(
                        client
                            .stream(execution_path(execution)?.as_str())?
                            .tail()
                            .await?,
                        expected_tail + 1
                    );
                    // Retrying the failed condition cannot turn dispatch into a passive wait.
                    assert!(
                        coordinator
                            .suspend_if_execution_idle(
                                IdempotencyKey::new("idle-suspend")?,
                                event,
                                execution,
                                expected_tail
                            )
                            .await
                            .is_err()
                    );
                } else {
                    assert_eq!(retained.phase, OperationPhase::Suspended);
                    assert!(retained.reservation.is_none());
                    assert_eq!(
                        retained
                            .workflow
                            .as_ref()
                            .and_then(|slot| slot.waiting_command),
                        Some(execution)
                    );
                    // Suspension won: the old coordinator condition cannot dispatch next.
                    let outcome = client
                        .commit(acyclic_stream::CommitRequest {
                            conditions: vec![
                                acyclic_stream::CommitCondition::Tail {
                                    path: acyclic_stream::StreamPath::new(COORDINATOR_PATH)?,
                                    expected: reopened.revision() - 1,
                                },
                                if existing_tail.is_some() {
                                    acyclic_stream::CommitCondition::Tail {
                                        path: execution_path(execution)?,
                                        expected: expected_tail,
                                    }
                                } else {
                                    acyclic_stream::CommitCondition::Absent {
                                        path: execution_path(execution)?,
                                    }
                                },
                            ],
                            mutations: vec![acyclic_stream::CommitMutation::Append {
                                path: execution_path(execution)?,
                                records: vec![Bytes::from_static(b"stale dispatch")],
                            }],
                            idempotency_key: stream_key("stale-idle-dispatch")?,
                        })
                        .await?;
                    assert!(matches!(
                        outcome,
                        acyclic_stream::CommitOutcome::Conflict(_)
                    ));
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn root_limits_reconcile_a_lost_atomic_append_acknowledgement() -> Result<()> {
        for hide_receipt in [false, true] {
            let client = StreamClient::new(Arc::new(LostSessionAck {
                inner: MemoryStream::default(),
                lose_ack: std::sync::atomic::AtomicBool::new(true),
                hide_receipt: std::sync::atomic::AtomicBool::new(hide_receipt),
                location_fault: std::sync::atomic::AtomicU8::new(0),
                hide_location_read: std::sync::atomic::AtomicBool::new(false),
                ..Default::default()
            }));
            let mut coordinator =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let root = OperationId::from_bytes([87; 16]);
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "owner".into(),
            };
            let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
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

    #[tokio::test]
    async fn intent_projection_faults_recover_without_another_semantic_append() -> Result<()> {
        for fault in [1, 2, 3] {
            let client = StreamClient::new(Arc::new(LostSessionAck {
                inner: MemoryStream::default(),
                lose_ack: std::sync::atomic::AtomicBool::new(false),
                hide_receipt: std::sync::atomic::AtomicBool::new(false),
                location_fault: std::sync::atomic::AtomicU8::new(fault),
                hide_location_read: std::sync::atomic::AtomicBool::new(false),
                ..Default::default()
            }));
            let mut coordinator =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let operation = OperationId::from_bytes([89; 16]);
            let original = spec(operation, 0)?;
            let first = declare(&mut coordinator, original.clone(), "projection-fault").await;
            if fault == 2 {
                first?;
            } else {
                assert!(matches!(first, Err(Error::Storage(_))));
                assert_eq!(coordinator.revision, 0);
                assert!(coordinator.scheduler().operation(operation).is_none());
            }
            assert_eq!(client.bounds(COORDINATOR_PATH).await?.tail, 1);
            coordinator.refresh().await?;
            assert_eq!(coordinator.revision, 1);
            assert_eq!(
                declare(&mut coordinator, original, "projection-fault").await?,
                CoordinatorApply::Replayed
            );
            assert_eq!(client.bounds(COORDINATOR_PATH).await?.tail, 1);
            assert_eq!(
                client
                    .bounds(&intent_location_path("projection-fault"))
                    .await?
                    .tail,
                1
            );
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
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
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
                    loser.operation_id(),
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
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
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
                one.operation_id(),
                IdempotencyKey::new("budget-one")?,
                one.clone()
            ),
            second.apply(
                two.operation_id(),
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
                    winner.operation_id(),
                    IdempotencyKey::new(key)?,
                    winner.clone()
                )
                .await?,
            CoordinatorApply::Replayed
        );
        assert!(
            reopened
                .apply(
                    loser.operation_id(),
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
                    changed.operation_id(),
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
        let winner_id = winner.operation_id();
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
