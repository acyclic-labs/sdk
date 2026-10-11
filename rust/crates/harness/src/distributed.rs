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
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::BTreeMap, sync::Arc};

const COORDINATOR_PATH: &str = "harness/v3/coordinator/events";
const COORDINATOR_WIRE_VERSION: &str = "3";
const COORDINATOR_WIRE_CONTRACT: &[u8] = b"acyclic.harness.coordinator.scheduler-event-envelope.v3";
const READ_PAGE_SIZE: u32 = 1_024;
const MAX_CACHED_INTENTS: usize = 64;

type RetainedIntent = ([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId);

struct CachedIntent {
    value: RetainedIntent,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

fn prepare_cached_intent(
    key: String,
    value: RetainedIntent,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(String, CachedIntent)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let custody = if let Some(observer) = work.as_deref_mut()
        .filter(|observer| observer.original_native_work().is_some())
    {
        let bytes = value.1.logical_owned_bytes()?.checked_add(key.capacity() as u64)
            .ok_or(Error::NativeBoundary {
                code: wire::ErrorCode::Invalid, reason: "coordinator cache custody overflows",
            })?;
        Some(observer.take_native_custody(bytes)?)
    } else { None };
    let _ = work;
    Ok((key, CachedIntent {
        value,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        _custody: custody,
    }))
}

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
        "harness/v3/coordinator/intent-locations/{}",
        blake3::hash(key.as_bytes()).to_hex()
    )
}

fn intent_location_stream_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    key: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<crate::store::operations::ObservedStream<P>> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    if let Some(work) = custody.work().as_deref_mut() { work.hashed_bytes(key.len() as u64)?; }
    let digest = blake3::hash(key.as_bytes()).to_hex();
    let text = formatted_text_with_work(
        "harness/v3/coordinator/intent-locations/".len() + digest.len(),
        format_args!("harness/v3/coordinator/intent-locations/{digest}"), custody.work(),
    )?;
    let capacity = text.capacity() as u64;
    if custody.work().is_some() { custody.adopt(capacity)?; }
    let portable = custody.work().is_some()
        && !custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode);
    if portable {
        let work = custody.work().as_deref_mut().expect("portable observer");
        work.admit_allocation(text.len() as u64)?;
        work.copy_bytes(text.len() as u64)?;
    }
    let stream = crate::store::operations::stream_text_with_work(client, &text, custody.work())?;
    drop(text);
    if custody.work().is_some() { custody.relinquish(capacity)?; }
    if portable {
        custody.work().as_deref_mut().expect("portable observer")
            .retain_owned(stream.stream.path().as_str().len() as u64)?;
    }
    Ok(stream)
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(
    feature = "wasm",
    tsify(large_number_types_as_bigints, hashmap_as_object)
)]
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct WorkLease {
    /// Complete immutable operation declaration.
    pub operation: OperationSpec,
    /// Pinned execution allocation.
    pub reservation: Reservation,
    /// Latest durable resumable checkpoint, if any.
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "(WasmResourceRefWire & { kind: 'checkpoint' }) | null")
    )]
    pub checkpoint: Option<crate::resources::CheckpointRef>,
    /// Coordinator-observed revision associated with this reservation. Recovery
    /// may observe progress since the reservation's initial admission.
    pub operation_revision: u64,
}

impl WorkLease {
    pub(crate) fn clone_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() { return Ok(self.clone()); }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let operation = self.operation.clone_with_work(custody.work())?;
        custody.adopt(operation.logical_owned_bytes()?)?;
        let reservation = self.reservation.clone_with_work(custody.work())?;
        custody.adopt(reservation.logical_owned_bytes()?)?;
        let checkpoint = self.checkpoint.as_ref()
            .map(|checkpoint| checkpoint.clone_with_work(custody.work())).transpose()?;
        if let Some(checkpoint) = &checkpoint { custody.adopt(checkpoint.logical_owned_bytes()?)?; }
        let lease = Self {
            operation, reservation, checkpoint, operation_revision: self.operation_revision,
        };
        custody.transfer();
        Ok(lease)
    }
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

    /// Stages the same original operation and retry key on the caller's ledger.
    /// Custom stores without this observer reject before touching a provider.
    #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
    fn stage_with_native_work<'a>(
        &'a self,
        _operation_id: OperationId,
        _idempotency_key: &'a str,
        _bytes: &'a [u8],
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> BoxFuture<'a,Result<(FileRef,acyclic_fs::NativeAllocation)>> {
        Box::pin(async { Err(Error::Unsupported("scheduler payload store has no native work observer".into())) })
    }
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
    observe_declaration_with_work(client, owner, scope, verifier, operation_id,
        declaration_key, &mut None).await
}

pub(crate) async fn observe_declaration_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    operation_id: OperationId,
    declaration_key: &IdempotencyKey,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<OperationSpec>> {
    verifier.verify_audience_with_work(owner, work)?;
    verifier.verify_scope_for_with_work(owner, scope, work)?;
    if !scope.capabilities().contains(capability::OPERATION_OBSERVE) {
        return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
            "declaration observation requires operation:observe"));
    }
    let authorization = work.is_some().then_some((owner, scope, verifier));
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let Some((_, event, ..)) = read_indexed_intent_with_work(
        client, declaration_key.as_str(), None, authorization, custody.work(),
    ).await?
    else {
        return Ok(None);
    };
    if custody.work().is_some() { custody.adopt(event.logical_owned_bytes()?)?; }
    let SchedulerEvent::Declared { spec } = event else {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "declaration key does not identify a declaration"));
    };
    if spec.operation_id != operation_id {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "declaration operation identity differs"));
    }
    spec.verify_owner_with_work(owner, verifier, custody.work())?;
    let spec = *spec;
    if custody.work().is_some() { custody.relinquish(std::mem::size_of::<OperationSpec>() as u64)?; }
    custody.transfer();
    Ok(Some(spec))
}

#[cfg(feature = "filesystem")]
/// Source-bound historical evidence, never current dispatch or write authority.
/// Construction is private and moves the authenticated declaration and the
/// unique original Started fence out of their canonical source records.
pub(crate) struct OriginalReservationProof {
    declaration: OperationSpec,
    original_fence: LeaseFence,
    declaration_revision: u64,
    declaration_commit: acyclic_stream::CommitId,
    started_revision: u64,
    started_commit: acyclic_stream::CommitId,
}

#[cfg(feature = "filesystem")]
impl OriginalReservationProof {
    pub(crate) fn declaration(&self) -> &OperationSpec { &self.declaration }
    pub(crate) fn original_fence(&self) -> &LeaseFence { &self.original_fence }
    pub(crate) fn source_positions(
        &self,
    ) -> ((u64, &acyclic_stream::CommitId), (u64, &acyclic_stream::CommitId)) {
        ((self.declaration_revision, &self.declaration_commit),
            (self.started_revision, &self.started_commit))
    }
    pub(crate) fn into_parts(self) -> (OperationSpec, LeaseFence) {
        (self.declaration, self.original_fence)
    }
}

#[cfg(feature = "filesystem")]
// Source values precede their guard so rejection/cancellation drops their real
// allocations before releasing the same observer's logical custody.
struct OriginalReservationSource<'a, 'b> {
    declaration: Box<OperationSpec>,
    started: Option<(LeaseFence, u64, acyclic_stream::CommitId)>,
    custody: crate::wire_codec::CodecQuota<'a, 'b>,
}

fn authenticate_reservation_observation(
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    verifier.verify_audience_with_work(owner, work)?;
    verifier.verify_scope_for_with_work(owner, scope, work)?;
    if !scope.capabilities().contains(capability::OPERATION_OBSERVE) {
        return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
            "historical reservation requires operation:observe"));
    }
    Ok(())
}

#[cfg(feature = "filesystem")]
#[allow(clippy::too_many_arguments, reason = "original owner authorization and protected preparation's exact historical selector")]
pub(crate) async fn original_reservation_proof_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    candidate: &LeaseFence,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<OriginalReservationProof> {
    if work.is_none() {
        return Err(Error::Unsupported("historical reservation needs a finite original observer".into()));
    }
    authenticate_reservation_observation(owner, scope, verifier, work)?;
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let key = formatted_text_with_work(
        "task-admission:".len() + 36, format_args!("task-admission:{operation}"), custody.work(),
    )?;
    custody.adopt(key.capacity() as u64)?;
    let declaration = read_indexed_intent_with_work(
        client, &key, None, Some((owner, scope, verifier)), custody.work(),
    ).await;
    let key_capacity = key.capacity() as u64;
    drop(key);
    custody.relinquish(key_capacity)?;
    let (_, event, declaration_revision, declaration_commit) = declaration?
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
            "original task declaration source"))?;
    custody.adopt(event.logical_owned_bytes()?)?;
    let SchedulerEvent::Declared { spec } = event else {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "task admission does not identify a declaration"));
    };
    let mut source = OriginalReservationSource { declaration: spec, started: None, custody };
    if source.declaration.operation_id != operation {
        return Err(coordinator_boundary(source.custody.work(), wire::ErrorCode::Conflict,
            "historical declaration identifies another operation"));
    }
    source.declaration.verify_owner_with_work(owner, verifier, source.custody.work())?;
    authenticate_reservation_observation(owner, scope, verifier, source.custody.work())?;
    let stream = coordinator_source_stream_with_work(client, source.custody.work())?;
    let path_bytes = if source.custody.work().as_deref()
        .is_some_and(crate::store::operations::original_native_decode_mode)
    { 0 } else { COORDINATOR_PATH.len() as u64 };
    source.custody.adopt(path_bytes)?;
    if let Some(work) = source.custody.work().as_deref_mut() { work.begin_call()?; }
    let tail = stream.stream.tail().await
        .map_err(|error| crate::store::operations::stream_error_with_work(error, source.custody.work()))?;
    if tail < declaration_revision {
        return Err(coordinator_boundary(source.custody.work(), wire::ErrorCode::Storage,
            "declaration lies beyond coordinator source"));
    }
    let mut next = declaration_revision;
    let mut admitted = None;
    let mut last_time = None;
    while next < tail {
        authenticate_reservation_observation(owner, scope, verifier, source.custody.work())?;
        source.declaration.verify_owner_with_work(owner, verifier, source.custody.work())?;
        let maximum = (tail - next).min(u64::from(READ_PAGE_SIZE)) as u32;
        let records = read_records_with_work(&stream.stream, next, maximum, source.custody.work()).await?
            .ok_or_else(|| coordinator_boundary(source.custody.work(), wire::ErrorCode::Storage,
                "historical coordinator prefix is absent"))?;
        let received_bytes = received_page_bytes(&records, source.custody.work())?;
        source.custody.adopt(received_bytes)?;
        if records.is_empty() {
            return Err(coordinator_boundary(source.custody.work(), wire::ErrorCode::Storage,
                "historical coordinator prefix is incomplete"));
        }
        let capacity = records.capacity();
        let mut expected = next;
        for record in &records {
            if record.sequence != expected {
                return Err(coordinator_boundary(source.custody.work(), wire::ErrorCode::Storage,
                    "historical coordinator prefix has a cursor gap"));
            }
            expected = expected.checked_add(1).ok_or_else(|| crate::store::operations::stream_error_with_work(
                StreamError::LimitExceeded, source.custody.work(),
            ))?;
        }
        for record in records {
            let raw_bytes = record.value.len() as u64;
            let mut event_custody = crate::wire_codec::CodecQuota::new(source.custody.work(), 0)?;
            let (revision, identity, key, _, time, event) = decode_with_work(&record.value, event_custody.work())?;
            let owned = event.logical_owned_bytes()?;
            event_custody.adopt(owned)?;
            event_custody.adopt(key.capacity() as u64)?;
            if revision != next_revision(record.sequence)? || identity != event.operation_id()
                || last_time.is_some_and(|previous| time <= previous)
            {
                return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Storage,
                    "historical coordinator source identity differs"));
            }
            last_time = Some(time);
            let key_capacity = key.capacity() as u64;
            drop(key);
            event_custody.relinquish(key_capacity)?;
            match event {
                SchedulerEvent::Admitted { operation_id, reservation }
                    if operation_id == operation && reservation.id == candidate.reservation_id
                        && reservation.placement == candidate.placement =>
                {
                    if admitted.is_some() {
                        return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                            "historical reservation has distinct admission origins"));
                    }
                    admitted = Some(revision);
                    drop(reservation);
                    drop(event_custody);
                }
                SchedulerEvent::Started { operation_id, fence }
                    if operation_id == operation && &fence == candidate =>
                {
                    if admitted.is_none_or(|admission| admission >= revision) || source.started.is_some() {
                        return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                            "historical reservation has no unique admitted start"));
                    }
                    event_custody.transfer();
                    drop(event_custody);
                    source.custody.adopt(owned)?;
                    source.started = Some((fence, revision, record.commit_id));
                }
                event => {
                    drop(event);
                    drop(event_custody);
                }
            }
            next = revision;
            drop(record);
            source.custody.relinquish(raw_bytes)?;
        }
        source.custody.relinquish((capacity * std::mem::size_of::<acyclic_stream::Record>()) as u64)?;
    }
    drop(stream);
    source.custody.relinquish(path_bytes)?;
    let (original_fence, started_revision, started_commit) = source.started.take()
        .ok_or_else(|| coordinator_boundary(source.custody.work(), wire::ErrorCode::NotFound,
            "selected original reservation start"))?;
    let declaration = *source.declaration;
    source.custody.relinquish(std::mem::size_of::<OperationSpec>() as u64)?;
    source.custody.transfer();
    Ok(OriginalReservationProof {
        declaration, original_fence, declaration_revision, declaration_commit,
        started_revision, started_commit,
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[allow(clippy::too_many_arguments, reason = "same original historical proof with its native ledger's custody handoff")]
pub(crate) async fn original_reservation_proof_with_native_work<P: StreamProvider>(
    client: &StreamClient<P>,
    owner: &Authority,
    scope: &Scope,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    candidate: &LeaseFence,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(OriginalReservationProof, acyclic_fs::NativeAllocation)> {
    let mut io = crate::store::history::native_work_io(work)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let proof = original_reservation_proof_with_work(
        client, owner, scope, verifier, operation, candidate, &mut Some(&mut io),
    ).await?;
    Ok((proof, io.into_custody()))
}

// Shared by ordinary and observed coordinator replay. Collecting the original
// SDK page here exposes yielded prefixes that Replay::next_page would discard
// on a stream error; the whole page is still checked before reducer admission.
async fn read_records_with_work<P: StreamProvider>(
    stream: &Stream<P>,
    from: u64,
    maximum: u32,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<Vec<acyclic_stream::Record>>> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    if let Some(work) = custody.work().as_deref_mut() { work.begin_call()?; }
    let mut source = match stream.read(from, maximum).await {
        Ok(source) => source,
        Err(StreamError::NotFound) if from == 0 => return Ok(None),
        Err(error) => return Err(crate::store::operations::stream_error_with_work(error, custody.work())),
    };
    // Both the SDK receiver and every yielded physical record die before the
    // original observer's claims on an error or cancellation.
    let mut records = Vec::new();
    loop {
        if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
        let Some(record) = source.try_next().await
            .map_err(|error| crate::store::operations::stream_error_with_work(error, custody.work()))?
        else {
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            break;
        };
        if let Some(work) = custody.work().as_deref_mut() {
            // A rejected receiver prefix has no new retained claim.
            work.receive_record(record.value.len() as u64)?;
            custody.adopt(record.value.len() as u64)?;
        }
        if records.len() >= maximum as usize
            || record.value.len() > acyclic_stream::MAX_RECORD_BYTES
        {
            return Err(crate::store::operations::stream_error_with_work(
                StreamError::LimitExceeded, custody.work(),
            ));
        }
        let old_capacity = records.capacity();
        if let Some(work) = custody.work().as_deref_mut() {
            work.reserve_records(&mut records, maximum as usize)?;
            custody.adopt(((records.capacity() - old_capacity)
                * std::mem::size_of::<acyclic_stream::Record>()) as u64)?;
        }
        records.push(record);
    }
    custody.transfer();
    Ok(Some(records))
}

fn received_page_bytes(
    records: &Vec<acyclic_stream::Record>,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    let slots = records.capacity().checked_mul(std::mem::size_of::<acyclic_stream::Record>())
        .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Storage,
            "coordinator record custody overflows"))? as u64;
    records.iter().try_fold(slots, |bytes, record| {
        bytes.checked_add(record.value.len() as u64)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Storage,
                "coordinator record custody overflows"))
    })
}

fn coordinator_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: wire::ErrorCode,
    reason: &'static str,
    ordinary: impl FnOnce() -> Error,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = (work, code, reason);
    ordinary()
}

fn coordinator_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: wire::ErrorCode,
    reason: &'static str,
) -> Error {
    coordinator_error(work, code, reason, || match code {
        wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    })
}


async fn read_intent_location<P: StreamProvider>(
    client: &StreamClient<P>,
    key: &str,
) -> Result<Option<IntentLocation>> {
    read_intent_location_with_work(client, key, &mut None).await
}

fn decode_intent_location_with_work(
    input: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<IntentLocation> {
    if work.is_none() {
        return crate::contract::json_from_slice(input)
            .map_err(|error| Error::Storage(error.to_string()));
    }
    // The original typed decoder admits this closed one-String schema and its
    // scratch before Deserialize. Its diagnostic keeps the same source guard.
    let diagnostic = crate::contract::audited_serde_diagnostic_bytes(input.len() as u64,
        concat!("IntentLocation key revision event_digest commit_id unknown field duplicate field ",
            "missing field invalid type invalid value invalid length expected u8 u64 string array ").len() as u64)?;
    let maximum = (input.len() as u64).checked_mul(2)
        .and_then(|bytes| bytes.checked_add(diagnostic))
        .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Storage,
            "intent location admission overflows"))?;
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut reservation = 0;
    let mut observed_failure = false;
    let native = custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode);
    let decoded = {
        let observer = custody.work().as_deref_mut().expect("observed intent location");
        crate::store::operations::decode_preclaimed_json_with_mode::<IntentLocation, _>(
            input, crate::contract::JsonBounds {
                maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
                maximum_depth: 256,
            }, maximum, wire::ErrorCode::Storage, native, crate::contract::JsonValueSchema::None, &mut |charge| {
                let charged = observer.codec_charge(charge);
                if charged.is_ok()
                    && let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, .. } = charge
                { reservation = maximum_owned_bytes; }
                if charged.is_err() { observed_failure = true; }
                charged
            },
        )
    };
    let decoded = crate::store::operations::retain_original_decode_error(
        decoded, &mut reservation, custody.work().as_deref_mut().expect("observed intent location"),
    );
    custody.adopt(reservation)?;
    let location = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error { Error::Invalid(message) => Error::Storage(message), other => other }
        }
    })?;
    if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
    let retained = location.key.capacity() as u64;
    let slack = reservation.checked_sub(retained)
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "intent location exceeds its closed admission"))?;
    custody.relinquish(slack)?;
    custody.transfer();
    Ok(location)
}

fn coordinator_source_stream_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<crate::store::operations::ObservedStream<P>> {
    let portable = work.is_some()
        && !work.as_deref().is_some_and(crate::store::operations::original_native_decode_mode);
    if portable {
        let work = work.as_deref_mut().expect("portable observer");
        work.admit_allocation(COORDINATOR_PATH.len() as u64)?;
        work.copy_bytes(COORDINATOR_PATH.len() as u64)?;
    }
    let stream = crate::store::operations::stream_text_with_work(client, COORDINATOR_PATH, work)?;
    if portable {
        work.as_deref_mut().expect("portable observer").retain_owned(COORDINATOR_PATH.len() as u64)?;
    }
    Ok(stream)
}

async fn read_intent_location_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    key: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<IntentLocation>> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let owned_key = text_with_work(key, custody.work())?;
    let key_capacity = owned_key.capacity() as u64;
    let validation = checked_idempotency_key(owned_key, custody.work()).map(|_| ());
    if let Some(work) = custody.work().as_deref_mut() { work.release_owned(key_capacity)?; }
    validation?;
    let stream = intent_location_stream_with_work(client, key, custody.work())?;
    let path_bytes = if custody.work().is_some()
        && !custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode)
    { stream.stream.path().as_str().len() as u64 } else { 0 };
    custody.adopt(path_bytes)?;
    let Some(records) = read_records_with_work(&stream.stream, 0, 2, custody.work()).await? else {
        return Ok(None);
    };
    let raw_bytes = if custody.work().is_some() { received_page_bytes(&records, custody.work())? } else { 0 };
    custody.adopt(raw_bytes)?;
    let [record] = records.as_slice() else {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "intent location must contain one immutable record"));
    };
    let location = decode_intent_location_with_work(&record.value, custody.work())?;
    if custody.work().is_some() { custody.adopt(location.key.capacity() as u64)?; }
    let canonical = crate::store::history::canonical_bytes_with_work(
        &location, acyclic_stream::MAX_RECORD_BYTES as u64, custody.work(),
    )?;
    let canonical_bytes = if custody.work().is_some() { canonical.capacity() as u64 } else { 0 };
    custody.adopt(canonical_bytes)?;
    if record.sequence != 0 || location.key != key || location.revision == 0
        || canonical.as_slice() != record.value.as_ref()
    {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "intent location identity differs"));
    }
    drop(canonical);
    custody.relinquish(canonical_bytes)?;
    drop(records);
    custody.relinquish(raw_bytes)?;
    drop(stream);
    custody.relinquish(path_bytes)?;
    custody.transfer();
    Ok(Some(location))
}


async fn read_indexed_intent_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    key: &str,
    maximum_revision: Option<u64>,
    authorization: Option<(&Authority, &Scope, &AuthorityVerifier)>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<RetainedIntent>> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    if let Some((owner, scope, verifier)) = authorization {
        authenticate_reservation_observation(owner, scope, verifier, custody.work())?;
    }
    let Some(location) = read_intent_location_with_work(client, key, custody.work()).await? else {
        return Ok(None);
    };
    let location_bytes = if custody.work().is_some() { location.key.capacity() as u64 } else { 0 };
    custody.adopt(location_bytes)?;
    // The original coordinator CAS, not a derived later index, fences writes.
    if maximum_revision.is_some_and(|maximum| location.revision > maximum) { return Ok(None); }
    let source = coordinator_source_stream_with_work(client, custody.work())?;
    let path_bytes = if custody.work().is_some()
        && !custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode)
    { source.stream.path().as_str().len() as u64 } else { 0 };
    custody.adopt(path_bytes)?;
    if let Some((owner, scope, verifier)) = authorization {
        authenticate_reservation_observation(owner, scope, verifier, custody.work())?;
    }
    let Some(records) = read_records_with_work(
        &source.stream, location.revision - 1, 1, custody.work(),
    ).await? else {
        return Err(crate::store::operations::stream_error_with_work(StreamError::NotFound, custody.work()));
    };
    let raw_bytes = if custody.work().is_some() { received_page_bytes(&records, custody.work())? } else { 0 };
    custody.adopt(raw_bytes)?;
    let [record] = records.as_slice() else {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "intent source record is absent"));
    };
    let (revision, operation, source_key, digest, _, event) = decode_with_work(&record.value, custody.work())?;
    let key_bytes = if custody.work().is_some() { source_key.capacity() as u64 } else { 0 };
    custody.adopt(key_bytes)?;
    if custody.work().is_some() { custody.adopt(event.logical_owned_bytes()?)?; }
    if record.sequence != location.revision - 1 || revision != location.revision
        || record.commit_id.as_bytes() != &location.commit_id || source_key != key
        || digest != location.event_digest || event.operation_id() != operation
    {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "intent location differs from coordinator source"));
    }
    let intent = (digest, event, revision, record.commit_id);
    drop(source_key);
    custody.relinquish(key_bytes)?;
    drop(location);
    custody.relinquish(location_bytes)?;
    drop(records);
    custody.relinquish(raw_bytes)?;
    drop(source);
    custody.relinquish(path_bytes)?;
    custody.transfer();
    Ok(Some(intent))
}

/// One logical coordinator whose entire semantic history is a Stream.
pub struct DistributedCoordinator<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    scheduler: Scheduler,
    revision: u64,
    last_committed_at_ms: u64,
    intents: BTreeMap<String, CachedIntent>,
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
        self.journal_condition_with_work(
            owner, scope, verifier, operation_id, fence, write, &mut None,
        ).await
    }

    #[cfg(feature = "filesystem")]
    #[allow(
        clippy::too_many_arguments,
        reason = "the original signed journal fence and its shared receipt observer"
    )]
    pub(crate) async fn journal_condition_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        fence: &LeaseFence,
        write: JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.refresh_with_work(work).await?;
        let operation = self.observe_operation_ref_with_work(
            owner, scope, verifier, operation_id, work,
        )?;
        crate::scheduler::require_execution_owner_with_work(operation, fence, write.settlement(), work)?;
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

    /// Opens and replays the owner-proof coordinator history (version 3).
    /// Legacy version-2 histories remain separate; their missing original
    /// owner grants are never synthesized or adopted into this history.
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
        self.refresh_with_work(&mut None).await
    }

    pub(crate) async fn refresh_with_work(
        &mut self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let initial_revision = self.revision;
        let mut next = self.revision;
        loop {
            let mut page_custody = crate::wire_codec::CodecQuota::new(work, 0)?;
            let page = read_records_with_work(&self.stream, next, READ_PAGE_SIZE, page_custody.work())
                .await?.unwrap_or_default();
            if page_custody.work().is_some() {
                let received_bytes = received_page_bytes(&page, page_custody.work())?;
                page_custody.adopt(received_bytes)?;
            }
            if page.is_empty() { break; }
            for record in &page {
                if record.sequence != next {
                    return Err(crate::store::operations::stream_error_with_work(
                        StreamError::Unavailable, page_custody.work(),
                    ));
                }
                next = next.checked_add(1).ok_or_else(|| crate::store::operations::stream_error_with_work(
                    StreamError::LimitExceeded, page_custody.work(),
                ))?;
            }
            let page_slots = (page.capacity() * std::mem::size_of::<acyclic_stream::Record>()) as u64;
            for record in page {
                let raw_bytes = record.value.len() as u64;
                let mut event_custody = crate::wire_codec::CodecQuota::new(page_custody.work(), 0)?;
                if let Some(work) = event_custody.work().as_deref_mut() { work.check_cancellation()?; }
                let (revision, operation_id, key, digest, committed_at_ms, event) =
                    decode_with_work(&record.value, event_custody.work())?;
                if event_custody.work().is_some() {
                    event_custody.adopt(event.logical_owned_bytes()?)?;
                    event_custody.adopt(key.capacity() as u64)?;
                }
                if revision != next_revision(record.sequence)? {
                    return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Storage,
                        "coordinator revision is not gapless"));
                }
                if let SchedulerEvent::Declared { spec } = &event {
                    self.verify_content_with_work(&spec.state, event_custody.work()).await?;
                }
                self.verify_published_result_with_work(&event, event_custody.work()).await?;
                let location = IntentLocation {
                    key, revision, event_digest: digest, commit_id: *record.commit_id.as_bytes(),
                };
                self.ensure_intent_location_with_work(&location, event_custody.work()).await?;
                let key = location.key;
                if let Some(work) = event_custody.work().as_deref_mut() { work.check_cancellation()?; }
                let evicted_capacity = if event_custody.work().is_some()
                    && !event_custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode)
                    && self.intents.len() >= MAX_CACHED_INTENTS
                {
                    self.intents.iter().min_by_key(|(_, intent)| intent.value.2)
                        .filter(|(_, intent)| intent.value.2 > initial_revision)
                        .map(|(key, intent)| intent.value.1.logical_owned_bytes().and_then(|event_bytes| {
                            event_bytes.checked_add(key.capacity() as u64)
                                .ok_or_else(|| coordinator_boundary(&None, wire::ErrorCode::Invalid,
                                    "coordinator cache custody overflows"))
                        })).transpose()?.unwrap_or(0)
                } else { 0 };
                event_custody.transfer();
                drop(event_custody);
                self.apply_committed_with_work(
                    (revision, record.commit_id), operation_id, key, digest,
                    committed_at_ms, event, page_custody.work(),
                )?;
                drop(record);
                if let Some(work) = page_custody.work().as_deref_mut() {
                    work.release_owned(evicted_capacity)?;
                }
                if page_custody.work().is_some() { page_custody.relinquish(raw_bytes)?; }
            }
            // The consumed Vec iterator has physically retired its slots.
            if page_custody.work().is_some() { page_custody.relinquish(page_slots)?; }
        }
        Ok(())
    }

    #[cfg(test)]
    async fn read_intent_location(&self, key: &str) -> Result<Option<IntentLocation>> {
        read_intent_location(&self.client, key).await
    }


    async fn ensure_intent_location_with_work(
        &self,
        location: &IntentLocation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        if let Some(existing) = read_intent_location_with_work(
            &self.client, &location.key, custody.work(),
        ).await? {
            if custody.work().is_some() { custody.adopt(existing.key.capacity() as u64)?; }
            return if existing == *location { Ok(()) } else {
                Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                    "coordinator history repeats an idempotency key or index differs"))
            };
        }
        let bytes = crate::store::history::canonical_bytes_with_work(
            location, acyclic_stream::MAX_RECORD_BYTES as u64, custody.work(),
        )?;
        let byte_length = bytes.len() as u64;
        let byte_capacity = if custody.work().is_some() { bytes.capacity() as u64 } else { 0 };
        custody.adopt(byte_capacity)?;
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_acknowledgment(byte_length, 1)?;
            work.hashed_bytes(byte_length)?;
        }
        let location_digest = blake3::hash(&bytes).to_hex();
        let physical_identity = formatted_text_with_work(
            "intent-location:".len() + location_digest.len(),
            format_args!("intent-location:{location_digest}"), custody.work(),
        )?;
        let physical_capacity = if custody.work().is_some() { physical_identity.capacity() as u64 } else { 0 };
        custody.adopt(physical_capacity)?;
        let physical = stream_key_with_work(&physical_identity, custody.work())?;
        let key_bytes = if custody.work().is_some() { 32 } else { 0 };
        custody.adopt(key_bytes)?;
        drop(physical_identity);
        custody.relinquish(physical_capacity)?;
        let destination = intent_location_stream_with_work(&self.client, &location.key, custody.work())?;
        let path_bytes = if custody.work().is_some()
            && !custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode)
        { destination.stream.path().as_str().len() as u64 } else { 0 };
        custody.adopt(path_bytes)?;
        let mut records = Vec::new();
        let mut records_capacity = 0u64;
        reserve_prepared_vector(&mut records, 1, &mut custody, &mut records_capacity)?;
        records.push(Bytes::from(bytes));
        if let Some(work) = custody.work().as_deref_mut() { work.begin_write()?; }
        let outcome = destination.stream.append_batch(records, Some(0), Some(physical)).await;
        if custody.work().is_some() { custody.relinquish(records_capacity + byte_capacity + key_bytes)?; }
        drop(destination);
        custody.relinquish(path_bytes)?;
        match outcome {
            Ok(AppendOutcome::Committed(receipt))
                if receipt.start == 0 && receipt.end == 1 && receipt.tail == 1 =>
            {
                if let Some(work) = custody.work().as_deref_mut() { work.acknowledged(byte_length, 1)?; }
                Ok(())
            }
            Ok(AppendOutcome::TailConflict { .. }) | Err(StreamError::Unavailable) => {
                let existing = read_intent_location_with_work(&self.client, &location.key, custody.work()).await?;
                if let Some(existing) = &existing
                    && custody.work().is_some()
                { custody.adopt(existing.key.capacity() as u64)?; }
                match existing {
                    Some(existing) if existing == *location => Ok(()),
                    Some(_) => Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                        "intent location publication differs")),
                    None => Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                        "intent location publication is unavailable")),
                }
            }
            Ok(AppendOutcome::Committed(_)) => Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "intent location receipt differs")),
            Err(error) => Err(crate::store::operations::stream_error_with_work(error, custody.work())),
        }
    }

    fn cache_intent(&mut self, key: String, intent: CachedIntent) {
        self.intents.insert(key, intent);
        if self.intents.len() > MAX_CACHED_INTENTS {
            let oldest = self.intents.iter().enumerate()
                .min_by_key(|(_, (_, value))| value.value.2).map(|(index, _)| index);
            let mut index = 0;
            self.intents.retain(|_, _| {
                let keep = Some(index) != oldest;
                index += 1;
                keep
            });
        }
    }

    async fn retained_intent(&mut self, key: &str) -> Result<Option<RetainedIntent>> {
        self.retained_intent_with_work(key, &mut None).await
            .map(|intent| intent.map(std::borrow::Cow::into_owned))
    }

    async fn retained_intent_with_work(
        &mut self,
        key: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<std::borrow::Cow<'_, RetainedIntent>>> {
        if self.intents.contains_key(key) {
            return Ok(self.intents.get(key).map(|intent| std::borrow::Cow::Borrowed(&intent.value)));
        }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let Some(intent) = read_indexed_intent_with_work(
            &self.client, key, Some(self.revision), None, custody.work(),
        ).await? else { return Ok(None) };
        if custody.work().is_some() { custody.adopt(intent.1.logical_owned_bytes()?)?; }
        // Preserve the original bounded cache's cold-hit ordering, without
        // cloning a received event that would immediately be evicted.
        if self.intents.len() >= MAX_CACHED_INTENTS
            && self.intents.iter().min_by_key(|(_, value)| value.value.2)
                .is_some_and(|(oldest_key, oldest)| {
                    intent.2 < oldest.value.2 || (intent.2 == oldest.value.2 && key < oldest_key.as_str())
                })
        {
            custody.transfer();
            return Ok(Some(std::borrow::Cow::Owned(intent)));
        }
        let owned_key = text_with_work(key, custody.work())?;
        if custody.work().is_some() { custody.adopt(owned_key.capacity() as u64)?; }
        let (owned_key, intent) = prepare_cached_intent(owned_key, intent, custody.work())?;
        custody.transfer();
        self.cache_intent(owned_key, intent);
        Ok(self.intents.get(key).map(|intent| std::borrow::Cow::Borrowed(&intent.value)))
    }

    async fn intent_matches_with_work(
        &mut self,
        key: &str,
        event: &SchedulerEvent,
        digest: &[u8; 32],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<bool>> {
        let intent = self.retained_intent_with_work(key, work).await?;
        let mut temporary = 0;
        let matches = intent.map(|intent| {
            if work.is_some() && matches!(&intent, std::borrow::Cow::Owned(_)) {
                temporary = intent.as_ref().1.logical_owned_bytes()?;
            }
            Ok::<_, Error>(&intent.as_ref().0 == digest && &intent.as_ref().1 == event)
        }).transpose()?;
        if let Some(work) = work.as_deref_mut() { work.release_owned(temporary)?; }
        Ok(matches)
    }

    /// Binds the owner-controlled staging boundary for joins, quorums and reducers.
    #[must_use]
    pub fn with_payload_store(mut self, store: Arc<dyn SchedulerPayloadStore>) -> Self {
        self.payload_store = Some(store);
        self
    }

    async fn verify_published_result(&self, event: &SchedulerEvent) -> Result<()> {
        self.verify_published_result_with_work(event, &mut None).await
    }

    async fn verify_content_with_work(
        &self,
        reference: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        match work.as_deref_mut() {
            Some(work) => crate::store::history::verify_content_with_work(
                work, self.content_verifier.as_ref(), reference).await,
            None => self.content_verifier.verify(reference).await,
        }
    }

    async fn verify_published_result_with_work(
        &self,
        event: &SchedulerEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let SchedulerEvent::WorkflowResumed { input, .. } = event {
            return self.verify_content_with_work(input, work).await;
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
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (value, value_bytes) = self.load_json_with_work(reference, custody.work()).await?;
        custody.adopt(value_bytes)?;
        if let SchedulerEvent::Orchestrated { operation_id, reducer: None, .. } = event {
            let decision = self.scheduler.orchestration_with_work(*operation_id, custody.work())?;
            let decision_bytes = if custody.work().is_some() { decision.logical_owned_bytes()? } else { 0 };
            custody.adopt(decision_bytes)?;
            if let OrchestrationDecision::Assemble { assembly, values, .. } = &decision {
                let (expected, expected_bytes) =
                    self.assemble_values_with_work(*assembly, values, custody.work()).await?;
                custody.adopt(expected_bytes)?;
                let differs = value != expected;
                drop(expected);
                custody.relinquish(expected_bytes)?;
                if differs {
                    return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                        "assembled scheduler result differs from pinned children"));
                }
            }
            drop(decision);
            custody.relinquish(decision_bytes)?;
        }
        let operation = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| coordinator_error(custody.work(), wire::ErrorCode::NotFound,
                "scheduler result operation is absent", || Error::NotFound(format!("operation {operation_id}"))))?;
        for schema in [
            Some(&operation.spec.entrypoint.result_schema),
            reducer.map(|item| &item.result_schema),
        ]
        .into_iter()
        .flatten()
        {
            crate::contract::validate_json_schema_value_with_work(
                schema, &value, "scheduler result", custody.work(),
            )?;
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
        self.observe_operation_ref_with_work(owner, scope, verifier, operation_id, &mut None)
            .cloned()
    }

    pub(crate) fn observe_operation_ref_with_work<'a>(
        &'a self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&'a OperationState> {
        self.authorize_operation_with_work(
            owner, scope, verifier, operation_id, capability::OPERATION_OBSERVE, work,
        )
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
        self.cancel_operation_with_work(
            owner, scope, verifier, operation_id, idempotency_key, recursive, &mut None,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "original signed cancellation and optional observer")]
    pub(crate) async fn cancel_operation_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        recursive: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(CoordinatorApply, OperationState)> {
        Self::authorize_operation_scope_with_work(
            owner, scope, verifier, capability::OPERATION_CANCEL, work,
        )?;
        self.refresh_with_work(work).await?;
        self.authorize_operation_owner_with_work(owner, verifier, operation_id, work)?;
        let applied = self
            .apply_with_work(
                operation_id,
                idempotency_key,
                SchedulerEvent::CancellationRequested {
                    operation_id,
                    recursive,
                },
                work,
            )
            .await?;
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| coordinator_error(work, wire::ErrorCode::NotFound,
                "operation is absent after cancellation", || Error::NotFound(format!("operation {operation_id}"))))?
            .clone_with_work(work)?;
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
        self.authorize_operation_with_work(
            owner, scope, verifier, operation_id, capability, &mut None,
        )
    }

    fn authorize_operation_with_work<'a>(
        &'a self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        capability: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&'a OperationState> {
        Self::authorize_operation_scope_with_work(owner, scope, verifier, capability, work)?;
        self.authorize_operation_owner_with_work(owner, verifier, operation_id, work)
    }

    fn authorize_operation_scope_with_work(
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        capability: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        verifier.verify_audience_with_work(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains(capability) {
            return Err(coordinator_error(work, wire::ErrorCode::Unauthorized,
                "owner scope lacks the required operation capability", || Error::Unauthorized(format!(
                    "scope {} lacks capability {capability}", scope.id(),
                ))));
        }
        Ok(())
    }

    fn authorize_operation_owner_with_work<'a>(
        &'a self,
        owner: &Authority,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&'a OperationState> {
        let operation = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| coordinator_error(work, wire::ErrorCode::NotFound,
                "operation is absent for this owner", || Error::NotFound(format!("operation {operation_id}"))))?;
        let declared_owner = operation.spec.owner.authority();
        if declared_owner != owner {
            return Err(coordinator_error(work, wire::ErrorCode::NotFound,
                "operation is absent for this owner", || Error::NotFound(format!("operation {operation_id}"))));
        }
        operation.spec.verify_owner_with_work(owner, verifier, work)?;
        Ok(operation)
    }

    /// Durably applies one scheduler event with exact retry semantics.
    pub async fn apply(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
    ) -> Result<CoordinatorApply> {
        self.apply_with_work(operation_id, idempotency_key, event, &mut None).await
    }

    pub(crate) async fn apply_with_work(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        if matches!(&event, SchedulerEvent::Declared { .. }) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "declarations require owner-scoped admission"));
        }
        if matches!(&event, SchedulerEvent::SessionConfigured { .. }) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "session configuration requires owner authority"));
        }
        if matches!(&event, SchedulerEvent::WorkflowResumed { .. }) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "workflow wake requires owner authority"));
        }
        if matches!(&event, SchedulerEvent::Orchestrated { .. }) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "orchestration decisions require coordinator-owned materialization"));
        }
        self.apply_internal_guarded_with_work(
            operation_id, idempotency_key, event, None, None, None, None, None, work,
        ).await
    }

    /// Compares the original execution journal at the scheduler quiescence boundary.
    pub(crate) async fn apply_if_execution_quiescent(
        &mut self,
        key: IdempotencyKey,
        event: SchedulerEvent,
        execution: OperationId,
        expected_tail: u64,
    ) -> Result<CoordinatorApply> {
        self.apply_if_execution_quiescent_with_work(
            key, event, execution, expected_tail, None, &mut None,
        ).await
    }

    pub(crate) async fn apply_if_execution_quiescent_with_work(
        &mut self,
        key: IdempotencyKey,
        event: SchedulerEvent,
        execution: OperationId,
        expected_tail: u64,
        workflow_guard: Option<(&str, u64)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        let verified_revision = work.is_some().then_some(self.revision);
        if !matches!(&event, SchedulerEvent::WorkflowSuspended { .. } | SchedulerEvent::LeaseReleased { .. }) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "idle execution guard requires suspension or lease release"));
        }
        if work.is_some() && matches!(&event, SchedulerEvent::LeaseReleased { .. }) && workflow_guard.is_none() {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "measured lease release requires its original workflow witness"));
        }
        self.apply_internal_guarded_with_work(
            event.operation_id(), key, event, None, None,
            Some((execution, expected_tail)), workflow_guard, verified_revision, work,
        ).await
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
        self.resume_workflow_with_work(
            owner, scope, verifier, operation_id, workflow_revision, input,
            idempotency_key, &mut None,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "original authenticated workflow wake and optional observer")]
    pub(crate) async fn resume_workflow_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        self.resume_workflow_guarded_with_work(
            owner, scope, verifier, operation_id, workflow_revision, input,
            idempotency_key, None, work,
        ).await
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
        self.resume_command_with_work(
            owner, scope, verifier, operation_id, workflow_revision, input,
            idempotency_key, waiting_command, &mut None,
        ).await
    }

    #[cfg(any(feature = "filesystem", test))]
    #[allow(clippy::too_many_arguments, reason = "original authenticated command wake and optional observer")]
    pub(crate) async fn resume_command_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
        waiting_command: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        self.resume_workflow_guarded_with_work(
            owner, scope, verifier, operation_id, workflow_revision, input,
            idempotency_key, Some(waiting_command), work,
        ).await
    }


    #[allow(clippy::too_many_arguments, reason = "one original authenticated wake and optional observer")]
    async fn resume_workflow_guarded_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
        waiting_command: Option<OperationId>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        verifier.verify_audience_with_work(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains("operation:wake") {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "owner scope lacks operation:wake"));
        }
        self.refresh_with_work(work).await?;
        self.observe_operation_ref_with_work(owner, scope, verifier, operation_id, work)?;
        self.apply_internal_guarded_with_work(
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
            None,
            None,
            work,
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
        self.declare_operation_with_work(owner, scope, verifier, spec, idempotency_key, &mut None).await
    }

    pub(crate) async fn declare_operation_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        idempotency_key: IdempotencyKey,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        self.declare_operation_with_parent_with_work(
            owner, scope, verifier, spec, idempotency_key, None, work,
        ).await
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
        self.declare_operation_owned_with_work(
            owner, scope, verifier, spec, idempotency_key, parent_fence, &mut None,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "signed owner, exact parent lease and optional observer")]
    pub(crate) async fn declare_operation_owned_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        idempotency_key: IdempotencyKey,
        parent_fence: LeaseFence,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        if spec.parent.is_none() {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "owned child declaration has no parent"));
        }
        self.declare_operation_with_parent_with_work(
            owner,
            scope,
            verifier,
            spec,
            idempotency_key,
            Some(parent_fence),
            work,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one authenticated declaration path"
    )]
    async fn declare_operation_with_parent_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        idempotency_key: IdempotencyKey,
        parent_fence: Option<LeaseFence>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        verifier.verify_audience_with_work(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains(capability::OPERATION_DECLARE) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized, "scope lacks operation:declare"));
        }
        let declared_owner = spec.owner.authority();
        if declared_owner != owner {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "declaration owner does not match authenticated owner"));
        }
        spec.verify_owner_with_work(owner, verifier, work)?;
        if let Some(parent) = &spec.parent {
            self.refresh_with_work(work).await?;
            self.authorize_operation_with_work(
                owner,
                scope,
                verifier,
                parent.operation_id,
                capability::OPERATION_DECLARE,
                work,
            )?;
        }
        let operation_id = spec.operation_id;
        let box_bytes = std::mem::size_of::<OperationSpec>() as u64;
        if let Some(work) = work.as_deref_mut() { work.admit_allocation(box_bytes)?; }
        let event = SchedulerEvent::Declared { spec: Box::new(spec) };
        if let Some(work) = work.as_deref_mut() { work.retain_owned(box_bytes)?; }
        let result = self.apply_internal_guarded_with_work(
            operation_id, idempotency_key, event, parent_fence, None, None, None, None, work,
        ).await;
        if let Some(work) = work.as_deref_mut() { work.release_owned(box_bytes)?; }
        result
    }

    /// Publishes a root and its immutable shared ceilings in one atomic append.
    /// No worker can observe the root before its session budget is configured.
    pub async fn declare_session(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.declare_session_with_work(
            owner, scope, verifier, spec, limits, idempotency_key, &mut None,
        ).await
    }

    #[allow(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "original authenticated atomic two-event publication and its observer"
    )]
    pub(crate) async fn declare_session_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        spec: OperationSpec,
        limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let spec = spec;
        let idempotency_key = idempotency_key;
        verifier.verify_audience_with_work(owner, custody.work())?;
        verifier.verify_with_work(scope, custody.work())?;
        if !scope.capabilities().contains("operation:declare") || spec.owner.authority() != owner {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Unauthorized,
                "session declaration owner is invalid"));
        }
        spec.verify_owner_with_work(owner, verifier, custody.work())?;
        if spec.parent.is_some() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                "session declaration must be a root"));
        }
        limits.validate_with_work(custody.work())?;
        let idempotency_key = if custody.work().is_some() {
            // Validate the original owned input; no native validation copy.
            checked_idempotency_key(idempotency_key.0, custody.work())?
        } else {
            IdempotencyKey::new(idempotency_key.0.clone())?;
            idempotency_key
        };
        let operation_id = spec.operation_id;
        let declaration_key = text_with_work(idempotency_key.as_str(), custody.work())?;
        if custody.work().is_some() { custody.adopt(declaration_key.capacity() as u64)?; }
        if let Some(work) = custody.work().as_deref_mut() {
            work.hashed_bytes(idempotency_key.as_str().len() as u64)?;
        }
        let session_key = formatted_text_with_work(
            "session:".len() + 36 + 1 + 64,
            format_args!("session:{operation_id}:{}",
                blake3::hash(idempotency_key.as_str().as_bytes()).to_hex()),
            custody.work(),
        )?;
        if custody.work().is_some() { custody.adopt(session_key.capacity() as u64)?; }
        let keys = [declaration_key, session_key];
        let box_bytes = std::mem::size_of::<OperationSpec>() as u64;
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_owned(box_bytes)?;
            work.admit_allocation(box_bytes)?;
        }
        let events = [
            SchedulerEvent::Declared {
                spec: Box::new(spec),
            },
            SchedulerEvent::SessionConfigured {
                operation_id,
                limits,
            },
        ];
        if let Some(work) = custody.work().as_deref_mut() {
            work.retain_owned(box_bytes)?;
            custody.adopt(box_bytes)?;
        }
        self.refresh_with_work(custody.work()).await?;
        if self.session_intent_with_work(&keys, &events, custody.work()).await? {
            return Ok(CoordinatorApply::Replayed);
        }
        // Ordinary replay keeps its original projection. Native admission
        // borrows the same validators against the prospective declared root.
        let mut projected = if custody.work().is_none() {
            Some(self.scheduler.clone())
        } else {
            None
        };
        let mut request = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
        let mut records = Vec::new();
        let slots = (2 * std::mem::size_of::<Bytes>()) as u64;
        if let Some(work) = request.work().as_deref_mut() {
            work.admit_owned(slots)?;
            work.admit_allocation(slots)?;
        }
        records.try_reserve_exact(2).map_err(|cause| match request.work().as_deref_mut() {
            Some(work) => work.allocation_failure(cause),
            None => Error::Storage(cause.to_string()),
        })?;
        if let Some(work) = request.work().as_deref_mut() {
            let capacity = (records.capacity() * std::mem::size_of::<Bytes>()) as u64;
            work.retain_owned(capacity)?;
            request.adopt(capacity)?;
        }
        let mut revision = self.revision;
        let mut committed_at_ms = self.next_committed_at_ms_with_work(request.work())?;
        for (key, event) in keys.iter().zip(&events) {
            if let SchedulerEvent::Declared { spec } = event {
                self.verify_content_with_work(&spec.state, request.work()).await?;
                if projected.is_none() {
                    self.scheduler.validate_session_with_work(spec, &limits, request.work())?;
                }
            }
            if let Some(projected) = &mut projected { projected.apply(event.clone())?; }
            revision = revision.checked_add(1).ok_or_else(|| {
                coordinator_boundary(request.work(), wire::ErrorCode::Invalid,
                    "coordinator revision exhausted")
            })?;
            let canonical = crate::store::history::canonical_bytes_with_work(
                event, acyclic_stream::MAX_RECORD_BYTES as u64, request.work(),
            )?;
            let canonical_held = if request.work().is_some() { canonical.capacity() as u64 } else { 0 };
            let mut canonical_custody = crate::wire_codec::CodecQuota::from_retained(
                request.work(), canonical_held,
            );
            let canonical = canonical;
            if let Some(work) = canonical_custody.work().as_deref_mut() {
                work.hashed_bytes(canonical.len() as u64)?;
            }
            let digest = *blake3::hash(&canonical).as_bytes();
            canonical_custody.transfer();
            let encoded = encode_with_work(
                revision, operation_id, key, digest, canonical, committed_at_ms, canonical_custody.work(),
            )?;
            drop(canonical_custody);
            if request.work().is_some() {
                request.adopt(encoded.capacity() as u64)?;
            }
            records.push(Bytes::from(encoded));
            committed_at_ms = committed_at_ms.checked_add(1).ok_or_else(|| {
                coordinator_boundary(request.work(), wire::ErrorCode::Invalid,
                    "coordinator commit time exhausted")
            })?;
        }
        let stream_key = stream_key_with_work(idempotency_key.as_str(), request.work())?;
        let key_held = if request.work().is_some() { 32 } else { 0 };
        let retained_request = request.retained();
        request.transfer();
        let mut key_custody = crate::wire_codec::CodecQuota::from_retained(request.work(), key_held);
        let stream_key = stream_key;
        let appended = append_records_keyed_with_work(
            (&self.stream, &self.client), records, retained_request, Some(self.revision),
            stream_key, operation_id, "session retry", key_custody.work(),
        ).await?;
        drop(key_custody);
        let outcome = match appended {
            Ok(outcome) => outcome,
            Err(StreamError::IdempotencyMismatch) => {
                self.refresh_with_work(request.work()).await?;
                if self.session_intent_with_work(&keys, &events, request.work()).await? {
                    return Ok(CoordinatorApply::Replayed);
                }
                return Err(coordinator_boundary(request.work(), wire::ErrorCode::Conflict,
                    "session retry identity reused"));
            }
            Err(error) => return Err(append_error_with_work(error, "session retry", request.work())),
        };
        self.finish_session_append_with_work(
            outcome, &keys, &events, revision - 2, request.work(),
        ).await
    }

    async fn session_intent_with_work(
        &mut self,
        keys: &[String; 2],
        events: &[SchedulerEvent; 2],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut retained = 0;
        let mut previous: Option<(u64, acyclic_stream::CommitId)> = None;
        for (key, event) in keys.iter().zip(events) {
            let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
            if let Some(intent) = self.retained_intent_with_work(key, custody.work()).await? {
                if custody.work().is_some() && matches!(&intent, std::borrow::Cow::Owned(_)) {
                    custody.adopt(intent.1.logical_owned_bytes()?)?;
                }
                let (digest, existing, revision, commit_id) = intent.as_ref();
                if existing != event {
                    return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                        "session retry identity reused"));
                }
                let canonical = crate::store::history::canonical_bytes_with_work(
                    event, acyclic_stream::MAX_RECORD_BYTES as u64, custody.work(),
                )?;
                if custody.work().is_some() { custody.adopt(canonical.capacity() as u64)?; }
                if let Some(work) = custody.work().as_deref_mut() { work.hashed_bytes(canonical.len() as u64)?; }
                if digest != blake3::hash(&canonical).as_bytes() {
                    return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                        "session retry identity reused"));
                }
                if let Some((prior, envelope)) = previous
                    && (prior.checked_add(1) != Some(*revision) || envelope != *commit_id)
                {
                    return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                        "session declaration is not one atomic envelope"));
                }
                previous = Some((*revision, *commit_id));
                retained += 1;
            }
        }
        match retained {
            0 => Ok(false),
            2 => Ok(true),
            _ => Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "session declaration is not atomic")),
        }
    }

    async fn finish_session_append_with_work(
        &mut self,
        outcome: AppendOutcome,
        keys: &[String; 2],
        events: &[SchedulerEvent; 2],
        expected_start: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        let (result, receipt) = match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(2) || receipt.tail < receipt.end {
                    return Err(coordinator_boundary(work, wire::ErrorCode::Storage,
                        "invalid session append receipt"));
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
        self.refresh_with_work(work).await?;
        if !self.session_intent_with_work(keys, events, work).await? {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "session append conflicted without its complete intent"));
        }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        if let Some((envelope, start)) = receipt {
            let intent = self.retained_intent_with_work(&keys[0], custody.work()).await?;
            if custody.work().is_some()
                && let Some(std::borrow::Cow::Owned(intent)) = &intent
            {
                custody.adopt(intent.1.logical_owned_bytes()?)?;
            }
            if intent.as_ref().is_none_or(|intent| {
                let (_, _, revision, commit_id) = intent.as_ref();
                *commit_id != envelope || revision.checked_sub(1) != Some(start)
            }) {
                return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                    "session append receipt differs from its committed envelope"));
            }
        }
        Ok(result)
    }

    fn replay_intent_with_work(
        &self, event: &SchedulerEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        if let SchedulerEvent::ModelDispatchClaimed {
            operation_id,
            fence,
            ..
        } = event
        {
            let operation = self
                .scheduler
                .operation(*operation_id)
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound, "model task"))?;
            crate::scheduler::require_execution_owner_with_work(operation, fence, false, work)?;
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
        self.apply_internal_guarded_with_work(
            operation_id, idempotency_key, event, parent_fence, waiting_command,
            idle_execution, None, None, &mut None,
        ).await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one original publication body preserves its independent atomic guards"
    )]
    async fn apply_internal_guarded_with_work(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
        parent_fence: Option<LeaseFence>,
        waiting_command: Option<OperationId>,
        idle_execution: Option<(OperationId, u64)>,
        workflow_guard: Option<(&str, u64)>,
        verified_revision: Option<u64>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        self.refresh_with_work(work).await?;
        if verified_revision.is_some_and(|revision| revision != self.revision) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "coordinator changed after owner verification"));
        }
        self.require_parent_owner_with_work(&event, parent_fence.as_ref(), work)?;
        let validated = text_with_work(idempotency_key.as_str(), work)?;
        let validation_capacity = validated.capacity() as u64;
        let validation = checked_idempotency_key(validated, work).map(|_| ());
        if let Some(work) = work.as_deref_mut() { work.release_owned(validation_capacity)?; }
        validation?;
        let key = idempotency_key.as_str();
        if event.operation_id() != operation_id {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "scheduler event belongs to another operation"));
        }
        let canonical = crate::store::history::canonical_bytes_with_work(
            &event, acyclic_stream::MAX_RECORD_BYTES as u64, work,
        )?;
        let mut canonical_held = canonical.capacity() as u64;
        let mut bytes_held = 0u64;
        let mut key_held = 0u64;
        let result = async {
            if let Some(work) = work.as_deref_mut() { work.hashed_bytes(canonical.len() as u64)?; }
            let digest = *blake3::hash(&canonical).as_bytes();
            if let Some(matches) = self.intent_matches_with_work(key, &event, &digest, work).await? {
                return if matches {
                    // A retained accounting receipt is not an execution permit.
                    self.require_command_wait_with_work(&event, waiting_command, true, work)?;
                    self.replay_intent_with_work(&event, work)
                } else {
                    Err(coordinator_boundary(work, wire::ErrorCode::Conflict, "coordinator retry identity reused"))
                };
            }
            self.require_command_wait_with_work(&event, waiting_command, false, work)?;
            if let SchedulerEvent::Declared { spec } = &event {
                self.verify_content_with_work(&spec.state, work).await?;
            }
            self.verify_published_result_with_work(&event, work).await?;
            self.scheduler.validate_event_with_work(&event, work)?;
            let revision = next_revision(self.revision)?;
            let committed_at_ms = self.next_committed_at_ms_with_work(work)?;
            canonical_held = 0; // the original envelope producer acquires input custody
            let bytes = encode_with_work(
                revision, operation_id, key, digest, canonical, committed_at_ms, work,
            )?;
            bytes_held = bytes.capacity() as u64;
            // Failed CAS requests remain keyed by the exact original physical
            // identity; logical canonical intent still prevents republishing.
            let stream_key = physical_key_with_work(
                key, self.revision, parent_fence.is_some(), waiting_command.is_some(), idle_execution, work,
            )?;
            key_held = 32;
            let record = Bytes::from(bytes);
            let appended = if let Some((execution, expected_tail)) = idle_execution {
                append_if_execution_idle_with_work(
                    &self.client, &self.stream, record, self.revision, stream_key,
                    operation_id, execution, expected_tail, workflow_guard, work,
                ).await
            } else {
                append_keyed_with_work(
                    (&self.stream, &self.client), record, Some(self.revision),
                    stream_key, operation_id, "coordinator retry identity", work,
                ).await
            };
            if let Some(work) = work.as_deref_mut() {
                work.release_owned(bytes_held + key_held)?;
            }
            bytes_held = 0;
            key_held = 0;
            let outcome = match appended {
                Ok(outcome) => outcome,
                Err(error) if idle_execution.is_none() && coordinator_retry_conflict(&error) => {
                    // Preserve the original reconciliation of envelope timestamp
                    // differences against the retained logical event.
                    self.refresh_with_work(work).await?;
                    if self.intent_matches_with_work(key, &event, &digest, work).await? == Some(true) {
                        self.require_command_wait_with_work(&event, waiting_command, true, work)?;
                        self.require_parent_owner_with_work(&event, parent_fence.as_ref(), work)?;
                        return self.replay_intent_with_work(&event, work);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            let applied = self.finish_append_with_work(outcome, &event, key, &digest, revision, work).await?;
            self.require_parent_owner_with_work(&event, parent_fence.as_ref(), work)?;
            Ok(applied)
        }.await;
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(canonical_held + bytes_held + key_held)?;
        }
        result
    }

    fn require_command_wait_with_work(
        &self,
        event: &SchedulerEvent,
        waiting: Option<OperationId>,
        replaying: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "command wait guard only applies to wake"));
        };
        let operation = self
            .scheduler
            .operation(*operation_id)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound, "waiting task"))?;
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
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "selected command wait is no longer current"));
        }
        Ok(())
    }

    fn require_parent_owner_with_work(
        &self,
        event: &SchedulerEvent,
        fence: Option<&LeaseFence>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let Some(fence) = fence else {
            return Ok(());
        };
        let SchedulerEvent::Declared { spec } = event else {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "parent fence only applies to declaration"));
        };
        let parent = spec
            .parent
            .as_ref()
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Invalid, "child declaration has no parent"))?;
        let operation = self
            .scheduler
            .operation(parent.operation_id)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound, "admitting parent"))?;
        crate::scheduler::require_execution_owner_with_work(operation, fence, false, work)
    }

    async fn finish_append(
        &mut self,
        outcome: AppendOutcome,
        event: &SchedulerEvent,
        key: &str,
        digest: &[u8; 32],
        revision: u64,
    ) -> Result<CoordinatorApply> {
        self.finish_append_with_work(outcome, event, key, digest, revision, &mut None).await
    }

    async fn finish_append_with_work(
        &mut self,
        outcome: AppendOutcome,
        event: &SchedulerEvent,
        key: &str,
        digest: &[u8; 32],
        revision: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(1) || receipt.tail < receipt.end {
                    return Err(coordinator_boundary(work, wire::ErrorCode::Storage, "invalid coordinator append receipt"));
                }
                self.refresh_with_work(work).await?;
                match self.intent_matches_with_work(key, event, digest, work).await? {
                    Some(true) => {
                        self.replay_intent_with_work(event, work)?;
                        Ok(if receipt.start == revision - 1 {
                            CoordinatorApply::Applied
                        } else {
                            CoordinatorApply::Replayed
                        })
                    }
                    _ => Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                        "coordinator committed identity differs from its intent")),
                }
            }
            AppendOutcome::TailConflict { actual_tail } => {
                self.refresh_with_work(work).await?;
                match self.intent_matches_with_work(key, event, digest, work).await? {
                    Some(true) => self.replay_intent_with_work(event, work),
                    Some(false) => Err(coordinator_boundary(work, wire::ErrorCode::Conflict, "coordinator retry identity reused")),
                    None => Err(coordinator_error(work, wire::ErrorCode::Conflict,
                        "coordinator tail changed", || Error::Conflict(format!("coordinator tail is {actual_tail}")))),
                }
            }
        }
    }


    fn next_committed_at_ms_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        let after_previous = self
            .last_committed_at_ms
            .checked_add(1)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Invalid, "coordinator commit time exhausted"))?;
        Ok(current_time_millis_with_work(work)?.max(after_previous))
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
        self.pull_owned_operation_with_work(
            owner, scope, verifier, worker, operation_id, &mut None,
        ).await
    }

    pub(crate) async fn pull_owned_operation_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        worker: &Worker,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<WorkPull> {
        self.pull_owned_target_with_work(
            owner, scope, verifier, worker, Some(operation_id), work,
        ).await
    }

    async fn pull_owned_target(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        worker: &Worker,
        operation_id: Option<OperationId>,
    ) -> Result<WorkPull> {
        self.pull_owned_target_with_work(
            owner, scope, verifier, worker, operation_id, &mut None,
        ).await
    }

    async fn pull_owned_target_with_work(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        worker: &Worker,
        operation_id: Option<OperationId>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<WorkPull> {
        verifier.verify_audience_with_work(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains("operation:observe")
            || !scope.capabilities().contains("operation:declare")
        {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "owned pull requires operation:declare and operation:observe"));
        }
        self.pull_for_with_work(worker, Some((owner, verifier)), operation_id, work).await
    }

    fn verify_pull_owner_with_work(
        &self,
        authorization: Option<(&Authority, &AuthorityVerifier)>,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some((owner, verifier)) = authorization {
            self.scheduler
                .operation(operation_id)
                .ok_or_else(|| coordinator_error(work, wire::ErrorCode::NotFound,
                    "owned pull operation is absent", || Error::NotFound(format!("operation {operation_id}"))))?
                .spec
                .verify_owner_with_work(owner, verifier, work)?;
        }
        Ok(())
    }

    async fn pull_for(
        &mut self,
        worker: &Worker,
        authorization: Option<(&Authority, &AuthorityVerifier)>,
        target: Option<OperationId>,
    ) -> Result<WorkPull> {
        self.pull_for_with_work(worker, authorization, target, &mut None).await
    }

    async fn pull_for_with_work(
        &mut self,
        worker: &Worker,
        authorization: Option<(&Authority, &AuthorityVerifier)>,
        target: Option<OperationId>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<WorkPull> {
        let owner = authorization.map(|(owner, _)| owner);
        self.refresh_with_work(work).await?;
        if worker.id.trim().is_empty() {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid, "worker identity is empty"));
        }
        if let Some(target) = target {
            let state = self
                .scheduler
                .operation(target)
                .filter(|state| owner.is_some_and(|owner| state.spec.owner.authority() == owner))
                .ok_or_else(|| coordinator_error(work, wire::ErrorCode::NotFound,
                    "owned pull target is absent", || Error::NotFound(format!("operation {target}"))))?;
            self.verify_pull_owner_with_work(authorization, target, work)?;
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
            self.verify_pull_owner_with_work(authorization, operation_id, work)?;
            let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
            let key = formatted_text_with_work(
                "dependency-rejected:".len() + 36,
                format_args!("dependency-rejected:{operation_id}"), custody.work(),
            )?;
            if custody.work().is_some() { custody.adopt(key.capacity() as u64)?; }
            let key = checked_idempotency_key(key, custody.work())?;
            let reason = text_with_work("a required dependency did not succeed", custody.work())?;
            if custody.work().is_some() { custody.adopt(reason.capacity() as u64)?; }
            self.apply_with_work(
                operation_id, key, SchedulerEvent::Rejected { operation_id, reason }, custody.work(),
            ).await?;
            return Ok(WorkPull::Idle);
        }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let available = self.scheduler.available_for_with_work(
            &worker.id, &worker.available, custody.work(),
        )?;
        let available_bytes = if custody.work().is_some() { available.logical_owned_bytes()? } else { 0 };
        custody.adopt(available_bytes)?;
        let selected = match target {
            Some(target) => self
                .scheduler
                .operation_ready_for(target, &available, &worker.labels, owner)
                .then_some(target),
            None => self
                .scheduler
                .next_ready_for(&available, &worker.labels, owner),
        };
        drop(available);
        custody.relinquish(available_bytes)?;
        let Some(operation_id) = selected else {
            return Ok(WorkPull::Idle);
        };
        self.verify_pull_owner_with_work(authorization, operation_id, custody.work())?;
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| coordinator_error(custody.work(), wire::ErrorCode::NotFound,
                "selected owned operation is absent", || Error::NotFound(format!("operation {operation_id}"))))?;
        let operation_revision = state.revision.checked_add(1)
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "operation revision exhausted"))?;
        let operation = state.spec.clone_with_work(custody.work())?;
        if custody.work().is_some() { custody.adopt(operation.logical_owned_bytes()?)?; }
        let checkpoint = state.checkpoint.as_ref()
            .map(|checkpoint| checkpoint.clone_with_work(custody.work())).transpose()?;
        if custody.work().is_some()
            && let Some(checkpoint) = &checkpoint
        {
            custody.adopt(checkpoint.logical_owned_bytes()?)?;
        }
        let revision = next_revision(self.revision)?;
        let revision_digits = revision.checked_ilog10().unwrap_or(0) as usize + 1;
        let reservation_id = formatted_text_with_work(
            worker.id.len() + 1 + 36 + 1 + revision_digits,
            format_args!("{}:{operation_id}:{revision}", worker.id), custody.work(),
        )?;
        if custody.work().is_some() { custody.adopt(reservation_id.capacity() as u64)?; }
        let placement = text_with_work(&worker.id, custody.work())?;
        if custody.work().is_some() { custody.adopt(placement.capacity() as u64)?; }
        let admitted = operation.resources.clone_with_work(custody.work())?;
        if custody.work().is_some() { custody.adopt(admitted.logical_owned_bytes()?)?; }
        let mut lease = WorkLease {
            operation,
            reservation: Reservation { id: reservation_id, placement, admitted },
            checkpoint,
            operation_revision,
        };
        let appended = {
            let mut attempt = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
            let reservation = lease.reservation.clone_with_work(attempt.work())?;
            if attempt.work().is_some() { attempt.adopt(reservation.logical_owned_bytes()?)?; }
            let key = formatted_text_with_work(
                "pull:".len() + worker.id.len() + 1 + 36 + 1 + revision_digits,
                format_args!("pull:{}:{operation_id}:{revision}", worker.id), attempt.work(),
            )?;
            if attempt.work().is_some() { attempt.adopt(key.capacity() as u64)?; }
            self.apply_with_work(
                operation_id, checked_idempotency_key(key, attempt.work())?,
                SchedulerEvent::Admitted { operation_id, reservation }, attempt.work(),
            ).await
        };
        if let Err(error) = appended {
            if custody.work().is_some() && error.is_native_failure() { return Err(error); }
            custody.transfer();
            return Ok(WorkPull::Unresolved { lease, error });
        }
        lease.operation_revision = self
            .scheduler
            .operation(operation_id)
            .map_or(0, |value| value.revision);
        custody.transfer();
        Ok(WorkPull::Claimed(lease))
    }

    /// Returns a crashed worker's exact lease to admission without losing its checkpoint.
    pub async fn release_lease(
        &mut self,
        lease: &WorkLease,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.release_lease_with_work(lease, idempotency_key, &mut None).await
    }

    pub(crate) async fn release_lease_with_work(
        &mut self,
        lease: &WorkLease,
        idempotency_key: IdempotencyKey,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CoordinatorApply> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let reservation_id = text_with_work(&lease.reservation.id, custody.work())?;
        if custody.work().is_some() { custody.adopt(reservation_id.capacity() as u64)?; }
        let placement = text_with_work(&lease.reservation.placement, custody.work())?;
        if custody.work().is_some() { custody.adopt(placement.capacity() as u64)?; }
        self.apply_internal_guarded_with_work(
            lease.operation.operation_id, idempotency_key,
            SchedulerEvent::LeaseReleased {
                operation_id: lease.operation.operation_id,
                fence: LeaseFence { reservation_id, placement },
            }, None, None, None, None, None, custody.work(),
        ).await
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
        self.load_json_with_work(reference, &mut None).await.map(|(value, _)| value)
    }

    async fn load_json_with_work(
        &self,
        reference: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(serde_json::Value, u64)> {
        reference.validate_with_work(work)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "scheduler result must be a JSON file"));
        }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let bytes = match custody.work().as_deref_mut() {
            Some(work) => crate::store::history::read_content_with_work(
                work, self.content_verifier.as_ref(), reference).await?,
            None => self.content_verifier.read(reference).await?,
        };
        let raw_bytes = if custody.work().is_some() { bytes.capacity() as u64 } else { 0 };
        custody.adopt(raw_bytes)?;
        reference.descriptor().verify_with_work(&bytes, custody.work())?;
        let (value, value_bytes) = decode_result_json_with_work(&bytes, custody.work())?;
        custody.adopt(value_bytes)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &value, reference.descriptor().byte_length(), custody.work(),
        )?;
        let canonical_bytes = if custody.work().is_some() { canonical.capacity() as u64 } else { 0 };
        custody.adopt(canonical_bytes)?;
        if canonical != bytes {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                "scheduler result is not canonical JSON"));
        }
        drop(canonical);
        custody.relinquish(canonical_bytes)?;
        drop(bytes);
        custody.relinquish(raw_bytes)?;
        custody.transfer();
        Ok((value, value_bytes))
    }

    async fn assemble_values(
        &self,
        assembly: AssemblyKind,
        values: &[(String, FileRef)],
    ) -> Result<serde_json::Value> {
        self.assemble_values_with_work(assembly, values, &mut None).await.map(|(value, _)| value)
    }

    async fn assemble_values_with_work(
        &self,
        assembly: AssemblyKind,
        values: &[(String, FileRef)],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(serde_json::Value, u64)> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut assembled = if custody.work().is_some() {
            Vec::new()
        } else { Vec::with_capacity(values.len()) };
        for (slot, reference) in values {
            let old_capacity = assembled.capacity();
            if let Some(work) = custody.work().as_deref_mut() {
                work.reserve_values(&mut assembled, values.len())?;
                custody.adopt(((assembled.capacity() - old_capacity)
                    * std::mem::size_of::<serde_json::Value>()) as u64)?;
            }
            let (value, value_bytes) = self.load_json_with_work(reference, custody.work()).await?;
            custody.adopt(value_bytes)?;
            let value = match assembly {
                AssemblyKind::Join if custody.work().is_some() => {
                    let copied_slot = text_with_work(slot, custody.work())?;
                    custody.adopt(copied_slot.capacity() as u64)?;
                    let slot_key = text_with_work("slot", custody.work())?;
                    custody.adopt(slot_key.capacity() as u64)?;
                    let value_key = text_with_work("value", custody.work())?;
                    custody.adopt(value_key.capacity() as u64)?;
                    let slots = (2 * std::mem::size_of::<(String, serde_json::Value)>()) as u64;
                    let work = custody.work().as_deref_mut().expect("observed join fields");
                    work.admit_owned(slots)?;
                    work.retain_owned(slots)?;
                    custody.adopt(slots)?;
                    let mut fields = serde_json::Map::new();
                    fields.insert(slot_key, serde_json::Value::String(copied_slot));
                    fields.insert(value_key, value);
                    serde_json::Value::Object(fields)
                }
                AssemblyKind::Join => serde_json::json!({"slot": slot, "value": value}),
                AssemblyKind::Quorum => value,
            };
            assembled.push(value);
        }
        let retained = custody.retained();
        custody.transfer();
        Ok((serde_json::Value::Array(assembled), retained))
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

    fn apply_committed_with_work(
        &mut self,
        position: (u64, acyclic_stream::CommitId),
        operation_id: OperationId,
        key: String,
        digest: [u8; 32],
        committed_at_ms: u64,
        event: SchedulerEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let retained = if work.is_some() {
            event.logical_owned_bytes()? + key.capacity() as u64
        } else { 0 };
        let mut custody = crate::wire_codec::CodecQuota::from_retained(work, retained);
        let key = key;
        let event = event;
        let (revision, commit_id) = position;
        if revision != next_revision(self.revision)? || event.operation_id() != operation_id {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                "invalid committed coordinator event"));
        }
        if committed_at_ms <= self.last_committed_at_ms {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "coordinator commit time is not increasing"));
        }
        let copied_key = text_with_work(&key, custody.work())?;
        let key_capacity = copied_key.capacity() as u64;
        let validation = checked_idempotency_key(copied_key, custody.work()).map(|_| ());
        if let Some(work) = custody.work().as_deref_mut() { work.release_owned(key_capacity)?; }
        validation?;
        if self.intents.contains_key(&key) {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                "coordinator history repeats an idempotency key"));
        }
        let (key, cached) = prepare_cached_intent(key, (digest, event, revision, commit_id), custody.work())?;
        if custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
            custody.transfer();
        }
        self.scheduler.apply_with_work(&cached.value.1, custody.work())?;
        self.revision = revision;
        self.last_committed_at_ms = committed_at_ms;
        self.cache_intent(key, cached);
        custody.transfer();
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
) -> Result<Vec<u8>> {
    encode_with_work(revision, operation_id, key, digest, canonical, committed_at_ms, &mut None)
}

fn checked_idempotency_key(
    value: String,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<IdempotencyKey> {
    if work.as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid, "idempotency key is invalid"));
        }
        return Ok(IdempotencyKey(value));
    }
    IdempotencyKey::new(value)
}

fn text_with_work(
    value: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    match work.as_deref_mut() {
        Some(work) => work.clone_text(value),
        None => Ok(value.to_owned()),
    }
}

fn formatted_text_with_work(
    length: usize,
    arguments: std::fmt::Arguments<'_>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    use std::fmt::Write as _;
    let mut text = String::new();
    if let Some(work) = work.as_deref_mut() {
        work.reserve_text(&mut text, length)?;
        if let Err(error) = work.copy_bytes(length as u64) {
            let capacity = text.capacity() as u64;
            drop(text);
            work.release_owned(capacity)?;
            return Err(error);
        }
    } else {
        text.reserve_exact(length);
    }
    if text.write_fmt(arguments).is_err() {
        let capacity = text.capacity() as u64;
        drop(text);
        if let Some(work) = work.as_deref_mut() { work.release_owned(capacity)?; }
        return Err(coordinator_boundary(work, wire::ErrorCode::Storage, "coordinator identity formatting failed"));
    }
    Ok(text)
}

fn encode_with_work(
    revision: u64,
    operation_id: OperationId,
    key: &str,
    digest: [u8; 32],
    canonical: Vec<u8>,
    committed_at_ms: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Vec<u8>> {
    let incoming_held = if work.is_some() { canonical.capacity() as u64 } else { 0 };
    let mut custody = crate::wire_codec::CodecQuota::from_retained(work, incoming_held);
    let canonical = canonical;
    let version = text_with_work(COORDINATOR_WIRE_VERSION, custody.work())?;
    if custody.work().is_some() { custody.adopt(version.capacity() as u64)?; }
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(COORDINATOR_WIRE_CONTRACT.len() as u64)?;
    }
    let hash = blake3::hash(COORDINATOR_WIRE_CONTRACT).to_hex();
    let descriptor_digest = text_with_work(hash.as_str(), custody.work())?;
    if custody.work().is_some() { custody.adopt(descriptor_digest.capacity() as u64)?; }
    let operation_id = formatted_text_with_work(36, format_args!("{operation_id}"), custody.work())?;
    if custody.work().is_some() { custody.adopt(operation_id.capacity() as u64)?; }
    let idempotency_key = text_with_work(key, custody.work())?;
    if custody.work().is_some() { custody.adopt(idempotency_key.capacity() as u64)?; }
    let mut event_digest = Vec::new();
    if let Some(work) = custody.work().as_deref_mut() {
        work.reserve_bytes(&mut event_digest, digest.len())?;
        custody.adopt(event_digest.capacity() as u64)?;
        if let Some(work) = custody.work().as_deref_mut() { work.copy_bytes(digest.len() as u64)?; }
    } else {
        event_digest.reserve_exact(digest.len());
    }
    event_digest.extend_from_slice(&digest);
    let envelope = wire::SchedulerEventEnvelope {
        protocol: Some(wire::ProtocolIdentity { version, descriptor_digest }),
        revision, operation_id, idempotency_key,
        canonical_event_json: canonical, event_digest,
        committed_at_ms: Some(committed_at_ms),
    };
    let length = envelope.encoded_len();
    let mut bytes = Vec::new();
    if let Some(work) = custody.work().as_deref_mut() {
        work.admit_encoding(length as u64)?;
        work.reserve_bytes(&mut bytes, length)?;
        custody.adopt(bytes.capacity() as u64)?;
    } else {
        bytes.reserve_exact(length);
    }
    let encoded = envelope.encode(&mut bytes);
    if let Some(work) = custody.work().as_deref_mut() {
        work.encoded_bytes(bytes.len() as u64)?;
    }
    encoded.map_err(|source| coordinator_error(custody.work(), wire::ErrorCode::Storage,
        "scheduler envelope encoding failed", || Error::Storage(source.to_string())))?;
    let retained_output = if custody.work().is_some() { bytes.capacity() as u64 } else { 0 };
    drop(envelope);
    let metadata_bytes = custody.retained() - retained_output;
    custody.relinquish(metadata_bytes)?;
    custody.transfer();
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<(u64, OperationId, String, [u8; 32], u64, SchedulerEvent)> {
    decode_with_work(bytes, &mut None)
}

// Closed to SchedulerEvent's original Deserialize schema and serde 1.0.229's
// Content carrier. This is logical quota, not observed allocation or SDK/OS IO.
// Reachable DTO/serde changes require updating this concrete admission audit.
struct SchedulerEventJsonAdmission<'a,'w> {
    input: &'a [u8],
    work: &'w mut dyn crate::store::history::HistoryIo,
    held: u64,
}

impl<'a,'w> SchedulerEventJsonAdmission<'a,'w> {
    fn prepare(input: &'a [u8], work: &'w mut dyn crate::store::history::HistoryIo) -> Result<Self> {
        work.check_cancellation()?;
        Ok(Self { input, work, held: 0 })
    }

    fn maximum_owned_bytes(input_bytes: u64, items: u64, native: bool) -> Result<u64> {
        let multiply = |value:u64, factor:u64| value.checked_mul(factor)
            .ok_or_else(|| scheduler_json_admission_error(native, "scheduler JSON ownership admission overflows"));
        const fn largest(slots:&[usize]) -> usize {
            let mut maximum = 0;
            let mut index = 0;
            while index < slots.len() {
                if slots[index] > maximum {maximum = slots[index];}
                index += 1;
            }
            maximum
        }
        // Exactly the variable typed elements: dependencies/cancel OperationIds,
        // scope capabilities, resource quantities, placement pairs, descriptor/
        // resource-key bytes and arbitrary schema Value. BTree payload slots
        // count; opaque node headers/allocator slack/RSS do not.
        const TYPED_SLOT:usize = largest(&[
            std::mem::size_of::<OperationId>(), std::mem::size_of::<u8>(),
            std::mem::size_of::<String>(), std::mem::size_of::<(String,u64)>(),
            std::mem::size_of::<(String,String)>(), std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<(String,serde_json::Value)>(),
        ]);
        const CONTENT_SLOT:usize = largest(&[
            std::mem::size_of::<serde::__private229::de::Content<'static>>(),
            std::mem::size_of::<(serde::__private229::de::Content<'static>,
                serde::__private229::de::Content<'static>)>(),
        ]);
        // Outer SchedulerEvent, one outcome/owner/orchestration carrier and
        // adjacent VolumeOwner. DurableOwner and Orchestration are parallel;
        // no longer tagged chain occurs in any of the 17 original variants.
        // JSON Content doesn't produce its non-JSON Some/Newtype box variants.
        const CONTENT_LAYERS:u64 = 3;
        // <=2 elements plus eight initial Vec slots per nonempty collection;
        // total collections/elements <= lexical tokens, including duplicate keys.
        let slots = multiply(items, 2 + 8)?;
        let content_bytes = multiply(multiply(slots, CONTENT_SLOT as u64)?, CONTENT_LAYERS)?;
        let typed_bytes = multiply(slots, TYPED_SLOT as u64)?;
        const VALUE_SLOT:usize = largest(&[std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<(String,serde_json::Value)>()]);
        let value_overlap = multiply(slots, VALUE_SLOT as u64)?;
        // Every Content pass, Value pass and typed output can own source strings.
        // Decoded escaping never exceeds source bytes; scratch growth is <=2x.
        let strings = multiply(input_bytes, CONTENT_LAYERS + 2)?;
        let scratch = multiply(input_bytes.max(8), 2)?;
        const LABELS:&str = concat!(
            "SchedulerEvent SessionConfigured ModelDispatchClaimed Declared WaitingForCapacity ",
            "Admitted PartiallyAdmitted Rejected Started Checkpointed WaitingForChildren ",
            "WorkflowSuspended WorkflowResumed ReconciliationResumed LeaseReleased ",
            "CancellationRequested Completed Orchestrated ",
            "session_configured model_dispatch_claimed declared waiting_for_capacity admitted ",
            "partially_admitted rejected started checkpointed waiting_for_children workflow_suspended ",
            "workflow_resumed reconciliation_resumed lease_released cancellation_requested completed orchestrated ",
            "operation_id limits active_tasks total_tasks depth model_steps attempt_id step request_digest ",
            "fence ceiling spec reservation reason checkpoint workflow_revision waiting_command input recursive ",
            "outcome execution_duration_ns expected_revision cancel reducer reduction_digest reservation_id ",
            "placement id admitted parent slot owner owner_scope entrypoint dependencies resources orchestration ",
            "state authority kind attached detached leaf join race quorum reduce required name version digest ",
            "result_schema capabilities agent issuer parent_proof proof Succeeded Failed Cancelled Indeterminate message ",
            "namespace family provider key volume path descriptor display_name sha256 byte_length media_type class ",
            "workspace generation artifact sandbox checkpoint stream context run project session agent ",
            "user agent_private session_shared Authority Scope SessionLimits DurableOwner ParentLink ",
            "EntrypointRef Orchestration Reservation LeaseFence Outcome VolumeClass VolumeOwner VolumeRefWire ",
            "FileDescriptorWire FileRefWire ProviderRefWire ResourceRefWire ResourceKind Capabilities ",
            "conversation turn task ",
            "provider identity is invalid resource key is empty resource version is invalid typed resource kind mismatch ",
            "SHA-256 digest must contain exactly 32 bytes file or volume label is invalid ",
            "volume-relative path is invalid file byte length exceeds cross-language precision media type is invalid ",
            "invalid request: storage failure: a valid UUID string an array of length 32 ",
        );
        // Four punctuation bytes per label, conservatively at most one label
        // per source character. Shared codec bounds actual dynamic interpolation
        // and the two overlapping diagnostic buffers before serde constructs them.
        let diagnostics = crate::contract::audited_serde_diagnostic_bytes(
            input_bytes, LABELS.len() as u64 * 5,
        )?;
        // Declared's Box<OperationSpec> is the only fixed heap-owned pointee.
        let maximum_owned_bytes = [content_bytes, typed_bytes, value_overlap, strings,
            scratch, diagnostics, std::mem::size_of::<OperationSpec>() as u64]
            .into_iter().try_fold(0u64, |total, bytes| total.checked_add(bytes)
                .ok_or_else(|| scheduler_json_admission_error(native, "scheduler JSON quota admission overflows")))?;
        Ok(maximum_owned_bytes)
    }

    fn decode(mut self) -> Result<SchedulerEvent> {
        use crate::contract::JsonCodecCharge;
        let native = crate::store::operations::original_native_decode_mode(self.work);
        let mut input_bytes = 0u64;
        let mut items = 0u64;
        let mut observed_failure = false;
        let decoded = crate::store::operations::decode_preclaimed_json_with_mode::<SchedulerEvent, _>(
            self.input, crate::contract::JsonBounds {
                maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
                maximum_depth: 256,
            }, 0, wire::ErrorCode::Storage, native, crate::contract::JsonValueSchema::SchedulerEvent, &mut |charge| {
                let charged = (|| {
                    match charge {
                        JsonCodecCharge::Read { bytes } => {
                            input_bytes = input_bytes.checked_add(bytes).ok_or_else(||
                                scheduler_json_admission_error(native, "scheduler JSON byte admission overflows"))?;
                            self.work.codec_charge(charge)
                        }
                        JsonCodecCharge::Work { items: count, .. } => {
                            items = items.checked_add(count).ok_or_else(||
                                scheduler_json_admission_error(native, "scheduler JSON item admission overflows"))?;
                            self.work.codec_charge(charge)
                        }
                        JsonCodecCharge::Admission { value_shape, .. } => {
                            let maximum_owned_bytes = Self::maximum_owned_bytes(input_bytes, items, native)?
                                .checked_add(crate::contract::json_value_virtual_owned_bytes(value_shape)?)
                                .ok_or_else(|| scheduler_json_admission_error(native,
                                    "scheduler virtual Value admission overflows"))?;
                            self.work.codec_charge(JsonCodecCharge::Admission {
                                maximum_owned_bytes, value_shape,
                            })?;
                            self.held = maximum_owned_bytes;
                            Ok(())
                        }
                        _ => self.work.codec_charge(charge),
                    }
                })();
                if charged.is_err() { observed_failure = true; }
                charged
            },
        );
        let decoded = crate::store::operations::retain_original_decode_error(
            decoded, &mut self.held, self.work,
        );
        let event = decoded.map_err(|error| {
            if observed_failure { error } else {
                match error { Error::Invalid(message) => Error::Storage(message), other => other }
            }
        })?;
        self.work.check_cancellation()?;
        let retained = event.logical_owned_bytes()?;
        let slack = self.maximum_owned_bytes.checked_sub(retained)
            .ok_or_else(|| scheduler_json_admission_error(native,
                "scheduler decoder exceeds its audited schema reservation"))?;
        self.work.release_owned(slack)?;
        self.held = retained;
        // Transfer only the actual typed output; errors or unused witnesses
        // release their reservation through producer-local custody.
        self.held = 0;
        Ok(event)
    }
}

fn scheduler_json_admission_error(native: bool, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native { return Error::NativeBoundary { code: wire::ErrorCode::Invalid, reason }; }
    let _ = native;
    Error::Invalid(reason.into())
}

impl Drop for SchedulerEventJsonAdmission<'_,'_> {
    fn drop(&mut self) {
        // Cleanup cannot replace a cancellation/quota/serde first cause.
        let _ = self.work.release_owned(self.held);
    }
}

fn decode_result_json_with_work(
    input: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(serde_json::Value, u64)> {
    if work.is_none() {
        return crate::contract::json_from_slice(input)
            .map(|value| (value, 0))
            .map_err(|source| Error::Invalid(format!("scheduler result is invalid JSON: {source}")));
    }
    let diagnostic = crate::contract::audited_serde_diagnostic_bytes(input.len() as u64,
        "Value Number map sequence string number boolean null invalid type invalid value".len() as u64)?;
    let maximum = (input.len() as u64).checked_add(diagnostic)
        .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Invalid, "scheduler value admission overflows"))?;
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let native = custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode);
    let mut reservation = 0u64;
    let mut observed_failure = false;
    let decoded = {
        let observer = custody.work().as_deref_mut().expect("observed scheduler value");
        crate::store::operations::decode_preclaimed_json_with_mode::<serde_json::Value, _>(
            input, crate::contract::JsonBounds {
                maximum_bytes: input.len() as u64,
                maximum_items: input.len() as u64 + 1,
                maximum_depth: 256,
            }, maximum, wire::ErrorCode::Invalid, native, crate::contract::JsonValueSchema::Root, &mut |charge| {
                let charged = (|| {
                    match charge {
                        crate::contract::JsonCodecCharge::Admission { value_shape, .. } => {
                            let planned = crate::contract::json_value_decode_owned_bytes(value_shape)?
                                .checked_add(diagnostic)
                                .ok_or_else(|| scheduler_json_admission_error(native,
                                    "scheduler value ownership admission overflows"))?;
                            observer.codec_charge(crate::contract::JsonCodecCharge::Admission {
                                maximum_owned_bytes: planned, value_shape,
                            })?;
                            reservation = planned;
                            Ok(())
                        }
                        _ => observer.codec_charge(charge),
                    }
                })();
                if charged.is_err() { observed_failure = true; }
                charged
            },
        )
    };
    let decoded = crate::store::operations::retain_original_decode_error(
        decoded, &mut reservation, custody.work().as_deref_mut().expect("observed scheduler value"),
    );
    custody.adopt(reservation)?;
    let value = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Invalid(format!("scheduler result is invalid JSON: {message}")),
                other => other,
            }
        }
    })?;
    let retained = crate::scheduler::json_value_owned_bytes(&value)?;
    let slack = reservation.checked_sub(retained).ok_or_else(|| scheduler_json_admission_error(native,
        "scheduler value exceeds its concrete admission"))?;
    custody.relinquish(slack)?;
    custody.transfer();
    Ok((value, retained))
}

fn scheduler_envelope_admission(
    mut input: &[u8],
    protocol: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(u64, u64)> {
    use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
    let mut payload = 0u64;
    let mut buffers = 0u64;
    while !input.is_empty() {
        if let Some(work) = work.as_deref_mut() {
            work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        let (tag, kind) = decode_key(&mut input).map_err(|source| coordinator_error(
            work, wire::ErrorCode::Storage, "invalid scheduler protobuf field",
            || Error::Storage(source.to_string()),
        ))?;
        let nested = !protocol && tag == 1;
        let owned = if protocol { matches!(tag, 1 | 2) } else { matches!(tag, 3 | 4 | 5 | 6) };
        if kind == WireType::LengthDelimited && (nested || owned) {
            let length = decode_varint(&mut input).map_err(|source| coordinator_error(
                work, wire::ErrorCode::Storage, "invalid scheduler protobuf length",
                || Error::Storage(source.to_string()),
            ))?;
            let length = usize::try_from(length).ok().filter(|length| *length <= input.len())
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Storage,
                    "scheduler protobuf field exceeds its frame"))?;
            let (value, rest) = input.split_at(length);
            input = rest;
            let (bytes, count) = if nested {
                scheduler_envelope_admission(value, true, work)?
            } else { (length as u64, 1) };
            payload = payload.checked_add(bytes).ok_or_else(|| coordinator_boundary(
                work, wire::ErrorCode::Storage, "scheduler protobuf payload admission overflows",
            ))?;
            buffers = buffers.checked_add(count).ok_or_else(|| coordinator_boundary(
                work, wire::ErrorCode::Storage, "scheduler protobuf buffer admission overflows",
            ))?;
        } else {
            skip_field(kind, tag, &mut input, DecodeContext::default())
                .map_err(|source| coordinator_error(work, wire::ErrorCode::Storage,
                    "invalid scheduler protobuf framing", || Error::Storage(source.to_string())))?;
        }
    }
    Ok((payload, buffers))
}

fn decode_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(u64, OperationId, String, [u8; 32], u64, SchedulerEvent)> {
    let diagnostic = if work.is_some() {
        crate::contract::audited_serde_diagnostic_bytes(bytes.len() as u64, concat!(
            "SchedulerEventEnvelope ProtocolIdentity protocol revision operation_id idempotency_key ",
            "canonical_event_json event_digest committed_at_ms version descriptor_digest ",
            "failed to decode Protobuf message buffer underflow invalid varint invalid wire type ",
            "invalid tag value unexpected end group tag recursion limit reached invalid string value ",
        ).len() as u64)?
    } else { 0 };
    let mut custody = crate::wire_codec::CodecQuota::new(work, diagnostic)?;
    let mut admitted = diagnostic;
    if custody.work().is_some() {
        let (payload, buffers) = scheduler_envelope_admission(bytes, false, custody.work())?;
        let fields = payload.checked_mul(2).and_then(|payload| buffers.checked_mul(8)
            .and_then(|minimum| payload.checked_add(minimum)))
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "scheduler protobuf carrier admission overflows"))?;
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_owned(fields)?;
            work.retain_owned(fields)?;
        }
        custody.adopt(fields)?;
        admitted = admitted.checked_add(fields).ok_or_else(|| coordinator_boundary(
            custody.work(), wire::ErrorCode::Storage, "scheduler protobuf admission overflows",
        ))?;
    }
    let envelope = wire::SchedulerEventEnvelope::decode(bytes)
        .map_err(|source| coordinator_error(custody.work(), wire::ErrorCode::Storage,
            "invalid scheduler protobuf envelope", || Error::Storage(source.to_string())))?;
    let retained = if custody.work().is_some() {
        (envelope.canonical_event_json.capacity() + envelope.event_digest.capacity()
            + envelope.operation_id.capacity() + envelope.idempotency_key.capacity()
            + envelope.protocol.as_ref().map_or(0, |protocol| {
                protocol.version.capacity() + protocol.descriptor_digest.capacity()
            })) as u64
    } else { 0 };
    let slack = admitted.checked_sub(retained).ok_or_else(|| coordinator_boundary(
        custody.work(), wire::ErrorCode::Storage, "scheduler protobuf exceeds its concrete admission",
    ))?;
    custody.relinquish(slack)?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(COORDINATOR_WIRE_CONTRACT.len() as u64)?;
    }
    if custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
        let protocol = envelope.protocol.as_ref().ok_or_else(|| coordinator_boundary(
            custody.work(), wire::ErrorCode::Storage, "scheduler protocol identity is absent",
        ))?;
        let expected = blake3::hash(COORDINATOR_WIRE_CONTRACT).to_hex();
        if protocol.version != COORDINATOR_WIRE_VERSION || protocol.descriptor_digest != expected.as_str() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "scheduler protocol identity differs"));
        }
    } else {
        validate_protocol(envelope.protocol.as_ref(), &coordinator_protocol_identity(), Error::Storage)?;
    }
    let digest: [u8; 32] = envelope.event_digest.as_slice().try_into()
        .map_err(|_| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "scheduler digest must be 32 bytes"))?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(envelope.canonical_event_json.len() as u64)?;
    }
    if *blake3::hash(&envelope.canonical_event_json).as_bytes() != digest {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "scheduler event digest mismatch"));
    }
    let event = match custody.work().as_deref_mut() {
        Some(work) => SchedulerEventJsonAdmission::prepare(&envelope.canonical_event_json, work)?.decode()?,
        None => crate::contract::json_from_slice(&envelope.canonical_event_json)
            .map_err(|source| Error::Storage(source.to_string()))?,
    };
    if custody.work().is_some() { custody.adopt(event.logical_owned_bytes()?)?; }
    let committed_at_ms = envelope.committed_at_ms.filter(|time| *time > 0)
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "coordinator event lacks commit time"))?;
    let operation_id = if custody.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
        let parsed = uuid::Uuid::parse_str(&envelope.operation_id).map_err(|_| coordinator_boundary(
            custody.work(), wire::ErrorCode::Invalid, "scheduler operation id is not a UUID",
        ))?;
        OperationId::from_bytes(*parsed.as_bytes())
    } else { OperationId::parse(&envelope.operation_id)? };
    let key_bytes = if custody.work().is_some() { envelope.idempotency_key.capacity() as u64 } else { 0 };
    let wire::SchedulerEventEnvelope { revision, idempotency_key, protocol, operation_id: text,
        canonical_event_json, event_digest, .. } = envelope;
    drop((protocol, text, canonical_event_json, event_digest));
    custody.relinquish(retained - key_bytes)?;
    custody.transfer();
    Ok((revision, operation_id, idempotency_key, digest, committed_at_ms, event))
}

fn coordinator_retry_conflict(error: &Error) -> bool {
    if matches!(error, Error::Conflict(_)) { return true; }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    return matches!(error,
        Error::NativeStream { source: StreamError::IdempotencyMismatch, .. }
        | Error::NativeBoundary { code: wire::ErrorCode::Conflict, .. });
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    false
}




#[cfg(not(target_arch = "wasm32"))]
fn current_time_millis_with_work(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| coordinator_boundary(work, wire::ErrorCode::Invalid, "coordinator clock predates Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| coordinator_boundary(work, wire::ErrorCode::Invalid, "coordinator clock exceeds supported range"))
}

#[cfg(target_arch = "wasm32")]
fn current_time_millis_with_work(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    use acyclic_stream::UnixMillisClock as _;
    let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
    if now == 0 || now == u64::MAX {
        return Err(coordinator_boundary(
            work,
            wire::ErrorCode::Invalid,
            "coordinator clock exceeds supported range",
        ));
    }
    Ok(now)
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
    append_keyed_with_work((stream, client), record, if_tail, key, operation_id, identity, &mut None).await
}


pub(crate) async fn append_keyed_with_work<P: StreamProvider>(
    (stream, client): (&Stream<P>, &StreamClient<P>),
    record: Bytes,
    if_tail: Option<u64>,
    key: StreamIdempotencyKey,
    operation_id: OperationId,
    identity: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<AppendOutcome> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    // Declaration order keeps the physical request ahead of its quota on
    // every pre-call failure and when the original SDK future is cancelled.
    let mut records = Vec::new();
    let slots = std::mem::size_of::<Bytes>() as u64;
    if let Some(work) = custody.work().as_deref_mut() {
        work.admit_owned(slots)?;
        work.admit_allocation(slots)?;
    }
    records.try_reserve_exact(1).map_err(|cause| {
        match custody.work().as_deref_mut() {
            Some(work) => work.allocation_failure(cause),
            None => Error::Storage(cause.to_string()),
        }
    })?;
    let capacity = (records.capacity() * std::mem::size_of::<Bytes>()) as u64;
    if let Some(work) = custody.work().as_deref_mut() {
        work.retain_owned(capacity)?;
        custody.adopt(capacity)?;
    }
    records.push(record);
    let retained_request = custody.retained();
    custody.transfer();
    match append_records_keyed_with_work(
        (stream, client), records, retained_request, if_tail, key,
        operation_id, identity, custody.work(),
    ).await? {
        Ok(outcome) => Ok(outcome),
        Err(error) => Err(append_error_with_work(error, identity, custody.work())),
    }
}

#[allow(clippy::too_many_arguments, reason = "same original SDK append, exact batch custody and retry identity")]
async fn append_records_keyed_with_work<P: StreamProvider>(
    (stream, client): (&Stream<P>, &StreamClient<P>),
    records: Vec<Bytes>,
    retained_request: u64,
    if_tail: Option<u64>,
    key: StreamIdempotencyKey,
    operation_id: OperationId,
    identity: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<std::result::Result<AppendOutcome, StreamError>> {
    let mut custody = crate::wire_codec::CodecQuota::from_retained(work, retained_request);
    let records = records;
    let count = records.len() as u64;
    let byte_length = records.iter().try_fold(0u64, |total, record| {
        total.checked_add(record.len() as u64).ok_or_else(|| {
            coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                "append request byte count exhausted")
        })
    })?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.admit_acknowledgment(byte_length, count)?;
        work.begin_write()?;
    }
    let appended = stream.append_batch(records, if_tail, Some(key.clone())).await;
    // The completed SDK future has dropped its physical request vector.
    if custody.work().is_some() { custody.relinquish(retained_request)?; }
    match appended {
        Ok(outcome) => {
            if let Some(work) = custody.work().as_deref_mut()
                && let AppendOutcome::Committed(receipt) = &outcome
                && receipt.end.checked_sub(receipt.start) == Some(count)
                && receipt.tail >= receipt.end
            {
                // A direct SDK ACK confirms the offered record, including an
                // idempotent ACK. Scalar receipts contain no received records.
                work.acknowledged(byte_length, count)?;
            }
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            Ok(Ok(outcome))
        }
        Err(StreamError::Unavailable) => {
            let mut received = crate::wire_codec::CodecQuota::from_retained(custody.work(), 0);
            match inspect_idempotency_once_with_work(client, key, 0, received.work()).await? {
                Ok(Some((observation, retained))) => {
                    received.adopt(retained)?;
                    match observation.outcome {
                        IdempotencyOutcome::Append(outcome) => {
                            if let Some(work) = received.work().as_deref_mut() { work.check_cancellation()?; }
                            Ok(Ok(outcome))
                        }
                        _ => {
                            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                            if received.work().as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
                                return Err(Error::NativeBoundary {
                                    code: wire::ErrorCode::Conflict,
                                    reason: "append recovery identity has another operation kind",
                                });
                            }
                            Err(Error::Conflict(format!("{identity} has another operation kind")))
                        }
                    }
                }
                Ok(None) | Err(_) => Err(Error::Indeterminate(operation_id)),
            }
        }
        Err(error) => Ok(Err(error)),
    }
}

fn append_error_with_work(
    error: StreamError,
    identity: &str,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
        let code = if error == StreamError::IdempotencyMismatch {
            wire::ErrorCode::Conflict
        } else {
            wire::ErrorCode::Storage
        };
        // StreamError's original closed unit source needs no diagnostic heap.
        return Error::NativeStream { code, source: error, custody: None };
    }
    if error == StreamError::IdempotencyMismatch {
        Error::Conflict(format!("{identity} was reused"))
    } else {
        Error::Storage(error.to_string())
    }
}

pub(crate) fn execution_path(operation: OperationId) -> Result<acyclic_stream::StreamPath> {
    execution_path_with_work(operation, &mut None)
}

pub(crate) fn execution_path_with_work(
    operation: OperationId,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let text = formatted_text_with_work(
        "harness/v2/execution/".len() + 36,
        format_args!("harness/v2/execution/{operation}"), custody.work(),
    )?;
    let text_capacity = if custody.work().is_some() { text.capacity() as u64 } else { 0 };
    custody.adopt(text_capacity)?;
    let path = path_with_work(&text, custody.work())?;
    if custody.work().is_some() { custody.adopt(path.as_str().len() as u64)?; }
    drop(text);
    custody.relinquish(text_capacity)?;
    custody.transfer();
    Ok(path)
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitPhase {
    NotCalled,
    Unknown,
    Committed,
    Conflict,
    Failed,
}


/// The original SDK request and its producer-owned logical custody. Fields
/// remain private so callers cannot claim that an unprotected request was built.
/// The request drops before its borrowed quota on every prepublication failure.
pub(crate) struct PreparedCommitRequest<'a, 'b> {
    request: acyclic_stream::CommitRequest,
    custody: crate::wire_codec::CodecQuota<'a, 'b>,
    vectors: u64,
    phase: CommitPhase,
}

fn retain_received_owned(
    bytes: u64,
    custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
    failure: &mut Option<Error>,
) -> Result<()> {
    let Some(work) = custody.work().as_deref_mut() else { return Ok(()); };
    match work.retain_owned(bytes) {
        Ok(()) => custody.adopt(bytes)?,
        Err(error) => { if failure.is_none() { *failure = Some(error); } }
    }
    Ok(())
}

fn observe_commit_with_work(
    outcome: &acyclic_stream::CommitOutcome,
    direct_acknowledgment: bool,
    custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
    failure: &mut Option<Error>,
) -> Result<()> {
    use acyclic_stream::{CommitConflict, CommitOutcome, CommittedMutation};
    if custody.work().is_none() { return Ok(()); }
    match outcome {
        CommitOutcome::Committed(envelope) => {
            retain_received_owned((envelope.mutations.capacity()
                * std::mem::size_of::<CommittedMutation>()) as u64, custody, failure)?;
            for mutation in &envelope.mutations {
                let records = match mutation {
                    CommittedMutation::Append(append) => {
                        retain_received_owned(append.path.as_str().len() as u64, custody, failure)?;
                        &append.records
                    }
                    CommittedMutation::Fork(fork) => {
                        retain_received_owned(fork.source.as_str().len() as u64, custody, failure)?;
                        retain_received_owned(fork.destination.as_str().len() as u64, custody, failure)?;
                        &fork.records
                    }
                };
                retain_received_owned((records.capacity()
                    * std::mem::size_of::<acyclic_stream::Record>()) as u64, custody, failure)?;
                for record in records {
                    let bytes = record.value.len() as u64;
                    match custody.work().as_deref_mut().expect("observed envelope").receive_record(bytes) {
                        Ok(()) => custody.adopt(bytes)?,
                        Err(error) => { if failure.is_none() { *failure = Some(error); } }
                    }
                    if direct_acknowledgment {
                        let acknowledged = custody.work().as_deref_mut().expect("observed envelope")
                            .acknowledged(bytes, 1);
                        if let Err(error) = acknowledged
                            && failure.is_none()
                        { *failure = Some(error); }
                    }
                }
            }
        }
        CommitOutcome::Conflict(conflicts) => {
            retain_received_owned((conflicts.capacity()
                * std::mem::size_of::<CommitConflict>()) as u64, custody, failure)?;
            for conflict in conflicts {
                let (CommitConflict::Tail { path, .. } | CommitConflict::Exists { path }) = conflict;
                retain_received_owned(path.as_str().len() as u64, custody, failure)?;
            }
        }
    }
    Ok(())
}

fn observe_recovery_with_work(
    observation: &acyclic_stream::IdempotencyObservation,
    custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
    failure: &mut Option<Error>,
) -> Result<()> {
    retain_received_owned(observation.idempotency_key.as_bytes().len() as u64, custody, failure)?;
    match &observation.outcome {
        IdempotencyOutcome::Commit(outcome) => observe_commit_with_work(outcome, false, custody, failure)?,
        IdempotencyOutcome::Fork(receipt) => {
            retain_received_owned(receipt.source.as_str().len() as u64, custody, failure)?;
            retain_received_owned(receipt.destination.as_str().len() as u64, custody, failure)?;
        }
        IdempotencyOutcome::Append(_) => {}
    }
    Ok(())
}

pub(crate) async fn inspect_idempotency_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    key: StreamIdempotencyKey,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<(acyclic_stream::IdempotencyObservation, u64)>> {
    let retained_key = if work.is_some() { key.as_bytes().len() as u64 } else { 0 };
    match inspect_idempotency_once_with_work(client, key, retained_key, work).await? {
        Ok(observation) => Ok(observation),
        Err(source) => Err(crate::store::operations::stream_error_with_work(source, work)),
    }
}

// Aliased recovery keys remain owned by their original caller's guard; new
// inspector keys transfer their already-retained payload through retained_key.
async fn inspect_idempotency_once_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    key: StreamIdempotencyKey,
    retained_key: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<std::result::Result<Option<(acyclic_stream::IdempotencyObservation, u64)>, StreamError>> {
    let mut custody = crate::wire_codec::CodecQuota::from_retained(work, retained_key);
    let key = key;
    if let Some(work) = custody.work().as_deref_mut() { work.begin_call()?; }
    let inspected = client.inspect_idempotency(key).await;
    match inspected {
        Err(source) => Ok(Err(source)),
        Ok(None) => Ok(Ok(None)),
        Ok(Some(observation)) => {
            let mut failure = None;
            observe_recovery_with_work(&observation, &mut custody, &mut failure)?;
            if let Some(error) = failure { return Err(error); }
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            // The original SDK future has physically consumed its input key.
            custody.relinquish(retained_key)?;
            let retained = custody.retained();
            custody.transfer();
            Ok(Ok(Some((observation, retained))))
        }
    }
}

fn reserve_prepared_vector<T>(
    values: &mut Vec<T>,
    capacity: usize,
    custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
    vectors: &mut u64,
) -> Result<()> {
    if capacity <= values.capacity() { return Ok(()); }
    let old = values.capacity();
    let requested = (capacity - old).checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
            "commit vector capacity overflows"))? as u64;
    if let Some(work) = custody.work().as_deref_mut() {
        work.admit_owned(requested)?;
        work.admit_allocation(requested)?;
    }
    values.try_reserve_exact(capacity - values.len()).map_err(|cause| {
        match custody.work().as_deref_mut() {
            Some(work) => work.allocation_failure(cause),
            None => Error::Storage(cause.to_string()),
        }
    })?;
    let actual = (values.capacity() - old).checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
            "commit vector capacity overflows"))? as u64;
    if let Some(work) = custody.work().as_deref_mut() {
        work.retain_owned(actual)?;
        custody.adopt(actual)?;
    }
    *vectors = vectors.checked_add(actual)
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
            "commit vector capacity overflows"))?;
    Ok(())
}

impl<'a, 'b> PreparedCommitRequest<'a, 'b> {
    pub(crate) fn new(
        key: StreamIdempotencyKey,
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        Ok(Self {
            request: acyclic_stream::CommitRequest {
                conditions: Vec::new(), mutations: Vec::new(), idempotency_key: key,
            },
            custody: crate::wire_codec::CodecQuota::new(work, 0)?,
            vectors: 0,
            phase: CommitPhase::NotCalled,
        })
    }

    pub(crate) fn new_with_retained_key(
        key: StreamIdempotencyKey,
        retained_key_bytes: u64,
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> Self {
        Self {
            request: acyclic_stream::CommitRequest {
                conditions: Vec::new(), mutations: Vec::new(), idempotency_key: key,
            },
            custody: crate::wire_codec::CodecQuota::from_retained(work, retained_key_bytes),
            vectors: 0,
            phase: CommitPhase::NotCalled,
        }
    }

    pub(crate) fn commit_phase(&self) -> CommitPhase { self.phase }

    pub(crate) fn new_with_digest(
        digest: [u8; 32],
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_owned(32)?;
            work.admit_allocation(32)?;
            work.copy_bytes(32)?;
        }
        let key = StreamIdempotencyKey::new(Bytes::copy_from_slice(&digest))
            .map_err(|source| crate::store::operations::stream_error_with_work(source, custody.work()))?;
        // Put the owned key ahead of the quota before a fallible retention hook.
        let mut prepared = Self {
            request: acyclic_stream::CommitRequest {
                conditions: Vec::new(), mutations: Vec::new(), idempotency_key: key,
            },
            custody, vectors: 0, phase: CommitPhase::NotCalled,
        };
        if let Some(work) = prepared.work().as_deref_mut() {
            work.retain_owned(32)?;
            prepared.custody.adopt(32)?;
        }
        Ok(prepared)
    }

    pub(crate) fn work(&mut self) -> &mut Option<&'b mut dyn crate::store::history::HistoryIo> {
        self.custody.work()
    }

    pub(crate) fn push_condition(&mut self, condition: acyclic_stream::CommitCondition) -> Result<()> {
        let length = self.request.conditions.len().checked_add(1)
            .ok_or_else(|| coordinator_boundary(self.work(), wire::ErrorCode::Invalid,
                "commit condition count overflows"))?;
        reserve_prepared_vector(&mut self.request.conditions, length, &mut self.custody, &mut self.vectors)?;
        self.request.conditions.push(condition);
        Ok(())
    }

    pub(crate) fn append(&mut self, path: acyclic_stream::StreamPath, record: Bytes) -> Result<()> {
        let length = self.request.mutations.len().checked_add(1)
            .ok_or_else(|| coordinator_boundary(self.work(), wire::ErrorCode::Invalid,
                "commit mutation count overflows"))?;
        reserve_prepared_vector(&mut self.request.mutations, length, &mut self.custody, &mut self.vectors)?;
        let mut records = Vec::new();
        reserve_prepared_vector(&mut records, 1, &mut self.custody, &mut self.vectors)?;
        records.push(record);
        self.request.mutations.push(acyclic_stream::CommitMutation::Append { path, records });
        Ok(())
    }

    /// Calls the original SDK once. The inner error is the original unit SDK
    /// error; successful retained bytes transfer with the actual outcome.
    pub(crate) async fn commit_once_with_work<P: StreamProvider>(
        &mut self,
        client: &StreamClient<P>,
    ) -> Result<std::result::Result<(acyclic_stream::CommitOutcome, u64), StreamError>> {
        if self.work().is_some() {
            let (bytes, count) = self.request.mutations.iter().try_fold(
                (0u64, 0u64), |(bytes, count), mutation| {
                    let records = match mutation {
                        acyclic_stream::CommitMutation::Append { records, .. }
                        | acyclic_stream::CommitMutation::Fork { records, .. } => records,
                    };
                    let bytes = records.iter().try_fold(bytes, |bytes, record| {
                        bytes.checked_add(record.len() as u64)
                            .ok_or_else(|| coordinator_boundary(&None, wire::ErrorCode::Invalid,
                                "commit payload byte count overflows"))
                    })?;
                    let count = count.checked_add(records.len() as u64)
                        .ok_or_else(|| coordinator_boundary(&None, wire::ErrorCode::Invalid,
                            "commit record count overflows"))?;
                    Ok::<_, Error>((bytes, count))
                },
            )?;
            if let Some(work) = self.work().as_deref_mut() {
                work.admit_acknowledgment(bytes, count)?;
                work.begin_write()?;
            }
        }
        let key = self.request.idempotency_key.clone();
        let request = std::mem::replace(&mut self.request, acyclic_stream::CommitRequest {
            conditions: Vec::new(), mutations: Vec::new(), idempotency_key: key,
        });
        self.phase = CommitPhase::Unknown;
        let committed = client.commit(request).await;
        self.phase = match &committed {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => CommitPhase::Committed,
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => CommitPhase::Conflict,
            Err(StreamError::Unavailable) => CommitPhase::Unknown,
            Err(_) => CommitPhase::Failed,
        };
        if self.work().is_some() { self.custody.relinquish(self.vectors)?; }
        self.vectors = 0;
        let mut received = crate::wire_codec::CodecQuota::from_retained(self.custody.work(), 0);
        let committed = committed;
        match committed {
            Err(source) => Ok(Err(source)),
            Ok(outcome) => {
                let mut failure = None;
                observe_commit_with_work(&outcome, true, &mut received, &mut failure)?;
                if let Some(error) = failure { return Err(error); }
                if let Some(work) = received.work().as_deref_mut() { work.check_cancellation()?; }
                let retained = received.retained();
                received.transfer();
                Ok(Ok((outcome, retained)))
            }
        }
    }
}

pub(crate) async fn commit_keyed<P: StreamProvider>(
    client: &StreamClient<P>,
    request: acyclic_stream::CommitRequest,
    operation: OperationId,
) -> Result<acyclic_stream::CommitOutcome> {
    let mut work = None;
    let prepared = PreparedCommitRequest {
        request, custody: crate::wire_codec::CodecQuota::new(&mut work, 0)?, vectors: 0,
        phase: CommitPhase::NotCalled,
    };
    commit_prepared_with_work(client, prepared, operation).await
}

pub(crate) async fn commit_prepared_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    prepared: PreparedCommitRequest<'_, '_>,
    operation: OperationId,
) -> Result<acyclic_stream::CommitOutcome> {
    commit_prepared_with_work_and_phase(client, prepared, operation, None).await
}

pub(crate) async fn commit_prepared_with_work_and_phase<P: StreamProvider>(
    client: &StreamClient<P>,
    mut prepared: PreparedCommitRequest<'_, '_>,
    operation: OperationId,
    mut phase: Option<&mut CommitPhase>,
) -> Result<acyclic_stream::CommitOutcome> {
    if let Some(phase) = phase.as_deref_mut() { *phase = prepared.commit_phase(); }
    let key = prepared.request.idempotency_key.clone();
    let committed = prepared.commit_once_with_work(client).await;
    // Observe actual SDK disposition even when a subsequent quota/receiver
    // callback fails. The original typed cause remains untouched.
    if let Some(phase) = phase.as_deref_mut() { *phase = prepared.commit_phase(); }
    let committed = committed?;
    let mut received = crate::wire_codec::CodecQuota::from_retained(prepared.work(), 0);
    let outcome = match committed {
        Ok((outcome, retained)) => {
            received.adopt(retained)?;
            outcome
        }
        Err(StreamError::Unavailable) => {
            match inspect_idempotency_once_with_work(client, key, 0, received.work()).await? {
                Ok(Some((observation, retained))) => {
                    if let (Some(phase), IdempotencyOutcome::Commit(outcome)) =
                        (phase.as_deref_mut(), &observation.outcome) {
                        *phase = match outcome {
                            acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
                            acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Conflict,
                        };
                    }
                    received.adopt(retained)?;
                    let metadata = if received.work().is_some() {
                        observation.idempotency_key.as_bytes().len() as u64
                    } else { 0 };
                    let acyclic_stream::IdempotencyObservation { idempotency_key, outcome, .. } = observation;
                    drop(idempotency_key);
                    received.relinquish(metadata)?;
                    match outcome {
                        IdempotencyOutcome::Commit(outcome) => outcome,
                        _ => return Err(coordinator_boundary(received.work(), wire::ErrorCode::Conflict,
                            "commit recovery identity reused")),
                    }
                }
                Ok(None) | Err(_) => return Err(Error::Indeterminate(operation)),
            }
        }
        Err(error) => return Err(crate::store::operations::stream_error_with_work(error, received.work())),
    };
    if let Some(work) = received.work().as_deref_mut() { work.check_cancellation()?; }
    received.transfer();
    Ok(outcome)
}

fn path_with_work(
    value: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    if let Some(work) = work.as_deref_mut() {
        // StreamPath owns Arc<str>'s actual UTF-8 payload; the Arc header and
        // provider-internal allocation are deliberately not inferred.
        work.admit_allocation(value.len() as u64)?;
        work.copy_bytes(value.len() as u64)?;
    }
    let path = acyclic_stream::StreamPath::new(value)
        .map_err(|error| crate::store::operations::stream_error_with_work(error, work))?;
    if let Some(work) = work.as_deref_mut() { work.retain_owned(value.len() as u64)?; }
    Ok(path)
}

fn commit_outcome_owned_bytes(
    outcome: &acyclic_stream::CommitOutcome,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    use acyclic_stream::{CommitConflict, CommitOutcome, CommittedMutation};
    let add = |left: u64, right: u64| left.checked_add(right)
        .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Storage, "commit source custody overflows"));
    match outcome {
        CommitOutcome::Committed(envelope) => {
            let mut bytes = (envelope.mutations.capacity() * std::mem::size_of::<CommittedMutation>()) as u64;
            for mutation in &envelope.mutations {
                let records = match mutation {
                    CommittedMutation::Append(append) => {
                        bytes = add(bytes, append.path.as_str().len() as u64)?;
                        &append.records
                    }
                    CommittedMutation::Fork(fork) => {
                        bytes = add(bytes, fork.source.as_str().len() as u64)?;
                        bytes = add(bytes, fork.destination.as_str().len() as u64)?;
                        &fork.records
                    }
                };
                bytes = add(bytes, (records.capacity() * std::mem::size_of::<acyclic_stream::Record>()) as u64)?;
                for record in records { bytes = add(bytes, record.value.len() as u64)?; }
            }
            Ok(bytes)
        }
        CommitOutcome::Conflict(conflicts) => {
            let mut bytes = (conflicts.capacity() * std::mem::size_of::<CommitConflict>()) as u64;
            for conflict in conflicts {
                let (CommitConflict::Tail { path, .. } | CommitConflict::Exists { path }) = conflict;
                bytes = add(bytes, path.as_str().len() as u64)?;
            }
            Ok(bytes)
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one original coordinator append with exact execution and workflow conditions"
)]
async fn append_if_execution_idle_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    stream: &Stream<P>,
    record: Bytes,
    tail: u64,
    key: StreamIdempotencyKey,
    operation: OperationId,
    execution: OperationId,
    expected_tail: u64,
    workflow_guard: Option<(&str, u64)>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<AppendOutcome> {
    use acyclic_stream::{CommitCondition, CommitOutcome, CommittedMutation};
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let record = record;
    let key = key;
    let path = execution_path_with_work(execution, custody.work())?;
    if custody.work().is_some() { custody.adopt(path.as_str().len() as u64)?; }
    let bounds = {
        let mut source_custody = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
        let portable = source_custody.work().as_deref().is_some_and(|work|
            !crate::store::operations::original_native_decode_mode(work));
        if portable {
            let work = source_custody.work().as_deref_mut().expect("portable observer");
            work.admit_allocation(path.as_str().len() as u64)?;
            work.copy_bytes(path.as_str().len() as u64)?;
        }
        let source = crate::store::operations::stream_with_work(client, &path, source_custody.work())?;
        if portable {
            source_custody.work().as_deref_mut().expect("portable observer")
                .retain_owned(path.as_str().len() as u64)?;
            source_custody.adopt(path.as_str().len() as u64)?;
        }
        if let Some(work) = source_custody.work().as_deref_mut() { work.begin_call()?; }
        source.stream.bounds().await
    };
    let guard = match bounds {
        Ok(bounds) if bounds.tail == expected_tail => CommitCondition::Tail { path, expected: expected_tail },
        Ok(_) => return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "execution changed before suspension")),
        Err(StreamError::NotFound) if expected_tail == 0 => CommitCondition::Absent { path },
        Err(error) => return Err(crate::store::operations::stream_error_with_work(error, custody.work())),
    };
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes((b"harness/v2/idle-execution\0".len() + key.as_bytes().len() + 1) as u64)?;
    }
    let mut digest = blake3::Hasher::new();
    digest.update(b"harness/v2/idle-execution\0");
    digest.update(key.as_bytes());
    digest.update(&[u8::from(matches!(guard, CommitCondition::Absent { .. }))]);
    let workflow_condition = if let Some((workflow_path, workflow_tail)) = workflow_guard {
        let path = path_with_work(workflow_path, custody.work())?;
        if custody.work().is_some() { custody.adopt(path.as_str().len() as u64)?; }
        if let Some(work) = custody.work().as_deref_mut() {
            work.hashed_bytes((b"workflow\0".len() + workflow_path.len() + 8) as u64)?;
        }
        digest.update(b"workflow\0");
        digest.update(workflow_path.as_bytes());
        digest.update(&workflow_tail.to_le_bytes());
        Some(CommitCondition::Tail { path, expected: workflow_tail })
    } else { None };
    let mut request = PreparedCommitRequest::new_with_digest(*digest.finalize().as_bytes(), custody.work())?;
    request.push_condition(CommitCondition::Tail { path: stream.path().clone(), expected: tail })?;
    request.push_condition(guard)?;
    if let Some(condition) = workflow_condition { request.push_condition(condition)?; }
    request.append(stream.path().clone(), record.clone())?;
    let outcome = commit_prepared_with_work(client, request, operation).await?;
    let outcome_bytes = if custody.work().is_some() {
        commit_outcome_owned_bytes(&outcome, custody.work())?
    } else { 0 };
    custody.adopt(outcome_bytes)?;
    let receipt = (|| {
        let CommitOutcome::Committed(envelope) = &outcome else {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
                "execution or coordinator changed before suspension"));
        };
        let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "invalid guarded suspension receipt"));
        };
        if append.path != *stream.path() || append.start != tail
            || append.end != next_revision(tail)? || append.tail != append.end
            || append.records.len() != 1 || append.records.first().is_none_or(|entry| {
                entry.sequence != tail || entry.value != record || entry.commit_id != envelope.commit_id
            })
        {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "guarded suspension differs from its commit"));
        }
        Ok(AppendOutcome::Committed(acyclic_stream::AppendReceipt {
            start: append.start, end: append.end, tail: append.tail, commit_id: envelope.commit_id,
        }))
    })();
    drop(outcome);
    custody.relinquish(outcome_bytes)?;
    receipt
}
fn physical_key_with_work(
    key: &str,
    revision: u64,
    parent: bool,
    waiting: bool,
    idle_execution: Option<(OperationId, u64)>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<StreamIdempotencyKey> {
    let digits = |value: u64| value.checked_ilog10().map_or(1, |digits| digits as usize + 1);
    let physical = if parent {
        Some(formatted_text_with_work(key.len() + ":owned-parent:".len() + digits(revision),
            format_args!("{key}:owned-parent:{revision}"), work)?)
    } else if waiting {
        Some(formatted_text_with_work(key.len() + ":owned-wait:".len() + digits(revision),
            format_args!("{key}:owned-wait:{revision}"), work)?)
    } else if let Some((execution, tail)) = idle_execution {
        Some(formatted_text_with_work(
            key.len() + ":idle-execution:".len() + 36 + 2 + digits(tail) + digits(revision),
            format_args!("{key}:idle-execution:{execution}:{tail}:{revision}"), work)?)
    } else { None };
    let result = stream_key_with_work(physical.as_deref().unwrap_or(key), work);
    if let Some(physical) = physical {
        let capacity = physical.capacity() as u64;
        drop(physical);
        if let Some(work) = work.as_deref_mut() { work.release_owned(capacity)?; }
    }
    result
}

fn stream_key(key: &str) -> Result<StreamIdempotencyKey> {
    stream_key_with_work(key, &mut None)
}

fn stream_key_with_work(
    key: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<StreamIdempotencyKey> {
    if let Some(work) = work.as_deref_mut() {
        work.hashed_bytes((b"acyclic-harness-coordinator-v3".len() + key.len()) as u64)?;
        work.admit_allocation(32)?;
        work.copy_bytes(32)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-coordinator-v3");
    hasher.update(key.as_bytes());
    let key = StreamIdempotencyKey::new(Bytes::copy_from_slice(hasher.finalize().as_bytes()))
        .map_err(|source| {
            if work.as_deref().is_some_and(crate::store::operations::original_native_decode_mode) {
                crate::store::operations::stream_error_with_work(source, work)
            } else { Error::Invalid(source.to_string()) }
        })?;
    if let Some(work) = work.as_deref_mut() { work.retain_owned(32)?; }
    Ok(key)
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn keyed_append_native_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 8,
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 1024,
            backend_write_operations: 8,
            backend_read_operations: 8,
            authority_records_appended: 8,
            authority_bytes_written: 1024,
            authority_records_read: 8,
            authority_bytes_read: 1024,
            items_examined: 8,
            ..acyclic_fs::WorkBudget::default()
        }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_atomic_batch_counts_two_actual_sdk_acks_not_recovery() {
        use std::sync::atomic::Ordering;
        for lost_ack in [false, true] {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
            let stream = client.stream("native-atomic-pair").expect("original source stream");
            let key = StreamIdempotencyKey::new(Bytes::from_static(b"native-atomic-pair-key")).expect("original key");
            let cancellation = acyclic_fs::CancellationToken::new();
            let native = acyclic_fs::NativeMaterializationWork::new(keyed_append_native_budget(), &cancellation)
                .expect("original native work");
            provider.lose_ack.store(lost_ack, Ordering::SeqCst);
            for attempt in 0..2 {
                let mut observer = crate::store::history::native_work_io(&native).expect("original observer");
                let mut optional: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
                let mut custody = crate::wire_codec::CodecQuota::new(&mut optional, 0).expect("request custody");
                let mut records = Vec::new();
                let bytes = (2 * std::mem::size_of::<Bytes>()) as u64;
                custody.work().as_deref_mut().expect("observer").admit_owned(bytes).expect("fund slots");
                custody.work().as_deref_mut().expect("observer").admit_allocation(bytes).expect("fund allocation");
                records.try_reserve_exact(2).expect("original vector growth");
                let retained = (records.capacity() * std::mem::size_of::<Bytes>()) as u64;
                custody.work().as_deref_mut().expect("observer").retain_owned(retained).expect("original claim");
                custody.adopt(retained).expect("adopt actual paid slots");
                records.extend([Bytes::from_static(b"declared"), Bytes::from_static(b"limits")]);
                custody.transfer();
                let outcome = append_records_keyed_with_work(
                    (&stream, &client), records, retained, Some(0), key.clone(),
                    OperationId::from_bytes([18; 16]), "native atomic retry", custody.work(),
                ).await.expect("original batch engine").expect("actual SDK outcome");
                assert!(matches!(outcome, AppendOutcome::Committed(_)));
                drop(custody);
                drop(optional);
                assert_eq!(observer.into_custody().bytes(), 0, "physical request precedes release");
                let acknowledgments = (attempt + 1) - u64::from(lost_ack);
                assert_eq!(native.receipt().authority_records_appended, 2 * acknowledgments);
                assert_eq!(native.receipt().authority_bytes_written, 14 * acknowledgments);
            }
            assert_eq!(stream.tail().await.expect("original SDK tail"), 2);
            assert_eq!(provider.io.writes.load(Ordering::SeqCst), 2);
            assert_eq!(provider.io.inspections.load(Ordering::SeqCst), u64::from(lost_ack));
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_session_rejects_missing_declare_authority_before_sdk_io() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(LostSessionAck::default());
        let client = StreamClient::new(provider.clone());
        let mut coordinator = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let reads = provider.io.reads.load(Ordering::SeqCst);
        let owner = Authority { kind: AggregateKind::Task, id: "owner".into() };
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
        let scope = issuer.root("owner", Capabilities::default());
        let limits = crate::scheduler::SessionLimits {
            active_tasks: 1, total_tasks: 1, depth: 0, model_steps: 1,
        };
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = keyed_append_native_budget();
        budget.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 1_048_576;
        budget.allocation_operations = 4096;
        budget.items_examined = 4096;
        budget.bytes_copied = 1_048_576;
        budget.bytes_encoded = 1_048_576;
        budget.bytes_hashed = 1_048_576;
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).expect("original work");
        let mut observer = crate::store::history::native_work_io(&native).expect("original observer");
        let error = coordinator.declare_session_with_work(
            &owner, &scope, &issuer.verifier(), spec(OperationId::from_bytes([19; 16]), 0)?,
            limits, IdempotencyKey::new("native-unauthorized-session")?, &mut Some(&mut observer),
        ).await.expect_err("same original declaration capability");
        assert!(matches!(error, Error::NativeBoundary { code: wire::ErrorCode::Unauthorized, .. }));
        assert_eq!(coordinator.revision(), 0);
        assert_eq!(provider.io.reads.load(Ordering::SeqCst), reads);
        assert_eq!(provider.io.writes.load(Ordering::SeqCst), 0);
        assert_eq!(observer.into_custody().bytes(), 0);
        Ok(())
    }


    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn keyed_append_native_counts_sdk_ack_not_inspected_or_physical_new_records() {
        use std::sync::atomic::Ordering;
        for lost_ack in [false, true] {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
            let stream = client.stream("keyed-append-native-ack").expect("source stream");
            let key = StreamIdempotencyKey::new(Bytes::from_static(b"original-key")).expect("key");
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(keyed_append_native_budget(), &cancellation)
                .expect("original ledger");
            provider.lose_ack.store(lost_ack, Ordering::SeqCst);
            for attempt in 0..2 {
                let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
                let outcome = append_keyed_with_work(
                    (&stream, &client), Bytes::from_static(b"original"), Some(0), key.clone(),
                    OperationId::from_bytes([13; 16]), "original retry", &mut Some(&mut observer),
                ).await.expect("same engine append or recovery");
                assert!(matches!(outcome, AppendOutcome::Committed(_)));
                assert_eq!(observer.into_custody().bytes(), 0, "request and receiver custody released");
                assert_eq!(work.receipt().authority_records_appended,
                    (attempt + 1) - u64::from(lost_ack));
                assert_eq!(work.receipt().authority_bytes_written,
                    8 * ((attempt + 1) - u64::from(lost_ack)));
            }
            assert_eq!(stream.tail().await.expect("actual tail"), 1, "same identity never appends twice");
            assert_eq!(provider.io.writes.load(Ordering::SeqCst), 2);
            assert_eq!(provider.io.inspections.load(Ordering::SeqCst), u64::from(lost_ack));
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn keyed_append_native_denials_precede_actual_provider_request() {
        use std::sync::atomic::Ordering;
        for denial in 0..5 {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
            let stream = client.stream("keyed-append-native-denied").expect("source stream");
            let key = StreamIdempotencyKey::new(Bytes::from_static(b"denied-key")).expect("key");
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget = keyed_append_native_budget();
            match denial {
                0 => budget.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
                1 => budget.allocation_operations = 1,
                2 => budget.authority_records_appended = 0,
                3 => budget.backend_write_operations = 0,
                _ => {}
            }
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .expect("original ledger");
            let prefix = work.receipt();
            let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
            if denial == 4 { cancellation.cancel(); }
            let error = append_keyed_with_work(
                (&stream, &client), Bytes::from_static(b"original"), Some(0), key,
                OperationId::from_bytes([14; 16]), "denied retry", &mut Some(&mut observer),
            ).await.expect_err("native quota or cancellation");
            assert!(matches!(error, Error::NativeWork { .. }));
            assert_eq!(observer.into_custody().bytes(), 0, "denied request physically dropped and quota released");
            assert_eq!(provider.io.writes.load(Ordering::SeqCst), 0);
            assert_eq!(provider.io.inspections.load(Ordering::SeqCst), 0);
            assert_eq!(work.receipt().authority_records_appended, 0);
            if denial < 2 || denial == 4 {
                assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
            }
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn keyed_append_native_keeps_exact_original_sdk_mismatch_source() {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let stream = client.stream("keyed-append-native-mismatch").expect("source stream");
        let key = StreamIdempotencyKey::new(Bytes::from_static(b"mismatch-key")).expect("key");
        let operation = OperationId::from_bytes([15; 16]);
        append_keyed((&stream, &client), Bytes::from_static(b"original"), Some(0),
            key.clone(), operation, "original retry").await.expect("original append");
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(keyed_append_native_budget(), &cancellation)
            .expect("original ledger");
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        let error = append_keyed_with_work((&stream, &client), Bytes::from_static(b"changed"),
            Some(0), key.clone(), operation, "original retry", &mut Some(&mut observer))
            .await.expect_err("SDK mismatch");
        assert!(matches!(error, Error::NativeStream {
            code: wire::ErrorCode::Conflict, source: StreamError::IdempotencyMismatch, custody: None,
        }));
        assert_eq!(observer.into_custody().bytes(), 0);
        assert_eq!(work.receipt().authority_records_appended, 0);
        assert!(matches!(append_keyed((&stream, &client), Bytes::from_static(b"changed"),
            Some(0), key, operation, "original retry").await, Err(Error::Conflict(_))));
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_record_receive_denial_keeps_raw_prefix_without_retention() {
        use std::sync::atomic::Ordering;
        for denied_before_sdk in [true, false] {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
            let stream = client.stream("native-receiver-prefix").expect("source stream");
            stream.append_batch(
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")], Some(0), None,
            ).await.expect("original physical records");
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget = keyed_append_native_budget();
            budget.backend_read_operations = u64::from(!denied_before_sdk);
            budget.authority_bytes_read = 1;
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .expect("original ledger");
            let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
            let error = read_records_with_work(&stream, 0, 2, &mut Some(&mut observer))
                .await.expect_err("exact receiver or SDK admission denial");
            assert!(matches!(error, Error::NativeWork { .. }));
            assert_eq!(work.receipt().authority_records_read, if denied_before_sdk { 0 } else { 2 });
            assert_eq!(work.receipt().authority_bytes_read, if denied_before_sdk { 0 } else { 3 });
            assert_eq!(provider.io.reads.load(Ordering::SeqCst), u64::from(!denied_before_sdk));
            assert_eq!(observer.into_custody().bytes(), 0, "all actual records dropped before release");
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_refresh_keeps_original_serde_error_and_raw_record_prefix() {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(LostSessionAck::default());
        let client = StreamClient::new(provider.clone());
        let mut coordinator = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier))
            .await.expect("original empty coordinator");
        let raw = Bytes::from(encode(
            1, OperationId::from_bytes([16; 16]), "native-invalid-schema",
            *blake3::hash(b"{}").as_bytes(), b"{}".to_vec(), 1,
        ).expect("original envelope producer"));
        let raw_length = raw.len() as u64;
        coordinator.stream.append_batch(vec![raw], Some(0), None)
            .await.expect("original physical envelope");
        let reads = provider.io.reads.load(Ordering::SeqCst);
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = keyed_append_native_budget();
        budget.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 524_288;
        budget.authority_bytes_read = raw_length;
        budget.allocation_operations = 128;
        budget.items_examined = 128;
        budget.bytes_copied = 4096;
        budget.bytes_encoded = 4096;
        budget.bytes_hashed = COORDINATOR_WIRE_CONTRACT.len() as u64 + 2;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("original ledger");
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        let error = coordinator.refresh_with_work(&mut Some(&mut observer)).await
            .expect_err("original typed scheduler Deserialize failure");
        assert!(matches!(&error, Error::NativeDecode {
            code: wire::ErrorCode::Storage, source, custody: Some(guard),
        } if source.is_data() && guard.bytes() > 0));
        assert_eq!(work.receipt().authority_records_read, 1);
        assert_eq!(work.receipt().authority_bytes_read, raw_length);
        assert_eq!(provider.io.reads.load(Ordering::SeqCst), reads + 1);
        assert_eq!(coordinator.revision(), 0);
        assert_eq!(observer.into_custody().bytes(), 0, "record and envelope die before observer release");
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_envelope_cancellation_releases_original_adopted_input() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = keyed_append_native_budget();
        budget.bytes_copied = 2;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("original ledger");
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        let mut canonical = Vec::new();
        crate::store::history::HistoryIo::reserve_bytes(&mut observer, &mut canonical, 2)
            .expect("fund original buffer before growth");
        crate::store::history::HistoryIo::copy_bytes(&mut observer, 2).expect("fund original copy");
        canonical.extend_from_slice(b"{}");
        cancellation.cancel();
        let error = encode_with_work(
            1, OperationId::from_bytes([17; 16]), "native-cancelled-envelope",
            *blake3::hash(b"{}").as_bytes(), canonical, 1, &mut Some(&mut observer),
        ).expect_err("original cancellation before metadata construction");
        assert!(matches!(error, Error::NativeWork { .. }));
        assert_eq!(observer.into_custody().bytes(), 0, "input physically dropped before release");
        assert_eq!(work.receipt().backend_write_operations, 0);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_commit_once_preserves_original_sdk_error_and_separate_inspection() {
        use std::sync::atomic::Ordering;
        for lost_ack in [false, true] {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
            let path = acyclic_stream::StreamPath::new("native-one-shot-commit").expect("original path");
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget = keyed_append_native_budget();
            budget.bytes_copied = 64;
            budget.bytes_hashed = 2 * (b"acyclic-harness-coordinator-v3".len() + "native-once-key".len()) as u64;
            budget.peak_allocation_bytes = acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 8192;
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .expect("original ledger");
            let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
            let mut optional = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
            let key = stream_key_with_work("native-once-key", &mut optional).expect("original paid key");
            let mut prepared = PreparedCommitRequest::new_with_retained_key(key, 32, &mut optional);
            assert_eq!(prepared.commit_phase(), CommitPhase::NotCalled);
            prepared.push_condition(acyclic_stream::CommitCondition::Tail {
                path: path.clone(), expected: 0,
            }).expect("original condition");
            prepared.append(path.clone(), Bytes::from_static(b"original")).expect("original record");
            provider.lose_ack.store(lost_ack, Ordering::SeqCst);
            let result = prepared.commit_once_with_work(&client).await.expect("one SDK boundary");
            assert_eq!(prepared.commit_phase(), if lost_ack { CommitPhase::Unknown } else { CommitPhase::Committed });
            assert_eq!(provider.io.inspections.load(Ordering::SeqCst), 0, "once never reconciles or inspects");
            let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
            let quota = crate::wire_codec::CodecQuota::from_retained(prepared.work(), retained);
            let result = result;
            match result {
                Err(source) => assert_eq!(source, StreamError::Unavailable),
                Ok((outcome, _)) => {
                    assert!(matches!(&outcome, acyclic_stream::CommitOutcome::Committed(_)));
                    drop(outcome);
                }
            }
            drop(quota);
            drop(prepared);
            let key = stream_key_with_work("native-once-key", &mut optional).expect("fresh original paid key");
            let inspected = inspect_idempotency_with_work(&client, key, &mut optional).await
                .expect("same original inspector").expect("actual immutable observation");
            let quota = crate::wire_codec::CodecQuota::from_retained(&mut optional, inspected.1);
            let (observation, _) = inspected;
            assert!(matches!(&observation.outcome, IdempotencyOutcome::Commit(_)));
            drop(observation);
            drop(quota);
            drop(optional);
            assert_eq!(observer.into_custody().bytes(), 0, "actual keys/outcomes die before captured grants");
            assert_eq!(provider.io.writes.load(Ordering::SeqCst), 1);
            assert_eq!(provider.io.inspections.load(Ordering::SeqCst), 1);
            assert_eq!(work.receipt().authority_records_appended, u64::from(!lost_ack));
            assert_eq!(work.receipt().authority_records_read, 1 + u64::from(!lost_ack));
        }
    }

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
            )?));
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
                )?)],
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

    pub(super) fn spec(operation_id: OperationId, cpu: u64) -> Result<OperationSpec> {
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
        let legacy_protocol = wire::ProtocolIdentity {
            version: "2".into(),
            descriptor_digest: blake3::hash(
                b"acyclic.harness.coordinator.scheduler-event-envelope.v2",
            )
            .to_hex()
            .to_string(),
        };
        assert!(
            validate_protocol(
                Some(&legacy_protocol),
                &coordinator_protocol_identity(),
                Error::Storage,
            )
            .is_err()
        );
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
                    .apply_if_execution_quiescent(
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
                            .apply_if_execution_quiescent(
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

#[cfg(all(test, feature = "filesystem"))]
#[path = "distributed/measured_tests.rs"]
mod measured_tests;
