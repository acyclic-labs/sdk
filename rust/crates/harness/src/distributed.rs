//! Customer-hostable Stream-backed coordinator and pull-worker admission.

use crate::contract::capability;
use crate::store::{history::HistoryIo, operations};
use crate::wire_codec::CodecQuota;
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
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamClient, StreamError, StreamProvider,
};
use acyclic_stream::OwnedBytes;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) type SdkCallError = std::result::Result<StreamError, Error>;

pub(crate) fn sdk_source(error: &SdkCallError) -> &StreamError {
    match error {
        Ok(source) => source,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Err(Error::NativeStream { source, .. }) => source,
        Err(_) => unreachable!("non-SDK failures use the outer result"),
    }
}

pub(crate) fn sdk_error(error: SdkCallError, work: &Option<&mut dyn HistoryIo>) -> Error {
    match error {
        Ok(source) => operations::stream_error_with_work(source, work),
        Err(error) => error,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_call_result<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    result: acyclic_stream::NativeStreamResult<T>,
) -> Result<std::result::Result<acyclic_stream::NativeStreamOutput<T>, SdkCallError>> {
    match crate::store::history::native_sdk_result(original, result) {
        Ok(output) => Ok(Ok(output)),
        Err(error @ Error::NativeStream { .. }) => Ok(Err(Err(error))),
        Err(error) => Err(error),
    }
}

const COORDINATOR_PATH: &str = "harness/v3/coordinator/events";
const COORDINATOR_WIRE_VERSION: &str = "3";
const COORDINATOR_WIRE_CONTRACT: &[u8] = b"acyclic.harness.coordinator.scheduler-event-envelope.v3";
const READ_PAGE_SIZE: u32 = 1_024;

/// One direct, same-owner child in stable declared-slot order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChildOperationLink {
    /// Parent-owned slot, independent of execution or completion order.
    pub slot: String,
    /// Exact child operation identity.
    pub operation_id: OperationId,
}

/// Bounded hierarchy observation at one coordinator revision.
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
    /// Coordinator-observed revision associated with this reservation.
    pub operation_revision: u64,
}

/// Owner-scoped pull result.
pub enum WorkPull {
    /// No operation was admitted by this invocation.
    Idle,
    /// Coordinator publication was observed; execution must still verify it.
    Claimed(WorkLease),
    /// Publication or its observation failed.
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
pub trait SchedulerPayloadStore: acyclic_stream::ProviderPlatform {
    /// Stages JSON bytes before the coordinator publishes their reference.
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<FileRef>>;

    /// Stages the same original operation and retry key on the caller's ledger.
    #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
    fn stage_with_native_work<'a>(
        &'a self,
        _operation_id: OperationId,
        _idempotency_key: &'a str,
        _bytes: &'a [u8],
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code: wire::ErrorCode::Unsupported,
            reason: "scheduler payload store has no native work observer" })
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
        crate::contract::compile_json_schema(&entrypoint.result_schema, "reducer result", None)?;
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

/// One verified durable coordinator event for an asynchronous projector.
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedSchedulerEvent {
    /// Dense one-based coordinator revision.
    pub revision: u64,
    /// Monotonic UTC time fixed by the coordinator at durable append.
    pub committed_at_ms: u64,
    /// Decoded scheduler transition.
    pub event: SchedulerEvent,
}

/// Reads one bounded page of verified coordinator events.
pub async fn read_coordinator_event_page<P: StreamProvider>(
    client: &StreamClient<P>, after_revision: u64, limit: u32,
) -> Result<Vec<CommittedSchedulerEvent>> {
    if limit == 0 {
        return Err(Error::Invalid("coordinator page limit is out of bounds".into()));
    }
    let stream = client.stream(COORDINATOR_PATH)?;
    let page = read_records_with_work(client, &stream, after_revision,
        limit.min(READ_PAGE_SIZE), &mut None).await?.unwrap_or_default().0;
    let mut events = Vec::with_capacity(page.len());
    for record in page {
        let (revision, _, _, committed_at_ms, event) =
            decode_with_work(&record, &mut None)?;
        events.push(CommittedSchedulerEvent {
            revision, committed_at_ms, event,
        });
    }
    Ok(events)
}

pub(crate) async fn observe_declaration<P: StreamProvider>(
    client: &StreamClient<P>, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
    operation_id: OperationId, declaration_key: &IdempotencyKey,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<OperationSpec>> {
    DistributedCoordinator::<P>::authorize_operation_scope_with_work(
        owner, scope, verifier, capability::OPERATION_OBSERVE, work,
    )?;
    let mut custody = CodecQuota::new(work, 0)?;
    let Some((_, event, ..)) = read_intent(
        client, declaration_key.as_str(), None, custody.work(),
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
    spec.verify_owner(owner, verifier, custody.work())?;
    let spec = *spec;
    if custody.work().is_some() { custody.relinquish(std::mem::size_of::<OperationSpec>() as u64)?; }
    custody.transfer();
    Ok(Some(spec))
}

#[cfg(feature = "filesystem")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn original_reservation_proof_with_work<P: StreamProvider>(
    client: &StreamClient<P>, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
    operation: OperationId, candidate: &LeaseFence, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(OperationSpec, LeaseFence)> {
    if work.is_none() {
        return Err(Error::Unsupported("historical reservation needs a finite original observer".into()));
    }
    DistributedCoordinator::<P>::authorize_operation_scope_with_work(
        owner, scope, verifier, capability::OPERATION_OBSERVE, work,
    )?;
    let mut custody = CodecQuota::new(work, 0)?;
    let key = formatted_text_with_work(
        "task-admission:".len() + 36, format_args!("task-admission:{operation}"), custody.work(),
    )?;
    custody.adopt(key.capacity() as u64)?;
    let declaration = read_intent(
        client, &key, None, custody.work(),
    ).await;
    let key_capacity = key.capacity() as u64;
    drop(key);
    custody.relinquish(key_capacity)?;
    let (_, event, declaration_revision, _) = declaration?
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
            "original task declaration source"))?;
    custody.adopt(event.logical_owned_bytes()?)?;
    let SchedulerEvent::Declared { spec } = event else {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "task admission does not identify a declaration"));
    };
    let declaration = spec;
    let mut started = None;
    if declaration.operation_id != operation {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "historical declaration identifies another operation"));
    }
    declaration.verify_owner(owner, verifier, custody.work())?;
    let stream = source_stream(client, COORDINATOR_PATH, custody.work())?;
    let path_bytes = if custody.work().as_deref()
        .is_some_and(operations::original_native_decode_mode)
    { 0 } else { COORDINATOR_PATH.len() as u64 };
    custody.adopt(path_bytes)?;
    let tail = operations::tail_with_work(client, stream.stream.path(), custody.work()).await?;
    if tail < declaration_revision {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "declaration lies beyond coordinator source"));
    }
    let mut next = declaration_revision;
    let mut admitted = None;
    let mut last_time = None;
    while next < tail {
        let maximum = (tail - next).min(u64::from(READ_PAGE_SIZE)) as u32;
        let (records, retained) = read_records_with_work(client, &stream.stream, next, maximum, custody.work()).await?
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "historical coordinator prefix is absent"))?;
        custody.adopt(retained)?;
        if records.is_empty() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "historical coordinator prefix is incomplete"));
        }
        let capacity = records.capacity();
        for record in records {
            let raw_bytes = operations::sdk_record_retained_bytes(&record, custody.work());
            let mut event_custody = CodecQuota::new(custody.work(), 0)?;
            let (revision, key, _, time, event) = decode_with_work(&record, event_custody.work())?;
            let owned = event.logical_owned_bytes()?;
            event_custody.adopt(owned)?;
            event_custody.adopt(key.capacity() as u64)?;
            if last_time.is_some_and(|previous| time <= previous)
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
                    if admitted.is_none_or(|admission| admission >= revision) || started.is_some() {
                        return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                            "historical reservation has no unique admitted start"));
                    }
                    event_custody.transfer();
                    drop(event_custody);
                    custody.adopt(owned)?;
                    started = Some(fence);
                }
                event => {
                    drop(event);
                    drop(event_custody);
                }
            }
            next = revision;
            drop(record);
            custody.relinquish(raw_bytes)?;
        }
        custody.relinquish((capacity * std::mem::size_of::<operations::ObservedRecord>()) as u64)?;
    }
    drop(stream);
    custody.relinquish(path_bytes)?;
    let original_fence = started.take()
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
            "selected original reservation start"))?;
    let declaration = *declaration;
    custody.relinquish(std::mem::size_of::<OperationSpec>() as u64)?;
    custody.transfer();
    Ok((declaration, original_fence))
}

async fn read_records_with_work<P: StreamProvider>(
    client: &StreamClient<P>, stream: &Stream<P>, from: u64, maximum: u32,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<(Vec<operations::ObservedRecord>, u64)>> {
    let mut custody = CodecQuota::new(work, 0)?;
    let mut source = match operations::read_records_with_work(
        client, stream, from, maximum, custody.work(),
    ).await? {
        Ok(source) => source,
        Err(error) if *sdk_source(&error) == StreamError::NotFound && from == 0 => return Ok(None),
        Err(error) => return Err(sdk_error(error, custody.work())),
    };
    let mut records = Vec::new();
    loop {
        if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
        let Some(record) = source.next(custody.work()).await? else {
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            break;
        };
        if custody.work().is_some()
            && !custody.work().as_deref().is_some_and(operations::original_native_decode_mode)
        {
            custody.adopt(record.value.len() as u64)?;
        }
        if records.len() >= maximum as usize
            || record.value.len() > acyclic_stream::MAX_RECORD_BYTES
        {
            return Err(operations::stream_error_with_work(
                StreamError::LimitExceeded, custody.work(),
            ));
        }
        let old_capacity = records.capacity();
        if let Some(work) = custody.work().as_deref_mut() {
            work.reserve_records(&mut records, maximum as usize)?;
            custody.adopt(((records.capacity() - old_capacity)
                * std::mem::size_of::<operations::ObservedRecord>()) as u64)?;
        }
        records.push(record);
    }
    let mut expected = from;
    for record in &records {
        if record.sequence != expected {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "coordinator record prefix has a cursor gap"));
        }
        expected = next_revision(expected)?;
    }
    let retained = custody.retained();
    custody.transfer();
    Ok(Some((records, retained)))
}

fn coordinator_boundary(
    work: &Option<&mut dyn HistoryIo>, code: wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(operations::original_native_decode_mode) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

fn source_stream<P: StreamProvider>(
    client: &StreamClient<P>, path: &str, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<operations::ObservedStream<P>> {
    let portable = work.is_some()
        && !work.as_deref().is_some_and(operations::original_native_decode_mode);
    if portable {
        let work = work.as_deref_mut().expect("portable observer");
        work.admit_allocation(path.len() as u64)?;
        work.copy_bytes(path.len() as u64)?;
    }
    let stream = operations::stream_text_with_work(client, path, work)?;
    if portable {
        work.as_deref_mut().expect("portable observer").retain_owned(path.len() as u64)?;
    }
    Ok(stream)
}

async fn read_intent<P: StreamProvider>(
    client: &StreamClient<P>, key: &str, maximum_revision: Option<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<([u8; 32], SchedulerEvent, u64, acyclic_stream::CommitId)>> {
    validate_idempotency_key(key, work)?;
    if maximum_revision == Some(0) { return Ok(None); }
    let mut custody = CodecQuota::new(work, 0)?;
    let source = source_stream(client, COORDINATOR_PATH, custody.work())?;
    let path_bytes = if custody.work().is_some()
        && !custody.work().as_deref().is_some_and(operations::original_native_decode_mode)
    { COORDINATOR_PATH.len() as u64 } else { 0 };
    custody.adopt(path_bytes)?;
    let tail = match maximum_revision {
        Some(tail) => tail,
        None => operations::tail_optional_with_work(client, source.stream.path(), custody.work())
            .await?.unwrap_or(0),
    };
    let mut next = 0;
    let mut intent = None;
    while next < tail {
        let maximum = (tail - next).min(u64::from(READ_PAGE_SIZE)) as u32;
        let (records, retained) = read_records_with_work(client, &source.stream, next, maximum, custody.work()).await?
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "coordinator intent prefix is absent"))?;
        if records.is_empty() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "coordinator intent prefix is incomplete"));
        }
        custody.adopt(retained)?;
        let slots = (records.capacity() * std::mem::size_of::<operations::ObservedRecord>()) as u64;
        for record in records {
            let raw_bytes = operations::sdk_record_retained_bytes(&record, custody.work());
            if inspect_envelope(&record.value, false, custody.work())?.2 == Some(key) {
                let mut event_custody = CodecQuota::new(custody.work(), 0)?;
                let (revision, source_key, digest, _, event) = decode_with_work(&record, event_custody.work())?;
                let owned = if event_custody.work().is_some() { event.logical_owned_bytes()? } else { 0 };
                let key_bytes = if event_custody.work().is_some() { source_key.capacity() as u64 } else { 0 };
                event_custody.adopt(owned + key_bytes)?;
                drop(source_key);
                event_custody.relinquish(key_bytes)?;
                if intent.is_some() {
                    return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                        "coordinator history repeats an idempotency key"));
                }
                event_custody.transfer();
                drop(event_custody);
                custody.adopt(owned)?;
                intent = Some((digest, event, revision, record.commit_id));
            }
            next = next_revision(record.sequence)?;
            drop(record);
            if custody.work().is_some() { custody.relinquish(raw_bytes)?; }
        }
        if custody.work().is_some() { custody.relinquish(slots)?; }
    }
    drop(source);
    custody.relinquish(path_bytes)?;
    custody.transfer();
    Ok(intent)
}

/// One logical coordinator whose entire semantic history is a Stream.
pub struct DistributedCoordinator<P> {
    client: StreamClient<P>,
    stream: operations::ObservedStream<P>,
    scheduler: Scheduler,
    revision: u64,
    last_committed_at_ms: u64,
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

impl<P: StreamProvider> DistributedCoordinator<P> {
    #[cfg(feature = "filesystem")]
    pub(crate) fn journal_client(&self) -> StreamClient<P> {
        self.client.clone()
    }

    #[cfg(feature = "filesystem")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn journal_condition(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        operation_id: OperationId, fence: &LeaseFence, write: JournalWrite,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.refresh_with_work(work).await?;
        let operation = self.observe_operation(
            owner, scope, verifier, operation_id, work,
        )?;
        crate::scheduler::require_execution_owner(operation, fence, matches!(write, JournalWrite::Settlement), work)?;
        if let JournalWrite::Model {
            attempt_id,
            step,
            request_digest,
        } = write
        {
            self.scheduler
                .require_model_claim(operation_id, attempt_id, step, &request_digest, &mut None)?;
        }
        Ok(acyclic_stream::CommitCondition::Tail {
            path: self.stream.stream.path().clone(),
            expected: self.revision,
        })
    }

    /// Opens and replays the coordinator history.
    pub async fn open(
        client: &StreamClient<P>, content_verifier: Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        Self::open_with_work(client, content_verifier, &mut None).await
    }

    /// Opens and replays the coordinator on the caller's native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn open_with_native_work(
        client: &StreamClient<P>, content_verifier: Arc<dyn ContentResidencyVerifier>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let mut io = crate::store::history::native_work_io(work)?;
        Self::open_with_work(client, content_verifier, &mut Some(&mut io)).await
    }

    async fn open_with_work(
        client: &StreamClient<P>, content_verifier: Arc<dyn ContentResidencyVerifier>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let stream = source_stream(client, COORDINATOR_PATH, work)?;
        let mut value = Self {
            client: client.clone(), stream, scheduler: Scheduler::new(), revision: 0,
            last_committed_at_ms: 0, content_verifier, payload_store: None,
        };
        value.refresh_with_work(work).await?;
        Ok(value)
    }

    /// Replays commits made by other coordinator instances from the exact observed revision.
    pub async fn refresh(&mut self) -> Result<()> {
        self.refresh_with_work(&mut None).await
    }

    pub(crate) async fn refresh_with_work(
        &mut self, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let mut next = self.revision;
        loop {
            let mut page_custody = CodecQuota::new(work, 0)?;
            let (page, retained) = read_records_with_work(&self.client, &self.stream.stream, next, READ_PAGE_SIZE, page_custody.work())
                .await?.unwrap_or_default();
            page_custody.adopt(retained)?;
            if page.is_empty() { break; }
            let page_slots = (page.capacity() * std::mem::size_of::<operations::ObservedRecord>()) as u64;
            for record in page {
                let raw_bytes = operations::sdk_record_retained_bytes(&record, page_custody.work());
                let mut event_custody = CodecQuota::new(page_custody.work(), 0)?;
                if let Some(work) = event_custody.work().as_deref_mut() { work.check_cancellation()?; }
                let (revision, key, _, committed_at_ms, event) =
                    decode_with_work(&record, event_custody.work())?;
                if event_custody.work().is_some() {
                    event_custody.adopt(event.logical_owned_bytes()?)?;
                    event_custody.adopt(key.capacity() as u64)?;
                }
                if let SchedulerEvent::Declared { spec } = &event {
                    self.verify_content_with_work(&spec.state, event_custody.work()).await?;
                }
                self.verify_published_result_with_work(&event, event_custody.work()).await?;
                if let Some((_, existing, ..)) = read_intent(
                    &self.client, &key, Some(self.revision), event_custody.work(),
                ).await? {
                    if event_custody.work().is_some() { event_custody.adopt(existing.logical_owned_bytes()?)?; }
                    return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Conflict,
                        "coordinator history repeats an idempotency key"));
                }
                if let Some(work) = event_custody.work().as_deref_mut() { work.check_cancellation()?; }
                if committed_at_ms <= self.last_committed_at_ms {
                    return Err(coordinator_boundary(event_custody.work(), wire::ErrorCode::Storage,
                        "coordinator commit time is not increasing"));
                }
                self.scheduler.apply(&event, event_custody.work())?;
                self.revision = revision;
                next = revision;
                self.last_committed_at_ms = committed_at_ms;
                drop((key, event));
                drop(event_custody);
                drop(record);
                if page_custody.work().is_some() { page_custody.relinquish(raw_bytes)?; }
            }
            if page_custody.work().is_some() { page_custody.relinquish(page_slots)?; }
        }
        Ok(())
    }

    async fn intent_matches_with_work(
        &self, key: &str, event: &SchedulerEvent, digest: &[u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<bool>> {
        let mut custody = CodecQuota::new(work, 0)?;
        let Some(intent) = read_intent(&self.client, key, Some(self.revision), custody.work()).await? else {
            return Ok(None);
        };
        if custody.work().is_some() { custody.adopt(intent.1.logical_owned_bytes()?)?; }
        Ok(Some(&intent.0 == digest && &intent.1 == event))
    }

    /// Binds the owner-controlled staging boundary for joins, quorums and reducers.
    #[must_use]
    pub fn with_payload_store(mut self, store: Arc<dyn SchedulerPayloadStore>) -> Self {
        self.payload_store = Some(store);
        self
    }

    async fn verify_content_with_work(
        &self, reference: &FileRef, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        match work.as_deref_mut() {
            Some(work) => crate::store::history::verify_content_with_work(
                work, self.content_verifier.as_ref(), reference).await,
            None => self.content_verifier.verify(reference).await,
        }
    }

    async fn verify_published_result_with_work(
        &self, event: &SchedulerEvent, work: &mut Option<&mut dyn HistoryIo>,
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
        let mut custody = CodecQuota::new(work, 0)?;
        let (value, value_bytes) = self.load_json_with_work(reference, custody.work()).await?;
        custody.adopt(value_bytes)?;
        if let SchedulerEvent::Orchestrated { operation_id, reducer: None, .. } = event {
            let decision = self.scheduler.orchestration(*operation_id, custody.work())?;
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
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
                "scheduler result operation is absent"))?;
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

    pub(crate) fn observe_operation<'a>(
        &'a self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        operation_id: OperationId, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<&'a OperationState> {
        self.authorize_operation_with_work(
            owner, scope, verifier, operation_id, capability::OPERATION_OBSERVE, work,
        )
    }

    /// Returns the page with the exact deep custody produced by its field copies.
    pub(crate) async fn observe_children(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        parent: OperationId, request: ChildOperationPageRequest<'_>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(ChildOperationPage, u64)> {
        runtime::validate_child_page_request(
            "child hierarchy", parent.into_bytes(), request.after_slot, request.maximum,
        )?;
        Self::authorize_operation_scope_with_work(
            owner, scope, verifier, capability::OPERATION_OBSERVE, work,
        )?;
        if work.as_deref().is_some_and(operations::original_native_decode_mode)
            && self.scheduler.operation(parent).is_some()
        {
            self.observe_operation(owner, scope, verifier, parent, work)?;
        }
        self.refresh_with_work(work).await?;
        self.observe_operation(owner, scope, verifier, parent, work)?;
        if request
            .expected_revision
            .is_some_and(|revision| revision != self.revision)
        {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "child hierarchy revision changed"));
        }
        let mut custody = CodecQuota::new(work, 0)?;
        let mut entries = crate::core::reserve_copy_vector::<ChildOperationLink>(
            request.maximum, &mut custody,
        )?;
        let mut has_more = false;
        let mut children = self.scheduler.children(parent);
        loop {
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            let Some((slot, child)) = children.next() else { break };
            if let Some(work) = custody.work().as_deref_mut() {
                work.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            if request.after_slot.is_some_and(|after| slot <= after)
                || child.spec.owner.authority() != owner
            {
                continue;
            }
            if entries.len() == request.maximum {
                has_more = true;
                break;
            }
            child.spec.verify_owner(owner, verifier, custody.work())?;
            if child.spec.operation_id.into_bytes() == [0; 16] {
                return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                    "child hierarchy contains a nil operation"));
            }
            let slot = crate::core::copy_text(slot, &mut custody)?;
            entries.push(ChildOperationLink {
                slot,
                operation_id: child.spec.operation_id,
            });
        }
        let next_after = if has_more {
            entries.last().map(|entry| crate::core::copy_text(&entry.slot, &mut custody)).transpose()?
        } else {
            None
        };
        let page = ChildOperationPage {
            revision: self.revision,
            entries,
            next_after,
        };
        let retained = custody.retained();
        custody.transfer();
        Ok((page, retained))
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
    ) -> Result<bool> {
        self.refresh().await?;
        self.authorize_operation_with_work(owner, scope, verifier, operation_id, "operation:declare", &mut None)?;
        self.apply_internal(
            operation_id,
            idempotency_key,
            SchedulerEvent::SessionConfigured {
                operation_id,
                limits,
            }, None, None, None, None, None, &mut None
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
    ) -> Result<(bool, OperationState)> {
        self.cancel_operation_with_work(
            owner, scope, verifier, operation_id, idempotency_key, recursive, &mut None,
        ).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn cancel_operation_with_work(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        operation_id: OperationId, idempotency_key: IdempotencyKey, recursive: bool,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(bool, OperationState)> {
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
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                "operation is absent after cancellation"))?
            .clone_with_work(work)?;
        Ok((applied, state))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn cancel_child_operation(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        operation_id: OperationId, idempotency_key: IdempotencyKey, parent: OperationId,
        fence: &LeaseFence, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<OperationState> {
        Self::authorize_operation_scope_with_work(
            owner, scope, verifier, capability::OPERATION_CANCEL, work,
        )?;
        self.refresh_with_work(work).await?;
        let child = self.authorize_operation_owner_with_work(owner, verifier, operation_id, work)?;
        if child.spec.parent.as_ref().map(|link| link.operation_id) != Some(parent) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "cancellation requires an authorized direct child"));
        }
        let parent_state = self.authorize_operation_owner_with_work(owner, verifier, parent, work)?;
        crate::scheduler::require_execution_owner(parent_state, fence, false, work)?;
        let verified_revision = self.revision;
        self.apply_internal(
            operation_id, idempotency_key,
            SchedulerEvent::CancellationRequested { operation_id, recursive: true },
            Some(fence), None, None, None, Some(verified_revision), work,
        ).await?;
        self.scheduler.operation(operation_id)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                "operation is absent after cancellation"))?
            .clone_with_work(work)
    }

    fn authorize_operation_with_work<'a>(
        &'a self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        operation_id: OperationId, capability: &str, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<&'a OperationState> {
        Self::authorize_operation_scope_with_work(owner, scope, verifier, capability, work)?;
        self.authorize_operation_owner_with_work(owner, verifier, operation_id, work)
    }

    pub(crate) fn authorize_operation_scope_with_work(
        owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier, capability: &str,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        verifier.verify_audience_with_work(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains(capability) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "owner scope lacks the required operation capability"));
        }
        Ok(())
    }

    fn authorize_operation_owner_with_work<'a>(
        &'a self, owner: &Authority, verifier: &AuthorityVerifier, operation_id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<&'a OperationState> {
        let operation = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                "operation is absent for this owner"))?;
        let declared_owner = operation.spec.owner.authority();
        if declared_owner != owner {
            return Err(coordinator_boundary(work, wire::ErrorCode::NotFound,
                "operation is absent for this owner"));
        }
        operation.spec.verify_owner(owner, verifier, work)?;
        Ok(operation)
    }

    /// Applies an event and returns whether this call appended it.
    pub async fn apply(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
    ) -> Result<bool> {
        self.apply_with_work(operation_id, idempotency_key, event, &mut None).await
    }

    pub(crate) async fn apply_with_work(
        &mut self, operation_id: OperationId, idempotency_key: IdempotencyKey,
        event: SchedulerEvent, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        if matches!(&event, SchedulerEvent::Declared { .. } | SchedulerEvent::SessionConfigured { .. }
            | SchedulerEvent::WorkflowResumed { .. } | SchedulerEvent::Orchestrated { .. })
        {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "scheduler event requires its scoped coordinator API"));
        }
        self.apply_internal(
            operation_id, idempotency_key, event, None, None, None, None, None, work,
        ).await
    }

    pub(crate) async fn apply_if_execution_quiescent(
        &mut self, key: IdempotencyKey, event: SchedulerEvent, execution: OperationId,
        expected_tail: u64, workflow_guard: Option<(&str, u64)>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let verified_revision = work.is_some().then_some(self.revision);
        if !matches!(&event, SchedulerEvent::WorkflowSuspended { .. } | SchedulerEvent::LeaseReleased { .. }) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "idle execution guard requires suspension or lease release"));
        }
        if work.is_some() && matches!(&event, SchedulerEvent::LeaseReleased { .. }) && workflow_guard.is_none() {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "measured lease release requires its original workflow witness"));
        }
        self.apply_internal(
            event.operation_id(), key, event, None, None,
            Some((execution, expected_tail)), workflow_guard, verified_revision, work,
        ).await
    }

    /// Fills a durable resume slot, optionally guarded by its selected command.
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_workflow(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        workflow_revision: u64,
        input: FileRef,
        idempotency_key: IdempotencyKey,
        waiting_command: Option<OperationId>,
    ) -> Result<bool> {
        self.resume_workflow_with_work(owner, scope, verifier, operation_id, workflow_revision,
            input, idempotency_key, waiting_command, &mut None).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn resume_workflow_with_work(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        operation_id: OperationId, workflow_revision: u64, input: FileRef,
        idempotency_key: IdempotencyKey, waiting_command: Option<OperationId>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        verifier.verify_audience_with_work(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains("operation:wake") {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                "owner scope lacks operation:wake"));
        }
        self.refresh_with_work(work).await?;
        self.observe_operation(owner, scope, verifier, operation_id, work)?;
        self.apply_internal(
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
        parent_fence: Option<LeaseFence>,
    ) -> Result<bool> {
        self.declare_operation_with_work(owner, scope, verifier, spec, idempotency_key, parent_fence, &mut None).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn declare_operation_with_work(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        spec: OperationSpec, idempotency_key: IdempotencyKey, parent_fence: Option<LeaseFence>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        if parent_fence.is_some() && spec.parent.is_none() {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "owned child declaration has no parent"));
        }
        verifier.verify_audience_with_work(owner, work)?;
        verifier.verify_scope_for_with_work(owner, scope, work)?;
        if !scope.capabilities().contains(capability::OPERATION_DECLARE) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Unauthorized, "scope lacks operation:declare"));
        }
        spec.verify_owner(owner, verifier, work)?;
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
        let result = self.apply_internal(
            operation_id, idempotency_key, event, parent_fence.as_ref(), None, None, None, None, work,
        ).await;
        if let Some(work) = work.as_deref_mut() { work.release_owned(box_bytes)?; }
        result
    }

    #[allow(clippy::too_many_lines,         clippy::too_many_arguments)]
    pub(crate) async fn declare_session(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier,
        spec: OperationSpec, limits: crate::scheduler::SessionLimits,
        idempotency_key: IdempotencyKey, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let mut custody = CodecQuota::new(work, 0)?;
        verifier.verify_audience_with_work(owner, custody.work())?;
        verifier.verify_with_work(scope, custody.work())?;
        if !scope.capabilities().contains("operation:declare") || spec.owner.authority() != owner {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Unauthorized,
                "session declaration owner is invalid"));
        }
        spec.verify_owner(owner, verifier, custody.work())?;
        if spec.parent.is_some() {
            return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                "session declaration must be a root"));
        }
        limits.validate(custody.work())?;
        validate_idempotency_key(idempotency_key.as_str(), custody.work())?;
        let operation_id = spec.operation_id;
        let declaration_key = crate::core::copy_text(idempotency_key.as_str(), &mut custody)?;
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
        if self.session_intent_with_work(&keys, &events, custody.work()).await?.is_some() {
            return Ok(false);
        }
        let mut request = CodecQuota::new(custody.work(), 0)?;
        let mut records = [OwnedBytes::new(), OwnedBytes::new()];
        let mut revision = self.revision;
        let mut committed_at_ms = self.next_committed_at_ms_with_work(request.work())?;
        for (index, (key, event)) in keys.iter().zip(&events).enumerate() {
            if let SchedulerEvent::Declared { spec } = event {
                self.verify_content_with_work(&spec.state, request.work()).await?;
                self.scheduler.validate_session(spec, &limits, request.work())?;
            }
            revision = revision.checked_add(1).ok_or_else(|| {
                coordinator_boundary(request.work(), wire::ErrorCode::Invalid,
                    "coordinator revision exhausted")
            })?;
            let canonical = crate::store::history::canonical_bytes_with_work(
                event, acyclic_stream::MAX_RECORD_BYTES as u64, request.work(),
            )?;
            let canonical_held = if request.work().is_some() { canonical.capacity() as u64 } else { 0 };
            let mut canonical_custody = CodecQuota::from_retained(
                request.work(), canonical_held,
            );
            if let Some(work) = canonical_custody.work().as_deref_mut() {
                work.hashed_bytes(canonical.len() as u64)?;
            }
            let digest = *blake3::hash(&canonical).as_bytes();
            canonical_custody.transfer();
            let encoded = encode(
                revision, operation_id, key, digest, canonical, committed_at_ms, canonical_custody.work(),
            )?;
            drop(canonical_custody);
            let (encoded, encoded_held) = operations::owned_bytes_from_retained_vec(
                encoded, request.work(),
            )?;
            request.adopt(encoded_held)?;
            records[index] = encoded;
            committed_at_ms = committed_at_ms.checked_add(1).ok_or_else(|| {
                coordinator_boundary(request.work(), wire::ErrorCode::Invalid,
                    "coordinator commit time exhausted")
            })?;
        }
        let (stream_key, key_held) = stream_key_with_work(idempotency_key.as_str(), request.work())?;
        let retained_request = request.retained();
        request.transfer();
        let mut key_custody = CodecQuota::from_retained(request.work(), key_held);
        let appended = append(
            (&self.stream.stream, &self.client), records, retained_request, Some(self.revision),
            stream_key, operation_id, key_custody.work(),
        ).await;
        drop(key_custody);
        let outcome = match appended {
            Ok(outcome) => outcome,
            Err(error) if coordinator_retry_conflict(&error) => {
                self.refresh_with_work(request.work()).await?;
                if self.session_intent_with_work(&keys, &events, request.work()).await?.is_some() {
                    return Ok(false);
                }
                return Err(coordinator_boundary(request.work(), wire::ErrorCode::Conflict,
                    "session retry identity reused"));
            }
            Err(error) => return Err(error),
        };
        self.finish_session_append_with_work(
            outcome, &keys, &events, revision - 2, request.work(),
        ).await
    }

    async fn session_intent_with_work(
        &mut self, keys: &[String; 2], events: &[SchedulerEvent; 2],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<(u64, acyclic_stream::CommitId)>> {
        let mut retained = 0;
        let mut previous: Option<(u64, acyclic_stream::CommitId)> = None;
        for (key, event) in keys.iter().zip(events) {
            let mut custody = CodecQuota::new(work, 0)?;
            if let Some(intent) = read_intent(&self.client, key, Some(self.revision), custody.work()).await? {
                if custody.work().is_some() {
                    custody.adopt(intent.1.logical_owned_bytes()?)?;
                }
                let (digest, existing, revision, commit_id) = &intent;
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
            0 => Ok(None),
            2 => Ok(previous),
            _ => Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "session declaration is not atomic")),
        }
    }

    async fn finish_session_append_with_work(
        &mut self, outcome: AppendOutcome, keys: &[String; 2], events: &[SchedulerEvent; 2],
        expected_start: u64, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let (result, receipt) = match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(2) || receipt.tail < receipt.end {
                    return Err(coordinator_boundary(work, wire::ErrorCode::Storage,
                        "invalid session append receipt"));
                }
                let result = receipt.start == expected_start;
                (result, Some((receipt.commit_id, receipt.start)))
            }
            AppendOutcome::TailConflict { .. } => (false, None),
        };
        self.refresh_with_work(work).await?;
        let Some((revision, commit_id)) = self.session_intent_with_work(keys, events, work).await? else {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "session append conflicted without its complete intent"));
        };
        if let Some((envelope, start)) = receipt
            && (commit_id != envelope || revision.checked_sub(2) != Some(start))
        {
            return Err(coordinator_boundary(work, wire::ErrorCode::Storage,
                "session append receipt differs from its committed envelope"));
        }
        Ok(result)
    }

    fn validate_replay(
        &self, event: &SchedulerEvent, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
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
            crate::scheduler::require_execution_owner(operation, fence, false, work)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn apply_internal(
        &mut self, operation_id: OperationId, idempotency_key: IdempotencyKey,
        event: SchedulerEvent, parent_fence: Option<&LeaseFence>,
        waiting_command: Option<OperationId>, idle_execution: Option<(OperationId, u64)>,
        workflow_guard: Option<(&str, u64)>, verified_revision: Option<u64>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        self.refresh_with_work(work).await?;
        if verified_revision.is_some_and(|revision| revision != self.revision) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "coordinator changed after owner verification"));
        }
        self.require_parent_owner_with_work(&event, parent_fence, work)?;
        validate_idempotency_key(idempotency_key.as_str(), work)?;
        let key = idempotency_key.as_str();
        if event.operation_id() != operation_id {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "scheduler event belongs to another operation"));
        }
        let mut custody = CodecQuota::new(work, 0)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &event, acyclic_stream::MAX_RECORD_BYTES as u64, custody.work(),
        )?;
        if custody.work().is_some() { custody.adopt(canonical.capacity() as u64)?; }
        if let Some(work) = custody.work().as_deref_mut() { work.hashed_bytes(canonical.len() as u64)?; }
        let digest = *blake3::hash(&canonical).as_bytes();
        if let Some(matches) = self.intent_matches_with_work(key, &event, &digest, custody.work()).await? {
            return if matches {
                self.require_command_wait_with_work(&event, waiting_command, true, custody.work())?;
                self.validate_replay(&event, custody.work()).map(|()| false)
            } else {
                Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict, "coordinator retry identity reused"))
            };
        }
        self.require_command_wait_with_work(&event, waiting_command, false, custody.work())?;
        if let SchedulerEvent::Declared { spec } = &event {
            self.verify_content_with_work(&spec.state, custody.work()).await?;
        }
        self.verify_published_result_with_work(&event, custody.work()).await?;
        self.scheduler.validate_event(&event, custody.work())?;
        let revision = next_revision(self.revision)?;
        let committed_at_ms = self.next_committed_at_ms_with_work(custody.work())?;
        custody.transfer();
        let bytes = encode(
            revision, operation_id, key, digest, canonical, committed_at_ms, custody.work(),
        )?;
        let (record, retained) = operations::owned_bytes_from_retained_vec(bytes, custody.work())?;
        custody.adopt(retained)?;
        let (stream_key, retained) = physical_key_with_work(
            key, self.revision, parent_fence.is_some(), waiting_command.is_some(), idle_execution, custody.work(),
        )?;
        custody.adopt(retained)?;
        let appended = if let Some((execution, expected_tail)) = idle_execution {
            append_if_execution_idle_with_work(
                &self.client, &self.stream.stream, record, self.revision, stream_key,
                operation_id, execution, expected_tail, workflow_guard, custody.work(),
            ).await
        } else {
            append(
                (&self.stream.stream, &self.client), [record], 0, Some(self.revision),
                stream_key, operation_id, custody.work(),
            ).await
        };
        custody.relinquish(custody.retained())?;
        let outcome = match appended {
            Ok(outcome) => outcome,
            Err(error) if idle_execution.is_none() && coordinator_retry_conflict(&error) => {
                self.refresh_with_work(custody.work()).await?;
                if self.intent_matches_with_work(key, &event, &digest, custody.work()).await? == Some(true) {
                    self.require_command_wait_with_work(&event, waiting_command, true, custody.work())?;
                    self.require_parent_owner_with_work(&event, parent_fence, custody.work())?;
                    return self.validate_replay(&event, custody.work()).map(|()| false);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let applied = self.finish_append_with_work(outcome, &event, key, &digest, revision, custody.work()).await?;
        self.require_parent_owner_with_work(&event, parent_fence, custody.work())?;
        Ok(applied)
    }

    fn require_command_wait_with_work(
        &self, event: &SchedulerEvent, waiting: Option<OperationId>, replaying: bool,
        work: &mut Option<&mut dyn HistoryIo>,
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
                    || slot.input.as_ref() != replaying.then_some(input)
            })
        {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "selected command wait is no longer current"));
        }
        Ok(())
    }

    fn require_parent_owner_with_work(
        &self, event: &SchedulerEvent, fence: Option<&LeaseFence>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let Some(fence) = fence else {
            return Ok(());
        };
        let parent = match event {
            SchedulerEvent::Declared { spec } => spec.parent.as_ref()
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Invalid,
                    "child declaration has no parent"))?.operation_id,
            SchedulerEvent::CancellationRequested { operation_id, recursive: true } => {
                self.scheduler.operation(*operation_id)
                    .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                        "cancellation child"))?
                    .spec.parent.as_ref()
                    .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Unauthorized,
                        "cancellation requires an authorized direct child"))?.operation_id
            }
            _ => return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "parent fence only applies to declaration or recursive child cancellation")),
        };
        let operation = self.scheduler.operation(parent)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound, "admitting parent"))?;
        crate::scheduler::require_execution_owner(operation, fence, false, work)
    }

    async fn finish_append_with_work(
        &mut self, outcome: AppendOutcome, event: &SchedulerEvent, key: &str, digest: &[u8; 32],
        revision: u64, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let appended = match outcome {
            AppendOutcome::Committed(receipt) => {
                if receipt.end.checked_sub(receipt.start) != Some(1) || receipt.tail < receipt.end {
                    return Err(coordinator_boundary(work, wire::ErrorCode::Storage, "invalid coordinator append receipt"));
                }
                receipt.start == revision - 1
            }
            AppendOutcome::TailConflict { .. } => false,
        };
        self.refresh_with_work(work).await?;
        if self.intent_matches_with_work(key, event, digest, work).await? != Some(true) {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "coordinator publication differs from its intent"));
        }
        self.validate_replay(event, work)?;
        Ok(appended)
    }

    fn next_committed_at_ms_with_work(
        &self, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<u64> {
        let after_previous = self
            .last_committed_at_ms
            .checked_add(1)
            .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Invalid, "coordinator commit time exhausted"))?;
        Ok(current_time_millis_with_work(work)?.max(after_previous))
    }

    /// Pulls and atomically admits one dependency- and resource-ready operation.
    pub async fn pull(&mut self, worker: &Worker) -> Result<Option<WorkLease>> {
        match self.pull_for_with_work(worker, None, None, &mut None).await? {
            WorkPull::Idle => Ok(None),
            WorkPull::Claimed(lease) => Ok(Some(lease)),
            WorkPull::Unresolved { error, .. } => Err(error),
        }
    }

    pub(crate) async fn pull_owned(
        &mut self, owner: &Authority, scope: &Scope, verifier: &AuthorityVerifier, worker: &Worker,
        operation_id: Option<OperationId>, work: &mut Option<&mut dyn HistoryIo>,
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
        &self, authorization: Option<(&Authority, &AuthorityVerifier)>, operation_id: OperationId,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if let Some((owner, verifier)) = authorization {
            self.scheduler
                .operation(operation_id)
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                "owned pull operation is absent"))?
                .spec
                .verify_owner(owner, verifier, work)?;
        }
        Ok(())
    }

    async fn pull_for_with_work(
        &mut self, worker: &Worker, authorization: Option<(&Authority, &AuthorityVerifier)>,
        target: Option<OperationId>, work: &mut Option<&mut dyn HistoryIo>,
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
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::NotFound,
                "owned pull target is absent"))?;
            self.verify_pull_owner_with_work(authorization, target, work)?;
            if state.reservation.is_some() || state.cancellation_requested {
                return Ok(WorkPull::Idle);
            }
        }
        let blocked = match target {
            Some(target) => self
                .scheduler
                .operation_blocked_by_dependencies(target, owner)
                .then_some(target),
            None => self.scheduler.blocked_by_dependencies(owner).next(),
        };
        if let Some(operation_id) = blocked {
            self.verify_pull_owner_with_work(authorization, operation_id, work)?;
            let mut custody = CodecQuota::new(work, 0)?;
            let key = formatted_text_with_work(
                "dependency-rejected:".len() + 36,
                format_args!("dependency-rejected:{operation_id}"), custody.work(),
            )?;
            if custody.work().is_some() { custody.adopt(key.capacity() as u64)?; }
            validate_idempotency_key(&key, custody.work())?;
            let key = IdempotencyKey(key);
            let reason = crate::core::copy_text("a required dependency did not succeed", &mut custody)?;
            self.apply_with_work(
                operation_id, key, SchedulerEvent::Rejected { operation_id, reason }, custody.work(),
            ).await?;
            return Ok(WorkPull::Idle);
        }
        let mut custody = CodecQuota::new(work, 0)?;
        let available = self.scheduler.available_for(&worker.id, &worker.available, custody.work())?;
        let available_bytes = if custody.work().is_some() { available.logical_owned_bytes()? } else { 0 };
        custody.adopt(available_bytes)?;
        let selected = match target {
            Some(target) => self
                .scheduler
                .operation_ready_for(target, &available, &worker.labels, owner)
                .then_some(target),
            None => self
                .scheduler
                .ready(&available, &worker.labels, owner).next(),
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
            .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::NotFound,
                "selected owned operation is absent"))?;
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
        let placement = crate::core::copy_text(&worker.id, &mut custody)?;
        let admitted = operation.resources.clone_with_work(custody.work())?;
        if custody.work().is_some() { custody.adopt(admitted.logical_owned_bytes()?)?; }
        let mut lease = WorkLease {
            operation,
            reservation: Reservation { id: reservation_id, placement, admitted },
            checkpoint,
            operation_revision,
        };
        let appended = {
            let mut attempt = CodecQuota::new(custody.work(), 0)?;
            let reservation = lease.reservation.clone_with_work(attempt.work())?;
            if attempt.work().is_some() { attempt.adopt(reservation.logical_owned_bytes()?)?; }
            let key = formatted_text_with_work(
                "pull:".len() + worker.id.len() + 1 + 36 + 1 + revision_digits,
                format_args!("pull:{}:{operation_id}:{revision}", worker.id), attempt.work(),
            )?;
            if attempt.work().is_some() { attempt.adopt(key.capacity() as u64)?; }
            validate_idempotency_key(&key, attempt.work())?;
            self.apply_with_work(
                operation_id, IdempotencyKey(key),
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
    ) -> Result<bool> {
        self.release_lease_with_work(lease, idempotency_key, &mut None).await
    }

    pub(crate) async fn release_lease_with_work(
        &mut self, lease: &WorkLease, idempotency_key: IdempotencyKey,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        let mut custody = CodecQuota::new(work, 0)?;
        let reservation_id = crate::core::copy_text(&lease.reservation.id, &mut custody)?;
        let placement = crate::core::copy_text(&lease.reservation.placement, &mut custody)?;
        self.apply_internal(
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
        let (outcome, cancel, reduction_digest) = match self.scheduler.orchestration(parent, &mut None)? {
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
                        &self.assemble_values_with_work(assembly, &values, &mut None).await.map(|(value, _)| value)?,
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
            }, None, None, None, None, None, &mut None
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
    ) -> Result<bool> {
        self.refresh().await?;
        if self
            .replay_orchestration(parent, idempotency_key.as_str(), true)
            .await?
        {
            return Ok(false);
        }
        let state = self
            .scheduler
            .operation(parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?;
        let expected_revision = state.revision;
        let OrchestrationDecision::Reduce { reducer, values } =
            self.scheduler.orchestration(parent, &mut None)?
        else {
            return Err(Error::Conflict("no reducer invocation is ready".into()));
        };
        let implementation = reducers.get(&reducer).ok_or_else(|| {
            Error::Unsupported("exact reducer implementation is not registered".into())
        })?;
        let mut decoded = Vec::with_capacity(values.len());
        for (slot, reference) in &values {
            decoded.push((slot.clone(), self.load_json_with_work(reference, &mut None).await.map(|(value, _)| value)?));
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
            }, None, None, None, None, None, &mut None
        )
        .await
    }

    async fn load_json_with_work(
        &self, reference: &FileRef, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(serde_json::Value, u64)> {
        reference.validate(work)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
                "scheduler result must be a JSON file"));
        }
        let mut custody = CodecQuota::new(work, 0)?;
        let (bytes, raw_bytes) = match custody.work().as_deref_mut() {
            Some(work) => crate::store::history::read_content_with_work(
                work, self.content_verifier.as_ref(), reference).await?,
            None => (self.content_verifier.read(reference).await?, 0),
        };
        custody.adopt(raw_bytes)?;
        reference.descriptor().verify(bytes.as_ref(), custody.work())?;
        let (value, value_bytes) = decode_json(bytes.as_ref(), crate::contract::JsonValueSchema::Root,
            crate::contract::json_value_logical_owned_bytes, custody.work())?;
        custody.adopt(value_bytes)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &value, reference.descriptor().byte_length(), custody.work(),
        )?;
        let canonical_bytes = if custody.work().is_some() { canonical.capacity() as u64 } else { 0 };
        custody.adopt(canonical_bytes)?;
        if canonical.as_slice() != bytes.as_ref() {
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

    async fn assemble_values_with_work(
        &self, assembly: AssemblyKind, values: &[(String, FileRef)],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(serde_json::Value, u64)> {
        let mut custody = CodecQuota::new(work, 0)?;
        let mut assembled = crate::core::reserve_copy_vector(values.len(), &mut custody)?;
        for (slot, reference) in values {
            let (value, value_bytes) = self.load_json_with_work(reference, custody.work()).await?;
            custody.adopt(value_bytes)?;
            let value = match assembly {
                AssemblyKind::Join => {
                    let copied_slot = crate::core::copy_text(slot, &mut custody)?;
                    let slot_key = crate::core::copy_text("slot", &mut custody)?;
                    let value_key = crate::core::copy_text("value", &mut custody)?;
                    let slots = (2 * std::mem::size_of::<(String, serde_json::Value)>()) as u64;
                    if let Some(work) = custody.work().as_deref_mut() {
                        work.admit_owned(slots)?;
                        work.retain_owned(slots)?;
                        custody.adopt(slots)?;
                    }
                    let mut fields = serde_json::Map::new();
                    fields.insert(slot_key, serde_json::Value::String(copied_slot));
                    fields.insert(value_key, value);
                    serde_json::Value::Object(fields)
                }
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
        let Some((_, event, ..)) = read_intent(&self.client, key, Some(self.revision), &mut None).await? else {
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
        reference.descriptor().verify(&bytes, &mut None)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid(
                "scheduler payload store returned a non-JSON file".into(),
            ));
        }
        Ok(reference)
    }

}

fn validate_idempotency_key(value: &str, work: &Option<&mut dyn HistoryIo>) -> Result<()> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(coordinator_boundary(work, wire::ErrorCode::Invalid, "idempotency key is invalid"));
    }
    Ok(())
}

fn formatted_text_with_work(
    length: usize, arguments: std::fmt::Arguments<'_>, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<String> {
    use std::fmt::Write as _;
    let mut custody = CodecQuota::new(work, 0)?;
    let mut text = String::new();
    if let Some(work) = custody.work().as_deref_mut() {
        work.reserve_text(&mut text, length)?;
        custody.adopt(text.capacity() as u64)?;
        custody.work().as_deref_mut().expect("format observer").copy_bytes(length as u64)?;
    } else {
        text.reserve_exact(length);
    }
    text.write_fmt(arguments).expect("primitive coordinator formatting");
    custody.transfer();
    Ok(text)
}

fn encode(
    revision: u64, operation_id: OperationId, key: &str, digest: [u8; 32], canonical: Vec<u8>,
    committed_at_ms: u64, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Vec<u8>> {
    let incoming_held = if work.is_some() { canonical.capacity() as u64 } else { 0 };
    let mut custody = CodecQuota::from_retained(work, incoming_held);
    let version = crate::core::copy_text(COORDINATOR_WIRE_VERSION, &mut custody)?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(COORDINATOR_WIRE_CONTRACT.len() as u64)?;
    }
    let hash = blake3::hash(COORDINATOR_WIRE_CONTRACT).to_hex();
    let descriptor_digest = crate::core::copy_text(hash.as_str(), &mut custody)?;
    let operation_id = formatted_text_with_work(36, format_args!("{operation_id}"), custody.work())?;
    if custody.work().is_some() { custody.adopt(operation_id.capacity() as u64)?; }
    let idempotency_key = crate::core::copy_text(key, &mut custody)?;
    let event_digest = crate::core::copy_copy_vector(&digest, &mut custody)?;
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
    encoded.map_err(|_| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "scheduler envelope encoding failed"))?;
    let retained_output = if custody.work().is_some() { bytes.capacity() as u64 } else { 0 };
    drop(envelope);
    let metadata_bytes = custody.retained() - retained_output;
    custody.relinquish(metadata_bytes)?;
    custody.transfer();
    Ok(bytes)
}

fn scheduler_json_owned_bytes(input_bytes: u64, items: u64) -> Result<u64> {
    use serde::__private229::de::Content;
    // Scheduler frames and item counts are bounded by MAX_RECORD_BYTES.
    const CONTENT_LAYERS: u64 = 3;
    let slots = items * (2 + 8);
    let content_bytes = slots * std::mem::size_of::<(Content<'static>, Content<'static>)>() as u64 * CONTENT_LAYERS;
    let typed_bytes = slots * std::mem::size_of::<(String, serde_json::Value)>() as u64;
    let strings = input_bytes * (CONTENT_LAYERS + 2);
    let scratch = input_bytes.max(8) * 2;
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
    let diagnostics = crate::contract::audited_serde_diagnostic_bytes(
        input_bytes, LABELS.len() as u64 * 5,
    )?;
    Ok(content_bytes + typed_bytes * 2 + strings + scratch + diagnostics
        + std::mem::size_of::<OperationSpec>() as u64)
}

fn decode_json<T: serde::de::DeserializeOwned>(
    input: &[u8], schema: crate::contract::JsonValueSchema, owned: fn(&T) -> Result<u64>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(T, u64)> {
    use crate::contract::{JsonCodecCharge, JsonValueSchema};
    let result_value = matches!(schema, JsonValueSchema::Root);
    let code = if result_value { wire::ErrorCode::Invalid } else { wire::ErrorCode::Storage };
    let invalid = |message: String| if result_value {
        Error::Invalid(format!("scheduler result is invalid JSON: {message}"))
    } else { Error::Storage(message) };
    if work.is_none() {
        return crate::contract::json_from_slice(input).map(|value| (value, 0))
            .map_err(|source| invalid(source.to_string()));
    }
    let native = work.as_deref().is_some_and(operations::original_native_decode_mode);
    let diagnostic = if result_value {
        crate::contract::audited_serde_diagnostic_bytes(input.len() as u64,
            "Value Number map sequence string number boolean null invalid type invalid value".len() as u64)?
    } else { 0 };
    let maximum = if result_value { (input.len() as u64).checked_add(diagnostic) } else { Some(0) };
    let maximum = maximum.ok_or_else(|| coordinator_boundary(work, code, "JSON admission overflows"))?;
    let bound = if result_value { input.len() as u64 } else { acyclic_stream::MAX_RECORD_BYTES as u64 };
    let mut reservation = 0;
    let mut observed_failure = false;
    let observer = work.as_deref_mut().expect("JSON observer");
    observer.check_cancellation()?;
    let decoded = operations::decode_preclaimed_json_with_mode::<T, _>(
        input, crate::contract::JsonBounds {
            maximum_bytes: bound, maximum_items: bound + 1, maximum_depth: 256,
        }, maximum, code, native, schema, &mut |charge| {
            let charged = (|| {
                let charge = match charge {
                    JsonCodecCharge::Admission { value_shape, .. } => {
                        let planned = if result_value {
                            crate::contract::json_value_decode_owned_bytes(value_shape)?.checked_add(diagnostic)
                        } else {
                            scheduler_json_owned_bytes(value_shape.input_bytes(), value_shape.lexical_items())?
                                .checked_add(crate::contract::json_value_virtual_owned_bytes(value_shape)?)
                        }.ok_or_else(|| coordinator_boundary(&Some(&mut *observer), wire::ErrorCode::Invalid, "JSON ownership admission overflows"))?;
                        observer.codec_charge(JsonCodecCharge::Admission {
                            maximum_owned_bytes: planned, value_shape,
                        })?;
                        reservation = planned;
                        return Ok(());
                    }
                    _ => charge,
                };
                observer.codec_charge(charge)
            })();
            observed_failure |= charged.is_err();
            charged
        },
    );
    let decoded = operations::retain_original_decode_error(decoded, &mut reservation, observer);
    let mut custody = CodecQuota::from_retained(work, reservation);
    let value = decoded.map_err(|error| match error {
        Error::Invalid(message) if !observed_failure => invalid(message),
        error => error,
    })?;
    custody.work().as_deref_mut().expect("JSON observer").check_cancellation()?;
    let retained = owned(&value)?;
    let slack = reservation.checked_sub(retained).ok_or_else(||
        coordinator_boundary(custody.work(), code, "JSON decoder exceeds its admission"))?;
    custody.relinquish(slack)?;
    custody.transfer();
    Ok((value, retained))
}

fn inspect_envelope<'a>(
    mut input: &'a [u8], protocol: bool, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(u64, u64, Option<&'a str>)> {
    use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
    let mut payload = 0u64;
    let mut buffers = 0u64;
    let mut key = None;
    while !input.is_empty() {
        if let Some(work) = work.as_deref_mut() {
            work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        let (tag, kind) = decode_key(&mut input).map_err(|_| coordinator_boundary(work, wire::ErrorCode::Storage,
                "invalid scheduler protobuf field"))?;
        let nested = !protocol && tag == 1;
        let owned = if protocol { matches!(tag, 1 | 2) } else { matches!(tag, 3 | 4 | 5 | 6) };
        if kind == WireType::LengthDelimited && (nested || owned) {
            let length = decode_varint(&mut input).map_err(|_| coordinator_boundary(work, wire::ErrorCode::Storage,
                "invalid scheduler protobuf length"))?;
            let length = usize::try_from(length).ok().filter(|length| *length <= input.len())
                .ok_or_else(|| coordinator_boundary(work, wire::ErrorCode::Storage,
                    "scheduler protobuf field exceeds its frame"))?;
            let (value, rest) = input.split_at(length);
            input = rest;
            if !protocol && tag == 4 {
                key = Some(std::str::from_utf8(value).map_err(|_| coordinator_boundary(
                    work, wire::ErrorCode::Storage, "scheduler retry key is not UTF-8"))?);
            }
            let (bytes, count, _) = if nested { inspect_envelope(value, true, work)? }
                else { (length as u64, 1, None) };
            // Disjoint fields contribute no more than their encoded frame.
            payload += bytes;
            buffers += count;
        } else {
            skip_field(kind, tag, &mut input, DecodeContext::default())
                .map_err(|_| coordinator_boundary(work, wire::ErrorCode::Storage,
                "invalid scheduler protobuf framing"))?;
        }
    }
    Ok((payload, buffers, key))
}

fn decode_with_work(
    record: &acyclic_stream::Record, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(u64, String, [u8; 32], u64, SchedulerEvent)> {
    let bytes = record.value.as_ref();
    let diagnostic = if work.is_some() {
        crate::contract::audited_serde_diagnostic_bytes(bytes.len() as u64, concat!(
            "SchedulerEventEnvelope ProtocolIdentity protocol revision operation_id idempotency_key ",
            "canonical_event_json event_digest committed_at_ms version descriptor_digest ",
            "failed to decode Protobuf message buffer underflow invalid varint invalid wire type ",
            "invalid tag value unexpected end group tag recursion limit reached invalid string value ",
        ).len() as u64)?
    } else { 0 };
    let mut custody = CodecQuota::new(work, diagnostic)?;
    let mut admitted = diagnostic;
    if custody.work().is_some() {
        let (payload, buffers, _) = inspect_envelope(bytes, false, custody.work())?;
        let fields = payload * 2 + buffers * 8;
        if let Some(work) = custody.work().as_deref_mut() {
            work.admit_owned(fields)?;
            work.retain_owned(fields)?;
        }
        custody.adopt(fields)?;
        admitted += fields;
    }
    let envelope = wire::SchedulerEventEnvelope::decode(bytes)
        .map_err(|_| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
                "invalid scheduler protobuf envelope"))?;
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
    let protocol = envelope.protocol.as_ref().ok_or_else(|| coordinator_boundary(
        custody.work(), wire::ErrorCode::Storage, "scheduler protocol identity is absent",
    ))?;
    let expected = blake3::hash(COORDINATOR_WIRE_CONTRACT).to_hex();
    if protocol.version != COORDINATOR_WIRE_VERSION || protocol.descriptor_digest != expected.as_str() {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "scheduler protocol identity differs"));
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
    let (event, _) = decode_json(&envelope.canonical_event_json, crate::contract::JsonValueSchema::SchedulerEvent,
        SchedulerEvent::logical_owned_bytes, custody.work())?;
    if custody.work().is_some() { custody.adopt(event.logical_owned_bytes()?)?; }
    let committed_at_ms = envelope.committed_at_ms.filter(|time| *time > 0)
        .ok_or_else(|| coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "coordinator event lacks commit time"))?;
    let operation_id = OperationId::from_bytes(*uuid::Uuid::parse_str(&envelope.operation_id)
        .map_err(|_| coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
            "scheduler operation id is not a UUID"))?.as_bytes());
    if envelope.revision != next_revision(record.sequence)? || event.operation_id() != operation_id {
        return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Storage,
            "coordinator record identity differs"));
    }
    let key_bytes = if custody.work().is_some() { envelope.idempotency_key.capacity() as u64 } else { 0 };
    let wire::SchedulerEventEnvelope { revision, idempotency_key, protocol, operation_id: text,
        canonical_event_json, event_digest, .. } = envelope;
    drop((protocol, text, canonical_event_json, event_digest));
    custody.relinquish(retained - key_bytes)?;
    custody.transfer();
    Ok((revision, idempotency_key, digest, committed_at_ms, event))
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

fn current_time_millis_with_work(work: &mut Option<&mut dyn HistoryIo>) -> Result<u64> {
    use acyclic_stream::UnixMillisClock as _;
    let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis();
    if now == 0 || now == u64::MAX {
        return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
            "coordinator clock exceeds supported range"));
    }
    Ok(now)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append<P: StreamProvider, const N: usize>(
    (stream, client): (&Stream<P>, &StreamClient<P>), input: [OwnedBytes; N],
    retained_request: u64, if_tail: Option<u64>, key: StreamIdempotencyKey,
    operation_id: OperationId, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<AppendOutcome> {
    let mut custody = CodecQuota::from_retained(work, retained_request);
    let mut records = Vec::new();
    reserve_prepared_vector(&mut records, N, &mut custody)?;
    records.extend(input);
    let retained_request = custody.retained();
    let count = records.len() as u64;
    let byte_length = records.iter().try_fold(0u64, |total, record| {
        total.checked_add(record.len() as u64).ok_or_else(|| {
            coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                "append request byte count exhausted")
        })
    })?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.admit_acknowledgment(byte_length, count)?;
    }
    let appended = async {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = custody.work().as_deref()
            .and_then(|observer| observer.original_native_work()).cloned()
        {
            let result = native_call_result(&original, client.provider.native_append(
                acyclic_stream::AppendRequest {
                    path: stream.path().clone(), records, if_tail, idempotency_key: Some(key.clone()),
                }, original.runtime_meter_owner(),
            ).await)?;
            return Ok(result.map(|output| output.into_parts().0));
        }
        if let Some(observer) = custody.work().as_deref_mut() { observer.begin_write()?; }
        Ok::<_, Error>(stream.append_batch(records, if_tail, Some(key.clone())).await.map_err(SdkCallError::Ok))
    }.await?;
    if custody.work().is_some() {
        let retired = custody.relinquish(retained_request);
        if appended.is_ok()
            || !custody.work().as_deref().is_some_and(operations::original_native_decode_mode)
        { retired?; }
    }
    match appended {
        Ok(outcome) => {
            if let Some(work) = custody.work().as_deref_mut()
                && let AppendOutcome::Committed(receipt) = &outcome
                && receipt.end.checked_sub(receipt.start) == Some(count)
                && receipt.tail >= receipt.end
            {
                work.acknowledged(byte_length, count)?;
            }
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            Ok(outcome)
        }
        Err(error) if *sdk_source(&error) == StreamError::Unavailable => {
            let mut received = CodecQuota::from_retained(custody.work(), 0);
            match inspect_idempotency(client, key, received.work(), None, None).await? {
                Ok(Some((observation, retained))) => {
                    received.adopt(retained)?;
                    match observation.outcome {
                        IdempotencyOutcome::Append(outcome) => {
                            if let Some(work) = received.work().as_deref_mut() { work.check_cancellation()?; }
                            Ok(outcome)
                        }
                        _ => Err(coordinator_boundary(received.work(), wire::ErrorCode::Conflict,
                            "append recovery identity has another operation kind")),
                    }
                }
                Ok(None) | Err(_) => Err(Error::Indeterminate(operation_id)),
            }
        }
        Err(error) => {
            if *sdk_source(&error) == StreamError::IdempotencyMismatch
                && !custody.work().as_deref().is_some_and(operations::original_native_decode_mode)
            {
                Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict, "append retry identity was reused"))
            } else { Err(sdk_error(error, custody.work())) }
        }
    }
}

pub(crate) fn execution_path(
    operation: OperationId, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    let mut custody = CodecQuota::new(work, 0)?;
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

/// The original SDK request and its producer-owned logical custody.
pub(crate) struct PreparedCommitRequest<'a, 'b> {
    request: acyclic_stream::CommitRequest,
    custody: CodecQuota<'a, 'b>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
type PreparedNativeDispatch = (acyclic_stream::PreparedNativeCommit, acyclic_stream::OwnedBytes);
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type PreparedNativeDispatch = std::convert::Infallible;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
type PreparedNativeRecovery = acyclic_stream::NativeProviderFuture<'static, Option<acyclic_stream::IdempotencyObservation>>;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type PreparedNativeRecovery = std::convert::Infallible;

/// One Store-issued completion accepted before its physical action.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) struct PreparedMaterializationCommit {
    template: crate::filesystem::NativePreparationCompletionTemplate,
    sdk: acyclic_stream::PreparedNativeCommit,
    sdk_recovery: PreparedNativeRecovery,
    sdk_custody: acyclic_fs::NativeAllocation,
    observer: crate::store::history::AcceptedCompletionIo,
}

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl PreparedMaterializationCommit {
    pub(crate) async fn prepare<P: StreamProvider>(
        owner: &crate::durable_host::TaskJournalOwner<P>,
        mut template: crate::filesystem::NativePreparationCompletionTemplate,
        reservation: acyclic_fs::NativeWorkReservation,
        original: &acyclic_fs::NativeMaterializationWork, work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        use acyclic_stream::{CommitCondition, CommitConflict, CommittedMutation, Record};
        use std::mem::size_of;
        let original_observer = work.as_deref().and_then(|io| io.original_native_work());
        if !original_observer.is_some_and(|native| native.shares_allocation_context(original))
            || !template.matches_work(original) || !reservation.matches_work(original)
        {
            return Err(Error::NativeBoundary {
                code: wire::ErrorCode::Unauthorized,
                reason: "native completion differs from its original observer and custody",
            });
        }
        let (_, output, maximum_bytes) = template.codec_parts();
        if maximum_bytes == 0 || maximum_bytes > acyclic_stream::MAX_RECORD_BYTES as u64
            || (output.len() as u64) < maximum_bytes
        {
            return Err(Error::NativeBoundary {
                code: wire::ErrorCode::Storage,
                reason: "native completion lacks its fixed prepaid output",
            });
        }
        acyclic_fs::WorkCounters {
            backend_read_operations: 1,
            backend_write_operations: 1,
            authority_records_read: 1,
            authority_records_appended: 1,
            authority_bytes_read: maximum_bytes,
            authority_bytes_written: maximum_bytes,
            bytes_encoded: maximum_bytes,
            bytes_copied: maximum_bytes,
            items_examined: 1,
            ..acyclic_fs::WorkCounters::default()
        }.verify(reservation.remaining_work()).map_err(|cause| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(cause), custody: None,
        })?;
        template.verify_owner(owner, original)?;
        let client = owner.stream();
        let owner_condition = owner.condition(JournalWrite::Fresh, work).await?;
        let bounds = operations::bounds_with_work(&client, template.path(), work).await?
            .map_err(|error| sdk_error(error, work))?;
        if bounds.tail != template.expected_revision() {
            return Err(coordinator_boundary(work, wire::ErrorCode::Conflict,
                "native completion Preparing record changed before acceptance"));
        }
        let target = CommitCondition::Tail {
            path: template.path().clone(), expected: template.expected_revision(),
        };
        let owner_path_bytes = match &owner_condition {
            CommitCondition::Tail { path, .. } => path.as_str().len() as u64,
            CommitCondition::Absent { .. } => return Err(Error::NativeBoundary {
                code: wire::ErrorCode::Unauthorized,
                reason: "native completion requires its original owner tail",
            }),
        };
        let path_bytes = template.path().as_str().len() as u64;
        let key_bytes = template.key().as_bytes().len() as u64;
        // SDK paths, retry keys and the completion record are bounded before this arithmetic.
        let append_metadata = (4 * size_of::<CommittedMutation>() + 4 * size_of::<Record>()) as u64;
        let conflict_metadata = (4 * size_of::<CommitConflict>()) as u64;
        let append_source = append_metadata + path_bytes + maximum_bytes;
        let conflict_source = conflict_metadata + owner_path_bytes + path_bytes;
        let pool_bytes = (append_source.max(conflict_source) + key_bytes
            + size_of::<acyclic_stream::IdempotencyObservation>() as u64
            + size_of::<Error>() as u64 + crate::contract::sdk_json_diagnostic_bytes()
            + size_of::<Self>() as u64)
            .max(size_of::<(acyclic_fs::MaterializationJournal, acyclic_fs::NativeAllocation)>() as u64);
        let mut prepared = PreparedCommitRequest::new(template.key().clone(), 0, work);
        reserve_prepared_vector(
            &mut prepared.request.conditions, 2, &mut prepared.custody,
        )?;
        prepared.request.conditions.push(owner_condition);
        prepared.request.conditions.push(target);
        prepared.append(template.path().clone(), acyclic_stream::OwnedBytes::new())?;
        if let Some(io) = prepared.work().as_deref_mut() {
            io.admit_owned(pool_bytes)?;
            io.retain_owned(pool_bytes)?;
        }
        prepared.custody.adopt(pool_bytes)?;
        let retained = prepared.custody.retained();
        let custody = prepared.work().as_deref_mut().expect("checked native observer")
            .take_native_custody(retained)?;
        prepared.custody.transfer();
        let PreparedCommitRequest { request, custody: quota } = prepared;
        drop(quota);
        let observer = match crate::store::history::AcceptedCompletionIo::new(original, reservation, custody) {
            Ok(observer) => observer,
            Err(error) => {
                drop((request, template));
                return Err(error);
            }
        };
        let maximum = template.codec_parts().2 as usize;
        let prepared = async {
            let prepared = native_call_result(original,
                client.native_prepare_commit(request, maximum, original.runtime_meter_owner()).await)?
                .map_err(|error| sdk_error(error, &None))?;
            let (mut prepared, incoming) = prepared.into_parts();
            let sdk_custody = match acyclic_fs::NativeAllocation::from_runtime_allocation(original, incoming) {
                Ok(incoming) => incoming,
                Err((cause, incoming)) => {
                    drop(prepared);
                    drop(incoming);
                    return Err(Error::NativeWork { cause, custody: None });
                }
            };
            let Some(recovery) = prepared.take_recovery_future() else {
                drop(prepared);
                return Err(Error::NativeBoundary { code: wire::ErrorCode::Invalid,
                    reason: "native completion lacks its pre-action same-key recovery" });
            };
            Ok((prepared, recovery, sdk_custody))
        }.await;
        match prepared {
            Ok((sdk, sdk_recovery, sdk_custody)) =>
                Ok(Self { template, sdk, sdk_recovery, sdk_custody, observer }),
            Err(error) => {
                drop(template);
                Err(completion_error(error, observer.into_custody()))
            }
        }
    }

    /// Returns the funded completion work.
    pub(crate) fn completion_work(&self) -> Result<acyclic_fs::WorkCounters> {
        self.observer.completion_work()
    }

    pub(crate) async fn finish<P: StreamProvider>(
        self, client: &StreamClient<P>, counters: acyclic_fs::WorkCounters,
        mut phase: Option<&mut CommitPhase>,
    ) -> Result<(acyclic_fs::MaterializationJournal, acyclic_fs::NativeAllocation)> {
        if let Some(phase) = phase.as_deref_mut() { *phase = CommitPhase::NotCalled; }
        let Self { mut template, sdk, sdk_recovery, mut sdk_custody, mut observer } = self;
        let operation = OperationId::from_bytes(template.task_binding().0.into_bytes());
        let finished = async {
            let key = template.key().clone();
            template.set_completed_work(counters)?;
            let (value, output, maximum_bytes) = template.codec_parts();
            let written = crate::contract::canonical_json_value_into_prepaid_native_trusted(
                value, output, maximum_bytes, wire::ErrorCode::Storage,
                &mut |charge| observer.codec_charge(charge),
            )?;
            let (journal, output, mut custody) = template.into_completed_parts();
            if let Err(cause) = sdk_custody.absorb(&mut custody) {
                drop(output);
                return Err(Error::NativeWork { cause, custody: Some(custody) });
            }
            let output = output.freeze(written).map_err(|output| {
                drop(output);
                Error::NativeBoundary { code: wire::ErrorCode::Storage,
                    reason: "native completion exceeds its original initialized output" }
            })?;
            let (outcome, received) = {
                let mut optional = Some(&mut observer as &mut dyn HistoryIo);
                let prepared = PreparedCommitRequest::new(key, 0, &mut optional);
                prepared.commit(client, Some(operation), phase,
                    Some((sdk, output)), Some(sdk_recovery)).await?
            };
            let committed = matches!(&outcome, acyclic_stream::CommitOutcome::Committed(_));
            drop(outcome);
            observer.release_owned(received)?;
            if !committed {
                return Err(Error::NativeBoundary { code: wire::ErrorCode::Conflict,
                    reason: "native materialization completion changed before its fenced commit" });
            }
            Ok(journal)
        }.await;
        drop(sdk_custody);
        let mut custody = observer.into_custody();
        let journal = match finished {
            Ok(journal) => journal,
            Err(error) => return Err(completion_error(error, custody)),
        };
        let output_bytes = std::mem::size_of::<(
            acyclic_fs::MaterializationJournal, acyclic_fs::NativeAllocation,
        )>() as u64;
        if let Err(cause) = custody.substitute(output_bytes) {
            drop(journal);
            return Err(Error::NativeWork { cause, custody: Some(custody) });
        }
        Ok((journal, custody))
    }
}

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
fn completion_error(error: Error, custody: acyclic_fs::NativeAllocation) -> Error {
    match error {
        Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(custody) },
        Error::NativeEncode { code, source, custody: None } => Error::NativeEncode { code, source, custody: Some(custody) },
        Error::NativeStream { code, source, custody: None } => Error::NativeStream { code, source, custody: Some(custody) },
        error => { drop(custody); error }
    }
}

fn retain_received_owned(
    bytes: u64, custody: &mut CodecQuota<'_, '_>, failure: &mut Option<Error>,
) -> Result<()> {
    let Some(work) = custody.work().as_deref_mut() else { return Ok(()); };
    if operations::original_native_decode_mode(work) { return Ok(()); }
    match work.retain_owned(bytes) {
        Ok(()) => custody.adopt(bytes)?,
        Err(error) => { if failure.is_none() { *failure = Some(error); } }
    }
    Ok(())
}

fn observe_commit_with_work(
    outcome: &acyclic_stream::CommitOutcome, direct_acknowledgment: bool,
    custody: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    use acyclic_stream::{CommitConflict, CommitOutcome, CommittedMutation};
    if custody.work().is_none() { return Ok(()); }
    let native = custody.work().as_deref().is_some_and(operations::original_native_decode_mode);
    let mut failure = None;
    match outcome {
        CommitOutcome::Committed(envelope) => {
            retain_received_owned((envelope.mutations.capacity()
                * std::mem::size_of::<CommittedMutation>()) as u64, custody, &mut failure)?;
            for mutation in &envelope.mutations {
                let records = match mutation {
                    CommittedMutation::Append(append) => {
                        retain_received_owned(append.path.as_str().len() as u64, custody, &mut failure)?;
                        &append.records
                    }
                    CommittedMutation::Fork(fork) => {
                        retain_received_owned(fork.source.as_str().len() as u64, custody, &mut failure)?;
                        retain_received_owned(fork.destination.as_str().len() as u64, custody, &mut failure)?;
                        &fork.records
                    }
                };
                retain_received_owned((records.capacity()
                    * std::mem::size_of::<acyclic_stream::Record>()) as u64, custody, &mut failure)?;
                for record in records {
                    let bytes = record.value.len() as u64;
                    let observer = custody.work().as_deref_mut().expect("observed envelope");
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    let received = if native { observer.receive_native_record(bytes) }
                        else { observer.receive_record(bytes) };
                    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                    let received = observer.receive_record(bytes);
                    match received {
                        Ok(()) if !native => custody.adopt(bytes)?,
                        Ok(()) => {},
                        Err(error) => { if failure.is_none() { failure = Some(error); } }
                    }
                    if direct_acknowledgment {
                        let acknowledged = custody.work().as_deref_mut().expect("observed envelope")
                            .acknowledged(bytes, 1);
                        if let Err(error) = acknowledged
                            && failure.is_none()
                        { failure = Some(error); }
                    }
                }
            }
        }
        CommitOutcome::Conflict(conflicts) => {
            retain_received_owned((conflicts.capacity()
                * std::mem::size_of::<CommitConflict>()) as u64, custody, &mut failure)?;
            for conflict in conflicts {
                let (CommitConflict::Tail { path, .. } | CommitConflict::Exists { path }) = conflict;
                retain_received_owned(path.as_str().len() as u64, custody, &mut failure)?;
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

fn observe_recovery_with_work(
    observation: &acyclic_stream::IdempotencyObservation, custody: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    let mut failure = None;
    retain_received_owned(observation.idempotency_key.as_bytes().len() as u64, custody, &mut failure)?;
    match &observation.outcome {
        IdempotencyOutcome::Commit(outcome) => {
            if let Err(error) = observe_commit_with_work(outcome, false, custody) {
                failure.get_or_insert(error);
            }
        }
        IdempotencyOutcome::Fork(receipt) => {
            retain_received_owned(receipt.source.as_str().len() as u64, custody, &mut failure)?;
            retain_received_owned(receipt.destination.as_str().len() as u64, custody, &mut failure)?;
        }
        IdempotencyOutcome::Append(_) => {}
    }
    failure.map_or(Ok(()), Err)
}

async fn inspect_idempotency<P: StreamProvider>(
    client: &StreamClient<P>, key: StreamIdempotencyKey,
    work: &mut Option<&mut dyn HistoryIo>, phase: Option<&mut CommitPhase>,
    recovery: Option<PreparedNativeRecovery>,
) -> Result<std::result::Result<Option<(acyclic_stream::IdempotencyObservation, u64)>, SdkCallError>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if recovery.is_none() && work.as_deref().is_some_and(|observer| observer.requires_prepared_native_dispatch()) {
        return Err(coordinator_boundary(work, wire::ErrorCode::Invalid,
            "accepted completion requires its pre-action SDK recovery"));
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = recovery;
    let mut custody = CodecQuota::from_retained(work, 0);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = custody.work().as_deref()
        .and_then(|observer| observer.original_native_work()).cloned()
    {
        let mut phase = phase;
        let inspected = native_call_result(&original, match recovery {
            Some(recovery) => {
                drop(key);
                recovery.await
            }
            None => client.provider.native_inspect_idempotency(key, original.runtime_meter_owner()).await,
        })?;
        let output = match inspected {
            Ok(output) => output,
            Err(error) => return Ok(Err(error)),
        };
        if let (Some(phase), Some(observation)) = (phase.as_deref_mut(), output.value())
            && let IdempotencyOutcome::Commit(outcome) = &observation.outcome
        {
            *phase = match outcome {
                acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
                acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Conflict,
            };
        }
        if let Some(observation) = output.value() {
            observe_recovery_with_work(observation, &mut custody)?;
        }
        let (observation, retained) = crate::store::history::adopt_native_sdk_output(
            &original, output, custody.work(),
        )?;
        custody.adopt(retained)?;
        let Some(observation) = observation else { return Ok(Ok(None)); };
        custody.work().as_deref_mut().expect("original native observer").check_cancellation()?;
        let retained = custody.retained();
        custody.transfer();
        return Ok(Ok(Some((observation, retained))));
    }
    if let Some(work) = custody.work().as_deref_mut() { work.begin_call()?; }
    let inspected = client.provider.inspect_idempotency(key).await;
    match inspected {
        Err(source) => Ok::<_, Error>(Err(SdkCallError::Ok(source))),
        Ok(None) => Ok(Ok(None)),
        Ok(Some(observation)) => {
            if let (Some(phase), IdempotencyOutcome::Commit(outcome)) = (phase, &observation.outcome) {
                *phase = match outcome {
                    acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
                    acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Conflict,
                };
            }
            observe_recovery_with_work(&observation, &mut custody)?;
            if let Some(work) = custody.work().as_deref_mut() { work.check_cancellation()?; }
            let retained = custody.retained();
            custody.transfer();
            Ok(Ok(Some((observation, retained))))
        }
    }
}

fn reserve_prepared_vector<T>(
    values: &mut Vec<T>, capacity: usize, custody: &mut CodecQuota<'_, '_>,
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
    let actual = ((values.capacity() - old) * std::mem::size_of::<T>()) as u64;
    if let Some(work) = custody.work().as_deref_mut() {
        work.retain_owned(actual)?;
        custody.adopt(actual)?;
    }
    Ok(())
}

fn commit_vector_bytes(request: &acyclic_stream::CommitRequest) -> u64 {
    (request.conditions.capacity() * std::mem::size_of::<acyclic_stream::CommitCondition>()
        + request.mutations.capacity() * std::mem::size_of::<acyclic_stream::CommitMutation>()
        + request.mutations.iter().map(|mutation| {
            let (acyclic_stream::CommitMutation::Append { records, .. }
                | acyclic_stream::CommitMutation::Fork { records, .. }) = mutation;
            records.capacity() * std::mem::size_of::<OwnedBytes>()
        }).sum::<usize>()) as u64
}

impl<'a, 'b> PreparedCommitRequest<'a, 'b> {

    pub(crate) fn new(
        key: StreamIdempotencyKey, retained_key_bytes: u64,
        work: &'a mut Option<&'b mut dyn HistoryIo>,
    ) -> Self {
        Self {
            request: acyclic_stream::CommitRequest {
                conditions: Vec::new(), mutations: Vec::new(), idempotency_key: key,
            },
            custody: CodecQuota::from_retained(work, retained_key_bytes),
        }
    }

    pub(crate) fn work(&mut self) -> &mut Option<&'b mut dyn HistoryIo> {
        self.custody.work()
    }

    pub(crate) fn push_condition(&mut self, condition: acyclic_stream::CommitCondition) -> Result<()> {
        let length = self.request.conditions.len() + 1;
        reserve_prepared_vector(&mut self.request.conditions, length, &mut self.custody)?;
        self.request.conditions.push(condition);
        Ok(())
    }

    pub(crate) fn append(&mut self, path: acyclic_stream::StreamPath, record: OwnedBytes) -> Result<()> {
        let length = self.request.mutations.len() + 1;
        reserve_prepared_vector(&mut self.request.mutations, length, &mut self.custody)?;
        let mut records = Vec::new();
        reserve_prepared_vector(&mut records, 1, &mut self.custody)?;
        records.push(record);
        self.request.mutations.push(acyclic_stream::CommitMutation::Append { path, records });
        Ok(())
    }

    pub(crate) async fn commit<P: StreamProvider>(
        self, client: &StreamClient<P>, operation: Option<OperationId>,
        mut phase: Option<&mut CommitPhase>, dispatch: Option<PreparedNativeDispatch>,
        recovery: Option<PreparedNativeRecovery>,
    ) -> Result<(acyclic_stream::CommitOutcome, u64)> {
        let Self { mut custody, request } = self;
        if let Some(phase) = phase.as_deref_mut() { *phase = CommitPhase::NotCalled; }
        let key = request.idempotency_key.clone();
        let committed = async {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if dispatch.is_none() && custody.work().as_deref().is_some_and(|io| io.requires_prepared_native_dispatch()) {
                return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Invalid,
                    "accepted completion requires its pre-action SDK dispatch"));
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let _ = dispatch;
            if custody.work().is_some() {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let late_record = dispatch.as_ref().map(|(_, record)| record.len() as u64);
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let late_record: Option<u64> = None;
                let (bytes, count) = if let Some(bytes) = late_record { (bytes, 1) } else {
                    request.mutations.iter().flat_map(|mutation| match mutation {
                        acyclic_stream::CommitMutation::Append { records, .. }
                        | acyclic_stream::CommitMutation::Fork { records, .. } => records,
                    }).try_fold((0u64, 0u64), |(bytes, count), record| {
                        let bytes = bytes.checked_add(record.len() as u64)
                            .ok_or_else(|| coordinator_boundary(&None, wire::ErrorCode::Invalid,
                                "commit payload byte count overflows"))?;
                        let count = count.checked_add(1)
                            .ok_or_else(|| coordinator_boundary(&None, wire::ErrorCode::Invalid,
                                "commit record count overflows"))?;
                        Ok::<_, Error>((bytes, count))
                    })?
                };
                if let Some(work) = custody.work().as_deref_mut() {
                    work.admit_acknowledgment(bytes, count)?;
                    if !operations::original_native_decode_mode(work) { work.begin_write()?; }
                }
            }
            let vectors = commit_vector_bytes(&request);
            if let Some(phase) = phase.as_deref_mut() { *phase = CommitPhase::Unknown; }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(original) = custody.work().as_deref()
                .and_then(|observer| observer.original_native_work()).cloned()
            {
                let committed = match dispatch {
                    Some((prepared, record)) => {
                        drop(request);
                        prepared.finish(record).await
                    }
                    None => client.provider.native_commit(request, original.runtime_meter_owner()).await,
                };
                if let Some(phase) = phase.as_deref_mut() { *phase = match &committed {
                    Ok(Ok(output)) => match output.value() {
                        acyclic_stream::CommitOutcome::Committed(_) => CommitPhase::Committed,
                        acyclic_stream::CommitOutcome::Conflict(_) => CommitPhase::Conflict,
                    },
                    Ok(Err(error)) if *error.source() == StreamError::Unavailable => CommitPhase::Unknown,
                    Ok(Err(_)) => CommitPhase::Failed,
                    Err(_) => CommitPhase::Unknown,
                }; }
                let released = custody.relinquish(vectors);
                let output = match native_call_result(&original, committed)? {
                    Ok(output) => output,
                    Err(error) => return Ok(Err(error)),
                };
                let observed = observe_commit_with_work(output.value(), true, &mut custody);
                released.and(observed)?;
                let mut received = CodecQuota::from_retained(custody.work(), 0);
                let (outcome, retained) = crate::store::history::adopt_native_sdk_output(
                    &original, output, received.work(),
                )?;
                received.adopt(retained)?;
                received.work().as_deref_mut().expect("original native observer").check_cancellation()?;
                let retained = received.retained();
                received.transfer();
                return Ok(Ok((outcome, retained)));
            }
            let committed = client.provider.commit(request).await;
            if let Some(phase) = phase.as_deref_mut() { *phase = match &committed {
                Ok(acyclic_stream::CommitOutcome::Committed(_)) => CommitPhase::Committed,
                Ok(acyclic_stream::CommitOutcome::Conflict(_)) => CommitPhase::Conflict,
                Err(StreamError::Unavailable) => CommitPhase::Unknown,
                Err(_) => CommitPhase::Failed,
            }; }
            if custody.work().is_some() { custody.relinquish(vectors)?; }
            let mut received = CodecQuota::from_retained(custody.work(), 0);
            match committed {
                Err(source) => Ok::<_, Error>(Err(SdkCallError::Ok(source))),
                Ok(outcome) => {
                    observe_commit_with_work(&outcome, true, &mut received)?;
                    if let Some(work) = received.work().as_deref_mut() { work.check_cancellation()?; }
                    let retained = received.retained();
                    received.transfer();
                    Ok(Ok((outcome, retained)))
                }
            }
        }.await?;
        let mut received = CodecQuota::from_retained(custody.work(), 0);
        let outcome = match committed {
            Ok((outcome, retained)) => {
                received.adopt(retained)?;
                outcome
            }
            Err(error) if operation.is_some() && *sdk_source(&error) == StreamError::Unavailable => {
                match inspect_idempotency(
                    client, key, received.work(), phase.as_deref_mut(), recovery,
                ).await? {
                    Ok(Some((observation, retained))) => {
                        received.adopt(retained)?;
                        let metadata = if received.work().is_some()
                            && !received.work().as_deref().is_some_and(operations::original_native_decode_mode)
                        { observation.idempotency_key.as_bytes().len() as u64 } else { 0 };
                        let acyclic_stream::IdempotencyObservation { idempotency_key, outcome, .. } = observation;
                        drop(idempotency_key);
                        received.relinquish(metadata)?;
                        match outcome {
                            IdempotencyOutcome::Commit(outcome) => outcome,
                            _ => return Err(coordinator_boundary(received.work(), wire::ErrorCode::Conflict,
                                "commit recovery identity reused")),
                        }
                    }
                    Ok(None) | Err(_) => return Err(Error::Indeterminate(operation.expect("recovery operation"))),
                }
            }
            Err(error) => return Err(sdk_error(error, received.work())),
        };
        if let Some(work) = received.work().as_deref_mut() { work.check_cancellation()?; }
        let retained = received.retained();
        received.transfer();
        Ok((outcome, retained))
    }
}

fn path_with_work(
    value: &str, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<acyclic_stream::StreamPath> {
    if let Some(work) = work.as_deref_mut() {
        work.admit_allocation(value.len() as u64)?;
        work.copy_bytes(value.len() as u64)?;
    }
    let path = acyclic_stream::StreamPath::new(value)
        .map_err(|error| operations::stream_error_with_work(error, work))?;
    if let Some(work) = work.as_deref_mut() { work.retain_owned(value.len() as u64)?; }
    Ok(path)
}

#[allow(clippy::too_many_arguments)]
async fn append_if_execution_idle_with_work<P: StreamProvider>(
    client: &StreamClient<P>, stream: &Stream<P>, record: OwnedBytes, tail: u64,
    key: StreamIdempotencyKey, operation: OperationId, execution: OperationId, expected_tail: u64,
    workflow_guard: Option<(&str, u64)>, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<AppendOutcome> {
    use acyclic_stream::{CommitCondition, CommitOutcome, CommittedMutation};
    let mut custody = CodecQuota::new(work, 0)?;
    let path = execution_path(execution, custody.work())?;
    if custody.work().is_some() { custody.adopt(path.as_str().len() as u64)?; }
    let bounds = {
        let mut source_custody = CodecQuota::new(custody.work(), 0)?;
        if let Some(io) = source_custody.work().as_deref_mut()
            && !operations::original_native_decode_mode(io)
        {
            let bytes = path.as_str().len() as u64;
            io.admit_allocation(bytes)?;
            io.copy_bytes(bytes)?;
            io.retain_owned(bytes)?;
            source_custody.adopt(bytes)?;
        }
        operations::bounds_with_work(client, &path, source_custody.work()).await?
    };
    let guard = match bounds {
        Ok(bounds) if bounds.tail == expected_tail => CommitCondition::Tail { path, expected: expected_tail },
        Ok(_) => return Err(coordinator_boundary(custody.work(), wire::ErrorCode::Conflict,
            "execution changed before suspension")),
        Err(error) if *sdk_source(&error) == StreamError::NotFound && expected_tail == 0 =>
            CommitCondition::Absent { path },
        Err(error) => return Err(sdk_error(error, custody.work())),
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
    let (key, held) = digest_key(digest.finalize().as_bytes(), custody.work())?;
    let mut request = PreparedCommitRequest::new(key, held, custody.work());
    request.push_condition(CommitCondition::Tail { path: stream.path().clone(), expected: tail })?;
    request.push_condition(guard)?;
    if let Some(condition) = workflow_condition { request.push_condition(condition)?; }
    request.append(stream.path().clone(), record.clone())?;
    let (outcome, outcome_bytes) = request.commit(client, Some(operation), None, None, None).await?;
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
    key: &str, revision: u64, parent: bool, waiting: bool,
    idle_execution: Option<(OperationId, u64)>, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamIdempotencyKey, u64)> {
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

fn stream_key_with_work(
    key: &str, work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamIdempotencyKey, u64)> {
    if let Some(work) = work.as_deref_mut() {
        work.hashed_bytes((b"acyclic-harness-coordinator-v3".len() + key.len()) as u64)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-coordinator-v3");
    hasher.update(key.as_bytes());
    digest_key(hasher.finalize().as_bytes(), work)
}

pub(crate) fn digest_key(
    digest: &[u8; 32], work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(StreamIdempotencyKey, u64)> {
    let mut custody = CodecQuota::new(work, 0)?;
    let bytes = crate::core::copy_copy_vector(digest, &mut custody)?;
    custody.transfer();
    let (bytes, retained) = operations::owned_bytes_from_retained_vec(bytes, custody.work())?;
    Ok((StreamIdempotencyKey::new(bytes).expect("32-byte digest is a bounded SDK key"), retained))
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
            allocation_operations: 4096,
            peak_allocation_bytes: acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 1_048_576,
            backend_write_operations: 8,
            backend_read_operations: 8,
            authority_records_appended: 8,
            authority_bytes_written: 1024,
            authority_records_read: 8,
            authority_bytes_read: 1024,
            items_examined: 4096,
            source_bytes_read: 1_048_576,
            output_bytes: 1_048_576,
            bytes_copied: 1_048_576,
            bytes_hashed: 1_048_576,
            bytes_encoded: 1_048_576,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn keyed_append_native_keeps_exact_original_sdk_mismatch_source() {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let stream = client.stream("keyed-append-native-mismatch").expect("source stream");
        let key = StreamIdempotencyKey::new(OwnedBytes::from_static(b"mismatch-key")).expect("key");
        let operation = OperationId::from_bytes([15; 16]);
        append((&stream, &client), [OwnedBytes::from_static(b"original")], 0, Some(0),
            key.clone(), operation, &mut None).await.expect("original append");
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(keyed_append_native_budget(), &cancellation)
            .expect("original ledger");
        let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
        let error = append((&stream, &client), [OwnedBytes::from_static(b"changed")],
            0, Some(0), key.clone(), operation, &mut Some(&mut observer))
            .await.expect_err("SDK mismatch");
        assert!(matches!(&error, Error::NativeStream {
            code: wire::ErrorCode::Conflict, source: StreamError::IdempotencyMismatch,
            custody: Some(custody),
        } if custody.bytes() == 0));
        assert_eq!(observer.into_custody().bytes(), 0);
        assert!(matches!(append((&stream, &client), [OwnedBytes::from_static(b"changed")],
            0, Some(0), key, operation, &mut None).await, Err(Error::Conflict(_))));
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_refresh_keeps_original_serde_error_and_raw_record_prefix() {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier))
            .await.expect("original empty coordinator");
        let raw = OwnedBytes::from(encode(1, OperationId::from_bytes([16; 16]), "native-invalid-schema", *blake3::hash(b"{}").as_bytes(), b"{}".to_vec(), 1, &mut None).expect("original envelope producer"));
        let raw_length = raw.len() as u64;
        coordinator.stream.stream.append_batch(vec![raw], Some(0), None)
            .await.expect("original physical envelope");
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
        assert_eq!(coordinator.revision(), 0);
        assert_eq!(observer.into_custody().bytes(), 0, "record and envelope die before observer release");
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_publication_phase_survives_receiver_denial_and_original_ack_recovery() {
        use std::sync::atomic::Ordering;
        for (lost_ack, denied, expected_phase) in [
            (true, "receiver", CommitPhase::Committed),
            (true, "inspection", CommitPhase::Unknown),
            (true, "none", CommitPhase::Committed),
            (false, "write", CommitPhase::Unknown),
        ] {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
            let path = acyclic_stream::StreamPath::new("native-publication-disposition")
                .expect("original publication path");
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget = keyed_append_native_budget();
            let expected_tail = u64::from(denied != "write");
            match denied {
                "receiver" => budget.authority_records_read = 0,
                "inspection" => budget.backend_read_operations = 0,
                "write" => budget.backend_write_operations = 0,
                _ => {},
            }
            let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .expect("original operation ledger");
            let mut io = crate::store::history::native_work_io(&native).expect("original observer");
            let mut work = Some(&mut io as &mut dyn HistoryIo);
            let (key, retained) = stream_key_with_work("native-phase-key", &mut work).expect("original key");
            let mut prepared = PreparedCommitRequest::new(key, retained, &mut work);
            prepared.push_condition(acyclic_stream::CommitCondition::Tail {
                path: path.clone(), expected: 0,
            }).expect("original guarded condition");
            prepared.append(path.clone(), OwnedBytes::from_static(b"original"))
                .expect("original publication payload");
            provider.native_only.store(true, Ordering::SeqCst);
            provider.lose_ack.store(lost_ack, Ordering::SeqCst);
            let mut phase = CommitPhase::Failed; // The engine must initialize its real pre-call state.
            let result = prepared.commit(&client, Some(OperationId::from_bytes([21; 16])), Some(&mut phase), None, None).await;
            assert_eq!(phase, expected_phase);
            if denied == "none" {
                let (outcome, bytes) = result.expect("original immutable observation proves commit");
                assert!(matches!(&outcome, acyclic_stream::CommitOutcome::Committed(_)));
                let quota = CodecQuota::from_retained(&mut work, bytes);
                drop(outcome);
                drop(quota);
            } else {
                let counter = match denied {
                    "receiver" => "authority_records_read",
                    "inspection" => "backend_read_operations",
                    "write" => "backend_write_operations",
                    _ => unreachable!("the fixture defines every denial"),
                };
                assert!(matches!(result, Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Work(
                        acyclic_fs::WorkError::BudgetExceeded { counter: actual, .. },
                    ), ..
                }) if actual == counter), "preserve the original typed first cause");
            }
            drop(work);
            assert_eq!(io.into_custody().bytes(), 0, "payloads retire before their exact claims");
            provider.native_only.store(false, Ordering::SeqCst);
            let stream = client.stream(path.as_str()).expect("original canonical stream");
            assert_eq!(stream.tail().await.expect("actual immutable tail"), expected_tail);
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
            Box::pin(async move { reference.descriptor().verify(b"null", &mut None) })
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<acyclic_stream::OwnedBytes>> + Send + 'a>>
        {
            Box::pin(async move {
                reference.descriptor().verify(b"null", &mut None)?;
                Ok(b"null".to_vec().into())
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
        for value in 2..82 {
            declare(
                &mut coordinator,
                spec(OperationId::from_bytes([value; 16]), 0)?,
                &format!("point-other-{value}"),
            )
            .await?;
        }
        drop(coordinator);
        provider.forbid_writes.store(true, Ordering::SeqCst);
        assert_eq!(
            observe_declaration(&client, &owner, &scope, &verifier, operation, &key, &mut None).await?,
            Some(declaration)
        );
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        let denied = issuer.root("denied", Capabilities::default());
        assert!(matches!(
            observe_declaration(&client, &owner, &denied, &verifier, operation, &key, &mut None).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            observe_declaration(
                &client,
                &owner,
                &scope,
                &verifier,
                OperationId::from_bytes([99; 16]),
                &key
            , &mut None)
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
            , &mut None)
            .await,
            Err(Error::Unauthorized(_))
        ));
        let missing = IdempotencyKey::new("point-missing")?;
        assert_eq!(
            observe_declaration(&client, &owner, &scope, &verifier, operation, &missing, &mut None).await?,
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
            , &mut None)
            .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn atomic_session_retry_uses_the_original_journal_after_reopen()
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
            records.push(OwnedBytes::from(encode(offset as u64 + 1, root, intent_key, *blake3::hash(&canonical).as_bytes(), canonical, offset as u64 + 10, &mut None)?));
        }
        let source = client.stream(COORDINATOR_PATH)?;
        source
            .append_batch(records, Some(0), Some(stream_key_with_work(key, &mut None).map(|(key, _)| key)?))
            .await?;
        let (first, second) = tokio::join!(
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)),
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier))
        );
        let mut coordinator = first?;
        let second = second?;
        assert_eq!(coordinator.scheduler(), second.scheduler());
        for identity in 2..=81u8 {
            declare(
                &mut coordinator,
                spec(OperationId::from_bytes([identity; 16]), 0)?,
                &format!("intent-eviction-{identity}"),
            )
            .await?;
        }
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
                , &mut None)
                .await?,
            false
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
                , &mut None)
                .await
                .is_err()
        );
        assert_eq!(source.bounds().await?.tail, 82);
        drop(coordinator);
        let mut reopened =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(
            reopened
                .declare_session(
                    &owner,
                    &scope,
                    &verifier,
                    root_spec,
                    limits,
                    IdempotencyKey::new(key)?
                , &mut None)
                .await?,
            false
        );
        assert_eq!(source.bounds().await?.tail, 82);
        let canonical = crate::contract::canonical_json_bytes(&events[1])?;
        source
            .append_batch(
                vec![OwnedBytes::from(encode(83, root, &config_key, *blake3::hash(&canonical).as_bytes(), canonical, reopened.last_committed_at_ms + 1, &mut None)?)],
                Some(82),
                None,
            )
            .await?;
        assert!(matches!(reopened.refresh().await, Err(Error::Conflict(_))));
        assert_eq!(reopened.revision, 82);
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
                first.declare_operation(
                            &owner,
                            &signed,
                            &verifier,
                            child.clone(),
                            IdempotencyKey::new("owned-child")?,
                            Some(fence.clone())
                        ),
                disturbance
            );
            disturbance?;
            assert!(matches!(attempt, Err(Error::Conflict(_))));
            assert!(first.scheduler().operation(child_id).is_none());
            let retry = first
                .declare_operation(
                            &owner,
                            &signed,
                            &verifier,
                            child.clone(),
                            IdempotencyKey::new("owned-child")?,
                            Some(fence.clone())
                        )
                .await;
            if cancel {
                assert!(retry.is_err());
                assert!(first.scheduler().operation(child_id).is_none());
            } else {
                assert_eq!(retry?, true);
                assert_eq!(
                    first
                        .declare_operation(
                            &owner,
                            &signed,
                            &verifier,
                            child.clone(),
                            IdempotencyKey::new("owned-child")?,
                            Some(fence.clone())
                        )
                        .await?,
                    false
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
                        .declare_operation(
                            &owner,
                            &signed,
                            &verifier,
                            child,
                            IdempotencyKey::new("owned-child")?,
                            Some(fence)
                        )
                        .await
                        .is_err()
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn child_cancellation_checks_full_parent_fence_and_replays_lost_ack() -> Result<()> {
        use std::sync::atomic::Ordering;
        for lost_ack in [false, true] {
            let provider = Arc::new(LostSessionAck::default());
            let client = StreamClient::new(provider.clone());
            let mut coordinator = DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            let parent = OperationId::from_bytes([44; 16]);
            let child = OperationId::from_bytes([45; 16]);
            let grandchild = OperationId::from_bytes([46; 16]);
            declare(&mut coordinator, spec(parent, 0)?, "cancel-parent-declare").await?;
            let worker = Worker {
                id: "cancel-parent-worker".into(),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            };
            let lease = coordinator.pull(&worker).await?
                .ok_or_else(|| Error::NotFound("parent lease".into()))?;
            assert_eq!(lease.operation.operation_id, parent);
            let fence = LeaseFence::from(&lease.reservation);
            coordinator.apply(parent, IdempotencyKey::new("cancel-parent-start")?,
                SchedulerEvent::Started { operation_id: parent, fence: fence.clone() }).await?;
            for (operation_id, parent_id, key) in [
                (child, parent, "cancel-child-declare"),
                (grandchild, child, "cancel-grandchild-declare"),
            ] {
                let mut operation = spec(operation_id, 0)?;
                operation.parent = Some(ParentLink { operation_id: parent_id, slot: "child".into() });
                declare(&mut coordinator, operation, key).await?;
            }
            let lease = coordinator.pull(&worker).await?
                .ok_or_else(|| Error::NotFound("child lease".into()))?;
            assert_eq!(lease.operation.operation_id, child);
            coordinator.apply(child, IdempotencyKey::new("cancel-child-start")?,
                SchedulerEvent::Started { operation_id: child, fence: LeaseFence::from(&lease.reservation) }).await?;
            let owner = Authority { kind: AggregateKind::Task, id: "owner".into() };
            let issuer = AuthorityIssuer::new("test-runtime", [9; 32], owner.clone());
            let scope = issuer.root("cancel-child", Capabilities::new(["operation:cancel"]));
            let verifier = issuer.verifier();
            let before = coordinator.revision();
            let mut stale = fence.clone();
            stale.placement.push_str("-stale");
            assert!(matches!(coordinator.cancel_child_operation(
                &owner, &scope, &verifier, child, IdempotencyKey::new("stale-parent-cancel")?,
                parent, &stale,
             &mut None,).await, Err(Error::Unauthorized(_))));
            assert!(matches!(coordinator.cancel_child_operation(
                &owner, &scope, &verifier, grandchild, IdempotencyKey::new("indirect-child-cancel")?,
                parent, &fence,
             &mut None,).await, Err(Error::Unauthorized(_))));
            let foreign = Authority { kind: AggregateKind::Task, id: "foreign".into() };
            let foreign_issuer = AuthorityIssuer::new("test-runtime", [9; 32], foreign.clone());
            let foreign_scope = foreign_issuer.root("cancel-child", Capabilities::new(["operation:cancel"]));
            assert!(matches!(coordinator.cancel_child_operation(
                &foreign, &foreign_scope, &foreign_issuer.verifier(), child,
                IdempotencyKey::new("foreign-child-cancel")?, parent, &fence,
             &mut None,).await, Err(Error::NotFound(_))));
            assert_eq!(coordinator.revision(), before);
            provider.lose_ack.store(lost_ack, Ordering::SeqCst);
            let state = coordinator.cancel_child_operation(
                &owner, &scope, &verifier, child, IdempotencyKey::new("owned-child-cancel")?,
                parent, &fence,
             &mut None,).await?;
            assert!(state.cancellation_requested);
            assert!(state.outcome.is_none(), "running child retains uncertain provider work");
            assert_eq!(coordinator.scheduler().operation(grandchild)
                .and_then(|state| state.outcome.as_ref()), Some(&crate::Outcome::Cancelled));
            assert_eq!(coordinator.revision(), before + 1);
            let writes = provider.io.writes.load(Ordering::SeqCst);
            let replay = coordinator.cancel_child_operation(
                &owner, &scope, &verifier, child, IdempotencyKey::new("owned-child-cancel")?,
                parent, &fence,
             &mut None,).await?;
            assert_eq!(replay, state);
            assert_eq!(provider.io.writes.load(Ordering::SeqCst), writes);
            let records = read_coordinator_event_page(&client, 0, 64).await?;
            assert_eq!(records.iter().filter(|record| matches!(&record.event,
                SchedulerEvent::CancellationRequested { operation_id, recursive: true } if *operation_id == child
            )).count(), 1);
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
                    .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("wrong-selected-wake")?, Some(OperationId::from_bytes([53; 16])))
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
                first.resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected)),
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
                .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected))
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
                            .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected))
                            .await?,
                        false
                    );
                } else {
                    assert_eq!(retry?, true);
                    assert_eq!(
                        first
                            .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected))
                            .await?,
                        false
                    );
                }
                assert!(
                    first
                        .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("another-wake")?, Some(selected))
                        .await
                        .is_err()
                );
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
                        .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("selected-wake")?, Some(selected))
                        .await
                        .is_err()
                );
                assert_eq!(
                    first
                        .resume_workflow(&owner, &signed, &verifier, task, 1, input.clone(), IdempotencyKey::new("second-selected-wake")?, Some(next))
                        .await?,
                    true
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
                        .resume_workflow(&owner, &signed, &verifier, task, 1, input, IdempotencyKey::new("second-selected-wake")?, Some(next))
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

    pub(super) async fn declare<P: StreamProvider>(
        coordinator: &mut DistributedCoordinator<P>,
        spec: OperationSpec,
        key: &str,
    ) -> Result<bool> {
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
                None,
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
            , &mut None)
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            reopened.observe_operation(&owner, &replacement, &replaced.verifier(), operation_id, &mut None).cloned(),
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
                .pull_owned(&owner, &replacement, &replaced.verifier(), &worker, Some(operation_id), &mut None)
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            reopened
                .pull_owned(&owner, &replacement, &replaced.verifier(), &worker, None, &mut None)
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
            , &mut None)
            .await?,
            Some(original.clone())
        );
        assert_eq!(
            reopened
                .declare_operation(&owner, &renewed, &issuer.verifier(), original, key, None)
                .await?,
            false
        );
        assert!(matches!(
            reopened
                .pull_owned(&owner, &renewed, &issuer.verifier(), &worker, Some(operation_id), &mut None)
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
        original.verify_owner(owner, &issuer.verifier(), &mut None)?;
        let foreign = Authority {
            kind: AggregateKind::Task,
            id: "foreign".into(),
        };
        let wrong_audience = AuthorityIssuer::new("test-runtime", [9; 32], foreign.clone());
        assert!(matches!(
            original.verify_owner(owner, &wrong_audience.verifier(), &mut None),
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            original.verify_owner(&foreign, &issuer.verifier(), &mut None),
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
            tampered.verify_owner(owner, &issuer.verifier(), &mut None),
            Err(Error::Unauthorized(_))
        ));
        let mut insufficient = original.clone();
        insufficient.owner_scope =
            issuer.root("observe-only", Capabilities::new(["operation:observe"]));
        assert!(matches!(
            insufficient.verify_owner(owner, &issuer.verifier(), &mut None),
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
            true
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
            false
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
                    IdempotencyKey::new("missing-capability")?,
                    None,
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
                    IdempotencyKey::new("wrong-audience")?,
                    None,
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
                    IdempotencyKey::new("wrong-owner")?,
                    None,
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
             &mut None,)
            .await.map(|(page, _)| page)?;
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
             &mut None,)
            .await.map(|(page, _)| page)?;
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
                , &mut None)
                .await.map(|(page, _)| page),
            Err(Error::Conflict(_))
        ));
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
        assert_eq!(page[0].event.operation_id(), operation_id);
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
    #[allow(clippy::too_many_lines)]
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
                .observe_operation(&owner, &scope, &issuer.verifier(), parent, &mut None).cloned()?
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
        assert_eq!(applied, true);
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
        assert_eq!(replayed, false);
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
                .observe_operation(&owner, &scope, &issuer.verifier(), child, &mut None).cloned()?
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
            true
        );
        assert_eq!(
            declare(&mut second, declaration, "shared-declare").await?,
            false
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
                None,
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
                , &mut None)
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
            .pull_owned(&owner, &scope, &issuer.verifier(), &worker, None, &mut None)
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
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker, None, &mut None)
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
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker, None, &mut None)
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
                .pull_owned(&owner, &denied, &issuer.verifier(), &worker, None, &mut None)
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(client.stream(COORDINATOR_PATH)?.tail().await?, before);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
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
            .pull_owned(&owner, &scope, &issuer.verifier(), &worker, Some(target), &mut None)
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
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker, Some(target), &mut None)
                .await?,
            WorkPull::Idle
        ));
        assert!(matches!(
            coordinator
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker, Some(first), &mut None)
                .await?,
            WorkPull::Idle
        ));
        assert!(matches!(
            coordinator
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker, Some(OperationId::from_bytes([63; 16])), &mut None)
                .await,
            Err(Error::NotFound(_))
        ));
        let denied = issuer.root("denied", Capabilities::new(["operation:observe"]));
        assert!(matches!(
            coordinator
                .pull_owned(&owner, &denied, &issuer.verifier(), &worker, Some(first), &mut None)
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
            .pull_owned(&owner, &scope, &issuer.verifier(), &worker, Some(ready), &mut None)
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
                .pull_owned(&owner, &scope, &issuer.verifier(), &worker, Some(blocked), &mut None)
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
                    .pull_owned(&owner, &scope, &issuer.verifier(), &worker, Some(operation), &mut None)
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
    async fn owned_pull_retains_exact_attempt_after_unresolved_admission() -> Result<()> {
        use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
        for (fault, targeted) in [(1, false), (2, false), (1, true), (2, true)] {
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
                    .store(4, Ordering::SeqCst);
            }
            let WorkPull::Unresolved { lease, error } = coordinator
                .pull_owned(
                    &owner,
                    &scope,
                    &issuer.verifier(),
                    &worker,
                    targeted.then_some(operation), &mut None)
                .await?
            else {
                return Err(Error::NotFound("unresolved admission attempt".into()));
            };
            assert!(matches!(error, Error::Indeterminate(id) if id == operation));
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
                    crate::scheduler::require_execution_owner(retained, &LeaseFence::from(&lease.reservation), false, &mut None)
                    .is_err()
                );
                let WorkPull::Claimed(retried) = reopened
                    .pull_owned(
                        &owner,
                        &scope,
                        &issuer.verifier(),
                        &worker,
                        targeted.then_some(operation), &mut None)
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
                    .pull_owned(
                        &owner,
                        &scope,
                        &issuer.verifier(),
                        &worker,
                        targeted.then_some(operation), &mut None)
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
                        .fork(execution_path(execution, &mut None)?.as_str(), Some(0), None)
                        .await?;
                    if tail != 0 {
                        client
                            .stream(execution_path(execution, &mut None)?.as_str())?
                            .append_batch(
                                vec![
                                    OwnedBytes::from_static(b"settled observation");
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
                        expected_tail, None, &mut None
                    )
                    .await;
                if mode == 3 {
                    assert!(matches!(result, Err(Error::Conflict(_))));
                } else if mode == 2 {
                    assert!(matches!(result, Err(Error::Indeterminate(id)) if id == task));
                } else {
                    assert_eq!(result?, true);
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
                            .stream(execution_path(execution, &mut None)?.as_str())?
                            .tail()
                            .await?,
                        expected_tail + 1
                    );
                    assert!(
                        coordinator
                            .apply_if_execution_quiescent(
                                IdempotencyKey::new("idle-suspend")?,
                                event,
                                execution,
                                expected_tail, None, &mut None
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
                    let outcome = client
                        .commit(acyclic_stream::CommitRequest {
                            conditions: vec![
                                acyclic_stream::CommitCondition::Tail {
                                    path: acyclic_stream::StreamPath::new(COORDINATOR_PATH)?,
                                    expected: reopened.revision() - 1,
                                },
                                if existing_tail.is_some() {
                                    acyclic_stream::CommitCondition::Tail {
                                        path: execution_path(execution, &mut None)?,
                                        expected: expected_tail,
                                    }
                                } else {
                                    acyclic_stream::CommitCondition::Absent {
                                        path: execution_path(execution, &mut None)?,
                                    }
                                },
                            ],
                            mutations: vec![acyclic_stream::CommitMutation::Append {
                                path: execution_path(execution, &mut None)?,
                                records: vec![OwnedBytes::from_static(b"stale dispatch")],
                            }],
                            idempotency_key: stream_key_with_work("stale-idle-dispatch", &mut None).map(|(key, _)| key)?,
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
                 &mut None,)
                .await;
            if hide_receipt {
                assert!(matches!(result, Err(Error::Indeterminate(id)) if id == root));
            } else {
                assert!(result.is_ok());
            }
            drop(coordinator);
            let mut recovered =
                DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
            assert_eq!(recovered.scheduler().session_limits(root, &mut None)?, limits);
            assert_eq!(
                recovered
                    .declare_session(
                        &owner,
                        &signed,
                        &issuer.verifier(),
                        spec(root, 0)?,
                        limits,
                        IdempotencyKey::new("lost-root")?
                    , &mut None)
                    .await?,
                false
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
        assert_eq!(coordinator.scheduler().session_limits(root, &mut None)?, limits);
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
            , &mut None)
            .await
            .is_err()
        );
        assert!(
            read_coordinator_event_page(&client, 0, 64)
                .await?
                .is_empty()
        );
        let mut observers = (None, None);
        let (first, second) = tokio::join!(
            left.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(root, 0)?,
                one,
                IdempotencyKey::new("atomic-one")?
            , &mut observers.0),
            right.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(root, 0)?,
                two,
                IdempotencyKey::new("atomic-two")?
            , &mut observers.1),
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
        assert_eq!(recovered.scheduler().session_limits(root, &mut None)?, limits);
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
                , &mut None)
                .await?,
            false
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
                , &mut None)
                .await
                .is_err()
        );
        assert_eq!(read_coordinator_event_page(&client, 0, 64).await?.len(), 3);
        let identical_root = OperationId::from_bytes([88; 16]);
        let mut duplicate =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        let mut observers = (None, None);
        let (first, second) = tokio::join!(
            recovered.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(identical_root, 0)?,
                limits,
                IdempotencyKey::new("identical-root")?
            , &mut observers.0),
            duplicate.declare_session(
                &owner,
                &signed,
                &verifier,
                spec(identical_root, 0)?,
                limits,
                IdempotencyKey::new("identical-root")?
            , &mut observers.1),
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
            false
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
            .inspect_idempotency(stream_key_with_work(key, &mut None).map(|(key, _)| key)?)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .ok_or_else(|| Error::NotFound("winning budget receipt".into()))?;
        let IdempotencyOutcome::Append(AppendOutcome::Committed(receipt)) = receipt.outcome else {
            return Err(Error::Invalid("winning budget append required".into()));
        };
        let revision = receipt.end;
        let mut delayed =
            DistributedCoordinator::open(&client, Arc::new(TestContentVerifier)).await?;
        assert_eq!(
            delayed
                .finish_append_with_work(
                    AppendOutcome::Committed(receipt.clone()),
                    winner,
                    key,
                    &digest,
                    revision, &mut None)
                .await?,
            true
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
                .finish_append_with_work(
                    AppendOutcome::Committed(receipt),
                    winner,
                    key,
                    &digest,
                    revision, &mut None)
                .await
                .is_err(),
            "a delayed committed response cannot bypass cancellation"
        );
        let conflict = delayed
            .stream.stream
            .append_batch(vec![OwnedBytes::from_static(b"not committed")], Some(0), None)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(matches!(conflict, AppendOutcome::TailConflict { .. }));
        assert!(
            delayed
                .finish_append_with_work(conflict, winner, key, &digest, revision, &mut None)
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
