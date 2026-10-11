//! Single-lock deterministic reference state machine; no durability or availability claim.
//!
//! Native calls observe this SDK boundary, not allocator/RSS or opaque transport
//! internals. New Vec capacities, boxed cursors/futures and history Arc payloads
//! retain original-meter custody; immutable Bytes aliases neither allocate nor
//! copy a body. Opaque std tree/hash nodes and Tokio watch internals have no
//! invented physical allocation count; tree entries use declared logical quota.
//! Reads return their completed ordered prefix before a late original error.
//! Accepted commits carry late causes to Local's canonical durability drain.
//! Deadline-bound new facts check the trusted provider clock at publication;
//! previously retained idempotent outcomes are replayed before deadline checks.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    ops::Bound,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::{RwLock, watch};

#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    ReadAllocation,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    NativeObservedRecord, NativeProviderFuture, NativeRecordStream, NativeStreamOutput,
    NativeStreamResult,
};

#[derive(Debug)]
pub(crate) enum MemoryFailure {
    Provider(StreamError),
    Observer(std::io::Error),
}

impl From<StreamError> for MemoryFailure {
    fn from(error: StreamError) -> Self { Self::Provider(error) }
}

impl From<std::io::Error> for MemoryFailure {
    fn from(error: std::io::Error) -> Self { Self::Observer(error) }
}

pub(crate) type MemoryResult<T> = Result<T, MemoryFailure>;

#[derive(Default)]
pub(crate) struct MemoryObserver {
    #[cfg(not(target_arch = "wasm32"))]
    meter: Option<Arc<dyn NativeReadMeter>>,
}

#[derive(Default)]
pub(crate) struct MemoryAllocation {
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<ReadAllocation>,
}

impl MemoryAllocation {
    pub(crate) fn absorb(&mut self, mut other: Self) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mut incoming) = other.allocation.take() {
            if let Some(allocation) = self.allocation.as_mut() {
                allocation.absorb(&mut incoming)?;
            } else {
                self.allocation = Some(incoming);
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = &mut other;
        Ok(())
    }
}

pub(crate) struct MemoryOutput<T> {
    pub(crate) value: T,
    pub(crate) allocation: MemoryAllocation,
}

struct MemoryByteReservation<'a> {
    #[cfg(not(target_arch = "wasm32"))]
    reservation: Option<NativeByteReservation<'a>>,
    #[cfg(target_arch = "wasm32")]
    _observer: &'a MemoryObserver,
}

impl MemoryByteReservation<'_> {
    fn record(&mut self, bytes: usize) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(reservation) = self.reservation.as_mut() { return reservation.record(bytes); }
        let _ = bytes;
        Ok(())
    }
}

impl MemoryObserver {
    fn check(&self) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter { meter.check()?; }
        Ok(())
    }

    fn allocate(&self, bytes: usize) -> std::io::Result<MemoryAllocation> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            return Ok(MemoryAllocation {
                allocation: Some(if bytes == 0 {
                    ReadAllocation::empty(meter)
                } else {
                    ReadAllocation::new(meter, bytes)?
                }),
            });
        }
        let _ = bytes;
        Ok(MemoryAllocation::default())
    }

    pub(crate) fn vec<T>(&self, capacity: usize) -> std::io::Result<MemoryOutput<Vec<T>>> {
        let bytes = capacity.checked_mul(std::mem::size_of::<T>())
            .ok_or(std::io::ErrorKind::InvalidInput)?;
        let allocation = self.allocate(bytes)?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut allocation = allocation;
        let value = Vec::with_capacity(capacity);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(allocation) = allocation.allocation.as_mut() {
            let actual = value.capacity().checked_mul(std::mem::size_of::<T>())
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            allocation.reconcile_capacity(actual)?;
        }
        Ok(MemoryOutput { value, allocation })
    }

    pub(crate) fn admit_item(&self) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter { meter.admit_examination(1, 0)?; }
        self.check()
    }

    pub(crate) fn record_item(&self) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter { meter.record_examination(1, 0)?; }
        Ok(())
    }

    fn read_bytes(&self, bytes: usize) -> std::io::Result<MemoryByteReservation<'_>> {
        #[cfg(not(target_arch = "wasm32"))]
        let reservation = self.meter.as_ref().map(|meter| {
            NativeByteReservation::new(meter.as_ref(), NativeOperationKind::Read, bytes)
        }).transpose()?;
        let _ = bytes;
        Ok(MemoryByteReservation {
            #[cfg(not(target_arch = "wasm32"))]
            reservation,
            #[cfg(target_arch = "wasm32")]
            _observer: self,
        })
    }


    fn boundary(&self) -> std::io::Result<()> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            // The boundary is this SDK call, not an opaque transport/syscall.
            let mut operation = NativeOperationReservation::new(meter.as_ref(), NativeOperationKind::Read, 1)?;
            let ((), recorded) = operation.attempt(|| ());
            recorded?;
        }
        Ok(())
    }

    fn adopt(&self, bytes: usize) -> std::io::Result<MemoryAllocation> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            return Ok(MemoryAllocation { allocation: Some(ReadAllocation::adopt(meter, bytes)?) });
        }
        let _ = bytes;
        Ok(MemoryAllocation::default())
    }

    fn write_boundary(&self) -> std::io::Result<()> {
        self.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            let mut operation = NativeOperationReservation::new(meter.as_ref(), NativeOperationKind::Write, 1)?;
            // Entry into this SDK call is the observable operation, not a
            // claim that an opaque native transport/syscall was performed.
            let ((), recorded) = operation.attempt(|| ());
            recorded?;
        }
        Ok(())
    }

    fn write_bytes(&self, bytes: usize) -> std::io::Result<MemoryByteReservation<'_>> {
        #[cfg(not(target_arch = "wasm32"))]
        let reservation = self.meter.as_ref().map(|meter| {
            NativeByteReservation::new(meter.as_ref(), NativeOperationKind::Write, bytes)
        }).transpose()?;
        let _ = bytes;
        Ok(MemoryByteReservation {
            #[cfg(not(target_arch = "wasm32"))]
            reservation,
            #[cfg(target_arch = "wasm32")]
            _observer: self,
        })
    }

    pub(crate) fn retain_path(&self, path: &StreamPath) -> std::io::Result<StreamPath> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter { return path.retain_native(meter); }
        Ok(path.clone())
    }

    fn parent_path(&self, path: &StreamPath) -> std::io::Result<Option<StreamPath>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter { return path.parent_native(meter); }
        Ok(path.parent())
    }

    fn bytes(&self, source: &[u8]) -> std::io::Result<Bytes> {
        let mut output = self.vec(source.len())?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter { meter.admit_codec(0, source.len() as u64, 0)?; }
        output.value.extend_from_slice(source);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            meter.record_codec(0, source.len() as u64, 0)?;
            meter.allocate_refcount_header()?;
        }
        Ok(Bytes::from_owner(MemoryBytes {
            bytes: output.value,
            _allocation: output.allocation,
        }))
    }
}

pub(crate) fn provider_result<T>(result: MemoryResult<T>) -> Result<T, StreamError> {
    match result {
        Ok(value) => Ok(value),
        Err(MemoryFailure::Provider(error)) => Err(error),
        Err(MemoryFailure::Observer(_)) => {
            unreachable!("an ordinary memory operation has no fallible observer")
        }
    }
}

fn ordinary<T>(result: MemoryResult<MemoryOutput<T>>) -> Result<T, StreamError> {
    provider_result(result).map(|output| output.value)
}

#[cfg(not(target_arch = "wasm32"))]
fn native<T>(
    result: MemoryResult<MemoryOutput<T>>,
    meter: &Arc<dyn NativeReadMeter>,
) -> NativeStreamResult<T> {
    match result {
        Ok(output) => Ok(Ok(NativeStreamOutput::new(
            output.value,
            output.allocation.allocation.unwrap_or_else(|| ReadAllocation::empty(meter)),
        ))),
        Err(MemoryFailure::Provider(error)) => Ok(Err(error)),
        Err(MemoryFailure::Observer(error)) => Err(error),
    }
}

#[cfg(feature = "local")]
mod snapshot;

use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, Child, ChildStream, ChildrenPage,
    ChildrenPageRequest, ChildrenRequest, CommitCondition, CommitConflict, CommitId,
    CommitMutation, CommitOutcome, CommitRequest, CommittedEnvelope, CommittedFork,
    CommittedMutation, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    IdempotencyOutcome, MAX_COMMAND_BYTES, MAX_ITEMS, MAX_RECORD_BYTES, ReadRequest, Record,
    RecordStream, StreamBounds, StreamError, StreamPath, StreamProvider, SystemUnixMillisClock,
    UnixMillisClock,
};

/// Explicit fail-closed process-memory ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryLimits {
    /// Maximum created paths, including implicit parents.
    pub paths: usize,
    /// Maximum unique retained path bytes.
    pub path_bytes: usize,
    /// Maximum unique committed records.
    pub records: usize,
    /// Maximum unique retained payload bytes.
    pub payload_bytes: usize,
    /// Maximum immutable commit envelopes.
    pub commits: usize,
    /// Maximum indefinitely retained replay identities. New identities fail
    /// closed at capacity; an admitted identity is never forgotten or reused.
    pub idempotency_results: usize,
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            paths: 100_000,
            path_bytes: 64 * 1024 * 1024,
            records: 1_000_000,
            payload_bytes: 256 * 1024 * 1024,
            commits: 1_000_000,
            idempotency_results: 1_000_000,
        }
    }
}

/// Deterministic process-local provider. It retains each idempotency result and committed
/// envelope indefinitely within explicit capacity limits. It deliberately claims
/// no crash recovery; `LocalStream` supplies persistence when enabled.
#[derive(Clone)]
pub struct MemoryStream {
    state: Arc<RwLock<State>>,
    limits: MemoryLimits,
    /// The trusted clock that deadlines are checked against.
    clock: Arc<dyn UnixMillisClock>,
    /// Clock whose sampled commit time is recorded in each new record.
    commit_clock: Arc<dyn UnixMillisClock>,
}

impl Default for MemoryStream {
    fn default() -> Self {
        Self::new(MemoryLimits::default())
    }
}

impl MemoryStream {
    pub(crate) async fn next_follow_record(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
    ) -> Result<Option<Record>, StreamError> {
        let state = self.state.read().await;
        let current = state.paths.get(path).ok_or(StreamError::NotFound)?;
        cursor.sync(current.history.as_ref(), current.tail);
        Ok(cursor.next_record())
    }

    /// Constructs one bounded independent provider.
    #[must_use]
    pub fn new(limits: MemoryLimits) -> Self {
        Self::new_with_clock(limits, Arc::new(SystemUnixMillisClock))
    }

    /// Constructs a provider with an injected trusted clock.
    #[must_use]
    pub fn new_with_clock(limits: MemoryLimits, clock: Arc<dyn UnixMillisClock>) -> Self {
        Self::new_with_commit_clock(limits, Arc::clone(&clock), clock)
    }

    pub(crate) fn new_with_commit_clock(
        limits: MemoryLimits,
        clock: Arc<dyn UnixMillisClock>,
        commit_clock: Arc<dyn UnixMillisClock>,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
            limits,
            clock,
            commit_clock,
        }
    }

    async fn inspect_inner(
        &self,
        idempotency_key: IdempotencyKey,
        observer: &MemoryObserver,
    ) -> MemoryResult<MemoryOutput<Option<IdempotencyObservation>>> {
        observer.boundary()?;
        let state = self.state.read().await;
        observer.admit_item()?;
        let replay = state.replays.get(idempotency_key.as_bytes());
        observer.record_item()?;
        let Some(replay) = replay else {
            return Ok(MemoryOutput { value: None, allocation: observer.allocate(0)? });
        };
        let outcome = clone_idempotency_outcome(&replay.result, observer)?;
        Ok(MemoryOutput {
            value: Some(IdempotencyObservation {
                idempotency_key,
                request_digest: replay.digest,
                outcome: outcome.value,
            }),
            allocation: outcome.allocation,
        })
    }

    async fn bounds_inner(
        &self,
        path: StreamPath,
        observer: &MemoryObserver,
    ) -> MemoryResult<MemoryOutput<StreamBounds>> {
        observer.boundary()?;
        let state = self.state.read().await;
        observer.admit_item()?;
        let stream = state.paths.get(&path);
        observer.record_item()?;
        let stream = stream.ok_or(StreamError::NotFound)?;
        Ok(MemoryOutput {
            value: StreamBounds { tail: stream.tail },
            allocation: observer.allocate(0)?,
        })
    }

    async fn read_inner(
        &self,
        request: ReadRequest,
        observer: &MemoryObserver,
    ) -> MemoryResult<ReadHistoryOutput> {
        observer.boundary()?;
        let state = self.state.read().await;
        observer.admit_item()?;
        let path = state.paths.get(&request.path);
        observer.record_item()?;
        let path = path.ok_or(StreamError::NotFound)?;
        if request.from > path.tail { return Err(StreamError::OutOfRange.into()); }
        read_history(
            path.history.as_ref(),
            path.tail,
            request.from,
            usize::try_from(request.limit).map_err(|_| StreamError::LimitExceeded)?,
            observer,
        )
    }

    async fn read_commit_inner(
        &self,
        commit_id: CommitId,
        observer: &MemoryObserver,
    ) -> MemoryResult<MemoryOutput<CommittedEnvelope>> {
        observer.boundary()?;
        let state = self.state.read().await;
        observer.admit_item()?;
        let envelope = state.commits.get(&commit_id);
        observer.record_item()?;
        clone_envelope(envelope.ok_or(StreamError::NotFound)?, observer, true)
    }

    async fn commit_inner(
        &self,
        mut request: CommitRequest,
        deadline_unix_millis: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        let completion = self.commit_observed(&mut request, deadline_unix_millis, &MemoryObserver::default()).await;
        ordinary(completion.map(|completion| {
            assert!(completion.error.is_none(), "ordinary commits have no fallible observer");
            completion.outcome
        }))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn native_commit_completion_inner(
        &self,
        mut request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> std::io::Result<Result<NativeMemoryCommitCompletion, StreamError>> {
        self.native_commit_completion_borrowed_inner(&mut request, deadline_unix_millis, meter).await
    }

    /// Keeps caller-owned input vectors intact through the canonical decision so
    /// a detached Local owner can retain their physical custody until its drain.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn native_commit_completion_borrowed_inner(
        &self,
        request: &mut CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> std::io::Result<Result<NativeMemoryCommitCompletion, StreamError>> {
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        match self.commit_observed(request, deadline_unix_millis, &observer).await {
            Ok(completion) => Ok(Ok(NativeMemoryCommitCompletion {
                outcome: native(Ok(completion.outcome), &meter)?
                    .expect("a completed memory outcome is already a provider success"),
                error: completion.error,
            })),
            Err(MemoryFailure::Provider(error)) => Ok(Err(error)),
            Err(MemoryFailure::Observer(error)) => Err(error),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn native_commit_inner(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeStreamResult<CommitOutcome> {
        match self.native_commit_completion_inner(request, deadline_unix_millis, meter).await? {
            Ok(completion) => match completion.error {
                Some(error) => Err(error),
                None => Ok(Ok(completion.outcome)),
            },
            Err(error) => Ok(Err(error)),
        }
    }

    async fn commit_observed(
        &self,
        request: &mut CommitRequest,
        deadline_unix_millis: Option<u64>,
        observer: &MemoryObserver,
    ) -> MemoryResult<MemoryCommitCompletion> {
        observer.check()?;
        normalize_commit_with_observer(request, observer)?;
        validate_commit_shape_with_observer(request, observer)?;
        #[cfg(not(target_arch = "wasm32"))]
        let digest = commit_digest_observed(request, observer.meter.as_ref())?;
        #[cfg(target_arch = "wasm32")]
        let digest = commit_digest_with_observer(request, observer)?;
        let bytes = request.mutations.iter().map(|mutation| match mutation {
            CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                records.iter().map(Bytes::len).sum::<usize>()
            }
        }).sum();
        observer.write_boundary()?;
        let mut write = observer.write_bytes(bytes)?;
        let mut state = self.state.write().await;
        observer.admit_item()?;
        let replay = state.replays.get(request.idempotency_key.as_bytes());
        observer.record_item()?;
        if let Some(replay) = replay {
            if replay.digest != digest { return Err(StreamError::IdempotencyMismatch.into()); }
            let IdempotencyOutcome::Commit(outcome) = &replay.result else {
                return Err(StreamError::IdempotencyMismatch.into());
            };
            return Ok(MemoryCommitCompletion {
                outcome: clone_commit_outcome(outcome, observer, false)?,
                error: None,
            });
        }
        if deadline_unix_millis.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
            return Err(StreamError::DeadlineElapsed.into());
        }
        admit_replay(&state, Some(&request.idempotency_key), self.limits)?;
        let conflicts = commit_conflicts(&state, &request.conditions, observer)?;
        let entry = std::mem::size_of::<(Bytes, Replay)>();
        let mut resident = observer.adopt(entry)?;
        let (outcome, envelope, paths, mutations, decision, committed_at_micros) = if conflicts.value.is_empty() {
            drop(conflicts);
            validate_commit_authority(&state, request, observer)?;
            let missing = missing_commit_paths(&state, request, observer)?;
            if reserve_coordinated(&state, request, &missing.value, self.limits).is_err() {
                recount_observed(&mut state, observer)?;
                reserve_coordinated(&state, request, &missing.value, self.limits)?;
            }
            let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
            let commit_id = planned_commit_id(decision, digest, observer)?;
            let committed_at_micros = state.last_committed_at_micros
                .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
            let envelope = prepare_envelope(&state, request, commit_id, committed_at_micros, observer)?;
            let mutations = prepare_mutations(&state, &envelope.value, observer)?;
            let paths = prepare_paths(&missing.value, &envelope.value, observer)?;
            let retained = clone_envelope(&envelope.value, observer, false)?;
            resident.absorb(retained.allocation)?;
            resident.absorb(observer.adopt(std::mem::size_of::<(CommitId, CommittedEnvelope)>())?)?;
            let outcome = MemoryOutput {
                value: CommitOutcome::Committed(envelope.value),
                allocation: envelope.allocation,
            };
            (outcome, Some(retained.value), paths, mutations, Some(decision), committed_at_micros)
        } else {
            let outcome = MemoryOutput { value: CommitOutcome::Conflict(conflicts.value), allocation: conflicts.allocation };
            (outcome, None, observer.vec(0)?, observer.vec(0)?, None, state.last_committed_at_micros)
        };
        let replay = clone_commit_outcome(&outcome.value, observer, false)?;
        resident.absorb(replay.allocation)?;
        let key = observer.bytes(request.idempotency_key.as_bytes())?;
        prepare_retained_capacity(&mut state, observer)?;
        let prepared = PreparedPublication {
            envelope, replay: IdempotencyOutcome::Commit(replay.value), key,
            paths, mutations, resident,
        };
        observer.check()?;
        if deadline_unix_millis.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
            return Err(StreamError::DeadlineElapsed.into());
        }
        // Everything fallible precedes publication; no await splits this CAS.
        publish(&mut state, prepared, digest);
        if let Some(decision) = decision { state.decision = decision; }
        state.last_committed_at_micros = committed_at_micros;
        // Accepted outcomes carry late original causes so Local can drain its
        // canonical journal/durability obligations before returning the cause.
        let error = write.record(bytes).err();
        Ok(MemoryCommitCompletion { outcome, error })
    }
}

struct MemoryCommitCompletion {
    outcome: MemoryOutput<CommitOutcome>,
    error: Option<std::io::Error>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeMemoryCommitCompletion {
    pub(crate) outcome: NativeStreamOutput<CommitOutcome>,
    pub(crate) error: Option<std::io::Error>,
}

#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    commits: BTreeMap<CommitId, CommittedEnvelope>,
    replays: BTreeMap<Bytes, Replay>,
    /// Indefinite receipt inventory used by the current snapshot codec.
    retained: VecDeque<Retained>,
    path_bytes: usize,
    /// Records and payload bytes retained, each counted once however many
    /// paths share it; an upper bound between [`recount`]s.
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    /// High-water mark survives clock rollback and snapshot restoration.
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    retained_allocation: MemoryAllocation,
}

struct MemoryBytes {
    bytes: Vec<u8>,
    _allocation: MemoryAllocation,
}

impl AsRef<[u8]> for MemoryBytes {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}



impl Drop for State {
    fn drop(&mut self) {
        // Forks and appends form a shared DAG. Dropping a long, uniquely owned
        // History chain through Arc's default destructor consumes one stack
        // frame per ancestor, so tear down the graph from its roots iteratively.
        for (_, mut path) in std::mem::take(&mut self.paths) {
            let mut cursor = path.history.take();
            while let Some(node) = cursor {
                let Ok(history) = Arc::try_unwrap(node) else { break; };
                cursor = match history {
                    History::Batch { parent, .. } => parent,
                    History::Prefix { source, .. } => source,
                };
            }
        }
    }
}

/// One indefinitely retained snapshot entry.
#[cfg_attr(
    not(feature = "local"),
    allow(dead_code, reason = "only local snapshots read the retained fields")
)]
struct Retained {
    replay: Option<Bytes>,
    commit: Option<CommitId>,
}

struct PathState {
    history: Option<Arc<History>>,
    tail: u64,
    changed: watch::Sender<u64>,
    _allocation: MemoryAllocation,
}

enum History {
    Batch {
        parent: Option<Arc<Self>>,
        records: Arc<MemoryRecords>,
        _allocation: MemoryAllocation,
    },
    Prefix {
        source: Option<Arc<Self>>,
        tail: u64,
        _allocation: MemoryAllocation,
    },
}

struct MemoryRecords {
    records: Vec<Record>,
    _allocation: MemoryAllocation,
}

impl From<Vec<Record>> for MemoryRecords {
    fn from(records: Vec<Record>) -> Self {
        Self { records, _allocation: MemoryAllocation::default() }
    }
}

impl std::ops::Deref for MemoryRecords {
    type Target = [Record];
    fn deref(&self) -> &Self::Target { &self.records }
}

impl AsRef<[Record]> for MemoryRecords {
    fn as_ref(&self) -> &[Record] { &self.records }
}

struct Replay {
    digest: [u8; 32],
    result: IdempotencyOutcome,
    _allocation: MemoryAllocation,
}

fn clone_records(
    records: &[Record],
    observer: &MemoryObserver,
    output_read: bool,
) -> MemoryResult<MemoryOutput<Vec<Record>>> {
    let mut output = observer.vec(records.len())?;
    for record in records {
        observer.admit_item()?;
        let mut read = if output_read { Some(observer.read_bytes(record.value.len())?) } else { None };
        output.value.push(record.clone());
        let examined = observer.record_item();
        let read = read.as_mut().map_or(Ok(()), |read| read.record(record.value.len()));
        examined.and(read)?;
        observer.check()?;
    }
    Ok(output)
}

fn clone_envelope(
    envelope: &CommittedEnvelope,
    observer: &MemoryObserver,
    output_read: bool,
) -> MemoryResult<MemoryOutput<CommittedEnvelope>> {
    let mut output = observer.vec(envelope.mutations.len())?;
    for mutation in &envelope.mutations {
        observer.admit_item()?;
        let cloned = match mutation {
            CommittedMutation::Append(append) => {
                let records = clone_records(&append.records, observer, output_read)?;
                output.allocation.absorb(records.allocation)?;
                CommittedMutation::Append(crate::CommittedAppend {
                    path: append.path.clone(),
                    start: append.start,
                    end: append.end,
                    tail: append.tail,
                    records: records.value,
                })
            }
            CommittedMutation::Fork(fork) => {
                let records = clone_records(&fork.records, observer, output_read)?;
                output.allocation.absorb(records.allocation)?;
                CommittedMutation::Fork(CommittedFork {
                    source: fork.source.clone(),
                    destination: fork.destination.clone(),
                    forked_at: fork.forked_at,
                    tail: fork.tail,
                    records: records.value,
                })
            }
        };
        output.value.push(cloned);
        observer.record_item()?;
    }
    Ok(MemoryOutput {
        value: CommittedEnvelope { commit_id: envelope.commit_id, mutations: output.value },
        allocation: output.allocation,
    })
}

fn clone_commit_outcome(
    outcome: &CommitOutcome,
    observer: &MemoryObserver,
    output_read: bool,
) -> MemoryResult<MemoryOutput<CommitOutcome>> {
    match outcome {
        CommitOutcome::Committed(envelope) => {
            let output = clone_envelope(envelope, observer, output_read)?;
            Ok(MemoryOutput {
                value: CommitOutcome::Committed(output.value),
                allocation: output.allocation,
            })
        }
        CommitOutcome::Conflict(conflicts) => {
            let mut output = observer.vec(conflicts.len())?;
            for conflict in conflicts {
                observer.admit_item()?;
                output.value.push(conflict.clone());
                observer.record_item()?;
            }
            Ok(MemoryOutput {
                value: CommitOutcome::Conflict(output.value),
                allocation: output.allocation,
            })
        }
    }
}

fn clone_idempotency_outcome(
    outcome: &IdempotencyOutcome,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<IdempotencyOutcome>> {
    match outcome {
        IdempotencyOutcome::Commit(outcome) => {
            let output = clone_commit_outcome(outcome, observer, true)?;
            Ok(MemoryOutput {
                value: IdempotencyOutcome::Commit(output.value),
                allocation: output.allocation,
            })
        }
        IdempotencyOutcome::Append(outcome) => Ok(MemoryOutput {
            value: IdempotencyOutcome::Append(outcome.clone()),
            allocation: observer.allocate(0)?,
        }),
        IdempotencyOutcome::Fork(outcome) => Ok(MemoryOutput {
            value: IdempotencyOutcome::Fork(outcome.clone()),
            allocation: observer.allocate(0)?,
        }),
    }
}

struct MemoryRecordIterator {
    records: std::vec::IntoIter<Record>,
    terminal: Option<std::io::Error>,
    _allocation: MemoryAllocation,
}

impl Iterator for MemoryRecordIterator {
    type Item = std::io::Result<Record>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(record) = self.records.next() { return Some(Ok(record)); }
        self.terminal.take().map(Err)
    }
}

fn record_iterator(output: ReadHistoryOutput) -> MemoryRecordIterator {
    MemoryRecordIterator {
        records: output.records.value.into_iter(),
        terminal: output.terminal,
        _allocation: output.records.allocation,
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeMemoryRecordCursor {
    records: MemoryRecordIterator,
    meter: Arc<dyn NativeReadMeter>,
}

#[cfg(not(target_arch = "wasm32"))]
impl futures::Stream for NativeMemoryRecordCursor {
    type Item = std::io::Result<Result<NativeObservedRecord, StreamError>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let cursor = self.get_mut();
        std::task::Poll::Ready(cursor.records.next().map(|record| record.map(|record| {
            Ok(NativeStreamOutput::new(record, ReadAllocation::empty(&cursor.meter)))
        })))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StreamProvider for MemoryStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        ordinary(self.inspect_inner(idempotency_key, &MemoryObserver::default()).await)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        ordinary(self.bounds_inner(path, &MemoryObserver::default()).await).map(|bounds| bounds.tail)
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        ordinary(self.bounds_inner(path, &MemoryObserver::default()).await)
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        validate_records(&request.records)?;
        validate_append_size(&request)?;
        let digest = append_digest(&request);
        let mut state = self.state.write().await;
        if let Some(result) = replay_append(&state, request.idempotency_key.as_ref(), digest)? {
            return Ok(result);
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        let actual_tail = state.paths.get(&request.path).map_or(0, |path| path.tail);
        if request
            .if_tail
            .is_some_and(|expected| expected != actual_tail)
        {
            let result = AppendOutcome::TailConflict { actual_tail };
            retain(
                &mut state,
                None,
                request.idempotency_key,
                digest,
                IdempotencyOutcome::Append(result.clone()),
            );
            return Ok(result);
        }
        if reserve_append(&state, &request.path, &request.records, self.limits).is_err() {
            recount(&mut state);
            reserve_append(&state, &request.path, &request.records, self.limits)?;
        }
        let commit_id = next_commit_id(&mut state, digest)?;
        let committed_at_micros = state
            .last_committed_at_micros
            .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
        let fact = crate::preparation::append_fact(
            request.path.clone(),
            actual_tail,
            request.records,
            commit_id,
            committed_at_micros,
        )?;
        let records = &fact.records;
        ensure_path(&mut state, &request.path, commit_id);
        let resulting_tail = {
            let stream = state
                .paths
                .get_mut(&request.path)
                .ok_or(StreamError::Unavailable)?;
            stream.history = Some(Arc::new(History::Batch {
                parent: stream.history.clone(),
                records: Arc::new(MemoryRecords::from(records.clone())),
                _allocation: MemoryAllocation::default(),
            }));
            stream.tail = fact.tail;
            stream.changed.send_replace(stream.tail);
            stream.tail
        };
        state.record_count += records.len();
        state.payload_bytes += records
            .iter()
            .map(|record| record.value.len())
            .sum::<usize>();
        let receipt = AppendReceipt {
            start: actual_tail,
            end: resulting_tail,
            tail: resulting_tail,
            commit_id,
        };
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Append(fact)],
        };
        let result = AppendOutcome::Committed(receipt);
        state.last_committed_at_micros = committed_at_micros;
        retain(
            &mut state,
            Some(envelope),
            request.idempotency_key,
            digest,
            IdempotencyOutcome::Append(result.clone()),
        );
        Ok(result)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        if request.source == request.destination {
            return Err(StreamError::InvalidArgument);
        }
        validate_fork_size(&request)?;
        let digest = fork_digest(&request);
        let mut state = self.state.write().await;
        if let Some(result) = replay_fork(&state, request.idempotency_key.as_ref(), digest)? {
            return Ok(result);
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        let forked_at = crate::preparation::fork_cut(
            &request,
            state.paths.get(&request.source).map(|stream| stream.tail),
            state
                .paths
                .get(&request.destination)
                .map(|stream| stream.tail),
        )?;
        let source = state
            .paths
            .get(&request.source)
            .ok_or(StreamError::NotFound)?;
        let source_history = source.history.clone();
        reserve_paths(&state, &request.destination, self.limits)?;
        reserve_commit(&state, self.limits)?;
        let commit_id = next_commit_id(&mut state, digest)?;
        ensure_path(&mut state, &request.destination, commit_id);
        let destination = state
            .paths
            .get_mut(&request.destination)
            .ok_or(StreamError::Unavailable)?;
        destination.history = Some(Arc::new(History::Prefix {
            source: source_history,
            tail: forked_at,
            _allocation: MemoryAllocation::default(),
        }));
        destination.tail = forked_at;
        destination.changed.send_replace(forked_at);
        let receipt = ForkReceipt {
            source: request.source,
            destination: request.destination,
            forked_at,
            tail: forked_at,
            commit_id,
        };
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Fork(CommittedFork {
                source: receipt.source.clone(),
                destination: receipt.destination.clone(),
                forked_at,
                tail: forked_at,
                records: Vec::new(),
            })],
        };
        retain(
            &mut state,
            Some(envelope),
            request.idempotency_key,
            digest,
            IdempotencyOutcome::Fork(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        validate_limit(request.limit)?;
        let result = self.read_inner(request, &MemoryObserver::default()).await;
        match result {
            Ok(records) => {
                Ok(stream::iter(record_iterator(records)).map(|record| {
                    match record {
                        Ok(record) => Ok(record),
                        Err(_) => unreachable!("ordinary reads have no fallible observer"),
                    }
                }).boxed())
            }
            Err(MemoryFailure::Provider(error)) => Err(error),
            Err(MemoryFailure::Observer(_)) => {
                unreachable!("ordinary reads have no fallible observer")
            }
        }
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let (receiver, cursor) = {
            let state = self.state.read().await;
            let stream = state.paths.get(&path).ok_or(StreamError::NotFound)?;
            if from > stream.tail {
                return Err(StreamError::OutOfRange);
            }
            (
                stream.changed.subscribe(),
                HistoryCursor::new(stream.history.as_ref(), stream.tail, from),
            )
        };
        let provider = self.clone();
        Ok(stream::unfold(
            (provider, path, receiver, cursor),
            |(provider, path, mut receiver, mut cursor)| async move {
                loop {
                    let record = provider.next_follow_record(&path, &mut cursor).await;
                    let Some(record) = (match record {
                        Ok(record) => record,
                        Err(error) => {
                            return Some((Err(error), (provider, path, receiver, cursor)));
                        }
                    }) else {
                        if receiver.changed().await.is_err() {
                            return Some((
                                Err(StreamError::Unavailable),
                                (provider, path, receiver, cursor),
                            ));
                        }
                        continue;
                    };
                    return Some((Ok(record), (provider, path, receiver, cursor)));
                }
            },
        )
        .boxed())
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        validate_limit(request.limit)?;
        let state = self.state.read().await;
        let limit = usize::try_from(request.limit).map_err(|_| StreamError::LimitExceeded)?;
        let children = state
            .paths
            .keys()
            .filter(|path| {
                request.parent.as_ref().map_or_else(
                    || !path.as_str().contains('/'),
                    |parent| is_direct_child(parent, path),
                )
            })
            .take(limit)
            .cloned()
            .map(|path| Child { path })
            .collect::<Vec<_>>();
        Ok(stream::iter(children.into_iter().map(Ok)).boxed())
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        validate_limit(request.limit)?;
        if request.after.is_some() && request.hierarchy_version.is_none() {
            return Err(StreamError::InvalidArgument);
        }
        if request.after.as_ref().is_some_and(|after| {
            !request.parent.as_ref().map_or_else(
                || !after.as_str().contains('/'),
                |parent| is_direct_child(parent, after),
            )
        }) {
            return Err(StreamError::InvalidArgument);
        }
        let state = self.state.read().await;
        if request
            .hierarchy_version
            .is_some_and(|version| version != state.hierarchy_version)
        {
            return Err(StreamError::HierarchyChanged);
        }
        let start = request.after.as_ref().or(request.parent.as_ref());
        let candidates: Box<dyn Iterator<Item = &StreamPath> + '_> = match start {
            Some(start) => Box::new(
                state
                    .paths
                    .range((Bound::Excluded(start.clone()), Bound::Unbounded))
                    .map(|(path, _)| path),
            ),
            None => Box::new(state.paths.keys()),
        };
        let parent_prefix = request
            .parent
            .as_ref()
            .map(|parent| format!("{}/", parent.as_str()));
        let mut paths = candidates
            .take_while(|path| {
                parent_prefix
                    .as_ref()
                    .is_none_or(|prefix| path.as_str().starts_with(prefix))
            })
            .filter(|path| {
                request.parent.as_ref().map_or_else(
                    || !path.as_str().contains('/'),
                    |parent| is_direct_child(parent, path),
                )
            })
            .take(request.limit as usize + 1)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = paths.len() > request.limit as usize;
        paths.truncate(request.limit as usize);
        let next_after = if has_more {
            paths.last().cloned()
        } else {
            None
        };
        Ok(ChildrenPage {
            hierarchy_version: state.hierarchy_version,
            children: paths.into_iter().map(|path| Child { path }).collect(),
            next_after,
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.commit_inner(request, None).await
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_inner(request, Some(deadline_unix_millis)).await
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        ordinary(self.read_commit_inner(commit_id, &MemoryObserver::default()).await)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            native(self.inspect_inner(key, &observer).await, &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            native(self.bounds_inner(path, &observer).await.map(|output| MemoryOutput {
                value: output.value.tail,
                allocation: output.allocation,
            }), &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, StreamBounds> {
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            native(self.bounds_inner(path, &observer).await, &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            if let Err(error) = validate_limit(request.limit) { return Ok(Err(error)); }
            let allocation = observer.allocate(std::mem::size_of::<NativeMemoryRecordCursor>())?;
            // Allocate the real control box now, not after a denied read or
            // cancellation. Completion only moves already-owned fields.
            let mut cursor = MemoryOutput {
                value: Box::pin(NativeMemoryRecordCursor {
                    records: MemoryRecordIterator {
                        records: Vec::new().into_iter(),
                        terminal: None,
                        _allocation: MemoryAllocation::default(),
                    },
                    meter: Arc::clone(&meter),
                }),
                allocation,
            };
            let result = self.read_inner(request, &observer).await.map(|records| {
                cursor.value.as_mut().get_mut().records = record_iterator(records);
                MemoryOutput {
                    value: cursor.value as NativeRecordStream,
                    allocation: cursor.allocation,
                }
            });
            native(result, &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, self.native_commit_inner(request, None, meter))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, self.native_commit_inner(request, Some(deadline_unix_millis), meter))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_read_commit(
        &self,
        commit_id: CommitId,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommittedEnvelope> {
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            native(self.read_commit_inner(commit_id, &observer).await, &meter)
        })
    }
}

pub(crate) fn validate_limit(limit: u32) -> Result<(), StreamError> {
    if limit == 0 || usize::try_from(limit).map_or(true, |limit| limit > MAX_ITEMS) {
        Err(StreamError::LimitExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_records(records: &[Bytes]) -> Result<(), StreamError> {
    provider_result(validate_records_with_observer(records, &MemoryObserver::default()))
}

fn validate_records_with_observer(records: &[Bytes], observer: &MemoryObserver) -> MemoryResult<()> {
    if records.is_empty() || records.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded.into());
    }
    for record in records {
        observer.admit_item()?;
        let oversized = record.len() > MAX_RECORD_BYTES;
        observer.record_item()?;
        if oversized { return Err(StreamError::LimitExceeded.into()); }
    }
    Ok(())
}

pub(crate) fn validate_append_size(request: &AppendRequest) -> Result<(), StreamError> {
    let wire = crate::wire::AppendRequest {
        path: request.path.to_string(),
        records: request.records.clone(),
        if_tail: request.if_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    };
    (wire.encoded_len() <= MAX_COMMAND_BYTES)
        .then_some(())
        .ok_or(StreamError::LimitExceeded)
}

pub(crate) fn validate_fork_size(request: &ForkRequest) -> Result<(), StreamError> {
    let wire = crate::wire::ForkRequest {
        source: request.source.to_string(),
        destination: request.destination.to_string(),
        at_tail: request.at_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    };
    (wire.encoded_len() <= MAX_COMMAND_BYTES)
        .then_some(())
        .ok_or(StreamError::LimitExceeded)
}

fn missing_paths(state: &State, path: &StreamPath) -> usize {
    let mut count = 0;
    let mut current = Some(path.clone());
    while let Some(path) = current {
        count += usize::from(!state.paths.contains_key(&path));
        current = path.parent();
    }
    count
}

fn reserve_paths(
    state: &State,
    path: &StreamPath,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    let missing_path_bytes = missing_path_bytes(state, path);
    if state.paths.len().saturating_add(missing_paths(state, path)) > limits.paths
        || state.path_bytes.saturating_add(missing_path_bytes) > limits.path_bytes
    {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}

fn missing_path_bytes(state: &State, path: &StreamPath) -> usize {
    let mut bytes = 0_usize;
    let mut current = Some(path.clone());
    while let Some(path) = current {
        if !state.paths.contains_key(&path) {
            bytes = bytes.saturating_add(path.as_str().len());
        }
        current = path.parent();
    }
    bytes
}

fn reserve_commit(state: &State, limits: MemoryLimits) -> Result<(), StreamError> {
    if state.commits.len() >= limits.commits {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}

fn reserve_append(
    state: &State,
    path: &StreamPath,
    records: &[Bytes],
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    reserve_paths(state, path, limits)?;
    reserve_commit(state, limits)?;
    let bytes = records.iter().map(Bytes::len).sum::<usize>();
    if state.record_count.saturating_add(records.len()) > limits.records
        || state.payload_bytes.saturating_add(bytes) > limits.payload_bytes
    {
        return Err(StreamError::Capacity);
    }
    Ok(())
}

fn ensure_path(state: &mut State, path: &StreamPath, commit_id: CommitId) {
    let mut missing = Vec::new();
    let mut current = Some(path.clone());
    while let Some(path) = current {
        if state.paths.contains_key(&path) {
            break;
        }
        current = path.parent();
        missing.push(path);
    }
    for path in missing.into_iter().rev() {
        let (changed, _) = watch::channel(0);
        state.path_bytes = state.path_bytes.saturating_add(path.as_str().len());
        state.paths.insert(
            path,
            PathState {
                history: None,
                tail: 0,
                changed,
                _allocation: MemoryAllocation::default(),
            },
        );
        state.hierarchy_version = commit_id;
    }
}

struct HistoryWindow {
    records: Arc<MemoryRecords>,
    start: usize,
    end: usize,
}

/// Incremental cursor over the immutable history graph used by `follow`.
///
/// A follow can live for a long time while one-record batches are appended.
/// Replaying the linked graph from its root for every item makes that workload
/// quadratic.  The cursor indexes the initial graph once and then only walks
/// the newly added batch suffix after each publication.
pub(crate) struct HistoryCursor {
    root: Option<Arc<History>>,
    windows: Vec<HistoryWindow>,
    window: usize,
    record: usize,
    next: u64,
}

impl HistoryCursor {
    pub(crate) fn at(next: u64) -> Self {
        Self {
            root: None,
            windows: Vec::new(),
            window: 0,
            record: 0,
            next,
        }
    }

    fn new(history: Option<&Arc<History>>, tail: u64, next: u64) -> Self {
        let mut cursor = Self::at(next);
        cursor.rebuild(history, tail);
        cursor
    }

    fn rebuild(&mut self, root: Option<&Arc<History>>, tail: u64) {
        self.root = root.cloned();
        self.windows = collect_history_windows(root, tail);
        self.window = 0;
        self.record = 0;
        self.seek_next();
    }

    fn sync(&mut self, history: Option<&Arc<History>>, tail: u64) {
        let Some(root) = history else {
            self.rebuild(None, tail);
            return;
        };
        if self.root.as_ref().is_some_and(|old| Arc::ptr_eq(old, root)) {
            return;
        }

        // Appends form a new Batch chain whose parent is the previous root.
        // Walk that suffix once, then attach its windows in chronological order.
        // A Prefix in a changed suffix can alter visibility of old batches, so
        // use the bounded full rebuild for that uncommon graph transition.
        let mut cursor = Some(Arc::clone(root));
        let mut suffix = Vec::new();
        let mut reaches_old = false;
        while let Some(node) = cursor {
            if self
                .root
                .as_ref()
                .is_some_and(|old| Arc::ptr_eq(old, &node))
            {
                reaches_old = true;
                break;
            }
            match node.as_ref() {
                History::Batch { parent, records, .. } => {
                    suffix.push(Arc::clone(records));
                    cursor = parent.clone();
                }
                History::Prefix { .. } => break,
            }
        }
        if !reaches_old {
            self.rebuild(Some(root), tail);
            return;
        }

        suffix.reverse();
        let mut added = suffix
            .into_iter()
            .filter_map(|records| history_window(records, tail))
            .collect::<Vec<_>>();
        self.windows.append(&mut added);
        self.root = Some(Arc::clone(root));
    }

    fn seek_next(&mut self) {
        while let Some(window) = self.windows.get(self.window) {
            while self.record < window.end.saturating_sub(window.start) {
                let index = window.start + self.record;
                let Some(record) = window.records.get(index) else {
                    break;
                };
                if record.sequence >= self.next {
                    return;
                }
                self.record += 1;
            }
            self.window += 1;
            self.record = 0;
        }
    }

    fn next_record(&mut self) -> Option<Record> {
        self.seek_next();
        let window = self.windows.get(self.window)?;
        let index = window.start + self.record;
        let record = window.records.get(index)?.clone();
        self.record += 1;
        self.next = record.sequence.checked_add(1)?;
        Some(record)
    }
}

fn collect_history_windows(history: Option<&Arc<History>>, tail: u64) -> Vec<HistoryWindow> {
    let mut cursor = history.cloned();
    let mut ceiling = tail;
    let mut batches = Vec::new();
    while let Some(history) = cursor {
        match history.as_ref() {
            History::Batch { parent, records, .. } => {
                batches.push((Arc::clone(records), ceiling));
                cursor = parent.clone();
            }
            History::Prefix { source, tail, .. } => {
                ceiling = ceiling.min(*tail);
                cursor = source.clone();
            }
        }
    }
    batches.reverse();
    batches
        .into_iter()
        .filter_map(|(records, ceiling)| history_window(records, ceiling))
        .collect()
}

fn history_window(records: Arc<MemoryRecords>, ceiling: u64) -> Option<HistoryWindow> {
    let first = records.first()?.sequence;
    let batch_end = records
        .len()
        .try_into()
        .ok()
        .and_then(|length: u64| first.checked_add(length))
        .unwrap_or(u64::MAX);
    let end_sequence = ceiling.min(batch_end);
    if end_sequence <= first {
        return None;
    }
    let end = usize::try_from(end_sequence - first).ok()?;
    Some(HistoryWindow {
        records,
        start: 0,
        end,
    })
}

struct ReadHistoryOutput {
    records: MemoryOutput<Vec<Record>>,
    terminal: Option<std::io::Error>,
}

fn read_history(
    history: Option<&Arc<History>>,
    tail: u64,
    from: u64,
    limit: usize,
    observer: &MemoryObserver,
) -> MemoryResult<ReadHistoryOutput> {
    // A nonempty slice contributes at least one requested record, so the
    // validated request limit bounds this stack index without a helper Vec.
    let end = tail.min(from.saturating_add(limit as u64));
    let capacity = usize::try_from(end.saturating_sub(from))
        .map_err(|_| StreamError::LimitExceeded)?;
    let mut output = observer.vec(capacity)?;
    let mut cursor = history;
    let mut ceiling = tail;
    let mut windows: [Option<&[Record]>; MAX_ITEMS] = [None; MAX_ITEMS];
    let mut count = 0;
    while ceiling > from && let Some(node) = cursor {
        observer.admit_item()?;
        match node.as_ref() {
            History::Batch { parent, records, .. } => {
                if let Some(first) = records.first().map(|record| record.sequence) {
                    let offset = |sequence: u64| {
                        usize::try_from(sequence.saturating_sub(first))
                            .map_or(records.len(), |offset| offset.min(records.len()))
                    };
                    let start = offset(from);
                    if let Some(slice) = records.get(start..offset(ceiling.min(end)).max(start))
                        && !slice.is_empty()
                    {
                        let slot = windows.get_mut(count).ok_or(StreamError::Unavailable)?;
                        *slot = Some(slice);
                        count += 1;
                    }
                    ceiling = ceiling.min(first);
                }
                cursor = parent.as_ref();
            }
            History::Prefix { source, tail, .. } => {
                ceiling = ceiling.min(*tail);
                cursor = source.as_ref();
            }
        }
        observer.record_item()?;
    }
    let mut terminal = None;
    for window in windows.iter().take(count).rev().flatten() {
        for record in *window {
            if let Err(error) = observer.admit_item() {
                terminal = Some(error);
                break;
            }
            let mut read = match observer.read_bytes(record.value.len()) {
                Ok(read) => read,
                Err(error) => { terminal = Some(error); break; }
            };
            // Clone only immutable aliases, never a guessed body capacity/copy.
            output.value.push(record.clone());
            let read = read.record(record.value.len());
            let examined = observer.record_item();
            if let Err(error) = read.and(examined).and_then(|()| observer.check()) {
                terminal = Some(error);
                break;
            }
        }
        if terminal.is_some() { break; }
    }
    Ok(ReadHistoryOutput { records: output, terminal })
}

fn is_direct_child(parent: &StreamPath, candidate: &StreamPath) -> bool {
    candidate.parent().as_ref() == Some(parent)
}

/// Counts exactly the records and payload bytes the live paths retain,
/// each shared history once.
fn recount(state: &mut State) {
    recount_observed(state, &MemoryObserver::default())
        .expect("ordinary recount has no fallible observer");
}

fn recount_observed(state: &mut State, observer: &MemoryObserver) -> MemoryResult<()> {
    let mut seen = MemoryOutput {
        value: HashSet::new(),
        allocation: observer.allocate(0)?,
    };
    let mut records = 0_usize;
    let mut bytes = 0_usize;
    for path in state.paths.values() {
        observer.admit_item()?;
        let mut cursor = path.history.as_ref();
        observer.record_item()?;
        while let Some(node) = cursor {
            observer.admit_item()?;
            let pointer = Arc::as_ptr(node);
            if seen.value.contains(&pointer) {
                observer.record_item()?;
                break;
            }
            seen.allocation.absorb(observer.adopt(std::mem::size_of::<*const History>())?)?;
            seen.value.insert(pointer);
            cursor = match node.as_ref() {
                History::Batch { parent, records: batch, .. } => {
                    records = records.saturating_add(batch.len());
                    for record in batch.iter() {
                        observer.admit_item()?;
                        bytes = bytes.saturating_add(record.value.len());
                        observer.record_item()?;
                    }
                    parent.as_ref()
                }
                History::Prefix { source, .. } => source.as_ref(),
            };
            observer.record_item()?;
        }
    }
    state.record_count = records;
    state.payload_bytes = bytes;
    Ok(())
}

fn next_commit_id(state: &mut State, digest: [u8; 32]) -> Result<CommitId, StreamError> {
    state.decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
    Ok(planned_commit_id(state.decision, digest, &MemoryObserver::default())
        .expect("ordinary hashing has no fallible observer"))
}

fn planned_commit_id(
    decision: u64,
    digest: [u8; 32],
    observer: &MemoryObserver,
) -> std::io::Result<CommitId> {
    let mut hash = Sha256::new();
    hash_update_observed(&mut hash, b"acyclic.stream.commit.v1\0", observer)?;
    hash_update_observed(&mut hash, &decision.to_le_bytes(), observer)?;
    hash_update_observed(&mut hash, &digest, observer)?;
    Ok(CommitId::from_bytes(hash.finalize().into()))
}

fn replay(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<IdempotencyOutcome>, StreamError> {
    let Some(key) = key else { return Ok(None) };
    let Some(replay) = state.replays.get(key.as_bytes()) else {
        return Ok(None);
    };
    if replay.digest != digest {
        return Err(StreamError::IdempotencyMismatch);
    }
    Ok(Some(replay.result.clone()))
}

fn replay_append(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<AppendOutcome>, StreamError> {
    match replay(state, key, digest)? {
        Some(IdempotencyOutcome::Append(result)) => Ok(Some(result)),
        Some(_) => Err(StreamError::IdempotencyMismatch),
        None => Ok(None),
    }
}

fn replay_fork(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<ForkReceipt>, StreamError> {
    match replay(state, key, digest)? {
        Some(IdempotencyOutcome::Fork(result)) => Ok(Some(result)),
        Some(_) => Err(StreamError::IdempotencyMismatch),
        None => Ok(None),
    }
}


fn admit_replay(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    if key.is_some() && state.replays.len() >= limits.idempotency_results {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}

/// Retains a mutation envelope and its replay result indefinitely.
fn retain(
    state: &mut State,
    envelope: Option<CommittedEnvelope>,
    key: Option<crate::IdempotencyKey>,
    digest: [u8; 32],
    result: IdempotencyOutcome,
) {
    let commit = envelope.map(|envelope| {
        let commit_id = envelope.commit_id;
        state.commits.insert(commit_id, envelope);
        commit_id
    });
    let replay = key.map(|key| {
        let key = Bytes::copy_from_slice(key.as_bytes());
        state.replays.insert(key.clone(), Replay {
            digest, result, _allocation: MemoryAllocation::default(),
        });
        key
    });
    if commit.is_some() || replay.is_some() {
        state.retained.push_back(Retained { replay, commit });
    }
}

pub(crate) fn append_digest(request: &AppendRequest) -> [u8; 32] {
    let mut hash = request_hasher(b"append");
    hash_path(&mut hash, &request.path);
    hash.update(request.if_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash_records(&mut hash, &request.records);
    hash.finalize().into()
}

pub(crate) fn fork_digest(request: &ForkRequest) -> [u8; 32] {
    let mut hash = request_hasher(b"fork");
    hash_path(&mut hash, &request.source);
    hash_path(&mut hash, &request.destination);
    hash.update(request.at_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash.finalize().into()
}

pub(crate) fn commit_digest(request: &CommitRequest) -> [u8; 32] {
    commit_digest_with_observer(request, &MemoryObserver::default())
        .expect("ordinary hashing has no fallible observer")
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn commit_digest_observed(
    request: &CommitRequest,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<[u8; 32]> {
    commit_digest_with_observer(request, &MemoryObserver { meter: meter.cloned() })
}

fn hash_update_observed(
    hash: &mut Sha256,
    bytes: &[u8],
    observer: &MemoryObserver,
) -> std::io::Result<()> {
    observer.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = &observer.meter { meter.admit_codec(0, 0, bytes.len() as u64)?; }
    hash.update(bytes);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = &observer.meter { meter.record_codec(0, 0, bytes.len() as u64)?; }
    Ok(())
}

fn hash_path_observed(hash: &mut Sha256, path: &StreamPath, observer: &MemoryObserver) -> std::io::Result<()> {
    hash_update_observed(hash, &(path.as_str().len() as u64).to_le_bytes(), observer)?;
    hash_update_observed(hash, path.as_str().as_bytes(), observer)
}

fn hash_records_observed(hash: &mut Sha256, records: &[Bytes], observer: &MemoryObserver) -> std::io::Result<()> {
    hash_update_observed(hash, &(records.len() as u64).to_le_bytes(), observer)?;
    for record in records {
        observer.admit_item()?;
        hash_update_observed(hash, &(record.len() as u64).to_le_bytes(), observer)?;
        hash_update_observed(hash, record, observer)?;
        observer.record_item()?;
    }
    Ok(())
}

fn commit_digest_with_observer(request: &CommitRequest, observer: &MemoryObserver) -> std::io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash_update_observed(&mut hash, b"acyclic.stream.request.v1\0", observer)?;
    hash_update_observed(&mut hash, b"commit", observer)?;
    for condition in &request.conditions {
        observer.admit_item()?;
        match condition {
            CommitCondition::Tail { path, expected } => {
                hash_update_observed(&mut hash, &[1], observer)?;
                hash_path_observed(&mut hash, path, observer)?;
                hash_update_observed(&mut hash, &expected.to_le_bytes(), observer)?;
            }
            CommitCondition::Absent { path } => {
                hash_update_observed(&mut hash, &[2], observer)?;
                hash_path_observed(&mut hash, path, observer)?;
            }
        }
        observer.record_item()?;
    }
    for mutation in &request.mutations {
        observer.admit_item()?;
        match mutation {
            CommitMutation::Append { path, records } => {
                hash_update_observed(&mut hash, &[1], observer)?;
                hash_path_observed(&mut hash, path, observer)?;
                hash_records_observed(&mut hash, records, observer)?;
            }
            CommitMutation::Fork { source, destination, at_tail, records } => {
                hash_update_observed(&mut hash, &[2], observer)?;
                hash_path_observed(&mut hash, source, observer)?;
                hash_path_observed(&mut hash, destination, observer)?;
                hash_update_observed(&mut hash, &at_tail.to_le_bytes(), observer)?;
                hash_records_observed(&mut hash, records, observer)?;
            }
        }
        observer.record_item()?;
    }
    Ok(hash.finalize().into())
}

fn request_hasher(kind: &[u8]) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.stream.request.v1\0");
    hash.update(kind);
    hash
}

fn hash_path(hash: &mut Sha256, path: &StreamPath) {
    hash.update((path.as_str().len() as u64).to_le_bytes());
    hash.update(path.as_str().as_bytes());
}

fn hash_records(hash: &mut Sha256, records: &[Bytes]) {
    hash.update((records.len() as u64).to_le_bytes());
    for record in records {
        hash.update((record.len() as u64).to_le_bytes());
        hash.update(record);
    }
}

fn condition_path(condition: &CommitCondition) -> &StreamPath {
    match condition {
        CommitCondition::Tail { path, .. } | CommitCondition::Absent { path } => path,
    }
}

fn mutation_path(mutation: &CommitMutation) -> &StreamPath {
    match mutation {
        CommitMutation::Fork { destination, .. } => destination,
        CommitMutation::Append { path, .. } => path,
    }
}

pub(crate) fn normalize_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    provider_result(normalize_commit_with_observer(request, &MemoryObserver::default()))
}

fn normalize_commit_with_observer(request: &mut CommitRequest, observer: &MemoryObserver) -> MemoryResult<()> {
    if request.conditions.is_empty()
        || request.conditions.len() > MAX_ITEMS
        || request.mutations.is_empty()
        || request.mutations.len() > MAX_ITEMS
    {
        return Err(StreamError::LimitExceeded.into());
    }
    for mutation in &request.mutations {
        observer.admit_item()?;
        match mutation {
            CommitMutation::Append { records, .. } => validate_records_with_observer(records, observer)?,
            CommitMutation::Fork { records, .. } if !records.is_empty() => {
                validate_records_with_observer(records, observer)?;
            }
            CommitMutation::Fork { .. } => {}
        }
        observer.record_item()?;
    }
    validate_commit_size(request, observer)?;
    request.conditions.sort_unstable_by(|left, right| {
        condition_path(left)
            .cmp(condition_path(right))
            .then_with(|| {
                matches!(left, CommitCondition::Absent { .. })
                    .cmp(&matches!(right, CommitCondition::Absent { .. }))
            })
    });
    request
        .mutations
        .sort_unstable_by(|left, right| mutation_path(left).cmp(mutation_path(right)));
    for pair in request.conditions.windows(2) {
        observer.admit_item()?;
        let duplicate = matches!(pair, [left, right] if condition_path(left) == condition_path(right));
        observer.record_item()?;
        if duplicate { return Err(StreamError::InvalidArgument.into()); }
    }
    for pair in request.mutations.windows(2) {
        observer.admit_item()?;
        let duplicate = matches!(pair, [left, right] if mutation_path(left) == mutation_path(right));
        observer.record_item()?;
        if duplicate { return Err(StreamError::InvalidArgument.into()); }
    }
    Ok(())
}

fn validate_commit_size(request: &CommitRequest, observer: &MemoryObserver) -> MemoryResult<()> {
    #[cfg(not(target_arch = "wasm32"))]
    let length = commit_encoded_len_observed(request, observer.meter.as_ref())?;
    #[cfg(target_arch = "wasm32")]
    let length = commit_encoded_len_with_observer(request, observer)?;
    if length > MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded.into());
    }
    Ok(())
}

#[cfg(any(feature = "local", test))]
pub(crate) fn commit_encoded_len(request: &CommitRequest) -> usize {
    commit_encoded_len_with_observer(request, &MemoryObserver::default())
        .expect("ordinary size inspection has no fallible observer")
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn commit_encoded_len_observed(
    request: &CommitRequest,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<usize> {
    commit_encoded_len_with_observer(request, &MemoryObserver { meter: meter.cloned() })
}

fn commit_encoded_len_with_observer(request: &CommitRequest, observer: &MemoryObserver) -> std::io::Result<usize> {
    // Exact canonical protobuf sizing is metadata inspection, not encoding or
    // a payload copy. The same allocation-free body serves every caller.
    let mut length = wire_bytes_len(request.idempotency_key.as_bytes().len());
    for condition in &request.conditions {
        observer.admit_item()?;
        let body = match condition {
            CommitCondition::Tail { path, expected } => {
                wire_bytes_len(path.as_str().len()) + wire_uint_len(*expected)
            }
            CommitCondition::Absent { path } => wire_bytes_len(path.as_str().len()),
        };
        length += wire_bytes_len(wire_bytes_len(body));
        observer.record_item()?;
    }
    for mutation in &request.mutations {
        observer.admit_item()?;
        let (mut body, records) = match mutation {
            CommitMutation::Append { path, records } => (wire_bytes_len(path.as_str().len()), records),
            CommitMutation::Fork { source, destination, at_tail, records } => (
                wire_bytes_len(source.as_str().len()) + wire_bytes_len(destination.as_str().len()) + wire_uint_len(*at_tail),
                records,
            ),
        };
        for record in records {
            observer.admit_item()?;
            body += wire_bytes_len(record.len());
            observer.record_item()?;
        }
        length += wire_bytes_len(wire_bytes_len(body));
        observer.record_item()?;
    }
    Ok(length)
}

fn wire_varint_len(value: u64) -> usize {
    (64 - value.leading_zeros()).max(1).div_ceil(7) as usize
}

fn wire_bytes_len(length: usize) -> usize {
    1 + wire_varint_len(length as u64) + length
}

fn wire_uint_len(value: u64) -> usize {
    if value == 0 { 0 } else { 1 + wire_varint_len(value) }
}

pub(crate) fn validate_commit_shape(request: &CommitRequest) -> Result<(), StreamError> {
    provider_result(validate_commit_shape_with_observer(request, &MemoryObserver::default()))
}

fn validate_commit_shape_with_observer(request: &CommitRequest, observer: &MemoryObserver) -> MemoryResult<()> {
    // Every caller normalizes first, so a binary search avoids a temporary tree.
    let condition = |path: &StreamPath| {
        request.conditions
            .binary_search_by(|condition| condition_path(condition).cmp(path))
            .ok()
            .and_then(|index| request.conditions.get(index))
    };
    for mutation in &request.mutations {
        observer.admit_item()?;
        match mutation {
            CommitMutation::Append { path, .. } => match condition(path) {
                Some(CommitCondition::Tail { .. } | CommitCondition::Absent { .. }) => {}
                _ => return Err(StreamError::InvalidArgument.into()),
            },
            CommitMutation::Fork { destination, .. } => {
                if !matches!(
                    condition(destination),
                    Some(CommitCondition::Absent { .. })
                ) {
                    return Err(StreamError::InvalidArgument.into());
                }
            }
        }
        observer.record_item()?;
    }
    Ok(())
}

fn validate_commit_authority(
    state: &State,
    request: &CommitRequest,
    observer: &MemoryObserver,
) -> MemoryResult<()> {
    crate::preparation::authority_with_observer(request, |path| {
        state.paths.get(path).map(|stream| stream.tail)
    }, observer)
}

fn commit_conflicts(
    state: &State,
    conditions: &[CommitCondition],
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<Vec<CommitConflict>>> {
    crate::preparation::conditions_with_observer(conditions, |path| {
        state.paths.get(path).map(|stream| stream.tail)
    }, observer)
}

fn missing_commit_paths<'a>(
    state: &State,
    request: &'a CommitRequest,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<BTreeSet<&'a str>>> {
    let mut output = MemoryOutput {
        value: BTreeSet::new(),
        allocation: observer.allocate(0)?,
    };
    for mutation in &request.mutations {
        let mut current = Some(mutation_path(mutation).as_str());
        while let Some(path) = current {
            observer.admit_item()?;
            if !state.paths.contains_key(path) && !output.value.contains(path) {
                // Declared logical entry quota, not an invented std BTree node.
                output.allocation.absorb(observer.adopt(std::mem::size_of::<&str>())?)?;
                output.value.insert(path);
            }
            observer.record_item()?;
            current = path.rsplit_once('/').map(|(parent, _)| parent);
        }
    }
    Ok(output)
}

fn reserve_coordinated(
    state: &State,
    request: &CommitRequest,
    paths: &BTreeSet<&str>,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    reserve_commit(state, limits)?;
    let (records, bytes) = request.mutations.iter().fold((0_usize, 0_usize), |(count, bytes), mutation| {
        let records = match mutation {
            CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => records,
        };
        (count.saturating_add(records.len()), bytes.saturating_add(records.iter().map(Bytes::len).sum()))
    });
    if state.paths.len().saturating_add(paths.len()) > limits.paths
        || state.path_bytes.saturating_add(paths.iter().map(|path| path.len()).sum()) > limits.path_bytes
        || state.record_count.saturating_add(records) > limits.records
        || state.payload_bytes.saturating_add(bytes) > limits.payload_bytes
    {
        return Err(StreamError::Capacity);
    }
    Ok(())
}

struct PreparedMutation {
    path: StreamPath,
    history: Arc<History>,
    tail: u64,
    records: usize,
    bytes: usize,
}

struct PreparedPublication {
    envelope: Option<CommittedEnvelope>,
    replay: IdempotencyOutcome,
    key: Bytes,
    paths: MemoryOutput<Vec<(StreamPath, PathState)>>,
    mutations: MemoryOutput<Vec<PreparedMutation>>,
    resident: MemoryAllocation,
}

fn prepare_paths(
    paths: &BTreeSet<&str>,
    envelope: &CommittedEnvelope,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<Vec<(StreamPath, PathState)>>> {
    let mut output = observer.vec(paths.len())?;
    for path in paths {
        observer.admit_item()?;
        let mut ancestor = envelope.mutations.iter().map(|mutation| match mutation {
            CommittedMutation::Append(append) => &append.path,
            CommittedMutation::Fork(fork) => &fork.destination,
        }).find(|candidate| {
            candidate.as_str() == *path || candidate.as_str().strip_prefix(*path)
                .is_some_and(|suffix| suffix.starts_with('/'))
        }).ok_or(StreamError::Unavailable)?.clone();
        while ancestor.as_str() != *path {
            observer.admit_item()?;
            ancestor = observer.parent_path(&ancestor)?.ok_or(StreamError::Unavailable)?;
            observer.record_item()?;
        }
        let allocation = observer.adopt(std::mem::size_of::<(StreamPath, PathState)>())?;
        let (changed, _) = watch::channel(0);
        output.value.push((ancestor, PathState { history: None, tail: 0, changed, _allocation: allocation }));
        observer.record_item()?;
    }
    Ok(output)
}


fn prepare_envelope(
    state: &State,
    request: &CommitRequest,
    commit_id: CommitId,
    committed_at_micros: u64,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<CommittedEnvelope>> {
    let output = crate::preparation::mutations_with_observer(
        &request.mutations,
        |path| state.paths.get(path).map(|stream| stream.tail),
        commit_id,
        committed_at_micros,
        observer,
    )?;
    Ok(MemoryOutput {
        value: CommittedEnvelope { commit_id, mutations: output.value },
        allocation: output.allocation,
    })
}

fn batch_history(
    parent: Option<Arc<History>>,
    records: &[Record],
    observer: &MemoryObserver,
) -> MemoryResult<Arc<History>> {
    let mut records = clone_records(records, observer, false)?;
    records.allocation.absorb(observer.allocate(std::mem::size_of::<MemoryRecords>())?)?;
    let records = Arc::new(MemoryRecords {
        records: records.value,
        _allocation: records.allocation,
    });
    let allocation = observer.allocate(std::mem::size_of::<History>())?;
    Ok(Arc::new(History::Batch { parent, records, _allocation: allocation }))
}

fn prepare_mutations(
    state: &State,
    envelope: &CommittedEnvelope,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<Vec<PreparedMutation>>> {
    let mut output = observer.vec(envelope.mutations.len())?;
    for mutation in &envelope.mutations {
        observer.admit_item()?;
        let (path, history, tail, records) = match mutation {
            CommittedMutation::Append(append) => (
                append.path.clone(),
                batch_history(state.paths.get(&append.path).and_then(|path| path.history.clone()), &append.records, observer)?,
                append.tail,
                &append.records,
            ),
            CommittedMutation::Fork(fork) => {
                let source = state.paths.get(&fork.source).ok_or(StreamError::NotFound)?;
                let allocation = observer.allocate(std::mem::size_of::<History>())?;
                let prefix = Arc::new(History::Prefix {
                    source: source.history.clone(), tail: fork.forked_at, _allocation: allocation,
                });
                let history = if fork.records.is_empty() { prefix } else {
                    batch_history(Some(prefix), &fork.records, observer)?
                };
                (fork.destination.clone(), history, fork.tail, &fork.records)
            }
        };
        output.value.push(PreparedMutation {
            path, history, tail, records: records.len(),
            bytes: records.iter().map(|record| record.value.len()).sum(),
        });
        observer.record_item()?;
    }
    Ok(output)
}

fn prepare_retained_capacity(state: &mut State, observer: &MemoryObserver) -> MemoryResult<()> {
    if state.retained.len() < state.retained.capacity() { return Ok(()); }
    let minimum = state.retained.len().checked_add(1).ok_or(StreamError::Capacity)?;
    let capacity = state.retained.capacity().checked_mul(2).ok_or(StreamError::Capacity)?
        .max(minimum).max(4);
    let allocation = observer.allocate(capacity.checked_mul(std::mem::size_of::<Retained>())
        .ok_or(StreamError::Capacity)?)?;
    state.retained.reserve_exact(capacity - state.retained.len());
    // The old buffer was physically retired by reserve_exact. Transfer the new
    // capacity before reconciliation, including a late excess/cancellation.
    state.retained_allocation = allocation;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(allocation) = state.retained_allocation.allocation.as_mut() {
        allocation.reconcile_capacity(state.retained.capacity().checked_mul(std::mem::size_of::<Retained>())
            .ok_or(StreamError::Capacity)?)?;
    }
    Ok(())
}



fn publish(state: &mut State, prepared: PreparedPublication, digest: [u8; 32]) {
    let creates_paths = !prepared.paths.value.is_empty();
    for (path, stream) in prepared.paths.value {
        state.path_bytes = state.path_bytes.saturating_add(path.as_str().len());
        state.paths.insert(path, stream);
    }
    for mutation in prepared.mutations.value {
        let stream = state.paths.get_mut(&mutation.path)
            .expect("all destinations were prepared under this same write lock");
        stream.history = Some(mutation.history);
        stream.tail = mutation.tail;
        stream.changed.send_replace(stream.tail);
        state.record_count += mutation.records;
        state.payload_bytes += mutation.bytes;
    }
    let commit = prepared.envelope.map(|envelope| {
        let commit_id = envelope.commit_id;
        state.commits.insert(commit_id, envelope);
        if creates_paths { state.hierarchy_version = commit_id; }
        commit_id
    });
    let key = prepared.key;
    state.replays.insert(key.clone(), Replay {
        digest, result: prepared.replay, _allocation: prepared.resident,
    });
    state.retained.push_back(Retained { replay: Some(key), commit });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(not(target_arch = "wasm32"))]
    mod native_controls {
        use super::*;
        use std::sync::atomic::AtomicBool;

        struct Meter {
            held: AtomicU64,
            allocations: AtomicU64,
            deny_allocation: u64,
            read: AtomicU64,
            read_limit: u64,
            pending_read: AtomicU64,
            pending_write: AtomicU64,
            read_operations: AtomicU64,
            write_operations: AtomicU64,
            pending_read_operations: AtomicU64,
            pending_write_operations: AtomicU64,
            pending_durability: AtomicU64,
            copied: AtomicU64,
            cancel_after_read: u64,
            late_write_error: bool,
            clock_after_copy: Option<(Arc<TestClock>, u64)>,
            cancelled: AtomicBool,
        }

        impl Default for Meter {
            fn default() -> Self {
                Self {
                    held: AtomicU64::new(0), allocations: AtomicU64::new(0),
                    deny_allocation: u64::MAX, read: AtomicU64::new(0), read_limit: u64::MAX,
                    pending_read: AtomicU64::new(0), pending_write: AtomicU64::new(0),
                    read_operations: AtomicU64::new(0), write_operations: AtomicU64::new(0),
                    pending_read_operations: AtomicU64::new(0), pending_write_operations: AtomicU64::new(0),
                    pending_durability: AtomicU64::new(0), copied: AtomicU64::new(0),
                    cancel_after_read: u64::MAX, late_write_error: false,
                    clock_after_copy: None,
                    cancelled: AtomicBool::new(false),
                }
            }
        }

        impl Meter {
            fn original(self: &Arc<Self>) -> Arc<dyn NativeReadMeter> { self.clone() }
            fn pending_operations(&self, kind: NativeOperationKind) -> &AtomicU64 {
                match kind {
                    NativeOperationKind::Read => &self.pending_read_operations,
                    NativeOperationKind::Write => &self.pending_write_operations,
                    NativeOperationKind::Durability => &self.pending_durability,
                }
            }
            fn pending_bytes(&self, kind: NativeOperationKind) -> &AtomicU64 {
                match kind {
                    NativeOperationKind::Read => &self.pending_read,
                    NativeOperationKind::Write => &self.pending_write,
                    NativeOperationKind::Durability => unreachable!("durability carries no bytes"),
                }
            }
        }

        impl NativeReadMeter for Meter {
            fn check(&self) -> std::io::Result<()> {
                if self.cancelled.load(Ordering::SeqCst) {
                    Err(std::io::ErrorKind::Interrupted.into())
                } else { Ok(()) }
            }
            fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> std::io::Result<()> {
                self.check()?;
                self.pending_operations(kind).fetch_add(count, Ordering::SeqCst);
                Ok(())
            }
            fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
                assert!(self.pending_operations(kind).fetch_sub(count, Ordering::SeqCst) >= count);
            }
            fn record_reserved_operation(&self, kind: NativeOperationKind) -> std::io::Result<()> {
                assert!(self.pending_operations(kind).fetch_sub(1, Ordering::SeqCst) >= 1);
                match kind {
                    NativeOperationKind::Read => { self.read_operations.fetch_add(1, Ordering::SeqCst); }
                    NativeOperationKind::Write => { self.write_operations.fetch_add(1, Ordering::SeqCst); }
                    NativeOperationKind::Durability => {}
                }
                Ok(())
            }
            fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> { self.check() }
            fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> std::io::Result<()> {
                self.check()?;
                let pending = self.pending_bytes(kind);
                pending.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                    let next = held.checked_add(maximum)?;
                    if kind == NativeOperationKind::Read
                        && self.read.load(Ordering::SeqCst).checked_add(next)? > self.read_limit
                    { None } else { Some(next) }
                }).map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
                Ok(())
            }
            fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64) {
                assert!(self.pending_bytes(kind).fetch_sub(maximum, Ordering::SeqCst) >= maximum);
            }
            fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
                self.cancel_reserved_bytes(kind, maximum);
                assert!(actual <= maximum);
                match kind {
                    NativeOperationKind::Read => self.record_read(actual as usize),
                    NativeOperationKind::Write => self.record_write(actual as usize),
                    NativeOperationKind::Durability => unreachable!("durability carries no bytes"),
                }
            }
            fn allocate(&self, bytes: u64) -> std::io::Result<()> {
                self.check()?;
                if self.allocations.fetch_add(1, Ordering::SeqCst) + 1 == self.deny_allocation {
                    return Err(std::io::ErrorKind::OutOfMemory.into());
                }
                self.held.fetch_add(bytes, Ordering::SeqCst);
                Ok(())
            }
            fn allocate_refcount_header(&self) -> std::io::Result<()> { self.allocate(0) }
            fn adopt(&self, bytes: u64) -> std::io::Result<()> {
                self.check()?;
                self.held.fetch_add(bytes, Ordering::SeqCst);
                Ok(())
            }
            fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
                self.held.fetch_add(bytes, Ordering::SeqCst);
                (bytes, self.check())
            }
            fn admit_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> { self.check() }
            fn record_codec(&self, _: u64, copied: u64, _: u64) -> std::io::Result<()> {
                self.copied.fetch_add(copied, Ordering::SeqCst);
                if copied != 0 {
                    if let Some((clock, now)) = &self.clock_after_copy {
                        clock.0.store(*now, Ordering::SeqCst);
                    }
                }
                Ok(())
            }
            fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
                self.admit_examination(items, copied)?;
                self.record_examination(items, copied)
            }
            fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> { self.check() }
            fn record_examination(&self, _: u64, copied: u64) -> std::io::Result<()> {
                self.copied.fetch_add(copied, Ordering::SeqCst);
                Ok(())
            }
            fn release(&self, bytes: u64) {
                assert!(self.held.fetch_sub(bytes, Ordering::SeqCst) >= bytes);
            }
            fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
                self.check()?;
                if self.read.load(Ordering::SeqCst).saturating_add(maximum as u64) > self.read_limit {
                    Err(std::io::ErrorKind::OutOfMemory.into())
                } else { Ok(()) }
            }
            fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
                self.admit_bytes(maximum)?;
                self.read_operations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn admit_operations(&self, _: u64) -> std::io::Result<()> { self.check() }
            fn record_operations(&self, count: u64) -> std::io::Result<()> {
                self.read_operations.fetch_add(count, Ordering::SeqCst);
                Ok(())
            }
            fn record_read(&self, bytes: usize) -> std::io::Result<()> {
                let completed = self.read.fetch_add(bytes as u64, Ordering::SeqCst) + bytes as u64;
                if completed >= self.cancel_after_read { self.cancelled.store(true, Ordering::SeqCst); }
                Ok(())
            }
            fn admit_write(&self, _: usize) -> std::io::Result<()> { self.check() }
            fn record_write(&self, _: usize) -> std::io::Result<()> {
                if self.late_write_error {
                    self.cancelled.store(true, Ordering::SeqCst);
                    Err(std::io::ErrorKind::Interrupted.into())
                } else { Ok(()) }
            }
            fn admit_write_operations(&self, _: u64) -> std::io::Result<()> { self.check() }
            fn record_write_operations(&self, count: u64) -> std::io::Result<()> {
                self.write_operations.fetch_add(count, Ordering::SeqCst);
                Ok(())
            }
            fn admit_durability(&self) -> std::io::Result<()> { self.check() }
        }

        async fn seeded() -> MemoryResult<MemoryStream> {
            let provider = MemoryStream::default();
            provider.append(AppendRequest {
                path: path("native/read")?,
                records: vec![Bytes::from_static(b"aa"), Bytes::from_static(b"bb"), Bytes::from_static(b"cc")],
                if_tail: Some(0), idempotency_key: None,
            }).await?;
            Ok(provider)
        }

        fn read_request() -> Result<ReadRequest, StreamError> {
            Ok(ReadRequest { path: path("native/read")?, from: 0, limit: 3 })
        }

        fn commit_request() -> Result<CommitRequest, StreamError> {
            Ok(CommitRequest {
                conditions: vec![CommitCondition::Absent { path: path("native/new")? }],
                mutations: vec![CommitMutation::Append {
                    path: path("native/new")?, records: vec![Bytes::from_static(b"payload")],
                }],
                idempotency_key: key(b"native-commit")?,
            })
        }

        #[tokio::test]
        async fn allocation_denial_precedes_record_alias_clone_and_box() -> MemoryResult<()> {
            let provider = seeded().await?;
            for denied in [1, 2, 3] {
                let meter = Arc::new(Meter { deny_allocation: denied, ..Meter::default() });
                let result = provider.native_read(read_request()?, meter.original()).await;
                assert!(matches!(result, Err(error) if error.kind() == std::io::ErrorKind::OutOfMemory));
                assert_eq!(meter.allocations.load(Ordering::SeqCst), denied);
                assert_eq!(meter.read.load(Ordering::SeqCst), 0);
                assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
                assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            }
            Ok(())
        }

        #[tokio::test]
        async fn partial_budget_and_cancellation_keep_the_exact_ordered_prefix() -> MemoryResult<()> {
            let provider = seeded().await?;
            for cancellation in [false, true] {
                let meter = Arc::new(Meter {
                    read_limit: if cancellation { u64::MAX } else { 2 },
                    cancel_after_read: if cancellation { 2 } else { u64::MAX },
                    ..Meter::default()
                });
                let mut output = provider.native_read(read_request()?, meter.original()).await??;
                let first = output.value_mut().next().await.ok_or(StreamError::Unavailable)???;
                assert_eq!(first.value().sequence, 0);
                assert_eq!(first.value().value, Bytes::from_static(b"aa"));
                let alias = first.value().value.clone();
                drop(first);
                let error = output.value_mut().next().await.ok_or(StreamError::Unavailable)?;
                assert!(matches!(error, Err(error) if error.kind() == if cancellation {
                    std::io::ErrorKind::Interrupted
                } else { std::io::ErrorKind::OutOfMemory }));
                assert!(output.value_mut().next().await.is_none());
                assert_eq!(meter.read.load(Ordering::SeqCst), 2);
                assert_eq!(meter.read_operations.load(Ordering::SeqCst), 1);
                assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
                assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
                assert!(meter.held.load(Ordering::SeqCst) > 0);
                drop(output);
                assert_eq!(meter.held.load(Ordering::SeqCst), 0);
                assert_eq!(alias, Bytes::from_static(b"aa"));
            }
            Ok(())
        }

        #[tokio::test]
        async fn output_drop_retires_real_vec_and_box_capacity_not_borrowed_bytes() -> MemoryResult<()> {
            let provider = seeded().await?;
            let meter = Arc::new(Meter::default());
            let mut output = provider.native_read(read_request()?, meter.original()).await??;
            let mut aliases = Vec::new();
            while let Some(record) = output.value_mut().next().await {
                let record = record??;
                aliases.push(record.value().value.clone());
            }
            assert_eq!(meter.read.load(Ordering::SeqCst), 6);
            assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
            assert!(meter.held.load(Ordering::SeqCst) >= (3 * std::mem::size_of::<Record>()) as u64);
            drop(output);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            assert_eq!(aliases, vec![Bytes::from_static(b"aa"), Bytes::from_static(b"bb"), Bytes::from_static(b"cc")]);
            Ok(())
        }

        #[tokio::test]
        async fn native_commit_replay_inspection_and_read_commit_share_the_actual_state() -> MemoryResult<()> {
            let provider = MemoryStream::default();
            let request = commit_request()?;
            let meter = Arc::new(Meter::default());
            let committed = provider.native_commit(request.clone(), meter.original()).await??;
            let CommitOutcome::Committed(envelope) = committed.value() else {
                return Err(StreamError::Unavailable.into());
            };
            let id = envelope.commit_id;
            let replay = provider.native_commit(request, meter.original()).await??;
            assert_eq!(replay.value(), committed.value());
            let read = provider.native_read_commit(id, meter.original()).await??;
            assert_eq!(read.value(), envelope);
            let observation = provider.native_inspect_idempotency(key(b"native-commit")?, meter.original()).await??;
            assert_eq!(observation.value().as_ref().map(|value| &value.outcome),
                Some(&IdempotencyOutcome::Commit(committed.value().clone())));
            let tail = provider.native_tail(path("native/new")?, meter.original()).await??;
            let bounds = provider.native_bounds(path("native/new")?, meter.original()).await??;
            assert_eq!(*tail.value(), 1);
            assert_eq!(bounds.value().tail, 1);
            assert_eq!(provider.state.read().await.decision, 1);
            drop((committed, replay, read, observation, tail, bounds));
            drop(provider);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            Ok(())
        }

        #[tokio::test]
        async fn resident_record_arc_keeps_its_actual_capacity_until_the_last_alias() -> MemoryResult<()> {
            let provider = MemoryStream::default();
            let meter = Arc::new(Meter::default());
            let committed = provider.native_commit(commit_request()?, meter.original()).await??;
            let records = {
                let state = provider.state.read().await;
                let history = state.paths.get("native/new").and_then(|path| path.history.as_ref())
                    .ok_or(StreamError::Unavailable)?;
                let History::Batch { records, .. } = history.as_ref() else {
                    return Err(StreamError::Unavailable.into());
                };
                Arc::clone(records)
            };
            let expected = records.records.capacity() * std::mem::size_of::<Record>()
                + std::mem::size_of::<MemoryRecords>();
            drop(committed);
            drop(provider);
            assert_eq!(meter.held.load(Ordering::SeqCst), expected as u64);
            assert_eq!(records.first().map(|record| record.value.clone()), Some(Bytes::from_static(b"payload")));
            drop(records);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            Ok(())
        }

        #[tokio::test]
        async fn denied_commit_never_publishes_and_late_cause_keeps_accepted_outcome() -> MemoryResult<()> {
            for denied in [1, 2, 3] {
                let provider = MemoryStream::default();
                let meter = Arc::new(Meter { deny_allocation: denied, ..Meter::default() });
                let result = provider.native_commit(commit_request()?, meter.original()).await;
                assert!(matches!(result, Err(error) if error.kind() == std::io::ErrorKind::OutOfMemory));
                assert_eq!(provider.tail(path("native/new")?).await, Err(StreamError::NotFound));
                assert!(provider.inspect_idempotency(key(b"native-commit")?).await?.is_none());
                assert_eq!(meter.held.load(Ordering::SeqCst), 0);
                assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
            }
            let provider = MemoryStream::default();
            let meter = Arc::new(Meter { late_write_error: true, ..Meter::default() });
            let completion = provider.native_commit_completion_inner(commit_request()?, None, meter.original()).await??;
            assert!(matches!(completion.error, Some(error) if error.kind() == std::io::ErrorKind::Interrupted));
            assert!(matches!(completion.outcome.value(), CommitOutcome::Committed(_)));
            assert_eq!(provider.tail(path("native/new")?).await?, 1);
            let retained = provider.inspect_idempotency(key(b"native-commit")?).await?
                .ok_or(StreamError::Unavailable)?;
            assert_eq!(retained.outcome, IdempotencyOutcome::Commit(completion.outcome.value().clone()));
            assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
            drop((completion.outcome, retained));
            drop(provider);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            Ok(())
        }

        #[tokio::test]
        async fn borrowed_completion_preserves_actual_input_vectors_until_owner_drop() -> MemoryResult<()> {
            let provider = MemoryStream::default();
            let meter = Arc::new(Meter { late_write_error: true, ..Meter::default() });
            let request = commit_request()?;
            let conditions = request.conditions.as_ptr() as usize;
            let mutations = request.mutations.as_ptr() as usize;
            let bodies = match request.mutations.first().ok_or(StreamError::Unavailable)? {
                CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => records.as_ptr() as usize,
            };
            let mut input_bytes = request.conditions.capacity()
                .checked_mul(std::mem::size_of::<CommitCondition>()).ok_or(StreamError::Capacity)?;
            input_bytes = input_bytes.checked_add(request.mutations.capacity()
                .checked_mul(std::mem::size_of::<CommitMutation>()).ok_or(StreamError::Capacity)?)
                .ok_or(StreamError::Capacity)?;
            for mutation in &request.mutations {
                let records = match mutation {
                    CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => records,
                };
                input_bytes = input_bytes.checked_add(records.capacity()
                    .checked_mul(std::mem::size_of::<Bytes>()).ok_or(StreamError::Capacity)?)
                    .ok_or(StreamError::Capacity)?;
            }
            // The fixture transfers its already allocated, exactly observable
            // caller Vecs; the SDK must neither adopt nor consume them again.
            let observer = MemoryObserver { meter: Some(meter.original()) };
            let mut input = MemoryOutput { value: request, allocation: observer.adopt(input_bytes)? };
            assert_eq!(meter.allocations.load(Ordering::SeqCst), 0);
            let completion = provider.native_commit_completion_borrowed_inner(
                &mut input.value, None, meter.original(),
            ).await??;
            assert!(matches!(completion.error.as_ref(), Some(error)
                if error.kind() == std::io::ErrorKind::Interrupted));
            assert!(matches!(completion.outcome.value(), CommitOutcome::Committed(_)));
            assert_eq!(input.value.conditions.as_ptr() as usize, conditions);
            assert_eq!(input.value.mutations.as_ptr() as usize, mutations);
            let retained_bodies = match input.value.mutations.first().ok_or(StreamError::Unavailable)? {
                CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => records,
            };
            assert_eq!(retained_bodies.as_ptr() as usize, bodies);
            assert_eq!(retained_bodies.len(), 1);
            assert_eq!(provider.tail(path("native/new")?).await?, 1);
            drop(completion);
            let before = meter.held.load(Ordering::SeqCst);
            drop(input);
            assert_eq!(before - meter.held.load(Ordering::SeqCst), input_bytes as u64);
            drop(provider);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            Ok(())
        }

        #[tokio::test]
        async fn inspection_and_envelope_denial_precede_retained_record_clones() -> MemoryResult<()> {
            let provider = MemoryStream::default();
            let committed = provider.commit(commit_request()?).await?;
            let CommitOutcome::Committed(envelope) = committed else {
                return Err(StreamError::Unavailable.into());
            };
            for inspection in [false, true] {
                let meter = Arc::new(Meter { deny_allocation: 2, ..Meter::default() });
                let denied = if inspection {
                    matches!(provider.native_inspect_idempotency(key(b"native-commit")?, meter.original()).await,
                        Err(error) if error.kind() == std::io::ErrorKind::OutOfMemory)
                } else {
                    matches!(provider.native_read_commit(envelope.commit_id, meter.original()).await,
                        Err(error) if error.kind() == std::io::ErrorKind::OutOfMemory)
                };
                assert!(denied);
                assert_eq!(meter.allocations.load(Ordering::SeqCst), 2);
                assert_eq!(meter.read.load(Ordering::SeqCst), 0);
                assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
                assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            }
            assert_eq!(provider.read_commit(envelope.commit_id).await?, envelope);
            Ok(())
        }

        #[tokio::test]
        async fn native_deadline_checks_actual_publication_and_replay_precedes_clock() -> MemoryResult<()> {
            let clock = Arc::new(TestClock::default());
            clock.0.store(100, Ordering::SeqCst);
            let provider = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
            let request = commit_request()?;
            let late = Arc::new(Meter {
                clock_after_copy: Some((clock.clone(), 101)), ..Meter::default()
            });
            assert!(matches!(provider.native_commit_before(request.clone(), 100, late.original()).await?,
                Err(StreamError::DeadlineElapsed)));
            assert!(matches!(provider.native_commit_before(request.clone(), 101, late.original()).await?,
                Err(StreamError::DeadlineElapsed)));
            {
                let state = provider.state.read().await;
                assert_eq!(state.decision, 0);
                assert!(state.paths.is_empty());
                assert!(state.commits.is_empty());
                assert!(state.replays.is_empty());
            }
            assert_eq!(late.pending_write.load(Ordering::SeqCst), 0);
            clock.0.store(100, Ordering::SeqCst);
            let admitted = Arc::new(Meter::default());
            let committed = provider.native_commit_before(request.clone(), 101, admitted.original()).await??;
            assert!(matches!(committed.value(), CommitOutcome::Committed(_)));
            let mut conflict = request.clone();
            conflict.idempotency_key = key(b"native-conflict-deadline")?;
            assert!(matches!(provider.native_commit_before(conflict.clone(), 101, late.original()).await?,
                Err(StreamError::DeadlineElapsed)));
            assert!(provider.inspect_idempotency(conflict.idempotency_key).await?.is_none());
            assert_eq!(provider.state.read().await.decision, 1);
            clock.0.store(1_000, Ordering::SeqCst);
            let replay = provider.native_commit_before(request, 101, admitted.original()).await??;
            assert_eq!(committed.value(), replay.value());
            drop((committed, replay));
            drop(provider);
            assert_eq!(late.held.load(Ordering::SeqCst), 0);
            assert_eq!(admitted.held.load(Ordering::SeqCst), 0);
            Ok(())
        }

        #[tokio::test]
        async fn native_fork_and_append_capture_the_same_precommit_prefix() -> MemoryResult<()> {
            let provider = MemoryStream::default();
            provider.append(AppendRequest {
                path: path("native/source")?, records: vec![Bytes::from_static(b"old")],
                if_tail: Some(0), idempotency_key: None,
            }).await?;
            let request = CommitRequest {
                conditions: vec![
                    CommitCondition::Tail { path: path("native/source")?, expected: 1 },
                    CommitCondition::Absent { path: path("native/fork")? },
                ],
                mutations: vec![
                    CommitMutation::Append { path: path("native/source")?, records: vec![Bytes::from_static(b"new")] },
                    CommitMutation::Fork {
                        source: path("native/source")?, destination: path("native/fork")?,
                        at_tail: 1, records: vec![Bytes::from_static(b"child")],
                    },
                ],
                idempotency_key: key(b"native-fork")?,
            };
            let meter = Arc::new(Meter::default());
            let committed = provider.native_commit(request.clone(), meter.original()).await??;
            let replay = provider.native_commit(request, meter.original()).await??;
            assert_eq!(committed.value(), replay.value());
            assert_eq!(provider.tail(path("native/source")?).await?, 2);
            let mut output = provider.native_read(ReadRequest {
                path: path("native/fork")?, from: 0, limit: 3,
            }, meter.original()).await??;
            let first = output.value_mut().next().await.ok_or(StreamError::Unavailable)???;
            let second = output.value_mut().next().await.ok_or(StreamError::Unavailable)???;
            assert_eq!(first.value().value, Bytes::from_static(b"old"));
            assert_eq!(second.value().value, Bytes::from_static(b"child"));
            assert_eq!((first.value().sequence, second.value().sequence), (0, 1));
            assert!(output.value_mut().next().await.is_none());
            drop((committed, replay, output, first, second));
            drop(provider);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            Ok(())
        }
    }

    #[tokio::test]
    async fn committed_record_times_never_regress_with_the_clock() -> Result<(), StreamError> {
        let clock = Arc::new(TestClock::default());
        let stream = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
        let path = StreamPath::new("accounting/authority")?;
        for (time, body) in [(100, b"first".as_slice()), (105, b"recovered")] {
            clock.0.store(time, Ordering::SeqCst);
            if time == 105 {
                clock.0.store(90, Ordering::SeqCst);
                let outcome = stream
                    .commit(CommitRequest {
                        conditions: vec![CommitCondition::Tail {
                            path: path.clone(),
                            expected: 1,
                        }],
                        mutations: vec![CommitMutation::Append {
                            path: path.clone(),
                            records: vec![Bytes::from_static(b"rollback")],
                        }],
                        idempotency_key: key(b"rollback-commit")?,
                    })
                    .await?;
                assert!(matches!(outcome, CommitOutcome::Committed(_)));
                clock.0.store(time, Ordering::SeqCst);
            }
            stream
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![Bytes::copy_from_slice(body)],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await?;
        }
        let records = stream
            .read(ReadRequest {
                path,
                from: 0,
                limit: 3,
            })
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            records
                .iter()
                .map(|record| record.committed_at_micros)
                .collect::<Vec<_>>(),
            vec![100_000, 100_000, 105_000]
        );
        Ok(())
    }

    #[derive(Default)]
    struct TestClock(AtomicU64);

    impl UnixMillisClock for TestClock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn path(value: &str) -> Result<StreamPath, StreamError> {
        StreamPath::new(value)
    }

    fn key(value: &'static [u8]) -> Result<crate::IdempotencyKey, StreamError> {
        crate::IdempotencyKey::new(Bytes::from_static(value))
    }

    #[test]
    fn commit_size_matches_canonical_protobuf_without_projection() -> Result<(), StreamError> {
        for expected in [0, 127, 128, u64::MAX] {
            let request = CommitRequest {
                conditions: vec![
                    CommitCondition::Tail { path: path("sizing/source")?, expected },
                    CommitCondition::Absent { path: path("sizing/destination")? },
                ],
                mutations: vec![
                    CommitMutation::Append {
                        path: path("sizing/source")?,
                        records: vec![Bytes::new(), Bytes::from(vec![0; 128])],
                    },
                    CommitMutation::Fork {
                        source: path("sizing/source")?,
                        destination: path("sizing/destination")?,
                        at_tail: expected,
                        records: vec![Bytes::from_static(b"alias")],
                    },
                ],
                idempotency_key: key(b"sizing")?,
            };
            assert_eq!(
                commit_encoded_len(&request),
                crate::wire_codec::commit_to_wire(&request).encoded_len(),
            );
        }
        Ok(())
    }

    #[test]
    fn command_size_uses_exact_protobuf_encoding() -> Result<(), StreamError> {
        let records = vec![Bytes::from(vec![0_u8; MAX_RECORD_BYTES]); 16];
        let mut low = 1_usize;
        let mut high = crate::MAX_PATH_BYTES;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let path = StreamPath::new("x".repeat(middle))?;
            let wire = crate::wire::AppendRequest {
                path: path.to_string(),
                records: records.clone(),
                if_tail: None,
                idempotency_key: None,
            };
            if wire.encoded_len() <= MAX_COMMAND_BYTES {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let request = AppendRequest {
            path: StreamPath::new("x".repeat(low))?,
            records,
            if_tail: None,
            idempotency_key: None,
        };
        assert!(validate_append_size(&request).is_ok());
        let next = StreamPath::new("x".repeat(low + 1))?;
        let next_wire = crate::wire::AppendRequest {
            path: next.to_string(),
            records: request.records,
            if_tail: None,
            idempotency_key: None,
        };
        assert!(next_wire.encoded_len() > MAX_COMMAND_BYTES);
        Ok(())
    }

    #[tokio::test]
    async fn paginates_more_children_than_one_wire_page_without_gaps() -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        for index in 0..2_049 {
            provider
                .append(AppendRequest {
                    path: path(&format!("agents/agent-{index:04}"))?,
                    records: vec![Bytes::from_static(b"bound")],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await?;
        }
        let mut after = None;
        let mut revision = None;
        let mut found = Vec::new();
        loop {
            let page = provider
                .children_page(ChildrenPageRequest {
                    parent: Some(path("agents")?),
                    after,
                    hierarchy_version: revision,
                    limit: 127,
                })
                .await?;
            revision = Some(page.hierarchy_version);
            found.extend(page.children.into_iter().map(|child| child.path));
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
        assert_eq!(found.len(), 2_049);
        assert!(
            found
                .windows(2)
                .all(|pair| matches!(pair, [left, right] if left < right))
        );
        Ok(())
    }

    #[tokio::test]
    async fn deep_fork_history_is_dropped_without_recursive_stack_growth() -> Result<(), StreamError>
    {
        let provider = MemoryStream::default();
        let mut source = path("deep/root")?;
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![Bytes::from_static(b"root")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        for depth in 0..2_048 {
            let destination = path(&format!("deep/fork-{depth}"))?;
            provider
                .fork(ForkRequest {
                    source,
                    destination: destination.clone(),
                    at_tail: None,
                    idempotency_key: None,
                })
                .await?;
            source = destination;
        }
        drop(provider);
        Ok(())
    }

    #[tokio::test]
    async fn provider_deadline_is_exclusive_and_replay_precedes_time() -> Result<(), StreamError> {
        let clock = Arc::new(TestClock::default());
        clock.0.store(100, Ordering::SeqCst);
        let provider = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
        let request = CommitRequest {
            conditions: vec![CommitCondition::Absent {
                path: path("deadline/accepted")?,
            }],
            mutations: vec![CommitMutation::Append {
                path: path("deadline/accepted")?,
                records: vec![Bytes::from_static(b"accepted")],
            }],
            idempotency_key: key(b"deadline-accepted")?,
        };
        assert_eq!(
            provider.commit_before(request.clone(), 100).await,
            Err(StreamError::DeadlineElapsed)
        );
        assert_eq!(
            provider.commit_before(request.clone(), 99).await,
            Err(StreamError::DeadlineElapsed)
        );
        let committed = provider.commit_before(request.clone(), 101).await?;
        assert!(matches!(committed, CommitOutcome::Committed(_)));
        clock.0.store(1_000, Ordering::SeqCst);
        assert_eq!(provider.commit_before(request, 101).await?, committed);

        let retryable = CommitRequest {
            conditions: vec![CommitCondition::Absent {
                path: path("deadline/retry")?,
            }],
            mutations: vec![CommitMutation::Append {
                path: path("deadline/retry")?,
                records: vec![Bytes::from_static(b"retry")],
            }],
            idempotency_key: key(b"deadline-retry")?,
        };
        assert_eq!(
            provider.commit_before(retryable.clone(), 1_000).await,
            Err(StreamError::DeadlineElapsed)
        );
        assert!(matches!(
            provider.commit_before(retryable, 1_001).await?,
            CommitOutcome::Committed(_)
        ));
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::indexing_slicing,
        reason = "indices are preceded by an assert_eq!(read.len(), 2), proving them in-bounds"
    )]
    async fn append_fork_follow_and_replay_preserve_exact_lineage() -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let source = path("runs/a")?;
        let first = AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
            if_tail: Some(0),
            idempotency_key: Some(key(b"append")?),
        };
        let committed = provider.append(first.clone()).await;
        assert!(committed.is_ok());
        assert_eq!(provider.append(first).await, committed);
        let forked = provider
            .fork(ForkRequest {
                source: source.clone(),
                destination: path("runs/b")?,
                at_tail: Some(1),
                idempotency_key: Some(key(b"fork")?),
            })
            .await?;
        assert_eq!(forked.tail, 1);
        let child = forked.destination.clone();
        provider
            .append(AppendRequest {
                path: child.clone(),
                records: vec![Bytes::from_static(b"child")],
                if_tail: Some(1),
                idempotency_key: None,
            })
            .await?;
        let read = provider
            .read(ReadRequest {
                path: child.clone(),
                from: 0,
                limit: 10,
            })
            .await?
            .collect::<Vec<_>>()
            .await;
        assert_eq!(read.len(), 2);
        assert_eq!(
            read[0].as_ref().map(|record| record.value.as_ref()),
            Ok(b"one".as_slice())
        );
        assert_eq!(
            read[1].as_ref().map(|record| record.value.as_ref()),
            Ok(b"child".as_slice())
        );
        let mut follow = provider.follow(child.clone(), 2).await?;
        provider
            .append(AppendRequest {
                path: child,
                records: vec![Bytes::from_static(b"live")],
                if_tail: Some(2),
                idempotency_key: None,
            })
            .await?;
        let live = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next()).await;
        assert!(matches!(live, Ok(Some(Ok(Record { sequence: 2, .. })))));
        Ok(())
    }

    #[tokio::test]
    async fn single_record_reads_seek_deep_history_and_prefixes() -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let source = path("seek/source")?;
        for index in 0..256_u64 {
            provider
                .append(AppendRequest {
                    path: source.clone(),
                    records: vec![Bytes::from(index.to_string())],
                    if_tail: Some(index),
                    idempotency_key: None,
                })
                .await?;
        }
        let destination = path("seek/prefix")?;
        provider
            .fork(ForkRequest {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: Some(128),
                idempotency_key: None,
            })
            .await?;

        for (path, expected_tail, expected) in [
            (&source, 256_u64, b"255".as_slice()),
            (&destination, 128_u64, b"127".as_slice()),
        ] {
            let record = provider
                .read(ReadRequest {
                    path: path.clone(),
                    from: expected_tail - 1,
                    limit: 1,
                })
                .await?
                .next()
                .await
                .ok_or(StreamError::Unavailable)??;
            assert_eq!(record.sequence, expected_tail - 1);
            assert_eq!(record.value, expected);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "bounded scaling benchmark; run with --ignored --nocapture"]
    async fn follow_scales_with_history_depth() -> Result<(), StreamError> {
        for count in [1_000_u64, 10_000] {
            let provider = MemoryStream::default();
            let stream = path("bench/follow")?;
            for index in 0..count {
                provider
                    .append(AppendRequest {
                        path: stream.clone(),
                        records: vec![Bytes::from_static(b"x")],
                        if_tail: Some(index),
                        idempotency_key: None,
                    })
                    .await?;
            }
            let started = std::time::Instant::now();
            let mut follow = provider.follow(stream, 0).await?;
            for expected in 0..count {
                let record = follow.next().await.ok_or(StreamError::Unavailable)??;
                assert_eq!(record.sequence, expected);
            }
            eprintln!("follow history={count} read={:?}", started.elapsed());
        }
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::indexing_slicing,
        reason = "index is preceded by an assert_eq!(forked.len(), 1), proving it in-bounds"
    )]
    async fn coordinated_conflict_changes_nothing_and_success_has_one_envelope()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let source = path("jobs")?;
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![Bytes::from_static(b"ready")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        let request = CommitRequest {
            conditions: vec![CommitCondition::Absent {
                path: path("runs/a")?,
            }],
            mutations: vec![CommitMutation::Fork {
                source: source.clone(),
                destination: path("runs/a")?,
                at_tail: 1,
                records: Vec::new(),
            }],
            idempotency_key: key(b"commit")?,
        };
        provider
            .append(AppendRequest {
                path: source,
                records: vec![Bytes::from_static(b"source-progress")],
                if_tail: Some(1),
                idempotency_key: None,
            })
            .await?;
        let outcome = provider.commit(request.clone()).await;
        let Ok(CommitOutcome::Committed(envelope)) = &outcome else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(provider.commit(request).await, outcome);
        assert_eq!(
            provider.read_commit(envelope.commit_id).await.as_ref(),
            Ok(envelope)
        );
        let forked = provider
            .read(ReadRequest {
                path: path("runs/a")?,
                from: 0,
                limit: 10,
            })
            .await?
            .collect::<Vec<_>>()
            .await;
        assert_eq!(forked.len(), 1);
        assert_eq!(
            forked[0].as_ref().map(|record| record.value.as_ref()),
            Ok(b"ready".as_slice())
        );
        Ok(())
    }

    #[tokio::test]
    async fn exact_replay_precedes_capacity_and_changed_arguments_never_reclassify()
    -> Result<(), StreamError> {
        let provider = MemoryStream::new(MemoryLimits {
            paths: 2,
            path_bytes: 4,
            records: 1,
            payload_bytes: 3,
            commits: 1,
            idempotency_results: 1,
        });
        let request = AppendRequest {
            path: path("a/b")?,
            records: vec![Bytes::from_static(b"one")],
            if_tail: Some(0),
            idempotency_key: Some(key(b"only-key")?),
        };
        let committed = provider.append(request.clone()).await?;
        assert_eq!(
            provider.append(request.clone()).await,
            Ok(committed.clone())
        );
        let observation = provider
            .inspect_idempotency(key(b"only-key")?)
            .await?
            .ok_or(StreamError::Unavailable)?;
        assert_ne!(observation.request_digest, [0; 32]);
        assert_eq!(observation.outcome, IdempotencyOutcome::Append(committed));
        assert_eq!(
            provider
                .append(AppendRequest {
                    records: vec![Bytes::from_static(b"two")],
                    ..request
                })
                .await,
            Err(StreamError::IdempotencyMismatch)
        );
        Ok(())
    }

    #[tokio::test]
    async fn every_fork_cut_reads_the_exact_prefix_and_never_aliases_suffixes()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let source = path("lineage/source")?;
        let bodies = (0_u8..32)
            .map(|value| Bytes::from(vec![value]))
            .collect::<Vec<_>>();
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: bodies,
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        for cut in 0_u64..=32 {
            let destination = path(&format!("lineage/forks/{cut}"))?;
            provider
                .fork(ForkRequest {
                    source: source.clone(),
                    destination: destination.clone(),
                    at_tail: Some(cut),
                    idempotency_key: None,
                })
                .await?;
            let records = provider
                .read(ReadRequest {
                    path: destination.clone(),
                    from: 0,
                    limit: 64,
                })
                .await?
                .collect::<Vec<_>>()
                .await;
            assert_eq!(
                u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?,
                cut
            );
            // A window across the cut reads the source prefix, then the
            // destination's own suffix, never the source's later records.
            provider
                .append(AppendRequest {
                    path: destination.clone(),
                    records: vec![Bytes::from_static(b"own"); 2],
                    if_tail: Some(cut),
                    idempotency_key: None,
                })
                .await?;
            let from = cut.saturating_sub(3);
            let window = provider
                .read(ReadRequest {
                    path: destination,
                    from,
                    limit: 4,
                })
                .await?
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            let expected = (from..cut + 2)
                .take(4)
                .map(|sequence| {
                    if sequence < cut {
                        Bytes::from(vec![u8::try_from(sequence).unwrap_or_default()])
                    } else {
                        Bytes::from_static(b"own")
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(
                window
                    .iter()
                    .map(|record| record.sequence)
                    .collect::<Vec<_>>(),
                (from..from + expected.len() as u64).collect::<Vec<_>>()
            );
            assert_eq!(
                window
                    .into_iter()
                    .map(|record| record.value)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(provider.tail(source).await?, 32);
        Ok(())
    }

    #[test]
    fn permanent_path_validation_rejects_ambiguous_names() {
        for invalid in [
            "", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "a\n", "a b", "a\\b",
        ] {
            assert_eq!(StreamPath::new(invalid), Err(StreamError::InvalidPath));
        }
        assert!(StreamPath::new("a".repeat(crate::MAX_PATH_BYTES)).is_ok());
        assert_eq!(
            StreamPath::new("a".repeat(crate::MAX_PATH_BYTES + 1)),
            Err(StreamError::InvalidPath)
        );
        assert!(StreamPath::new("runs/run_42/agents/researcher").is_ok());
    }

    /// Time cannot free a retry identity for reuse, including at u64 exhaustion.
    #[tokio::test]
    async fn results_and_envelopes_never_expire_or_execute_again() -> Result<(), StreamError> {
        let clock = Arc::new(TestClock::default());
        let limits = MemoryLimits {
            idempotency_results: 1,
            commits: 1,
            ..MemoryLimits::default()
        };
        let provider = MemoryStream::new_with_clock(limits, clock.clone());
        let append = |name: &'static [u8]| AppendRequest {
            path: StreamPath::new("retained").unwrap_or_else(|_| unreachable!()),
            records: vec![Bytes::from_static(name)],
            if_tail: None,
            idempotency_key: Some(
                crate::IdempotencyKey::new(Bytes::from_static(name))
                    .unwrap_or_else(|_| unreachable!()),
            ),
        };
        let AppendOutcome::Committed(first) = provider.append(append(b"first")).await? else {
            return Err(StreamError::Unavailable);
        };
        clock.0.store(1, Ordering::SeqCst);
        assert_eq!(
            provider.append(append(b"second")).await,
            Err(StreamError::Capacity),
            "the first result is still retained"
        );
        assert!(
            provider
                .inspect_idempotency(key(b"first")?)
                .await?
                .is_some()
        );
        assert!(provider.read_commit(first.commit_id).await.is_ok());

        for now in [0, 1, 24 * 60 * 60 * 1_000, u64::MAX] {
            clock.0.store(now, Ordering::SeqCst);
            assert_eq!(
                provider.append(append(b"second")).await,
                Err(StreamError::Capacity)
            );
            assert_eq!(
                provider.append(append(b"first")).await?,
                AppendOutcome::Committed(first.clone())
            );
            assert!(
                provider
                    .inspect_idempotency(key(b"first")?)
                    .await?
                    .is_some()
            );
            assert!(provider.read_commit(first.commit_id).await.is_ok());
            assert_eq!(provider.tail(path("retained")?).await?, 1);
            let mut changed = append(b"first");
            changed.path = path("another/path")?;
            assert_eq!(
                provider.append(changed).await,
                Err(StreamError::IdempotencyMismatch)
            );
        }
        Ok(())
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            rng_seed: proptest::test_runner::RngSeed::Fixed(0x5eed),
            failure_persistence: None,
            ..proptest::prelude::ProptestConfig::with_cases(64)
        })]

        /// Record and child pages concatenate to exactly the full, ordered
        /// set: no page skips or repeats an entry, whatever its bound.
        #[test]
        fn pages_neither_skip_nor_repeat(
            names in proptest::collection::btree_set("[a-c]{1,2}(/[a-c])?", 1..12),
            records in 1..40_usize,
            limit in 1..6_u32,
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread().build()?;
            let provider = MemoryStream::default();
            runtime.block_on(async {
                for name in &names {
                    provider
                        .append(AppendRequest {
                            path: path(&format!("root/{name}"))?,
                            records: vec![Bytes::from_static(b"x")],
                            if_tail: None,
                            idempotency_key: None,
                        })
                        .await?;
                }
                let log = path("log")?;
                let values: Vec<_> = (0..records).map(|value| Bytes::from(value.to_string())).collect();
                for chunk in values.chunks(7) {
                    provider
                        .append(AppendRequest {
                            path: log.clone(),
                            records: chunk.to_vec(),
                            if_tail: None,
                            idempotency_key: None,
                        })
                        .await?;
                }

                let mut read = Vec::new();
                loop {
                    let page: Vec<_> = provider
                        .read(ReadRequest {
                            path: log.clone(),
                            from: read.len() as u64,
                            limit,
                        })
                        .await?
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<_, _>>()?;
                    if page.is_empty() {
                        break;
                    }
                    proptest::prop_assert!(page.len() <= limit as usize);
                    read.extend(page);
                }
                proptest::prop_assert!(read.iter().enumerate().all(|(index, record)| record.sequence == index as u64));
                proptest::prop_assert_eq!(read.into_iter().map(|record| record.value).collect::<Vec<_>>(), values);

                // Only first segments are direct children; deeper paths imply them.
                let expected: std::collections::BTreeSet<_> = names
                    .iter()
                    .map(|name| name.split('/').next().unwrap_or_default().to_owned())
                    .collect();
                let (mut found, mut after, mut version) = (Vec::new(), None, None);
                loop {
                    let page = provider
                        .children_page(ChildrenPageRequest {
                            parent: Some(path("root")?),
                            after,
                            hierarchy_version: version,
                            limit,
                        })
                        .await?;
                    proptest::prop_assert!(page.children.len() <= limit as usize);
                    version = Some(page.hierarchy_version);
                    found.extend(page.children.into_iter().map(|child| child.path.to_string()));
                    after = page.next_after;
                    if after.is_none() {
                        break;
                    }
                }
                let expected: Vec<_> = expected.into_iter().map(|name| format!("root/{name}")).collect();
                proptest::prop_assert_eq!(found, expected);
                Ok(())
            })?;
        }
    }
}
