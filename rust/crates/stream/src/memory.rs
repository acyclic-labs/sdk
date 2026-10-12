//! Single-lock deterministic reference state machine; no durability or availability claim.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    ops::Bound,
    sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}},
};

use async_trait::async_trait;
use crate::OwnedBytes;
use futures::{StreamExt as _, stream};
use sha2::{Digest, Sha256};
use tokio::sync::{RwLock, watch};

#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    ReadAllocation,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    NativeProviderFuture, NativeRecordStream, NativeStreamOutput,
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
    pub(crate) meter: Option<Arc<dyn NativeReadMeter>>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Allocation = Option<ReadAllocation>;
#[cfg(target_arch = "wasm32")]
pub(crate) type Allocation = ();

pub(crate) fn absorb(output: &mut Allocation, incoming: Allocation) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(mut incoming) = incoming {
        if let Some(output) = output.as_mut() { output.absorb(&mut incoming)?; }
        else { *output = Some(incoming); }
    }
    #[cfg(target_arch = "wasm32")]
    let _ = (output, incoming);
    Ok(())
}

#[derive(Default)]
pub(crate) struct MemoryOutput<T> {
    pub(crate) value: T,
    pub(crate) allocation: Allocation,
}

impl<T> MemoryOutput<T> {
    pub(crate) fn map<U>(self, convert: impl FnOnce(T) -> U) -> MemoryOutput<U> {
        MemoryOutput { value: convert(self.value), allocation: self.allocation }
    }
}

struct MemoryByteReservation<'a> {
    #[cfg(not(target_arch = "wasm32"))]
    reservation: Option<NativeByteReservation<&'a dyn NativeReadMeter>>,
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

    fn allocate(&self, bytes: usize) -> std::io::Result<Allocation> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            return Ok(Some(if bytes == 0 {
                ReadAllocation::empty(meter)
            } else { ReadAllocation::new(meter, bytes)? }));
        }
        let _ = bytes;
        Ok(Allocation::default())
    }

    pub(crate) fn vec<T>(&self, capacity: usize) -> std::io::Result<MemoryOutput<Vec<T>>> {
        #[cfg(not(target_arch = "wasm32"))]
        let meter = self.meter.as_ref();
        #[cfg(target_arch = "wasm32")]
        let meter = None;
        let mut funding = crate::wire_codec::WireFunding::new(meter);
        let value = funding.vector(capacity)?;
        Ok(funding.finish(value))
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

    fn bytes(&self, write: bool, bytes: usize) -> std::io::Result<MemoryByteReservation<'_>> {
        #[cfg(not(target_arch = "wasm32"))]
        let reservation = self.meter.as_ref().map(|meter| {
            NativeByteReservation::new(meter.as_ref(), if write { NativeOperationKind::Write } else { NativeOperationKind::Read }, bytes)
        }).transpose()?;
        let _ = (write, bytes);
        Ok(MemoryByteReservation {
            #[cfg(not(target_arch = "wasm32"))]
            reservation,
            #[cfg(target_arch = "wasm32")]
            _observer: self,
        })
    }

    fn boundary(&self, write: bool) -> std::io::Result<()> {
        self.check()?;
        let _ = write;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            // The boundary is this SDK call, not an opaque transport/syscall.
            let mut operation = NativeOperationReservation::new(meter.as_ref(), if write { NativeOperationKind::Write } else { NativeOperationKind::Read }, 1)?;
            let ((), recorded) = operation.attempt(|| ());
            recorded?;
        }
        Ok(())
    }

    fn adopt(&self, bytes: usize) -> std::io::Result<Allocation> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            return Ok(Some(ReadAllocation::adopt(meter, bytes)?));
        }
        let _ = bytes;
        Ok(Allocation::default())
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
pub(crate) fn native_result<T>(result: MemoryResult<T>) -> std::io::Result<Result<T, StreamError>> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(MemoryFailure::Provider(error)) => Ok(Err(error)),
        Err(MemoryFailure::Observer(error)) => Err(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native<T>(result: MemoryResult<MemoryOutput<T>>, meter: &Arc<dyn NativeReadMeter>) -> NativeStreamResult<T> {
    native_result(result).map(|result| result.map(|output| NativeStreamOutput::new(
        output.value, output.allocation.unwrap_or_else(|| ReadAllocation::empty(meter)),
    )))
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
    /// Maximum indefinitely retained replay identities.
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

/// Deterministic process-local provider.
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

    pub(crate) async fn inspect_inner(
        &self,
        idempotency_key: IdempotencyKey,
        observer: &MemoryObserver,
    ) -> MemoryResult<MemoryOutput<Option<IdempotencyObservation>>> {
        observer.boundary(false)?;
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

    pub(crate) async fn bounds_inner(
        &self,
        path: StreamPath,
        observer: &MemoryObserver,
    ) -> MemoryResult<MemoryOutput<StreamBounds>> {
        observer.boundary(false)?;
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
    ) -> MemoryResult<(MemoryOutput<Vec<Record>>, Option<std::io::Error>)> {
        observer.boundary(false)?;
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
        observer.boundary(false)?;
        let state = self.state.read().await;
        observer.admit_item()?;
        let envelope = state.commits.get(&commit_id);
        observer.record_item()?;
        clone_envelope(envelope.and_then(|cached| cached.envelope()).ok_or(StreamError::NotFound)?, observer, true)
    }

    async fn commit_value(
        &self, mut request: CommitRequest, deadline: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        let result = provider_result(self.apply_commit(
            &mut request, deadline, &MemoryObserver::default(),
            None::<&mut fn() -> std::future::Ready<Result<(), std::convert::Infallible>>>,
        ).await)?;
        let (outcome, error, _) = result.unwrap_or_else(|never| match never {});
        assert!(error.is_none(), "ordinary commits have no fallible observer");
        Ok(outcome.value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn commit_future(
        &self, mut request: CommitRequest, deadline: Option<u64>, meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
            match self.apply_commit(&mut request, deadline, &observer,
                None::<&mut fn() -> std::future::Ready<Result<(), StreamError>>>,
            ).await {
                Ok(Ok((outcome, error, _))) => match error {
                    Some(error) => Err(error),
                    None => native(Ok(outcome), &meter),
                },
                Ok(Err(error)) => Ok(Err(error)),
                Err(error) => native(Err(error), &meter),
            }
        })
    }

    pub(crate) async fn apply_commit<E, F, Fut, C>(
        &self,
        request: &mut CommitRequest,
        deadline_unix_millis: Option<u64>,
        observer: &MemoryObserver,
        mut before: Option<&mut F>,
    ) -> MemoryResult<Result<(MemoryOutput<CommitOutcome>, Option<std::io::Error>, Option<C>), E>>
    where
        E: Send,
        F: FnMut() -> Fut + Send,
        Fut: std::future::Future<Output = Result<C, E>> + Send,
        C: Send,
    {
        observer.check()?;
        normalize_commit(request, observer)?;
        validate_commit_shape(request, observer)?;
        let digest = commit_digest(request, &mut DigestWork::Live(observer))?;
        let bytes = request.mutations.iter().map(|mutation| match mutation {
            CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                records.iter().map(OwnedBytes::len).sum::<usize>()
            }
        }).sum();
        observer.boundary(true)?;
        let mut write = observer.bytes(true, bytes)?;
        loop {
            let mut state = self.state.write().await;
            if before.is_some() { state.prepared_capacity.current_revision()?; }
            let mut attempt = self.prepare_commit_attempt(
                &mut state, request, deadline_unix_millis, digest, observer,
            )?;
            if before.is_some() && attempt.decision.is_some() {
                let mut slots = 0_u64;
                let CommitOutcome::Committed(envelope) = &attempt.outcome.value else {
                    unreachable!("a new canonical decision has a committed envelope")
                };
                for mutation in &envelope.mutations {
                    observer.admit_item()?;
                    let records = match mutation {
                        CommittedMutation::Append(append) => &append.records,
                        CommittedMutation::Fork(fork) => &fork.records,
                    };
                    slots += 1 + records.len() as u64;
                    observer.record_item()?;
                }
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(meter) = &observer.meter { meter.admit_examination(slots * 4, 0)?; }
            }
            let continuation = if let Some(before) = before.as_deref_mut() {
                let version = CommitStateVersion::capture(&state)?;
                drop(state);
                let continuation = match before().await { Ok(value) => value, Err(error) => return Ok(Err(error)) };
                state = match self.state.try_write() {
                    Ok(state) => state,
                    Err(_) => {
                        // Release the continuation before waiting on contention.
                        drop(continuation);
                        drop(attempt);
                        drop(self.state.write().await);
                        continue;
                    }
                };
                if !version.matches(&state)? {
                    drop(state);
                    drop(continuation);
                    drop(attempt);
                    continue;
                }
                Some(continuation)
            } else {
                None
            };
            if continuation.is_some() && attempt.decision.is_some() {
                let committed_at = state.last_committed_at_micros
                    .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
                attempt.bind_timestamp(committed_at, observer)?;
            }
            let MemoryCommitAttempt { outcome, publication, decision, committed_at_micros } = attempt;
            let error = if let Some(prepared) = publication {
                observer.check()?;
                if deadline_unix_millis.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
                    return Err(StreamError::DeadlineElapsed.into());
                }
                // Publish without awaiting after the guard and state recheck.
                publish(&mut state, prepared, digest);
                if let Some(decision) = decision { state.decision = decision; }
                state.last_committed_at_micros = committed_at_micros;
                write.record(bytes).err()
            } else {
                None
            };
            return Ok(Ok((outcome, error, continuation)));
        }
    }

    fn prepare_commit_attempt(
        &self,
        state: &mut State,
        request: &CommitRequest,
        deadline_unix_millis: Option<u64>,
        digest: [u8; 32],
        observer: &MemoryObserver,
    ) -> MemoryResult<MemoryCommitAttempt> {
        observer.admit_item()?;
        let replay = state.replays.get(request.idempotency_key.as_bytes());
        observer.record_item()?;
        if let Some(replay) = replay {
            if replay.digest != digest { return Err(StreamError::IdempotencyMismatch.into()); }
            let IdempotencyOutcome::Commit(outcome) = &replay.result else {
                return Err(StreamError::IdempotencyMismatch.into());
            };
            return Ok(MemoryCommitAttempt {
                outcome: clone_commit_outcome(outcome, observer, false)?,
                publication: None, decision: None,
                committed_at_micros: state.last_committed_at_micros,
            });
        }
        if deadline_unix_millis.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
            return Err(StreamError::DeadlineElapsed.into());
        }
        admit_replay(state, Some(&request.idempotency_key), self.limits)?;
        let conflicts = crate::preparation::conditions(&request.conditions, |path| {
            state.paths.get(path).map(|stream| stream.tail)
        }, observer)?;
        let entry = std::mem::size_of::<(OwnedBytes, Arc<Replay>)>();
        let mut resident = observer.adopt(entry)?;
        let (outcome, paths, mutations, decision, committed_at_micros) = if conflicts.value.is_empty() {
            drop(conflicts);
            crate::preparation::authority(request, |path| {
                state.paths.get(path).map(|stream| stream.tail)
            }, observer)?;
            let missing = missing_paths(state, request.mutations.iter().map(mutation_path), observer)?;
            let (records, bytes) = request.mutations.iter().fold((0_usize, 0_usize), |(count, bytes), mutation| {
                let records = match mutation {
                    CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => records,
                };
                (count.saturating_add(records.len()), bytes.saturating_add(records.iter().map(OwnedBytes::len).sum()))
            });
            if reserve(state, &missing.value, records, bytes, self.limits).is_err() {
                recount(state, observer)?;
                reserve(state, &missing.value, records, bytes, self.limits)?;
            }
            let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
            let commit_id = planned_commit_id(decision, digest, observer)?;
            let committed_at_micros = state.last_committed_at_micros
                .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
            let facts = crate::preparation::mutations(&request.mutations, |path| {
                state.paths.get(path).map(|stream| stream.tail)
            }, commit_id, committed_at_micros, observer)?;
            let envelope = facts.map(|mutations| CommittedEnvelope { commit_id, mutations });
            let mutations = prepare_mutations(state, &envelope.value, observer)?;
            let paths = prepare_paths(&missing.value, &envelope.value, observer)?;
            absorb(&mut resident, observer.adopt(std::mem::size_of::<(CommitId, Arc<Replay>)>())?)?;
            let outcome = envelope.map(CommitOutcome::Committed);
            (outcome, paths, mutations, Some(decision), committed_at_micros)
        } else {
            let outcome = conflicts.map(CommitOutcome::Conflict);
            (outcome, observer.vec(0)?, observer.vec(0)?, None, state.last_committed_at_micros)
        };
        let replay = clone_commit_outcome(&outcome.value, observer, false)?;
        let cached = observer.cached(None, IdempotencyOutcome::Commit(replay.value), replay.allocation)?;
        let key = request.idempotency_key.0.clone();
        prepare_retained_capacity(state, observer)?;
        let publication = PreparedPublication {
            cached, key: Some(key),
            paths, mutations, resident,
        };
        Ok(MemoryCommitAttempt { outcome, publication: Some(publication), decision, committed_at_micros })
    }

    pub(crate) async fn apply_append(
        &self,
        request: &AppendRequest,
        observer: &MemoryObserver,
    ) -> MemoryResult<(MemoryOutput<AppendOutcome>, Option<std::io::Error>)> {
        observer.check()?;
        validate_records(&request.records, observer)?;
        let encoded_len = crate::wire_codec::CanonicalWire::Append(request).encoded_len(None, Some(observer))?;
        if encoded_len > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded.into());
        }
        let digest = append_digest(request, observer)?;
        observer.boundary(true)?;
        let bytes = request.records.iter().map(OwnedBytes::len).sum();
        let mut write = observer.bytes(true, bytes)?;
        let mut state = self.state.write().await;
        observer.admit_item()?;
        let replay = request.idempotency_key.as_ref()
            .and_then(|key| state.replays.get(key.as_bytes()));
        observer.record_item()?;
        if let Some(replay) = replay {
            if replay.digest != digest { return Err(StreamError::IdempotencyMismatch.into()); }
            let IdempotencyOutcome::Append(outcome) = &replay.result else {
                return Err(StreamError::IdempotencyMismatch.into());
            };
            return Ok((MemoryOutput { value: outcome.clone(), allocation: observer.allocate(0)? }, None));
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        observer.admit_item()?;
        let actual_tail = state.paths.get(&request.path).map_or(0, |path| path.tail);
        observer.record_item()?;
        let mut resident = observer.allocate(0)?;
        if request.idempotency_key.is_some() {
            absorb(&mut resident, observer.adopt(std::mem::size_of::<(OwnedBytes, Arc<Replay>)>())?)?;
        }
        let (outcome, envelope, cached_allocation, paths, mutations, decision, committed_at) =
            if request.if_tail.is_some_and(|expected| expected != actual_tail) {
                (AppendOutcome::TailConflict { actual_tail }, None, Allocation::default(), observer.vec(0)?,
                    observer.vec(0)?, None, state.last_committed_at_micros)
            } else {
                let missing = missing_paths(&state, std::iter::once(&request.path), observer)?;
                if reserve(&state, &missing.value, request.records.len(), bytes, self.limits).is_err() {
                    recount(&mut state, observer)?;
                    reserve(&state, &missing.value, request.records.len(), bytes, self.limits)?;
                }
                let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
                let commit_id = planned_commit_id(decision, digest, observer)?;
                let committed_at = state.last_committed_at_micros
                    .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
                let mut facts = observer.vec(1)?;
                let fact = crate::preparation::append_fact(
                    &request.path, actual_tail, &request.records, commit_id, committed_at, observer,
                )?;
                let outcome = AppendOutcome::Committed(AppendReceipt {
                    start: fact.value.start, end: fact.value.end, tail: fact.value.tail, commit_id,
                });
                absorb(&mut facts.allocation, fact.allocation)?;
                facts.value.push(CommittedMutation::Append(fact.value));
                let envelope = CommittedEnvelope { commit_id, mutations: facts.value };
                let mutations = prepare_mutations(&state, &envelope, observer)?;
                let paths = prepare_paths(&missing.value, &envelope, observer)?;
                absorb(&mut resident, observer.adopt(std::mem::size_of::<(CommitId, Arc<Replay>)>())?)?;
                (outcome, Some(envelope), facts.allocation, paths, mutations, Some(decision), committed_at)
            };
        let key = request.idempotency_key.as_ref().map(|key| key.0.clone());
        if envelope.is_some() || key.is_some() { prepare_retained_capacity(&mut state, observer)?; }
        let output = MemoryOutput { value: outcome.clone(), allocation: observer.allocate(0)? };
        let prepared = PreparedPublication {
            cached: observer.cached(envelope, IdempotencyOutcome::Append(outcome), cached_allocation)?,
            key, paths, mutations, resident,
        };
        observer.check()?;
        publish(&mut state, prepared, digest);
        if let Some(decision) = decision { state.decision = decision; }
        state.last_committed_at_micros = committed_at;
        let error = if decision.is_some() { write.record(bytes).err() } else { None };
        Ok((output, error))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryStream {
    pub(crate) async fn prepare_commit(
        &self,
        request: &mut CommitRequest,
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> std::io::Result<Result<NativeMemoryPreparedCommit, StreamError>> {
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        let result: MemoryResult<NativeMemoryPreparedCommit> = async {
            observer.check()?;
            if maximum > MAX_RECORD_BYTES { return Err(StreamError::LimitExceeded.into()); }
            normalize_commit(request, &observer)?;
            validate_commit_shape(request, &observer)?;
            let (_, start) = crate::preparation::single_append_template(request)?;
            let command_bytes = crate::wire_codec::CanonicalWire::Commit(request).encoded_len(Some(maximum), None)?;
            if command_bytes > MAX_COMMAND_BYTES { return Err(StreamError::LimitExceeded.into()); }
            let mut state = self.state.write().await;
            observer.admit_item()?;
            let replay = state.replays.get(request.idempotency_key.as_bytes());
            observer.record_item()?;
            if replay.is_some_and(|replay| !matches!(replay.result, IdempotencyOutcome::Commit(_))) {
                return Err(StreamError::IdempotencyMismatch.into());
            }
            let already_retained = replay.is_some();
            let observe = |condition: &CommitCondition| -> MemoryResult<PreparedPathObservation> {
                let CommitCondition::Tail { path, expected } = condition else { unreachable!() };
                observer.admit_item()?;
                let stream = state.paths.get(path);
                observer.record_item()?;
                if !already_retained {
                    let current = stream.ok_or(StreamError::NotFound)?;
                    if current.tail != *expected { return Err(StreamError::OutOfRange.into()); }
                }
                Ok(PreparedPathObservation {
                    path: observer.retain_path(path)?, expected: *expected,
                    history: stream.and_then(|stream| stream.history.clone()),
                    changed: stream.map(|stream| stream.changed.clone()),
                })
            };
            let observations = [observe(&request.conditions[0])?, observe(&request.conditions[1])?];
            let reservation = if already_retained { None } else {
                admit_replay(&state, Some(&request.idempotency_key), self.limits)?;
                reserve(&state, &BTreeSet::new(), 1, 0, self.limits)?;
                if state.payload_bytes
                    .checked_add(state.prepared_capacity.payload_bytes.load(Ordering::Acquire))
                    .and_then(|bytes| bytes.checked_add(maximum))
                    .is_none_or(|bytes| bytes > self.limits.payload_bytes)
                { return Err(StreamError::Capacity.into()); }
                // Every mutation honors these reservations.
                prepare_retained_capacity(&mut state, &observer)?;
                let capacity = Arc::clone(&state.prepared_capacity);
                capacity.advance_revision();
                capacity.commits.fetch_add(1, Ordering::AcqRel);
                capacity.records.fetch_add(1, Ordering::AcqRel);
                capacity.payload_bytes.fetch_add(maximum, Ordering::AcqRel);
                capacity.replays.fetch_add(1, Ordering::AcqRel);
                capacity.retained.fetch_add(1, Ordering::AcqRel);
                Some(PreparedCapacityReservation { capacity, bytes: maximum })
            };
            let facts = crate::preparation::mutations(
                &request.mutations, |_| Some(start), CommitId::default(), 0, &observer,
            )?;
            let output = facts.map(|mutations| CommittedEnvelope { commit_id: CommitId::default(), mutations });
            let mutations = prepare_mutations(&state, &output.value, &observer)?;
            let replay = clone_envelope(&output.value, &observer, false)?;
            let cached = observer.cached(None, IdempotencyOutcome::Commit(CommitOutcome::Committed(replay.value)), replay.allocation)?;
            let conflicts = observer.vec(request.conditions.len())?;
            let replay_conflicts = observer.vec(request.conditions.len())?;
            let conflict_cache = observer.cached(None, IdempotencyOutcome::Commit(CommitOutcome::Conflict(replay_conflicts.value)), replay_conflicts.allocation)?;
            let key = request.idempotency_key.0.clone();
            let replay_entry = observer.adopt(std::mem::size_of::<(OwnedBytes, Arc<Replay>)>())?;
            let commit_entry = observer.adopt(std::mem::size_of::<(CommitId, Arc<Replay>)>())?;
            let write_operation = NativeOperationReservation::new(
                Arc::clone(&meter), NativeOperationKind::Write, 1,
            )?;
            let write_bytes = NativeByteReservation::new(
                Arc::clone(&meter), NativeOperationKind::Write, maximum,
            )?;
            // Bound request, state, and preallocated output visits.
            meter.admit_examination(5 + 4 + 1 + 2 + 2 + 1 + 2 + 3, 0)?;
            meter.admit_codec(0, 0, prepared_hash_bytes(request, maximum)
                + b"acyclic.stream.commit.v1\0".len() as u64 + 8 + 32)?;
            observer.check()?;
            Ok(NativeMemoryPreparedCommit {
                output, cached, mutations, conflicts, conflict_cache,
                observations, key, reservation, write_operation, write_bytes, meter,
                maximum, replay_entry, commit_entry,
            })
        }.await;
        match result {
            Ok(prepared) => Ok(Ok(prepared)),
            Err(MemoryFailure::Provider(error)) => Ok(Err(error)),
            Err(MemoryFailure::Observer(error)) => Err(error),
        }
    }

    pub(crate) fn prepare_recovery(
        &self, prepared: &NativeMemoryPreparedCommit,
    ) -> MemoryResult<NativeProviderFuture<'static, Option<IdempotencyObservation>>> {
        let meter = Arc::clone(&prepared.meter);
        let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
        let output = clone_envelope(&prepared.output.value, &observer, false)?;
        let conflicts = observer.vec(2)?;
        let read_operation = NativeOperationReservation::new(
            Arc::clone(&meter), NativeOperationKind::Read, 1,
        )?;
        let read_bytes = NativeByteReservation::new(
            Arc::clone(&meter), NativeOperationKind::Read, prepared.maximum,
        )?;
        // Bound lookup, output slots, and comparisons against two held conditions.
        meter.admit_examination(1 + 2 + 2 * 2, 0)?;
        let recovery = NativeMemoryPreparedRecovery {
            output, conflicts, key: IdempotencyKey(prepared.key.clone()),
            conditions: [
                (prepared.observations[0].path.clone(), prepared.observations[0].expected),
                (prepared.observations[1].path.clone(), prepared.observations[1].expected),
            ],
            maximum: prepared.maximum, read_operation, read_bytes,
            meter: Arc::clone(&meter),
        };
        observer.check()?;
        let provider = self.clone();
        NativeProviderFuture::try_new(&meter, async move {
            provider.finish_recovery(recovery).await
        }).map_err(MemoryFailure::Observer)
    }

    async fn finish_recovery(
        &self, mut prepared: NativeMemoryPreparedRecovery,
    ) -> std::io::Result<Result<NativeStreamOutput<Option<IdempotencyObservation>>, StreamError>> {
        let meter = Arc::clone(&prepared.meter);
        let mut late = None;
        let ((), recorded) = prepared.read_operation.attempt(|| ());
        retain_prepared_error(&mut late, recorded);
        let state = self.state.read().await;
        let replay = state.replays.get(prepared.key.as_bytes());
        record_prepared_work(&meter, &mut late, 1, 0);
        let Some(replay) = replay else {
            return match late {
                Some(error) => Err(error),
                None => Ok(Ok(NativeStreamOutput::new(None, ReadAllocation::empty(&meter)))),
            };
        };
        let IdempotencyOutcome::Commit(outcome) = &replay.result else {
            return prepared_failure(late, StreamError::IdempotencyMismatch);
        };
        let request_digest = replay.digest;
        let mut outcome = match outcome {
            CommitOutcome::Committed(envelope) => {
                if envelope.mutations.len() != 1 {
                    return prepared_failure(late, StreamError::LimitExceeded);
                }
                let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                    return prepared_failure(late, StreamError::IdempotencyMismatch);
                };
                record_prepared_work(&meter, &mut late, 1, 0);
                if append.records.len() != 1 {
                    return prepared_failure(late, StreamError::LimitExceeded);
                }
                let [record] = append.records.as_slice() else { unreachable!("the record count was checked") };
                record_prepared_work(&meter, &mut late, 1, 0);
                if record.value.len() > prepared.maximum {
                    return prepared_failure(late, StreamError::LimitExceeded);
                }
                let [CommittedMutation::Append(expected)] = prepared.output.value.mutations.as_slice()
                    else { unreachable!("prepared recovery has one Append template") };
                if append.path != expected.path || append.start != expected.start
                    || append.end != expected.start + 1 || append.tail != append.end
                    || record.sequence != expected.start || record.commit_id != envelope.commit_id
                    || record.value.is_empty()
                {
                    return prepared_failure(late, StreamError::IdempotencyMismatch);
                }
                crate::preparation::fill_single_append(&mut prepared.output.value, record, || {
                    record_prepared_work(&meter, &mut late, 1, 0);
                }).expect("the protected recovery output has one preallocated record");
                retain_prepared_error(&mut late, prepared.read_bytes.record(record.value.len()));
                prepared.output.map(CommitOutcome::Committed)
            }
            CommitOutcome::Conflict(conflicts) => {
                if conflicts.len() > 2 {
                    return prepared_failure(late, StreamError::LimitExceeded);
                }
                for conflict in conflicts {
                    record_prepared_work(&meter, &mut late, 1, 0);
                    let CommitConflict::Tail { path, expected, .. } = conflict else {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    };
                    let mut matched = false;
                    for (known, tail) in &prepared.conditions {
                        record_prepared_work(&meter, &mut late, 1, 0);
                        if path == known && expected == tail {
                            matched = true;
                            break;
                        }
                    }
                    if !matched { return prepared_failure(late, StreamError::IdempotencyMismatch); }
                    prepared.conflicts.value.push(conflict.clone());
                }
                prepared.conflicts.map(CommitOutcome::Conflict)
            }
        };
        drop(state);
        if let Some(error) = late { return Err(error); }
        Ok(Ok(NativeStreamOutput::new(Some(IdempotencyObservation {
            idempotency_key: prepared.key, request_digest,
            outcome: IdempotencyOutcome::Commit(outcome.value),
        }), outcome.allocation.take().unwrap_or_else(|| ReadAllocation::empty(&meter)))))
    }

    pub(crate) async fn finish_commit(
        &self,
        mut prepared: NativeMemoryPreparedCommit,
        request: &mut CommitRequest,
    ) -> std::io::Result<Result<(MemoryOutput<CommitOutcome>, Option<std::io::Error>), StreamError>> {
        let mut late = None;
        let meter = Arc::clone(&prepared.meter);
        let [CommitMutation::Append { path, records }] = request.mutations.as_slice() else {
            return Ok(Err(StreamError::InvalidArgument));
        };
        record_prepared_work(&meter, &mut late, 1, 0);
        let [body] = records.as_slice() else { return prepared_failure(late, StreamError::InvalidArgument); };
        record_prepared_work(&meter, &mut late, 1, 0);
        if request.conditions.len() != 2 || body.len() > prepared.maximum {
            return prepared_failure(late, StreamError::LimitExceeded);
        }
        let unchanged_key = request.idempotency_key.as_bytes() == prepared.key.as_ref();
        record_prepared_work(&meter, &mut late, 1, 0);
        if !unchanged_key { return prepared_failure(late, StreamError::IdempotencyMismatch); }
        let [CommittedMutation::Append(append)] = prepared.output.value.mutations.as_slice() else {
            unreachable!("preparation creates the exact single Append fact")
        };
        if path != &append.path { return prepared_failure(late, StreamError::InvalidArgument); }
        for (condition, observed) in request.conditions.iter().zip(&prepared.observations) {
            let unchanged = matches!(condition, CommitCondition::Tail { path, expected }
                if path == &observed.path && expected == &observed.expected);
            record_prepared_work(&meter, &mut late, 1, 0);
            if !unchanged { return prepared_failure(late, StreamError::InvalidArgument); }
        }
        // Record prepaid work without fresh admission.
        let digest = commit_digest(request, &mut DigestWork::Prepared {
            meter: &meter, late: &mut late,
        }).expect("the prepared writer retains late causes instead of abandoning settlement");
        let ((), recorded) = prepared.write_operation.attempt(|| ());
        retain_prepared_error(&mut late, recorded);
        let mut state = self.state.write().await;
        let replay = state.replays.get(prepared.key.as_ref());
        record_prepared_work(&meter, &mut late, 1, 0);
        if let Some(replay) = replay {
            if replay.digest != digest {
                return prepared_failure(late, StreamError::IdempotencyMismatch);
            }
            let IdempotencyOutcome::Commit(outcome) = &replay.result else {
                return prepared_failure(late, StreamError::IdempotencyMismatch);
            };
            let outcome = match outcome {
                CommitOutcome::Committed(envelope) => {
                    let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    };
                    let [record] = append.records.as_slice() else {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    };
                    crate::preparation::fill_single_append(&mut prepared.output.value, record, || {
                        record_prepared_work(&meter, &mut late, 1, 0);
                    }).expect("the original canonical replay has one preallocated record");
                    prepared.output.map(CommitOutcome::Committed)
                }
                CommitOutcome::Conflict(conflicts) => {
                    if conflicts.len() > prepared.conflicts.value.capacity() {
                        return prepared_failure(late, StreamError::IdempotencyMismatch);
                    }
                    for conflict in conflicts {
                        prepared.conflicts.value.push(conflict.clone());
                        record_prepared_work(&meter, &mut late, 1, 0);
                    }
                    prepared.conflicts.map(CommitOutcome::Conflict)
                }
            };
            return Ok(Ok((outcome, late)));
        }
        let Some(reservation) = prepared.reservation.as_ref() else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        if !Arc::ptr_eq(&reservation.capacity, &state.prepared_capacity) {
            return prepared_failure(late, StreamError::PrefixNotRetained);
        }
        let IdempotencyOutcome::Commit(CommitOutcome::Conflict(replay_conflicts)) =
            &mut Arc::get_mut(&mut prepared.conflict_cache)
                .expect("an unpublished conflict cache has one owner").result
            else { unreachable!("preparation creates a conflict cache") };
        for observed in &prepared.observations {
            let actual = state.paths.get(&observed.path).map(|stream| stream.tail);
            if actual != Some(observed.expected) {
                let conflict = CommitConflict::Tail {
                    path: observed.path.clone(), expected: observed.expected, actual,
                };
                replay_conflicts.push(conflict.clone());
                prepared.conflicts.value.push(conflict);
            }
            record_prepared_work(&meter, &mut late, 1, 0);
        }
        if !prepared.conflicts.value.is_empty() {
            let publication = PreparedPublication {
                cached: prepared.conflict_cache,
                key: Some(prepared.key), paths: MemoryOutput::default(),
                mutations: MemoryOutput::default(),
                resident: prepared.replay_entry,
            };
            drop(prepared.reservation.take());
            publish(&mut state, publication, digest);
            return Ok(Ok((prepared.conflicts.map(CommitOutcome::Conflict), late)));
        }
        for observed in &prepared.observations {
            let stream = state.paths.get(&observed.path).expect("the tails were rechecked above");
            let unchanged = observed.changed.as_ref().is_some_and(|changed| stream.changed.same_channel(changed))
                && same_history(stream.history.as_ref(), observed.history.as_ref());
            record_prepared_work(&meter, &mut late, 1, 0);
            if !unchanged {
                return prepared_failure(late, StreamError::PrefixNotRetained);
            }
        }
        let decision = match state.decision.checked_add(1) {
            Some(decision) => decision,
            None => return prepared_failure(late, StreamError::Capacity),
        };
        let commit_id = planned_commit_id(decision, digest, &MemoryObserver::default())
            .expect("the canonical scalar Commit ID writer cannot fail without an observer");
        record_prepared_work(&meter, &mut late, 0,
            b"acyclic.stream.commit.v1\0".len() as u64 + 8 + 32);
        let committed_at_micros = state.last_committed_at_micros
            .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
        let record = Record {
            sequence: append.start, value: body.clone(), commit_id, committed_at_micros,
        };
        let mutation = &mut prepared.mutations.value[0];
        let Some(history) = Arc::get_mut(&mut mutation.history) else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        let History::Batch { records, .. } = history else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        let Some(records) = Arc::get_mut(records) else {
            return prepared_failure(late, StreamError::Unavailable);
        };
        record_prepared_work(&meter, &mut late, 1, 0);
        records.value[0] = record.clone();
        record_prepared_work(&meter, &mut late, 1, 0);
        let cached = Arc::get_mut(&mut prepared.cached).expect("an unpublished cache has one owner");
        let envelope = cached.envelope_mut().expect("preparation creates the canonical Commit cache");
        for envelope in [&mut prepared.output.value, envelope] {
            crate::preparation::fill_single_append(envelope, &record, || {
                record_prepared_work(&meter, &mut late, 1, 0);
            }).expect("preparation created both canonical envelope slots");
        }
        mutation.bytes = body.len();
        absorb(&mut prepared.replay_entry, prepared.commit_entry)?;
        let publication = PreparedPublication {
            cached: prepared.cached,
            key: Some(prepared.key), paths: MemoryOutput::default(),
            mutations: prepared.mutations, resident: prepared.replay_entry,
        };
        drop(prepared.reservation.take());
        publish(&mut state, publication, digest);
        state.decision = decision;
        state.last_committed_at_micros = committed_at_micros;
        retain_prepared_error(&mut late, prepared.write_bytes.record(body.len()));
        Ok(Ok((prepared.output.map(CommitOutcome::Committed), late)))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn retain_prepared_error(late: &mut Option<std::io::Error>, result: std::io::Result<()>) {
    if let Err(error) = result { if late.is_none() { *late = Some(error); } }
}

#[cfg(not(target_arch = "wasm32"))]
fn record_prepared_work(
    meter: &Arc<dyn NativeReadMeter>, late: &mut Option<std::io::Error>, items: u64, hashed: u64,
) {
    if items != 0 { retain_prepared_error(late, meter.record_examination(items, 0)); }
    if hashed != 0 { retain_prepared_error(late, meter.record_codec(0, 0, hashed)); }
}

#[cfg(not(target_arch = "wasm32"))]
fn prepared_failure<T>(
    late: Option<std::io::Error>, error: StreamError,
) -> std::io::Result<Result<T, StreamError>> {
    match late { Some(error) => Err(error), None => Ok(Err(error)) }
}

struct MemoryCommitAttempt {
    outcome: MemoryOutput<CommitOutcome>,
    publication: Option<PreparedPublication>,
    decision: Option<u64>,
    committed_at_micros: u64,
}

impl MemoryCommitAttempt {
    fn bind_timestamp(&mut self, committed_at: u64, observer: &MemoryObserver) -> MemoryResult<()> {
        fn envelope(
            envelope: &mut CommittedEnvelope, committed_at: u64, observer: &MemoryObserver,
        ) -> MemoryResult<()> {
            for mutation in &mut envelope.mutations {
                observer.record_item()?;
                let records = match mutation {
                    CommittedMutation::Append(append) => &mut append.records,
                    CommittedMutation::Fork(fork) => &mut fork.records,
                };
                for record in records {
                    record.committed_at_micros = committed_at;
                    observer.record_item()?;
                }
            }
            Ok(())
        }
        let CommitOutcome::Committed(output) = &mut self.outcome.value else {
            return Err(StreamError::Unavailable.into());
        };
        let publication = self.publication.as_mut().ok_or(StreamError::Unavailable)?;
        let cached = Arc::get_mut(&mut publication.cached).ok_or(StreamError::Unavailable)?;
        let replay = cached.envelope_mut().ok_or(StreamError::Unavailable)?;
        envelope(output, committed_at, observer)?;
        envelope(replay, committed_at, observer)?;
        for mutation in &mut publication.mutations.value {
            observer.record_item()?;
            let history = Arc::get_mut(&mut mutation.history).ok_or(StreamError::Unavailable)?;
            if let History::Batch { records, .. } = history {
                let records = Arc::get_mut(records).ok_or(StreamError::Unavailable)?;
                for record in &mut records.value {
                    record.committed_at_micros = committed_at;
                    observer.record_item()?;
                }
            }
        }
        self.committed_at_micros = committed_at;
        Ok(())
    }
}

#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    commits: BTreeMap<CommitId, Arc<Replay>>,
    replays: BTreeMap<OwnedBytes, Arc<Replay>>,
    /// Indefinite receipt inventory used by the current snapshot codec.
    retained: VecDeque<Retained>,
    path_bytes: usize,
    /// Records and payload bytes retained, each counted once however many paths share it; an upper bound between [`recount`]s.
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
    /// High-water mark survives clock rollback and snapshot restoration.
    last_committed_at_micros: u64,
    hierarchy_version: CommitId,
    retained_allocation: Allocation,
    /// Numeric capacity held by preaccepted completion packets.
    prepared_capacity: Arc<PreparedCapacity>,
}

impl Drop for State {
    fn drop(&mut self) {
        // Tear down unique history chains iteratively.
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
    replay: Option<OwnedBytes>,
    commit: Option<CommitId>,
    _allocation: Allocation,
}

struct PathState {
    history: Option<Arc<History>>,
    tail: u64,
    changed: watch::Sender<u64>,
    _allocation: Allocation,
}

enum History {
    Batch {
        parent: Option<Arc<Self>>,
        records: Arc<MemoryOutput<Vec<Record>>>,
        _allocation: Allocation,
    },
    Prefix {
        source: Option<Arc<Self>>,
        tail: u64,
        _allocation: Allocation,
    },
}

struct Replay {
    digest: [u8; 32],
    result: IdempotencyOutcome,
    envelope: Option<CommittedEnvelope>,
    _allocation: Allocation,
}

impl Replay {
    fn envelope_mut(&mut self) -> Option<&mut CommittedEnvelope> {
        match &mut self.result {
            IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) => Some(envelope),
            _ => self.envelope.as_mut(),
        }
    }

    fn envelope(&self) -> Option<&CommittedEnvelope> {
        match &self.result {
            IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) => Some(envelope),
            _ => self.envelope.as_ref(),
        }
    }
}

impl MemoryObserver {
    fn cached(
        &self, envelope: Option<CommittedEnvelope>, result: IdempotencyOutcome,
        mut allocation: Allocation,
    ) -> std::io::Result<Arc<Replay>> {
        absorb(&mut allocation, self.allocate(std::mem::size_of::<Replay>())?)?;
        Ok(Arc::new(Replay { digest: [0; 32], result, envelope, _allocation: allocation }))
    }
}

#[derive(Default)]
struct PreparedCapacity {
    commits: AtomicUsize,
    records: AtomicUsize,
    payload_bytes: AtomicUsize,
    replays: AtomicUsize,
    retained: AtomicUsize,
    revision: AtomicU64,
}

impl PreparedCapacity {
    /// Fails new guarded acceptance after revision exhaustion.
    fn advance_revision(&self) {
        let _ = self.revision.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.checked_add(1).unwrap_or(u64::MAX))
        });
    }

    fn current_revision(&self) -> Result<u64, StreamError> {
        let revision = self.revision.load(Ordering::Acquire);
        if revision == u64::MAX { Err(StreamError::Capacity) } else { Ok(revision) }
    }
}

/// The genuine canonical mutation revision on the existing state lifetime.
struct CommitStateVersion {
    identity: Arc<PreparedCapacity>,
    revision: u64,
    reserved: [usize; 5],
}

impl CommitStateVersion {
    fn reserved(capacity: &PreparedCapacity) -> [usize; 5] {
        [
            capacity.commits.load(Ordering::Acquire),
            capacity.records.load(Ordering::Acquire),
            capacity.payload_bytes.load(Ordering::Acquire),
            capacity.replays.load(Ordering::Acquire),
            capacity.retained.load(Ordering::Acquire),
        ]
    }

    fn capture(state: &State) -> Result<Self, StreamError> {
        Ok(Self {
            identity: Arc::clone(&state.prepared_capacity),
            revision: state.prepared_capacity.current_revision()?,
            reserved: Self::reserved(&state.prepared_capacity),
        })
    }

    fn matches(&self, state: &State) -> Result<bool, StreamError> {
        Ok(Arc::ptr_eq(&self.identity, &state.prepared_capacity)
            && self.revision == state.prepared_capacity.current_revision()?
            && self.reserved == Self::reserved(&state.prepared_capacity))
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct PreparedCapacityReservation {
    capacity: Arc<PreparedCapacity>,
    bytes: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PreparedCapacityReservation {
    fn drop(&mut self) {
        self.capacity.advance_revision();
        self.capacity.commits.fetch_sub(1, Ordering::AcqRel);
        self.capacity.records.fetch_sub(1, Ordering::AcqRel);
        self.capacity.payload_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.capacity.replays.fetch_sub(1, Ordering::AcqRel);
        self.capacity.retained.fetch_sub(1, Ordering::AcqRel);
        // Revision changes bracket the lock-free capacity refund.
        self.capacity.advance_revision();
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct PreparedPathObservation {
    path: StreamPath,
    expected: u64,
    history: Option<Arc<History>>,
    changed: Option<watch::Sender<u64>>,
}

/// Actual canonical controls, created before an external source action.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeMemoryPreparedCommit {
    output: MemoryOutput<CommittedEnvelope>,
    cached: Arc<Replay>,
    mutations: MemoryOutput<Vec<PreparedMutation>>,
    conflicts: MemoryOutput<Vec<CommitConflict>>,
    conflict_cache: Arc<Replay>,
    observations: [PreparedPathObservation; 2],
    key: OwnedBytes,
    reservation: Option<PreparedCapacityReservation>,
    write_operation: NativeOperationReservation<Arc<dyn NativeReadMeter>>,
    write_bytes: NativeByteReservation<Arc<dyn NativeReadMeter>>,
    meter: Arc<dyn NativeReadMeter>,
    maximum: usize,
    replay_entry: Allocation,
    commit_entry: Allocation,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeMemoryPreparedRecovery {
    output: MemoryOutput<CommittedEnvelope>,
    conflicts: MemoryOutput<Vec<CommitConflict>>,
    key: IdempotencyKey,
    conditions: [(StreamPath, u64); 2],
    maximum: usize,
    read_operation: NativeOperationReservation<Arc<dyn NativeReadMeter>>,
    read_bytes: NativeByteReservation<Arc<dyn NativeReadMeter>>,
    meter: Arc<dyn NativeReadMeter>,
}

#[cfg(not(target_arch = "wasm32"))]
fn same_history(left: Option<&Arc<History>>, right: Option<&Arc<History>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn prepared_hash_bytes(request: &CommitRequest, body_length: usize) -> u64 {
    // Exactly the byte slices consumed by the canonical Commit digest writer.
    b"acyclic.stream.request.v1\0".len() as u64 + b"commit".len() as u64
        + request.conditions.iter().map(|condition| {
            1 + 8 + condition_path(condition).as_str().len() as u64 + 8
        }).sum::<u64>()
        + 1 + 8 + mutation_path(&request.mutations[0]).as_str().len() as u64
        + 8 + 8 + body_length as u64
}

fn clone_records(
    records: &[Record],
    observer: &MemoryObserver,
    output_read: bool,
) -> MemoryResult<MemoryOutput<Vec<Record>>> {
    let mut output = observer.vec(records.len())?;
    for record in records {
        observer.admit_item()?;
        let mut read = if output_read { Some(observer.bytes(false, record.value.len())?) } else { None };
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
                absorb(&mut output.allocation, records.allocation)?;
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
                absorb(&mut output.allocation, records.allocation)?;
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
    Ok(output.map(|mutations| CommittedEnvelope { commit_id: envelope.commit_id, mutations }))
}

fn clone_commit_outcome(
    outcome: &CommitOutcome,
    observer: &MemoryObserver,
    output_read: bool,
) -> MemoryResult<MemoryOutput<CommitOutcome>> {
    match outcome {
        CommitOutcome::Committed(envelope) => {
            let output = clone_envelope(envelope, observer, output_read)?;
            Ok(output.map(CommitOutcome::Committed))
        }
        CommitOutcome::Conflict(conflicts) => {
            let mut output = observer.vec(conflicts.len())?;
            for conflict in conflicts {
                observer.admit_item()?;
                output.value.push(conflict.clone());
                observer.record_item()?;
            }
            Ok(output.map(CommitOutcome::Conflict))
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
            Ok(output.map(IdempotencyOutcome::Commit))
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

fn record_iterator(
    (records, mut terminal): (MemoryOutput<Vec<Record>>, Option<std::io::Error>),
) -> impl Iterator<Item = std::io::Result<Record>> {
    let mut records = records.map(Vec::into_iter);
    std::iter::from_fn(move || {
        // Capture the whole carrier so vector storage drops before its claim.
        let _retained = &records;
        records.value.next().map(Ok).or_else(|| terminal.take().map(Err))
    })
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
        let completion = self.apply_append(&request, &MemoryObserver::default()).await;
        ordinary(completion.map(|(outcome, error)| {
            assert!(error.is_none(), "ordinary appends have no fallible observer");
            outcome
        }))
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
        let observer = MemoryObserver::default();
        let missing = provider_result(missing_paths(&state, std::iter::once(&request.destination), &observer))?;
        reserve(&state, &missing.value, 0, 0, self.limits)?;
        provider_result(prepare_retained_capacity(&mut state, &observer))?;
        let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
        let commit_id = provider_result(planned_commit_id(decision, digest, &observer).map_err(MemoryFailure::from))?;
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Fork(CommittedFork {
                source: request.source.clone(), destination: request.destination.clone(),
                forked_at, tail: forked_at, records: Vec::new(),
            })],
        };
        let paths = provider_result(prepare_paths(&missing.value, &envelope, &observer))?;
        let mutations = provider_result(prepare_mutations(&state, &envelope, &observer))?;
        drop(missing);
        let receipt = ForkReceipt {
            source: request.source, destination: request.destination,
            forked_at, tail: forked_at, commit_id,
        };
        let cached = provider_result(observer.cached(Some(envelope), IdempotencyOutcome::Fork(receipt.clone()), Allocation::default())
            .map_err(MemoryFailure::Observer))?;
        publish(&mut state, PreparedPublication {
            cached, key: request.idempotency_key.map(|key| key.0), paths, mutations,
            resident: Allocation::default(),
        }, digest);
        state.decision = decision;
        Ok(receipt)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        validate_limit(request.limit)?;
        let records = provider_result(self.read_inner(request, &MemoryObserver::default()).await)?;
        Ok(stream::iter(record_iterator(records)).map(|record| {
            Ok(record.expect("ordinary reads have no fallible observer"))
        }).boxed())
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
        let lower = start.map_or(Bound::Unbounded, |path| Bound::Excluded(path.as_str()));
        let mut paths = state.paths.range::<str, _>((lower, Bound::Unbounded))
            .map(|(path, _)| path)
            .take_while(|path| request.parent.as_ref().is_none_or(|parent| {
                path.as_str().strip_prefix(parent.as_str()).is_some_and(|suffix| suffix.starts_with('/'))
            }))
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
        self.commit_value(request, None).await
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_value(request, Some(deadline_unix_millis)).await
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
            native(self.bounds_inner(path, &observer).await.map(|output| output.map(|bounds| bounds.tail)), &meter)
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
            if let Err(error) = validate_limit(request.limit) {
                return Ok(Err(error));
            }
            let cursor_meter = Arc::clone(&meter);
            let cursor = stream::iter(record_iterator((MemoryOutput::default(), None))).map(move |record| record.map(|record| Ok(NativeStreamOutput::new(record, ReadAllocation::empty(&cursor_meter)))));
            let allocation = observer.allocate(std::mem::size_of_val(&cursor))?;
            let mut cursor = MemoryOutput { value: Box::pin(cursor), allocation };
            let result = self.read_inner(request, &observer).await.map(|records| {
                *cursor.value.as_mut().get_mut().get_mut().get_mut() = record_iterator(records);
                MemoryOutput { value: cursor.value as NativeRecordStream, allocation: cursor.allocation }
            });
            native(result, &meter)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
            match self.apply_append(&request, &observer).await {
                Ok((outcome, error)) => match error {
                    Some(error) => Err(error),
                    None => native(Ok(outcome), &meter),
                },
                Err(error) => native(Err(error), &meter),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.commit_future(request, None, meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
        mut before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let mut input = crate::capture_commit(&request, &meter)?;
            drop(request);
            let observer = MemoryObserver { meter: Some(Arc::clone(&meter)) };
            match self.apply_commit(&mut input.value, deadline_unix_millis, &observer, Some(&mut before)).await {
                Ok(Ok((outcome, error, _))) => match error {
                    Some(error) => Err(error),
                    None => native(Ok(outcome), &meter).map(|result| result.map_err(E::from)),
                },
                Ok(Err(error)) => Ok(Err(error)),
                Err(error) => native(Err(error), &meter).map(|result| result.map_err(E::from)),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let mut input = crate::capture_commit(&request, &meter)?;
            drop(request);
            let prepared = match self.prepare_commit(
                &mut input.value, maximum_record_bytes, Arc::clone(&meter),
            ).await? {
                Ok(prepared) => prepared,
                Err(error) => return Ok(Err(error)),
            };
            let recovery = match self.prepare_recovery(&prepared) {
                Ok(future) => future,
                Err(MemoryFailure::Observer(error)) => return Err(error),
                Err(MemoryFailure::Provider(error)) => return Ok(Err(error)),
            };
            let slot = crate::NativeCommitRecordSlot::new(&meter)?;
            let received = Arc::clone(&slot);
            let provider = self.clone();
            let finish_meter = Arc::clone(&meter);
            // Fund the completion future before accepting the final record.
            let future = NativeProviderFuture::try_new(&meter, async move {
                let body = match received.receive().await {
                    Ok(body) => body,
                    Err(error) => return Ok(Err(error)),
                };
                let [CommitMutation::Append { records, .. }] = input.value.mutations.as_mut_slice()
                    else { unreachable!("the private template was validated before issuance") };
                records[0] = body;
                match provider.finish_commit(prepared, &mut input.value).await? {
                    Ok((outcome, error)) => match error {
                        Some(error) => Err(error),
                        None => native(Ok(outcome), &finish_meter),
                    },
                    Err(error) => Ok(Err(error)),
                }
            })?;
            Ok(Ok(NativeStreamOutput::new(
                crate::PreparedNativeCommit::new(future, recovery, slot, maximum_record_bytes),
                ReadAllocation::empty(&meter),
            )))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.commit_future(request, Some(deadline_unix_millis), meter)
    }

}

pub(crate) fn validate_limit(limit: u32) -> Result<(), StreamError> {
    if limit == 0 || usize::try_from(limit).map_or(true, |limit| limit > MAX_ITEMS) {
        Err(StreamError::LimitExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_records(records: &[OwnedBytes], observer: &MemoryObserver) -> MemoryResult<()> {
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
    (crate::wire_codec::CanonicalWire::Append(request).encoded_len(None, None)
        .expect("ordinary sizing has no fallible observer") <= MAX_COMMAND_BYTES)
        .then_some(()).ok_or(StreamError::LimitExceeded)
}

pub(crate) fn validate_fork_size(request: &ForkRequest) -> Result<(), StreamError> {
    (crate::wire_codec::CanonicalWire::Fork(request).encoded_len(None, None)
        .expect("ordinary sizing has no fallible observer") <= MAX_COMMAND_BYTES)
        .then_some(())
        .ok_or(StreamError::LimitExceeded)
}

fn reserve(
    state: &State,
    paths: &BTreeSet<&str>,
    records: usize,
    bytes: usize,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    if state.commits.len().saturating_add(state.prepared_capacity.commits.load(Ordering::Acquire)) >= limits.commits
        || state.paths.len().saturating_add(paths.len()) > limits.paths
        || state.path_bytes.saturating_add(paths.iter().map(|path| path.len()).sum()) > limits.path_bytes
        || state.record_count.saturating_add(state.prepared_capacity.records.load(Ordering::Acquire))
            .saturating_add(records) > limits.records
        || state.payload_bytes.saturating_add(state.prepared_capacity.payload_bytes.load(Ordering::Acquire))
            .saturating_add(bytes) > limits.payload_bytes
    {
        Err(StreamError::Capacity)
    } else { Ok(()) }
}

struct HistoryWindow {
    records: Arc<MemoryOutput<Vec<Record>>>,
    end: usize,
}

/// Incremental cursor over the immutable history graph used by `follow`.
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

        // Rebuild only the changed suffix, preserving chronological order.
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
            while self.record < window.end {
                let index = self.record;
                let Some(record) = window.records.value.get(index) else {
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
        let index = self.record;
        let record = window.records.value.get(index)?.clone();
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

fn history_window(records: Arc<MemoryOutput<Vec<Record>>>, ceiling: u64) -> Option<HistoryWindow> {
    let first = records.value.first()?.sequence;
    let batch_end = records.value
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
        end,
    })
}

fn read_history(
    history: Option<&Arc<History>>,
    tail: u64,
    from: u64,
    limit: usize,
    observer: &MemoryObserver,
) -> MemoryResult<(MemoryOutput<Vec<Record>>, Option<std::io::Error>)> {
    // The request limit bounds this stack index.
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
                if let Some(first) = records.value.first().map(|record| record.sequence) {
                    let offset = |sequence: u64| {
                        usize::try_from(sequence.saturating_sub(first))
                            .map_or(records.value.len(), |offset| offset.min(records.value.len()))
                    };
                    let start = offset(from);
                    if let Some(slice) = records.value.get(start..offset(ceiling.min(end)).max(start))
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
            let mut read = match observer.bytes(false, record.value.len()) {
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
    Ok((output, terminal))
}

fn is_direct_child(parent: &StreamPath, candidate: &StreamPath) -> bool {
    candidate.as_str().rsplit_once('/').map(|(path, _)| path) == Some(parent.as_str())
}

/// Counts exactly the records and payload bytes the live paths retain, each shared history once.
fn recount(state: &mut State, observer: &MemoryObserver) -> MemoryResult<()> {
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
            absorb(&mut seen.allocation, observer.adopt(std::mem::size_of::<*const History>())?)?;
            seen.value.insert(pointer);
            cursor = match node.as_ref() {
                History::Batch { parent, records: batch, .. } => {
                    records = records.saturating_add(batch.value.len());
                    for record in batch.value.iter() {
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
    if state.record_count != records || state.payload_bytes != bytes {
        state.prepared_capacity.advance_revision();
    }
    state.record_count = records;
    state.payload_bytes = bytes;
    Ok(())
}

fn planned_commit_id(
    decision: u64,
    digest: [u8; 32],
    observer: &MemoryObserver,
) -> std::io::Result<CommitId> {
    let mut hash = Sha256::new();
    hash_update(&mut hash, b"acyclic.stream.commit.v1\0", observer)?;
    hash_update(&mut hash, &decision.to_le_bytes(), observer)?;
    hash_update(&mut hash, &digest, observer)?;
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
    if key.is_some() && state.replays.len()
        .saturating_add(state.prepared_capacity.replays.load(Ordering::Acquire)) >= limits.idempotency_results {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}


pub(crate) fn append_digest(
    request: &AppendRequest,
    observer: &MemoryObserver,
) -> std::io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    let mut work = DigestWork::Live(observer);
    work.update(&mut hash, b"acyclic.stream.request.v1\0")?;
    work.update(&mut hash, b"append")?;
    work.path(&mut hash, &request.path)?;
    work.update(&mut hash, &request.if_tail.unwrap_or(u64::MAX).to_le_bytes())?;
    work.records(&mut hash, &request.records)?;
    Ok(hash.finalize().into())
}

pub(crate) fn fork_digest(request: &ForkRequest) -> [u8; 32] {
    let mut hash = request_hasher(b"fork");
    hash_path(&mut hash, &request.source);
    hash_path(&mut hash, &request.destination);
    hash.update(request.at_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash.finalize().into()
}

fn hash_update(
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

pub(crate) enum DigestWork<'a> {
    Live(&'a MemoryObserver),
    #[cfg(not(target_arch = "wasm32"))]
    Prepared {
        meter: &'a Arc<dyn NativeReadMeter>,
        late: &'a mut Option<std::io::Error>,
    },
}

impl DigestWork<'_> {
    fn admit_item(&self) -> std::io::Result<()> {
        match self {
            Self::Live(observer) => observer.admit_item(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Prepared { .. } => Ok(()),
        }
    }

    fn record_item(&mut self) -> std::io::Result<()> {
        match self {
            Self::Live(observer) => observer.record_item(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Prepared { meter, late } => {
                record_prepared_work(meter, late, 1, 0);
                Ok(())
            }
        }
    }

    fn update(&mut self, hash: &mut Sha256, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Live(observer) => hash_update(hash, bytes, observer),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Prepared { meter, late } => {
                hash.update(bytes);
                record_prepared_work(meter, late, 0, bytes.len() as u64);
                Ok(())
            }
        }
    }

    fn path(&mut self, hash: &mut Sha256, path: &StreamPath) -> std::io::Result<()> {
        self.update(hash, &(path.as_str().len() as u64).to_le_bytes())?;
        self.update(hash, path.as_str().as_bytes())
    }

    fn records(&mut self, hash: &mut Sha256, records: &[OwnedBytes]) -> std::io::Result<()> {
        self.update(hash, &(records.len() as u64).to_le_bytes())?;
        for record in records {
            self.admit_item()?;
            self.update(hash, &(record.len() as u64).to_le_bytes())?;
            self.update(hash, record)?;
            self.record_item()?;
        }
        Ok(())
    }
}

/// One canonical Commit writer.
pub(crate) fn commit_digest(request: &CommitRequest, work: &mut DigestWork<'_>) -> std::io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    work.update(&mut hash, b"acyclic.stream.request.v1\0")?;
    work.update(&mut hash, b"commit")?;
    for condition in &request.conditions {
        work.admit_item()?;
        match condition {
            CommitCondition::Tail { path, expected } => {
                work.update(&mut hash, &[1])?;
                work.path(&mut hash, path)?;
                work.update(&mut hash, &expected.to_le_bytes())?;
            }
            CommitCondition::Absent { path } => {
                work.update(&mut hash, &[2])?;
                work.path(&mut hash, path)?;
            }
        }
        work.record_item()?;
    }
    for mutation in &request.mutations {
        work.admit_item()?;
        match mutation {
            CommitMutation::Append { path, records } => {
                work.update(&mut hash, &[1])?;
                work.path(&mut hash, path)?;
                work.records(&mut hash, records)?;
            }
            CommitMutation::Fork { source, destination, at_tail, records } => {
                work.update(&mut hash, &[2])?;
                work.path(&mut hash, source)?;
                work.path(&mut hash, destination)?;
                work.update(&mut hash, &at_tail.to_le_bytes())?;
                work.records(&mut hash, records)?;
            }
        }
        work.record_item()?;
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

pub(crate) fn normalize_commit(request: &mut CommitRequest, observer: &MemoryObserver) -> MemoryResult<()> {
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
            CommitMutation::Append { records, .. } => validate_records(records, observer)?,
            CommitMutation::Fork { records, .. } if !records.is_empty() => {
                validate_records(records, observer)?;
            }
            CommitMutation::Fork { .. } => {}
        }
        observer.record_item()?;
    }
    if crate::wire_codec::CanonicalWire::Commit(request).encoded_len(None, Some(observer))? > MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded.into());
    }
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

pub(crate) fn validate_commit_shape(request: &CommitRequest, observer: &MemoryObserver) -> MemoryResult<()> {
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

fn missing_paths<'a>(
    state: &State,
    paths: impl IntoIterator<Item = &'a StreamPath>,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<BTreeSet<&'a str>>> {
    let mut output = MemoryOutput {
        value: BTreeSet::new(),
        allocation: observer.allocate(0)?,
    };
    for path in paths {
        let mut current = Some(path.as_str());
        while let Some(path) = current {
            observer.admit_item()?;
            if !state.paths.contains_key(path) && !output.value.contains(path) {
                // Declared logical entry quota, not an invented std BTree node.
                absorb(&mut output.allocation, observer.adopt(std::mem::size_of::<&str>())?)?;
                output.value.insert(path);
            }
            observer.record_item()?;
            current = path.rsplit_once('/').map(|(parent, _)| parent);
        }
    }
    Ok(output)
}

struct PreparedMutation {
    path: StreamPath,
    history: Arc<History>,
    tail: u64,
    records: usize,
    bytes: usize,
}

struct PreparedPublication {
    cached: Arc<Replay>,
    key: Option<OwnedBytes>,
    paths: MemoryOutput<Vec<(StreamPath, PathState)>>,
    mutations: MemoryOutput<Vec<PreparedMutation>>,
    resident: Allocation,
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

fn batch_history(
    parent: Option<Arc<History>>,
    records: &[Record],
    observer: &MemoryObserver,
) -> MemoryResult<Arc<History>> {
    let mut records = clone_records(records, observer, false)?;
    absorb(&mut records.allocation, observer.allocate(std::mem::size_of::<MemoryOutput<Vec<Record>>>())?)?;
    let records = Arc::new(records);
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
    let minimum = state.retained.len()
        .checked_add(state.prepared_capacity.retained.load(Ordering::Acquire))
        .and_then(|length| length.checked_add(1)).ok_or(StreamError::Capacity)?;
    if minimum <= state.retained.capacity() { return Ok(()); }
    let capacity = state.retained.capacity().checked_mul(2).ok_or(StreamError::Capacity)?
        .max(minimum).max(4);
    let allocation = observer.allocate(capacity.checked_mul(std::mem::size_of::<Retained>())
        .ok_or(StreamError::Capacity)?)?;
    state.prepared_capacity.advance_revision();
    state.retained.reserve_exact(capacity - state.retained.len());
    // Transfer the new capacity before fallible reconciliation.
    state.retained_allocation = allocation;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(allocation) = state.retained_allocation.as_mut() {
        allocation.reconcile_capacity(state.retained.capacity().checked_mul(std::mem::size_of::<Retained>())
            .ok_or(StreamError::Capacity)?)?;
    }
    Ok(())
}

fn publish(state: &mut State, mut prepared: PreparedPublication, digest: [u8; 32]) {
    state.prepared_capacity.advance_revision();
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
    Arc::get_mut(&mut prepared.cached).expect("an unpublished cache has one owner").digest = digest;
    let commit = prepared.cached.envelope().map(|envelope| envelope.commit_id);
    if let Some(commit_id) = commit {
        state.commits.insert(commit_id, Arc::clone(&prepared.cached));
        if creates_paths { state.hierarchy_version = commit_id; }
    }
    let replay = prepared.key.map(|key| {
        state.replays.insert(key.clone(), prepared.cached);
        key
    });
    let resident = prepared.resident;
    if commit.is_some() || replay.is_some() {
        state.retained.push_back(Retained { replay, commit, _allocation: resident });
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(not(target_arch = "wasm32"))]
    mod native_controls {
        use super::*;
        use std::sync::atomic::AtomicBool;


        struct Meter {
            held: AtomicU64,
            allocations: AtomicU64,
            read: AtomicU64,
            read_limit: u64,
            pending_read: AtomicU64,
            pending_write: AtomicU64,
            pending_read_operations: AtomicU64,
            pending_write_operations: AtomicU64,
            pending_durability: AtomicU64,
            copied: AtomicU64,
            cancel_after_read: u64,
            late_write_error: bool,
            cancelled: AtomicBool,
        }

        impl Default for Meter {
            fn default() -> Self {
                Self {
                    held: AtomicU64::new(0), allocations: AtomicU64::new(0),
                    read: AtomicU64::new(0), read_limit: u64::MAX,
                    pending_read: AtomicU64::new(0), pending_write: AtomicU64::new(0),

                    pending_read_operations: AtomicU64::new(0), pending_write_operations: AtomicU64::new(0),
                    pending_durability: AtomicU64::new(0), copied: AtomicU64::new(0),

                    cancel_after_read: u64::MAX, late_write_error: false,
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
                self.allocations.fetch_add(1, Ordering::SeqCst);
                self.held.fetch_add(bytes, Ordering::SeqCst);
                Ok(())
            }
            fn reserve_allocation(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
            fn cancel_reserved_allocation(&self, _: u64) {}
            fn record_reserved_allocation(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
            fn allocation_failed(&self, _: std::collections::TryReserveError) -> std::io::Error {
                std::io::ErrorKind::OutOfMemory.into()
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

                Ok(())
            }
            fn admit_operations(&self, _: u64) -> std::io::Result<()> { self.check() }
            fn record_operations(&self, _: u64) -> std::io::Result<()> {

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
            fn record_write_operations(&self, _: u64) -> std::io::Result<()> {

                Ok(())
            }
            fn admit_durability(&self) -> std::io::Result<()> { self.check() }
        }

        async fn seeded() -> MemoryResult<MemoryStream> {
            let provider = MemoryStream::default();
            provider.append(AppendRequest {
                path: path("native/read")?,
                records: vec![OwnedBytes::from_static(b"aa"), OwnedBytes::from_static(b"bb"), OwnedBytes::from_static(b"cc")],
                if_tail: Some(0), idempotency_key: None,
            }).await?;
            Ok(provider)
        }

        async fn prepared_fixture(limits: MemoryLimits) -> MemoryResult<(MemoryStream, CommitRequest)> {
            let provider = MemoryStream::new(limits);
            for name in ["completion/owner", "completion/target"] {
                provider.append(AppendRequest {
                    path: path(name)?, records: vec![OwnedBytes::from_static(b"seed")],
                    if_tail: Some(0), idempotency_key: None,
                }).await?;
            }
            let request = CommitRequest {
                conditions: vec![
                    CommitCondition::Tail { path: path("completion/owner")?, expected: 1 },
                    CommitCondition::Tail { path: path("completion/target")?, expected: 1 },
                ],
                mutations: vec![CommitMutation::Append {
                    path: path("completion/target")?, records: vec![OwnedBytes::new()],
                }],
                idempotency_key: key(b"prepared-completion")?,
            };
            Ok((provider, request))
        }

        fn fill_prepared_fixture(request: &mut CommitRequest, body: &'static [u8]) {
            let CommitMutation::Append { records, .. } = &mut request.mutations[0] else { unreachable!() };
            records[0] = OwnedBytes::from_static(body);
        }

        #[tokio::test]
        async fn guarded_cas_drops_contended_continuation_before_wait_and_rechecks_original_guard() -> MemoryResult<()> {
            let (provider, mut request) = prepared_fixture(MemoryLimits::default()).await?;
            fill_prepared_fixture(&mut request, b"contention-safe");
            let meter = Arc::new(Meter::default());
            let observer = MemoryObserver { meter: Some(meter.original()) };
            let calls = Arc::new(AtomicU64::new(0));
            let mut before = || {
                let state = Arc::clone(&provider.state);
                let calls = Arc::clone(&calls);
                async move {
                    let read = state.read_owned().await;
                    let held = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Some(read)
                    } else {
                        drop(read);
                        None
                    };
                    Ok::<_, MemoryFailure>(held)
                }
            };
            let (completion, _, continuation) = provider.apply_commit(
                &mut request, None, &observer, Some(&mut before),
            ).await??;
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert!(matches!(continuation, Some(None)));
            assert!(matches!(completion.value, CommitOutcome::Committed(_)));
            assert_eq!(provider.tail(path("completion/target")?).await?, 2);
            Ok(())
        }

        #[tokio::test]
        async fn public_guarded_commit_binds_current_clock_and_keeps_exact_cached_replay_time() -> MemoryResult<()> {
            let clock = Arc::new(TestClock(AtomicU64::new(100)));
            let provider = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
            for name in ["completion/owner", "completion/target"] {
                provider.append(AppendRequest {
                    path: path(name)?, records: vec![OwnedBytes::from_static(b"seed")],
                    if_tail: Some(0), idempotency_key: None,
                }).await?;
            }
            let request = CommitRequest {
                conditions: vec![
                    CommitCondition::Tail { path: path("completion/owner")?, expected: 1 },
                    CommitCondition::Tail { path: path("completion/target")?, expected: 1 },
                ],
                mutations: vec![CommitMutation::Append {
                    path: path("completion/target")?, records: vec![OwnedBytes::from_static(b"real-guarded-body")],
                }],
                idempotency_key: key(b"guarded-clock")?,
            };
            let meter = Arc::new(Meter::default());
            let guarded_provider = provider.clone();
            let guarded_meter = Arc::clone(&meter);
            let guarded_clock = Arc::clone(&clock);
            let before = move || {
                let provider = guarded_provider.clone();
                let meter = Arc::clone(&guarded_meter);
                let clock = Arc::clone(&guarded_clock);
                async move {
                    let tail = provider.native_tail(path("completion/target")?, meter.original()).await??;
                    assert_eq!(*tail.value(), 1);
                    clock.0.store(125, Ordering::SeqCst);
                    Ok::<(), MemoryFailure>(())
                }
            };
            let output = provider.native_commit_guarded(request.clone(), None, meter.original(), before).await??;
            let CommitOutcome::Committed(envelope) = output.value() else { panic!("canonical Commit") };
            let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else { panic!("single append") };
            assert_eq!(append.records[0].committed_at_micros, 125_000);
            let retained = provider.read_commit(envelope.commit_id).await?;
            assert_eq!(&retained, envelope);
            let mut rows = provider.read(ReadRequest {
                path: path("completion/target")?, from: 1, limit: 1,
            }).await?;
            assert_eq!(rows.next().await.ok_or(StreamError::Unavailable)??.committed_at_micros, 125_000);
            let replay_clock = Arc::clone(&clock);
            let replay = provider.native_commit_guarded(request, None, meter.original(), move || {
                replay_clock.0.store(999, Ordering::SeqCst);
                std::future::ready(Ok::<(), MemoryFailure>(()))
            }).await??;
            assert_eq!(replay.value(), output.value(), "guarded replay preserves original record time");
            assert_eq!(provider.state.read().await.last_committed_at_micros, 125_000);
            Ok(())
        }

        #[tokio::test]
        async fn canonical_revision_exhaustion_is_closed_and_cleanup_keeps_real_capacity_refunds() -> MemoryResult<()> {
            let (provider, mut template) = prepared_fixture(MemoryLimits::default()).await?;
            let meter = Arc::new(Meter::default());
            let prepared = provider.prepare_commit(&mut template, 64, meter.original()).await??;
            let identity = Arc::clone(&provider.state.read().await.prepared_capacity);
            identity.revision.store(u64::MAX, Ordering::Release);
            drop(prepared);
            assert_eq!(identity.revision.load(Ordering::Acquire), u64::MAX);
            assert_eq!(identity.commits.load(Ordering::Acquire), 0);
            assert_eq!(identity.records.load(Ordering::Acquire), 0);
            assert_eq!(identity.payload_bytes.load(Ordering::Acquire), 0);
            assert_eq!(identity.replays.load(Ordering::Acquire), 0);
            assert_eq!(identity.retained.load(Ordering::Acquire), 0);
            fill_prepared_fixture(&mut template, b"must-not-authorize-overflow");
            let observer = MemoryObserver { meter: Some(meter.original()) };
            let calls = AtomicU64::new(0);
            let mut before = || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok::<(), MemoryFailure>(()))
            };
            let allocations = meter.allocations.load(Ordering::SeqCst);
            let result = provider.apply_commit(&mut template, None, &observer, Some(&mut before)).await;
            assert!(matches!(result,
                Err(MemoryFailure::Provider(StreamError::Capacity))));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            assert_eq!(provider.tail(path("completion/target")?).await?, 1);
            Ok(())
        }

        #[tokio::test]
        async fn public_prepared_future_keeps_original_payload_through_the_last_empty_alias() -> MemoryResult<()> {
            struct PayloadGuard {
                retired: Arc<AtomicBool>,
                _allocation: ReadAllocation,
            }
            impl Drop for PayloadGuard {
                fn drop(&mut self) { self.retired.store(true, Ordering::SeqCst); }
            }
            let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
            let meter = Arc::new(Meter::default());
            let original = meter.original();
            let retired = Arc::new(AtomicBool::new(false));
            let mut allocation = ReadAllocation::new(&original, 64)?;
            let mut data = Vec::new();
            data.try_reserve_exact(64).map_err(|error| original.allocation_failed(error))?;
            allocation.reconcile_capacity(data.capacity())?;
            data.resize(data.capacity(), 0);
            let mut builder = crate::PreparedOwnedBytes::new(data, PayloadGuard {
                retired: Arc::clone(&retired), _allocation: allocation,
            }, Arc::clone(&original)).map_err(|failure| match failure.into_error() {
                crate::PreparedOwnedBytesError::Admission(error) => MemoryFailure::Observer(error),
                crate::PreparedOwnedBytesError::NotFullyInitialized
                    => MemoryFailure::Provider(StreamError::InvalidArgument),
            })?;
            let issued = provider.native_prepare_commit(request, 64, original).await??;
            let (mut issued, packet_guard) = issued.into_parts();
            let recovery = issued.take_recovery_future().ok_or(StreamError::Unavailable)?;
            assert!(issued.take_recovery_future().is_none());
            let allocations = meter.allocations.load(Ordering::SeqCst);
            builder.writable()[..9].copy_from_slice(b"root-body");
            meter.cancelled.store(true, Ordering::SeqCst);
            let body = builder.freeze(9).map_err(|_| StreamError::InvalidArgument)?;
            let last_empty_alias = body.slice(..0);
            let output = issued.finish(body).await??;
            let CommitOutcome::Committed(envelope) = output.value() else { panic!("canonical Commit result") };
            let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else { panic!("single append") };
            assert_eq!(append.records[0].value.as_ref(), b"root-body");
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
            assert_eq!(meter.pending_write_operations.load(Ordering::SeqCst), 0);
            let observed = recovery.await??;
            let observation = observed.value().as_ref().ok_or(StreamError::Unavailable)?;
            let IdempotencyOutcome::Commit(recovered) = &observation.outcome else { panic!("original Commit family") };
            assert_eq!(recovered, output.value());
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
            assert_eq!(meter.pending_read_operations.load(Ordering::SeqCst), 0);
            drop(observed);
            assert!(!retired.load(Ordering::SeqCst));
            drop(output);
            drop(packet_guard);
            drop(provider);
            assert!(last_empty_alias.is_empty());
            assert!(!retired.load(Ordering::SeqCst), "the empty view still owns its full physical backing");
            assert!(meter.held.load(Ordering::SeqCst) > 0);
            drop(last_empty_alias);
            assert!(retired.load(Ordering::SeqCst));
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            Ok(())
        }

        #[tokio::test]
        async fn prebuilt_recovery_rejects_actual_foreign_family_and_larger_commit_without_growth() -> MemoryResult<()> {
            for larger_commit in [false, true] {
                let (provider, request) = prepared_fixture(MemoryLimits::default()).await?;
                let foreign_key = request.idempotency_key.clone();
                let meter = Arc::new(Meter::default());
                let issued = provider.native_prepare_commit(request, 64, meter.original()).await??;
                let (mut issued, packet_guard) = issued.into_parts();
                let recovery = issued.take_recovery_future().ok_or(StreamError::Unavailable)?;
                let published = if larger_commit {
                    let outcome = provider.commit(CommitRequest {
                        // Each real Append mutation needs its own current-tail guard.
                        conditions: vec![
                            CommitCondition::Tail { path: path("completion/owner")?, expected: 1 },
                            CommitCondition::Tail { path: path("completion/target")?, expected: 1 },
                        ],
                        mutations: vec![
                            CommitMutation::Append { path: path("completion/owner")?,
                                records: vec![OwnedBytes::from_static(b"foreign-owner")] },
                            CommitMutation::Append { path: path("completion/target")?,
                                records: vec![OwnedBytes::from_static(b"foreign-target")] },
                        ],
                        idempotency_key: foreign_key.clone(),
                    }).await?;
                    let CommitOutcome::Committed(envelope) = &outcome else {
                        panic!("the larger foreign Commit must actually publish")
                    };
                    assert_eq!(envelope.mutations.len(), 2);
                    assert_eq!(provider.tail(path("completion/owner")?).await?, 2);
                    IdempotencyOutcome::Commit(outcome)
                } else {
                    let outcome = provider.append(AppendRequest {
                        path: path("completion/target")?, records: vec![OwnedBytes::from_static(b"foreign-append")],
                        if_tail: Some(1), idempotency_key: Some(foreign_key.clone()),
                    }).await?;
                    assert!(matches!(&outcome, AppendOutcome::Committed(_)));
                    IdempotencyOutcome::Append(outcome)
                };
                assert_eq!(provider.tail(path("completion/target")?).await?, 2);
                let retained = provider.inspect_idempotency(foreign_key).await?
                    .ok_or(StreamError::Unavailable)?;
                assert_eq!(retained.outcome, published);
                drop(retained);
                drop(published);
                let allocations = meter.allocations.load(Ordering::SeqCst);
                meter.cancelled.store(true, Ordering::SeqCst);
                let failure = match recovery.await? {
                    Err(failure) => failure,
                    Ok(_) => panic!("a closed recovery plan cannot pretend this foreign outcome is absent"),
                };
                assert!(if larger_commit {
                    matches!(&failure, StreamError::LimitExceeded)
                } else {
                    matches!(&failure, StreamError::IdempotencyMismatch)
                });
                assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
                assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
                assert_eq!(meter.pending_read_operations.load(Ordering::SeqCst), 0);
                drop(failure);
                drop(issued);
                drop(packet_guard);
                assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
                assert_eq!(meter.pending_write_operations.load(Ordering::SeqCst), 0);
                drop(provider);
                assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            }
            Ok(())
        }

        #[tokio::test]
        async fn preaccepted_completion_survives_cancellation_and_lost_ack_with_exact_commit_replay() -> MemoryResult<()> {
            let (provider, mut request) = prepared_fixture(MemoryLimits::default()).await?;
            let mut repeat = request.clone();
            let mut mismatch = request.clone();
            let meter = Arc::new(Meter { late_write_error: true, ..Meter::default() });
            let prepared = provider.prepare_commit(&mut request, 64, meter.original()).await??;
            let repeat_prepared = provider.prepare_commit(&mut repeat, 64, meter.original()).await??;
            let mismatch_prepared = provider.prepare_commit(&mut mismatch, 64, meter.original()).await??;
            let allocation_count = meter.allocations.load(Ordering::SeqCst);
            let input_pointer = request.mutations.as_ptr();
            fill_prepared_fixture(&mut request, b"actual-scalars");
            fill_prepared_fixture(&mut repeat, b"actual-scalars");
            fill_prepared_fixture(&mut mismatch, b"different-scalars");
            meter.cancelled.store(true, Ordering::SeqCst);
            let completion = provider.finish_commit(prepared, &mut request).await??;
            assert!(matches!(completion.1.as_ref(), Some(error) if error.kind() == std::io::ErrorKind::Interrupted));
            let CommitOutcome::Committed(envelope) = &completion.0.value else { panic!("real canonical publication") };
            let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else { panic!("single append") };
            assert_eq!(append.records[0].value, OwnedBytes::from_static(b"actual-scalars"));
            assert_eq!(append.start, 1);
            assert_eq!(append.end, 2);
            assert_eq!(input_pointer, request.mutations.as_ptr());
            let replay = provider.finish_commit(repeat_prepared, &mut repeat).await??;
            assert_eq!(&replay.0.value, &completion.0.value);
            assert!(matches!(provider.finish_commit(
                mismatch_prepared, &mut mismatch,
            ).await?, Err(StreamError::IdempotencyMismatch)));
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocation_count);
            assert_eq!(provider.state.read().await.replays.get(request.idempotency_key.as_bytes())
                .ok_or(StreamError::Unavailable)?.digest, commit_digest(&request, &mut DigestWork::Live(&MemoryObserver::default()))?);
            assert_eq!(meter.pending_write.load(Ordering::SeqCst), 0);
            assert_eq!(meter.pending_write_operations.load(Ordering::SeqCst), 0);
            drop(replay);
            drop(completion);
            assert!(meter.held.load(Ordering::SeqCst) > 0, "published controls still own original custody");
            drop(provider);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            Ok(())
        }

        #[tokio::test]
        async fn prepared_completion_competing_tail_keeps_input_and_canonical_conflict() -> MemoryResult<()> {
            let (provider, mut request) = prepared_fixture(MemoryLimits::default()).await?;
            let meter = Arc::new(Meter::default());
            let prepared = provider.prepare_commit(&mut request, 64, meter.original()).await??;
            provider.append(AppendRequest {
                path: path("completion/owner")?, records: vec![OwnedBytes::from_static(b"competing")],
                if_tail: Some(1), idempotency_key: None,
            }).await?;
            fill_prepared_fixture(&mut request, b"settled-prefix");
            let input = request.clone();
            let allocations = meter.allocations.load(Ordering::SeqCst);
            meter.cancelled.store(true, Ordering::SeqCst);
            let completion = provider.finish_commit(prepared, &mut request).await??;
            let CommitOutcome::Conflict(conflicts) = &completion.0.value else { panic!("truthful conflict") };
            assert!(matches!(conflicts.as_slice(), [CommitConflict::Tail { expected: 1, actual: Some(2), .. }]));
            assert_eq!(request, input);
            assert_eq!(provider.tail(path("completion/target")?).await?, 1);
            assert_eq!(provider.commit(request).await?, completion.0.value);
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            Ok(())
        }

        #[tokio::test]
        async fn prepared_completion_rejects_retired_history_even_with_unchanged_tail() -> MemoryResult<()> {
            let (provider, mut request) = prepared_fixture(MemoryLimits::default()).await?;
            let meter = Arc::new(Meter::default());
            let prepared = provider.prepare_commit(&mut request, 64, meter.original()).await??;
            {
                let mut state = provider.state.write().await;
                let target = state.paths.get_mut("completion/target").ok_or(StreamError::NotFound)?;
                let old = target.history.take();
                target.history = Some(Arc::new(History::Prefix {
                    source: old, tail: target.tail, _allocation: Allocation::default(),
                }));
            }
            fill_prepared_fixture(&mut request, b"whole-live-prefix");
            let input = request.clone();
            let allocations = meter.allocations.load(Ordering::SeqCst);
            meter.cancelled.store(true, Ordering::SeqCst);
            assert!(matches!(provider.finish_commit(
                prepared, &mut request,
            ).await?, Err(StreamError::PrefixNotRetained)));
            assert_eq!(request, input);
            assert_eq!(provider.tail(path("completion/target")?).await?, 1);
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            assert!(!provider.state.read().await.replays.contains_key(request.idempotency_key.as_bytes()));
            Ok(())
        }

        #[tokio::test]
        async fn unrelated_stock_mutations_cannot_consume_preaccepted_resident_capacity() -> MemoryResult<()> {
            let limits = MemoryLimits { commits: 3, records: 3, payload_bytes: 72,
                idempotency_results: 1, ..MemoryLimits::default() };
            let (provider, mut request) = prepared_fixture(limits).await?;
            let meter = Arc::new(Meter::default());
            let prepared = provider.prepare_commit(&mut request, 64, meter.original()).await??;
            assert!(matches!(provider.append(AppendRequest {
                path: path("unrelated")?, records: vec![OwnedBytes::new()], if_tail: Some(0), idempotency_key: None,
            }).await, Err(StreamError::Capacity)));
            assert!(matches!(provider.fork(ForkRequest {
                source: path("completion/owner")?, destination: path("unrelated")?,
                at_tail: Some(1), idempotency_key: None,
            }).await, Err(StreamError::Capacity)));
            let unrelated = CommitRequest {
                conditions: vec![CommitCondition::Absent { path: path("unrelated")? }],
                mutations: vec![CommitMutation::Append { path: path("unrelated")?, records: vec![OwnedBytes::new()] }],
                idempotency_key: key(b"unrelated")?,
            };
            assert!(matches!(provider.commit(unrelated).await, Err(StreamError::Capacity)));
            fill_prepared_fixture(&mut request, b"settled");
            meter.cancelled.store(true, Ordering::SeqCst);
            let completion = provider.finish_commit(prepared, &mut request).await??;
            assert!(matches!(&completion.0.value, CommitOutcome::Committed(_)));
            assert_eq!(provider.state.read().await.commits.len(), 3);
            Ok(())
        }

        fn read_request() -> Result<ReadRequest, StreamError> {
            Ok(ReadRequest { path: path("native/read")?, from: 0, limit: 3 })
        }



        #[test]
        fn append_size_and_digest_match_canonical_optional_scalar_fields() -> Result<(), StreamError> {
            let wire_records = vec![Bytes::new(), Bytes::from(vec![0; 128])];
            for tail in [None, Some(0), Some(127), Some(128), Some(u64::MAX)] {
                let request = AppendRequest {
                    path: path("native/append")?,
                    records: wire_records.iter().cloned().map(OwnedBytes::from).collect(),
                    if_tail: tail, idempotency_key: Some(key(b"native-append")?),
                };
                let wire = crate::wire::AppendRequest {
                    path: request.path.to_string(), records: wire_records.clone(),
                    if_tail: tail, idempotency_key: request.idempotency_key.as_ref()
                        .map(|key| Bytes::copy_from_slice(key.as_bytes())),
                };
                assert_eq!(crate::wire_codec::CanonicalWire::Append(&request).encoded_len(None, None)
                    .expect("ordinary sizing"), wire.encoded_len());
                let mut old = request_hasher(b"append");
                hash_path(&mut old, &request.path);
                old.update(tail.unwrap_or(u64::MAX).to_le_bytes());
                old.update((request.records.len() as u64).to_le_bytes());
                for record in &request.records {
                    old.update((record.len() as u64).to_le_bytes());
                    old.update(record);
                }
                let expected: [u8; 32] = old.finalize().into();
                assert_eq!(append_digest(&request, &MemoryObserver::default()).expect("ordinary hashing"), expected);
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
                assert_eq!(first.value().value, OwnedBytes::from_static(b"aa"));
                let alias = first.value().value.clone();
                drop(first);
                let error = output.value_mut().next().await.ok_or(StreamError::Unavailable)?;
                assert!(matches!(error, Err(error) if error.kind() == if cancellation {
                    std::io::ErrorKind::Interrupted
                } else { std::io::ErrorKind::OutOfMemory }));
                assert!(output.value_mut().next().await.is_none());
                assert_eq!(meter.read.load(Ordering::SeqCst), 2);
                assert_eq!(meter.pending_read.load(Ordering::SeqCst), 0);
                assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
                assert!(meter.held.load(Ordering::SeqCst) > 0);
                drop(output);
                assert_eq!(meter.held.load(Ordering::SeqCst), 0);
                assert_eq!(alias, OwnedBytes::from_static(b"aa"));
            }
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
                            records: vec![OwnedBytes::from_static(b"rollback")],
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
                    records: vec![OwnedBytes::copy_from_slice(body)],
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
        crate::IdempotencyKey::new(OwnedBytes::from_static(value))
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
                        records: vec![OwnedBytes::new(), OwnedBytes::from(vec![0; 128])],
                    },
                    CommitMutation::Fork {
                        source: path("sizing/source")?,
                        destination: path("sizing/destination")?,
                        at_tail: expected,
                        records: vec![OwnedBytes::from_static(b"alias")],
                    },
                ],
                idempotency_key: key(b"sizing")?,
            };
            assert_eq!(
                crate::wire_codec::CanonicalWire::Commit(&request).encoded_len(None, None)
                    .expect("ordinary size inspection has no fallible observer"),
                crate::wire_codec::commit_to_wire(&request).encoded_len(),
            );
        }
        Ok(())
    }

    #[test]
    fn command_size_uses_exact_protobuf_encoding() -> Result<(), StreamError> {
        let wire_records = vec![Bytes::from(vec![0_u8; MAX_RECORD_BYTES]); 16];
        let records: Vec<_> = wire_records.iter().cloned().map(OwnedBytes::from).collect();
        let mut low = 1_usize;
        let mut high = crate::MAX_PATH_BYTES;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let path = StreamPath::new("x".repeat(middle))?;
            let wire = crate::wire::AppendRequest {
                path: path.to_string(),
                records: wire_records.clone(),
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
            records: wire_records,
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
                    records: vec![OwnedBytes::from_static(b"bound")],
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
                records: vec![OwnedBytes::from_static(b"root")],
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
                records: vec![OwnedBytes::from_static(b"accepted")],
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
                records: vec![OwnedBytes::from_static(b"retry")],
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
    async fn single_record_reads_seek_deep_history_and_prefixes() -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let source = path("seek/source")?;
        for index in 0..256_u64 {
            provider
                .append(AppendRequest {
                    path: source.clone(),
                    records: vec![OwnedBytes::from(index.to_string())],
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
            assert_eq!(record.value.as_ref(), expected);
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
                records: vec![OwnedBytes::from_static(b"ready")],
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
                records: vec![OwnedBytes::from_static(b"source-progress")],
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
            records: vec![OwnedBytes::from_static(b"one")],
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
                    records: vec![OwnedBytes::from_static(b"two")],
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
            .map(|value| OwnedBytes::from(vec![value]))
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
            provider
                .append(AppendRequest {
                    path: destination.clone(),
                    records: vec![OwnedBytes::from_static(b"own"); 2],
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
                        OwnedBytes::from(vec![u8::try_from(sequence).unwrap_or_default()])
                    } else {
                        OwnedBytes::from_static(b"own")
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
            records: vec![OwnedBytes::from_static(name)],
            if_tail: None,
            idempotency_key: Some(
                crate::IdempotencyKey::new(OwnedBytes::from_static(name))
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
                            records: vec![OwnedBytes::from_static(b"x")],
                            if_tail: None,
                            idempotency_key: None,
                        })
                        .await?;
                }
                let log = path("log")?;
                let values: Vec<_> = (0..records).map(|value| OwnedBytes::from(value.to_string())).collect();
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
