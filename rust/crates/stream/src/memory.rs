//! Single-lock deterministic reference state machine; no durability or availability claim.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use sha2::{Digest, Sha256};
use tokio::sync::{RwLock, watch};

#[cfg(feature = "local")]
mod snapshot;

use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, Child, ChildStream, ChildrenRequest,
    CommitCondition, CommitConflict, CommitId, CommitMutation, CommitOutcome, CommitRequest,
    CommittedAppend, CommittedDelete, CommittedEnvelope, CommittedFork, CommittedMutation,
    CommittedTrim, DeleteReceipt, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    IdempotencyOutcome, MAX_COMMAND_BYTES, MAX_ITEMS, MAX_RECORD_BYTES,
    MIN_IDEMPOTENCY_RETENTION_SECS, ReadRequest, Record, RecordStream, StreamError, StreamPath,
    StreamProvider, SystemUnixMillisClock, TrimReceipt, UnixMillisClock,
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
    /// Maximum replay identities retained at once; each is kept for
    /// [`MIN_IDEMPOTENCY_RETENTION_SECS`] of the provider's retention clock.
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

/// How long, in retention-clock milliseconds, a result and its envelope are
/// kept after they are made.
const RETENTION_MILLIS: u64 = MIN_IDEMPOTENCY_RETENTION_SECS * 1000;

/// Deterministic process-local provider. It retains each idempotency result and committed
/// envelope for [`MIN_IDEMPOTENCY_RETENTION_SECS`], then forgets it; it deliberately claims no
/// crash recovery.
#[derive(Clone)]
pub struct MemoryStream {
    state: Arc<RwLock<State>>,
    limits: MemoryLimits,
    /// The trusted clock that deadlines are checked against.
    clock: Arc<dyn UnixMillisClock>,
    /// The clock retention is measured on; see [`MemoryStream::new_with_clocks`].
    retention: Arc<dyn UnixMillisClock>,
}

impl Default for MemoryStream {
    fn default() -> Self {
        Self::new(MemoryLimits::default())
    }
}

impl MemoryStream {
    /// Constructs one bounded independent provider.
    #[must_use]
    pub fn new(limits: MemoryLimits) -> Self {
        Self::new_with_clock(limits, Arc::new(SystemUnixMillisClock))
    }

    /// Constructs a provider with an injected trusted clock.
    #[must_use]
    pub fn new_with_clock(limits: MemoryLimits, clock: Arc<dyn UnixMillisClock>) -> Self {
        Self::new_with_clocks(limits, Arc::clone(&clock), clock)
    }

    /// Constructs a provider whose retention is measured on `retention`
    /// rather than on the deadline clock: a durable provider measures it on
    /// a clock that stands still while the provider is closed, and pins it
    /// while it replays, so replay forgets exactly what the original did.
    #[must_use]
    pub(crate) fn new_with_clocks(
        limits: MemoryLimits,
        clock: Arc<dyn UnixMillisClock>,
        retention: Arc<dyn UnixMillisClock>,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
            limits,
            clock,
            retention,
        }
    }

    /// Takes the state for one mutation, first forgetting every result and
    /// envelope whose retention ended; returns the state and the time the
    /// mutation happens at.
    async fn mutate(&self) -> (tokio::sync::RwLockWriteGuard<'_, State>, u64) {
        let mut state = self.state.write().await;
        let now = self.retention.now_unix_millis();
        expire(&mut state, now);
        (state, now)
    }

    async fn commit_inner(
        &self,
        mut request: CommitRequest,
        deadline_unix_millis: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        normalize_commit(&mut request)?;
        validate_commit_shape(&request)?;
        let digest = commit_digest(&request);
        let (mut state, now) = self.mutate().await;
        if let Some(result) = replay_commit(&state, &request.idempotency_key, digest)? {
            return Ok(result);
        }
        if deadline_unix_millis.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
            return Err(StreamError::DeadlineElapsed);
        }
        admit_replay(&state, Some(&request.idempotency_key), self.limits)?;
        let conflicts = commit_conflicts(&state, &request.conditions);
        if !conflicts.is_empty() {
            let result = CommitOutcome::Conflict(conflicts);
            retain(
                &mut state,
                now,
                None,
                Some(request.idempotency_key),
                digest,
                IdempotencyOutcome::Commit(result.clone()),
            );
            return Ok(result);
        }
        validate_commit_authority(&state, &request)?;
        if reserve_coordinated(&state, &request, self.limits).is_err() {
            recount(&mut state);
            reserve_coordinated(&state, &request, self.limits)?;
        }
        let commit_id = next_commit_id(&mut state, digest)?;
        let before = request
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                CommitMutation::Fork { source, .. } => state
                    .paths
                    .get(source)
                    .map(|stream| (source.clone(), (stream.history.clone(), stream.tail))),
                CommitMutation::Append { .. }
                | CommitMutation::Trim { .. }
                | CommitMutation::Delete { .. } => None,
            })
            .collect::<BTreeMap<_, _>>();
        let mutations = apply_coordinated(&mut state, request.mutations, &before, commit_id)?;
        let envelope = CommittedEnvelope {
            commit_id,
            mutations,
        };
        let result = CommitOutcome::Committed(envelope.clone());
        retain(
            &mut state,
            now,
            Some(envelope),
            Some(request.idempotency_key),
            digest,
            IdempotencyOutcome::Commit(result.clone()),
        );
        Ok(result)
    }
}

#[derive(Default)]
struct State {
    paths: BTreeMap<StreamPath, PathState>,
    /// Deleted paths, none beneath another: retiring a path retires all
    /// beneath it.
    retired: BTreeSet<StreamPath>,
    commits: BTreeMap<CommitId, CommittedEnvelope>,
    replays: BTreeMap<Bytes, Replay>,
    /// Every retained result and envelope, in the order its retention ends.
    retained: VecDeque<Retained>,
    path_bytes: usize,
    /// Records and payload bytes retained, each counted once however many
    /// paths share it; an upper bound between [`recount`]s.
    record_count: usize,
    payload_bytes: usize,
    decision: u64,
}

/// What one mutation retains until `until` on the retention clock.
struct Retained {
    until: u64,
    replay: Option<Bytes>,
    commit: Option<CommitId>,
}

struct PathState {
    history: Option<Arc<History>>,
    tail: u64,
    trim_point: u64,
    changed: watch::Sender<u64>,
}

enum History {
    Batch {
        parent: Option<Arc<Self>>,
        records: Arc<[Record]>,
    },
    Prefix {
        source: Option<Arc<Self>>,
        tail: u64,
    },
}

#[derive(Clone)]
struct Replay {
    digest: [u8; 32],
    result: IdempotencyOutcome,
}

#[async_trait]
impl StreamProvider for MemoryStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let state = self.state.read().await;
        Ok(state
            .replays
            .get(idempotency_key.as_bytes())
            .map(|replay| IdempotencyObservation {
                idempotency_key,
                request_digest: replay.digest,
                outcome: replay.result.clone(),
            }))
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        let state = self.state.read().await;
        reject_retired(&state, &path)?;
        state
            .paths
            .get(&path)
            .map(|stream| stream.tail)
            .ok_or(StreamError::NotFound)
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        validate_records(&request.records)?;
        validate_append_size(&request)?;
        let digest = append_digest(&request);
        let (mut state, now) = self.mutate().await;
        if let Some(result) = replay_append(&state, request.idempotency_key.as_ref(), digest)? {
            return Ok(result);
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        reject_retired(&state, &request.path)?;
        let actual_tail = state.paths.get(&request.path).map_or(0, |path| path.tail);
        if request
            .if_tail
            .is_some_and(|expected| expected != actual_tail)
        {
            let result = AppendOutcome::TailConflict { actual_tail };
            retain(
                &mut state,
                now,
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
        let records = build_records(actual_tail, request.records, commit_id)?;
        ensure_path(&mut state, &request.path);
        let resulting_tail = {
            let stream = state
                .paths
                .get_mut(&request.path)
                .ok_or(StreamError::Unavailable)?;
            stream.history = Some(Arc::new(History::Batch {
                parent: stream.history.clone(),
                records: Arc::from(records.clone()),
            }));
            stream.tail = records
                .last()
                .map_or(actual_tail, |record| record.sequence + 1);
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
            mutations: vec![CommittedMutation::Append(CommittedAppend {
                path: request.path,
                start: receipt.start,
                end: receipt.end,
                tail: receipt.tail,
                records,
            })],
        };
        let result = AppendOutcome::Committed(receipt);
        retain(
            &mut state,
            now,
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
        let (mut state, now) = self.mutate().await;
        if let Some(result) = replay_fork(&state, request.idempotency_key.as_ref(), digest)? {
            return Ok(result);
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        reject_retired(&state, &request.destination)?;
        if state.paths.contains_key(&request.destination) {
            return Err(StreamError::AlreadyExists);
        }
        let source = state
            .paths
            .get(&request.source)
            .ok_or(StreamError::NotFound)?;
        let forked_at = request.at_tail.unwrap_or(source.tail);
        if forked_at > source.tail {
            return Err(StreamError::PrefixNotRetained);
        }
        let source_history = source.history.clone();
        reserve_paths(&state, &request.destination, self.limits)?;
        reserve_commit(&state, self.limits)?;
        let commit_id = next_commit_id(&mut state, digest)?;
        ensure_path(&mut state, &request.destination);
        let destination = state
            .paths
            .get_mut(&request.destination)
            .ok_or(StreamError::Unavailable)?;
        destination.history = Some(Arc::new(History::Prefix {
            source: source_history,
            tail: forked_at,
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
            now,
            Some(envelope),
            request.idempotency_key,
            digest,
            IdempotencyOutcome::Fork(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn trim(
        &self,
        path: StreamPath,
        before: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TrimReceipt, StreamError> {
        let digest = trim_digest(&path, before);
        let (mut state, now) = self.mutate().await;
        if let Some(result) = replay_trim(&state, &idempotency_key, digest)? {
            return Ok(result);
        }
        admit_replay(&state, Some(&idempotency_key), self.limits)?;
        reserve_commit(&state, self.limits)?;
        let current = state.paths.get(&path).ok_or(StreamError::NotFound)?;
        if before > current.tail {
            return Err(StreamError::OutOfRange);
        }
        let trim_point = current.trim_point.max(before);
        let commit_id = next_commit_id(&mut state, digest)?;
        let stream = state.paths.get_mut(&path).ok_or(StreamError::Unavailable)?;
        stream.trim_point = trim_point;
        stream.changed.send_replace(stream.tail);
        let receipt = TrimReceipt {
            path: path.clone(),
            trim_point,
            commit_id,
        };
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Trim(CommittedTrim { path, trim_point })],
        };
        retain(
            &mut state,
            now,
            Some(envelope),
            Some(idempotency_key),
            digest,
            IdempotencyOutcome::Trim(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn delete(
        &self,
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    ) -> Result<DeleteReceipt, StreamError> {
        let digest = delete_digest(&path);
        let (mut state, now) = self.mutate().await;
        if let Some(result) = replay_delete(&state, &idempotency_key, digest)? {
            return Ok(result);
        }
        admit_replay(&state, Some(&idempotency_key), self.limits)?;
        reserve_commit(&state, self.limits)?;
        if !state.paths.contains_key(&path) {
            return Err(StreamError::NotFound);
        }
        if beneath(
            state
                .paths
                .range(path.clone()..)
                .map(|(candidate, _)| candidate),
            &path,
        )
        .next()
        .is_some()
        {
            return Err(StreamError::InvalidArgument);
        }
        let commit_id = next_commit_id(&mut state, digest)?;
        retire_path(&mut state, &path);
        let receipt = DeleteReceipt {
            path: path.clone(),
            commit_id,
        };
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Delete(CommittedDelete { path })],
        };
        retain(
            &mut state,
            now,
            Some(envelope),
            Some(idempotency_key),
            digest,
            IdempotencyOutcome::Delete(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        validate_limit(request.limit)?;
        let state = self.state.read().await;
        reject_retired(&state, &request.path)?;
        let path = state
            .paths
            .get(&request.path)
            .ok_or(StreamError::NotFound)?;
        if request.from > path.tail {
            return Err(StreamError::OutOfRange);
        }
        if request.from < path.trim_point {
            return Err(StreamError::OutOfRange);
        }
        let records = read_history(
            path.history.as_ref(),
            path.tail,
            request.from,
            usize::try_from(request.limit).map_err(|_| StreamError::LimitExceeded)?,
        );
        Ok(stream::iter(records.into_iter().map(Ok)).boxed())
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let (receiver, cursor) = {
            let state = self.state.read().await;
            reject_retired(&state, &path)?;
            let stream = state.paths.get(&path).ok_or(StreamError::NotFound)?;
            if from > stream.tail {
                return Err(StreamError::OutOfRange);
            }
            if from < stream.trim_point {
                return Err(StreamError::OutOfRange);
            }
            (
                stream.changed.subscribe(),
                HistoryCursor::new(stream.history.as_ref(), stream.tail, from),
            )
        };
        let state = Arc::clone(&self.state);
        Ok(stream::unfold(
            (state, path, receiver, cursor),
            |(state, path, mut receiver, mut cursor)| async move {
                loop {
                    let record = {
                        let guard = state.read().await;
                        if reject_retired(&guard, &path).is_err() {
                            Err(StreamError::Retired)
                        } else if let Some(current) = guard.paths.get(&path) {
                            if cursor.next < current.trim_point {
                                Err(StreamError::OutOfRange)
                            } else {
                                cursor.sync(current.history.as_ref(), current.tail);
                                Ok(cursor.next_record())
                            }
                        } else {
                            Err(StreamError::NotFound)
                        }
                    };
                    let Some(record) = (match record {
                        Ok(record) => record,
                        Err(error) => {
                            return Some((Err(error), (state, path, receiver, cursor)));
                        }
                    }) else {
                        if receiver.changed().await.is_err() {
                            return Some((
                                Err(StreamError::Unavailable),
                                (state, path, receiver, cursor),
                            ));
                        }
                        continue;
                    };
                    return Some((Ok(record), (state, path, receiver, cursor)));
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
                request.after.as_ref().is_none_or(|after| *path > after)
                    && request.parent.as_ref().map_or_else(
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
        self.state
            .read()
            .await
            .commits
            .get(&commit_id)
            .cloned()
            .ok_or(StreamError::NotFound)
    }
}

fn validate_limit(limit: u32) -> Result<(), StreamError> {
    if limit == 0 || usize::try_from(limit).map_or(true, |limit| limit > MAX_ITEMS) {
        Err(StreamError::LimitExceeded)
    } else {
        Ok(())
    }
}

fn validate_records(records: &[Bytes]) -> Result<(), StreamError> {
    if records.is_empty() || records.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded);
    }
    let mut total = 0_usize;
    for record in records {
        if record.len() > MAX_RECORD_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        total = total
            .checked_add(record.len())
            .and_then(|value| value.checked_add(8))
            .ok_or(StreamError::LimitExceeded)?;
    }
    if total > MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    Ok(())
}

fn add_size(total: &mut usize, amount: usize) -> Result<(), StreamError> {
    *total = total
        .checked_add(amount)
        .ok_or(StreamError::LimitExceeded)?;
    if *total > MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    Ok(())
}

fn add_path_size(total: &mut usize, path: &StreamPath) -> Result<(), StreamError> {
    add_size(total, 8)?;
    add_size(total, path.as_str().len())
}

fn add_records_size(total: &mut usize, records: &[Bytes]) -> Result<(), StreamError> {
    add_size(total, 8)?;
    for record in records {
        add_size(total, 8)?;
        add_size(total, record.len())?;
    }
    Ok(())
}

fn validate_append_size(request: &AppendRequest) -> Result<(), StreamError> {
    let mut total = 1 + 8 + 1;
    add_path_size(&mut total, &request.path)?;
    add_records_size(&mut total, &request.records)?;
    if let Some(key) = &request.idempotency_key {
        add_size(&mut total, 8 + key.as_bytes().len())?;
    }
    Ok(())
}

fn validate_fork_size(request: &ForkRequest) -> Result<(), StreamError> {
    let mut total = 1 + 8 + 1;
    add_path_size(&mut total, &request.source)?;
    add_path_size(&mut total, &request.destination)?;
    if let Some(key) = &request.idempotency_key {
        add_size(&mut total, 8 + key.as_bytes().len())?;
    }
    Ok(())
}

fn reject_retired(state: &State, path: &StreamPath) -> Result<(), StreamError> {
    let mut current = Some(path.clone());
    while let Some(path) = current {
        if state.retired.contains(&path) {
            return Err(StreamError::Retired);
        }
        current = path.parent();
    }
    Ok(())
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

fn ensure_path(state: &mut State, path: &StreamPath) {
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
                trim_point: 0,
                changed,
            },
        );
    }
}

fn build_records(
    start: u64,
    bodies: Vec<Bytes>,
    commit_id: CommitId,
) -> Result<Vec<Record>, StreamError> {
    bodies
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let sequence = start
                .checked_add(u64::try_from(index).map_err(|_| StreamError::LimitExceeded)?)
                .ok_or(StreamError::LimitExceeded)?;
            Ok(Record {
                sequence,
                value,
                commit_id,
            })
        })
        .collect()
}

struct HistoryWindow {
    records: Arc<[Record]>,
    start: usize,
    end: usize,
}

/// Incremental cursor over the immutable history graph used by `follow`.
///
/// A follow can live for a long time while one-record batches are appended.
/// Replaying the linked graph from its root for every item makes that workload
/// quadratic.  The cursor indexes the initial graph once and then only walks
/// the newly added batch suffix after each publication.
struct HistoryCursor {
    root: Option<Arc<History>>,
    windows: Vec<HistoryWindow>,
    window: usize,
    record: usize,
    next: u64,
}

impl HistoryCursor {
    fn new(history: Option<&Arc<History>>, tail: u64, next: u64) -> Self {
        let mut cursor = Self {
            root: None,
            windows: Vec::new(),
            window: 0,
            record: 0,
            next,
        };
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
                History::Batch { parent, records } => {
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
            History::Batch { parent, records } => {
                batches.push((Arc::clone(records), ceiling));
                cursor = parent.clone();
            }
            History::Prefix { source, tail } => {
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

fn history_window(records: Arc<[Record]>, ceiling: u64) -> Option<HistoryWindow> {
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

fn read_history(history: Option<&Arc<History>>, tail: u64, from: u64, limit: usize) -> Vec<Record> {
    // Follow asks for one record at a time.  Walking every historical batch and
    // materializing all of them makes that hot path quadratic as a stream grows.
    // A single-record read can seek through the newest immutable batches and
    // clone only the requested record.
    if limit == 1 {
        return read_one_history(history, tail, from).into_iter().collect();
    }
    let mut cursor = history.cloned();
    let mut ceiling = tail;
    let mut batches = Vec::new();
    while let Some(history) = cursor {
        match history.as_ref() {
            History::Batch { parent, records } => {
                batches.push((Arc::clone(records), ceiling));
                cursor = parent.clone();
            }
            History::Prefix { source, tail } => {
                ceiling = ceiling.min(*tail);
                cursor = source.clone();
            }
        }
    }
    let mut result = Vec::with_capacity(limit);
    for (records, ceiling) in batches.into_iter().rev() {
        for record in records.iter() {
            if record.sequence >= from && record.sequence < ceiling {
                result.push(record.clone());
                if result.len() == limit {
                    return result;
                }
            }
        }
    }
    result
}

fn read_one_history(history: Option<&Arc<History>>, tail: u64, from: u64) -> Option<Record> {
    let mut cursor = history.cloned();
    let mut ceiling = tail;
    while let Some(history) = cursor {
        match history.as_ref() {
            History::Batch { parent, records } => {
                let Some(first) = records.first() else {
                    cursor = parent.clone();
                    continue;
                };
                let batch_end = records
                    .len()
                    .try_into()
                    .ok()
                    .and_then(|length: u64| first.sequence.checked_add(length))
                    .unwrap_or(u64::MAX);
                let visible_end = ceiling.min(batch_end);
                if from >= first.sequence && from < visible_end {
                    let offset = usize::try_from(from - first.sequence).ok()?;
                    return records.get(offset).cloned();
                }
                ceiling = ceiling.min(first.sequence);
                cursor = parent.clone();
            }
            History::Prefix { source, tail } => {
                ceiling = ceiling.min(*tail);
                cursor = source.clone();
            }
        }
    }
    None
}

fn is_direct_child(parent: &StreamPath, candidate: &StreamPath) -> bool {
    candidate.parent().as_ref() == Some(parent)
}

/// The paths beneath `path` among `ordered`, which lists paths in order
/// from `path` on. They follow it at once but for siblings that extend its
/// last name with a byte sorting before `/`, so the walk ends at the first
/// path past them.
fn beneath<'a>(
    ordered: impl Iterator<Item = &'a StreamPath>,
    path: &'a StreamPath,
) -> impl Iterator<Item = &'a StreamPath> {
    let prefix = path.as_str().as_bytes();
    ordered
        .take_while(move |candidate| {
            let candidate = candidate.as_str().as_bytes();
            candidate.starts_with(prefix)
                && candidate.get(prefix.len()).is_none_or(|next| *next <= b'/')
        })
        .filter(move |candidate| is_descendant(path, candidate))
}

fn is_descendant(parent: &StreamPath, candidate: &StreamPath) -> bool {
    candidate
        .as_str()
        .strip_prefix(parent.as_str())
        .is_some_and(|suffix| suffix.starts_with('/'))
}

/// Forgets every result and envelope whose retention ended by `now`.
fn expire(state: &mut State, now: u64) {
    while state
        .retained
        .front()
        .is_some_and(|retained| retained.until <= now)
    {
        let Some(retained) = state.retained.pop_front() else {
            break;
        };
        if let Some(key) = retained.replay {
            state.replays.remove(&key);
        }
        if let Some(commit) = retained.commit {
            state.commits.remove(&commit);
        }
    }
}

/// Deletes `path`, which has no live descendants, for good.
fn retire_path(state: &mut State, path: &StreamPath) {
    if state.paths.remove(path).is_some() {
        state.path_bytes = state.path_bytes.saturating_sub(path.as_str().len());
    }
    // Retiring the path retires everything beneath it, retired or not.
    let retired = beneath(state.retired.range(path.clone()..), path)
        .cloned()
        .collect::<Vec<_>>();
    for candidate in retired {
        state.retired.remove(&candidate);
    }
    state.retired.insert(path.clone());
}

/// Counts exactly the records and payload bytes the live paths retain,
/// each shared history once.
fn recount(state: &mut State) {
    let mut seen = HashSet::new();
    let mut records = 0_usize;
    let mut bytes = 0_usize;
    for path in state.paths.values() {
        let mut cursor = path.history.clone();
        while let Some(node) = cursor {
            if !seen.insert(Arc::as_ptr(&node)) {
                break;
            }
            cursor = match node.as_ref() {
                History::Batch {
                    parent,
                    records: batch,
                } => {
                    records = records.saturating_add(batch.len());
                    bytes = bytes.saturating_add(
                        batch.iter().map(|record| record.value.len()).sum::<usize>(),
                    );
                    parent.clone()
                }
                History::Prefix { source, .. } => source.clone(),
            };
        }
    }
    state.record_count = records;
    state.payload_bytes = bytes;
}

fn next_commit_id(state: &mut State, digest: [u8; 32]) -> Result<CommitId, StreamError> {
    state.decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
    let mut hash = Sha256::new();
    hash.update(b"acyclic.stream.commit.v1\0");
    hash.update(state.decision.to_le_bytes());
    hash.update(digest);
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

fn replay_trim(
    state: &State,
    key: &IdempotencyKey,
    digest: [u8; 32],
) -> Result<Option<TrimReceipt>, StreamError> {
    match replay(state, Some(key), digest)? {
        Some(IdempotencyOutcome::Trim(result)) => Ok(Some(result)),
        Some(_) => Err(StreamError::IdempotencyMismatch),
        None => Ok(None),
    }
}

fn replay_delete(
    state: &State,
    key: &IdempotencyKey,
    digest: [u8; 32],
) -> Result<Option<DeleteReceipt>, StreamError> {
    match replay(state, Some(key), digest)? {
        Some(IdempotencyOutcome::Delete(result)) => Ok(Some(result)),
        Some(_) => Err(StreamError::IdempotencyMismatch),
        None => Ok(None),
    }
}

fn replay_commit(
    state: &State,
    key: &crate::IdempotencyKey,
    digest: [u8; 32],
) -> Result<Option<CommitOutcome>, StreamError> {
    match replay(state, Some(key), digest)? {
        Some(IdempotencyOutcome::Commit(result)) => Ok(Some(result)),
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

/// Retains what one mutation made at `now`: its envelope, when it
/// committed, and its result, when it has an identity to replay under.
fn retain(
    state: &mut State,
    now: u64,
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
        state.replays.insert(key.clone(), Replay { digest, result });
        key
    });
    if commit.is_some() || replay.is_some() {
        state.retained.push_back(Retained {
            until: now.saturating_add(RETENTION_MILLIS),
            replay,
            commit,
        });
    }
}

fn append_digest(request: &AppendRequest) -> [u8; 32] {
    let mut hash = request_hasher(b"append");
    hash_path(&mut hash, &request.path);
    hash.update(request.if_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash_records(&mut hash, &request.records);
    hash.finalize().into()
}

fn fork_digest(request: &ForkRequest) -> [u8; 32] {
    let mut hash = request_hasher(b"fork");
    hash_path(&mut hash, &request.source);
    hash_path(&mut hash, &request.destination);
    hash.update(request.at_tail.unwrap_or(u64::MAX).to_le_bytes());
    hash.finalize().into()
}

fn trim_digest(path: &StreamPath, before: u64) -> [u8; 32] {
    let mut hash = request_hasher(b"trim");
    hash_path(&mut hash, path);
    hash.update(before.to_le_bytes());
    hash.finalize().into()
}

fn delete_digest(path: &StreamPath) -> [u8; 32] {
    let mut hash = request_hasher(b"delete");
    hash_path(&mut hash, path);
    hash.finalize().into()
}

fn commit_digest(request: &CommitRequest) -> [u8; 32] {
    let mut hash = request_hasher(b"commit");
    for condition in &request.conditions {
        match condition {
            CommitCondition::Tail { path, expected } => {
                hash.update([1]);
                hash_path(&mut hash, path);
                hash.update(expected.to_le_bytes());
            }
            CommitCondition::Absent { path } => {
                hash.update([2]);
                hash_path(&mut hash, path);
            }
        }
    }
    for mutation in &request.mutations {
        match mutation {
            CommitMutation::Append { path, records } => {
                hash.update([1]);
                hash_path(&mut hash, path);
                hash_records(&mut hash, records);
            }
            CommitMutation::Fork {
                source,
                destination,
                at_tail,
                records,
            } => {
                hash.update([2]);
                hash_path(&mut hash, source);
                hash_path(&mut hash, destination);
                hash.update(at_tail.to_le_bytes());
                hash_records(&mut hash, records);
            }
            CommitMutation::Trim { path, before } => {
                hash.update([3]);
                hash_path(&mut hash, path);
                hash.update(before.to_le_bytes());
            }
            CommitMutation::Delete { path } => {
                hash.update([4]);
                hash_path(&mut hash, path);
            }
        }
    }
    hash.finalize().into()
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
        CommitMutation::Append { path, .. }
        | CommitMutation::Trim { path, .. }
        | CommitMutation::Delete { path } => path,
    }
}

fn normalize_commit(request: &mut CommitRequest) -> Result<(), StreamError> {
    if request.conditions.is_empty()
        || request.conditions.len() > MAX_ITEMS
        || request.mutations.is_empty()
        || request.mutations.len() > MAX_ITEMS
    {
        return Err(StreamError::LimitExceeded);
    }
    for mutation in &request.mutations {
        match mutation {
            CommitMutation::Append { records, .. } => validate_records(records)?,
            CommitMutation::Fork { records, .. } if !records.is_empty() => {
                validate_records(records)?;
            }
            CommitMutation::Fork { .. }
            | CommitMutation::Trim { .. }
            | CommitMutation::Delete { .. } => {}
        }
    }
    validate_commit_size(request)?;
    request.conditions.sort_by(|left, right| {
        condition_path(left)
            .cmp(condition_path(right))
            .then_with(|| {
                matches!(left, CommitCondition::Absent { .. })
                    .cmp(&matches!(right, CommitCondition::Absent { .. }))
            })
    });
    request
        .mutations
        .sort_by(|left, right| mutation_path(left).cmp(mutation_path(right)));
    if request
        .conditions
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if condition_path(left) == condition_path(right)))
        || request.mutations.windows(2).any(
            |pair| matches!(pair, [left, right] if mutation_path(left) == mutation_path(right)),
        )
    {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

fn validate_commit_size(request: &CommitRequest) -> Result<(), StreamError> {
    let mut total = 1 + 8 + 8 + 8 + request.idempotency_key.as_bytes().len();
    for condition in &request.conditions {
        add_size(&mut total, 1 + 8)?;
        add_path_size(&mut total, condition_path(condition))?;
    }
    for mutation in &request.mutations {
        add_size(&mut total, 1)?;
        match mutation {
            CommitMutation::Append { path, records } => {
                add_path_size(&mut total, path)?;
                add_records_size(&mut total, records)?;
            }
            CommitMutation::Fork {
                source,
                destination,
                records,
                ..
            } => {
                add_path_size(&mut total, source)?;
                add_path_size(&mut total, destination)?;
                add_size(&mut total, 8)?;
                add_records_size(&mut total, records)?;
            }
            CommitMutation::Trim { path, .. } => {
                add_path_size(&mut total, path)?;
                add_size(&mut total, 8)?;
            }
            CommitMutation::Delete { path } => add_path_size(&mut total, path)?,
        }
    }
    Ok(())
}

fn validate_commit_shape(request: &CommitRequest) -> Result<(), StreamError> {
    let conditions = request
        .conditions
        .iter()
        .map(|condition| (condition_path(condition), condition))
        .collect::<BTreeMap<_, _>>();
    for mutation in &request.mutations {
        match mutation {
            CommitMutation::Append { path, .. } => match conditions.get(path) {
                Some(CommitCondition::Tail { .. } | CommitCondition::Absent { .. }) => {}
                _ => return Err(StreamError::InvalidArgument),
            },
            CommitMutation::Fork { destination, .. } => {
                if !matches!(
                    conditions.get(destination),
                    Some(CommitCondition::Absent { .. })
                ) {
                    return Err(StreamError::InvalidArgument);
                }
            }
            CommitMutation::Trim { path, before: _ } | CommitMutation::Delete { path } => {
                if !matches!(conditions.get(path), Some(CommitCondition::Tail { .. })) {
                    return Err(StreamError::InvalidArgument);
                }
            }
        }
    }
    Ok(())
}

fn validate_commit_authority(state: &State, request: &CommitRequest) -> Result<(), StreamError> {
    for mutation in &request.mutations {
        match mutation {
            CommitMutation::Append { .. } => {}
            CommitMutation::Fork {
                source, at_tail, ..
            } => {
                let Some(source_state) = state.paths.get(source) else {
                    return Err(StreamError::NotFound);
                };
                if *at_tail > source_state.tail {
                    return Err(StreamError::InvalidArgument);
                }
            }
            CommitMutation::Trim { path, before } => {
                let Some(stream) = state.paths.get(path) else {
                    return Err(StreamError::NotFound);
                };
                if *before > stream.tail {
                    return Err(StreamError::InvalidArgument);
                }
            }
            CommitMutation::Delete { path } => {
                if !state.paths.contains_key(path)
                    || beneath(
                        state
                            .paths
                            .range(path.clone()..)
                            .map(|(candidate, _)| candidate),
                        path,
                    )
                    .next()
                    .is_some()
                {
                    return Err(StreamError::InvalidArgument);
                }
            }
        }
    }
    Ok(())
}

fn commit_conflicts(state: &State, conditions: &[CommitCondition]) -> Vec<CommitConflict> {
    conditions
        .iter()
        .filter_map(|condition| match condition {
            CommitCondition::Tail { path, expected } => match state.paths.get(path) {
                Some(stream) if stream.tail == *expected => None,
                Some(stream) => Some(CommitConflict::Tail {
                    path: path.clone(),
                    expected: *expected,
                    actual: Some(stream.tail),
                }),
                None => Some(CommitConflict::Tail {
                    path: path.clone(),
                    expected: *expected,
                    actual: None,
                }),
            },
            CommitCondition::Absent { path } => {
                if state.paths.contains_key(path) {
                    Some(CommitConflict::Exists { path: path.clone() })
                } else if reject_retired(state, path).is_err() {
                    Some(CommitConflict::Retired { path: path.clone() })
                } else {
                    None
                }
            }
        })
        .collect()
}

fn reserve_coordinated(
    state: &State,
    request: &CommitRequest,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    reserve_commit(state, limits)?;
    let mut new_paths = BTreeSet::new();
    let mut records = 0_usize;
    let mut bytes = 0_usize;
    for mutation in &request.mutations {
        let destination = mutation_path(mutation);
        let mut current = Some(destination.clone());
        while let Some(path) = current {
            if !state.paths.contains_key(&path) {
                new_paths.insert(path.clone());
            }
            current = path.parent();
        }
        if let CommitMutation::Append { records: batch, .. }
        | CommitMutation::Fork { records: batch, .. } = mutation
        {
            records = records.saturating_add(batch.len());
            bytes = bytes.saturating_add(batch.iter().map(Bytes::len).sum::<usize>());
        }
    }
    if state.paths.len().saturating_add(new_paths.len()) > limits.paths
        || state
            .path_bytes
            .saturating_add(new_paths.iter().map(|path| path.as_str().len()).sum())
            > limits.path_bytes
        || state.record_count.saturating_add(records) > limits.records
        || state.payload_bytes.saturating_add(bytes) > limits.payload_bytes
    {
        return Err(StreamError::Capacity);
    }
    Ok(())
}

fn apply_coordinated(
    state: &mut State,
    mutations: Vec<CommitMutation>,
    before: &BTreeMap<StreamPath, (Option<Arc<History>>, u64)>,
    commit_id: CommitId,
) -> Result<Vec<CommittedMutation>, StreamError> {
    let mut committed = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        match mutation {
            CommitMutation::Append { path, records } => {
                let start = state.paths.get(&path).map_or(0, |stream| stream.tail);
                let records = build_records(start, records, commit_id)?;
                ensure_path(state, &path);
                let resulting_tail = {
                    let stream = state.paths.get_mut(&path).ok_or(StreamError::Unavailable)?;
                    stream.history = Some(Arc::new(History::Batch {
                        parent: stream.history.clone(),
                        records: Arc::from(records.clone()),
                    }));
                    stream.tail = records.last().map_or(start, |record| record.sequence + 1);
                    stream.changed.send_replace(stream.tail);
                    stream.tail
                };
                state.record_count += records.len();
                state.payload_bytes += records
                    .iter()
                    .map(|record| record.value.len())
                    .sum::<usize>();
                committed.push(CommittedMutation::Append(CommittedAppend {
                    path,
                    start,
                    end: resulting_tail,
                    tail: resulting_tail,
                    records,
                }));
            }
            CommitMutation::Fork {
                source,
                destination,
                at_tail,
                records,
            } => {
                let (history, source_tail) = before.get(&source).ok_or(StreamError::NotFound)?;
                if at_tail > *source_tail {
                    return Err(StreamError::InvalidArgument);
                }
                let records = build_records(at_tail, records, commit_id)?;
                ensure_path(state, &destination);
                let tail = {
                    let stream = state
                        .paths
                        .get_mut(&destination)
                        .ok_or(StreamError::Unavailable)?;
                    let prefix = Arc::new(History::Prefix {
                        source: history.clone(),
                        tail: at_tail,
                    });
                    stream.history = Some(if records.is_empty() {
                        prefix
                    } else {
                        Arc::new(History::Batch {
                            parent: Some(prefix),
                            records: Arc::from(records.clone()),
                        })
                    });
                    stream.tail = records.last().map_or(at_tail, |record| record.sequence + 1);
                    stream.changed.send_replace(stream.tail);
                    stream.tail
                };
                state.record_count += records.len();
                state.payload_bytes += records
                    .iter()
                    .map(|record| record.value.len())
                    .sum::<usize>();
                committed.push(CommittedMutation::Fork(CommittedFork {
                    source,
                    destination,
                    forked_at: at_tail,
                    tail,
                    records,
                }));
            }
            CommitMutation::Trim { path, before } => {
                let stream = state.paths.get_mut(&path).ok_or(StreamError::NotFound)?;
                stream.trim_point = stream.trim_point.max(before);
                stream.changed.send_replace(stream.tail);
                committed.push(CommittedMutation::Trim(CommittedTrim {
                    path,
                    trim_point: stream.trim_point,
                }));
            }
            CommitMutation::Delete { path } => {
                if !state.paths.contains_key(&path) {
                    return Err(StreamError::NotFound);
                }
                retire_path(state, &path);
                committed.push(CommittedMutation::Delete(CommittedDelete { path }));
            }
        }
    }
    Ok(committed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

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
                    path: destination,
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

    /// A result and its envelope are kept for the retention period of the
    /// retention clock, then forgotten; the capacity they held is free again.
    #[tokio::test]
    async fn results_and_envelopes_expire_after_their_retention() -> Result<(), StreamError> {
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
        clock.0.store(RETENTION_MILLIS - 1, Ordering::SeqCst);
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

        clock.0.store(RETENTION_MILLIS, Ordering::SeqCst);
        assert!(matches!(
            provider.append(append(b"second")).await?,
            AppendOutcome::Committed(_)
        ));
        assert_eq!(provider.inspect_idempotency(key(b"first")?).await?, None);
        assert_eq!(
            provider.read_commit(first.commit_id).await,
            Err(StreamError::NotFound)
        );
        assert_eq!(provider.tail(path("retained")?).await?, 2, "records stay");
        Ok(())
    }

    /// Deleting a path frees what it held and folds the retirements beneath
    /// it into its own.
    #[tokio::test]
    async fn deleting_a_path_frees_it_and_folds_retirements_beneath() -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        for name in ["tree/a", "tree/b", "tree-x"] {
            provider
                .append(AppendRequest {
                    path: path(name)?,
                    records: vec![Bytes::from_static(b"record")],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await?;
        }
        provider.delete(path("tree-x")?, key(b"delete-x")?).await?;
        assert_eq!(
            provider.delete(path("tree")?, key(b"early")?).await,
            Err(StreamError::InvalidArgument),
            "the tree still has live paths"
        );
        provider.delete(path("tree/a")?, key(b"delete-a")?).await?;
        provider.delete(path("tree/b")?, key(b"delete-b")?).await?;
        provider.delete(path("tree")?, key(b"delete-tree")?).await?;
        let state = provider.state.read().await;
        assert!(state.paths.is_empty());
        assert_eq!(state.path_bytes, 0);
        assert_eq!(
            state.retired.iter().collect::<Vec<_>>(),
            vec![&path("tree")?, &path("tree-x")?],
            "a sibling sorting before the subtree stays"
        );
        drop(state);
        assert_eq!(
            provider.tail(path("tree/a")?).await,
            Err(StreamError::Retired)
        );
        Ok(())
    }

    /// A listing longer than its limit pages on from the last child listed.
    #[tokio::test]
    async fn children_page_after_the_last_listed() -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        for name in ["tree/a", "tree/b", "tree/c", "tree-x/d"] {
            provider
                .append(AppendRequest {
                    path: path(name)?,
                    records: vec![Bytes::from_static(b"record")],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await?;
        }
        let mut listed = Vec::new();
        let mut after = None;
        loop {
            let page = provider
                .children(ChildrenRequest {
                    parent: Some(path("tree")?),
                    limit: 2,
                    after: after.take(),
                })
                .await?
                .map(|child| child.map(|child| child.path))
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            after = page.last().cloned();
            let complete = page.len() < 2;
            listed.extend(page);
            if complete {
                break;
            }
        }
        assert_eq!(listed, vec![path("tree/a")?, path("tree/b")?, path("tree/c")?]);
        Ok(())
    }
}
