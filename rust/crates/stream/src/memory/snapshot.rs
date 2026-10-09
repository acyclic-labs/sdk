//! The whole state machine as one self-contained encoding, from which a durable provider
//! restarts instead of replaying every command it ever ran.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message as _;
use tokio::sync::watch;

use super::{History, MemoryStream, PathState, Replay, Retained, State, recount};
use crate::wire_codec::{envelope_wire, observation_wire};
use crate::{CommitId, IdempotencyKey, IdempotencyObservation, Record, StreamError, StreamPath};

const MAGIC: &[u8] = b"ACYCLIC-STREAM-STATE-V1\0";

const NODE_BATCH: u8 = 0;
const NODE_PREFIX: u8 = 1;
const RETAINS_REPLAY: u8 = 1;
const RETAINS_COMMIT: u8 = 2;

impl MemoryStream {
    /// Encodes the whole state.
    pub(crate) async fn encode_state(&self) -> Vec<u8> {
        let state = self.state.read().await;
        encode(&state)
    }

    /// Replaces the whole state with one [`Self::encode_state`] encoded.
    ///
    /// # Errors
    ///
    /// Fails with [`StreamError::InvalidArgument`], changing nothing, when
    /// `encoded` is not such an encoding. Fails with [`StreamError::Capacity`],
    /// also changing nothing, when the decoded state exceeds this provider's limits.
    pub(crate) async fn install_state(&self, encoded: &[u8]) -> Result<(), StreamError> {
        let decoded = decode(encoded).ok_or(StreamError::InvalidArgument)?;
        if decoded.paths.len() > self.limits.paths
            || decoded.path_bytes > self.limits.path_bytes
            || decoded.record_count > self.limits.records
            || decoded.payload_bytes > self.limits.payload_bytes
            || decoded.commits.len() > self.limits.commits
            || decoded.replays.len() > self.limits.idempotency_results
        {
            return Err(StreamError::Capacity);
        }
        *self.state.write().await = decoded;
        Ok(())
    }
}

fn encode(state: &State) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    put_u64(&mut out, state.decision);
    put_u64(&mut out, state.last_committed_at_micros);
    out.extend_from_slice(state.hierarchy_version.as_bytes());
    // Each node after every node it reaches, so decoding links backwards.
    let mut indices = HashMap::new();
    let mut nodes = Vec::new();
    for path in state.paths.values() {
        let mut chain = Vec::new();
        let mut cursor = path.history.clone();
        while let Some(node) = cursor {
            if indices.contains_key(&Arc::as_ptr(&node)) {
                break;
            }
            cursor = match node.as_ref() {
                History::Batch { parent, .. } => parent.clone(),
                History::Prefix { source, .. } => source.clone(),
            };
            chain.push(node);
        }
        for node in chain.into_iter().rev() {
            indices.insert(Arc::as_ptr(&node), nodes.len());
            nodes.push(node);
        }
    }
    let reference = |node: &Option<Arc<History>>| {
        node.as_ref()
            .and_then(|node| indices.get(&Arc::as_ptr(node)))
            .map_or(0, |index| index + 1)
    };
    put_len(&mut out, nodes.len());
    for node in &nodes {
        match node.as_ref() {
            History::Batch { parent, records } => {
                out.push(NODE_BATCH);
                put_len(&mut out, reference(parent));
                put_len(&mut out, records.len());
                for record in records.iter() {
                    put_u64(&mut out, record.sequence);
                    out.extend_from_slice(record.commit_id.as_bytes());
                    put_u64(&mut out, record.committed_at_micros);
                    put_bytes(&mut out, &record.value);
                }
            }
            History::Prefix { source, tail } => {
                out.push(NODE_PREFIX);
                put_len(&mut out, reference(source));
                put_u64(&mut out, *tail);
            }
        }
    }
    put_len(&mut out, state.paths.len());
    for (path, stream) in &state.paths {
        put_bytes(&mut out, path.as_str().as_bytes());
        put_len(&mut out, reference(&stream.history));
        put_u64(&mut out, stream.tail);
    }
    put_len(&mut out, state.retained.len());
    for retained in &state.retained {
        let replay = retained
            .replay
            .as_ref()
            .and_then(|key| Some((key, state.replays.get(key)?)));
        let commit = retained
            .commit
            .as_ref()
            .and_then(|commit| state.commits.get(commit));
        out.push(
            if replay.is_some() { RETAINS_REPLAY } else { 0 }
                | if commit.is_some() { RETAINS_COMMIT } else { 0 },
        );
        if let Some((key, replay)) = replay {
            let observation = observation_wire(IdempotencyObservation {
                idempotency_key: IdempotencyKey::new(key.clone())
                    .unwrap_or_else(|_| unreachable!("retained keys were admitted")),
                request_digest: replay.digest,
                outcome: replay.result.clone(),
            });
            put_bytes(&mut out, &observation.encode_to_vec());
        }
        if let Some(envelope) = commit {
            put_bytes(&mut out, &envelope_wire(envelope.clone()).encode_to_vec());
        }
    }
    out
}

fn decode(encoded: &[u8]) -> Option<State> {
    let mut input = Input(encoded.strip_prefix(MAGIC)?);
    let decision = input.u64()?;
    let last_committed_at_micros = input.u64()?;
    let hierarchy_version = CommitId::from_bytes(input.array()?);
    let nodes = decode_nodes(&mut input)?;
    let (paths, path_bytes) = decode_paths(&mut input, &nodes.0)?;
    let mut state = State::default();
    state.paths = paths;
    state.path_bytes = path_bytes;
    state.decision = decision;
    state.last_committed_at_micros = last_committed_at_micros;
    state.hierarchy_version = hierarchy_version;
    decode_retained(&mut input, &mut state)?;
    if !input.0.is_empty() {
        return None;
    }
    // The first-party codec has one canonical encoding. This also rejects
    // unknown protobuf fields, duplicate fields, reordered paths, and unused nodes.
    if encode(&state) != encoded {
        return None;
    }
    for path in state.paths.keys() {
        if path
            .parent()
            .is_some_and(|parent| !state.paths.contains_key(&parent))
        {
            return None;
        }
    }
    if nodes.0.iter().any(|node| match node.as_ref() {
        History::Batch { records, .. } => records
            .iter()
            .any(|record| record.committed_at_micros > last_committed_at_micros),
        History::Prefix { .. } => false,
    }) {
        return None;
    }
    // Every successful current operation retains one immutable envelope; no
    // expiry, trim or deletion can make the decision counter diverge.
    if state.decision != u64::try_from(state.commits.len()).ok()? {
        return None;
    }
    validate_inventory(&state)?;
    recount(&mut state);
    Some(state)
}

// Index each path's own immutable nodes once, stopping at its fork Prefix.
// Match envelopes in retained publication order against these facts. Forks
// capture pre-commit source heads; all destination updates publish together.
// This checks chronology and inventory without re-executing commands.
fn validate_inventory(state: &State) -> Option<()> {
    let mut batches = BTreeMap::new();
    let mut prefixes = BTreeMap::new();
    let mut seen = HashSet::new();
    for (path, stream) in &state.paths {
        let mut cursor = stream.history.as_ref();
        while let Some(node) = cursor {
            let pointer = Arc::as_ptr(node);
            if !seen.insert(pointer) {
                return None;
            }
            match node.as_ref() {
                History::Batch { parent, records } => {
                    let fact = (records.as_ref(), pointer, parent.as_ref().map(Arc::as_ptr));
                    if batches
                        .insert((path, records.first()?.commit_id), fact)
                        .is_some()
                    {
                        return None;
                    }
                    cursor = parent.as_ref();
                }
                History::Prefix { source, tail } => {
                    prefixes.insert(path, (source.as_ref().map(Arc::as_ptr), *tail, pointer));
                    break;
                }
            }
        }
    }
    let mut heads = BTreeMap::new();
    for retained in &state.retained {
        let Some(commit) = retained.commit else {
            continue;
        };
        let envelope = state.commits.get(&commit)?;
        let mut pending = Vec::with_capacity(envelope.mutations.len());
        for mutation in &envelope.mutations {
            let (path, records, parent) = match mutation {
                crate::CommittedMutation::Append(append) => (
                    &append.path,
                    &append.records,
                    heads.get(&append.path).copied().flatten(),
                ),
                crate::CommittedMutation::Fork(fork) => {
                    if heads.contains_key(&fork.destination) {
                        return None;
                    }
                    let source_head = *heads.get(&fork.source)?;
                    let (source, tail, prefix) = prefixes.remove(&fork.destination)?;
                    if source != source_head || tail != fork.forked_at {
                        return None;
                    }
                    (&fork.destination, &fork.records, Some(prefix))
                }
            };
            let head = if records.is_empty() {
                parent
            } else {
                let (stored, node, stored_parent) = batches.remove(&(path, commit))?;
                if stored != records.as_slice() || stored_parent != parent {
                    return None;
                }
                Some(node)
            };
            pending.push((path, head));
        }
        for (path, head) in pending {
            let mut ancestor = path.clone();
            loop {
                let (known, _) = state.paths.get_key_value(&ancestor)?;
                if heads.contains_key(known) {
                    break;
                }
                heads.insert(known, None);
                let Some(parent) = ancestor.parent() else {
                    break;
                };
                ancestor = parent;
            }
            heads.insert(path, head);
        }
    }
    if !batches.is_empty() || !prefixes.is_empty() || heads.len() != state.paths.len() {
        return None;
    }
    state
        .paths
        .iter()
        .all(|(path, stream)| {
            heads.get(path).copied() == Some(stream.history.as_ref().map(Arc::as_ptr))
        })
        .then_some(())
}

/// The node an encoded reference names: 0 for none, otherwise one after
/// its index.
fn reference(nodes: &[Arc<History>], index: usize) -> Option<Option<Arc<History>>> {
    match index {
        0 => Some(None),
        index => nodes.get(index - 1).cloned().map(Some),
    }
}

// Release decoder-owned nodes from children to parents, including on failure.
// Otherwise a malformed long chain can recurse through Arc destruction.
struct DecodedNodes(Vec<Arc<History>>);

impl Drop for DecodedNodes {
    fn drop(&mut self) {
        while let Some(node) = self.0.pop() {
            if let Ok(history) = Arc::try_unwrap(node) {
                match history {
                    History::Batch { parent, .. } | History::Prefix { source: parent, .. } => {
                        drop(parent);
                    }
                }
            }
        }
    }
}

fn history_tail(history: Option<&Arc<History>>) -> Option<u64> {
    match history.map(AsRef::as_ref) {
        None => Some(0),
        Some(History::Batch { records, .. }) => records.last()?.sequence.checked_add(1),
        Some(History::Prefix { tail, .. }) => Some(*tail),
    }
}

fn decode_nodes(input: &mut Input<'_>) -> Option<DecodedNodes> {
    let count = input.len()?;
    let mut nodes = DecodedNodes(Vec::with_capacity(count.min(input.0.len())));
    for _ in 0..count {
        let tag = input.u8()?;
        let parent = reference(&nodes.0, input.len()?)?;
        let node = match tag {
            NODE_BATCH => {
                let count = input.len()?;
                if count == 0 || count > crate::MAX_ITEMS {
                    return None;
                }
                let mut sequence = history_tail(parent.as_ref())?;
                let mut records = Vec::with_capacity(count.min(input.0.len()));
                for _ in 0..count {
                    let record = Record {
                        sequence: input.u64()?,
                        commit_id: CommitId::from_bytes(input.array()?),
                        committed_at_micros: input.u64()?,
                        value: Bytes::copy_from_slice(input.bytes()?),
                    };
                    if record.sequence != sequence || record.value.len() > crate::MAX_RECORD_BYTES {
                        return None;
                    }
                    sequence = sequence.checked_add(1)?;
                    records.push(record);
                }
                History::Batch {
                    parent,
                    records: Arc::from(records),
                }
            }
            NODE_PREFIX => {
                let tail = input.u64()?;
                if tail > history_tail(parent.as_ref())? {
                    return None;
                }
                History::Prefix {
                    source: parent,
                    tail,
                }
            }
            _ => return None,
        };
        nodes.0.push(Arc::new(node));
    }
    Some(nodes)
}

fn decode_paths(
    input: &mut Input<'_>,
    nodes: &[Arc<History>],
) -> Option<(BTreeMap<StreamPath, PathState>, usize)> {
    let mut paths = BTreeMap::new();
    let mut path_bytes = 0_usize;
    for _ in 0..input.len()? {
        let path = input.path()?;
        let history = reference(nodes, input.len()?)?;
        let tail = input.u64()?;
        if tail != history_tail(history.as_ref())? || paths.contains_key(&path) {
            return None;
        }
        let (changed, _) = watch::channel(tail);
        path_bytes = path_bytes.checked_add(path.as_str().len())?;
        paths.insert(
            path,
            PathState {
                history,
                tail,
                changed,
            },
        );
    }
    Some((paths, path_bytes))
}

// A current command is bounded by MAX_COMMAND_BYTES and each protobuf bytes
// record costs at least two command bytes. Its retained record adds fewer than
// 96 bytes of IDs, timestamps, sequence and framing, so 64x command bytes is
// conservative even for empty records. Each mutation/conflict additionally
// receives two maximum paths and 128 bytes of framing/range metadata. The
// outer observation adds its maximum key and 128 bytes of digest/framing.
// This is a writer-derived bound, independent of configured memory capacity.
fn retained_fact_bound() -> Option<usize> {
    crate::MAX_COMMAND_BYTES
        .checked_mul(64)?
        .checked_add(
            crate::MAX_ITEMS
                .checked_mul(crate::MAX_PATH_BYTES.checked_mul(2)?.checked_add(128)?)?,
        )?
        .checked_add(crate::MAX_IDEMPOTENCY_KEY_BYTES)?
        .checked_add(128)
}

fn decode_retained(input: &mut Input<'_>, state: &mut State) -> Option<()> {
    for _ in 0..input.len()? {
        let flags = input.u8()?;
        if flags == 0 || flags & !(RETAINS_REPLAY | RETAINS_COMMIT) != 0 {
            return None;
        }
        let replay = if flags & RETAINS_REPLAY == 0 {
            None
        } else {
            let bytes = input.bytes()?;
            let observation =
                crate::persistence::decode_observation(bytes, retained_fact_bound()?).ok()?;
            let key = Bytes::copy_from_slice(observation.idempotency_key.as_bytes());
            if state.replays.contains_key(&key) {
                return None;
            }
            state.replays.insert(
                key.clone(),
                Replay {
                    digest: observation.request_digest,
                    result: observation.outcome,
                },
            );
            Some(key)
        };
        let commit = if flags & RETAINS_COMMIT == 0 {
            None
        } else {
            let bytes = input.bytes()?;
            let envelope =
                crate::persistence::decode_envelope(bytes, retained_fact_bound()?).ok()?;
            let commit_id = envelope.commit_id;
            if state.commits.insert(commit_id, envelope).is_some() {
                return None;
            }
            Some(commit_id)
        };
        if let Some(key) = &replay {
            let result = &state.replays.get(key)?.result;
            let result_commit = replay_commit_id(state, result)?;
            if result_commit != commit {
                return None;
            }
        }
        state.retained.push_back(Retained { replay, commit });
    }
    Some(())
}

fn replay_commit_id(state: &State, result: &crate::IdempotencyOutcome) -> Option<Option<CommitId>> {
    let commit = match result {
        crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) => {
            let envelope = state.commits.get(&receipt.commit_id)?;
            let [crate::CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                return None;
            };
            if (receipt.start, receipt.end, receipt.tail) != (append.start, append.end, append.tail)
            {
                return None;
            }
            Some(receipt.commit_id)
        }
        crate::IdempotencyOutcome::Fork(receipt) => {
            let envelope = state.commits.get(&receipt.commit_id)?;
            let [crate::CommittedMutation::Fork(fork)] = envelope.mutations.as_slice() else {
                return None;
            };
            if receipt.source != fork.source
                || receipt.destination != fork.destination
                || receipt.forked_at != fork.forked_at
                || receipt.tail != fork.tail
                || !fork.records.is_empty()
            {
                return None;
            }
            Some(receipt.commit_id)
        }
        crate::IdempotencyOutcome::Commit(crate::CommitOutcome::Committed(envelope)) => {
            if state.commits.get(&envelope.commit_id) != Some(envelope) {
                return None;
            }
            Some(envelope.commit_id)
        }
        crate::IdempotencyOutcome::Append(crate::AppendOutcome::TailConflict { .. })
        | crate::IdempotencyOutcome::Commit(crate::CommitOutcome::Conflict(_)) => None,
    };
    Some(commit)
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_len(out: &mut Vec<u8>, value: usize) {
    put_u64(out, value as u64);
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_len(out, value.len());
    out.extend_from_slice(value);
}

struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.0.split_at_checked(count)?;
        self.0 = rest;
        Some(taken)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.array::<1>()?[0])
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.array()?))
    }

    fn len(&mut self) -> Option<usize> {
        usize::try_from(self.u64()?).ok()
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let count = self.len()?;
        self.take(count)
    }

    fn path(&mut self) -> Option<StreamPath> {
        let bytes = self.bytes()?;
        StreamPath::new(std::str::from_utf8(bytes).ok()?).ok()
    }
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "Malformed snapshot fixtures deliberately modify and truncate encoded bytes"
)]
mod tests {
    use super::*;
    use crate::{AppendRequest, MemoryLimits, ReadRequest, StreamProvider, UnixMillisClock};
    use futures::StreamExt as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct TestClock(AtomicU64);

    impl UnixMillisClock for TestClock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn snapshots_retain_retry_identity_at_capacity() -> Result<(), StreamError> {
        let limits = MemoryLimits {
            idempotency_results: 1,
            ..MemoryLimits::default()
        };
        let stream = MemoryStream::new(limits);
        let request = AppendRequest {
            path: StreamPath::new("snapshot/retained")?,
            records: vec![Bytes::from_static(b"once")],
            if_tail: None,
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"snapshot-once"))?),
        };
        let first = stream.append(request.clone()).await?;
        let restored = MemoryStream::new(limits);
        restored.install_state(&stream.encode_state().await).await?;
        assert_eq!(restored.append(request.clone()).await?, first);
        assert_eq!(restored.tail(request.path.clone()).await?, 1);
        let mut changed = request.clone();
        changed.records = vec![Bytes::from_static(b"different")];
        assert_eq!(
            restored.append(changed).await,
            Err(StreamError::IdempotencyMismatch)
        );
        let mut fresh = request;
        fresh.idempotency_key = Some(IdempotencyKey::new(Bytes::from_static(b"fresh"))?);
        assert_eq!(restored.append(fresh).await, Err(StreamError::Capacity));
        Ok(())
    }

    #[tokio::test]
    async fn real_append_fork_and_commit_snapshots_preserve_exclusive_tails_and_retries()
    -> Result<(), StreamError> {
        let stream = MemoryStream::default();
        let source = StreamPath::new("roundtrip/source")?;
        let destination = StreamPath::new("roundtrip/fork")?;
        let appended = AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"append"))?),
        };
        let append_result = stream.append(appended.clone()).await?;
        let forked = crate::ForkRequest {
            source: source.clone(),
            destination: destination.clone(),
            at_tail: Some(1),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"fork"))?),
        };
        let fork_result = stream.fork(forked.clone()).await?;
        let committed = crate::CommitRequest {
            conditions: vec![crate::CommitCondition::Tail {
                path: destination.clone(),
                expected: 1,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: destination.clone(),
                records: vec![Bytes::from_static(b"third")],
            }],
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"commit"))?,
        };
        let commit_result = stream.commit(committed.clone()).await?;
        let restored = MemoryStream::default();
        restored.install_state(&stream.encode_state().await).await?;
        assert_eq!(restored.append(appended).await?, append_result);
        assert_eq!(restored.fork(forked).await?, fork_result);
        assert_eq!(restored.commit(committed).await?, commit_result);
        for (path, expected) in [
            (source, vec![b"first".as_slice(), b"second".as_slice()]),
            (destination, vec![b"first".as_slice(), b"third".as_slice()]),
        ] {
            assert_eq!(restored.tail(path.clone()).await?, 2);
            let records = restored
                .read(ReadRequest {
                    path,
                    from: 0,
                    limit: 3,
                })
                .await?
                .collect::<Vec<_>>()
                .await;
            assert_eq!(records.len(), 2);
            for (index, (record, expected)) in records.into_iter().zip(expected).enumerate() {
                let record = record?;
                assert_eq!(
                    record.sequence,
                    u64::try_from(index).map_err(|_| StreamError::LimitExceeded)?
                );
                assert_eq!(record.value.as_ref(), expected);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn current_writer_empty_forks_and_atomic_source_changes_roundtrip()
    -> Result<(), StreamError> {
        let stream = MemoryStream::default();
        let root = StreamPath::new("writer/root")?;
        let parent = StreamPath::new("writer")?;
        let no_history = StreamPath::new("writer/no-history")?;
        let source = StreamPath::new("writer/source")?;
        let empty = StreamPath::new("writer/empty")?;
        let destination = StreamPath::new("writer/destination")?;
        let append = |path: StreamPath, value: &'static [u8]| AppendRequest {
            path,
            records: vec![Bytes::from_static(value)],
            if_tail: None,
            idempotency_key: None,
        };
        stream.append(append(root.clone(), b"root")).await?;
        stream
            .fork(crate::ForkRequest {
                source: parent.clone(),
                destination: no_history.clone(),
                at_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        stream
            .append(append(parent.clone(), b"parent-later"))
            .await?;
        stream
            .fork(crate::ForkRequest {
                source: root.clone(),
                destination: source.clone(),
                at_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        stream
            .fork(crate::ForkRequest {
                source: source.clone(),
                destination: empty.clone(),
                at_tail: None,
                idempotency_key: None,
            })
            .await?;
        stream.append(append(source.clone(), b"before")).await?;
        let request = crate::CommitRequest {
            conditions: vec![
                crate::CommitCondition::Tail {
                    path: source.clone(),
                    expected: 1,
                },
                crate::CommitCondition::Absent {
                    path: destination.clone(),
                },
            ],
            mutations: vec![
                crate::CommitMutation::Append {
                    path: source.clone(),
                    records: vec![Bytes::from_static(b"after")],
                },
                crate::CommitMutation::Fork {
                    source: source.clone(),
                    destination: destination.clone(),
                    at_tail: 1,
                    records: vec![Bytes::from_static(b"forked")],
                },
            ],
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"atomic-fork"))?,
        };
        let outcome = stream.commit(request.clone()).await?;
        let restored = MemoryStream::default();
        restored.install_state(&stream.encode_state().await).await?;
        assert_eq!(restored.commit(request).await?, outcome);
        assert_eq!(restored.tail(empty).await?, 0);
        assert_eq!(restored.tail(no_history).await?, 0);
        for (path, expected) in [
            (root, vec![b"root".as_slice()]),
            (parent, vec![b"parent-later".as_slice()]),
            (source, vec![b"before".as_slice(), b"after".as_slice()]),
            (
                destination,
                vec![b"before".as_slice(), b"forked".as_slice()],
            ),
        ] {
            assert_eq!(
                restored.tail(path.clone()).await?,
                u64::try_from(expected.len()).map_err(|_| StreamError::LimitExceeded)?
            );
            let records = restored
                .read(ReadRequest {
                    path,
                    from: 0,
                    limit: 3,
                })
                .await?
                .collect::<Vec<_>>()
                .await;
            assert_eq!(records.len(), expected.len());
            for (record, expected) in records.into_iter().zip(expected) {
                assert_eq!(record?.value.as_ref(), expected);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn malformed_and_over_capacity_snapshots_leave_live_state_unchanged()
    -> Result<(), StreamError> {
        let stream = MemoryStream::new(MemoryLimits::default());
        stream
            .append(AppendRequest {
                path: StreamPath::new("snapshot/atomic")?,
                records: vec![Bytes::from_static(b"kept")],
                if_tail: None,
                idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"kept"))?),
            })
            .await?;
        let valid = stream.encode_state().await;
        for length in 0..valid.len() {
            assert_eq!(
                stream.install_state(&valid[..length]).await,
                Err(StreamError::InvalidArgument)
            );
            assert_eq!(stream.encode_state().await, valid);
        }
        let mut impossible_decision = valid.clone();
        impossible_decision[MAGIC.len()..MAGIC.len() + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(
            stream.install_state(&impossible_decision).await,
            Err(StreamError::InvalidArgument)
        );
        let mut trailing = valid.clone();
        trailing.push(0);
        assert_eq!(
            stream.install_state(&trailing).await,
            Err(StreamError::InvalidArgument)
        );
        let mut wrong_magic = valid.clone();
        if let Some(first) = wrong_magic.first_mut() {
            *first ^= 1;
        }
        assert_eq!(
            stream.install_state(&wrong_magic).await,
            Err(StreamError::InvalidArgument)
        );
        // Reject obsolete version markers explicitly, without changing live state.
        for version in *b"024" {
            let mut obsolete = valid.clone();
            obsolete[MAGIC.len() - 2] = version;
            assert_eq!(
                stream.install_state(&obsolete).await,
                Err(StreamError::InvalidArgument)
            );
            assert_eq!(stream.encode_state().await, valid);
        }
        for limits in [
            MemoryLimits {
                paths: 0,
                ..MemoryLimits::default()
            },
            MemoryLimits {
                path_bytes: 0,
                ..MemoryLimits::default()
            },
            MemoryLimits {
                records: 0,
                ..MemoryLimits::default()
            },
            MemoryLimits {
                payload_bytes: 0,
                ..MemoryLimits::default()
            },
            MemoryLimits {
                commits: 0,
                ..MemoryLimits::default()
            },
            MemoryLimits {
                idempotency_results: 0,
                ..MemoryLimits::default()
            },
        ] {
            let constrained = MemoryStream::new(limits);
            let empty = constrained.encode_state().await;
            assert_eq!(
                constrained.install_state(&valid).await,
                Err(StreamError::Capacity)
            );
            assert_eq!(constrained.encode_state().await, empty);
        }
        assert_eq!(stream.encode_state().await, valid);
        Ok(())
    }

    #[test]
    fn current_codec_rejects_duplicate_entries_and_impossible_history() {
        let header = || {
            let mut bytes = MAGIC.to_vec();
            put_u64(&mut bytes, 0);
            put_u64(&mut bytes, 0);
            bytes.extend_from_slice(CommitId::from_bytes([0; 32]).as_bytes());
            bytes
        };
        let mut duplicate = header();
        put_len(&mut duplicate, 0);
        put_len(&mut duplicate, 2);
        for _ in 0..2 {
            put_bytes(&mut duplicate, b"duplicate");
            put_len(&mut duplicate, 0);
            put_u64(&mut duplicate, 0);
        }
        put_len(&mut duplicate, 0);
        assert!(decode(&duplicate).is_none());
        let mut impossible = header();
        put_len(&mut impossible, 1);
        impossible.push(NODE_PREFIX);
        put_len(&mut impossible, 0);
        put_u64(&mut impossible, 1);
        assert!(decode(&impossible).is_none());
        let mut unknown_flags = header();
        put_len(&mut unknown_flags, 0);
        put_len(&mut unknown_flags, 0);
        put_len(&mut unknown_flags, 1);
        unknown_flags.push(4);
        assert!(decode(&unknown_flags).is_none());
    }

    #[test]
    fn current_codec_rejects_duplicate_retry_identity_and_unknown_wire_fields()
    -> Result<(), StreamError> {
        let observation = observation_wire(IdempotencyObservation {
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"conflict"))?,
            request_digest: [0; 32],
            outcome: crate::IdempotencyOutcome::Append(crate::AppendOutcome::TailConflict {
                actual_tail: 0,
            }),
        })
        .encode_to_vec();
        let fixture = |count: usize, observation: &[u8]| {
            let mut encoded = MAGIC.to_vec();
            put_u64(&mut encoded, 0);
            put_u64(&mut encoded, 0);
            encoded.extend_from_slice(CommitId::from_bytes([0; 32]).as_bytes());
            put_len(&mut encoded, 0);
            put_len(&mut encoded, 0);
            put_len(&mut encoded, count);
            for _ in 0..count {
                encoded.push(RETAINS_REPLAY);
                put_bytes(&mut encoded, observation);
            }
            encoded
        };
        assert!(decode(&fixture(1, &observation)).is_some());
        assert!(decode(&fixture(2, &observation)).is_none());
        let mut unknown = observation;
        unknown.extend_from_slice(&[0xa0, 0x06, 0x01]);
        assert!(decode(&fixture(1, &unknown)).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn retained_facts_reject_impossible_receipts_records_and_phantom_payloads()
    -> Result<(), StreamError> {
        let stream = MemoryStream::default();
        let key = IdempotencyKey::new(Bytes::from_static(b"inventory"))?;
        stream
            .append(AppendRequest {
                path: StreamPath::new("inventory/path")?,
                records: vec![Bytes::from_static(b"real")],
                if_tail: Some(0),
                idempotency_key: Some(key.clone()),
            })
            .await?;
        let mut state = stream.state.write().await;
        let replay = state
            .replays
            .get_mut(key.as_bytes())
            .ok_or(StreamError::Unavailable)?;
        let original_result = replay.result.clone();
        let crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) =
            &mut replay.result
        else {
            return Err(StreamError::Unavailable);
        };
        receipt.start = receipt.end;
        assert!(decode(&encode(&state)).is_none());
        state
            .replays
            .get_mut(key.as_bytes())
            .ok_or(StreamError::Unavailable)?
            .result = original_result.clone();
        let replay = state
            .replays
            .get_mut(key.as_bytes())
            .ok_or(StreamError::Unavailable)?;
        let crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) =
            &mut replay.result
        else {
            return Err(StreamError::Unavailable);
        };
        receipt.start = 1;
        receipt.end = 2;
        receipt.tail = 2;
        assert!(decode(&encode(&state)).is_none());
        state
            .replays
            .get_mut(key.as_bytes())
            .ok_or(StreamError::Unavailable)?
            .result = original_result;
        let commit_id = *state
            .commits
            .keys()
            .next()
            .ok_or(StreamError::Unavailable)?;
        let envelope = state
            .commits
            .get_mut(&commit_id)
            .ok_or(StreamError::Unavailable)?;
        let Some(crate::CommittedMutation::Append(append)) = envelope.mutations.first_mut() else {
            return Err(StreamError::Unavailable);
        };
        let record = append.records.first_mut().ok_or(StreamError::Unavailable)?;
        record.sequence = 9;
        assert!(decode(&encode(&state)).is_none());
        let envelope = state
            .commits
            .get_mut(&commit_id)
            .ok_or(StreamError::Unavailable)?;
        let Some(crate::CommittedMutation::Append(append)) = envelope.mutations.first_mut() else {
            return Err(StreamError::Unavailable);
        };
        let record = append.records.first_mut().ok_or(StreamError::Unavailable)?;
        record.sequence = 0;
        record.value = Bytes::from_static(b"phantom payload absent from immutable history");
        assert!(decode(&encode(&state)).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn retained_empty_forks_cannot_reference_future_destinations() -> Result<(), StreamError>
    {
        let stream = MemoryStream::default();
        let root = StreamPath::new("chronology")?;
        let first = StreamPath::new("chronology/first")?;
        let later = StreamPath::new("chronology/later")?;
        stream
            .append(AppendRequest {
                path: StreamPath::new("chronology/seed")?,
                records: vec![Bytes::from_static(b"seed")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let receipt = stream
            .fork(crate::ForkRequest {
                source: root,
                destination: first.clone(),
                at_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        stream
            .fork(crate::ForkRequest {
                source: first,
                destination: later.clone(),
                at_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        assert!(decode(&stream.encode_state().await).is_some());
        let mut state = stream.state.write().await;
        let envelope = state
            .commits
            .get_mut(&receipt.commit_id)
            .ok_or(StreamError::Unavailable)?;
        let Some(crate::CommittedMutation::Fork(fork)) = envelope.mutations.first_mut() else {
            return Err(StreamError::Unavailable);
        };
        fork.source = later;
        assert!(decode(&encode(&state)).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn retained_fork_cannot_capture_a_future_source_head() -> Result<(), StreamError> {
        let stream = MemoryStream::default();
        let source = StreamPath::new("chronology/source")?;
        let destination = StreamPath::new("chronology/destination")?;
        let append = |value: &'static [u8]| AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(value)],
            if_tail: None,
            idempotency_key: None,
        };
        stream.append(append(b"before")).await?;
        stream
            .fork(crate::ForkRequest {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: Some(1),
                idempotency_key: None,
            })
            .await?;
        stream.append(append(b"future")).await?;
        assert!(decode(&stream.encode_state().await).is_some());
        let mut state = stream.state.write().await;
        let future = state
            .paths
            .get(&source)
            .ok_or(StreamError::Unavailable)?
            .history
            .clone();
        state
            .paths
            .get_mut(&destination)
            .ok_or(StreamError::Unavailable)?
            .history = Some(Arc::new(History::Prefix {
            source: future,
            tail: 1,
        }));
        assert!(decode(&encode(&state)).is_none());
        Ok(())
    }

    #[test]
    fn malformed_deep_history_releases_without_recursive_drop() {
        let mut encoded = MAGIC.to_vec();
        put_u64(&mut encoded, 0);
        put_u64(&mut encoded, 0);
        encoded.extend_from_slice(CommitId::from_bytes([0; 32]).as_bytes());
        put_len(&mut encoded, 100_001);
        for index in 0..100_000 {
            encoded.push(NODE_PREFIX);
            put_len(&mut encoded, index);
            put_u64(&mut encoded, 0);
        }
        encoded.push(255);
        put_len(&mut encoded, 100_000);
        assert!(decode(&encoded).is_none());
    }

    #[tokio::test]
    async fn snapshots_preserve_committed_timestamps() -> Result<(), StreamError> {
        let clock = Arc::new(TestClock::default());
        clock.0.store(100, Ordering::SeqCst);
        let stream = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
        let path = StreamPath::new("snapshot/timed")?;
        stream
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"one")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let timed = stream.encode_state().await;
        clock.0.store(90, Ordering::SeqCst);
        let restored = MemoryStream::new_with_clock(MemoryLimits::default(), clock);
        restored.install_state(&timed).await?;
        let timed_record = restored
            .read(ReadRequest {
                path: path.clone(),
                from: 0,
                limit: 1,
            })
            .await?
            .next()
            .await
            .ok_or(StreamError::InvalidArgument)??;
        assert_eq!(timed_record.committed_at_micros, 100_000);
        let path = StreamPath::new("snapshot/after-rollback")?;
        restored
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"two")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let next = restored
            .read(ReadRequest {
                path,
                from: 0,
                limit: 1,
            })
            .await?
            .next()
            .await
            .ok_or(StreamError::InvalidArgument)??;
        assert_eq!(next.committed_at_micros, 100_000);
        Ok(())
    }
}
