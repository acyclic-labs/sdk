//! The whole state machine as one self-contained encoding, from which a durable provider

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::OwnedBytes as Bytes;
use prost::Message as _;
use tokio::sync::watch;

use super::{History, MemoryStream, PathState, RecordBatch, Retained, State, retain};
use crate::wire_codec::{envelope_to_wire, observation_wire};
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
            History::Batch {
                parent, records, ..
            } => {
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
            History::Prefix { source, tail, .. } => {
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
        let replay = retained.replay.as_ref().map(|key| (key, retained));
        let commit = retained.envelope();
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
            put_bytes(
                &mut out,
                &envelope_to_wire(envelope.clone()).encode_to_vec(),
            );
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
    let (paths, path_bytes) = decode_paths(&mut input, &nodes)?;
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
    if nodes.iter().any(|node| match node.as_ref() {
        History::Batch { records, .. } => records
            .iter()
            .any(|record| record.committed_at_micros > last_committed_at_micros),
        History::Prefix { .. } => false,
    }) {
        return None;
    }
    // Every successful current operation retains one immutable envelope; no
    if state.decision != u64::try_from(state.commits.len()).ok()? {
        return None;
    }
    validate_inventory(&state)?;
    for envelope in state.retained.iter().filter_map(Retained::envelope) {
        for mutation in &envelope.mutations {
            let records = match mutation {
                crate::CommittedMutation::Append(append) => &append.records,
                crate::CommittedMutation::Fork(fork) => &fork.records,
            };
            state.record_count = state.record_count.checked_add(records.len())?;
            state.payload_bytes = state.payload_bytes.checked_add(
                records
                    .iter()
                    .map(|record| record.value.len())
                    .sum::<usize>(),
            )?;
        }
    }
    Some(state)
}

// Index each path's own immutable nodes once, stopping at its fork Prefix.
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
                History::Batch {
                    parent, records, ..
                } => {
                    let fact = (records.as_ref(), pointer, parent.as_ref().map(Arc::as_ptr));
                    if batches
                        .insert((path, records.first()?.commit_id), fact)
                        .is_some()
                    {
                        return None;
                    }
                    cursor = parent.as_ref();
                }
                History::Prefix { source, tail, .. } => {
                    prefixes.insert(path, (source.as_ref().map(Arc::as_ptr), *tail, pointer));
                    break;
                }
            }
        }
    }
    let mut heads = BTreeMap::new();
    for retained in &state.retained {
        let Some(envelope) = retained.envelope() else {
            continue;
        };
        let commit = envelope.commit_id;
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
                if stored.as_ref() != records.as_slice() || stored_parent != parent {
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
fn reference(nodes: &[Arc<History>], index: usize) -> Option<Option<Arc<History>>> {
    match index {
        0 => Some(None),
        index => nodes.get(index - 1).cloned().map(Some),
    }
}

fn history_tail(history: Option<&Arc<History>>) -> Option<u64> {
    match history.map(AsRef::as_ref) {
        None => Some(0),
        Some(History::Batch { records, .. }) => records.last()?.sequence.checked_add(1),
        Some(History::Prefix { tail, .. }) => Some(*tail),
    }
}

fn decode_nodes(input: &mut Input<'_>) -> Option<Vec<Arc<History>>> {
    let count = input.len()?;
    let mut nodes = Vec::with_capacity(count.min(input.0.len()));
    for _ in 0..count {
        let tag = input.u8()?;
        let parent = reference(&nodes, input.len()?)?;
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
                    records: Arc::new(RecordBatch {
                        records: records,
                        _allocation: Default::default(),
                    }),
                    _allocation: Default::default(),
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
                    _allocation: Default::default(),
                }
            }
            _ => return None,
        };
        nodes.push(Arc::new(node));
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
                _allocation: Default::default(),
            },
        );
    }
    Some((paths, path_bytes))
}

// A current command is bounded by MAX_COMMAND_BYTES and each protobuf bytes
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
        let observation = if flags & RETAINS_REPLAY == 0 {
            None
        } else {
            Some(
                crate::persistence::decode_observation(input.bytes()?, retained_fact_bound()?)
                    .ok()?,
            )
        };
        let envelope = if flags & RETAINS_COMMIT == 0 {
            None
        } else {
            Some(crate::persistence::decode_envelope(input.bytes()?, retained_fact_bound()?).ok()?)
        };
        if envelope
            .as_ref()
            .is_some_and(|envelope| state.commits.contains_key(&envelope.commit_id))
        {
            return None;
        }
        let (key, digest, result, envelope) = match observation {
            Some(observation) => {
                let key = observation.idempotency_key;
                if state.replays.contains_key(key.as_bytes())
                    || replay_commit_id(envelope.as_ref(), &observation.outcome)?
                        != envelope.as_ref().map(|envelope| envelope.commit_id)
                {
                    return None;
                }
                (
                    Some(key),
                    observation.request_digest,
                    observation.outcome,
                    envelope,
                )
            }
            None => (
                None,
                [0; 32],
                crate::IdempotencyOutcome::Commit(crate::CommitOutcome::Committed(envelope?)),
                None,
            ),
        };
        retain(state, envelope, key, digest, result);
    }
    Some(())
}
fn replay_commit_id(
    envelope: Option<&crate::CommittedEnvelope>,
    result: &crate::IdempotencyOutcome,
) -> Option<Option<CommitId>> {
    let commit = match result {
        crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) => {
            let envelope = envelope.filter(|envelope| envelope.commit_id == receipt.commit_id)?;
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
            let envelope = envelope.filter(|envelope| envelope.commit_id == receipt.commit_id)?;
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
        crate::IdempotencyOutcome::Commit(crate::CommitOutcome::Committed(committed)) => {
            if envelope != Some(committed) {
                return None;
            }
            Some(committed.commit_id)
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
mod tests;
