//! Canonical snapshots of the stream state machine.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::OwnedBytes;
use tokio::sync::watch;

use super::{
    History, MemoryStream, NativeStreamOutput, PathState, Replay, Retained, State, recount,
};
#[cfg(test)]
use crate::IdempotencyKey;
use crate::wire_codec::{CanonicalWire, WireMeter};
use crate::{CommitId, Record, StreamError, StreamPath};

const MAGIC: &[u8] = b"ACYCLIC-STREAM-STATE-V1\0";

const NODE_BATCH: u8 = 0;
const NODE_PREFIX: u8 = 1;
const RETAINS_REPLAY: u8 = 1;
const RETAINS_COMMIT: u8 = 2;

impl MemoryStream {
    /// Encodes the state with optional native admission.
    pub(crate) async fn encode_state(
        &self,
        meter: WireMeter<'_>,
    ) -> std::io::Result<NativeStreamOutput<Vec<u8>>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = meter {
            meter.check()?;
        }
        let state = self.state.read().await;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = meter {
            meter.check()?;
        }
        encode(&state, meter)
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

fn encode(state: &State, meter: WireMeter<'_>) -> std::io::Result<NativeStreamOutput<Vec<u8>>> {
    let mut funding = NativeStreamOutput::fund(meter);
    let mut out = Vec::new();
    write(&mut out, &mut funding, MAGIC)?;
    write_u64(&mut out, &mut funding, state.commits.len() as u64)?;
    write_u64(&mut out, &mut funding, state.last_committed_at_micros)?;
    write(&mut out, &mut funding, state.hierarchy_version.as_bytes())?;
    // Each node follows every node it reaches: decoding links backwards.
    let mut indices = Vec::new();
    let mut nodes = Vec::new();
    for path in state.paths.values() {
        funding.examine(1)?;
        let mut chain = Vec::new();
        let mut cursor = path.history.clone();
        while let Some(node) = cursor {
            funding.examine(1)?;
            if find_node(&indices, Arc::as_ptr(&node), &funding)?.is_ok() {
                break;
            }
            cursor = match node.as_ref() {
                History::Batch { parent, .. } => parent.clone(),
                History::Prefix { source, .. } => source.clone(),
            };
            funding.ensure_vector(&mut chain, 1)?;
            chain.push(node);
        }
        let chain_bytes = chain.capacity() * std::mem::size_of::<Arc<History>>();
        for node in chain.into_iter().rev() {
            let pointer = Arc::as_ptr(&node);
            let position = find_node(&indices, pointer, &funding)?
                .expect_err("the canonical chain contains only unseen nodes");
            funding.ensure_vector(&mut indices, 1)?;
            let shifted =
                (indices.len() - position) * std::mem::size_of::<(*const History, usize)>();
            funding.copy_admission(shifted)?;
            indices.insert(position, (pointer, nodes.len()));
            funding.copy_completed(shifted)?;
            funding.ensure_vector(&mut nodes, 1)?;
            nodes.push(node);
        }
        funding.retire(chain_bytes)?;
    }
    write_u64(&mut out, &mut funding, nodes.len() as u64)?;
    for node in &nodes {
        funding.examine(1)?;
        match node.as_ref() {
            History::Batch {
                parent, records, ..
            } => {
                write(&mut out, &mut funding, &[NODE_BATCH])?;
                let reference = node_reference(&indices, parent, &funding)?;
                write_u64(&mut out, &mut funding, reference as u64)?;
                write_u64(&mut out, &mut funding, records.value.len() as u64)?;
                for record in records.value.iter() {
                    funding.examine(1)?;
                    write_u64(&mut out, &mut funding, record.sequence)?;
                    write(&mut out, &mut funding, record.commit_id.as_bytes())?;
                    write_u64(&mut out, &mut funding, record.committed_at_micros)?;
                    write_bytes(&mut out, &mut funding, &record.value)?;
                }
            }
            History::Prefix { source, tail, .. } => {
                write(&mut out, &mut funding, &[NODE_PREFIX])?;
                let reference = node_reference(&indices, source, &funding)?;
                write_u64(&mut out, &mut funding, reference as u64)?;
                write_u64(&mut out, &mut funding, *tail)?;
            }
        }
    }
    write_u64(&mut out, &mut funding, state.paths.len() as u64)?;
    for (path, stream) in &state.paths {
        funding.examine(1)?;
        write_bytes(&mut out, &mut funding, path.as_str().as_bytes())?;
        let reference = node_reference(&indices, &stream.history, &funding)?;
        write_u64(&mut out, &mut funding, reference as u64)?;
        write_u64(&mut out, &mut funding, stream.tail)?;
    }
    let node_bytes = nodes.capacity() * std::mem::size_of::<Arc<History>>();
    drop(nodes);
    funding.retire(node_bytes)?;
    let index_bytes = indices.capacity() * std::mem::size_of::<(*const History, usize)>();
    drop(indices);
    funding.retire(index_bytes)?;
    write_u64(&mut out, &mut funding, state.retained.len() as u64)?;
    for retained in &state.retained {
        funding.examine(1)?;
        let replay = retained
            .replay
            .as_ref()
            .and_then(|key| Some((key, state.replays.get(key)?)));
        let commit = retained
            .commit
            .as_ref()
            .and_then(|commit| state.commits.get(commit));
        write(
            &mut out,
            &mut funding,
            &[if replay.is_some() { RETAINS_REPLAY } else { 0 }
                | if commit.is_some() { RETAINS_COMMIT } else { 0 }],
        )?;
        if let Some((key, replay)) = replay {
            write_message(
                &mut out,
                &mut funding,
                &CanonicalWire::Observation(key, &replay.digest, &replay.result),
            )?;
        }
        if let Some(envelope) = commit {
            write_message(
                &mut out,
                &mut funding,
                &CanonicalWire::Envelope(
                    envelope
                        .envelope()
                        .expect("a commit index contains an envelope"),
                ),
            )?;
        }
    }
    Ok(funding.map(|_| out))
}

fn node_reference(
    indices: &[(*const History, usize)],
    node: &Option<Arc<History>>,
    funding: &NativeStreamOutput<WireMeter<'_>>,
) -> std::io::Result<usize> {
    let Some(node) = node else {
        return Ok(0);
    };
    match find_node(indices, Arc::as_ptr(node), funding)? {
        Ok(position) => Ok(indices.get(position).expect("a matched memo slot exists").1 + 1),
        Err(_) => Ok(0),
    }
}

fn find_node(
    indices: &[(*const History, usize)],
    pointer: *const History,
    funding: &NativeStreamOutput<WireMeter<'_>>,
) -> std::io::Result<Result<usize, usize>> {
    let mut low = 0;
    let mut high = indices.len();
    while low < high {
        funding.examine(1)?;
        let middle = low + (high - low) / 2;
        let known = indices
            .get(middle)
            .expect("binary search remains inside its memo");
        match known.0.cmp(&pointer) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}

fn write(
    out: &mut Vec<u8>,
    funding: &mut NativeStreamOutput<WireMeter<'_>>,
    value: &[u8],
) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = funding.value {
        meter.admit_codec(value.len() as u64, value.len() as u64, 0)?;
    }
    funding.ensure_vector(out, value.len())?;
    out.extend_from_slice(value);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = funding.value {
        meter.record_codec(value.len() as u64, value.len() as u64, 0)?;
        meter.check()?;
    }
    Ok(())
}

fn write_u64(
    out: &mut Vec<u8>,
    funding: &mut NativeStreamOutput<WireMeter<'_>>,
    value: u64,
) -> std::io::Result<()> {
    write(out, funding, &value.to_le_bytes())
}

fn write_bytes(
    out: &mut Vec<u8>,
    funding: &mut NativeStreamOutput<WireMeter<'_>>,
    value: &[u8],
) -> std::io::Result<()> {
    write_u64(out, funding, value.len() as u64)?;
    write(out, funding, value)
}

fn write_message(
    out: &mut Vec<u8>,
    funding: &mut NativeStreamOutput<WireMeter<'_>>,
    value: &CanonicalWire<'_>,
) -> std::io::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = funding.value {
        meter.admit_examination(1, 0)?;
    }
    let length = value.encoded_len(None, None)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = funding.value {
        meter.record_examination(1, 0)?;
        meter.check()?;
    }
    write_u64(out, funding, length as u64)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = funding.value {
        meter.admit_codec(length as u64, length as u64, 0)?;
    }
    funding.ensure_vector(out, length)?;
    value.encode(out)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(meter) = funding.value {
        meter.record_codec(length as u64, length as u64, 0)?;
        meter.check()?;
    }
    Ok(())
}

fn decode(encoded: &[u8]) -> Option<State> {
    let mut input = encoded.strip_prefix(MAGIC)?;
    let decision = read_u64(&mut input)?;
    let last_committed_at_micros = read_u64(&mut input)?;
    let hierarchy_version = CommitId::from_bytes(read_array(&mut input)?);
    let nodes = decode_nodes(&mut input)?;
    let (paths, path_bytes) = decode_paths(&mut input, &nodes.0)?;
    let mut state = State::default();
    state.paths = paths;
    state.path_bytes = path_bytes;
    state.last_committed_at_micros = last_committed_at_micros;
    state.hierarchy_version = hierarchy_version;
    decode_retained(&mut input, &mut state)?;
    if !input.is_empty() {
        return None;
    }
    if encode(&state, None)
        .expect("unmetered encoding cannot fail")
        .value
        != encoded
    {
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
            .value
            .iter()
            .any(|record| record.committed_at_micros > last_committed_at_micros),
        History::Prefix { .. } => false,
    }) {
        return None;
    }
    if decision != u64::try_from(state.commits.len()).ok()? {
        return None;
    }
    validate_inventory(&state)?;
    recount(&mut state, None).expect("unmetered recount cannot fail");
    Some(state)
}

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
                    let fact = (
                        records.value.as_slice(),
                        pointer,
                        parent.as_ref().map(Arc::as_ptr),
                    );
                    if batches
                        .insert((path, records.value.first()?.commit_id), fact)
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
        let Some(commit) = retained.commit else {
            continue;
        };
        let envelope = state.commits.get(&commit)?.envelope()?;
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

/// The node an encoded reference names: 0 for none, otherwise one after its index.
fn reference(nodes: &[Arc<History>], index: usize) -> Option<Option<Arc<History>>> {
    match index {
        0 => Some(None),
        index => nodes.get(index - 1).cloned().map(Some),
    }
}

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
        Some(History::Batch { records, .. }) => records.value.last()?.sequence.checked_add(1),
        Some(History::Prefix { tail, .. }) => Some(*tail),
    }
}

fn decode_nodes(input: &mut &[u8]) -> Option<DecodedNodes> {
    let count = read_len(input)?;
    let mut nodes = DecodedNodes(Vec::with_capacity(count.min(input.len())));
    for _ in 0..count {
        let tag = read_u8(input)?;
        let parent = reference(&nodes.0, read_len(input)?)?;
        let node = match tag {
            NODE_BATCH => {
                let count = read_len(input)?;
                if count == 0 || count > crate::MAX_ITEMS {
                    return None;
                }
                let mut sequence = history_tail(parent.as_ref())?;
                let mut records = Vec::with_capacity(count.min(input.len()));
                for _ in 0..count {
                    let record = Record {
                        sequence: read_u64(input)?,
                        commit_id: CommitId::from_bytes(read_array(input)?),
                        committed_at_micros: read_u64(input)?,
                        value: OwnedBytes::copy_from_slice(read_bytes(input)?),
                    };
                    if record.sequence != sequence || record.value.len() > crate::MAX_RECORD_BYTES {
                        return None;
                    }
                    sequence = sequence.checked_add(1)?;
                    records.push(record);
                }
                History::Batch {
                    parent,
                    records: Arc::new(NativeStreamOutput {
                        value: records,
                        allocation: Default::default(),
                    }),
                    _allocation: super::Allocation::default(),
                }
            }
            NODE_PREFIX => {
                let tail = read_u64(input)?;
                if tail > history_tail(parent.as_ref())? {
                    return None;
                }
                History::Prefix {
                    source: parent,
                    tail,
                    _allocation: super::Allocation::default(),
                }
            }
            _ => return None,
        };
        nodes.0.push(Arc::new(node));
    }
    Some(nodes)
}

fn decode_paths(
    input: &mut &[u8],
    nodes: &[Arc<History>],
) -> Option<(BTreeMap<StreamPath, PathState>, usize)> {
    let mut paths = BTreeMap::new();
    let mut path_bytes = 0_usize;
    for _ in 0..read_len(input)? {
        let path = StreamPath::new(std::str::from_utf8(read_bytes(input)?).ok()?).ok()?;
        let history = reference(nodes, read_len(input)?)?;
        let tail = read_u64(input)?;
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
                _allocation: super::Allocation::default(),
            },
        );
    }
    Some((paths, path_bytes))
}

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

fn decode_retained(input: &mut &[u8], state: &mut State) -> Option<()> {
    for _ in 0..read_len(input)? {
        let flags = read_u8(input)?;
        if flags == 0 || flags & !(RETAINS_REPLAY | RETAINS_COMMIT) != 0 {
            return None;
        }
        let observation = if flags & RETAINS_REPLAY == 0 {
            None
        } else {
            Some(
                crate::persistence::decode_observation(read_bytes(input)?, retained_fact_bound()?)
                    .ok()?,
            )
        };
        let envelope = if flags & RETAINS_COMMIT == 0 {
            None
        } else {
            Some(
                crate::persistence::decode_envelope(read_bytes(input)?, retained_fact_bound()?)
                    .ok()?,
            )
        };
        let commit = envelope.as_ref().map(|envelope| envelope.commit_id);
        let (replay, digest, result, envelope) = match observation {
            Some(observation) => {
                let envelope = if let crate::IdempotencyOutcome::Commit(
                    crate::CommitOutcome::Committed(canonical),
                ) = &observation.outcome
                {
                    if envelope.as_ref() != Some(canonical) {
                        return None;
                    }
                    None
                } else {
                    envelope
                };
                (
                    Some(observation.idempotency_key.0),
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
        let cached = Arc::new(Replay {
            digest,
            result,
            envelope,
            _allocation: super::Allocation::default(),
        });
        if let Some(commit_id) = commit {
            if state
                .commits
                .insert(commit_id, Arc::clone(&cached))
                .is_some()
            {
                return None;
            }
        }
        if let Some(key) = &replay {
            if replay_commit_id(state, &cached.result)? != commit {
                return None;
            }
            if state.replays.insert(key.clone(), cached).is_some() {
                return None;
            }
        }
        state.retained.push(Retained {
            replay,
            commit,
            _allocation: super::Allocation::default(),
        });
    }
    Some(())
}

fn replay_commit_id(state: &State, result: &crate::IdempotencyOutcome) -> Option<Option<CommitId>> {
    let commit = match result {
        crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) => {
            let envelope = state.commits.get(&receipt.commit_id)?.envelope()?;
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
            let envelope = state.commits.get(&receipt.commit_id)?.envelope()?;
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
            if state
                .commits
                .get(&envelope.commit_id)
                .and_then(|cached| cached.envelope())
                != Some(envelope)
            {
                return None;
            }
            Some(envelope.commit_id)
        }
        crate::IdempotencyOutcome::Append(crate::AppendOutcome::TailConflict { .. })
        | crate::IdempotencyOutcome::Commit(crate::CommitOutcome::Conflict(_)) => None,
    };
    Some(commit)
}

#[cfg(test)]
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
fn put_len(out: &mut Vec<u8>, value: usize) {
    put_u64(out, value as u64);
}

#[cfg(test)]
fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_len(out, value.len());
    out.extend_from_slice(value);
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Option<&'a [u8]> {
    let (taken, rest) = input.split_at_checked(count)?;
    *input = rest;
    Some(taken)
}

fn read_array<const N: usize>(input: &mut &[u8]) -> Option<[u8; N]> {
    take(input, N)?.try_into().ok()
}

fn read_u8(input: &mut &[u8]) -> Option<u8> {
    Some(read_array::<1>(input)?[0])
}

fn read_u64(input: &mut &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(read_array(input)?))
}

fn read_len(input: &mut &[u8]) -> Option<usize> {
    usize::try_from(read_u64(input)?).ok()
}

fn read_bytes<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let count = read_len(input)?;
    take(input, count)
}

#[cfg(test)]
mod tests;
