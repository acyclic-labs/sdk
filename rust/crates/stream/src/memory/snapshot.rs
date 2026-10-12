//! Canonical snapshots of the stream state machine.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;


use crate::OwnedBytes;
use tokio::sync::watch;

use super::{History, MemoryStream, PathState, Replay, Retained, State, recount};
use crate::wire_codec::{
    CanonicalWire, ObservedWire, WireFunding, WireMeter,
};
use crate::{CommitId, Record, StreamError, StreamPath};
#[cfg(test)]
use crate::IdempotencyKey;

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
    ) -> std::io::Result<ObservedWire<Vec<u8>>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = meter { meter.check()?; }
        let state = self.state.read().await;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = meter { meter.check()?; }
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

fn encode(state: &State, meter: WireMeter<'_>) -> std::io::Result<ObservedWire<Vec<u8>>> {
    let mut writer = SnapshotWriter { out: Vec::new(), funding: WireFunding::new(meter) };
    writer.write(MAGIC)?;
    writer.u64(state.decision)?;
    writer.u64(state.last_committed_at_micros)?;
    writer.write(state.hierarchy_version.as_bytes())?;
    // Each node follows every node it reaches: decoding links backwards.
    let mut indices = Vec::new();
    let mut nodes = Vec::new();
    for path in state.paths.values() {
        writer.funding.examine(1)?;
        let mut chain = Vec::new();
        let mut cursor = path.history.clone();
        while let Some(node) = cursor {
            writer.funding.examine(1)?;
            if find_node(&indices, Arc::as_ptr(&node), &writer.funding)?.is_ok() { break; }
            cursor = match node.as_ref() {
                History::Batch { parent, .. } => parent.clone(),
                History::Prefix { source, .. } => source.clone(),
            };
            writer.funding.ensure_vector(&mut chain, 1)?;
            chain.push(node);
        }
        let chain_bytes = chain.capacity() * std::mem::size_of::<Arc<History>>();
        for node in chain.into_iter().rev() {
            let pointer = Arc::as_ptr(&node);
            let position = find_node(&indices, pointer, &writer.funding)?
                .expect_err("the canonical chain contains only unseen nodes");
            writer.funding.ensure_vector(&mut indices, 1)?;
            let shifted = (indices.len() - position) * std::mem::size_of::<(*const History, usize)>();
            writer.funding.copy_admission(shifted)?;
            indices.insert(position, (pointer, nodes.len()));
            writer.funding.copy_completed(shifted)?;
            writer.funding.ensure_vector(&mut nodes, 1)?;
            nodes.push(node);
        }
        writer.funding.retire(chain_bytes)?;
    }
    writer.len(nodes.len())?;
    for node in &nodes {
        writer.funding.examine(1)?;
        match node.as_ref() {
            History::Batch { parent, records, .. } => {
                writer.write(&[NODE_BATCH])?;
                let reference = node_reference(&indices, parent, &writer.funding)?;
                writer.len(reference)?;
                writer.len(records.len())?;
                for record in records.iter() {
                    writer.funding.examine(1)?;
                    writer.u64(record.sequence)?;
                    writer.write(record.commit_id.as_bytes())?;
                    writer.u64(record.committed_at_micros)?;
                    writer.bytes(&record.value)?;
                }
            }
            History::Prefix { source, tail, .. } => {
                writer.write(&[NODE_PREFIX])?;
                let reference = node_reference(&indices, source, &writer.funding)?;
                writer.len(reference)?;
                writer.u64(*tail)?;
            }
        }
    }
    writer.len(state.paths.len())?;
    for (path, stream) in &state.paths {
        writer.funding.examine(1)?;
        writer.bytes(path.as_str().as_bytes())?;
        let reference = node_reference(&indices, &stream.history, &writer.funding)?;
        writer.len(reference)?;
        writer.u64(stream.tail)?;
    }
    let node_bytes = nodes.capacity() * std::mem::size_of::<Arc<History>>();
    drop(nodes);
    writer.funding.retire(node_bytes)?;
    let index_bytes = indices.capacity() * std::mem::size_of::<(*const History, usize)>();
    drop(indices);
    writer.funding.retire(index_bytes)?;
    writer.len(state.retained.len())?;
    for retained in &state.retained {
        writer.funding.examine(1)?;
        let replay = retained.replay.as_ref()
            .and_then(|key| Some((key, state.replays.get(key)?)));
        let commit = retained.commit.as_ref().and_then(|commit| state.commits.get(commit));
        writer.write(&[
            if replay.is_some() { RETAINS_REPLAY } else { 0 }
                | if commit.is_some() { RETAINS_COMMIT } else { 0 },
        ])?;
        if let Some((key, replay)) = replay {
            writer.message(&CanonicalWire::Observation(key, &replay.digest, &replay.result))?;
        }
        if let Some(envelope) = commit {
            writer.message(&CanonicalWire::Envelope(envelope.envelope().expect("a commit index contains an envelope")))?;
        }
    }
    Ok(writer.funding.finish(writer.out))
}

fn node_reference(
    indices: &[(*const History, usize)],
    node: &Option<Arc<History>>,
    funding: &WireFunding<'_>,
) -> std::io::Result<usize> {
    let Some(node) = node else { return Ok(0); };
    match find_node(indices, Arc::as_ptr(node), funding)? {
        Ok(position) => Ok(indices.get(position).expect("a matched memo slot exists").1 + 1),
        Err(_) => Ok(0),
    }
}

fn find_node(
    indices: &[(*const History, usize)],
    pointer: *const History,
    funding: &WireFunding<'_>,
) -> std::io::Result<Result<usize, usize>> {
    let mut low = 0;
    let mut high = indices.len();
    while low < high {
        funding.examine(1)?;
        let middle = low + (high - low) / 2;
        let known = indices.get(middle).expect("binary search remains inside its memo");
        match known.0.cmp(&pointer) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}

struct SnapshotWriter<'a> {
    out: Vec<u8>,
    funding: WireFunding<'a>,
}

impl SnapshotWriter<'_> {
    fn write(&mut self, value: &[u8]) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.funding.meter() {
            meter.admit_codec(value.len() as u64, value.len() as u64, 0)?;
        }
        self.funding.ensure_vector(&mut self.out, value.len())?;
        self.out.extend_from_slice(value);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.funding.meter() {
            meter.record_codec(value.len() as u64, value.len() as u64, 0)?;
            meter.check()?;
        }
        Ok(())
    }

    fn u64(&mut self, value: u64) -> std::io::Result<()> { self.write(&value.to_le_bytes()) }
    fn len(&mut self, value: usize) -> std::io::Result<()> { self.u64(value as u64) }
    fn bytes(&mut self, value: &[u8]) -> std::io::Result<()> {
        self.len(value.len())?;
        self.write(value)
    }

    fn message(&mut self, value: &CanonicalWire<'_>) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.funding.meter() { meter.admit_examination(1, 0)?; }
        let length = value.encoded_len()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.funding.meter() {
            meter.record_examination(1, 0)?;
            meter.check()?;
        }
        self.len(length)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.funding.meter() {
            meter.admit_codec(length as u64, length as u64, 0)?;
        }
        self.funding.ensure_vector(&mut self.out, length)?;
        value.encode(&mut self.out)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.funding.meter() {
            meter.record_codec(length as u64, length as u64, 0)?;
            meter.check()?;
        }
        Ok(())
    }
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
    if encode(&state, None).expect("unmetered encoding cannot fail").value != encoded {
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
    if state.decision != u64::try_from(state.commits.len()).ok()? {
        return None;
    }
    validate_inventory(&state)?;
    recount(&mut state, &super::MemoryObserver::default()).expect("unmetered recount cannot fail");
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
                History::Batch { parent, records, .. } => {
                    let fact = (records.as_ref().as_ref(), pointer, parent.as_ref().map(Arc::as_ptr));
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
                        value: OwnedBytes::copy_from_slice(input.bytes()?),
                    };
                    if record.sequence != sequence || record.value.len() > crate::MAX_RECORD_BYTES {
                        return None;
                    }
                    sequence = sequence.checked_add(1)?;
                    records.push(record);
                }
                History::Batch {
                    parent,
                    records: Arc::new(super::MemoryRecords::from(records)),
                    _allocation: super::MemoryAllocation::default(),
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
                    _allocation: super::MemoryAllocation::default(),
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
                _allocation: super::MemoryAllocation::default(),
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

fn decode_retained(input: &mut Input<'_>, state: &mut State) -> Option<()> {
    for _ in 0..input.len()? {
        let flags = input.u8()?;
        if flags == 0 || flags & !(RETAINS_REPLAY | RETAINS_COMMIT) != 0 {
            return None;
        }
        let observation = if flags & RETAINS_REPLAY == 0 { None } else {
            Some(crate::persistence::decode_observation(input.bytes()?, retained_fact_bound()?).ok()?)
        };
        let envelope = if flags & RETAINS_COMMIT == 0 { None } else {
            Some(crate::persistence::decode_envelope(input.bytes()?, retained_fact_bound()?).ok()?)
        };
        let commit = envelope.as_ref().map(|envelope| envelope.commit_id);
        let (replay, digest, result, envelope) = match observation {
            Some(observation) => {
                let envelope = if let crate::IdempotencyOutcome::Commit(crate::CommitOutcome::Committed(canonical)) = &observation.outcome {
                    if envelope.as_ref() != Some(canonical) { return None; }
                    None
                } else { envelope };
                (Some(observation.idempotency_key.0), observation.request_digest, observation.outcome, envelope)
            }
            None => (None, [0; 32], crate::IdempotencyOutcome::Commit(crate::CommitOutcome::Committed(envelope?)), None),
        };
        let cached = Arc::new(Replay { digest, result, envelope, _allocation: super::MemoryAllocation::default() });
        if let Some(commit_id) = commit {
            if state.commits.insert(commit_id, Arc::clone(&cached)).is_some() { return None; }
        }
        if let Some(key) = &replay {
            if replay_commit_id(state, &cached.result)? != commit { return None; }
            if state.replays.insert(key.clone(), cached).is_some() { return None; }
        }
        state.retained.push_back(Retained {
            replay, commit, _allocation: super::MemoryAllocation::default(),
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
            if state.commits.get(&envelope.commit_id).and_then(|cached| cached.envelope()) != Some(envelope) {
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
    use crate::{AppendRequest, IdempotencyObservation, MemoryLimits, ReadRequest, StreamProvider, UnixMillisClock};
    use crate::wire_codec::observation_wire;
    use futures::StreamExt as _;
    use prost::Message as _;
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
            records: vec![OwnedBytes::from_static(b"once")],
            if_tail: None,
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"snapshot-once"))?),
        };
        let first = stream.append(request.clone()).await?;
        let restored = MemoryStream::new(limits);
        restored.install_state(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).await?;
        assert_eq!(restored.append(request.clone()).await?, first);
        assert_eq!(restored.tail(request.path.clone()).await?, 1);
        let mut changed = request.clone();
        changed.records = vec![OwnedBytes::from_static(b"different")];
        assert_eq!(
            restored.append(changed).await,
            Err(StreamError::IdempotencyMismatch)
        );
        let mut fresh = request;
        fresh.idempotency_key = Some(IdempotencyKey::new(OwnedBytes::from_static(b"fresh"))?);
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
            records: vec![OwnedBytes::from_static(b"first"), OwnedBytes::from_static(b"second")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"append"))?),
        };
        let append_result = stream.append(appended.clone()).await?;
        let forked = crate::ForkRequest {
            source: source.clone(),
            destination: destination.clone(),
            at_tail: Some(1),
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"fork"))?),
        };
        let fork_result = stream.fork(forked.clone()).await?;
        let committed = crate::CommitRequest {
            conditions: vec![crate::CommitCondition::Tail {
                path: destination.clone(),
                expected: 1,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: destination.clone(),
                records: vec![OwnedBytes::from_static(b"third")],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"commit"))?,
        };
        let commit_result = stream.commit(committed.clone()).await?;
        let restored = MemoryStream::default();
        restored.install_state(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).await?;
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
            records: vec![OwnedBytes::from_static(value)],
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
                    records: vec![OwnedBytes::from_static(b"after")],
                },
                crate::CommitMutation::Fork {
                    source: source.clone(),
                    destination: destination.clone(),
                    at_tail: 1,
                    records: vec![OwnedBytes::from_static(b"forked")],
                },
            ],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"atomic-fork"))?,
        };
        let outcome = stream.commit(request.clone()).await?;
        let restored = MemoryStream::default();
        restored.install_state(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).await?;
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
                records: vec![OwnedBytes::from_static(b"kept")],
                if_tail: None,
                idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"kept"))?),
            })
            .await?;
        let valid = stream.encode_state(None).await.expect("unmetered snapshot encoding").value;
        for length in 0..valid.len() {
            assert_eq!(
                stream.install_state(&valid[..length]).await,
                Err(StreamError::InvalidArgument)
            );
            assert_eq!(stream.encode_state(None).await.expect("unmetered snapshot encoding").value, valid);
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
            assert_eq!(stream.encode_state(None).await.expect("unmetered snapshot encoding").value, valid);
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
            let empty = constrained.encode_state(None).await.expect("unmetered snapshot encoding").value;
            assert_eq!(
                constrained.install_state(&valid).await,
                Err(StreamError::Capacity)
            );
            assert_eq!(constrained.encode_state(None).await.expect("unmetered snapshot encoding").value, empty);
        }
        assert_eq!(stream.encode_state(None).await.expect("unmetered snapshot encoding").value, valid);
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
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"conflict"))?,
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
        let key = IdempotencyKey::new(OwnedBytes::from_static(b"inventory"))?;
        stream
            .append(AppendRequest {
                path: StreamPath::new("inventory/path")?,
                records: vec![OwnedBytes::from_static(b"real")],
                if_tail: Some(0),
                idempotency_key: Some(key.clone()),
            })
            .await?;
        let mut state = stream.state.write().await;
        let cached = state.replays.get_mut(key.as_bytes()).ok_or(StreamError::Unavailable)?;
        *cached = Arc::new(Replay {
            digest: cached.digest, result: cached.result.clone(), envelope: cached.envelope.clone(),
            _allocation: crate::memory::MemoryAllocation::default(),
        });
        let replay = state
            .replays
            .get_mut(key.as_bytes())
            .and_then(Arc::get_mut)
            .ok_or(StreamError::Unavailable)?;
        let original_result = replay.result.clone();
        let crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) =
            &mut replay.result
        else {
            return Err(StreamError::Unavailable);
        };
        receipt.start = receipt.end;
        assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
        state
            .replays
            .get_mut(key.as_bytes())
            .and_then(Arc::get_mut)
            .ok_or(StreamError::Unavailable)?
            .result = original_result.clone();
        let replay = state
            .replays
            .get_mut(key.as_bytes())
            .and_then(Arc::get_mut)
            .ok_or(StreamError::Unavailable)?;
        let crate::IdempotencyOutcome::Append(crate::AppendOutcome::Committed(receipt)) =
            &mut replay.result
        else {
            return Err(StreamError::Unavailable);
        };
        receipt.start = 1;
        receipt.end = 2;
        receipt.tail = 2;
        assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
        state
            .replays
            .get_mut(key.as_bytes())
            .and_then(Arc::get_mut)
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
            .and_then(Arc::get_mut).and_then(Replay::envelope_mut)
            .ok_or(StreamError::Unavailable)?;
        let Some(crate::CommittedMutation::Append(append)) = envelope.mutations.first_mut() else {
            return Err(StreamError::Unavailable);
        };
        let record = append.records.first_mut().ok_or(StreamError::Unavailable)?;
        record.sequence = 9;
        assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
        let envelope = state
            .commits
            .get_mut(&commit_id)
            .and_then(Arc::get_mut).and_then(Replay::envelope_mut)
            .ok_or(StreamError::Unavailable)?;
        let Some(crate::CommittedMutation::Append(append)) = envelope.mutations.first_mut() else {
            return Err(StreamError::Unavailable);
        };
        let record = append.records.first_mut().ok_or(StreamError::Unavailable)?;
        record.sequence = 0;
        record.value = OwnedBytes::from_static(b"phantom payload absent from immutable history");
        assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
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
                records: vec![OwnedBytes::from_static(b"seed")],
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
        assert!(decode(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).is_some());
        let mut state = stream.state.write().await;
        let envelope = state
            .commits
            .get_mut(&receipt.commit_id)
            .and_then(Arc::get_mut).and_then(Replay::envelope_mut)
            .ok_or(StreamError::Unavailable)?;
        let Some(crate::CommittedMutation::Fork(fork)) = envelope.mutations.first_mut() else {
            return Err(StreamError::Unavailable);
        };
        fork.source = later;
        assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn retained_fork_cannot_capture_a_future_source_head() -> Result<(), StreamError> {
        let stream = MemoryStream::default();
        let source = StreamPath::new("chronology/source")?;
        let destination = StreamPath::new("chronology/destination")?;
        let append = |value: &'static [u8]| AppendRequest {
            path: source.clone(),
            records: vec![OwnedBytes::from_static(value)],
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
        assert!(decode(&stream.encode_state(None).await.expect("unmetered snapshot encoding").value).is_some());
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
            _allocation: super::super::MemoryAllocation::default(),
        }));
        assert!(decode(&encode(&state, None).expect("unmetered encoding cannot fail").value).is_none());
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
                records: vec![OwnedBytes::from_static(b"one")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let timed = stream.encode_state(None).await.expect("unmetered snapshot encoding").value;
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
                records: vec![OwnedBytes::from_static(b"two")],
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
