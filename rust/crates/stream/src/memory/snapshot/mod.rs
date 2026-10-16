//! Canonical snapshot of retained Stream facts.
use super::{History, MemoryStream, Retained, State, publish, retain};
use crate::wire_codec::{envelope_to_wire, observation_wire};
use crate::{CommitId, CommittedMutation, IdempotencyKey, IdempotencyObservation, StreamError};
use prost::Message as _;
use std::collections::HashMap;
use std::sync::Arc;
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
            || decoded.decision > self.limits.commits as u64
            || decoded.replay_count() > self.limits.idempotency_results
        {
            return Err(StreamError::Capacity);
        }
        #[cfg(acyclic_loom)]
        if crate::verification::fault_is(crate::verification::Fault::RetainReplacedState) {
            let previous = std::mem::replace(&mut *self.state.write().await, decoded);
            std::mem::forget(previous);
            return Ok(());
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
                put_len(&mut out, records.records.len());
                for record in records.records.iter() {
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
    let time = input.u64()?;
    let hierarchy = CommitId::from_bytes(input.array()?);
    // Retained envelopes determine the history; the byte comparison verifies the encoded graph.
    for _ in 0..input.len()? {
        let tag = input.u8()?;
        input.len()?;
        match tag {
            NODE_BATCH => {
                for _ in 0..input.len()? {
                    input.take(48)?;
                    input.bytes()?;
                }
            }
            NODE_PREFIX => {
                input.u64()?;
            }
            _ => return None,
        }
    }
    for _ in 0..input.len()? {
        input.bytes()?;
        input.len()?;
        input.u64()?;
    }
    let mut state = State::default();
    decode_retained(&mut input, &mut state)?;
    if !input.0.is_empty()
        || decision
            != u64::try_from(
                state
                    .retained
                    .iter()
                    .filter(|entry| entry.envelope().is_some())
                    .count(),
            )
            .ok()?
    {
        return None;
    }
    let inventory = std::mem::take(&mut state.retained);
    let builder = MemoryStream::default();
    for envelope in inventory.iter().filter_map(Retained::envelope) {
        for mutation in &envelope.mutations {
            match mutation {
                CommittedMutation::Append(append) => {
                    if append.start != state.paths.get(&append.path).map_or(0, |path| path.tail) {
                        return None;
                    }
                }
                CommittedMutation::Fork(fork) => {
                    if state.paths.contains_key(&fork.destination)
                        || fork.forked_at > state.paths.get(&fork.source)?.tail
                    {
                        return None;
                    }
                }
            }
            if super::fact_records(mutation)
                .iter()
                .any(|record| record.committed_at_micros > time)
            {
                return None;
            }
        }
        let (histories, _histories) = builder.histories(&state, &envelope.mutations).ok()?;
        let (paths, _paths) = builder
            .paths(
                &state,
                envelope.mutations.iter().map(|mutation| match mutation {
                    CommittedMutation::Append(append) => &append.path,
                    CommittedMutation::Fork(fork) => &fork.destination,
                }),
            )
            .ok()?;
        publish(&mut state, paths, histories, envelope).ok()?;
    }
    state.retained = inventory;
    state.decision = decision;
    state.last_committed_at_micros = time;
    if state.hierarchy_version != hierarchy || encode(&state) != encoded {
        return None;
    }
    Some(state)
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
            .is_some_and(|envelope| state.commit(&envelope.commit_id).is_some())
        {
            return None;
        }
        let (key, digest, result, envelope) = match observation {
            Some(observation) => {
                let key = observation.idempotency_key;
                if state
                    .replay(key.as_bytes(), || Ok::<_, StreamError>(()))
                    .ok()?
                    .is_some()
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
        retain(
            state,
            &mut || {},
            envelope,
            key,
            digest,
            result,
            Default::default(),
        );
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
}

#[cfg(test)]
mod tests;
