//! The whole state machine as one self-contained encoding, from which a durable provider
//! restarts instead of replaying every command it ever ran.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message as _;
use tokio::sync::watch;

use super::{History, MemoryStream, PathState, Replay, Retained, State, recount};
use crate::wire_codec::{
    envelope_from_wire, envelope_wire, observation_from_wire, observation_wire,
};
use crate::{
    CommitId, IdempotencyKey, IdempotencyObservation, Record, StreamError, StreamPath, wire,
};

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
    /// `encoded` is not such an encoding.
    pub(crate) async fn install_state(&self, encoded: &[u8]) -> Result<(), StreamError> {
        let decoded = decode(encoded).ok_or(StreamError::InvalidArgument)?;
        *self.state.write().await = decoded;
        Ok(())
    }
}

fn encode(state: &State) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, state.decision);
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
        put_u64(&mut out, stream.trim_point);
    }
    put_len(&mut out, state.retired.len());
    for path in &state.retired {
        put_bytes(&mut out, path.as_str().as_bytes());
    }
    put_len(&mut out, state.retained.len());
    for retained in &state.retained {
        put_u64(&mut out, retained.until);
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
    let mut input = Input(encoded);
    let decision = input.u64()?;
    let nodes = decode_nodes(&mut input)?;
    let (paths, path_bytes) = decode_paths(&mut input, &nodes)?;
    let mut retired = BTreeSet::new();
    for _ in 0..input.len()? {
        retired.insert(input.path()?);
    }
    let mut state = State {
        paths,
        retired,
        path_bytes,
        decision,
        ..State::default()
    };
    decode_retained(&mut input, &mut state)?;
    if !input.0.is_empty() {
        return None;
    }
    recount(&mut state);
    Some(state)
}

/// The node an encoded reference names: 0 for none, otherwise one after
/// its index.
fn reference(nodes: &[Arc<History>], index: usize) -> Option<Option<Arc<History>>> {
    match index {
        0 => Some(None),
        index => nodes.get(index - 1).cloned().map(Some),
    }
}

fn decode_nodes(input: &mut Input<'_>) -> Option<Vec<Arc<History>>> {
    let count = input.len()?;
    let mut nodes: Vec<Arc<History>> = Vec::with_capacity(count.min(input.0.len()));
    for _ in 0..count {
        let tag = input.u8()?;
        let parent = reference(&nodes, input.len()?)?;
        let node = match tag {
            NODE_BATCH => {
                let count = input.len()?;
                let mut records = Vec::with_capacity(count.min(input.0.len()));
                for _ in 0..count {
                    records.push(Record {
                        sequence: input.u64()?,
                        commit_id: CommitId::from_bytes(input.array()?),
                        value: Bytes::copy_from_slice(input.bytes()?),
                    });
                }
                History::Batch {
                    parent,
                    records: Arc::from(records),
                }
            }
            NODE_PREFIX => History::Prefix {
                source: parent,
                tail: input.u64()?,
            },
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
        let trim_point = input.u64()?;
        if trim_point > tail {
            return None;
        }
        let (changed, _) = watch::channel(tail);
        path_bytes = path_bytes.checked_add(path.as_str().len())?;
        paths.insert(
            path,
            PathState {
                history,
                tail,
                trim_point,
                changed,
            },
        );
    }
    Some((paths, path_bytes))
}

fn decode_retained(input: &mut Input<'_>, state: &mut State) -> Option<()> {
    for _ in 0..input.len()? {
        let until = input.u64()?;
        let flags = input.u8()?;
        if flags & !(RETAINS_REPLAY | RETAINS_COMMIT) != 0 {
            return None;
        }
        let replay = if flags & RETAINS_REPLAY == 0 {
            None
        } else {
            let observation = wire::IdempotencyObservation::decode(input.bytes()?).ok()?;
            let observation = observation_from_wire(observation).ok()?;
            let key = Bytes::copy_from_slice(observation.idempotency_key.as_bytes());
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
            let envelope = wire::CommittedEnvelope::decode(input.bytes()?).ok()?;
            let envelope = envelope_from_wire(envelope).ok()?;
            let commit_id = envelope.commit_id;
            state.commits.insert(commit_id, envelope);
            Some(commit_id)
        };
        state.retained.push_back(Retained {
            until,
            replay,
            commit,
        });
    }
    Some(())
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
