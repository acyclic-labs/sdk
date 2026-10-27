//! The agent checkpoint's one encoding, with a static size bound.
//!
//! The parts that scale with the agent's limits are raw protobuf fields
//! rather than JSON: each open child is its 16-byte id (its path is derived
//! from the agent's), the children still to archive are a bitmap over them,
//! and the turn driver's checkpoint is raw bytes. The rest of the state is
//! JSON.
//!
//! Every step's state passes [`super::machine`]'s one size check before it
//! is checkpointed; a state that would exceed [`MAX_CHECKPOINT_BYTES`] fails
//! the agent visibly, dropping its input window and turn, so what remains is
//! only what is bounded by construction: two paths of at most
//! [`super::MAX_DEPTH`], the adoption's volumes and policy (one inbox record),
//! [`super::MAX_OPEN_CHILDREN`] ids, and one bounded outcome. `tests` checks
//! that this core, plus the actor record framing and the inbox subscription,
//! is within the stream's record limit, so no step can wedge an agent on a
//! record its stream refuses.
use super::{AgentId, AgentState, Error};
use acyclic_actors::guest::{Checkpoint, CodecIdentity};
use prost::Message as _;
use std::collections::BTreeSet;

/// Largest agent checkpoint.
pub const MAX_CHECKPOINT_BYTES: usize = 56 * 1024;
/// Most bytes a step's own settlement adds after the size check: the
/// conversation and status tails, two `u64`s in decimal.
pub(crate) const TAIL_SLACK: usize = 2 * 20;

/// The checkpoint codec every agent actor commits with.
pub(crate) fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "acyclic.harness.agent".into(),
        version: "1".into(),
    }
}

/// The agent state a committed checkpoint carries.
pub(crate) fn checkpoint_state(checkpoint: &Checkpoint) -> Result<AgentState, Error> {
    if checkpoint.codec != codec() {
        return Err(Error::Record("agent checkpoint codec differs"));
    }
    decode(&checkpoint.bytes).map(|(state, _)| state)
}

#[derive(Clone, PartialEq, prost::Message)]
struct Encoded {
    /// Inbox cursor of the last consumed record.
    #[prost(uint64, tag = "1")]
    cursor: u64,
    /// The state as JSON, without the fields below.
    #[prost(bytes = "vec", tag = "2")]
    state: Vec<u8>,
    /// Each open child's id, in path order.
    #[prost(bytes = "vec", repeated, tag = "3")]
    open_children: Vec<Vec<u8>>,
    /// Bit `i` set: open child `i` still has to receive `Archive`.
    #[prost(bytes = "vec", tag = "4")]
    archive_pending: Vec<u8>,
    /// Turn driver checkpoint.
    #[prost(bytes = "vec", optional, tag = "5")]
    turn: Option<Vec<u8>>,
}

/// `state`, after consuming the inbox records before `cursor`, as checkpoint
/// bytes, refused above [`MAX_CHECKPOINT_BYTES`].
pub(crate) fn checkpoint(state: &AgentState, cursor: u64) -> Result<Checkpoint, Error> {
    let bytes = encode(state, cursor)?;
    if bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(Error::Record(
            "agent checkpoint exceeds MAX_CHECKPOINT_BYTES",
        ));
    }
    Ok(Checkpoint {
        codec: codec(),
        bytes: bytes.into(),
    })
}

/// `state`, after consuming the inbox records before `cursor`, as checkpoint bytes.
pub(crate) fn encode(state: &AgentState, cursor: u64) -> Result<Vec<u8>, Error> {
    let mut rest = state.clone();
    let open_children = std::mem::take(&mut rest.open_children);
    let archive_pending = std::mem::take(&mut rest.archive_pending);
    let turn = rest.turn.take();
    let mut ids = Vec::with_capacity(open_children.len());
    let mut bitmap = vec![0; open_children.len().div_ceil(8)];
    for (index, child) in open_children.iter().enumerate() {
        if child.parent().as_ref() != Some(&state.path) {
            return Err(Error::Record("an open child is not a direct child"));
        }
        ids.push(child.id().to_bytes().to_vec());
        if archive_pending.contains(child) {
            bitmap[index / 8] |= 1 << (index % 8);
        }
    }
    if !archive_pending.is_subset(&open_children) {
        return Err(Error::Record("a child to archive is not open"));
    }
    Ok(Encoded {
        cursor,
        state: serde_json::to_vec(&rest)
            .map_err(|_| Error::Record("agent checkpoint does not encode"))?,
        open_children: ids,
        archive_pending: bitmap,
        turn,
    }
    .encode_to_vec())
}

/// The state and cursor checkpoint `bytes` hold.
pub(crate) fn decode(bytes: &[u8]) -> Result<(AgentState, u64), Error> {
    let malformed = || Error::Record("agent checkpoint is malformed");
    let encoded = Encoded::decode(bytes).map_err(|_| malformed())?;
    let mut state: AgentState = serde_json::from_slice(&encoded.state).map_err(|_| malformed())?;
    if !state.open_children.is_empty() || !state.archive_pending.is_empty() || state.turn.is_some()
    {
        return Err(malformed());
    }
    if encoded.archive_pending.len() != encoded.open_children.len().div_ceil(8) {
        return Err(malformed());
    }
    let mut open_children = BTreeSet::new();
    let mut archive_pending = BTreeSet::new();
    for (index, id) in encoded.open_children.iter().enumerate() {
        let id: [u8; 16] = id.as_slice().try_into().map_err(|_| malformed())?;
        let child = state.path.child(&AgentId::from_bytes(id))?;
        if encoded.archive_pending[index / 8] & (1 << (index % 8)) != 0 {
            archive_pending.insert(child.clone());
        }
        if !open_children.insert(child) {
            return Err(malformed());
        }
    }
    state.open_children = open_children;
    state.archive_pending = archive_pending;
    state.turn = encoded.turn;
    Ok((state, encoded.cursor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{
        AgentOutcome, AgentPath, INBOX, MAX_DEPTH, MAX_OPEN_CHILDREN, MAX_OUTCOME_BYTES,
        MAX_RECORD_BYTES, Phase,
    };
    use acyclic_actors::local::{SUBSCRIPTION_OVERHEAD, State};

    fn id(n: u64) -> AgentId {
        let mut bytes = [0xff; 16];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        AgentId::from_bytes(bytes)
    }

    /// The largest state the size check can leave at `depth`: every child
    /// it may hold open and to archive, settled, its counters at their widest,
    /// and with no adoption volumes or policy and an empty outcome, whose
    /// bounds the test adds.
    fn core(depth: usize) -> AgentState {
        let mut path = AgentPath::root(&id(0));
        for level in 1..=depth {
            path = path.child(&id(level as u64)).expect("within MAX_DEPTH");
        }
        let children = if depth < MAX_DEPTH {
            MAX_OPEN_CHILDREN
        } else {
            0
        };
        let open_children: BTreeSet<_> = (0..children as u64)
            .map(|n| path.child(&id(n)).expect("below MAX_DEPTH"))
            .collect();
        AgentState {
            parent: path.parent(),
            path,
            volumes: Vec::new(),
            policy: None,
            conversation_tail: u64::MAX,
            status_tail: u64::MAX,
            phase: Phase::Done {
                outcome: AgentOutcome::Failed {
                    message: String::new(),
                },
            },
            archive_pending: open_children.clone(),
            open_children,
            queued: Default::default(),
            overflow: None,
            taken: BTreeSet::new(),
            closed: true,
            reported: true,
            turn: None,
        }
    }

    #[test]
    fn the_checkpoint_of_any_state_the_size_check_leaves_fits_one_stream_record() {
        let mut core_bytes = 0;
        for depth in [MAX_DEPTH - 1, MAX_DEPTH] {
            let state = core(depth);
            let bytes = encode(&state, u64::MAX).expect("encodes");
            assert_eq!(decode(&bytes).expect("decodes"), (state, u64::MAX));
            core_bytes = core_bytes.max(bytes.len());
        }
        // Adoption volumes and policy are at most one inbox record of JSON,
        // and a settled outcome is bounded where it settles.
        let bound = core_bytes + MAX_RECORD_BYTES + MAX_OUTCOME_BYTES;
        assert!(
            bound <= MAX_CHECKPOINT_BYTES,
            "{bound} > {MAX_CHECKPOINT_BYTES}"
        );
        // Wrapped in the actor state record with its one inbox subscription.
        let inbox = core(MAX_DEPTH).path.inbox().expect("inbox");
        let codec = codec();
        let record = State::record_bound(
            codec.name.len() + codec.version.len() + MAX_CHECKPOINT_BYTES,
            INBOX.len() + inbox.as_str().len() + SUBSCRIPTION_OVERHEAD,
        );
        assert!(
            record <= acyclic_stream::MAX_RECORD_BYTES,
            "{record} > {}",
            acyclic_stream::MAX_RECORD_BYTES
        );
    }

    #[test]
    fn every_record_a_step_builds_from_bounded_parts_fits_its_limit() {
        use crate::agent::{
            AgentRecord, ChildDone, Closed, Control, InboxRecord, IntentId, MAX_INTENT_BYTES,
            Sender, StatusRecord,
        };
        assert!(MAX_RECORD_BYTES <= acyclic_stream::MAX_RECORD_BYTES);
        let deepest = core(MAX_DEPTH).path;
        let empty = AgentOutcome::Failed {
            message: String::new(),
        };
        // A report and a status carry at most one bounded outcome.
        let report = InboxRecord::ChildDone(ChildDone {
            child: deepest.clone(),
            outcome: empty.clone(),
        });
        let status = StatusRecord {
            phase: Phase::Done { outcome: empty },
            cursor: u64::MAX,
        };
        for bytes in [report.encode(), status.encode()] {
            let bound = bytes.expect("encodes").len() + MAX_OUTCOME_BYTES;
            assert!(bound <= MAX_RECORD_BYTES, "{bound} > {MAX_RECORD_BYTES}");
        }
        // An archive cascade's records: the deepest sender, the longest id.
        let id = IntentId::try_from("i".repeat(MAX_INTENT_BYTES)).expect("intent");
        let by = Sender::Agent {
            path: deepest.parent().expect("deep"),
        };
        InboxRecord::Archive(Control {
            by: by.clone(),
            id: id.clone(),
        })
        .encode()
        .expect("an archive fits");
        Closed { by, id }.encode().expect("a closed marker fits");
    }
}
