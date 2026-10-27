//! The agent checkpoint's one encoding, and the static size bound of every
//! record an agent step writes.
//!
//! The parts that scale with the agent's limits are raw protobuf fields
//! rather than JSON: each open child is its 16-byte id (its path, like the
//! agent's parent, is derived from the agent's path), the children still to
//! archive are a bitmap over them, and the turn driver's checkpoint is raw
//! bytes. The rest of the state is JSON.
//!
//! Every part of every agent record is bounded at admission: paths by
//! [`super::MAX_DEPTH`], file and volume references by the conversation
//! maxima ([`crate::conversation::MAX_PATH_BYTES`] and
//! [`crate::conversation::MAX_LABEL_BYTES`], enforced wherever a reference is
//! built or decoded), and texts by their own limits. `tests` builds the worst
//! case of each record kind a step appends and checks it encodes, and the
//! worst settled checkpoint within [`MAX_CHECKPOINT_BYTES`] and one stream
//! record. Only the input window and the turn grow with traffic; a state
//! whose window and turn would not fit fails the agent visibly instead (see
//! [`super::machine`]), so no step can wedge an agent on a record its stream
//! refuses.
use super::{AgentId, AgentState, Error};
use acyclic_actors::guest::{Checkpoint, CodecIdentity};
use prost::Message as _;
use std::collections::BTreeSet;

/// Largest agent checkpoint.
pub const MAX_CHECKPOINT_BYTES: usize = 60 * 1024;
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
pub(crate) fn checkpoint_state(checkpoint: &Checkpoint) -> Result<(AgentState, u64), Error> {
    if checkpoint.codec != codec() {
        return Err(Error::Record("agent checkpoint codec differs"));
    }
    decode(&checkpoint.bytes)
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
    /// Inbox sequence of the input that started the turn.
    #[prost(uint64, optional, tag = "6")]
    turn_inbox: Option<u64>,
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
    if rest.parent.take() != state.path.parent() {
        return Err(Error::Record("agent parent is not its path's parent"));
    }
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
        turn_inbox: turn.as_ref().map(|turn| turn.inbox),
        turn: turn.map(|turn| turn.checkpoint),
    }
    .encode_to_vec())
}

/// The state and cursor checkpoint `bytes` hold.
pub(crate) fn decode(bytes: &[u8]) -> Result<(AgentState, u64), Error> {
    let malformed = || Error::Record("agent checkpoint is malformed");
    let encoded = Encoded::decode(bytes).map_err(|_| malformed())?;
    let mut state: AgentState = serde_json::from_slice(&encoded.state).map_err(|_| malformed())?;
    if !state.open_children.is_empty()
        || !state.archive_pending.is_empty()
        || state.turn.is_some()
        || state.parent.is_some()
    {
        return Err(malformed());
    }
    state.parent = state.path.parent();
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
    state.turn = match (encoded.turn, encoded.turn_inbox) {
        (Some(checkpoint), Some(inbox)) => Some(super::Progress { inbox, checkpoint }),
        (None, None) => None,
        _ => return Err(malformed()),
    };
    Ok((state, encoded.cursor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{
        AgentOutcome, AgentPath, AgentRecord, ChildDone, Closed, Control, ConversationEntry,
        ForkAdopted, Hook, INBOX, InboxRecord, IntentId, MAX_DEPTH, MAX_INTENT_BYTES,
        MAX_MESSAGE_BYTES, MAX_OPEN_CHILDREN, MAX_OUTCOME_BYTES, MAX_RECORD_BYTES, MAX_VOLUMES,
        Message, Phase, Sender, SpawnRequest, StatusRecord, TurnInput, VolumeBinding,
    };
    use crate::conversation::{
        FileDescriptor, FileRef, MAX_EXACT_JS_INTEGER, MAX_LABEL_BYTES, MAX_PATH_BYTES,
        VolumeClass, VolumeOwner, VolumeRef,
    };
    use crate::resources::ProviderRef;
    use acyclic_actors::local::{SUBSCRIPTION_OVERHEAD, State};

    fn id(n: u64) -> AgentId {
        let mut bytes = [0xff; 16];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        AgentId::from_bytes(bytes)
    }

    /// The widest name, label or path of `bytes` bytes: none admits a control
    /// character, so `"`, which JSON doubles, escapes most.
    fn widest(bytes: usize) -> String {
        "\"".repeat(bytes)
    }

    /// The widest free text of `bytes` bytes: a control character, which JSON
    /// escapes to six bytes.
    fn widest_text(bytes: usize) -> String {
        "\0".repeat(bytes)
    }

    /// The widest failure a turn settles with: the longest control-character
    /// message within both its own limit and [`MAX_OUTCOME_BYTES`].
    fn widest_failure() -> AgentOutcome {
        let failure = |n| AgentOutcome::Failed {
            message: widest_text(n),
        };
        let fits = |n| serde_json::to_vec(&failure(n)).expect("outcome").len() <= MAX_OUTCOME_BYTES;
        let n = (0..=MAX_MESSAGE_BYTES)
            .rev()
            .find(|&n| fits(n))
            .expect("fits");
        failure(n)
    }

    fn agent(depth: usize) -> AgentPath {
        let mut path = AgentPath::root(&id(0));
        for level in 1..=depth {
            path = path.child(&id(level as u64)).expect("within MAX_DEPTH");
        }
        path
    }

    fn intent() -> IntentId {
        IntentId::try_from(widest(MAX_INTENT_BYTES)).expect("intent")
    }

    fn volume() -> VolumeRef {
        let part = || widest(MAX_LABEL_BYTES);
        let provider = ProviderRef::new(part(), part(), part(), &mut None).expect("provider");
        let owner = VolumeOwner::Project(part());
        VolumeRef::new(provider, part(), VolumeClass::Project, owner, &mut None).expect("volume")
    }

    fn file() -> FileRef {
        let media = format!("a/{}", "a".repeat(MAX_LABEL_BYTES - 2));
        let descriptor =
            FileDescriptor::new([0xff; 32], MAX_EXACT_JS_INTEGER, media).expect("descriptor");
        FileRef::new(
            volume(),
            widest(MAX_PATH_BYTES),
            widest(MAX_LABEL_BYTES),
            descriptor,
            widest(MAX_LABEL_BYTES),
            &mut None,
        )
        .expect("file")
    }

    fn outcomes() -> Vec<AgentOutcome> {
        vec![
            AgentOutcome::Succeeded {
                result: Some(Box::new(file())),
            },
            widest_failure(),
        ]
    }

    fn senders() -> Vec<Sender> {
        vec![
            Sender::Agent {
                path: agent(MAX_DEPTH),
            },
            Sender::User {
                principal: widest(MAX_INTENT_BYTES),
            },
            Sender::Host {
                host: widest(MAX_INTENT_BYTES),
            },
        ]
    }

    /// The widest adoption: deepest parent, every volume, brief and policy.
    fn adoption() -> ForkAdopted {
        ForkAdopted {
            agent: id(u64::MAX),
            parent: Some(agent(MAX_DEPTH - 1)),
            forked_at: u64::MAX,
            conversation_tail: u64::MAX,
            volumes: (0..MAX_VOLUMES)
                .map(|_| VolumeBinding {
                    name: widest(MAX_INTENT_BYTES),
                    volume: volume(),
                })
                .collect(),
            brief: Some(file()),
            policy: Some(file()),
        }
    }

    /// Every record kind a step sends, each at its widest.
    fn inbox_records() -> Vec<InboxRecord> {
        let deepest = agent(MAX_DEPTH);
        let mut records = vec![
            InboxRecord::ForkAdopted(Box::new(adoption())),
            InboxRecord::Approval(crate::agent::Approval { id: intent() }),
            InboxRecord::ApprovalRequested(crate::agent::ApprovalRequested {
                child: deepest.clone(),
                id: intent(),
            }),
            InboxRecord::Hook(Hook {
                host: widest(MAX_INTENT_BYTES),
                id: intent(),
                event: Box::new(file()),
            }),
        ];
        for outcome in outcomes() {
            records.push(InboxRecord::ChildDone(ChildDone {
                child: deepest.clone(),
                outcome,
            }));
        }
        for from in senders() {
            records.push(InboxRecord::Message(Message {
                from: from.clone(),
                id: intent(),
                body: Box::new(file()),
            }));
            records.push(InboxRecord::Spawn(SpawnRequest {
                by: from.clone(),
                id: intent(),
                brief: widest_text(MAX_MESSAGE_BYTES),
            }));
            for control in [InboxRecord::Cancel, InboxRecord::Archive] {
                records.push(control(Control {
                    by: from.clone(),
                    id: intent(),
                }));
            }
        }
        records
    }

    /// Every turn input, each at its widest.
    fn turn_inputs() -> Vec<TurnInput> {
        let mut inputs = vec![TurnInput::Brief {
            brief: Box::new(file()),
        }];
        for (sequence, record) in inbox_records().into_iter().enumerate() {
            inputs.extend(
                super::super::input::input(sequence as u64, record).map(|queued| queued.input),
            );
        }
        inputs
    }

    #[test]
    fn every_record_a_step_writes_fits_its_limit_at_the_configured_maxima() {
        assert!(MAX_RECORD_BYTES <= acyclic_stream::MAX_RECORD_BYTES);
        // Names admit no control character, so `widest` is their worst case.
        let control = Sender::User {
            principal: "\0".into(),
        };
        assert!(
            InboxRecord::Message(Message {
                from: control,
                id: intent(),
                body: Box::new(file()),
            })
            .encode()
            .is_err()
        );
        let deepest = agent(MAX_DEPTH);
        // Sends, adoptions (host-bound volumes included), reports, archives.
        for record in inbox_records() {
            record.encode().expect("every inbox record fits");
        }
        for by in senders() {
            Closed { by, id: intent() }
                .encode()
                .expect("a closed marker fits");
        }
        // Statuses and conversation entries.
        for outcome in outcomes() {
            let encoded = serde_json::to_vec(&outcome).expect("outcome");
            assert!(encoded.len() <= MAX_OUTCOME_BYTES, "{}", encoded.len());
            StatusRecord {
                phase: Phase::Done { outcome },
                cursor: u64::MAX,
            }
            .encode()
            .expect("a status fits");
        }
        for input in turn_inputs() {
            ConversationEntry {
                inbox: u64::MAX,
                input,
                output: file(),
            }
            .encode()
            .expect("a conversation entry fits");
        }
        // The widest settled checkpoint: deepest agent that can have children,
        // every child open and to archive, the widest adoption, settled with
        // the widest outcome, counters at their widest.
        let adopted = adoption();
        for depth in [MAX_DEPTH - 1, MAX_DEPTH] {
            let path = agent(depth);
            let children = if depth < MAX_DEPTH {
                MAX_OPEN_CHILDREN
            } else {
                0
            };
            let open_children: BTreeSet<_> = (0..children as u64)
                .map(|n| path.child(&id(n)).expect("below MAX_DEPTH"))
                .collect();
            for outcome in outcomes() {
                let state = AgentState {
                    parent: path.parent(),
                    path: path.clone(),
                    volumes: adopted.volumes.clone(),
                    policy: adopted.policy.clone(),
                    conversation_base: u64::MAX,
                    conversation_tail: u64::MAX,
                    status_tail: u64::MAX,
                    phase: Phase::Done { outcome },
                    archive_pending: open_children.clone(),
                    open_children: open_children.clone(),
                    queued: Default::default(),
                    overflow: None,
                    taken: BTreeSet::new(),
                    closed: true,
                    reported: true,
                    turn: None,
                    reserved_children: u16::MAX,
                };
                let bytes = encode(&state, u64::MAX).expect("encodes");
                assert!(
                    bytes.len() + TAIL_SLACK <= MAX_CHECKPOINT_BYTES,
                    "{} > {MAX_CHECKPOINT_BYTES}",
                    bytes.len() + TAIL_SLACK
                );
                assert_eq!(decode(&bytes).expect("decodes"), (state, u64::MAX));
            }
        }
        // Wrapped in the actor state record with its one inbox subscription.
        let inbox = deepest.inbox().expect("inbox");
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
}
