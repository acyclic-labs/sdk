//! The agent checkpoint's one encoding, and the static size bound of every
//! record an agent step writes.
//!
//! The parts that scale with the agent's limits are raw protobuf fields
//! rather than JSON: each open child is its 16-byte id (its path, like the
//! agent's parent, is derived from the agent's path), the children still to
//! archive and the children awaited are bitmaps over them, and the turn
//! driver's checkpoint is raw bytes. The rest of the state is JSON.
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
/// Maximum encoded width of the conversation and status tails, used by the
/// worst-case record construction proof.
#[cfg(test)]
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
    decode(&checkpoint.bytes)
}

#[derive(Clone, PartialEq, prost::Message)]
struct Encoded {
    // Tag 1 retired: Actors owns the committed inbox cursor.
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
    /// Bit `i` set: the turn waits for open child `i` to settle.
    #[prost(bytes = "vec", tag = "7")]
    awaiting: Vec<u8>,
}

/// Retains the exact encoding checked against the selected checkpoint limit.
pub(super) fn from_encoded(bytes: Vec<u8>) -> Result<Checkpoint, Error> {
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

/// Artifact state as checkpoint bytes. Subscription cuts belong to Actors.
pub(crate) fn encode(state: &AgentState) -> Result<Vec<u8>, Error> {
    let mut rest = state.clone();
    let open_children = std::mem::take(&mut rest.open_children);
    let archive_pending = std::mem::take(&mut rest.archive_pending);
    let awaiting = std::mem::take(&mut rest.awaiting);
    let turn = rest.turn.take();
    if rest.parent.take() != state.path.parent() {
        return Err(Error::Record("agent parent is not its path's parent"));
    }
    let mut ids = Vec::with_capacity(open_children.len());
    let mut bitmap = vec![0; open_children.len().div_ceil(8)];
    let mut awaited = vec![0; open_children.len().div_ceil(8)];
    for (index, child) in open_children.iter().enumerate() {
        if child.parent().as_ref() != Some(&state.path) {
            return Err(Error::Record("an open child is not a direct child"));
        }
        ids.push(child.id().to_bytes().to_vec());
        if archive_pending.contains(child) {
            bitmap[index / 8] |= 1 << (index % 8);
        }
        if awaiting.contains(child) {
            awaited[index / 8] |= 1 << (index % 8);
        }
    }
    if !archive_pending.is_subset(&open_children) {
        return Err(Error::Record("a child to archive is not open"));
    }
    if !awaiting.is_subset(&open_children) {
        return Err(Error::Record("an awaited child is not open"));
    }
    Ok(Encoded {
        state: serde_json::to_vec(&rest)
            .map_err(|_| Error::Record("agent checkpoint does not encode"))?,
        open_children: ids,
        archive_pending: bitmap,
        // Empty unless a turn waits: no bytes in every other checkpoint.
        awaiting: if awaiting.is_empty() {
            Vec::new()
        } else {
            awaited
        },
        turn_inbox: turn.as_ref().map(|turn| turn.inbox),
        turn: turn.map(|turn| turn.checkpoint),
    }
    .encode_to_vec())
}

/// The artifact state checkpoint `bytes` holds.
pub(crate) fn decode(bytes: &[u8]) -> Result<AgentState, Error> {
    let malformed = || Error::Record("agent checkpoint is malformed");
    let encoded = Encoded::decode(bytes).map_err(|_| malformed())?;
    let mut state: AgentState = serde_json::from_slice(&encoded.state).map_err(|_| malformed())?;
    if !state.open_children.is_empty()
        || !state.archive_pending.is_empty()
        || !state.awaiting.is_empty()
        || state.turn.is_some()
        || state.parent.is_some()
    {
        return Err(malformed());
    }
    state.parent = state.path.parent();
    let bitmap = encoded.open_children.len().div_ceil(8);
    if encoded.archive_pending.len() != bitmap
        || !(encoded.awaiting.is_empty() || encoded.awaiting.len() == bitmap)
    {
        return Err(malformed());
    }
    let mut open_children = BTreeSet::new();
    let mut archive_pending = BTreeSet::new();
    let mut awaiting = BTreeSet::new();
    for (index, id) in encoded.open_children.iter().enumerate() {
        let id: [u8; 16] = id.as_slice().try_into().map_err(|_| malformed())?;
        let child = state.path.child(&AgentId::from_bytes(id))?;
        if encoded.archive_pending[index / 8] & (1 << (index % 8)) != 0 {
            archive_pending.insert(child.clone());
        }
        if encoded
            .awaiting
            .get(index / 8)
            .is_some_and(|bits| bits & (1 << (index % 8)) != 0)
        {
            awaiting.insert(child.clone());
        }
        if !open_children.insert(child) {
            return Err(malformed());
        }
    }
    state.open_children = open_children;
    state.archive_pending = archive_pending;
    state.awaiting = awaiting;
    state.turn = match (encoded.turn, encoded.turn_inbox) {
        (Some(checkpoint), Some(inbox)) => Some(super::Progress { inbox, checkpoint }),
        (None, None) => None,
        _ => return Err(malformed()),
    };
    Ok(state)
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
        let overhead = serde_json::to_vec(&failure(0)).expect("outcome").len();
        let n = ((MAX_OUTCOME_BYTES - overhead) / 6).min(MAX_MESSAGE_BYTES);
        assert!(serde_json::to_vec(&failure(n)).expect("outcome").len() <= MAX_OUTCOME_BYTES);
        if n < MAX_MESSAGE_BYTES {
            assert!(
                serde_json::to_vec(&failure(n + 1)).expect("outcome").len() > MAX_OUTCOME_BYTES
            );
        }
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
            AgentOutcome::Succeeded { result: None },
            AgentOutcome::Succeeded {
                result: Some(Box::new(file())),
            },
            widest_failure(),
            AgentOutcome::Archived,
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
            checkpoint: Some(u64::MAX),
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
            InboxRecord::Awaited,
            InboxRecord::ChildSettled(crate::agent::ChildSettled {
                child: deepest.clone(),
            }),
        ];
        for outcome in outcomes().into_iter().chain([AgentOutcome::Failed {
            message: widest_text(MAX_MESSAGE_BYTES),
        }]) {
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
                attachments: None,
            }));
            records.push(InboxRecord::Spawn(SpawnRequest {
                by: from.clone(),
                id: intent(),
                brief: Box::new(file()),
            }));
            for action in [
                crate::agent::ContextAction::Compact {
                    instructions: Some(Box::new(file())),
                },
                crate::agent::ContextAction::Rewind,
                crate::agent::ContextAction::Clear,
            ] {
                records.push(InboxRecord::Context(crate::agent::ContextRequest {
                    by: from.clone(),
                    id: intent(),
                    action,
                }));
            }
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
                attachments: None,
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
                conversation: u64::MAX,
            }
            .encode()
            .expect("a status fits");
        }
        for input in turn_inputs() {
            ConversationEntry {
                inbox: u64::MAX,
                input,
                output: file(),
                outcome: crate::agent::TurnOutcome::Cancelled,
                goal: Some(crate::OperationId::from_bytes([255; 16])),
            }
            .encode()
            .expect("a conversation entry fits");
        }
        // The widest settled checkpoint: deepest agent that can have children,
        // every child open and to archive, the widest adoption, settled with
        // the widest outcome, counters at their widest, every child awaited.
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
                    brief: Some([u8::MAX; 32]),
                    conversation_base: u64::MAX,
                    conversation_tail: u64::MAX,
                    dropped_before: u64::MAX,
                    checkpoint: Some(u64::MAX),
                    status_tail: u64::MAX,
                    phase: Phase::Done { outcome },
                    archive_pending: open_children.clone(),
                    awaiting: open_children.clone(),
                    awaited: true,
                    open_children: open_children.clone(),
                    queued: Default::default(),
                    overflow: None,
                    taken: BTreeSet::new(),
                    closed: true,
                    reported: true,
                    turn: None,
                    reserved_children: u16::MAX,
                };
                let bytes = encode(&state).expect("encodes");
                assert!(
                    bytes.len() + TAIL_SLACK <= MAX_CHECKPOINT_BYTES,
                    "{} > {MAX_CHECKPOINT_BYTES}",
                    bytes.len() + TAIL_SLACK
                );
                assert_eq!(decode(&bytes).expect("decodes"), state);
                // Retired tag 1 cannot override Actors' committed cut. Keep
                // the same artifact decoder for checkpoints carrying it.
                let mut with_retired_cursor = bytes;
                with_retired_cursor.extend_from_slice(&[0x08, 0x01]);
                assert_eq!(decode(&with_retired_cursor).expect("retired cursor"), state);
            }
        }
        // Wrapped in the actor state record with its one inbox subscription.
        let inbox = deepest.inbox().expect("inbox");
        let codec = codec();
        let record = State::record_bound(
            codec.name.len() + codec.version.len() + MAX_CHECKPOINT_BYTES,
            INBOX.len() + inbox.as_str().len() + SUBSCRIPTION_OVERHEAD,
            0,
        );
        assert!(
            record <= acyclic_stream::MAX_RECORD_BYTES,
            "{record} > {}",
            acyclic_stream::MAX_RECORD_BYTES
        );
    }

    // Exercise the canonical encoder itself, before its size refusal. A refused
    // oversized record is a failure of this property, not a passing bound.
    fn assert_record_bound<T: AgentRecord>(record: &T) -> usize {
        record
            .check()
            .expect("generated record satisfies structural admission");
        let raw = crate::agent::records::seal(record).expect("canonical record");
        assert!(
            raw.len() <= MAX_RECORD_BYTES,
            "{}: {} > {MAX_RECORD_BYTES}",
            std::any::type_name::<T>(),
            raw.len()
        );
        assert_eq!(
            record.encode().expect("bounded encoder accepts record"),
            raw
        );
        raw.len()
    }

    fn vary_text(value: &mut serde_json::Value, name: u8, text: u8) {
        match value {
            serde_json::Value::String(value) => {
                if !value.is_empty() && value.bytes().all(|byte| byte == b'"') {
                    *value = char::from(name).to_string().repeat(value.len());
                } else if !value.is_empty() && value.bytes().all(|byte| byte == 0) {
                    *value = char::from(text).to_string().repeat(value.len());
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    vary_text(value, name, text);
                }
            }
            serde_json::Value::Object(values) => {
                for value in values.values_mut() {
                    vary_text(value, name, text);
                }
            }
            _ => {}
        }
    }

    fn varied<T: serde::Serialize + serde::de::DeserializeOwned>(
        record: &T,
        name: u8,
        text: u8,
    ) -> T {
        let mut value = serde_json::to_value(record).expect("record value");
        vary_text(&mut value, name, text);
        serde_json::from_value(value).expect("bounded field variation")
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn record_bounds_cover_every_kind_and_json_escape_expansion(
            name in proptest::sample::select(vec![b'"', b'a', b'~']),
            text in proptest::sample::select(vec![0, 1, 8, 9, 10, 13, b'"', b'\\', b'a']),
            cursor in proptest::num::u64::ANY,
        ) {
            // Every generated case includes every variant, at maximum field
            // lengths; selection never accidentally omits a large record kind.
            for record in inbox_records() {
                let worst = assert_record_bound(&record);
                let sample: InboxRecord = varied(&record, name, text);
                proptest::prop_assert!(assert_record_bound(&sample) <= worst);
            }
            for by in senders() {
                let record = Closed { by, id: intent() };
                let worst = assert_record_bound(&record);
                let sample: Closed = varied(&record, name, text);
                proptest::prop_assert!(assert_record_bound(&sample) <= worst);
            }
            let phases = [Phase::Idle, Phase::Running,
                Phase::WaitingApproval { id: intent() },
                Phase::WaitingHost { host: widest(MAX_INTENT_BYTES) },
                Phase::WaitingAlarm { deadline_unix_ms: u64::MAX }];
            let record_outcomes = outcomes().into_iter().chain([AgentOutcome::Failed {
                message: widest_text(MAX_MESSAGE_BYTES),
            }]);
            for phase in phases.into_iter().chain(record_outcomes.map(|outcome| Phase::Done { outcome })) {
                let record = StatusRecord { phase, cursor: u64::MAX, conversation: u64::MAX };
                let worst = assert_record_bound(&record);
                let mut sample: StatusRecord = varied(&record, name, text);
                sample.cursor = cursor;
                proptest::prop_assert!(assert_record_bound(&sample) <= worst);
            }
            for input in turn_inputs() {
                let record = ConversationEntry { inbox: u64::MAX, input, output: file(), outcome: crate::agent::TurnOutcome::Cancelled, goal: Some(crate::OperationId::from_bytes([255; 16])) };
                let worst = assert_record_bound(&record);
                let sample: ConversationEntry = varied(&record, name, text);
                proptest::prop_assert!(assert_record_bound(&sample) <= worst);
            }
        }
    }

    #[test]
    fn waiting_host_status_requires_a_bounded_name() {
        for host in [String::new(), widest(MAX_INTENT_BYTES + 1), "\0".into()] {
            let record = StatusRecord {
                phase: Phase::WaitingHost { host },
                cursor: 0,
                conversation: 0,
            };
            assert!(
                record.check().is_err(),
                "unbounded or malformed host name admitted"
            );
        }
        let record = StatusRecord {
            phase: Phase::WaitingHost {
                host: widest(MAX_INTENT_BYTES),
            },
            cursor: u64::MAX,
            conversation: u64::MAX,
        };
        assert_record_bound(&record);
    }
}
