//! The records an agent subtree holds: inbox input, status and the close marker.
//!
//! The inbox is the only input to an agent. Every record carries a stable intent
//! identity, which with its sender forms the Streams idempotency key, so a retry
//! after a lost acknowledgement replays exactly and a definitive conflict leaves
//! the key unspent.

use super::{AgentId, AgentPath, Error};
use crate::conversation::{FileRef, VolumeRef};
use serde::{Deserialize, Serialize};

/// Largest agent record. Every part of one is bounded (paths by
/// [`super::MAX_DEPTH`], file and volume references by the conversation
/// maxima, texts by their own limits), so every record a step builds is
/// within this; `checkpoint`'s bound test checks each kind at its worst.
pub const MAX_RECORD_BYTES: usize = 56 * 1024;
/// Largest intent identity.
pub const MAX_INTENT_BYTES: usize = 128;
/// Largest failure message carried in a record.
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024;
/// Most volumes bound to one agent.
pub const MAX_VOLUMES: usize = 8;
/// Format version of every persisted agent record and saved turn effect,
/// carried by their one shared header. Any change to a persisted shape bumps
/// it, and readers refuse every other version whole: there is no migration
/// code. The headerless format before it is version 1; version 2 carries a
/// spawn brief by file reference. Version 3 binds goal runs to their committed
/// terminal conversation entry.
pub const RECORD_VERSION: u32 = 3;

/// A persisted record of a format version this build does not read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unsupported record version {found}; this build reads version {expected}")]
pub struct UnsupportedRecordVersion {
    /// The version the record carries.
    pub found: u32,
    /// The only version this build reads, [`RECORD_VERSION`].
    pub expected: u32,
}

/// Why a versioned record did not open.
pub(crate) enum Unopened {
    /// The record carries another format version.
    Version(UnsupportedRecordVersion),
    /// The bytes are not a well-formed record of this version.
    Malformed,
}

/// `record` under the shared header, as canonical JSON.
pub(crate) fn seal<T: Serialize + ?Sized>(record: &T) -> crate::Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Sealed<'a, T: ?Sized> {
        record: &'a T,
        version: u32,
    }
    crate::contract::canonical_json_bytes(&Sealed {
        record,
        version: RECORD_VERSION,
    })
}

/// The record under the shared header. The version is checked before any of
/// the record is decoded, so a record of another version is refused whole.
pub(crate) fn open<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Unopened> {
    let header: Header<'_> = serde_json::from_slice(bytes).map_err(|_| Unopened::Malformed)?;
    let found = header.version.unwrap_or(1);
    if found != RECORD_VERSION {
        return Err(Unopened::Version(UnsupportedRecordVersion {
            found,
            expected: RECORD_VERSION,
        }));
    }
    match header.record {
        Some(record) if !header.unknown => {
            serde_json::from_str(record.get()).map_err(|_| Unopened::Malformed)
        }
        _ => Err(Unopened::Malformed),
    }
}

/// The record of any agent stream as `T`, such as a JSON value for a
/// reader of every record kind: bounded by [`MAX_RECORD_BYTES`], and of any
/// version but [`RECORD_VERSION`] refused whole.
pub fn decode_record<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Record("record exceeds MAX_RECORD_BYTES"));
    }
    open(bytes).map_err(|unopened| match unopened {
        Unopened::Version(version) => Error::UnsupportedRecordVersion(version),
        Unopened::Malformed => Error::Record("record is malformed"),
    })
}

/// One pass over the header's keys. Unknown keys are noted, not refused, so
/// a record of another version, header or not, still reports its version.
struct Header<'a> {
    version: Option<u32>,
    record: Option<&'a serde_json::value::RawValue>,
    unknown: bool,
}

impl<'de> Deserialize<'de> for Header<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Keys;
        impl<'de> serde::de::Visitor<'de> for Keys {
            type Value = Header<'de>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a versioned record")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut header = Header {
                    version: None,
                    record: None,
                    unknown: false,
                };
                while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                    match &*key {
                        "version" if header.version.is_none() => {
                            header.version = Some(map.next_value()?);
                        }
                        "record" if header.record.is_none() => {
                            header.record = Some(map.next_value()?);
                        }
                        _ => {
                            header.unknown = true;
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(header)
            }
        }
        deserializer.deserialize_map(Keys)
    }
}

/// Stable caller-chosen identity of one intent (a message, cancel, archive, hook or approval).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IntentId(String);

impl IntentId {
    /// Canonical text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for IntentId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        if !value.is_empty()
            && value.len() <= MAX_INTENT_BYTES
            && value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            Ok(Self(value))
        } else {
            Err(Error::Record(
                "intent id must be 1-128 printable ASCII bytes",
            ))
        }
    }
}

impl TryFrom<&str> for IntentId {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Error> {
        Self::try_from(value.to_owned())
    }
}

impl From<IntentId> for String {
    fn from(value: IntentId) -> Self {
        value.0
    }
}

/// Who wrote an inbox record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Sender {
    /// A human or application principal.
    User {
        /// Principal identity as authenticated by the caller's grant.
        principal: String,
    },
    /// Another agent.
    Agent {
        /// The sending agent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        path: AgentPath,
    },
    /// A host plugin (Codex, Claude Code, Pi).
    Host {
        /// Host name.
        host: String,
    },
}

/// A named volume bound to an agent at adoption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeBinding {
    /// Agent-visible name (`private`, `project`, `result`).
    pub name: String,
    /// The volume: provider, name, class and owner.
    pub volume: VolumeRef,
}

/// First record of every agent, written by the spawn fork or by root creation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkAdopted {
    /// The adopted agent.
    pub agent: AgentId,
    /// Spawning agent, `None` for a root.
    pub parent: Option<AgentPath>,
    /// Inbox sequence of this record; the agent's first consumed input.
    pub forked_at: u64,
    /// Tail of the inherited conversation at the fork cut.
    pub conversation_tail: u64,
    /// Sequence of the inherited conversation's latest context checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<u64>,
    /// Volumes the agent owns.
    pub volumes: Vec<VolumeBinding>,
    /// Task brief, if any.
    pub brief: Option<FileRef>,
    /// Approval policy file interpreted by the approvals feature, if any.
    pub policy: Option<FileRef>,
}

/// An agent-to-agent or user-to-agent message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Sender.
    pub from: Sender,
    /// Stable message identity.
    pub id: IntentId,
    /// Message body.
    pub body: Box<FileRef>,
    /// The files the body's `@` mentions name, pinned when the message was
    /// sent: one attachment manifest whose own directory holds them, so the
    /// record stays one reference however many files it names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Box<FileRef>>,
}

/// Terminal result of an agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentOutcome {
    /// Completed, with an optional result file.
    Succeeded {
        /// Result file.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef | null"))]
        result: Option<Box<FileRef>>,
    },
    /// Failed with a bounded description.
    Failed {
        /// Failure description.
        message: String,
    },
    /// Stopped by archive before completing.
    Archived,
}

/// A child reached a terminal status.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ChildDone {
    /// The child.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub child: AgentPath,
    /// Its outcome.
    pub outcome: AgentOutcome,
}

/// A child awaited by its parent settled: it is idle having consumed every
/// record before the one that asked, or done.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ChildSettled {
    /// The child.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub child: AgentPath,
}

/// A response file now exists for an approval request of this agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    /// Approval request identity.
    pub id: IntentId,
}

/// An authorized caller asks this agent to spawn a child. The agent's next
/// step spawns it exactly as its own turn's spawn tool does; the child is
/// named by this record's inbox sequence, so no two requests collide.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnRequest {
    /// Who asked.
    pub by: Sender,
    /// Stable intent identity.
    pub id: IntentId,
    /// Task brief, staged on the parent's private volume.
    pub brief: Box<FileRef>,
}

/// A child delegates an approval request to this agent (the parent approver).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRequested {
    /// The requesting child.
    pub child: AgentPath,
    /// Approval request identity.
    pub id: IntentId,
}

/// Cancel or archive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
    /// Requester.
    pub by: Sender,
    /// Stable intent identity.
    pub id: IntentId,
}

/// A host plugin hook event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hook {
    /// Host name.
    pub host: String,
    /// Stable hook identity.
    pub id: IntentId,
    /// Event file.
    pub event: Box<FileRef>,
}

/// How a context request selects the agent's later model context. Each one
/// settles as a conversation entry, a checkpoint: every later turn's context
/// is the latest checkpoint's summary, then the turns settled after it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextAction {
    /// One model request summarizes the current context; its answer is the
    /// checkpoint's summary.
    Compact {
        /// Caller guidance for the summary, staged like a message body.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef | null"))]
        instructions: Option<Box<FileRef>>,
    },
    /// Restores the latest checkpoint, dropping every turn settled after it;
    /// with none, the conversation's start.
    Rewind,
    /// Retires the conversation: later turns start with no history.
    Clear,
}

/// Compact, rewind or clear the agent's model context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRequest {
    /// Requester.
    pub by: Sender,
    /// Stable intent identity.
    pub id: IntentId,
    /// The selection it makes.
    pub action: ContextAction,
}

/// The only input to an agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum InboxRecord {
    /// First record of every agent.
    ForkAdopted(Box<ForkAdopted>),
    /// `send`.
    Message(Message),
    /// A child finished.
    ChildDone(ChildDone),
    /// The parent's turn waits for this agent to settle: once it is idle or
    /// done it answers with one [`ChildSettled`].
    Awaited,
    /// A child this agent's turn waits for settled.
    ChildSettled(ChildSettled),
    /// An approval response exists.
    Approval(Approval),
    /// A child delegates an approval.
    ApprovalRequested(ApprovalRequested),
    /// Stop the current turn; the agent stays open.
    Cancel(Control),
    /// Close the agent.
    Archive(Control),
    /// A host plugin hook.
    Hook(Hook),
    /// Spawn a child on an authorized caller's request.
    Spawn(SpawnRequest),
    /// Compact, rewind or clear the model context.
    Context(ContextRequest),
}

/// Current phase of an agent, appended to its status stream on change and on
/// the step that creates its conversation stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Phase {
    /// No queued input.
    Idle,
    /// A turn is executing.
    Running,
    /// Waiting for an approval response.
    WaitingApproval {
        /// Request identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Waiting for an event hook from its host.
    WaitingHost {
        /// Host name.
        host: String,
    },
    /// A turn's `wait` waits for its children to settle, holding no delivery:
    /// each child's [`ChildSettled`] or [`ChildDone`] wakes it.
    WaitingChildren {
        /// Whether the first awaited child to settle resumes the turn, rather
        /// than the last.
        any: bool,
    },
    /// Waiting for a deadline.
    WaitingAlarm {
        /// Unix milliseconds.
        deadline_unix_ms: u64,
    },
    /// Terminal.
    Done {
        /// Outcome.
        outcome: AgentOutcome,
    },
}

impl Phase {
    /// Whether a turn is open: running, or waiting on an approval, its host,
    /// its children or a deadline. Idle and done agents hold none.
    #[must_use]
    pub const fn in_turn(&self) -> bool {
        match self {
            Self::Idle | Self::Done { .. } => false,
            Self::Running
            | Self::WaitingApproval { .. }
            | Self::WaitingHost { .. }
            | Self::WaitingChildren { .. }
            | Self::WaitingAlarm { .. } => true,
        }
    }
}

/// One status stream record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct StatusRecord {
    /// Phase after the transition that wrote it.
    pub phase: Phase,
    /// Inbox cursor consumed by that transition.
    pub cursor: u64,
    /// The agent's conversation length after that transition, committed
    /// with it: the conversation stream exists once this is above zero.
    #[serde(default)]
    pub conversation: u64,
}

impl StatusRecord {
    /// JSON status plus Streams' opaque LATEST metadata. Attention transitions
    /// survive conflation; every publisher uses this same record constructor.
    pub fn append_record(&self) -> Result<acyclic_stream::AppendRecord, Error> {
        acyclic_stream::AppendRecord::latest(
            self.encode()?,
            "status",
            matches!(
                self.phase,
                Phase::WaitingApproval { .. } | Phase::WaitingHost { .. } | Phase::Done { .. }
            ),
        )
        .map_err(Error::Stream)
    }
}

/// The single record of the close marker stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Closed {
    /// Requester.
    pub by: Sender,
    /// Archive intent identity.
    pub id: IntentId,
}

/// Canonical bounded encoding shared by every agent record.
pub trait AgentRecord: Serialize + for<'de> Deserialize<'de> {
    /// Structural bounds beyond the type itself.
    fn check(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Canonical JSON with sorted keys under the shared [`RECORD_VERSION`]
    /// header, refused above [`MAX_RECORD_BYTES`].
    fn encode(&self) -> Result<Vec<u8>, Error> {
        self.check()?;
        let bytes = seal(self).map_err(|_| Error::Record("record is not encodable"))?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(Error::Record("record exceeds MAX_RECORD_BYTES"));
        }
        Ok(bytes)
    }

    /// Total, bounded decode of untrusted bytes; a record of any version but
    /// [`RECORD_VERSION`] is refused whole.
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let value: Self = decode_record(bytes)?;
        value.check()?;
        Ok(value)
    }
}

fn check_outcome(outcome: &AgentOutcome) -> Result<(), Error> {
    match outcome {
        AgentOutcome::Failed { message } if message.len() > MAX_MESSAGE_BYTES => {
            Err(Error::Record("failure message exceeds MAX_MESSAGE_BYTES"))
        }
        _ => Ok(()),
    }
}

/// At most [`MAX_VOLUMES`] bindings, each with a valid name.
pub(super) fn check_volumes(volumes: &[VolumeBinding]) -> Result<(), Error> {
    if volumes.len() > MAX_VOLUMES {
        return Err(Error::Record("too many volumes"));
    }
    volumes
        .iter()
        .try_for_each(|binding| check_text(&binding.name))
}

/// A name: a binding, principal or host. Like a label, it admits no control
/// character, so its encoding is at most twice its length.
pub(super) fn check_text(text: &str) -> Result<(), Error> {
    if text.is_empty() || text.len() > MAX_INTENT_BYTES || text.chars().any(char::is_control) {
        return Err(Error::Record(
            "name must be 1-128 bytes without control characters",
        ));
    }
    Ok(())
}

impl AgentRecord for InboxRecord {
    fn check(&self) -> Result<(), Error> {
        match self {
            Self::ForkAdopted(adopted) => {
                check_volumes(&adopted.volumes)?;
                if adopted
                    .parent
                    .as_ref()
                    .is_some_and(|parent| parent.child(&adopted.agent).is_err())
                {
                    return Err(Error::Record("adopted child exceeds MAX_DEPTH"));
                }
                Ok(())
            }
            Self::ChildDone(done) => check_outcome(&done.outcome),
            Self::Message(Message { from, .. })
            | Self::Cancel(Control { by: from, .. })
            | Self::Archive(Control { by: from, .. })
            | Self::Context(ContextRequest { by: from, .. }) => check_sender(from),
            Self::Hook(hook) => check_text(&hook.host),
            Self::Spawn(spawn) => check_sender(&spawn.by),
            Self::Approval(_)
            | Self::ApprovalRequested(_)
            | Self::Awaited
            | Self::ChildSettled(_) => Ok(()),
        }
    }
}

fn check_sender(sender: &Sender) -> Result<(), Error> {
    match sender {
        Sender::User { principal } => check_text(principal),
        Sender::Host { host } => check_text(host),
        Sender::Agent { .. } => Ok(()),
    }
}

impl AgentRecord for StatusRecord {
    fn check(&self) -> Result<(), Error> {
        match &self.phase {
            Phase::Done { outcome } => check_outcome(outcome),
            Phase::WaitingHost { host } => check_text(host),
            Phase::Idle
            | Phase::Running
            | Phase::WaitingApproval { .. }
            | Phase::WaitingChildren { .. }
            | Phase::WaitingAlarm { .. } => Ok(()),
        }
    }
}

impl AgentRecord for Closed {
    fn check(&self) -> Result<(), Error> {
        check_sender(&self.by)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        conversation::{FileDescriptor, VolumeClass, VolumeOwner},
        resources::ProviderRef,
    };

    /// A spawn request of the headerless version 1, its brief a string.
    const V1_SPAWN: &[u8] =
        br#"{"spawn":{"brief":"review the diff","by":{"user":{"principal":"p"}},"id":"spawn-1"}}"#;

    fn refused(bytes: &[u8]) -> UnsupportedRecordVersion {
        match InboxRecord::decode(bytes) {
            Err(Error::UnsupportedRecordVersion(version)) => version,
            other => panic!("expected a version refusal, got {other:?}"),
        }
    }

    #[test]
    fn an_inbox_record_of_another_version_is_refused_whole() -> crate::Result<()> {
        assert_eq!(
            refused(V1_SPAWN),
            UnsupportedRecordVersion {
                found: 1,
                expected: RECORD_VERSION
            }
        );
        // The header decides: a later version is refused before its record is read.
        assert_eq!(
            refused(br#"{"record":{"spawn":"not read"},"version":3}"#).found,
            3
        );
        // Negative controls. A spawn of this version decodes; the version 1
        // body under this version's header is malformed, never a partial spawn.
        let volume = VolumeRef::new(
            ProviderRef::new("agents".into(), "filesystem".into(), "2".into(), &mut None)?,
            "records-test".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([3; 16])),
            &mut None,
        )?;
        let spawn = InboxRecord::Spawn(SpawnRequest {
            by: Sender::User {
                principal: "p".into(),
            },
            id: IntentId::try_from("spawn-1")
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
            brief: Box::new(FileRef::new(
                volume,
                "briefs/spawn-1.md".into(),
                "version-1".into(),
                FileDescriptor::from_bytes(b"review the diff", "text/markdown", &mut None)?,
                "brief".into(),
                &mut None,
            )?),
        });
        let encoded = spawn
            .encode()
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        assert_eq!(InboxRecord::decode(&encoded).ok(), Some(spawn));
        let mut relabeled = br#"{"record":"#.to_vec();
        relabeled.extend_from_slice(V1_SPAWN);
        relabeled.extend_from_slice(br#","version":2}"#);
        assert!(matches!(
            InboxRecord::decode(&relabeled),
            Err(Error::Record("record is malformed"))
        ));
        // This version's header admits nothing beside its record.
        let mut extra = encoded[..encoded.len() - 1].to_vec();
        extra.extend_from_slice(br#","extra":0}"#);
        assert!(matches!(
            InboxRecord::decode(&extra),
            Err(Error::Record("record is malformed"))
        ));
        Ok(())
    }
}
