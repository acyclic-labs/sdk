//! The records an agent subtree holds: inbox input, status and the close marker.
//!
//! The inbox is the only input to an agent. Every record carries a stable intent
//! identity, which with its sender forms the Streams idempotency key, so a retry
//! after a lost acknowledgement replays exactly and a definitive conflict leaves
//! the key unspent.

use super::{AgentId, AgentPath, Error, WorkspaceChanged, WorkspaceEntry};
use crate::conversation::FileRef;
use serde::{Deserialize, Serialize};

/// Largest agent record. Every part of one is bounded (paths by
/// the primitive record envelope, file and volume references by the conversation
/// maxima, texts by their own limits), so every record a step builds is
/// within this; `checkpoint`'s bound test checks each kind at its worst.
pub const MAX_RECORD_BYTES: usize = 56 * 1024;
/// Largest external intent identity or spawn-name base. Generated spawn intents
/// append a separator and a canonical nonzero `u64` generation.
pub const MAX_INTENT_BYTES: usize = 128;
/// Inline failure packing threshold; longer terminal text uses a Diagnostic.
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024;
/// Version-three records use host-owned external interaction storage. Other formats are refused whole;
/// sessions must start fresh, with no migration or compatibility reader.
pub const RECORD_VERSION: u32 = 3;

/// Version-one creation event in an agent's append-only membership stream.
/// The first event names the owner; subsequent events name its direct children.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Membership {
    /// Schema version, always one.
    pub version: u32,
    /// Agent created atomically with this event.
    pub agent: AgentPath,
}

/// A persisted record of a format version this build does not read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "state format predates v1 table; start a fresh session (record {found}, expected {expected})"
)]
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

/// The canonical record after its fixed-width version. Named maps retain
/// optional fields without positional omissions, and strings never expand as
/// JSON escapes. Every authority field remains in the original serde shape.
pub(crate) fn seal<T: Serialize + ?Sized>(record: &T) -> crate::Result<Vec<u8>> {
    let mut bytes = RECORD_VERSION.to_be_bytes().to_vec();
    write_payload(&mut bytes, record)?;
    Ok(bytes)
}

/// The same payload encoding used by records and the actor checkpoint.
pub(super) fn encode_payload<T: Serialize + ?Sized>(value: &T) -> crate::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    write_payload(&mut bytes, value)?;
    Ok(bytes)
}

fn write_payload<W: std::io::Write, T: Serialize + ?Sized>(
    bytes: &mut W,
    value: &T,
) -> crate::Result<()> {
    let mut value =
        serde_json::to_value(value).map_err(|error| crate::Error::Invalid(error.to_string()))?;
    // Notice bodies may contain JSON objects. Canonical bytes cannot depend on
    // their insertion order or serde_json's feature-unified map implementation.
    value.sort_all_objects();
    value
        .serialize(&mut rmp_serde::Serializer::new(bytes).with_struct_map())
        .map_err(|error| crate::Error::Invalid(error.to_string()))
}

/// Admit through the SAME canonical encoder, retaining no second encoded buffer.
pub(super) fn fits_frame<T: Serialize + ?Sized>(record: &T) -> bool {
    struct FrameBudget(usize);
    impl std::io::Write for FrameBudget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or(std::io::ErrorKind::WriteZero)?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    write_payload(
        &mut FrameBudget(MAX_RECORD_BYTES - std::mem::size_of::<u32>()),
        record,
    )
    .is_ok()
}

/// Decode exactly one value, including all of its fields. The library's
/// from_slice helper accepts trailing values; a durable record must not.
pub(super) fn decode_payload<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Unopened> {
    decode_payload_with(
        bytes,
        |decoder| T::deserialize(decoder).map_err(|_| Unopened::Malformed),
        || Unopened::Malformed,
    )
}

type PayloadDecoder<'a> = rmp_serde::Deserializer<
    rmp_serde::decode::ReadRefReader<'a, [u8]>,
    rmp_serde::config::HumanReadableConfig<rmp_serde::config::DefaultConfig>,
>;

fn decode_payload_with<T, E>(
    bytes: &[u8],
    decode: impl FnOnce(&mut PayloadDecoder<'_>) -> Result<T, E>,
    malformed: impl FnOnce() -> E,
) -> Result<T, E> {
    // Both readers borrow strings instead of allocating codec scratch.
    let mut decoder = rmp_serde::Deserializer::from_read_ref(bytes).with_human_readable();
    let value = decode(&mut decoder)?;
    // Only EOF at a marker proves exact consumption. A truncated trailing
    // number is InvalidDataRead instead. This probe reads at most nine bytes.
    if !matches!(u8::deserialize(&mut decoder),
        Err(rmp_serde::decode::Error::InvalidMarkerRead(error))
        if error.kind() == std::io::ErrorKind::UnexpectedEof)
    {
        return Err(malformed());
    }
    Ok(value)
}

/// Select the format before decoding any payload, even a malformed one.
pub(crate) fn open<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Unopened> {
    decode_payload(versioned_payload(bytes)?)
}

fn versioned_payload(bytes: &[u8]) -> Result<&[u8], Unopened> {
    let (version, payload) = bytes.split_at_checked(4).ok_or(Unopened::Malformed)?;
    let found = u32::from_be_bytes(version.try_into().map_err(|_| Unopened::Malformed)?);
    if found != RECORD_VERSION {
        return Err(Unopened::Version(UnsupportedRecordVersion {
            found,
            expected: RECORD_VERSION,
        }));
    }
    Ok(payload)
}

/// The record of any agent stream as `T`, such as a JSON value for a
/// reader of every record kind: bounded by [`MAX_RECORD_BYTES`], and of any
/// version but [`RECORD_VERSION`] refused whole.
pub fn decode_record<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Error> {
    decode_payload(record_payload(bytes)?).map_err(record_decode_error)
}

fn record_payload(bytes: &[u8]) -> Result<&[u8], Error> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Record("record exceeds MAX_RECORD_BYTES"));
    }
    versioned_payload(bytes).map_err(record_decode_error)
}

fn record_decode_error(unopened: Unopened) -> Error {
    match unopened {
        Unopened::Version(version) => Error::UnsupportedRecordVersion(version),
        Unopened::Malformed => Error::Record("record is malformed"),
    }
}

/// Stable caller-chosen identity of one intent (a message, cancel, archive, hook or approval).
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, crate::HeapLayout,
)]
#[serde(try_from = "String", into = "String")]
#[heap_layout(as = String)]
pub struct IntentId(String);
crate::core::ownership::owned_tuple!(IntentId, (text));

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
        if (!value.is_empty()
            && value.len() <= MAX_INTENT_BYTES
            && value.bytes().all(|byte| byte.is_ascii_graphic()))
            || super::spawn_names::generated_intent(&value)
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

/// Conversation selected when an agent is created.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnContext {
    /// A root context that is never admitted as a child.
    TopLevel,
    /// Only the self-contained brief and explicitly sent files.
    Fresh,
    /// A stream fork at the parent's conversation cut, plus the brief.
    Fork,
}

/// Creation data admitted from one template snapshot. Later template changes
/// affect new children; this agent retains its context and immutable directive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreationConfig {
    /// Question plugin selection fixed when the root session was created.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub question_plugin: bool,
    /// Forced run policy pinned at creation and inherited by every child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology: Option<crate::topology::Topology>,
    /// Present only on an explicitly continued decider, never inherited by spawns.
    pub continuation: Option<Box<super::Continuation>>,
    /// Conversation selected at creation, independent of a template name.
    pub context: SpawnContext,
    /// The exact original template file, absent for the root default.
    pub template: Option<FileRef>,
    /// Peer inboxes this agent may `send` to beyond its parent and its open
    /// children, as `agent.send:<path>`, `agent.send-subtree:<path>` and
    /// `agent.send-sibling:<name>` (resolved under this agent's parent)
    /// labels its parent conferred at spawn over its own children.
    #[serde(default, skip_serializing_if = "crate::Capabilities::is_empty")]
    pub peers: crate::Capabilities,
    /// Durable provider/model selection. Absent only for externally owned loops
    /// or compositions that do not bind models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// A host's original template reader, shared by direct and model spawns.
pub trait SpawnTemplates: acyclic_stream::ProviderPlatform {
    /// The run-start policy pinned to roots by this template selector.
    fn topology(&self) -> Option<&crate::topology::Topology> {
        None
    }

    /// Reads one child template using the parent's original authority.
    fn child<'a>(
        &'a self,
        parent: &'a CreationConfig,
        path: &'a super::AgentPath,
        volumes: &'a [WorkspaceEntry],
        template: &'a str,
        ceiling: Option<&'a crate::Capabilities>,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<CreationConfig>>;
}

impl CreationConfig {
    /// Role identity from the immutable template selection; roots use orchestrator.
    #[must_use]
    pub fn role(&self) -> Option<&str> {
        match &self.template {
            None => Some("orchestrator"),
            Some(file) => file
                .path()
                .strip_prefix("templates/")?
                .strip_suffix("/AGENT.md"),
        }
    }
}

/// First record of every agent, written by the spawn fork or by root creation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkAdopted {
    /// The adopted agent.
    pub agent: AgentId,
    /// Spawning agent, `None` for a root.
    pub parent: Option<AgentPath>,
    /// Creation snapshot; `None` when an external host owns the agent's loop.
    pub profile: Option<CreationConfig>,
    /// Inbox sequence of this record; the agent's first consumed input.
    pub forked_at: u64,
    /// Tail of the inherited conversation at the fork cut.
    pub conversation_tail: u64,
    /// Sequence of the inherited conversation's latest context checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<u64>,
    /// Volumes the agent owns.
    pub volumes: Vec<WorkspaceEntry>,
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
    /// Message being answered. Threads are a projection of committed messages,
    /// with no separate thread store or authority conferred by this identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub re: Option<IntentId>,
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
    /// Failed with complete text retained on the original agent stream subtree.
    FailedDiagnostic {
        /// Immutable identity and length of the complete staged text.
        diagnostic: super::Diagnostic,
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
/// named by this record's inbox sequence, so no two requests collide. The host
/// admits the child's template against this agent's creation capabilities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnRequest {
    /// Who asked.
    pub by: Sender,
    /// Stable intent identity.
    pub id: IntentId,
    /// Optional first input, staged on the parent's private volume. A fork
    /// without a brief waits for an ordinary input after adoption.
    pub brief: Option<Box<FileRef>>,
    /// The child's admitted creation snapshot.
    pub profile: CreationConfig,
    /// Select a retained prior turn instead of the current conversation head.
    pub history: Option<super::HistorySelection>,
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
    /// Select a prior settled turn and/or restore its project generations.
    Restore {
        /// Sequence in this agent's history and conversation streams.
        at: u64,
        /// Explicit files, context or both semantics.
        mode: super::RestoreMode,
        /// Exact current project generations approved by the caller. A
        /// concurrent edit refuses the complete restoration.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown[]"))]
        expected: Vec<super::HistoryVolume>,
    },
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

/// Primitive event content. Waking payloads are staged like message bodies,
/// so the queued turn and its settled entry never retain the event's bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum NoticePayload {
    /// An event that does not schedule a turn.
    Quiet(crate::conversation::wire::Notice),
    /// The complete waking Notice, at its immutable staged version.
    Waking(Box<FileRef>),
}

impl NoticePayload {
    /// Whether delivery schedules a turn.
    pub const fn wakes(&self) -> bool {
        matches!(self, Self::Waking(_))
    }
}

/// The only input to an agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum InboxRecord {
    /// First record of every agent.
    ForkAdopted(Box<ForkAdopted>),
    /// An ordered workspace-table delta.
    WorkspaceChanged(Box<WorkspaceChanged>),
    /// `send`.
    Message(Message),
    /// A message reserved for a new turn, never injected into the active one.
    FollowUp(Message),
    /// One primitive event; the payload independently decides whether to run.
    Notice {
        /// Original producer event identity, retained across delivery replay.
        id: IntentId,
        /// Appended event, also readable without running a model.
        notice: NoticePayload,
    },
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

/// Derive the runtime's control discriminants from the canonical record writer.
/// Only the encoded payload is removed; sender and intent never enter a filter.
pub(super) fn interrupt_prefixes() -> Result<[Vec<u8>; 2], Error> {
    let control = Control {
        by: Sender::User {
            principal: "interrupt".into(),
        },
        id: "interrupt".try_into()?,
    };
    let payload =
        encode_payload(&control).map_err(|_| Error::Record("control payload is not encodable"))?;
    let prefix = |record: InboxRecord| -> Result<Vec<u8>, Error> {
        let mut bytes = record.encode()?;
        let length = bytes
            .strip_suffix(payload.as_slice())
            .ok_or(Error::Record("control payload is not a canonical suffix"))?
            .len();
        bytes.truncate(length);
        Ok(bytes)
    };
    Ok([
        prefix(InboxRecord::Cancel(control.clone()))?,
        prefix(InboxRecord::Archive(control))?,
    ])
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
    /// A settled turn authored an input fence in its private UI files.
    WaitingUser {
        /// Canonical path relative to the agent's `ui/` root.
        path: String,
    },
    /// A turn is executing or accepted input awaits execution.
    Running,
    /// Waiting for an approval response.
    WaitingApproval {
        /// Request identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Exact original action, or absent when an external host publishes no
        /// approval request. Display bodies stay in the immutable request file.
        binding: Option<crate::interaction::ApprovalBinding>,
    },
    /// Waiting for an event hook from its host.
    WaitingHost {
        /// Host name.
        host: String,
    },
    /// A turn waits, holding no delivery, for its condition (see
    /// [`super::Until`]): each awaited agent's [`ChildSettled`] or
    /// [`ChildDone`] counts toward it, and its deadline, when set, resumes
    /// the turn regardless.
    Waiting {
        /// Unix milliseconds.
        deadline_unix_ms: Option<u64>,
        /// Resume the retained turn on inbox input as well as an alarm.
        input: bool,
    },
    /// Terminal.
    Done {
        /// Outcome.
        outcome: AgentOutcome,
    },
}

impl Phase {
    /// Lossless LATEST attention metadata shared by publishers and UI queries.
    #[must_use]
    pub const fn attention(&self) -> bool {
        matches!(
            self,
            Self::Waiting { input: true, .. }
                | Self::WaitingApproval { .. }
                | Self::WaitingHost { .. }
                | Self::WaitingUser { .. }
                | Self::Done { .. }
        )
    }
    /// Whether a turn is open: running, or waiting on an approval, its host,
    /// its children or a deadline. Idle and done agents hold none.
    #[must_use]
    pub const fn in_turn(&self) -> bool {
        match self {
            Self::Idle | Self::WaitingUser { .. } | Self::Done { .. } => false,
            Self::Running
            | Self::WaitingApproval { .. }
            | Self::WaitingHost { .. }
            | Self::Waiting { .. } => true,
        }
    }
}

/// One child's original settlement commit and the return of its parent's wait.
/// Both timestamps use Unix microseconds. Clock skew remains visible rather
/// than being clamped into a fabricated zero-duration measurement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct ChildWake {
    /// Sequence of the original completion fact in the owner status stream.
    pub status: u64,
    /// Direct child id, relative to the owner of this status stream.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub child: AgentId,
    /// Commit timestamp of the ChildSettled or ChildDone inbox record.
    pub settled_at_micros: u64,
    /// Parent delivery clock when the waiting turn actually resumes.
    pub woke_at_micros: u64,
}

/// One admitted completion fact. It is not a wait-return measurement until
/// an original ChildWakes range supplies the parent's return clock.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct ChildSettlement {
    /// Direct child of the status owner.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub child: AgentId,
    /// Original inbox commit clock, retained in the same ordinary status commit.
    pub settled_at_micros: u64,
}

/// Original completion facts for one returned wait. These status sequences
/// form a dense range: only admitted completions publish while a wait is parked.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct ChildWakes {
    /// First completion fact in the owner's status stream.
    pub from: u64,
    /// Exclusive end, including a completion in the returning status itself.
    pub until: u64,
    /// Parent delivery clock at the actual wait return, before the driver runs.
    pub woke_at_micros: u64,
}
impl ChildWakes {
    /// Read only the requested original status facts at this immutable cut.
    /// No index IO, reconstruction of a subtree or per-child retained state.
    pub async fn page<P: acyclic_stream::StreamProvider + ?Sized>(
        &self,
        provider: std::sync::Arc<P>,
        agent: &AgentPath,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<ChildWake>, Error> {
        use futures::TryStreamExt as _;
        let from = after.unwrap_or(self.from);
        if self.from > from || from > self.until || self.from >= self.until {
            return Err(Error::Record("completion cursor is outside its status cut"));
        }
        if from == self.until || limit == 0 {
            return Ok(Vec::new());
        }
        let mut source = provider
            .read(acyclic_stream::ReadRequest {
                path: agent.status()?,
                from,
                limit: u32::try_from(limit)
                    .map_err(|_| Error::Record("completion page exceeds transport credit"))?,
                key: None,
                at_least: Default::default(),
            })
            .await
            .map_err(Error::Stream)?;
        let mut samples = Vec::new();
        let mut next = from;
        while let Some(record) = source.try_next().await.map_err(Error::Stream)? {
            if record.sequence >= self.until {
                break;
            }
            if record.sequence != next {
                return Err(Error::Record("completion status range has a gap"));
            }
            let fact = StatusRecord::decode(&record.value)?
                .child_settlement
                .ok_or(Error::Record(
                    "completion status range contains an unrelated phase",
                ))?;
            samples.push(ChildWake {
                status: record.sequence,
                child: fact.child,
                settled_at_micros: fact.settled_at_micros,
                woke_at_micros: self.woke_at_micros,
            });
            next += 1;
        }
        if samples.len() as u64 != (limit as u64).min(self.until - from) {
            return Err(Error::Record("completion status range is missing"));
        }
        Ok(samples)
    }
}

/// One status stream record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct StatusRecord {
    /// One admitted completion fact, durable before a waiting parent resumes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_settlement: Option<ChildSettlement>,
    /// Paged measurements, published only when the wait returns.
    /// Travels through the existing status/SessionFrame telemetry projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_wakes: Option<ChildWakes>,
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
    /// Encoded status plus Streams' opaque LATEST metadata. Attention transitions
    /// survive conflation; every publisher uses this same record constructor.
    pub fn append_record(&self) -> Result<acyclic_stream::AppendRecord, Error> {
        acyclic_stream::AppendRecord::latest(self.encode()?, "status", self.phase.attention())
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

    /// Canonical named MessagePack under the shared [`RECORD_VERSION`]
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

    /// Reads this record on the original filesystem accounting context.
    /// Version, whole-input and structural checks are identical to `decode`;
    /// decoded fields retain their original allocation credit.
    fn decode_accounted(
        bytes: &[u8],
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<acyclic_fs::kernel::Retained<Self>, acyclic_fs::kernel::MetadataDecodeError<Error>>
    {
        use acyclic_fs::kernel::MetadataDecodeError;
        accounting.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        let payload = record_payload(bytes).map_err(MetadataDecodeError::Codec)?;
        let value = decode_payload_with(
            payload,
            |decoder| {
                accounting
                    .deserialize::<Self, _>(decoder)
                    .map_err(|error| match error {
                        MetadataDecodeError::Work(error) => MetadataDecodeError::Work(error),
                        MetadataDecodeError::Codec(_) => {
                            MetadataDecodeError::Codec(Error::Record("record is malformed"))
                        }
                    })
            },
            || MetadataDecodeError::Codec(Error::Record("record is malformed")),
        )?;
        value.check().map_err(MetadataDecodeError::Codec)?;
        Ok(value)
    }
}

fn check_outcome(outcome: &AgentOutcome) -> Result<(), Error> {
    match outcome {
        AgentOutcome::Failed { message } if message.len() > MAX_MESSAGE_BYTES => {
            Err(Error::Record("failure message exceeds MAX_MESSAGE_BYTES"))
        }
        AgentOutcome::FailedDiagnostic { diagnostic } => {
            if diagnostic.bytes == 0 {
                return Err(Error::Record("diagnostic is empty"));
            }
            diagnostic.path().map(|_| ())
        }
        _ => Ok(()),
    }
}

/// Workspace metadata is admitted without a workspace-count ceiling.
pub(super) fn check_volumes<'a>(
    volumes: impl Iterator<Item = &'a WorkspaceEntry>,
) -> Result<(), Error> {
    super::workspaces::check_entries(volumes)
}

/// A path within the private UI root, using the shared content-path contract.
pub(super) fn check_ui_path(path: &str) -> Result<(), Error> {
    crate::conversation::validate_content_path(path, &mut None)
        .map_err(|_| Error::Record("invalid private ui/ path"))
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
                check_model(adopted.profile.as_ref())?;
                check_volumes(adopted.volumes.iter())?;
                Ok(())
            }
            Self::WorkspaceChanged(change) => change.check(),
            Self::ChildDone(done) => {
                check_outcome(&done.outcome)?;
                if matches!(&done.outcome, AgentOutcome::FailedDiagnostic { diagnostic } if diagnostic.agent != done.child)
                {
                    return Err(Error::Record("diagnostic belongs to another child"));
                }
                Ok(())
            }
            Self::Notice { notice, .. } => {
                match notice {
                    NoticePayload::Quiet(notice) => {
                        notice
                            .validate()
                            .map_err(|_| Error::Record("invalid primitive event notice"))?;
                        if notice.wake {
                            return Err(Error::Record("waking notice must be staged"));
                        }
                    }
                    NoticePayload::Waking(file) => {
                        if file.descriptor().media_type() != super::NOTICE_MEDIA_TYPE {
                            return Err(Error::Record("waking notice has the wrong media type"));
                        }
                    }
                }
                Ok(())
            }
            Self::Message(Message { from, .. })
            | Self::FollowUp(Message { from, .. })
            | Self::Cancel(Control { by: from, .. })
            | Self::Archive(Control { by: from, .. }) => check_sender(from),
            Self::Context(ContextRequest { by, action, .. }) => {
                check_sender(by)?;
                if let ContextAction::Restore { mode, expected, .. } = action {
                    check_volumes(expected.iter().map(|volume| &volume.binding))?;
                    if (!mode.files() && !expected.is_empty())
                        || expected.iter().enumerate().any(|(index, volume)| {
                            volume.binding.as_volume().is_none_or(|source| {
                                source.class() != crate::conversation::VolumeClass::Project
                            }) || expected[..index]
                                .iter()
                                .any(|prior| prior.binding.name == volume.binding.name)
                        })
                    {
                        return Err(Error::Record("restore project bindings are invalid"));
                    }
                }
                Ok(())
            }
            Self::Hook(hook) => check_text(&hook.host),
            Self::Spawn(spawn) => {
                check_model(Some(&spawn.profile))?;
                check_sender(&spawn.by)?;
                // Refused before the parent's inbox admits it: once queued, a
                // request its spawn cannot apply would block the parent.
                if spawn.profile.context == SpawnContext::Fresh && spawn.history.is_some() {
                    return Err(Error::Record("a fresh-context template selects no history"));
                }
                Ok(())
            }
            Self::Approval(_)
            | Self::ApprovalRequested(_)
            | Self::Awaited
            | Self::ChildSettled(_) => Ok(()),
        }
    }
}

fn check_model(profile: Option<&CreationConfig>) -> Result<(), Error> {
    if let Some(topology) = profile.and_then(|profile| profile.topology.as_ref()) {
        topology
            .validate()
            .map_err(|_| Error::Record("invalid forced topology"))?;
    }
    if let Some(model) = profile.and_then(|profile| profile.model.as_deref()) {
        crate::model::selection::validate(model)
            .map_err(|_| Error::Record("invalid model selection"))?;
    }
    Ok(())
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
            Phase::WaitingUser { path } => check_ui_path(path),
            Phase::WaitingApproval {
                binding: Some(binding),
                ..
            } => binding
                .validate()
                .map_err(|_| Error::Record("approval action binding is invalid")),
            Phase::Idle
            | Phase::Running
            | Phase::WaitingApproval { binding: None, .. }
            | Phase::Waiting { .. } => Ok(()),
        }
    }
}

impl AgentRecord for Closed {
    fn check(&self) -> Result<(), Error> {
        check_sender(&self.by)
    }
}
