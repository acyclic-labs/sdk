//! Backend-neutral agent controls. Host dispatch supplies authenticated identity;
//! no request can assert its sender, approval responder or account membership.

use acyclic_harness_core::{
    agent::{AgentPath, IntentId},
    conversation::{FileRef, validate_content_path},
    interaction::ApprovalBinding,
    model::ModelContent,
    resources::GenerationRef,
};
use serde::{Deserialize, Serialize};

fn peer_enabled() -> bool {
    true
}

/// Most bytes a binding's routing adds to a command frame in its invocation.
const MAX_ROUTING_BYTES: usize = 64 * 1024;
/// One command frame: the encoded bytes one invocation carries beside its
/// routing, within the session's one-frame bound. A larger command is not
/// refused; [`PreparedCommand::new`] stages it in parts of one frame each, so
/// this bounds only what the host decodes at once.
pub const MAX_COMMAND_BYTES: usize = acyclic_stream::MAX_COMMAND_BYTES - MAX_ROUTING_BYTES;
/// Most bytes one [`AgentCommand::Read`] returns; its encoded reply stays
/// within the reply budget.
pub const MAX_READ_BYTES: u64 = 32 * 1024;
/// Longest tool-output path a read names.
const MAX_READ_PATH_BYTES: usize = 1024;

/// Requested page custody. Both ceilings are enforced by the host before it
/// returns a page; the cursor binds the exact query, authorization and cut.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct AgentPageRequest {
    items: u32,
    bytes: u32,
    cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageWire {
    #[serde(default = "default_items")]
    items: u32,
    #[serde(default = "default_bytes")]
    bytes: u32,
    #[serde(default)]
    cursor: Option<String>,
}

const fn default_items() -> u32 {
    32
}

const fn default_bytes() -> u32 {
    32 * 1024
}

impl Default for AgentPageRequest {
    fn default() -> Self {
        Self {
            items: default_items(),
            bytes: default_bytes(),
            cursor: None,
        }
    }
}

impl AgentPageRequest {
    /// Validate finite page ceilings and retain the host's opaque cursor intact.
    pub fn new(items: u32, bytes: u32, cursor: Option<String>) -> Result<Self, CommandError> {
        if items == 0
            || items > 128
            || bytes == 0
            || u64::from(bytes) > MAX_COMMAND_BYTES as u64
            || cursor.as_ref().is_some_and(|cursor| cursor.is_empty())
        {
            return Err(CommandError::Capacity);
        }
        Ok(Self {
            items,
            bytes,
            cursor,
        })
    }

    /// Maximum returned entries, including status/approval projections.
    #[must_use]
    pub const fn items(&self) -> u32 {
        self.items
    }

    /// Maximum encoded page bytes, including framing and the next cursor.
    #[must_use]
    pub const fn bytes(&self) -> u32 {
        self.bytes
    }

    /// Host-owned exclusive continuation; the client never edits it.
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

impl TryFrom<PageWire> for AgentPageRequest {
    type Error = CommandError;

    fn try_from(page: PageWire) -> Result<Self, Self::Error> {
        Self::new(page.items, page.bytes, page.cursor)
    }
}

impl<'de> Deserialize<'de> for AgentPageRequest {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        Self::try_from(PageWire::deserialize(decoder)?).map_err(serde::de::Error::custom)
    }
}

/// The same controls for local and hosted sessions. The host authorizes each
/// operation before provider I/O and stages content through its existing owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentCommand {
    // Empty struct variants enforce deny_unknown_fields for fieldless commands too.
    /// Discover the binding of the tree this endpoint serves the session's
    /// credential, retaining the host's selected binding when one is known.
    Bind {
        /// Optional descendant selected within the authenticated tree.
        #[serde(default)]
        #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
        agent: Option<AgentPath>,
    },
    /// Discover the root admitted by this session's credential.
    Root {},
    /// Read the last committed root-directory membership in constant work.
    LatestSession {},
    /// Mark an explicit attach as latest for this checkout.
    SelectSession {
        /// Independent session in the admitted directory.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable identity of this selection.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Bind a stable alias in the admitted session directory.
    NameSession {
        /// Existing independent session.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Exact alias; changing a title leaves aliases usable.
        name: String,
        /// Stable identity of the binding request.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Resolve an exact alias without enumerating sessions.
    ResolveSession {
        /// Alias in this checkout's name index.
        name: String,
    },
    /// Page the explicitly admitted root-session directory.
    Sessions {
        /// Bounded directory credit and its opaque continuation.
        #[serde(default)]
        page: AgentPageRequest,
    },
    /// Create an independent root under the host's session admission.
    NewSession {
        /// Question plugin selection fixed for this new session.
        #[serde(default)]
        #[cfg_attr(feature = "wasm", tsify(optional))]
        questions: Option<bool>,
        /// Stable creation identity; reuse exactly after an uncertain result.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Launch-time peer participation; on unless the user opts out.
        #[serde(default = "peer_enabled")]
        peer: bool,
    },
    /// Change peer participation through the authenticated user connection.
    /// This operation is never a model tool or a file-backed request.
    SetPeerMembership {
        /// The selected independent session.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Caller-owned intent retained after an uncertain result.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// True to rejoin; false to opt out symmetrically.
        enabled: bool,
    },
    /// Fork a parentless session at a settled turn, retaining the source.
    ForkSession {
        /// Source session within the admitted directory.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable branch identity; retain the complete request on uncertainty.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Zero-based inclusive settled conversation offset.
        at: u64,
        /// The new independent root's user-selected peer participation.
        #[serde(default = "peer_enabled")]
        peer: bool,
    },
    /// Durably pause or explicitly resume the selected actor through its
    /// existing runtime, parking the same owned execution resources.
    Pause {
        /// Selected actor within the authenticated session tree or directory.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// True to pause, false to resume.
        paused: bool,
    },
    /// Integrate this fork through the platform's ordinary filesystem merge.
    /// A child targets its parent; an independent session targets the checkout.
    /// Conflicts remain files in the destination checkout.
    MergeSession {
        /// Session or child selected for integration.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable merge request identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Snapshot explicit paths in a named client session; wait on its original
    /// filesystem publication producer when `after` is still current.
    ClientFiles {
        /// Agent selected within the authenticated tree.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable UI-session identity within that agent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        session: acyclic_harness_core::OperationId,
        /// Distinct relative file paths; no directory scan is performed.
        paths: Vec<String>,
        /// Wait for a generation different from this committed snapshot.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown | null"))]
        after: Option<acyclic_harness_core::resources::GenerationRef>,
    },
    /// Atomic conditional publication to that same session volume.
    PublishClientFiles {
        /// Agent selected within the authenticated tree.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable UI-session identity within that agent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        session: acyclic_harness_core::OperationId,
        /// Caller-retained identity of this conditional publication.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        operation: acyclic_harness_core::OperationId,
        /// Distinct paths with their original source preconditions.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown[]"))]
        changes: Vec<acyclic_harness_core::client_files::ClientFileChange>,
    },
    /// Snapshot exactly the requested files in the agent's private volume.
    AgentFiles {
        /// Selected agent; no request supplies a volume grant.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Explicit paths, such as ui/title.
        paths: Vec<String>,
        /// Wait on the original filesystem producer after this generation.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown | null"))]
        after: Option<acyclic_harness_core::resources::GenerationRef>,
    },
    /// Read bounded text prefixes at one live agent-file snapshot.
    AgentFilePreviews {
        /// Selected agent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Explicit relative paths.
        paths: Vec<String>,
        /// Next live query waits after this cut.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown | null"))]
        after: Option<acyclic_harness_core::resources::GenerationRef>,
        /// Per-path display credit; aggregate bytes fit one read chunk.
        bytes: u64,
    },
    /// Conditionally publish authored files in the agent's private volume.
    PublishAgentFiles {
        /// Selected agent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Caller-owned publication identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        operation: acyclic_harness_core::OperationId,
        /// Exact source preconditions and replacement contents.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown[]"))]
        changes: Vec<acyclic_harness_core::client_files::ClientFileChange>,
    },
    /// Freeze an agent through its ordinary closing input and resource release.
    Archive {
        /// Exact recipient.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable archive identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Stage canonical user input and send one message with caller-owned intent.
    Prompt {
        /// Exact recipient; selection changes cannot retarget a prepared command.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Reuse with the same body after an uncertain delivery.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// The canonical harness content type, including immutable file inputs.
        body: ModelContent,
    },
    /// Stop the current input without reporting it as successfully settled.
    Cancel {
        /// Exact recipient.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable cancellation identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Compact the agent's model context: one model request summarizes it,
    /// and later turns start from that checkpoint.
    Compact {
        /// Exact recipient.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Reuse with the same instructions after an uncertain delivery.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Guidance for the summary, staged like a prompt body.
        instructions: Option<ModelContent>,
    },
    /// Restore the agent's latest checkpoint, dropping every turn after it
    /// from its model context.
    Rewind {
        /// Exact recipient.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable rewind identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Restore files, context or both at a selected committed turn. File
    /// restoration compares every current project head to `expected`.
    Restore {
        /// Exact recipient.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable identity; reuse exactly after an uncertain delivery.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Selected conversation sequence, inclusive.
        at: u64,
        /// Explicit scope of the restoration.
        mode: acyclic_harness_core::agent::RestoreMode,
        /// Project heads observed and approved by the caller.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown[]"))]
        expected: Vec<acyclic_harness_core::agent::HistoryVolume>,
    },
    /// Retire the agent's conversation: later turns start with no history.
    ClearAll {
        /// Exact recipient.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Stable clear identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
    },
    /// Spawn through the host's existing fork/volume publication mechanism.
    Spawn {
        /// Exact parent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        parent: AgentPath,
        /// Stable spawning identity; the host derives the child identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Brief staged by the host, not a client-created volume grant.
        brief: ModelContent,
        /// The child's role; the parent's role must admit it.
        role: String,
    },
    /// Fork the conversation and project generations saved at a prior turn.
    Fork {
        /// Continue the decider's own role, with an optional intervention.
        /// No brief is run; the caller drives this independent run normally.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "wasm", tsify(type = "unknown | null"))]
        continuation: Option<acyclic_harness_core::agent::ContinuationRequest>,
        /// Exact parent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        parent: AgentPath,
        /// Stable spawning identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Zero-based settled conversation turn.
        at: u64,
        /// New task brief, staged by the host.
        brief: ModelContent,
        /// The child's role; the parent's role must admit it, and it must
        /// inherit context, since a fresh-context role selects no history.
        role: String,
    },
    /// Discover only one bounded page of direct children.
    Children {
        /// Expanded tree node.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Whole-page limits and the unchanged host continuation.
        #[serde(default)]
        page: AgentPageRequest,
    },
    /// Read the authorized current project roots for a conditional restore.
    ProjectHeads {
        /// Selected agent, whose committed bindings supply the project volumes.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
    },
    /// Read committed generation-linked history, newest first. An initial
    /// request seeks directly to the tail; continuations visit older entries
    /// under the same immutable opening cut.
    History {
        /// Selected agent; cursor selection cannot retarget it.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Bounded page and continuation at the original exclusive cut.
        #[serde(default)]
        page: AgentPageRequest,
    },
    /// Read pending approvals through the host's authorized interaction reader.
    Approvals {
        /// Agent whose approvals are being displayed.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Whole-page limits and the unchanged host continuation.
        #[serde(default)]
        page: AgentPageRequest,
    },
    /// Decide the exact displayed action, then use canonical approval delivery.
    Decide {
        /// Agent that owns the request.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Immutable approval identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Prevents a decision from authorizing a different action.
        binding: ApprovalBinding,
        /// Human decision; the responder is derived from verified authority.
        verdict: acyclic_harness_core::interaction::ApprovalVerdict,
        /// Optional explanation.
        reason: Option<String>,
    },
    /// The committed reply to message `id`, accepted at inbox sequence
    /// `inbox` (see `acyclic_harness_core::agent::reply`).
    Reply {
        /// Agent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Message identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: IntentId,
        /// Inbox sequence `Delivered::Committed` returned for the message.
        inbox: u64,
    },
    /// How far `agent` has settled: its settled turns, published statuses
    /// and current phase.
    Settlement {
        /// Agent whose conversation is read.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Capture the current inbox tail and wait for its original actor
        /// settlement fence. Later inputs do not extend this wait.
        #[serde(default)]
        wait: bool,
    },
    /// One visible page of the agent's authorized workspace diff query.
    /// The host pins the initial cut; continuations retain that exact projection.
    Diff {
        /// Agent whose selected workspace the query observes.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Visible rows and complete encoded reply bytes, including continuation.
        page: AgentPageRequest,
    },
    /// Bounded completion measurements at an original status cut.
    ChildWakes {
        /// Selected owner, admitted within this authenticated tree.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Sequence in the owner's original status stream.
        status: u64,
        /// Row/byte credit; cursor is the next original status sequence.
        page: AgentPageRequest,
    },
    /// One authorized private directory page at its original immutable cut.
    Directory {
        /// Exact selected agent; paths never select a different private volume.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Normalized private-volume directory, or empty for its visible root.
        path: String,
        /// None selects the initial immutable generation.
        #[cfg_attr(
            feature = "wasm",
            tsify(type = "WasmResourceRefWire & { kind: 'generation' } | undefined")
        )]
        generation: Option<GenerationRef>,
        /// Shared row/byte credit; its cursor is the preceding page's last name.
        page: AgentPageRequest,
    },
    /// A bounded projection of an original input's committed call admissions and
    /// completions, read through its agent's original private grant.
    Usage {
        /// Agent whose original private journal is selected.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Original inbox sequence, initially zero (skips inherited inputs).
        input: u64,
        /// Original exclusive journal continuation, initially zero.
        after: u64,
        /// Wait for original source progress or committed input settlement
        /// when the selected page is quiet. False takes a bounded snapshot.
        wait: bool,
        /// Last observed direct open-child count. A waiting query also returns
        /// when the original actor checkpoint changes this value.
        observed_children: Option<u64>,
    },
    /// Original execution observations through the same authorized input query
    /// as Usage. Payloads retain their immutable references for bounded Read.
    Events {
        /// Agent whose original private journal is selected.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Original inbox sequence, initially zero (skips inherited inputs).
        input: u64,
        /// Exclusive original journal continuation, initially zero.
        after: u64,
        /// Park a quiet page on the original journal and actor producers.
        wait: bool,
    },
    /// Read one bounded range of a file the agent's turns produced, through
    /// the host's authorized reader of that agent's volumes.
    Read {
        /// Agent whose volumes hold the file.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// What to read.
        target: ReadTarget,
        /// First byte of the range; the host returns at most [`MAX_READ_BYTES`].
        offset: u64,
    },
    /// One part of a command whose encoding exceeds one frame, staged
    /// before it by [`PreparedCommand::new`].
    Stage {
        /// The staged command's agent, whose private volume holds the part.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// BLAKE3 of the whole command's encoding, lowercase hex.
        digest: String,
        /// Zero-based position of this part.
        part: u32,
        /// This part's slice of the encoding.
        text: String,
    },
    /// The command whose encoding is the `parts` staged under `digest`, in
    /// order, `bytes` long. The host authorizes this head, which names
    /// everything authorization needs, before it reads any part, then runs
    /// the command exactly as if sent in one frame.
    Staged {
        /// The staged command's agent.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// The staged command's kind.
        command: String,
        /// BLAKE3 of the whole encoding, lowercase hex.
        digest: String,
        /// Number of staged parts.
        parts: u32,
        /// Length of the whole encoding.
        bytes: u64,
    },
}

/// A file an agent's turns produced, as a frontend names it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadTarget {
    /// A private path selected at one immutable directory generation.
    Private {
        /// Normalized path under the selected agent's original private grant.
        path: String,
        /// Exact generation returned by directory discovery.
        #[cfg_attr(
            feature = "wasm",
            tsify(type = "WasmResourceRefWire & { kind: 'generation' }")
        )]
        generation: GenerationRef,
    },
    /// Immutable request owned by an exact original waiting-status record.
    Approval {
        /// Zero-based sequence in this agent's original status stream.
        status: u64,
    },
    /// A payload owned by an original execution observation. The server derives
    /// its turn and immutable reference from this exact agent/input/sequence.
    Execution {
        /// Original inbox sequence, without adoption clamping.
        input: u64,
        /// One-based original journal observation sequence.
        sequence: u64,
        /// A finite field of that observation; absent fields are refused.
        field: acyclic_harness_core::executor::ExecutionPayloadField,
    },
    /// An exact file: a settled turn's staged input or output.
    File {
        /// The immutable reference its record carries.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        file: FileRef,
    },
    /// An exact file named by one committed conversation record. Its original
    /// sequence proves inherited-file access without granting a source volume.
    Conversation {
        /// Zero-based sequence in the selected agent's conversation prefix.
        sequence: u64,
        /// Exact original reference; another file or version is refused.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        file: FileRef,
    },
    /// One chunk of the agent's tool output, as its change records name it.
    ToolOutput {
        /// Path below the agent's tool-output directory.
        path: String,
        /// Read from this settled conversation output's immutable generation.
        /// Required for a committed tool-result path. The file must belong to
        /// this agent's private volume; no current-head fallback is allowed.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef", optional))]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<FileRef>,
    },
}

impl AgentCommand {
    fn validate(&self) -> Result<(), CommandError> {
        match self {
            Self::SetPeerMembership { agent, .. } if agent.parent().is_some() => {
                Err(CommandError::Invalid)
            }
            Self::NameSession { name, .. } | Self::ResolveSession { name } if name.is_empty() => {
                Err(CommandError::Invalid)
            }
            Self::ClientFiles { paths, .. }
            | Self::AgentFiles { paths, .. }
            | Self::AgentFilePreviews { paths, .. } => {
                if let Self::AgentFilePreviews { bytes, .. } = self
                    && (*bytes == 0
                        || bytes
                            .checked_mul(paths.len() as u64)
                            .is_none_or(|total| total > MAX_READ_BYTES))
                {
                    return Err(CommandError::Invalid);
                }
                if paths.is_empty() {
                    return Err(CommandError::Invalid);
                }
                let mut unique = std::collections::BTreeSet::new();
                for path in paths {
                    if !unique.insert(path) {
                        return Err(CommandError::Invalid);
                    }
                    acyclic_harness_core::conversation::validate_content_path(path, &mut None)
                        .map_err(|_| CommandError::Invalid)?;
                }
                Ok(())
            }
            Self::PublishClientFiles { changes, .. } | Self::PublishAgentFiles { changes, .. } => {
                if changes.is_empty() {
                    return Err(CommandError::Invalid);
                }
                let mut unique = std::collections::BTreeSet::new();
                for change in changes {
                    if !unique.insert(&change.path) {
                        return Err(CommandError::Invalid);
                    }
                    acyclic_harness_core::conversation::validate_content_path(
                        &change.path,
                        &mut None,
                    )
                    .map_err(|_| CommandError::Invalid)?;
                    if change
                        .expected
                        .as_ref()
                        .is_some_and(|file| file.path() != change.path)
                    {
                        return Err(CommandError::Invalid);
                    }
                }
                Ok(())
            }
            Self::Spawn { role, .. } | Self::Fork { role, .. } if role.is_empty() => {
                Err(CommandError::Invalid)
            }
            Self::Read {
                target: ReadTarget::Execution { sequence: 0, .. },
                ..
            } => Err(CommandError::Invalid),
            Self::Prompt { body, .. }
            | Self::Spawn { brief: body, .. }
            | Self::Fork { brief: body, .. }
            | Self::Compact {
                instructions: Some(body),
                ..
            } => body
                .validate_user_input(&mut None)
                .map_err(|_| CommandError::Invalid),
            Self::Decide { binding, .. } => binding.validate().map_err(|_| CommandError::Invalid),
            Self::Read {
                target: ReadTarget::Private { path, generation },
                ..
            } => {
                validate_content_path(path, &mut None).map_err(|_| CommandError::Invalid)?;
                generation
                    .validate(&mut None)
                    .map_err(|_| CommandError::Invalid)
            }
            Self::ChildWakes { page, .. } => {
                if let Some(after) = page.cursor() {
                    if !after.parse::<u64>().is_ok_and(|n| n.to_string() == after) {
                        return Err(CommandError::Invalid);
                    }
                }
                Ok(())
            }
            Self::Directory {
                path,
                generation,
                page,
                ..
            } => {
                if generation.is_none() && page.cursor().is_some() {
                    return Err(CommandError::Invalid);
                }
                if !path.is_empty() {
                    validate_content_path(path, &mut None).map_err(|_| CommandError::Invalid)?;
                }
                if let Some(generation) = generation {
                    generation
                        .validate(&mut None)
                        .map_err(|_| CommandError::Invalid)?;
                }
                if let Some(after) = page.cursor() {
                    validate_content_path(after, &mut None).map_err(|_| CommandError::Invalid)?;
                    if after.contains('/') {
                        return Err(CommandError::Invalid);
                    }
                }
                Ok(())
            }
            Self::Stage { digest, text, .. } if !is_digest(digest) || text.is_empty() => {
                Err(CommandError::Invalid)
            }
            Self::Staged {
                digest,
                parts,
                bytes,
                ..
            } if !is_digest(digest) || *parts == 0 || *bytes == 0 => Err(CommandError::Invalid),
            Self::Read {
                target: ReadTarget::ToolOutput { path, .. },
                ..
            } if path.is_empty()
                || path.len() > MAX_READ_PATH_BYTES
                || path
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..") =>
            {
                Err(CommandError::Invalid)
            }
            _ => Ok(()),
        }
    }
}

impl AgentCommand {
    /// The agent this command acts on; discovery names none.
    #[must_use]
    pub const fn agent(&self) -> Option<&AgentPath> {
        match self {
            Self::Root {}
            | Self::ResolveSession { .. }
            | Self::LatestSession {}
            | Self::Sessions { .. }
            | Self::NewSession { .. } => None,
            Self::Bind { agent } => agent.as_ref(),
            Self::Spawn { parent: agent, .. }
            | Self::Fork { parent: agent, .. }
            | Self::ForkSession { agent, .. }
            | Self::Pause { agent, .. }
            | Self::SetPeerMembership { agent, .. }
            | Self::MergeSession { agent, .. }
            | Self::Prompt { agent, .. }
            | Self::Cancel { agent, .. }
            | Self::Compact { agent, .. }
            | Self::Rewind { agent, .. }
            | Self::Restore { agent, .. }
            | Self::ClearAll { agent, .. }
            | Self::Children { agent, .. }
            | Self::History { agent, .. }
            | Self::ProjectHeads { agent }
            | Self::Diff { agent, .. }
            | Self::Directory { agent, .. }
            | Self::ChildWakes { agent, .. }
            | Self::Approvals { agent, .. }
            | Self::Decide { agent, .. }
            | Self::Reply { agent, .. }
            | Self::Settlement { agent, .. }
            | Self::Usage { agent, .. }
            | Self::Events { agent, .. }
            | Self::AgentFilePreviews { agent, .. }
            | Self::NameSession { agent, .. }
            | Self::SelectSession { agent, .. }
            | Self::AgentFiles { agent, .. }
            | Self::PublishAgentFiles { agent, .. }
            | Self::Archive { agent, .. }
            | Self::ClientFiles { agent, .. }
            | Self::PublishClientFiles { agent, .. }
            | Self::Read { agent, .. }
            | Self::Stage { agent, .. }
            | Self::Staged { agent, .. } => Some(agent),
        }
    }

    /// This command's kind, as its encoding tags it.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Bind { .. } => "bind",
            Self::Root {} => "root",
            Self::LatestSession {} => "latest_session",
            Self::Sessions { .. } => "sessions",
            Self::NewSession { .. } => "new_session",
            Self::SetPeerMembership { .. } => "set_peer_membership",
            Self::ForkSession { .. } => "fork_session",
            Self::Pause { .. } => "pause",
            Self::MergeSession { .. } => "merge_session",
            Self::AgentFilePreviews { .. } => "agent_file_previews",
            Self::NameSession { .. } => "name_session",
            Self::ResolveSession { .. } => "resolve_session",
            Self::SelectSession { .. } => "select_session",
            Self::AgentFiles { .. } => "agent_files",
            Self::PublishAgentFiles { .. } => "publish_agent_files",
            Self::Archive { .. } => "archive",
            Self::ClientFiles { .. } => "client_files",
            Self::PublishClientFiles { .. } => "publish_client_files",
            Self::Prompt { .. } => "prompt",
            Self::Cancel { .. } => "cancel",
            Self::Compact { .. } => "compact",
            Self::Rewind { .. } => "rewind",
            Self::Restore { .. } => "restore",
            Self::ClearAll { .. } => "clear_all",
            Self::Spawn { .. } => "spawn",
            Self::Fork { .. } => "fork",
            Self::Children { .. } => "children",
            Self::History { .. } => "history",
            Self::ProjectHeads { .. } => "project_heads",
            Self::Diff { .. } => "diff",
            Self::Directory { .. } => "directory",
            Self::ChildWakes { .. } => "child_wakes",
            Self::Approvals { .. } => "approvals",
            Self::Decide { .. } => "decide",
            Self::Reply { .. } => "reply",
            Self::Settlement { .. } => "settlement",
            Self::Usage { .. } => "usage",
            Self::Events { .. } => "events",
            Self::Read { .. } => "read",
            Self::Stage { .. } => "stage",
            Self::Staged { .. } => "staged",
        }
    }
}

/// A BLAKE3 digest in lowercase hex.
fn is_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Bytes `ch` takes inside a JSON string, as `serde_json` escapes it.
const fn escaped_len(ch: char) -> usize {
    match ch {
        '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
        '\0'..='\u{1f}' => 6,
        _ => ch.len_utf8(),
    }
}

/// Immutable admitted bytes retained by the caller while a command is in flight.
/// Transport loss cannot turn an old intent into a newly reconstructed command.
#[derive(Clone)]
pub struct PreparedCommand {
    /// The command's whole encoding.
    command: Vec<u8>,
    /// Past one frame, the frames that carry it: its parts, then its head.
    staged: Option<Staging>,
}

#[derive(Clone)]
struct Staging {
    parts: Vec<Vec<u8>>,
    head: Vec<u8>,
}

/// Typed local refusal before transport or host execution begins.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// A command, page or cursor exceeds its finite budget.
    #[error("client command capacity exceeded")]
    Capacity,
    /// The command is malformed or contains fields outside this protocol.
    #[error("invalid client command")]
    Invalid,
}

impl CommandError {
    /// Stable classification shared by native and WASM adapters.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Capacity => "client_command_capacity",
            Self::Invalid => "invalid_client_command",
        }
    }
}

impl PreparedCommand {
    /// Admit an already encoded body without rewriting its original bytes.
    /// The caller keeps this value across an interrupted exchange.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, CommandError> {
        let command = Self::decode_whole(&bytes)?;
        Self::admit(bytes, &command)
    }

    /// Encode `command`, of any size: past one frame it is staged in parts.
    pub fn new(command: &AgentCommand) -> Result<Self, CommandError> {
        command.validate()?;
        let bytes = serde_json::to_vec(command).map_err(|_| CommandError::Invalid)?;
        Self::admit(bytes, command)
    }

    /// The frames carrying `bytes`, the encoding of `command`: itself when it
    /// fits one frame, else parts of one frame each and the head naming them.
    /// The host assembles the parts and checks them against the digest, so
    /// the command it runs is exactly these bytes.
    fn admit(bytes: Vec<u8>, command: &AgentCommand) -> Result<Self, CommandError> {
        if bytes.len() <= MAX_COMMAND_BYTES {
            return Ok(Self {
                command: bytes,
                staged: None,
            });
        }
        let agent = match command {
            AgentCommand::Stage { .. } | AgentCommand::Staged { .. } => {
                return Err(CommandError::Invalid);
            }
            command => command.agent().ok_or(CommandError::Invalid)?.clone(),
        };
        let text = std::str::from_utf8(&bytes).map_err(|_| CommandError::Invalid)?;
        let digest = blake3::hash(&bytes).to_hex().to_string();
        let frame =
            |command: &AgentCommand| serde_json::to_vec(command).map_err(|_| CommandError::Invalid);
        let part = |part: u32, text: &str| AgentCommand::Stage {
            agent: agent.clone(),
            digest: digest.clone(),
            part,
            text: text.to_owned(),
        };
        // Every part leaves room for the widest index; one escaped character
        // takes at most six bytes.
        let budget = MAX_COMMAND_BYTES
            .checked_sub(frame(&part(u32::MAX, ""))?.len())
            .filter(|budget| *budget >= 6)
            .ok_or(CommandError::Invalid)?;
        let (mut parts, mut start, mut used) = (Vec::new(), 0, 0);
        for (index, ch) in text.char_indices() {
            let cost = escaped_len(ch);
            if used + cost > budget {
                parts.push(text.get(start..index).ok_or(CommandError::Invalid)?);
                (start, used) = (index, 0);
            }
            used += cost;
        }
        parts.push(text.get(start..).ok_or(CommandError::Invalid)?);
        let parts = parts
            .into_iter()
            .enumerate()
            .map(|(index, text)| {
                frame(&part(
                    u32::try_from(index).map_err(|_| CommandError::Invalid)?,
                    text,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let head = frame(&AgentCommand::Staged {
            agent: agent.clone(),
            command: command.kind().to_owned(),
            digest: digest.clone(),
            parts: u32::try_from(parts.len()).map_err(|_| CommandError::Invalid)?,
            bytes: bytes.len() as u64,
        })?;
        Ok(Self {
            command: bytes,
            staged: Some(Staging { parts, head }),
        })
    }

    /// Decode one frame, bounded before any host work.
    pub fn decode(bytes: &[u8]) -> Result<AgentCommand, CommandError> {
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let command: AgentCommand =
            serde_json::from_slice(bytes).map_err(|_| CommandError::Invalid)?;
        command.validate()?;
        Ok(command)
    }

    /// Decode a whole command, of any size: a staged command its host
    /// assembled, or one a caller encoded. Staging frames are one frame each.
    pub fn decode_whole(bytes: &[u8]) -> Result<AgentCommand, CommandError> {
        let command: AgentCommand =
            serde_json::from_slice(bytes).map_err(|_| CommandError::Invalid)?;
        command.validate()?;
        if matches!(
            command,
            AgentCommand::Stage { .. } | AgentCommand::Staged { .. }
        ) && bytes.len() > MAX_COMMAND_BYTES
        {
            return Err(CommandError::Invalid);
        }
        Ok(command)
    }

    /// The whole command's exact encoding, as the host runs it.
    #[must_use]
    pub fn command(&self) -> &[u8] {
        &self.command
    }

    /// The whole command, decoded.
    ///
    /// # Errors
    /// Never for a prepared command; its encoding was admitted on creation.
    pub fn decoded(&self) -> Result<AgentCommand, CommandError> {
        Self::decode_whole(&self.command)
    }

    /// The frames that carry the command, in the order they are sent: its
    /// staged parts, then their head; or, within one frame, the command.
    pub(crate) fn frames(&self) -> impl Iterator<Item = &[u8]> {
        let (parts, last) = self
            .staged
            .as_ref()
            .map_or((&[][..], self.command.as_slice()), |staged| {
                (staged.parts.as_slice(), staged.head.as_slice())
            });
        parts.iter().map(Vec::as_slice).chain(std::iter::once(last))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_cannot_assert_a_principal() {
        let command = br#"{"kind":"cancel","agent":"harness/agents/00000000000000000000000000000001","id":"cancel-1","principal":"administrator"}"#;
        assert!(PreparedCommand::decode(command).is_err());
    }

    #[test]
    fn admitted_encoded_body_is_not_reconstructed() -> Result<(), Box<dyn std::error::Error>> {
        let original = concat!(
            "{ \"kind\" : \"cancel\", ",
            "\"agent\" : \"harness/agents/00000000000000000000000000000001\", ",
            "\"id\" : \"cancel-1\" }"
        )
        .as_bytes()
        .to_vec();
        let prepared = PreparedCommand::from_bytes(original.clone())?;
        assert_eq!(prepared.command(), original);
        assert!(prepared.frames().eq([original.as_slice()]));
        assert!(matches!(
            PreparedCommand::from_bytes(br#"{"kind":"cancel"}"#.to_vec()),
            Err(CommandError::Invalid)
        ));
        Ok(())
    }

    #[test]
    fn opaque_page_and_finite_defaults_survive_decode() -> Result<(), Box<dyn std::error::Error>> {
        let page: AgentPageRequest = serde_json::from_str(r#"{"cursor":"cut/opaque+token="}"#)?;
        assert_eq!(page.items(), 32);
        assert_eq!(page.bytes(), 32 * 1024);
        assert_eq!(page.cursor(), Some("cut/opaque+token="));
        assert!(serde_json::from_str::<AgentPageRequest>(r#"{"items":0}"#).is_err());
        assert!(serde_json::from_str::<AgentPageRequest>(r#"{"bytes":4294967295}"#).is_err());
        Ok(())
    }

    #[test]
    fn a_command_past_one_frame_is_staged_in_frames_that_reassemble_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let agent =
            AgentPath::try_from("harness/agents/00000000000000000000000000000001".to_owned())?;
        // Escapes expand text up to sixfold, and multibyte characters must
        // never split: each part is cut on the encoded, escaped length.
        let sample = "line \"quoted\" \u{1} é 🦀\n";
        let text = sample.repeat(MAX_COMMAND_BYTES / sample.len() + 1);
        let command = AgentCommand::Prompt {
            agent: agent.clone(),
            id: IntentId::try_from("prompt-1")?,
            body: ModelContent::Text(text.clone()),
        };
        let prepared = PreparedCommand::new(&command)?;
        let frames = prepared.frames().collect::<Vec<_>>();
        let (head, parts) = frames.split_last().ok_or("no frame")?;
        let mut assembled = String::new();
        for (index, part) in parts.iter().enumerate() {
            assert!(part.len() <= MAX_COMMAND_BYTES);
            let AgentCommand::Stage { part, text, .. } = PreparedCommand::decode(part)? else {
                return Err("expected a staged part".into());
            };
            assert_eq!(part as usize, index);
            assembled.push_str(&text);
        }
        assert_eq!(assembled.as_bytes(), prepared.command());
        let AgentCommand::Staged {
            command: kind,
            digest,
            parts,
            bytes,
            ..
        } = PreparedCommand::decode(head)?
        else {
            return Err("expected a staged head".into());
        };
        assert_eq!(kind, "prompt");
        assert_eq!(parts as usize, frames.len() - 1);
        assert_eq!(bytes as usize, assembled.len());
        assert_eq!(digest, blake3::hash(assembled.as_bytes()).to_hex().as_str());
        let AgentCommand::Prompt { id, body, .. } =
            PreparedCommand::decode_whole(assembled.as_bytes())?
        else {
            return Err("expected the prompt".into());
        };
        assert_eq!(id.as_str(), "prompt-1");
        assert_eq!(body, ModelContent::Text(text.clone()));
        assert!(
            matches!(prepared.decoded()?, AgentCommand::Prompt { id, body, .. }
                if id.as_str() == "prompt-1" && body == ModelContent::Text(text))
        );
        // Negative controls: one frame stays bounded, and a command that fits
        // one frame is sent as itself.
        assert!(matches!(
            PreparedCommand::decode(&vec![b' '; MAX_COMMAND_BYTES + 1]),
            Err(CommandError::Capacity)
        ));
        let small = PreparedCommand::new(&AgentCommand::Settlement { agent, wait: false })?;
        assert!(small.frames().eq([small.command()]));
        Ok(())
    }
}
