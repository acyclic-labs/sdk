//! Backend-neutral agent controls. Host dispatch supplies authenticated identity;
//! no request can assert its sender, approval responder or account membership.

use acyclic_harness_core::{
    agent::{AgentPath, IntentId},
    conversation::FileRef,
    interaction::ApprovalBinding,
    model::ModelContent,
};
use serde::{Deserialize, Serialize};

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
const MAX_CURSOR_BYTES: usize = 4096;

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
            || bytes > MAX_COMMAND_BYTES as u32
            || cursor
                .as_ref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
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
    /// credential, sent with [`crate::AgentBinding::discovery`].
    Bind {},
    /// Discover the root admitted by this session's credential.
    Root {},
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
        approved: bool,
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
    /// An exact file: a settled turn's staged input or output.
    File {
        /// The immutable reference its record carries.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
        file: FileRef,
    },
    /// One chunk of the agent's tool output, as its change records name it.
    ToolOutput {
        /// Path below the agent's tool-output directory.
        path: String,
    },
}

impl AgentCommand {
    fn validate(&self) -> Result<(), CommandError> {
        match self {
            Self::Prompt { body, .. }
            | Self::Spawn { brief: body, .. }
            | Self::Compact {
                instructions: Some(body),
                ..
            } => body
                .validate_user_input(&mut None)
                .map_err(|_| CommandError::Invalid),
            Self::Decide { binding, .. } => binding.validate().map_err(|_| CommandError::Invalid),
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
                target: ReadTarget::ToolOutput { path },
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
            Self::Bind {} | Self::Root {} => None,
            Self::Spawn { parent: agent, .. }
            | Self::Prompt { agent, .. }
            | Self::Cancel { agent, .. }
            | Self::Compact { agent, .. }
            | Self::Rewind { agent, .. }
            | Self::ClearAll { agent, .. }
            | Self::Children { agent, .. }
            | Self::Approvals { agent, .. }
            | Self::Decide { agent, .. }
            | Self::Reply { agent, .. }
            | Self::Settlement { agent }
            | Self::Read { agent, .. }
            | Self::Stage { agent, .. }
            | Self::Staged { agent, .. } => Some(agent),
        }
    }

    /// This command's kind, as its encoding tags it.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Bind {} => "bind",
            Self::Root {} => "root",
            Self::Prompt { .. } => "prompt",
            Self::Cancel { .. } => "cancel",
            Self::Compact { .. } => "compact",
            Self::Rewind { .. } => "rewind",
            Self::ClearAll { .. } => "clear_all",
            Self::Spawn { .. } => "spawn",
            Self::Children { .. } => "children",
            Self::Approvals { .. } => "approvals",
            Self::Decide { .. } => "decide",
            Self::Reply { .. } => "reply",
            Self::Settlement { .. } => "settlement",
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
                parts.push(&text[start..index]);
                (start, used) = (index, 0);
            }
            used += cost;
        }
        parts.push(&text[start..]);
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
        let text = "line \"quoted\" \u{1} é 🦀\n".repeat(400_000);
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
        let AgentCommand::Prompt { body, .. } =
            PreparedCommand::decode_whole(assembled.as_bytes())?
        else {
            return Err("expected the prompt".into());
        };
        assert_eq!(body, ModelContent::Text(text.clone()));
        assert!(
            matches!(prepared.decoded()?, AgentCommand::Prompt { body, .. } if body == ModelContent::Text(text))
        );
        // Negative controls: one frame stays bounded, and a command that fits
        // one frame is sent as itself.
        assert!(matches!(
            PreparedCommand::decode(&vec![b' '; MAX_COMMAND_BYTES + 1]),
            Err(CommandError::Capacity)
        ));
        let small = PreparedCommand::new(&AgentCommand::Settlement { agent })?;
        assert!(small.frames().eq([small.command()]));
        Ok(())
    }

    #[test]
    fn prepared_prompt_preserves_the_intent_and_body() -> Result<(), Box<dyn std::error::Error>> {
        let command = AgentCommand::Prompt {
            agent: AgentPath::try_from(
                "harness/agents/00000000000000000000000000000001".to_owned(),
            )?,
            id: IntentId::try_from("prompt-1")?,
            body: ModelContent::Text("exact input".into()),
        };
        let prepared = PreparedCommand::new(&command)?;
        let AgentCommand::Prompt { id, body, .. } = prepared.decoded()? else {
            return Err("expected prompt command".into());
        };
        assert_eq!(id.as_str(), "prompt-1");
        assert_eq!(body, ModelContent::Text("exact input".into()));
        Ok(())
    }
}
