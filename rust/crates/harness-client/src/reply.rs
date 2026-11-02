//! One frontend control result, independent of the endpoint's hosting backend.

use crate::{AgentCommand, CommandError, MAX_AGENT_COMMAND_BYTES, MAX_READ_BYTES};
use acyclic_harness_core::{
    agent::{
        AgentPath, Delivered, Error as AgentError, IntentId, Phase, Reply, StatusRecord, TurnInput,
    },
    context::{DiffCursor, DiffFilePage},
    interaction::ApprovalBinding,
};
use acyclic_stream::{CommitRequest, wire_codec};
use base64::Engine as _;
use prost::Message as _;
use serde::{Deserialize, Serialize};

/// The canonical commit can be larger than a displayed page. The session carrier
/// must credit/chunk this bounded envelope, never discard an uncertain request.
pub const MAX_AGENT_REPLY_BYTES: usize = acyclic_stream::MAX_COMMAND_BYTES + 128;

/// A refusal's reason is the host error's own text, cut at a character
/// boundary to this many bytes so a refusal always fits its envelope.
pub const MAX_REFUSAL_REASON_BYTES: usize = 4096;

/// Child discovery carries status at the page cut, without another follow.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ChildAgent {
    /// Canonical child identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub agent: AgentPath,
    /// Actual published status, or no status record at this cut.
    pub status: Option<StatusRecord>,
}

/// Original pending action identity; the immutable request is read separately.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(deny_unknown_fields)]
pub struct PendingApproval {
    /// Identity used unchanged in the decision command.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub id: IntentId,
    /// Original zero-based waiting-status record selecting the request bytes.
    pub status: u64,
    /// Exact action from that same actor/status publication.
    pub binding: ApprovalBinding,
}

/// Successful replies are finite views or canonical commit receipts. A delivery
/// receipt never claims that the agent has successfully completed the input.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentReply {
    /// The binding of the tree this endpoint serves the credential.
    Bound {
        /// The canonical bodyless invocation, base64 (standard alphabet,
        /// padded).
        binding: String,
        /// The `fs:/` resource whose follow announces the root's tool output.
        tools: String,
    },
    /// The root admitted by the verified credential.
    Root {
        /// Admitted root identity.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
    },
    /// Prompt, cancellation or approval inbox delivery.
    Delivered { delivery: Delivered },
    /// Spawn publication, including the canonical child path.
    Spawned {
        /// The newly published direct child.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        delivery: Delivered,
    },
    /// A single direct-child page, with an opaque exclusive continuation.
    Children {
        items: Vec<ChildAgent>,
        next: Option<String>,
    },
    /// Authorized project roots observed for an exact-head restore request.
    ProjectHeads {
        /// Current roots, never substituted from a saved history descriptor.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown[]"))]
        items: Vec<acyclic_harness_core::agent::HistoryVolume>,
    },
    /// One page of immutable turn summaries at a fixed exclusive cut.
    History {
        /// Consecutive saved turns, newest first. Roots remain at the source.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown[]"))]
        items: Vec<acyclic_harness_core::agent::HistorySummary>,
        /// Opaque exclusive continuation, or the end of this fixed cut.
        next: Option<String>,
    },
    /// A single authorized pending-approval page.
    Approvals {
        items: Vec<PendingApproval>,
        next: Option<String>,
    },
    /// The committed reply to one accepted message.
    Reply { reply: Reply },
    /// A bounded committed execution query, shared by every frontend.
    Usage {
        /// Exact original inbox sequence, at or after this agent's fork cut.
        input: u64,
        /// One fixed journal page; None marks the current committed inbox tail.
        page: Option<acyclic_harness_core::executor::ModelUsagePage>,
        /// The original actor checkpoint consumed this input and retains no
        /// paused, queued or overflowed work at or before it. A short page or
        /// status cursor alone does not establish this fence.
        settled: bool,
    },
    /// Original observations, without a second stream or reconstructed transcript.
    Events {
        /// Exact original inbox sequence, at or after this agent's fork cut.
        input: u64,
        /// Original inbox tail observed on query admission, without another read.
        /// A finite export freezes this cut; later inputs cannot extend it.
        inbox_tail: u64,
        /// One bounded source page. None marks the current committed inbox tail.
        page: Option<acyclic_harness_core::executor::ExecutionEventPage>,
        /// Original consumed-input fence, with no retained work at or before it.
        settled: bool,
    },
    /// How far an agent has settled.
    Settlement {
        /// Settled turns: the conversation's tail.
        settled: u64,
        /// Published statuses: the status stream's tail.
        statuses: u64,
        /// The agent's current phase.
        phase: Phase,
    },
    /// One visible file fragment from the authorized workspace query.
    Diff {
        /// Visible fragments in cursor order; empty only for an empty projection.
        items: Vec<DiffFilePage>,
    },
    /// One range of a read file, and the file's whole length.
    Content {
        /// The range's bytes, base64 (standard alphabet, padded).
        data: String,
        /// The file's length; a reader is done at this offset.
        length: u64,
    },
    /// One part of a staged command is held for its head.
    Staged {},
}

impl AgentReply {
    /// The read bytes of a [`Self::Content`] reply.
    ///
    /// # Errors
    /// When this is not a content reply or its data is not base64.
    pub fn content(&self) -> Result<(Vec<u8>, u64), CommandError> {
        match self {
            Self::Content { data, length } => Ok((
                base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|_| CommandError::Invalid)?,
                *length,
            )),
            _ => Err(CommandError::Invalid),
        }
    }

    /// A [`Self::Content`] reply carrying `bytes` of a file of `length`.
    #[must_use]
    pub fn read(bytes: &[u8], length: u64) -> Self {
        Self::Content {
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
            length,
        }
    }
}

/// Remote failures preserve uncertain custody as a typed original request.
#[derive(Debug, thiserror::Error)]
pub enum AgentControlFailure {
    /// A definitive refusal, classified by the host's canonical error code,
    /// with the host error's own text as its reason.
    #[error("{}", refusal(code, reason))]
    Refused { code: String, reason: String },
    /// The exact original request; presentation must not replace this with prose.
    #[error("agent commit outcome is unknown")]
    Unknown(Box<CommitRequest>),
}

impl AgentControlFailure {
    /// Stable classification without losing the owned recovery request.
    pub fn code(&self) -> &str {
        match self {
            Self::Refused { code, .. } => code,
            Self::Unknown(_) => "agent_unknown",
        }
    }
}

/// A refusal reads as its reason, or its code when the host gave none.
fn refusal(code: &str, reason: &str) -> String {
    if reason.is_empty() {
        format!("agent control refused: {code}")
    } else {
        reason.to_owned()
    }
}

/// `text` cut to at most [`MAX_REFUSAL_REASON_BYTES`] at a character boundary.
fn bounded_reason(mut text: String) -> String {
    if text.len() > MAX_REFUSAL_REASON_BYTES {
        let mut end = MAX_REFUSAL_REASON_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

// One binary envelope is shared by in-process and network session adapters.
// Unknown uses the canonical Streams protobuf, never JSON byte arrays or prose.
#[derive(prost::Message)]
struct Envelope {
    #[prost(oneof = "envelope::Value", tags = "1, 2, 3")]
    value: Option<envelope::Value>,
    /// A refusal's reason; present only beside [`envelope::Value::Refused`].
    #[prost(string, optional, tag = "4")]
    reason: Option<String>,
}

mod envelope {
    #[derive(prost::Oneof)]
    pub(super) enum Value {
        #[prost(bytes, tag = "1")]
        Reply(Vec<u8>),
        #[prost(string, tag = "2")]
        Refused(String),
        #[prost(message, tag = "3")]
        Unknown(acyclic_stream::wire::CommitRequest),
    }
}

/// Size a successful canonical reply from its compact JSON byte count, without
/// allocating either body or envelope. Page credit includes this wire framing.
pub fn encoded_agent_reply_size(json_bytes: u64) -> Result<u64, CommandError> {
    let length = usize::try_from(json_bytes).map_err(|_| CommandError::Capacity)?;
    if length > MAX_AGENT_COMMAND_BYTES {
        return Err(CommandError::Capacity);
    }
    let framing = Envelope {
        value: Some(envelope::Value::Reply(Vec::new())),
        reason: None,
    }
    .encoded_len()
        - prost::length_delimiter_len(0);
    Ok(json_bytes + (framing + prost::length_delimiter_len(length)) as u64)
}

/// Encode by borrowing the host's result. Capacity/encoding refusal leaves the
/// original uncertain request with its owner, including on export failure.
pub fn encode_agent_reply(
    result: &Result<AgentReply, AgentError>,
    command: &AgentCommand,
) -> Result<Vec<u8>, CommandError> {
    use envelope::Value;
    let mut reason = None;
    let value = match result {
        Ok(reply) => {
            let mut writer = ReplyWriter(Vec::new());
            serde_json::to_writer(&mut writer, reply).map_err(|_| CommandError::Capacity)?;
            Value::Reply(writer.0)
        }
        Err(AgentError::Unknown(request)) => {
            if request.wire_encoded_len() > acyclic_stream::MAX_COMMAND_BYTES {
                return Err(CommandError::Capacity);
            }
            Value::Unknown(wire_codec::commit_to_wire(request))
        }
        Err(error) => {
            reason = Some(bounded_reason(error.to_string()));
            Value::Refused(error.code().to_owned())
        }
    };
    let envelope = Envelope {
        value: Some(value),
        reason,
    };
    let encoded_len = match &envelope.value {
        Some(Value::Reply(bytes)) => encoded_agent_reply_size(bytes.len() as u64)? as usize,
        _ => envelope.encoded_len(),
    };
    if encoded_len > MAX_AGENT_REPLY_BYTES {
        return Err(CommandError::Capacity);
    }
    if let Ok(reply) = result {
        validate_reply(reply, command, encoded_len)?;
    }
    Ok(envelope.encode_to_vec())
}

/// Decode only a bounded complete result, checked against the original command.
/// A malformed wire result is distinct from a definitive host refusal.
pub fn decode_agent_reply(
    bytes: &[u8],
    command: &AgentCommand,
) -> Result<Result<AgentReply, AgentControlFailure>, CommandError> {
    if bytes.len() > MAX_AGENT_REPLY_BYTES {
        return Err(CommandError::Capacity);
    }
    let envelope_len = bytes.len();
    let envelope = Envelope::decode(bytes).map_err(|_| CommandError::Invalid)?;
    // The one Rust encoder is the protocol authority. Reject duplicate/unknown
    // protobuf fields instead of projecting a different recovery request.
    if envelope.encode_to_vec() != bytes {
        return Err(CommandError::Invalid);
    }
    let reason = envelope.reason;
    if reason
        .as_ref()
        .is_some_and(|reason| reason.len() > MAX_REFUSAL_REASON_BYTES)
        || (reason.is_some() && !matches!(envelope.value, Some(envelope::Value::Refused(_))))
    {
        return Err(CommandError::Invalid);
    }
    match envelope.value.ok_or(CommandError::Invalid)? {
        envelope::Value::Unknown(request) => {
            // AgentError owns a domain CommitRequest, which has no deadline.
            // Silently discarding this wire-only field would change custody.
            if request.deadline_unix_millis.is_some() {
                return Err(CommandError::Invalid);
            }
            if request.encoded_len() > acyclic_stream::MAX_COMMAND_BYTES
                || request.conditions.len() > acyclic_stream::MAX_ITEMS
                || request.mutations.is_empty()
                || request.mutations.len() > acyclic_stream::MAX_ITEMS
            {
                return Err(CommandError::Capacity);
            }
            let request =
                wire_codec::commit_from_wire(request).map_err(|_| CommandError::Invalid)?;
            Ok(Err(AgentControlFailure::Unknown(Box::new(request))))
        }
        envelope::Value::Refused(code) => {
            if code.is_empty()
                || code.len() > 128
                || code == "agent_unknown"
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(CommandError::Invalid);
            }
            Ok(Err(AgentControlFailure::Refused {
                code,
                reason: reason.unwrap_or_default(),
            }))
        }
        envelope::Value::Reply(bytes) => {
            if bytes.len() > MAX_AGENT_COMMAND_BYTES {
                return Err(CommandError::Capacity);
            }
            let reply: AgentReply =
                serde_json::from_slice(&bytes).map_err(|_| CommandError::Invalid)?;
            validate_reply(&reply, command, envelope_len)?;
            Ok(Ok(reply))
        }
    }
}

fn validate_reply(
    reply: &AgentReply,
    command: &AgentCommand,
    bytes: usize,
) -> Result<(), CommandError> {
    let page = match (reply, command) {
        (AgentReply::Root { .. }, AgentCommand::Root {})
        | (AgentReply::Bound { .. }, AgentCommand::Bind { .. })
        | (AgentReply::Settlement { .. }, AgentCommand::Settlement { .. })
        | (AgentReply::Staged {}, AgentCommand::Stage { .. })
        | (
            AgentReply::Delivered { .. },
            AgentCommand::Prompt { .. }
            | AgentCommand::Cancel { .. }
            | AgentCommand::Decide { .. }
            | AgentCommand::Compact { .. }
            | AgentCommand::Rewind { .. }
            | AgentCommand::Restore { .. }
            | AgentCommand::ClearAll { .. },
        ) => return Ok(()),
        (AgentReply::Reply { reply }, AgentCommand::Reply { agent, id, inbox }) => {
            // An answer is the entry of exactly the requested message.
            return match reply {
                Reply::Answered {
                    entry,
                    origin,
                    steering,
                    ..
                } => {
                    let request = origin.as_deref().unwrap_or(entry);
                    let matches_input = match steering {
                        Some(source) => {
                            source.inbox == *inbox
                                && &source.id == id
                                && request.inbox < source.inbox
                                && request
                                    .steering
                                    .as_ref()
                                    .is_some_and(|cut| source.inbox < cut.through)
                        }
                        None => {
                            request.inbox == *inbox
                                && matches!(&request.input, TurnInput::Message { id: settled, .. } if settled == id)
                        }
                    };
                    if !matches_input
                        || match origin {
                            Some(origin) => !origin
                                .goal
                                .is_some_and(|run| entry.continues_goal(agent, run)),
                            None => entry.goal.is_some(),
                        }
                    {
                        Err(CommandError::Invalid)
                    } else {
                        Ok(())
                    }
                }
                _ => Ok(()),
            };
        }
        (AgentReply::Content { data, length }, AgentCommand::Read { offset, .. }) => {
            // Padded base64: 4 characters per 3 bytes, less the padding.
            let padding = data.bytes().rev().take_while(|byte| *byte == b'=').count();
            let bytes = (data.len() / 4 * 3).saturating_sub(padding) as u64;
            return if data.len() % 4 == 0
                && bytes <= MAX_READ_BYTES
                && offset.checked_add(bytes).is_some_and(|end| end <= *length)
            {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
        }
        (
            AgentReply::Usage {
                page: None,
                input,
                settled,
            },
            AgentCommand::Usage {
                input: requested,
                after,
                wait,
                ..
            },
        ) => {
            return if input >= requested && *after == 0 && !wait && !settled {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
        }
        (
            AgentReply::Usage {
                page: Some(page),
                input,
                settled,
            },
            AgentCommand::Usage {
                agent,
                after,
                input: requested,
                wait,
            },
        ) => {
            return if input >= requested
                && (input == requested || *after == 0)
                && (!wait || *settled || page.end > *after)
                && page.items.len() <= 32
                && page.end.checked_sub(*after).is_some_and(|visited| {
                    visited <= 32
                        && page.items.len() as u64 <= visited
                        && page
                            .next
                            .is_none_or(|next| next == page.end && visited == 32)
                })
                && page.items.iter().all(|item| {
                    item.version == 1 && item.agent_path.as_deref() == Some(agent.as_str())
                }) {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
        }
        (
            AgentReply::Events {
                page: None,
                input,
                inbox_tail,
                settled,
            },
            AgentCommand::Events {
                input: requested,
                after,
                wait,
                ..
            },
        ) => {
            return if input >= requested && input == inbox_tail && *after == 0 && !wait && !settled
            {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
        }
        (
            AgentReply::Events {
                page: Some(page),
                input,
                settled,
                inbox_tail,
            },
            AgentCommand::Events {
                after,
                input: requested,
                wait,
                ..
            },
        ) => {
            use acyclic_harness_core::executor::EXECUTION_EVENT_PAGE_RECORDS;
            let count = page.items.len() as u64;
            return if input >= requested
                && (input == requested || *after == 0)
                && input < inbox_tail
                && (!wait || *settled || page.end > *after)
                && count <= u64::from(EXECUTION_EVENT_PAGE_RECORDS)
                && after.checked_add(count) == Some(page.end)
                && page.next.is_none_or(|next| {
                    next == page.end && count == u64::from(EXECUTION_EVENT_PAGE_RECORDS)
                })
                && (page.operation.is_some() || (*after == 0 && page.items.is_empty()))
                && page.items.iter().enumerate().all(|(index, record)| {
                    Some(record.operation_id) == page.operation
                        && after
                            .checked_add(index as u64)
                            .and_then(|n| n.checked_add(1))
                            == Some(record.sequence)
                }) {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
        }
        (
            AgentReply::Spawned { agent, .. },
            AgentCommand::Spawn { parent, .. } | AgentCommand::Fork { parent, .. },
        ) => {
            return if agent.parent().as_ref() == Some(parent) {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
        }
        (AgentReply::Diff { items }, AgentCommand::Diff { page, .. }) => {
            return validate_diff_page(items, page, bytes);
        }
        (AgentReply::Children { items, next }, AgentCommand::Children { agent, page }) => {
            let mut seen = std::collections::BTreeSet::new();
            if items.iter().any(|item| {
                item.agent.parent().as_ref() != Some(agent) || !seen.insert(&item.agent)
            }) {
                return Err(CommandError::Invalid);
            }
            (items.len(), next, page)
        }
        (AgentReply::ProjectHeads { items }, AgentCommand::ProjectHeads { .. }) => {
            let mut seen = std::collections::BTreeSet::new();
            return if items.iter().all(|item| {
                item.binding.volume.class()
                    == acyclic_harness_core::conversation::VolumeClass::Project
                    && seen.insert(&item.binding.name)
            }) {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
        }
        (AgentReply::History { items, next }, AgentCommand::History { page, .. }) => {
            if items
                .windows(2)
                .any(|pair| pair[1].conversation.checked_add(1) != Some(pair[0].conversation))
            {
                return Err(CommandError::Invalid);
            }
            if items
                .iter()
                .any(|item| item.conversation == 0 || item.context_tail > item.conversation)
            {
                return Err(CommandError::Invalid);
            }
            (items.len(), next, page)
        }
        (AgentReply::Approvals { items, next }, AgentCommand::Approvals { page, .. }) => {
            if items.len() > 1 || next.is_some() || (page.cursor().is_some() && !items.is_empty()) {
                return Err(CommandError::Invalid);
            }
            for item in items {
                item.binding.validate().map_err(|_| CommandError::Invalid)?;
            }
            (items.len(), next, page)
        }
        _ => return Err(CommandError::Invalid),
    };
    if page.0 > page.2.items() as usize
        || bytes > page.2.bytes() as usize
        || page
            .1
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || Some(cursor.as_str()) == page.2.cursor())
    {
        return Err(CommandError::Capacity);
    }
    Ok(())
}

fn validate_diff_page(
    fragments: &[DiffFilePage],
    page: &crate::AgentPageRequest,
    encoded_bytes: usize,
) -> Result<(), CommandError> {
    if encoded_bytes > page.bytes() as usize {
        return Err(CommandError::Capacity);
    }
    let requested: Option<DiffCursor> = page
        .cursor()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| CommandError::Invalid)?;
    if fragments.is_empty() && requested.is_some() {
        return Err(CommandError::Invalid);
    }
    if fragments.len() > page.items() as usize {
        return Err(CommandError::Capacity);
    }
    let mut previous = requested.as_ref();
    let mut rows = 0usize;
    for (index, fragment) in fragments.iter().enumerate() {
        acyclic_harness_core::conversation::validate_content_path(&fragment.path, &mut None)
            .map_err(|_| CommandError::Invalid)?;
        rows = rows
            .checked_add(usize::from(!fragment.continued))
            .and_then(|rows| rows.checked_add(fragment.patch.split_inclusive('\n').count()))
            .ok_or(CommandError::Capacity)?;
        if rows > page.items() as usize || (fragment.continued && fragment.patch.is_empty()) {
            return Err(CommandError::Capacity);
        }
        if (fragment.binary || fragment.omitted_bytes.is_some())
            && (!fragment.patch.is_empty()
                || fragment.continued
                || fragment.added != 0
                || fragment.removed != 0)
        {
            return Err(CommandError::Invalid);
        }
        if previous.is_some_and(|previous| {
            previous.publication != fragment.publication
                || previous.path != fragment.path
                || previous.offset.is_some() != fragment.continued
        }) || (previous.is_none() && fragment.continued)
        {
            return Err(CommandError::Invalid);
        }
        if let Some(next) = &fragment.next {
            acyclic_harness_core::conversation::validate_content_path(&next.path, &mut None)
                .map_err(|_| CommandError::Invalid)?;
            let consumed = previous
                .and_then(|cursor| cursor.offset)
                .unwrap_or(0)
                .checked_add(fragment.patch.len())
                .ok_or(CommandError::Invalid)?;
            if next.publication != fragment.publication
                || if next.path == fragment.path {
                    next.offset != Some(consumed)
                } else {
                    next.path <= fragment.path || next.offset.is_some()
                }
            {
                return Err(CommandError::Invalid);
            }
        } else if index + 1 != fragments.len() {
            return Err(CommandError::Invalid);
        }
        previous = fragment.next.as_ref();
    }
    Ok(())
}

struct ReplyWriter(Vec<u8>);

impl std::io::Write for ReplyWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_AGENT_COMMAND_BYTES - self.0.len() {
            return Err(std::io::Error::other("client reply capacity exceeded"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentPageRequest;
    use acyclic_stream::{
        AppendRecord, CommitCondition, CommitMutation, IdempotencyKey, StreamPath,
    };

    fn request() -> Result<CommitRequest, acyclic_stream::StreamError> {
        let path = StreamPath::new("agent/inbox")?;
        Ok(CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Tail {
                path: path.clone(),
                expected: 7,
            }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![AppendRecord::from("exact original payload")],
            }],
            idempotency_key: IdempotencyKey::new(bytes::Bytes::from_static(b"original-key"))?,
        })
    }

    #[test]
    fn unknown_roundtrip_retains_exact_request() -> Result<(), Box<dyn std::error::Error>> {
        let original = request()?;
        let result = Err(AgentError::Unknown(Box::new(original.clone())));
        let bytes = encode_agent_reply(&result, &AgentCommand::Root {})?;
        let Err(AgentControlFailure::Unknown(decoded)) =
            decode_agent_reply(&bytes, &AgentCommand::Root {})?
        else {
            return Err("uncertain result lost its request".into());
        };
        assert_eq!(*decoded, original);
        assert_eq!(
            result.as_ref().err().and_then(AgentError::unknown_request),
            Some(&original)
        );
        Ok(())
    }

    #[test]
    fn a_refusal_carries_its_host_reason() -> Result<(), Box<dyn std::error::Error>> {
        let result = Err(AgentError::Turn(Box::new(
            acyclic_harness_core::Error::NotFound("Unknown command: /marke".into()),
        )));
        let bytes = encode_agent_reply(&result, &AgentCommand::Root {})?;
        let Err(refused @ AgentControlFailure::Refused { .. }) =
            decode_agent_reply(&bytes, &AgentCommand::Root {})?
        else {
            return Err("a refusal decoded as another result".into());
        };
        assert_eq!(refused.code(), "agent_turn_error");
        assert!(
            refused.to_string().contains("Unknown command: /marke"),
            "{refused}"
        );
        // Negative controls: a reason beside a reply, or past its bound, is
        // malformed rather than shown.
        let reply = Envelope {
            value: Some(envelope::Value::Reply(b"{}".to_vec())),
            reason: Some("stray".into()),
        }
        .encode_to_vec();
        assert!(matches!(
            decode_agent_reply(&reply, &AgentCommand::Root {}),
            Err(CommandError::Invalid)
        ));
        let oversized = Envelope {
            value: Some(envelope::Value::Refused("agent_turn_error".into())),
            reason: Some("x".repeat(MAX_REFUSAL_REASON_BYTES + 1)),
        }
        .encode_to_vec();
        assert!(matches!(
            decode_agent_reply(&oversized, &AgentCommand::Root {}),
            Err(CommandError::Invalid)
        ));
        // A long host error is cut to the bound at a character boundary.
        let long = Err(AgentError::Turn(Box::new(
            acyclic_harness_core::Error::Invalid("\u{e9}".repeat(MAX_REFUSAL_REASON_BYTES)),
        )));
        let bytes = encode_agent_reply(&long, &AgentCommand::Root {})?;
        let Err(AgentControlFailure::Refused { reason, .. }) =
            decode_agent_reply(&bytes, &AgentCommand::Root {})?
        else {
            return Err("a long refusal decoded as another result".into());
        };
        assert!(reason.len() <= MAX_REFUSAL_REASON_BYTES && !reason.is_empty());
        Ok(())
    }

    #[test]
    fn unknown_without_owned_request_is_not_a_refusal() {
        let bytes = Envelope {
            value: Some(envelope::Value::Refused("agent_unknown".into())),
            reason: None,
        }
        .encode_to_vec();
        assert!(matches!(
            decode_agent_reply(&bytes, &AgentCommand::Root {}),
            Err(CommandError::Invalid)
        ));
    }

    #[test]
    fn wire_only_recovery_facts_cannot_be_discarded() -> Result<(), Box<dyn std::error::Error>> {
        let mut request = wire_codec::commit_to_wire(&request()?);
        request.deadline_unix_millis = Some(42);
        let bytes = Envelope {
            value: Some(envelope::Value::Unknown(request)),
            reason: None,
        }
        .encode_to_vec();
        assert!(matches!(
            decode_agent_reply(&bytes, &AgentCommand::Root {}),
            Err(CommandError::Invalid)
        ));
        Ok(())
    }

    #[test]
    fn page_budget_includes_result_framing() -> Result<(), Box<dyn std::error::Error>> {
        let reply = AgentReply::Children {
            items: vec![],
            next: None,
        };
        let body_bytes = serde_json::to_vec(&reply)?.len() as u32;
        let command = AgentCommand::Children {
            agent: AgentPath::try_from(
                "harness/agents/00000000000000000000000000000001".to_owned(),
            )?,
            page: AgentPageRequest::new(1, body_bytes, None)?,
        };
        assert!(matches!(
            encode_agent_reply(&Ok(reply), &command),
            Err(CommandError::Capacity)
        ));
        Ok(())
    }

    #[test]
    fn usage_reply_frontiers_preserve_empty_and_partial_source_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_harness_core::executor::ModelUsagePage;
        let agent = AgentPath::root(&acyclic_harness_core::agent::AgentId::from_bytes([1; 16]));
        for after in [0, 7, u64::MAX - 32, u64::MAX] {
            let command = AgentCommand::Usage {
                agent: agent.clone(),
                input: 3,
                after,
                wait: false,
            };
            for visited in [0, 1, 31, 32] {
                let Some(end) = after.checked_add(visited) else {
                    continue;
                };
                let reply = AgentReply::Usage {
                    input: 3,
                    settled: false,
                    page: Some(ModelUsagePage {
                        items: vec![],
                        end,
                        next: (visited == 32).then_some(end),
                    }),
                };
                let bytes = encode_agent_reply(&Ok(reply), &command)?;
                assert!(matches!(decode_agent_reply(&bytes, &command)?,
                    Ok(AgentReply::Usage { page: Some(page), .. }) if page.end == end));
                let live = AgentCommand::Usage {
                    agent: agent.clone(),
                    input: 3,
                    after,
                    wait: true,
                };
                if visited == 0 {
                    assert!(
                        matches!(
                            decode_agent_reply(&bytes, &live),
                            Err(CommandError::Invalid)
                        ),
                        "a quiet live page cannot masquerade as a progress event"
                    );
                    let mut envelope = Envelope::decode(bytes.as_slice())?;
                    let Some(envelope::Value::Reply(body)) = &mut envelope.value else {
                        return Err("usage envelope lost its reply".into());
                    };
                    let mut value: serde_json::Value = serde_json::from_slice(body)?;
                    value["settled"] = true.into();
                    *body = serde_json::to_vec(&value)?;
                    assert!(decode_agent_reply(&envelope.encode_to_vec(), &live)?.is_ok());
                } else {
                    assert!(decode_agent_reply(&bytes, &live)?.is_ok());
                }
                // Mutate the actual reply envelope: a premature continuation,
                // backward cursor or a skipped page must not advance the view.
                for (bad_end, next) in [
                    (end, Some(end.wrapping_add(1))),
                    (after.wrapping_sub(1), None),
                    (after.wrapping_add(33), None),
                ] {
                    let mut envelope = Envelope::decode(bytes.as_slice())?;
                    let Some(envelope::Value::Reply(body)) = &mut envelope.value else {
                        return Err("usage envelope lost its reply".into());
                    };
                    let mut value: serde_json::Value = serde_json::from_slice(body)?;
                    value["page"]["end"] = bad_end.into();
                    value["page"]["next"] = serde_json::to_value(next)?;
                    *body = serde_json::to_vec(&value)?;
                    assert!(matches!(
                        decode_agent_reply(&envelope.encode_to_vec(), &command),
                        Err(CommandError::Invalid)
                    ));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn event_reply_fences_original_sequences_and_quiet_frontiers()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_harness_core::{
            OperationId,
            executor::{ExecutionEvent, ExecutionEventPage, ExecutionRecord},
        };
        let agent = AgentPath::root(&acyclic_harness_core::agent::AgentId::from_bytes([1; 16]));
        let operation = OperationId::from_bytes([2; 16]);
        for after in [0, u64::MAX - 8, u64::MAX] {
            for count in [0, 1, 7, 8] {
                let Some(end) = after.checked_add(count) else {
                    continue;
                };
                let command = AgentCommand::Events {
                    agent: agent.clone(),
                    input: 3,
                    after,
                    wait: false,
                };
                let reply = AgentReply::Events {
                    inbox_tail: 4,
                    input: 3,
                    settled: false,
                    page: Some(ExecutionEventPage {
                        operation: Some(operation),
                        end,
                        next: (count == 8).then_some(end),
                        items: (0..count)
                            .map(|index| ExecutionRecord {
                                operation_id: operation,
                                sequence: after + index + 1,
                                idempotency_key: index.to_string(),
                                event: ExecutionEvent::Started {
                                    request_digest: [3; 32],
                                },
                            })
                            .collect(),
                    }),
                };
                let bytes = encode_agent_reply(&Ok(reply), &command)?;
                let live = AgentCommand::Events {
                    agent: agent.clone(),
                    input: 3,
                    after,
                    wait: true,
                };
                assert_eq!(decode_agent_reply(&bytes, &live).is_ok(), count > 0);
                let envelope = Envelope::decode(bytes.as_slice())?;
                let Some(envelope::Value::Reply(body)) = &envelope.value else {
                    return Err("event envelope lost its reply".into());
                };
                let original: serde_json::Value = serde_json::from_slice(body)?;
                // Mutations of our own wire format, never provider fixtures.
                let mut mutations = Vec::new();
                let mut changed = original.clone();
                changed["page"]["end"] = end.wrapping_add(1).into();
                mutations.push(changed);
                if after != 0 {
                    // Crossing adoption must never apply an earlier turn's
                    // source offset to the new agent's first journal.
                    let mut changed = original.clone();
                    changed["input"] = 4.into();
                    changed["inbox_tail"] = 5.into();
                    mutations.push(changed);
                    if count == 0 {
                        let mut changed = original.clone();
                        changed["page"]["operation"] = serde_json::Value::Null;
                        mutations.push(changed);
                    }
                }
                if count > 0 {
                    for field in ["sequence", "operation_id"] {
                        let mut changed = original.clone();
                        changed["page"]["items"][0][field] = if field == "sequence" {
                            after.into()
                        } else {
                            serde_json::to_value(OperationId::from_bytes([4; 16]))?
                        };
                        mutations.push(changed);
                    }
                    let mut changed = original.clone();
                    changed["page"]["operation"] = serde_json::Value::Null;
                    mutations.push(changed);
                }
                for changed in mutations {
                    let mut envelope = Envelope::decode(bytes.as_slice())?;
                    let Some(envelope::Value::Reply(body)) = &mut envelope.value else {
                        return Err("event envelope lost its reply".into());
                    };
                    *body = serde_json::to_vec(&changed)?;
                    assert!(matches!(
                        decode_agent_reply(&envelope.encode_to_vec(), &command),
                        Err(CommandError::Invalid)
                    ));
                }
            }
        }
        let command = AgentCommand::Events {
            agent,
            input: 3,
            after: 1,
            wait: false,
        };
        assert!(
            encode_agent_reply(
                &Ok(AgentReply::Events {
                    input: 3,
                    inbox_tail: 3,
                    page: None,
                    settled: false,
                }),
                &command
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn diff_pages_preserve_projection_progress_and_visible_credit()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_harness_core::context::DiffKind;
        let agent =
            AgentPath::try_from("harness/agents/00000000000000000000000000000001".to_owned())?;
        let cursor = DiffCursor {
            publication: [7; 32],
            path: "a.txt".into(),
            offset: Some(0),
        };
        let command = AgentCommand::Diff {
            agent: agent.clone(),
            page: AgentPageRequest::new(3, 4096, None)?,
        };
        let items = vec![
            DiffFilePage {
                publication: [7; 32],
                path: "a.txt".into(),
                kind: DiffKind::Added,
                added: 1,
                removed: 0,
                binary: false,
                omitted_bytes: None,
                continued: false,
                patch: String::new(),
                next: Some(cursor.clone()),
            },
            DiffFilePage {
                publication: [7; 32],
                path: "a.txt".into(),
                kind: DiffKind::Added,
                added: 1,
                removed: 0,
                binary: false,
                omitted_bytes: None,
                continued: true,
                patch: "+new\n".into(),
                next: None,
            },
        ];
        let reply = Ok(AgentReply::Diff {
            items: items.clone(),
        });
        let bytes = encode_agent_reply(&reply, &command)?;
        assert!(
            matches!(decode_agent_reply(&bytes, &command)?, Ok(AgentReply::Diff { items }) if items.len() == 2)
        );
        // Every malformed value goes through the actual wire envelope parser.
        for mutation in 0..7 {
            let mut malformed = items.clone();
            match mutation {
                0 => malformed[1].publication = [8; 32],
                1 => malformed[1].patch.clear(),
                2 => malformed[0].next.as_mut().unwrap().offset = Some(1),
                3 => malformed[1].path = "substituted.txt".into(),
                4 => malformed[1].omitted_bytes = Some(65537),
                5 => malformed[0].next = None,
                _ => malformed[1].path = "../outside.txt".into(),
            }
            let bytes = Envelope {
                value: Some(envelope::Value::Reply(serde_json::to_vec(
                    &AgentReply::Diff { items: malformed },
                )?)),
                reason: None,
            }
            .encode_to_vec();
            assert!(
                decode_agent_reply(&bytes, &command).is_err(),
                "accepted malformed diff {mutation}"
            );
        }
        let limited = AgentCommand::Diff {
            agent: agent.clone(),
            page: AgentPageRequest::new(1, 4096, None)?,
        };
        assert!(matches!(
            decode_agent_reply(&bytes, &limited),
            Err(CommandError::Capacity)
        ));
        let continued = AgentCommand::Diff {
            agent,
            page: AgentPageRequest::new(1, 4096, Some(serde_json::to_string(&cursor)?))?,
        };
        let bytes = encode_agent_reply(
            &Ok(AgentReply::Diff {
                items: vec![items[1].clone()],
            }),
            &continued,
        )?;
        assert!(matches!(
            decode_agent_reply(&bytes, &continued)?,
            Ok(AgentReply::Diff { .. })
        ));
        assert!(encode_agent_reply(&Ok(AgentReply::Diff { items: vec![] }), &continued).is_err());
        Ok(())
    }

    #[test]
    fn result_cannot_change_command_kind() -> Result<(), Box<dyn std::error::Error>> {
        let reply = Ok(AgentReply::Delivered {
            delivery: Delivered::Committed { inbox: 7 },
        });
        let command = AgentCommand::Cancel {
            agent: AgentPath::try_from(
                "harness/agents/00000000000000000000000000000001".to_owned(),
            )?,
            id: IntentId::try_from("cancel-1")?,
        };
        let bytes = encode_agent_reply(&reply, &command)?;
        assert!(matches!(
            decode_agent_reply(&bytes, &AgentCommand::Root {}),
            Err(CommandError::Invalid)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reply_keeps_the_original_request_and_exact_goal_run()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_harness_core::{
            AgentId, OperationId,
            agent::{ConversationEntry, GOAL_INTENT, Sender},
            conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
            resources::ProviderRef,
        };
        let agent = AgentPath::root(&acyclic_harness_core::agent::AgentId::from_bytes([1; 16]));
        let run = OperationId::from_bytes([2; 16]);
        let file = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None)?,
                "scratch".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                &mut None,
            )?,
            "output.json".into(),
            "version-1".into(),
            FileDescriptor::from_bytes(b"done", "application/json", &mut None)?,
            "output".into(),
            &mut None,
        )?;
        let origin = ConversationEntry {
            inbox: 1,
            input: TurnInput::Message {
                from: Sender::User {
                    principal: "user".into(),
                },
                id: IntentId::try_from("prompt")?,
                body: Box::new(file.clone()),
                attachments: None,
            },
            output: file.clone(),
            outcome: Default::default(),
            goal: Some(run),
            targets: None,
            tool_results: None,
            steering: None,
            notices: None,
        };
        let terminal = ConversationEntry {
            inbox: 3,
            input: TurnInput::Message {
                from: Sender::Agent {
                    path: agent.clone(),
                },
                id: IntentId::try_from(format!("{GOAL_INTENT}{run}:2"))?,
                body: Box::new(file),
                attachments: None,
            },
            ..origin.clone()
        };
        let command = AgentCommand::Reply {
            agent: agent.clone(),
            id: IntentId::try_from("prompt")?,
            inbox: 1,
        };
        let encode = |entry, origin| {
            encode_agent_reply(
                &Ok(AgentReply::Reply {
                    reply: Reply::Answered {
                        sequence: 2,
                        entry,
                        origin,
                        steering: None,
                    },
                }),
                &command,
            )
        };
        let bytes = encode(terminal.clone(), Some(Box::new(origin.clone())))?;
        assert!(
            matches!(decode_agent_reply(&bytes, &command)?, Ok(AgentReply::Reply {
            reply: Reply::Answered { entry, origin: Some(origin), .. }
        }) if entry.inbox == 3 && origin.inbox == 1)
        );
        let steered_command = AgentCommand::Reply {
            agent: agent.clone(),
            id: IntentId::try_from("steered")?,
            inbox: 2,
        };
        let mut steered_origin = origin.clone();
        steered_origin.steering = Some(acyclic_harness_core::agent::SteeringTranscript {
            through: 3,
            file: steered_origin.output.clone(),
        });
        let steered_goal = AgentReply::Reply {
            reply: Reply::Answered {
                sequence: 2,
                entry: terminal.clone(),
                origin: Some(Box::new(steered_origin)),
                steering: Some(acyclic_harness_core::agent::SteeringReply {
                    inbox: 2,
                    id: IntentId::try_from("steered")?,
                }),
            },
        };
        let bytes = encode_agent_reply(&Ok(steered_goal), &steered_command)?;
        assert!(matches!(
            decode_agent_reply(&bytes, &steered_command)?,
            Ok(AgentReply::Reply {
                reply: Reply::Answered {
                    origin: Some(_),
                    steering: Some(_),
                    ..
                }
            })
        ));
        // The first settlement cannot masquerade as terminal, nor can a
        // foreign goal or sender replace the authenticated self-continuation.
        assert!(matches!(
            encode(origin.clone(), None),
            Err(CommandError::Invalid)
        ));
        let mut foreign = terminal.clone();
        foreign.goal = Some(OperationId::from_bytes([3; 16]));
        assert!(matches!(
            encode(foreign, Some(Box::new(origin.clone()))),
            Err(CommandError::Invalid)
        ));
        let mut foreign = terminal;
        let TurnInput::Message { from, .. } = &mut foreign.input else {
            return Err("terminal input is not a message".into());
        };
        *from = Sender::User {
            principal: "user".into(),
        };
        assert!(matches!(
            encode(foreign, Some(Box::new(origin.clone()))),
            Err(CommandError::Invalid)
        ));
        let ordinary = ConversationEntry {
            goal: None,
            ..origin
        };
        let bytes = encode(ordinary.clone(), None)?;
        assert!(matches!(
            decode_agent_reply(&bytes, &command)?,
            Ok(AgentReply::Reply {
                reply: Reply::Answered { origin: None, .. }
            })
        ));
        // Exercise the actual inbox/conversation producer before crossing the
        // same envelope codec used by the remote client.
        use acyclic_harness_core::agent::{
            AgentRecord as _, AgentState, ForkAdopted, InboxRecord, Message, SteeringTranscript,
            Submitted, create_root, intent_key, reply, send, submit,
        };
        use acyclic_stream::{
            CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream,
            StreamProvider as _,
        };
        let provider = MemoryStream::default();
        let adopted = ForkAdopted {
            profile: None,
            agent: agent.id(),
            parent: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        };
        let created = create_root(&IntentId::try_from("create")?, adopted.clone())?;
        assert!(matches!(
            submit(&provider, created.request).await?,
            Submitted::Committed(_)
        ));
        let TurnInput::Message { from, body, .. } = &ordinary.input else {
            return Err("original request is not a message".into());
        };
        for name in ["prompt", "steered"] {
            let record = InboxRecord::Message(Message {
                from: from.clone(),
                id: IntentId::try_from(name)?,
                body: body.clone(),
                attachments: None,
            });
            assert!(matches!(
                submit(&provider, send(&agent, from, &record)?).await?,
                Submitted::Committed(_)
            ));
        }
        let mut settled = ordinary;
        settled.steering = Some(SteeringTranscript {
            through: 3,
            file: settled.output.clone(),
        });
        let conversation = agent.conversation()?;
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    conditions: vec![CommitCondition::Tail {
                        path: conversation.clone(),
                        expected: 0
                    }],
                    mutations: vec![CommitMutation::Append {
                        path: conversation,
                        records: vec![settled.encode()?.into()]
                    }],
                    idempotency_key: intent_key("settlement", &[])?,
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        let mut state = AgentState::adopted(agent.clone(), &adopted)?;
        state.conversation_tail = 1;
        let command = AgentCommand::Reply {
            agent: agent.clone(),
            id: IntentId::try_from("steered")?,
            inbox: 2,
        };
        let AgentCommand::Reply { id, inbox, .. } = &command else {
            unreachable!()
        };
        let answered = reply(&provider, &state, 3, id, *inbox).await?;
        let bytes = encode_agent_reply(&Ok(AgentReply::Reply { reply: answered }), &command)?;
        assert!(
            matches!(decode_agent_reply(&bytes, &command)?, Ok(AgentReply::Reply {
            reply: Reply::Answered { entry, steering: Some(source), origin: None, .. }
        }) if entry == settled && source.inbox == 2 && source.id == *id)
        );
        let envelope = Envelope::decode(bytes.as_slice())?;
        let Some(envelope::Value::Reply(body)) = &envelope.value else {
            return Err("steering envelope lost its reply".into());
        };
        let original: serde_json::Value = serde_json::from_slice(body)?;
        for field in ["id", "inbox", "through", "missing"] {
            let mut changed = original.clone();
            match field {
                "id" => changed["reply"]["steering"]["id"] = "foreign".into(),
                "inbox" => changed["reply"]["steering"]["inbox"] = 1.into(),
                "through" => changed["reply"]["entry"]["steering"]["through"] = 2.into(),
                _ => changed["reply"]["steering"] = serde_json::Value::Null,
            }
            let mut envelope = Envelope::decode(bytes.as_slice())?;
            let Some(envelope::Value::Reply(body)) = &mut envelope.value else {
                unreachable!()
            };
            *body = serde_json::to_vec(&changed)?;
            assert!(matches!(
                decode_agent_reply(&envelope.encode_to_vec(), &command),
                Err(CommandError::Invalid)
            ));
            let decoded: AgentReply = serde_json::from_value(changed)?;
            assert!(matches!(
                encode_agent_reply(&Ok(decoded), &command),
                Err(CommandError::Invalid)
            ));
        }
        Ok(())
    }

    #[test]
    fn failed_export_does_not_consume_uncertain_custody() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut original = request()?;
        let CommitMutation::Append { records, .. } = &mut original.mutations[0] else {
            return Err("expected append".into());
        };
        *records = vec![AppendRecord::from(bytes::Bytes::from(vec![
            1;
            acyclic_stream::MAX_COMMAND_BYTES
        ]))];
        let result = Err(AgentError::Unknown(Box::new(original)));
        assert!(matches!(
            encode_agent_reply(&result, &AgentCommand::Root {}),
            Err(CommandError::Capacity)
        ));
        assert_eq!(
            result
                .as_ref()
                .err()
                .and_then(AgentError::unknown_request)
                .ok_or("lost uncertain request")?
                .idempotency_key
                .as_bytes(),
            b"original-key"
        );
        Ok(())
    }
}
