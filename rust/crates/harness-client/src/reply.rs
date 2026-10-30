//! One frontend control result, independent of the endpoint's hosting backend.

use crate::{AgentCommand, CommandError, MAX_AGENT_COMMAND_BYTES, MAX_READ_BYTES};
use acyclic_harness_core::{
    agent::{
        AgentPath, Delivered, Error as AgentError, IntentId, Phase, Reply, StatusRecord, TurnInput,
    },
    interaction::Interaction,
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

/// Authorized immutable request displayed to a human responder.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct PendingApproval {
    /// Identity used unchanged in the decision command.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub id: IntentId,
    /// Canonical approval request read through the authorized host reader.
    pub request: Interaction,
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
    /// A single authorized pending-approval page.
    Approvals {
        items: Vec<PendingApproval>,
        next: Option<String>,
    },
    /// The committed reply to one accepted message.
    Reply { reply: Reply },
    /// How far an agent has settled.
    Settlement {
        /// Settled turns: the conversation's tail.
        settled: u64,
        /// Published statuses: the status stream's tail.
        statuses: u64,
        /// The agent's current phase.
        phase: Phase,
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
            let mut writer = ReplyWriter(Vec::with_capacity(MAX_AGENT_COMMAND_BYTES));
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
    let encoded_len = envelope.encoded_len();
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
        | (AgentReply::Bound { .. }, AgentCommand::Bind {})
        | (AgentReply::Settlement { .. }, AgentCommand::Settlement { .. })
        | (AgentReply::Staged {}, AgentCommand::Stage { .. })
        | (
            AgentReply::Delivered { .. },
            AgentCommand::Prompt { .. }
            | AgentCommand::Cancel { .. }
            | AgentCommand::Decide { .. }
            | AgentCommand::Compact { .. }
            | AgentCommand::Rewind { .. }
            | AgentCommand::ClearAll { .. },
        ) => return Ok(()),
        (AgentReply::Reply { reply }, AgentCommand::Reply { agent, id, inbox }) => {
            // An answer is the entry of exactly the requested message.
            return match reply {
                Reply::Answered { entry, origin, .. } => {
                    let request = origin.as_deref().unwrap_or(entry);
                    if request.inbox != *inbox
                        || !matches!(&request.input, TurnInput::Message { id: settled, .. } if settled == id)
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
        (AgentReply::Spawned { agent, .. }, AgentCommand::Spawn { parent, .. }) => {
            return if agent.parent().as_ref() == Some(parent) {
                Ok(())
            } else {
                Err(CommandError::Invalid)
            };
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
        (AgentReply::Approvals { items, next }, AgentCommand::Approvals { page, .. }) => {
            let mut seen = std::collections::BTreeSet::new();
            for item in items {
                if !matches!(item.request, Interaction::Approval { .. }) || !seen.insert(&item.id) {
                    return Err(CommandError::Invalid);
                }
                item.request
                    .validate(&mut None)
                    .map_err(|_| CommandError::Invalid)?;
            }
            (items.len(), next, page)
        }
        _ => return Err(CommandError::Invalid),
    };
    if page.0 > page.2.items() as usize
        || bytes > page.2.bytes() as usize
        || page.1.as_ref().is_some_and(|cursor| {
            cursor.is_empty() || cursor.len() > 4096 || Some(cursor.as_str()) == page.2.cursor()
        })
    {
        return Err(CommandError::Capacity);
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
