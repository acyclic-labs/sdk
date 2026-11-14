//! SocketHost calls across an admitted guest boundary. Actor identity is never
//! in a request: the native host derives it from the live delivery attempt.
use super::{GuestError, SocketHost, SocketPayload, WebSocketAutoResponse, WebSocketId};
use crate::addressing::ActorReference;
use bytes::Bytes;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// One host operation. Send and attachment bytes travel in the packet's binary
/// body, avoiding JSON/base64 copies and a smaller binary-frame size budget.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SocketCommand {
    /// Send one text (UTF-8 body) or binary frame.
    Send {
        /// Runtime-owned connection identity.
        connection: String,
        /// True for binary; false for a UTF-8 text body.
        binary: bool,
    },
    /// Close an actor-owned connection.
    Close {
        /// Runtime-owned connection identity.
        connection: String,
        /// WebSocket close code.
        code: u16,
        /// Application close reason.
        reason: String,
    },
    /// List retained connections, optionally with an exact tag.
    List {
        /// Exact tag, or every connection owned by this actor.
        tag: Option<String>,
    },
    /// Replace the connection's opaque attachment with the binary body.
    SetAttachment {
        /// Runtime-owned connection identity.
        connection: String,
    },
    /// Read a connection's opaque attachment.
    GetAttachment {
        /// Runtime-owned connection identity.
        connection: String,
    },
    /// Set or remove the actor's host-owned automatic response.
    SetAutoResponse {
        /// The replacement pair, or removal.
        value: Option<WebSocketAutoResponse>,
    },
    /// Read the actor's automatic response.
    GetAutoResponse,
    /// Read when this connection's automatic response last ran.
    AutoResponseTimestamp {
        /// Runtime-owned connection identity.
        connection: String,
    },
    /// Read this connection's retained tags.
    Tags {
        /// Runtime-owned connection identity.
        connection: String,
    },
}

/// The typed result of one SocketHost call. Attachment bytes are the binary
/// body; timestamps are decimal u64, retaining all bits across JavaScript.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SocketReply {
    /// A mutation completed.
    Done,
    /// Runtime-owned connection identities.
    Connections {
        /// Connections owned by the admitted actor.
        connections: Vec<String>,
    },
    /// Whether the binary body is an attachment, including an empty one.
    Attachment {
        /// Distinguishes no attachment from an empty attachment.
        present: bool,
    },
    /// The actor's automatic response.
    AutoResponse {
        /// The actor's current pair, or no automatic response.
        value: Option<WebSocketAutoResponse>,
    },
    /// Decimal Unix milliseconds, or no automatic response yet.
    Timestamp {
        /// Decimal u64 Unix milliseconds, or no response yet.
        unix_ms: Option<String>,
    },
    /// Retained connection tags.
    Tags {
        /// Connection tags in their retained order.
        tags: Vec<String>,
    },
}

#[derive(Clone, PartialEq, prost::Message)]
struct Packet {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    header: Vec<u8>,
    #[prost(bytes = "bytes", tag = "3")]
    body: Bytes,
}

fn encode<T: Serialize>(header: &T, body: Bytes) -> Result<Bytes, GuestError> {
    Ok(Packet {
        version: 1,
        header: serde_json::to_vec(header)
            .map_err(|error| GuestError::Encode(error.to_string()))?,
        body,
    }
    .encode_to_vec()
    .into())
}

fn decode<T: for<'de> Deserialize<'de> + Serialize>(
    bytes: &[u8],
) -> Result<(T, Bytes), GuestError> {
    let packet = Packet::decode(bytes)
        .map_err(|_| GuestError::Decode("invalid socket host packet".into()))?;
    let header: T = serde_json::from_slice(&packet.header)
        .map_err(|_| GuestError::Decode("invalid socket host operation".into()))?;
    if packet.version != 1 || encode(&header, packet.body.clone())?.as_ref() != bytes {
        return Err(GuestError::Decode("noncanonical socket host packet".into()));
    }
    Ok((header, packet.body))
}

/// Encode an original guest request. Only Send and SetAttachment own body bytes.
pub fn encode_socket_request(command: &SocketCommand, body: Bytes) -> Result<Bytes, GuestError> {
    if !body.is_empty()
        && !matches!(
            command,
            SocketCommand::Send { .. } | SocketCommand::SetAttachment { .. }
        )
    {
        return Err(GuestError::Encode("socket operation has no body".into()));
    }
    encode(command, body)
}

/// Decode the original native result without interpreting attachment bytes.
pub fn decode_socket_reply(bytes: &[u8]) -> Result<(SocketReply, Bytes), GuestError> {
    let (reply, body) = decode(bytes)?;
    if !body.is_empty() && !matches!(reply, SocketReply::Attachment { present: true }) {
        return Err(GuestError::Decode("socket result has no body".into()));
    }
    Ok((reply, body))
}

/// Dispatch through the one SocketHost. The caller MUST admit the delivery
/// attempt and derive `actor` before this call. The core independently checks
/// that actor against each connection; no caller-selected actor can cross it.
pub fn dispatch_socket_request(
    host: &dyn SocketHost,
    actor: &ActorReference,
    bytes: &[u8],
) -> Result<Bytes, GuestError> {
    let (command, body) = decode::<SocketCommand>(bytes)?;
    if !body.is_empty()
        && !matches!(
            command,
            SocketCommand::Send { .. } | SocketCommand::SetAttachment { .. }
        )
    {
        return Err(GuestError::Decode("socket operation has no body".into()));
    }
    let mut output = Bytes::new();
    let reply = match command {
        SocketCommand::Send { connection, binary } => {
            let payload = if binary {
                SocketPayload::Binary { binary: body }
            } else {
                SocketPayload::Text {
                    text: String::from_utf8(body.to_vec())
                        .map_err(|_| GuestError::Decode("socket text is not UTF-8".into()))?,
                }
            };
            host.web_socket_send(actor, &WebSocketId(connection), payload)?;
            SocketReply::Done
        }
        SocketCommand::Close {
            connection,
            code,
            reason,
        } => {
            host.web_socket_close(actor, &WebSocketId(connection), code, &reason)?;
            SocketReply::Done
        }
        SocketCommand::List { tag } => SocketReply::Connections {
            connections: host
                .get_web_sockets(actor, tag.as_deref())?
                .into_iter()
                .map(|id| id.0)
                .collect(),
        },
        SocketCommand::SetAttachment { connection } => {
            host.serialize_attachment(actor, &WebSocketId(connection), body)?;
            SocketReply::Done
        }
        SocketCommand::GetAttachment { connection } => {
            let attachment = host.deserialize_attachment(actor, &WebSocketId(connection))?;
            let present = attachment.is_some();
            output = attachment.unwrap_or_default();
            SocketReply::Attachment { present }
        }
        SocketCommand::SetAutoResponse { value } => {
            host.set_web_socket_auto_response(actor, value)?;
            SocketReply::Done
        }
        SocketCommand::GetAutoResponse => SocketReply::AutoResponse {
            value: host.get_web_socket_auto_response(actor)?,
        },
        SocketCommand::AutoResponseTimestamp { connection } => SocketReply::Timestamp {
            unix_ms: host
                .get_web_socket_auto_response_timestamp(actor, &WebSocketId(connection))?
                .map(|value| value.to_string()),
        },
        SocketCommand::Tags { connection } => SocketReply::Tags {
            tags: host.get_tags(actor, &WebSocketId(connection))?,
        },
    };
    encode(&reply, output)
}
