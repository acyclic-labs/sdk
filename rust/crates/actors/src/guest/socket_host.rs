//! SocketHost calls across an admitted guest boundary. Actor identity is never
//! in a request: the native host derives it from the live delivery attempt.
use super::{GuestError, SocketCommand, SocketHost, SocketPayload, SocketReply, WebSocketId};
use crate::addressing::ActorReference;
use bytes::Bytes;
use prost::Message as _;
use serde::{Deserialize, Serialize};

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
