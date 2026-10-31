//! Actor sockets v1, shared by every host that carries them over Streams.
//!
//! Two encodings live here:
//! - the session channel: the [`SocketOpen`] request a peer sends to open a
//!   socket, and the [`SocketFrame`]s both sides exchange on it;
//! - the actor inbox: a socket Open is an [`Invocation`] whose reply is the
//!   actor's [`GuestEventResult`], and every later socket event is an
//!   ordinary inbox record marked [`SOCKET_RECORD`]. Guests read both through
//!   [`Delivery::socket`], so a host never needs a second delivery path.
use crate::addressing::ActorReference;
use crate::guest::{
    Delivery, GuestError, GuestEventResult, GuestSocketEvent, SocketCloseInitiator, SocketPayload,
    SocketTransport, Trigger,
};
use crate::invocation::Invocation;
use acyclic_stream::{AppendRecord, Attribute, RecordHeaders};
use bytes::Bytes;
use prost::Message as _;

/// Record attribute marking a socket event in an actor inbox; its value is `1`.
pub const SOCKET_RECORD: &str = "acyclic.actors.socket";
const VERSION: &[u8] = b"1";
/// Invocation operations that open a socket start with this prefix.
const OPEN_OPERATION: &[u8] = b"acyclic.actors.socket.open/";

#[derive(Clone, PartialEq, prost::Message)]
struct HeaderWire {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct OpenWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    actor: Bytes,
    #[prost(string, tag = "3")]
    subscription: String,
    #[prost(string, optional, tag = "4")]
    app_channel: Option<String>,
    #[prost(string, repeated, tag = "5")]
    subprotocols: Vec<String>,
    #[prost(message, repeated, tag = "6")]
    headers: Vec<HeaderWire>,
    #[prost(uint64, tag = "7")]
    resume_after: u64,
    #[prost(bool, tag = "8")]
    reconnected: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CloseWire {
    #[prost(uint32, tag = "1")]
    code: u32,
    #[prost(string, tag = "2")]
    reason: String,
    /// 1 peer, 2 actor, 3 runtime; zero on a peer's own close request.
    #[prost(uint32, tag = "3")]
    initiator: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AcceptedWire {
    #[prost(string, optional, tag = "1")]
    subprotocol: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RejectedWire {
    #[prost(uint32, tag = "1")]
    status: u32,
    #[prost(string, tag = "2")]
    reason: String,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum FrameKind {
    #[prost(string, tag = "1")]
    Text(String),
    #[prost(bytes = "bytes", tag = "2")]
    Binary(Bytes),
    #[prost(message, tag = "3")]
    Close(CloseWire),
    #[prost(message, tag = "4")]
    Accepted(AcceptedWire),
    #[prost(message, tag = "5")]
    Rejected(RejectedWire),
}

#[derive(Clone, PartialEq, prost::Message)]
struct FrameWire {
    #[prost(oneof = "FrameKind", tags = "1, 2, 3, 4, 5")]
    kind: Option<FrameKind>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ErrorWire {
    #[prost(string, tag = "1")]
    code: String,
    #[prost(string, tag = "2")]
    message: String,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum InputKind {
    #[prost(message, tag = "2")]
    Open(OpenWire),
    #[prost(string, tag = "3")]
    Text(String),
    #[prost(bytes = "bytes", tag = "4")]
    Binary(Bytes),
    #[prost(message, tag = "5")]
    Close(CloseWire),
    #[prost(message, tag = "6")]
    Error(ErrorWire),
}

#[derive(Clone, PartialEq, prost::Message)]
struct InputWire {
    #[prost(string, tag = "1")]
    connection: String,
    #[prost(oneof = "InputKind", tags = "2, 3, 4, 5, 6")]
    kind: Option<InputKind>,
}

/// A peer's request to open a socket to one actor. Credentials travel on the
/// session, never in this request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketOpen {
    /// Target actor.
    pub actor: ActorReference,
    /// The actor's declared subscription that receives this socket's events.
    pub subscription: String,
    /// Optional application channel.
    pub app_channel: Option<String>,
    /// Offered subprotocols.
    pub subprotocols: Vec<String>,
    /// Host-allowlisted ordered headers, without credentials.
    pub headers: Vec<(String, String)>,
    /// Last applied actor offset the peer observed (zero before first delivery).
    pub resume_after: u64,
    /// Whether this open completes a reconnect of an earlier socket.
    pub reconnected: bool,
}

impl SocketOpen {
    fn wire(&self) -> OpenWire {
        OpenWire {
            version: 1,
            actor: self.actor.encode().into(),
            subscription: self.subscription.clone(),
            app_channel: self.app_channel.clone(),
            subprotocols: self.subprotocols.clone(),
            headers: self
                .headers
                .iter()
                .map(|(name, value)| HeaderWire {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            resume_after: self.resume_after,
            reconnected: self.reconnected,
        }
    }

    fn from_wire(wire: OpenWire) -> Result<Self, GuestError> {
        if wire.version != 1 || wire.subscription.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            actor: ActorReference::decode(&wire.actor).map_err(|_| invalid())?,
            subscription: wire.subscription,
            app_channel: wire.app_channel,
            subprotocols: wire.subprotocols,
            headers: wire
                .headers
                .into_iter()
                .map(|header| (header.name, header.value))
                .collect(),
            resume_after: wire.resume_after,
            reconnected: wire.reconnected,
        })
    }

    /// Canonical session encoding.
    pub fn encode(&self) -> Bytes {
        self.wire().encode_to_vec().into()
    }

    /// Decode only the canonical encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self, GuestError> {
        let wire = OpenWire::decode(bytes).map_err(|_| invalid())?;
        if wire.encode_to_vec().as_slice() != bytes {
            return Err(invalid());
        }
        Self::from_wire(wire)
    }

    /// The canonical resource a session admits as `actors/invoke`: the
    /// actor namespace, exactly as for an invocation.
    pub fn resource(&self) -> String {
        format!(
            "actors:/{}",
            acyclic_auth::encode_component(self.actor.namespace().as_str().as_bytes())
        )
    }
}

/// One frame on a socket channel, in either direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocketFrame {
    /// An application frame.
    Message(SocketPayload),
    /// From the peer: its close request. From the host: the final close.
    Close {
        /// WebSocket close code.
        code: u16,
        /// Close reason.
        reason: String,
        /// The closing party; the host ignores it on a peer's request.
        initiator: SocketCloseInitiator,
    },
    /// Host only: the actor accepted the open.
    Accepted {
        /// Selected subprotocol.
        subprotocol: Option<String>,
    },
    /// Host only: the actor rejected the open; nothing follows.
    Rejected {
        /// HTTP rejection status.
        status: u16,
        /// Rejection reason.
        reason: String,
    },
}

fn initiator_code(initiator: &SocketCloseInitiator) -> u32 {
    match initiator {
        SocketCloseInitiator::Peer => 1,
        SocketCloseInitiator::Actor => 2,
        SocketCloseInitiator::Runtime => 3,
    }
}

fn initiator_from(code: u32) -> Result<SocketCloseInitiator, GuestError> {
    match code {
        // A peer's own request carries no initiator; the host names it.
        0 | 1 => Ok(SocketCloseInitiator::Peer),
        2 => Ok(SocketCloseInitiator::Actor),
        3 => Ok(SocketCloseInitiator::Runtime),
        _ => Err(invalid()),
    }
}

fn close_wire(code: u16, reason: &str, initiator: &SocketCloseInitiator) -> CloseWire {
    CloseWire {
        code: code.into(),
        reason: reason.into(),
        initiator: initiator_code(initiator),
    }
}

fn close_code(code: u32) -> Result<u16, GuestError> {
    u16::try_from(code).map_err(|_| invalid())
}

impl SocketFrame {
    /// Canonical channel encoding.
    pub fn encode(&self) -> Bytes {
        let kind = match self {
            Self::Message(SocketPayload::Text { text }) => FrameKind::Text(text.clone()),
            Self::Message(SocketPayload::Binary { binary }) => FrameKind::Binary(binary.clone()),
            Self::Close {
                code,
                reason,
                initiator,
            } => FrameKind::Close(close_wire(*code, reason, initiator)),
            Self::Accepted { subprotocol } => FrameKind::Accepted(AcceptedWire {
                subprotocol: subprotocol.clone(),
            }),
            Self::Rejected { status, reason } => FrameKind::Rejected(RejectedWire {
                status: (*status).into(),
                reason: reason.clone(),
            }),
        };
        FrameWire { kind: Some(kind) }.encode_to_vec().into()
    }

    /// Decode only the canonical encoding.
    pub fn decode(bytes: &[u8]) -> Result<Self, GuestError> {
        let wire = FrameWire::decode(bytes).map_err(|_| invalid())?;
        if wire.encode_to_vec().as_slice() != bytes {
            return Err(invalid());
        }
        Ok(match wire.kind.ok_or_else(invalid)? {
            FrameKind::Text(text) => Self::Message(SocketPayload::Text { text }),
            FrameKind::Binary(binary) => Self::Message(SocketPayload::Binary { binary }),
            FrameKind::Close(close) => Self::Close {
                code: close_code(close.code)?,
                reason: close.reason,
                initiator: initiator_from(close.initiator)?,
            },
            FrameKind::Accepted(accepted) => Self::Accepted {
                subprotocol: accepted.subprotocol,
            },
            FrameKind::Rejected(rejected) => Self::Rejected {
                status: u16::try_from(rejected.status).map_err(|_| invalid())?,
                reason: rejected.reason,
            },
        })
    }
}

/// One socket event in an actor inbox, before the host appends it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SocketInput {
    Message(SocketPayload),
    Close {
        code: u16,
        reason: String,
        initiator: SocketCloseInitiator,
    },
}

/// The open invocation for `connection`: its reply is the actor's verdict.
pub(crate) fn open_invocation(
    connection: &str,
    open: &SocketOpen,
) -> Result<Invocation, GuestError> {
    let mut operation = OPEN_OPERATION.to_vec();
    operation.extend_from_slice(connection.as_bytes());
    Invocation::new(
        open.actor.clone(),
        open.subscription.clone(),
        operation.into(),
        InputWire {
            connection: connection.into(),
            kind: Some(InputKind::Open(open.wire())),
        }
        .encode_to_vec()
        .into(),
    )
}

/// One marked inbox record for a socket event after Open.
pub(crate) fn input_record(
    connection: &str,
    input: &SocketInput,
) -> Result<AppendRecord, GuestError> {
    let kind = match input {
        SocketInput::Message(SocketPayload::Text { text }) => InputKind::Text(text.clone()),
        SocketInput::Message(SocketPayload::Binary { binary }) => InputKind::Binary(binary.clone()),
        SocketInput::Close {
            code,
            reason,
            initiator,
        } => InputKind::Close(close_wire(*code, reason, initiator)),
    };
    Ok(AppendRecord {
        value: InputWire {
            connection: connection.into(),
            kind: Some(kind),
        }
        .encode_to_vec()
        .into(),
        headers: RecordHeaders::new(
            Vec::new(),
            vec![Attribute {
                name: SOCKET_RECORD.into(),
                value: Bytes::from_static(VERSION).into(),
            }],
        )
        .map_err(|_| invalid())?,
    })
}

/// The kind of one marked inbox value, for tests that audit an inbox.
#[cfg(test)]
pub(crate) fn input_kind(value: &[u8]) -> Option<&'static str> {
    Some(match decode_input(value).ok()?.kind? {
        InputKind::Open(_) => "open",
        InputKind::Text(_) | InputKind::Binary(_) => "message",
        InputKind::Close(_) => "close",
        InputKind::Error(_) => "error",
    })
}

/// A socket event delivered to the actor through its inbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketDelivery {
    /// Runtime-owned connection identity.
    pub connection_id: String,
    /// The event, with offsets taken from the delivered record.
    pub event: GuestSocketEvent,
}

fn decode_input(bytes: &[u8]) -> Result<InputWire, GuestError> {
    let wire = InputWire::decode(bytes).map_err(|_| invalid())?;
    if wire.encode_to_vec().as_slice() != bytes || wire.connection.is_empty() {
        return Err(invalid());
    }
    Ok(wire)
}

impl Delivery {
    /// The socket event this delivery carries, if any. An Open arrives as an
    /// invocation; answer it with [`GuestEventResult::SocketAccept`] or
    /// [`GuestEventResult::SocketReject`] encoded by [`socket_reply`] as the
    /// transition's response. Every delivered Open is followed by exactly one
    /// Close, after which no event for that connection arrives.
    pub fn socket(&self) -> Result<Option<SocketDelivery>, GuestError> {
        let Some(record) = &self.record else {
            return Ok(None);
        };
        if let Some(invocation) = self.invocation()? {
            let Some(connection) = invocation.operation().strip_prefix(OPEN_OPERATION) else {
                return Ok(None);
            };
            let wire = decode_input(invocation.request())?;
            let (Some(InputKind::Open(open)), true) =
                (wire.kind, wire.connection.as_bytes() == connection)
            else {
                return Err(invalid());
            };
            let open = SocketOpen::from_wire(open)?;
            if open.actor != self.actor {
                return Err(invalid());
            }
            return Ok(Some(SocketDelivery {
                connection_id: wire.connection,
                event: GuestSocketEvent::Open {
                    actor_id: open.actor.id().into(),
                    app_channel: open.app_channel,
                    subprotocols: open.subprotocols,
                    headers: open.headers,
                    resume_after: open.resume_after.to_string(),
                    reconnected: open.reconnected,
                    transport: Some(SocketTransport::Internal),
                },
            }));
        }
        let attributes = record.headers.attributes();
        let Ok(index) = attributes.binary_search_by(|attribute| attribute.name.cmp(SOCKET_RECORD))
        else {
            return Ok(None);
        };
        if attributes.get(index).ok_or_else(invalid)?.value.as_ref() != VERSION
            || !matches!(&self.trigger, Trigger::Record { sequence, .. } if *sequence == record.sequence)
        {
            return Err(invalid());
        }
        let wire = decode_input(&record.value)?;
        let offset = record
            .sequence
            .checked_add(1)
            .ok_or_else(invalid)?
            .to_string();
        let event = match wire.kind.ok_or_else(invalid)? {
            InputKind::Text(text) => GuestSocketEvent::Message {
                offset,
                payload: SocketPayload::Text { text },
            },
            InputKind::Binary(binary) => GuestSocketEvent::Message {
                offset,
                payload: SocketPayload::Binary { binary },
            },
            InputKind::Close(close) => GuestSocketEvent::Close {
                offset,
                code: close_code(close.code)?,
                reason: close.reason,
                initiator: initiator_from(close.initiator)?,
            },
            InputKind::Error(error) => GuestSocketEvent::Error {
                offset,
                code: error.code,
                message: error.message,
            },
            InputKind::Open(_) => return Err(invalid()),
        };
        Ok(Some(SocketDelivery {
            connection_id: wire.connection,
            event,
        }))
    }
}

/// The transition response that answers a socket Open.
pub fn socket_reply(result: &GuestEventResult) -> Result<Bytes, GuestError> {
    result.validate()?;
    serde_json::to_vec(result)
        .map(Bytes::from)
        .map_err(|error| GuestError::Encode(error.to_string()))
}

/// The actor's validated answer to `open`. An accept may select only a
/// subprotocol the peer offered, or none.
pub fn socket_verdict(reply: &[u8], open: &SocketOpen) -> Result<GuestEventResult, GuestError> {
    let result: GuestEventResult =
        serde_json::from_slice(reply).map_err(|error| GuestError::Decode(error.to_string()))?;
    result.validate()?;
    if let GuestEventResult::SocketAccept {
        subprotocol: Some(selected),
        ..
    } = &result
        && !open.subprotocols.contains(selected)
    {
        return Err(GuestError::Handler(
            "socket accept selected a subprotocol the peer did not offer".into(),
        ));
    }
    Ok(result)
}

fn invalid() -> GuestError {
    GuestError::Decode("invalid socket record".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_and_opens_round_trip_and_reject_noncanonical_bytes() -> Result<(), GuestError> {
        let open = SocketOpen {
            actor: ActorReference::new(b"account", b"targets", b"laptop").map_err(|_| invalid())?,
            subscription: "sockets".into(),
            app_channel: Some("presence".into()),
            subprotocols: vec!["v1".into()],
            headers: vec![("x-device".into(), "windows".into())],
            resume_after: 7,
            reconnected: true,
        };
        assert_eq!(SocketOpen::decode(&open.encode())?, open);
        assert_eq!(open.resource(), "actors:/targets");
        for frame in [
            SocketFrame::Message(SocketPayload::Text {
                text: "ping".into(),
            }),
            SocketFrame::Message(SocketPayload::Binary {
                binary: Bytes::from_static(b"\0\x01"),
            }),
            SocketFrame::Close {
                code: 1000,
                reason: "bye".into(),
                initiator: SocketCloseInitiator::Runtime,
            },
            SocketFrame::Accepted { subprotocol: None },
            SocketFrame::Rejected {
                status: 403,
                reason: "no".into(),
            },
        ] {
            assert_eq!(SocketFrame::decode(&frame.encode())?, frame);
        }
        // Negative controls: an empty frame, an out-of-range close code and
        // trailing bytes all fail instead of decoding to a default.
        assert!(SocketFrame::decode(&[]).is_err());
        let wide = FrameWire {
            kind: Some(FrameKind::Close(CloseWire {
                code: 70_000,
                reason: String::new(),
                initiator: 1,
            })),
        };
        assert!(SocketFrame::decode(&wide.encode_to_vec()).is_err());
        let mut trailing = open.encode().to_vec();
        trailing.extend_from_slice(&open.encode());
        assert!(SocketOpen::decode(&trailing).is_err());
        Ok(())
    }

    #[test]
    fn a_verdict_selects_only_an_offered_subprotocol() -> Result<(), GuestError> {
        let open = SocketOpen {
            actor: ActorReference::new(b"account", b"targets", b"laptop").map_err(|_| invalid())?,
            subscription: "sockets".into(),
            app_channel: None,
            subprotocols: vec!["v1".into()],
            headers: Vec::new(),
            resume_after: 0,
            reconnected: false,
        };
        let accept = |subprotocol: Option<&str>| {
            socket_reply(&GuestEventResult::SocketAccept {
                subprotocol: subprotocol.map(Into::into),
                attachment: Bytes::new(),
                tags: Vec::new(),
            })
        };
        assert!(socket_verdict(&accept(Some("v1"))?, &open).is_ok());
        assert!(socket_verdict(&accept(None)?, &open).is_ok());
        // Negative control: a protocol the peer never offered.
        assert!(socket_verdict(&accept(Some("v2"))?, &open).is_err());
        Ok(())
    }
}
