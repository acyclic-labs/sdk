use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// One application frame; protocol ping/pong and auto-response never reach guests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(untagged)]
pub enum SocketPayload {
    /// UTF-8 text frame.
    Text {
        /// Text content.
        text: String,
    },
    /// Binary frame.
    Binary {
        /// Binary content.
        #[ts(type = "Uint8Array")]
        binary: Bytes,
    },
}

/// Party that finally closed a socket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum SocketCloseInitiator {
    /// Remote application.
    Peer,
    /// Guest effect.
    Actor,
    /// Node timeout, backpressure or drain.
    Runtime,
}

/// Diagnostic transport; apps use the same API for either transport.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum SocketTransport {
    /// Platform virtual channel.
    Internal,
    /// Runtime-held real socket.
    External,
}

/// App-only lifecycle projection of the agreed Actors socket contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[non_exhaustive]
pub enum GuestSocketEvent {
    /// Open is a request; answer with SocketAccept or SocketReject.
    Open {
        /// Target actor; connection identity is on the enclosing socket event.
        actor_id: String,
        /// Optional application channel.
        app_channel: Option<String>,
        /// Offered subprotocols.
        subprotocols: Vec<String>,
        /// Host-allowlisted ordered headers, without credentials.
        headers: Vec<(String, String)>,
        /// Last applied actor offset, decimal u64 (zero before first delivery).
        resume_after: String,
        /// Whether a declared outbound reconnect completed.
        #[serde(default)]
        reconnected: bool,
        /// Optional transport diagnostics.
        transport: Option<SocketTransport>,
    },
    /// One ordered application frame.
    Message {
        /// Decimal positive u64 actor delivery offset.
        offset: String,
        /// Text or binary payload.
        payload: SocketPayload,
    },
    /// Final close; close and error participate in the delivery offset sequence.
    Close {
        /// Decimal positive u64 actor delivery offset.
        offset: String,
        /// WebSocket close code.
        code: u16,
        /// Close reason.
        reason: String,
        /// Closing party, including runtime 1013 backpressure close.
        initiator: SocketCloseInitiator,
    },
    /// Ordered runtime failure.
    Error {
        /// Decimal positive u64 actor delivery offset.
        offset: String,
        /// Stable error code.
        code: String,
        /// Diagnostic message without credentials.
        message: String,
    },
    /// Declared outbound backoff is in progress; this is not a final close.
    Reconnecting {
        /// One-based attempt number.
        attempt: u32,
        /// Decimal u64 backoff delay in milliseconds.
        delay_ms: String,
    },
    /// Additive future host event; older guests may ignore it.
    #[serde(other)]
    Unknown,
}

/// Typed event result, committed with state and the host's effect outbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum GuestEventResult {
    /// Public Invoke response bytes.
    Reply {
        /// Opaque result.
        #[ts(type = "Uint8Array")]
        payload: Bytes,
    },
    /// Admit the pending SocketOpen with bounded attachment and tags.
    SocketAccept {
        /// Selected offered subprotocol.
        subprotocol: Option<String>,
        /// Opaque checkpointed attachment (contract maximum 2 KiB).
        #[ts(type = "Uint8Array")]
        attachment: Bytes,
        /// Checkpointed broadcast tags (at most eight, each at most 64 UTF-8 bytes).
        tags: Vec<String>,
    },
    /// Reject the pending SocketOpen without installing a connection.
    SocketReject {
        /// HTTP rejection status.
        status: u16,
        /// Rejection reason.
        reason: String,
    },
}
