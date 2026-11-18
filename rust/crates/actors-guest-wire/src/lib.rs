//! Portable Actors guest socket messages shared by host adapters and code generation.

mod body;
pub use body::{
    GuestBody, GuestStreamingRequest, GuestStreamingResponse, JAVASCRIPT_FETCH_RESPONSE,
    streaming_request,
};
mod events;
pub use events::{GuestEvent, GuestRequest, GuestResponse};
mod codec;
/// Live-update compatibility declarations.
pub mod compatibility;
pub use codec::{Checkpoint, CodecIdentity, GuestError};
/// Portable live-update selection contract.
pub mod live_selection;
mod sockets;
mod state_packets;
use serde::{Deserialize, Serialize};
pub use sockets::{
    GuestEventResult, GuestSocketEvent, SocketCloseInitiator, SocketPayload, SocketTransport,
};
pub use state_packets::{ActorMetadataWrite, ActorStateAlarm, ActorStateCommit, ActorStateSeed};
use ts_rs::TS;

/// Runtime-owned hibernatable socket identity, unrelated to the actor's identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TS)]
pub struct WebSocketId(pub String);
/// A request/response pair answered by the host without waking the object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TS)]
pub struct WebSocketAutoResponse {
    /// Exact incoming text frame.
    pub request: String,
    /// Exact outgoing text frame.
    pub response: String,
}

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

/// Canonical environment property grammar for bindings and secret selections.
pub fn valid_environment_name(value: &str) -> bool {
    value.len() <= 63
        && value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Frozen guest ABI version, independent of artifact codec versions.
pub const GUEST_ABI_VERSION: u32 = 1;

/// Render the portable guest ABI declarations before the host authoring types.
pub fn typescript(config: &ts_rs::Config) -> String {
    let mut output = format!(
        "// Generated from acyclic_actors::guest. Do not edit.\nexport const GUEST_ABI_VERSION = {GUEST_ABI_VERSION} as const;\n"
    );
    macro_rules! declarations {
        ($($ty:ty),+ $(,)?) => {$(
            output.push_str("export ");
            output.push_str(&<$ty as TS>::decl(config));
            output.push('\n');
        )+};
    }
    output.push_str(JAVASCRIPT_FETCH_RESPONSE);
    declarations!(
        WebSocketId,
        WebSocketAutoResponse,
        SocketCommand,
        SocketReply,
        GuestRequest,
        GuestResponse,
        SocketPayload,
        SocketCloseInitiator,
        SocketTransport,
        GuestSocketEvent,
        GuestEvent,
        GuestEventResult,
        CodecIdentity,
        Checkpoint,
        GuestError,
    );
    output
}
