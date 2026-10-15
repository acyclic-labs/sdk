//! Guest ABI v1. Artifacts own their codec; hosts own admission and publication.

use serde::{Deserialize, Serialize};
use ts_rs::{Config, TS};

mod authoring;
pub use authoring::*;
mod update;
pub use update::*;

/// Required named export and its lifecycle hooks; HTTP enters through default.fetch.
pub const ACTOR_EXPORT: &str = "actor";
/// Hooks on the actor export, in lifecycle order.
pub const ACTOR_HOOKS: &[&str] = &["restore", "event", "checkpoint"];
/// HTTP hook on the default export.
pub const FETCH_EXPORT: &str = "fetch";
/// Frozen guest ABI version, independent of artifact codec versions.
pub const GUEST_ABI_VERSION: u32 = 1;

/// Portable HTTP input; header order and duplicate header names are preserved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GuestRequest {
    /// Original absolute URL.
    pub url: String,
    /// HTTP method.
    pub method: String,
    /// Ordered header pairs.
    pub headers: Vec<(String, String)>,
    /// Bounded request body bytes.
    pub body: Vec<u8>,
}

/// Portable HTTP output, adapted by the host to its HTTP implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GuestResponse {
    /// HTTP status code.
    pub status: u16,
    /// Ordered header pairs.
    pub headers: Vec<(String, String)>,
    /// Bounded response body bytes.
    pub body: Vec<u8>,
}

/// App-only socket lifecycle. Harness packages never consume these events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum GuestSocketEvent {
    /// Connection admitted by the runtime.
    Open {
        /// Negotiated subprotocol.
        protocol: String,
    },
    /// One ordered frame.
    Message {
        /// Decimal u64 frame offset.
        offset: String,
        /// Frame bytes.
        data: Vec<u8>,
        /// Whether this is a text frame.
        text: bool,
    },
    /// Connection closed.
    Close {
        /// WebSocket close code.
        code: u16,
        /// Close reason.
        reason: String,
    },
    /// Runtime-reported connection failure.
    Error {
        /// Stable error code, without credentials.
        code: String,
    },
    /// Runtime is attempting the declared reconnect policy.
    Reconnecting,
}

/// The single event union. Each delivered event has a stable context deliveryId.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum GuestEvent {
    /// Stream subscription delivery; projection matches the Cloud bridge.
    Stream {
        /// Logical subscription ID.
        subscription: String,
        /// Source stream path.
        stream: String,
        /// Decimal u64 sequence.
        sequence: String,
        /// Decimal u64 publication time.
        committed_at_micros: String,
        /// Standard base64 record bytes.
        value: String,
        /// Lowercase hex commit ID.
        commit_id: String,
    },
    /// Public Invoke payload, distinct from HTTP fetch.
    Invoke {
        /// Opaque invocation input bytes.
        payload: Vec<u8>,
    },
    /// HTTP input, routed through default.fetch using the same context.
    Fetch {
        /// Portable request.
        request: GuestRequest,
    },
    /// A timer fire delivered by an idempotent inbox append, never Invoke.
    Timer {
        /// Timer identity, also the inbox append idempotency key.
        timer_id: String,
        /// Decimal u64 Unix deadline in milliseconds.
        deadline_unix_ms: String,
    },
    /// App-only socket delivery; socket authority remains in the socket contract.
    Socket {
        /// Runtime-owned connection identity.
        connection_id: String,
        /// Socket event.
        event: GuestSocketEvent,
    },
}

/// Derive an unambiguous effect retry key from a stable delivery and effect name.
/// Hosts must reuse delivery identity on retries; this promises no external atomicity.
pub fn retry_key(delivery_id: &str, effect: &str) -> Result<String, GuestError> {
    if delivery_id.is_empty() || effect.is_empty() {
        return Err(GuestError::MissingIdentity);
    }
    Ok(format!(
        "guest-v1:{}:{delivery_id}:{effect}",
        delivery_id.len()
    ))
}

/// Generate one self-contained authoring contract directly from Rust declarations.
pub fn typescript() -> String {
    let config = Config::default();
    let mut output = String::from("// Generated from acyclic_actors::guest. Do not edit.\n");
    macro_rules! declarations {
        ($($ty:ty),+ $(,)?) => {$(
            output.push_str("export ");
            output.push_str(&<$ty as TS>::decl(&config));
            output.push('\n');
        )+};
    }
    declarations!(GuestRequest, GuestResponse, GuestSocketEvent, GuestEvent,
        CodecIdentity, Checkpoint, GuestError, CheckpointCodec<()>,
        GuestContext<(), ()>, ActorDefinition<(), ()>);
    output
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}
