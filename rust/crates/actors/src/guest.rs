//! Guest ABI v1. Artifacts own their codec; hosts own admission and publication.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ts_rs::{Config, TS};

mod authoring;
pub use authoring::*;
mod update;
pub use update::*;
mod runtime;
pub use runtime::*;
mod sockets;
pub use sockets::*;
mod body;
pub use body::*;
mod state;
pub use state::GuestState;

/// Required named export and its lifecycle hooks; HTTP enters through default.fetch.
pub const ACTOR_EXPORT: &str = "actor";
/// Hooks on the actor export, in lifecycle order.
pub const ACTOR_HOOKS: &[&str] = &["restore", "event", "checkpoint"];
/// HTTP hook on the default export.
pub const FETCH_EXPORT: &str = "fetch";
/// Frozen guest ABI version, independent of artifact codec versions.
pub const GUEST_ABI_VERSION: u32 = 1;
/// Artifact version handshake export, checked before guest restore or delivery.
pub const ABI_VERSION_EXPORT: &str = "abiVersion";
/// Artifact codec export, inspected before attempting an update.
pub const CODEC_EXPORT: &str = "codec";

/// Admit only explicitly supported artifact ABI versions before touching its heap.
pub fn negotiate_abi(version: u32) -> Result<(), GuestError> {
    if version == GUEST_ABI_VERSION {
        Ok(())
    } else {
        Err(GuestError::UnsupportedAbi(version))
    }
}

/// Portable HTTP input; header order and duplicate header names are preserved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GuestRequest {
    /// Original absolute URL.
    pub url: String,
    /// HTTP method.
    pub method: String,
    /// Ordered header pairs.
    pub headers: Vec<(String, String)>,
    /// Bounded request body bytes.
    #[ts(type = "Uint8Array")]
    pub body: Bytes,
}

/// Portable HTTP output, adapted by the host to its HTTP implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GuestResponse {
    /// HTTP status code.
    pub status: u16,
    /// Ordered header pairs.
    pub headers: Vec<(String, String)>,
    /// Bounded response body bytes.
    #[ts(type = "Uint8Array")]
    pub body: Bytes,
}

/// The single event union. Each delivered event has a stable context deliveryId.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[non_exhaustive]
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
        #[ts(type = "Uint8Array")]
        payload: Bytes,
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
    /// Future host event; ignore it when the negotiated ABI does not define it.
    #[serde(other)]
    Unknown,
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
    let mut output = format!(
        "// Generated from acyclic_actors::guest. Do not edit.\nexport const GUEST_ABI_VERSION = {GUEST_ABI_VERSION} as const;\n"
    );
    macro_rules! declarations {
        ($($ty:ty),+ $(,)?) => {$(
            output.push_str("export ");
            output.push_str(&<$ty as TS>::decl(&config));
            output.push('\n');
        )+};
    }
    declarations!(GuestRequest, GuestResponse, SocketPayload, SocketCloseInitiator,
        SocketTransport, GuestSocketEvent, GuestEvent, GuestEventResult,
        CodecIdentity, Checkpoint, GuestError, CheckpointCodec<()>,
        CallerCapability, AdmittedCaller, RuntimeHandle, ActorRuntime,
        GuestState<()>, GuestContext<()>, ActorDefinition<(), ()>);
    output
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}
