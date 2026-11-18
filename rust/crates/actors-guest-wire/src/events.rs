//! Portable guest HTTP messages and event delivery.

use crate::GuestSocketEvent;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

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
    /// Actor-scoped durable alarm delivered through the activation gate.
    Alarm,
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
