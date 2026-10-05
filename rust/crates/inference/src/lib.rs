#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
#![doc = include_str!("../docs/guide.md")]

/// Generated customer protobuf contract shared by host and WebAssembly.
#[allow(missing_docs, unused_qualifications, clippy::all, clippy::pedantic)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/inference.customer.v1.rs"));
}

mod contract;
#[cfg(feature = "http-codec")]
/// Canonical JSON framing and validation for inference requests and results.
pub mod http_codec;
#[cfg(feature = "http-client")]
/// HTTP transport selected when a service exposes the inference HTTP endpoint.
pub mod http;
#[cfg(feature = "http-client")]
/// Transport-neutral inference client and best-transport connection helper.
pub mod client;
pub use contract::{
    MAXIMUM_EVALUATION_CANDIDATES, MAXIMUM_EVALUATION_CASES, MAXIMUM_EVALUATION_METRICS,
    MAXIMUM_EVALUATION_RESULTS, MAXIMUM_HTTP_JSON_BYTES, MAXIMUM_MESSAGE_BYTES, WatchRunState,
    validate_customer_wire, watch_run_start_state_wire, watch_run_start_wire,
};

#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
mod host;
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
pub use host::*;

#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
mod grpc;

/// Generated independent negotiation bindings for native transport selection.
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
pub mod control_wire {
    #![allow(missing_docs, clippy::all, clippy::pedantic, reason = "generated control bindings")]
    pub mod protocol {
        /// Version-one protocol identity and capability messages.
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.protocol.v1.rs"));
        }
    }
    pub mod transport {
        /// Version-one handshake service bindings.
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.transport.v1.rs"));
        }
    }
}
