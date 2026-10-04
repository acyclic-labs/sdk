#![doc = include_str!("../README.md")]
#![doc = include_str!("../docs/guide.md")]
mod body;
pub mod client;
pub mod v2;
#[cfg(all(not(target_arch = "wasm32"), feature = "grpc"))]
pub mod control_wire {
    #![allow(
        missing_docs,
        clippy::all,
        clippy::pedantic,
        reason = "generated control bindings"
    )]
    pub mod protocol {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/control/acyclic.protocol.v1.rs"));
        }
    }
    pub mod transport {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/control/acyclic.transport.v1.rs"));
        }
    }
}
/// Canonical logical Objects v2 public contracts and providers.
pub use v2::*;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod local_options;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod physical;
pub use client::{Client, ConnectError, DEFAULT_HTTP_RESPONSE_BYTES, DEFAULT_TRANSPORT, connect};
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use local_options::{LocalDurability, LocalObjectsGarbageCollection, LocalObjectsLimits};
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub use v2::grpc::GrpcObjects;
#[cfg(feature = "http")]
pub use v2::http::HttpObjects;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use v2::local::{LocalObjects, LocalOpenError};
