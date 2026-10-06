//! Rust-owned WebAssembly bindings for the canonical Actors v1 contract.
//!
//! The browser ABI carries the protobuf bytes generated from the same Rust
//! wire declarations used by native clients. Transport selection and request
//! validation remain inside Rust; the JavaScript adapter only receives typed
//! operation methods and a cancellation handle.

pub use acyclic_actors::wire;

/// Maximum encoded message accepted by this boundary.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[cfg(not(target_arch = "wasm32"))]
pub mod native;

#[cfg(target_arch = "wasm32")]
mod wasm;
