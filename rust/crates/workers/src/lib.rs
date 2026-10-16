#![doc = include_str!("../README.md")]

/// Cloud stateless Worker deployment through Objects and Actors.
#[cfg(not(target_arch = "wasm32"))]
pub mod deployment;

/// Rust-owned deployment manifest, validation and language generation.
pub use acyclic_manifest as manifest;
