#![doc = include_str!("../README.md")]
/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;

/// Cloud stateless Worker deployment through Objects and Actors.
#[cfg(not(target_arch = "wasm32"))]
pub mod deployment;

/// Selected immutable entry publication and lazy loading over the original Objects provider.
pub use acyclic_manifest::entry;

/// Rust-owned deployment manifest, validation and language generation.
pub use acyclic_manifest as manifest;
pub mod service;
pub use service::{WorkerInvoker, WorkerServiceBinding};
pub mod bindings;
pub use bindings::{DeploymentBinding, materialize_bindings};
