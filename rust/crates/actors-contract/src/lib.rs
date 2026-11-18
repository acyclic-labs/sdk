#![doc = include_str!("../README.md")]

#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

use acyclic_actors_call::call;
/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;

/// Compiler-linked public method admission.
pub mod authorization {
    use acyclic_auth::action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/authorization.rs"));
}
pub use authorization::{AUTHORIZATION, ActorsMethod};

/// Worker methods served by the same canonical Actors service.
pub mod workers_authorization {
    use acyclic_auth::action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/workers_authorization.rs"));
}

/// Rust-owned Actors contract declarations and schema renderer.
pub mod contract;
pub use contract::{
    ContractError, domain, validate_add_subscription, validate_delete, validate_deployment,
    validate_fork, validate_remove_subscription, validate_resume_subscription, validate_update,
};

/// Rust-owned guest ABI and typed authoring hooks.
pub mod guest;
/// Inbox invocation protocol shared by native and hosted actors.
pub mod invocation;
/// Deployment versions, cohort selection, call provenance and durable handoff.
pub mod live_update;
/// Actor sockets v1 session channel frames and inbox socket events.
pub mod sockets;

/// Rust-owned Actors v1 wire types and schema metadata.
#[allow(
    clippy::allow_attributes_without_reason,
    dead_code,
    missing_docs,
    reason = "tonic emits this module and its generated server dispatch code"
)]
pub mod wire;

/// Canonical version-one descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-actors-v1.bin"));
