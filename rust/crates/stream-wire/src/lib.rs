//! Canonical Stream protobuf messages, services and RPC authorization.

pub mod authorization;

/// Generated canonical Stream v1 protocol.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    // All session data variants have total equality, including repeated headers.
    impl Eq for SessionData {}
    include!(concat!(env!("OUT_DIR"), "/acyclic.stream.v1.rs"));
}
/// Canonical public descriptor set used by compatibility gates.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-stream-v1.bin"));

/// Compiler-linked admission for the unary Stream service.
pub mod rpc {
    use acyclic_auth_action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/authorization.rs"));
}
