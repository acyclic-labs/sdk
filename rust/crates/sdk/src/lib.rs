#![doc = include_str!("../README.md")]

//! Unified namespace for the public Rust SDK families.
//!
//! This crate intentionally re-exports only public, independently maintained
//! family crates. Internal generators, qualification fixtures, and prototype
//! bindings are not part of the installable SDK surface.

/// Actors v1 remote contract and client.
pub use acyclic_actors as actors;
/// Versioned workspace and embedded filesystem APIs.
pub use acyclic_fs as filesystem;
/// Durable agent runtime and embedded harness APIs.
pub use acyclic_harness as harness;
/// Immutable inference contexts and recoverable runs.
pub use acyclic_inference as inference;
/// Machines contract, client, and deterministic simulator.
pub use acyclic_machines as machines;
/// Logical Objects v2 contract and clients.
pub use acyclic_objects as objects;
/// Hierarchical append-only Streams contract and clients.
pub use acyclic_stream as stream;
/// Workers v1 remote contract and client.
pub use acyclic_workers as workers;

/// Version of the unified facade and its public family crates.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_every_public_family() {
        let _ = actors::connect;
        let _ = filesystem::FILE_DESCRIPTOR_SET;
        let _ = harness::FILE_DESCRIPTOR_SET;
        let _ = inference::MAXIMUM_MESSAGE_BYTES;
        let _ = machines::FILE_DESCRIPTOR_SET;
        let _ = objects::connect;
        let _ = stream::connect;
        let _ = workers::connect;
        assert_eq!(SDK_VERSION, "0.2.0");
    }
}
