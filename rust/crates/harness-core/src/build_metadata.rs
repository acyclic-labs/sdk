//! The only library seam for compile-time Cargo metadata.
#![expect(
    clippy::disallowed_macros,
    reason = "explicit Cargo metadata only; user environment discovery is forbidden"
)]

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
pub(crate) const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const DESCRIPTOR_DIGEST: &str = env!("HARNESS_DESCRIPTOR_DIGEST");
pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/harness_descriptor.bin"));

/// Generated Protobuf packages, nested as their package names are, so the harness messages resolve the shared protocol handshake they import.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::large_enum_variant,
    reason = "generated prost bindings"
)]
pub(crate) mod generated {
    pub mod acyclic {
        pub mod harness {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v2.rs"));
            }
        }
        pub mod protocol {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.protocol.v1.rs"));
            }
        }
    }
}

pub(crate) const MCP_INITIALIZATION: &str = concat!(
    "{\"capabilities\":{},\"clientInfo\":{\"name\":\"acyclic-harness\",\"version\":\"",
    env!("CARGO_PKG_VERSION"),
    "\"},\"protocolVersion\":\"2025-11-25\"}",
);
