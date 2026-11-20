//! Harness type identity for the public messages and protocol handshake.
#![deny(unsafe_code)]

const PROTOCOL_VERSION: &str = "2";
const DESCRIPTOR_DIGEST: &str = env!("HARNESS_DESCRIPTOR_DIGEST");
const DESCRIPTOR_SET_BYTES: &[u8] =
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

/// Generated Protobuf envelopes shared by every transport: the harness contract and the protocol handshake it negotiates with.
pub mod wire {
    pub use super::generated::acyclic::harness::v2::*;
    pub use super::generated::acyclic::protocol::v1::*;
    pub use acyclic_machines::wire::selection::{
        ProviderRef, VolumeClass, VolumeOwner, VolumeRef, volume_owner,
    };
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] = DESCRIPTOR_SET_BYTES;

#[doc(hidden)]
pub const COMPILED_PROTOCOL: (&str, &str) = (PROTOCOL_VERSION, DESCRIPTOR_DIGEST);
#[doc(hidden)]
pub fn protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: COMPILED_PROTOCOL.0.into(),
        descriptor_digest: COMPILED_PROTOCOL.1.into(),
    }
}
