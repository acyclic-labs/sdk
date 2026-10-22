#![deny(unsafe_code)]
//! Filesystem wire messages, services and canonical protocol descriptor.

/// Generated public gRPC schema and client/server bindings.
#[allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "tonic-build generated code"
)]
pub mod wire {
    /// Protocol negotiation shared by every Acyclic service family.
    pub mod protocol {
        /// Version 1 of the shared handshake.
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.protocol.v1.rs"));
        }
    }

    /// Filesystem messages and service definitions.
    pub mod filesystem {
        /// Version 2 of the public Filesystem contract.
        pub mod v2 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.filesystem.v2.rs"));
        }
    }
}

/// Canonical public descriptor set used by compatibility and conformance gates.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-filesystem-v2.bin"));

#[cfg(test)]
mod tests {
    use super::{FILE_DESCRIPTOR_SET, wire};
    use prost::Message as _;

    #[test]
    fn imported_handshake_round_trips_without_the_engine() -> Result<(), prost::DecodeError> {
        let request = wire::filesystem::v2::HandshakeRequest {
            protocol: Some(wire::protocol::v1::HandshakeRequest {
                protocol: Some(wire::protocol::v1::ProtocolIdentity {
                    version: "filesystem-v2".to_owned(),
                    descriptor_digest: "fixture-digest".to_owned(),
                }),
                required: None,
            }),
        };
        assert_eq!(
            wire::filesystem::v2::HandshakeRequest::decode(request.encode_to_vec().as_slice())?,
            request
        );
        Ok(())
    }

    #[test]
    fn descriptor_contains_service_and_import_closure() -> Result<(), prost::DecodeError> {
        let descriptor = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)?;
        assert!(descriptor.file.iter().any(|file| {
            file.package.as_deref() == Some("acyclic.filesystem.v2")
                && file
                    .service
                    .iter()
                    .any(|service| service.name.as_deref() == Some("FilesystemService"))
        }));
        for package in [
            "acyclic.protocol.v1",
            "acyclic.validation.v1",
            "google.protobuf",
        ] {
            assert!(
                descriptor
                    .file
                    .iter()
                    .any(|file| file.package.as_deref() == Some(package))
            );
        }
        Ok(())
    }
}
