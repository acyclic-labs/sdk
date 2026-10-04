//! Non-mutating transport negotiation, separate from archived family contracts.
//!
//! This control service imports the unchanged shared handshake messages. Its
//! descriptor is generated independently and never enters a family's archived
//! runtime descriptor or changes an existing descriptor handshake identity.

use crate::{MethodSpec, ServiceSpec};
use prost::Message;
use prost_types::{
    FileDescriptorProto, FileDescriptorSet, FileOptions, MethodDescriptorProto,
    ServiceDescriptorProto,
};

/// File identity of the transport control plane.
pub const FILE_NAME: &str = "transport/v1/transport.proto";
/// Package identity of the independently versioned control plane.
pub const PACKAGE: &str = "acyclic.transport.v1";
/// gRPC path for an authenticated, non-mutating handshake.
pub const HANDSHAKE_RPC_PATH: &str = "/acyclic.transport.v1.ProtocolService/Handshake";
/// Metadata key selecting the family served by a shared endpoint.
pub const FAMILY_METADATA_KEY: &str = "acyclic-family";
const GO_PACKAGE: &str = "github.com/acyclic-labs/sdk/go/gen/transport/v1;transportv1";

/// One source for the control-plane operation and generated prose.
pub const HANDSHAKE_METHOD: MethodSpec = MethodSpec {
    name: "Handshake",
    input: "acyclic.protocol.v1.HandshakeRequest",
    output: "acyclic.protocol.v1.HandshakeResponse",
    docs: "Negotiate the selected family's protocol identity and supported capabilities before sending an application operation. This call cannot mutate application state.",
    client_streaming: false,
    server_streaming: false,
};
/// The control service is separate from every application service.
pub const CONTROL_SERVICE: ServiceSpec = ServiceSpec {
    name: "ProtocolService",
    methods: &[HANDSHAKE_METHOD],
};

/// Return the generated, non-mutating HTTP route for a registered family.
pub fn handshake_http_route(family: &str) -> Option<String> {
    crate::family_view(family).map(|view| format!("/v1/sdk/{}/handshake", view.name))
}

/// Build the descriptor directly from the Rust control-plane model.
pub fn control_file_descriptor() -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some(FILE_NAME.into()),
        package: Some(PACKAGE.into()),
        dependency: vec![crate::protocol::FILE_NAME.into()],
        service: vec![ServiceDescriptorProto {
            name: Some(CONTROL_SERVICE.name.into()),
            method: CONTROL_SERVICE
                .methods
                .iter()
                .map(|method| MethodDescriptorProto {
                    name: Some(method.name.into()),
                    input_type: Some(format!(".{}", method.input)),
                    output_type: Some(format!(".{}", method.output)),
                    client_streaming: Some(method.client_streaming),
                    server_streaming: Some(method.server_streaming),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        options: Some(FileOptions {
            go_package: Some(GO_PACKAGE.into()),
            ..Default::default()
        }),
        syntax: Some("proto3".into()),
        ..Default::default()
    }
}

/// Export the complete control-plane dependency closure.
pub fn control_descriptor() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![
            crate::protocol::protocol_file_descriptor(),
            control_file_descriptor(),
        ],
    }
    .encode_to_vec()
}

/// Render Protobuf from the same Rust operation model used by the descriptor.
pub fn control_proto() -> String {
    let mut source = format!(
        "syntax = \"proto3\";\npackage {PACKAGE};\nimport \"{}\";\noption go_package = \"{GO_PACKAGE}\";\n\nservice {} {{\n",
        crate::protocol::FILE_NAME,
        CONTROL_SERVICE.name
    );
    for method in CONTROL_SERVICE.methods {
        source.push_str(&format!(
            "  // {}\n  rpc {} (.{}) returns (.{});\n",
            method.docs, method.name, method.input, method.output
        ));
    }
    source.push_str("}\n");
    source
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn control_plane_preserves_shared_protocol_identity() {
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(crate::protocol::protocol_descriptor())
            ),
            "ce697a9dede342fa869397ca984dd148d6c6375d33a0a633483615a506398660"
        );
        let descriptor = FileDescriptorSet::decode(control_descriptor().as_slice()).unwrap();
        assert_eq!(descriptor.file.len(), 2);
        assert_eq!(
            descriptor.file[0],
            crate::protocol::protocol_file_descriptor()
        );
        let control = &descriptor.file[1];
        assert_eq!(control.name.as_deref(), Some(FILE_NAME));
        assert_eq!(control.dependency, [crate::protocol::FILE_NAME]);
        assert_eq!(control.service.len(), 1);
        assert_eq!(control.service[0].method.len(), 1);
        let method = &control.service[0].method[0];
        assert_eq!(
            method.input_type.as_deref(),
            Some(".acyclic.protocol.v1.HandshakeRequest")
        );
        assert_eq!(
            method.output_type.as_deref(),
            Some(".acyclic.protocol.v1.HandshakeResponse")
        );
        assert!(!method.client_streaming());
        assert!(!method.server_streaming());
        assert!(control.message_type.is_empty());
    }

    #[test]
    fn control_exports_resolve_without_an_authored_proto() {
        let pool = prost_reflect::DescriptorPool::decode(control_descriptor().as_slice()).unwrap();
        let service = pool
            .get_service_by_name("acyclic.transport.v1.ProtocolService")
            .unwrap();
        let method = service.methods().next().unwrap();
        assert_eq!(method.input().full_name(), HANDSHAKE_METHOD.input);
        assert_eq!(method.output().full_name(), HANDSHAKE_METHOD.output);
        assert!(control_proto().contains("rpc Handshake (.acyclic.protocol.v1.HandshakeRequest) returns (.acyclic.protocol.v1.HandshakeResponse)"));
    }

    #[test]
    fn every_family_has_an_unambiguous_control_route() {
        let routes: std::collections::BTreeSet<_> = crate::FAMILY_VIEWS
            .iter()
            .map(|family| handshake_http_route(family.name).unwrap())
            .collect();
        assert_eq!(routes.len(), crate::FAMILY_VIEWS.len());
        assert!(handshake_http_route("../actors").is_none());
        assert!(handshake_http_route("unknown").is_none());
    }
}
