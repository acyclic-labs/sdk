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

/// Version identity of the independent control plane for a selected family.
/// Existing application-specific handshake versions remain unchanged.
pub const fn control_protocol_version(family: crate::BindingFamily) -> &'static str {
    family.package()
}

/// Exact SHA256 identity of the selected immutable runtime descriptor.
pub fn archived_descriptor_digest(family: crate::BindingFamily) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(family.archived_runtime_descriptor()))
}

/// Bound applied before decoding a control handshake on any transport.
pub const MAXIMUM_HANDSHAKE_RESPONSE_BYTES: usize = 64 * 1024;

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

/// Generate maintained prost/tonic bindings directly from the control model.
/// No separately authored Protobuf or generated application contract is input.
///
/// # Errors
/// Returns an error when the output cannot be written or the model is rejected.
pub fn generate_control_bindings(
    output: impl AsRef<std::path::Path>,
    transport: crate::BindingTransport,
) -> Result<(), crate::BindingGenerationError> {
    let output = output.as_ref();
    std::fs::create_dir_all(output)?;
    // The control plane imports the shared Protocol model. Apply the same
    // Rust-owned source comments used by family binding generation so the
    // public handshake types retain their field semantics in rustdoc.
    let descriptor = FileDescriptorSet::decode(
        crate::descriptor_set_with_docs(crate::BindingFamily::Actors, &control_descriptor())?
            .as_slice(),
    )?;
    let mut config = prost_build::Config::new();
    config.out_dir(output);
    match transport {
        crate::BindingTransport::Prost => config.compile_fds(descriptor)?,
        crate::BindingTransport::Tonic { client, server } => {
            tonic_prost_build::configure()
                .out_dir(output)
                .build_client(client)
                .build_server(server)
                .build_transport(
                    std::env::var("CARGO_CFG_TARGET_ARCH").map_or(true, |arch| arch != "wasm32"),
                )
                .compile_fds_with_config(descriptor, config)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn maintained_generators_emit_control_clients_without_application_protos() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "acyclic-control-bindings-{}-{nonce}",
            std::process::id()
        ));
        let native = root.join("native");
        generate_control_bindings(
            &native,
            crate::BindingTransport::Tonic {
                client: true,
                server: true,
            },
        )
        .unwrap();
        let control = std::fs::read_to_string(native.join("acyclic.transport.v1.rs")).unwrap();
        assert!(control.contains("pub struct ProtocolServiceClient"));
        assert!(control.contains("pub struct ProtocolServiceServer"));
        assert!(control.contains(HANDSHAKE_RPC_PATH));
        let protocol = std::fs::read_to_string(native.join("acyclic.protocol.v1.rs")).unwrap();
        assert!(protocol.contains("pub struct HandshakeRequest"));
        assert!(protocol.contains("pub struct HandshakeResponse"));
        assert!(protocol.contains("The protocol identity presented by the caller."));
        assert!(protocol.contains("The capabilities requested by the caller."));
        let portable = root.join("portable");
        generate_control_bindings(&portable, crate::BindingTransport::Prost).unwrap();
        let portable_protocol =
            std::fs::read_to_string(portable.join("acyclic.protocol.v1.rs")).unwrap();
        assert_eq!(portable_protocol, protocol);
    }
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
/// A versioned capability required before selecting an application transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequiredCapability<'a> {
    pub name: &'a str,
    pub version: &'a str,
}

/// Identity and capabilities verified against the selected Rust-owned archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedHandshake {
    pub family: crate::BindingFamily,
    pub version: String,
    pub descriptor_digest: String,
    pub supported: Vec<(String, String)>,
}

/// A terminal negotiation failure; none authorizes replaying an application call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeValidationError {
    InvalidExpectation,
    ResponseTooLarge,
    MalformedResponse,
    MissingIdentity,
    VersionMismatch,
    DescriptorMismatch,
    MissingCapabilities,
    InvalidCapability,
    MissingRequiredCapability,
}

/// Verify the actual Protocol response before treating a transport as compatible.
/// The descriptor digest is always derived from the explicit archived runtime
/// descriptor, never from a freshly generated schema. The transport adapter
/// authenticates its endpoint and applies its deadline before calling this.
///
/// # Errors
/// Rejects oversized or malformed responses, missing message presence, changed
/// wire identities, ambiguous capabilities, and unsupported requirements.
pub fn validate_handshake_response(
    family: crate::BindingFamily,
    expected_version: &str,
    required: &[RequiredCapability<'_>],
    response: &[u8],
    maximum_response_bytes: usize,
) -> Result<ValidatedHandshake, HandshakeValidationError> {
    use HandshakeValidationError as E;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    use sha2::{Digest, Sha256};
    if expected_version.is_empty()
        || maximum_response_bytes == 0
        || required
            .iter()
            .any(|cap| cap.name.is_empty() || cap.version.is_empty())
    {
        return Err(E::InvalidExpectation);
    }
    if response.len() > maximum_response_bytes {
        return Err(E::ResponseTooLarge);
    }
    let pool = DescriptorPool::decode(crate::protocol::protocol_descriptor().as_slice())
        .map_err(|_| E::MalformedResponse)?;
    let descriptor = pool
        .get_message_by_name("acyclic.protocol.v1.HandshakeResponse")
        .ok_or(E::MalformedResponse)?;
    let decoded = DynamicMessage::decode(descriptor, response).map_err(|_| E::MalformedResponse)?;
    if !decoded.has_field_by_name("protocol") {
        return Err(E::MissingIdentity);
    }
    let protocol = decoded
        .get_field_by_name("protocol")
        .ok_or(E::MissingIdentity)?;
    let Value::Message(identity) = protocol.as_ref() else {
        return Err(E::MalformedResponse);
    };
    let string = |message: &DynamicMessage, field: &str| -> Result<String, E> {
        match message.get_field_by_name(field).as_deref() {
            Some(Value::String(value)) => Ok(value.clone()),
            _ => Err(E::MalformedResponse),
        }
    };
    let version = string(identity, "version")?;
    if version != expected_version {
        return Err(E::VersionMismatch);
    }
    let descriptor_digest = string(identity, "descriptor_digest")?;
    if descriptor_digest != format!("{:x}", Sha256::digest(family.archived_runtime_descriptor())) {
        return Err(E::DescriptorMismatch);
    }
    if !decoded.has_field_by_name("supported") {
        return Err(E::MissingCapabilities);
    }
    let supported_field = decoded
        .get_field_by_name("supported")
        .ok_or(E::MissingCapabilities)?;
    let Value::Message(capability_set) = supported_field.as_ref() else {
        return Err(E::MalformedResponse);
    };
    let capabilities = capability_set
        .get_field_by_name("capabilities")
        .ok_or(E::MalformedResponse)?;
    let Value::List(capabilities) = capabilities.as_ref() else {
        return Err(E::MalformedResponse);
    };
    let mut names = std::collections::BTreeSet::new();
    let mut supported = Vec::with_capacity(capabilities.len());
    for capability in capabilities {
        let Value::Message(capability) = capability else {
            return Err(E::MalformedResponse);
        };
        let name = string(capability, "name")?;
        let version = string(capability, "version")?;
        if name.is_empty() || version.is_empty() || !names.insert(name.clone()) {
            return Err(E::InvalidCapability);
        }
        supported.push((name, version));
    }
    if required.iter().any(|cap| {
        !supported
            .iter()
            .any(|(name, version)| name == cap.name && version == cap.version)
    }) {
        return Err(E::MissingRequiredCapability);
    }
    Ok(ValidatedHandshake {
        family,
        version,
        descriptor_digest,
        supported,
    })
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    use prost_reflect::{DescriptorPool, DynamicMessage};
    use sha2::{Digest, Sha256};

    fn response(
        family: crate::BindingFamily,
        version: &str,
        supported: serde_json::Value,
    ) -> Vec<u8> {
        let json = serde_json::json!({
            "protocol": { "version": version, "descriptorDigest": format!("{:x}", Sha256::digest(family.archived_runtime_descriptor())) },
            "supported": { "capabilities": supported }
        }).to_string();
        let pool =
            DescriptorPool::decode(crate::protocol::protocol_descriptor().as_slice()).unwrap();
        let descriptor = pool
            .get_message_by_name("acyclic.protocol.v1.HandshakeResponse")
            .unwrap();
        let mut deserializer = serde_json::Deserializer::from_str(&json);
        DynamicMessage::deserialize(descriptor, &mut deserializer)
            .unwrap()
            .encode_to_vec()
    }

    #[test]
    fn validates_each_exact_archived_identity_before_transport_selection() {
        for family in crate::BindingFamily::ALL {
            let bytes = response(
                *family,
                "test-v1",
                serde_json::json!([{"name": "read", "version": "1"}]),
            );
            let required = [RequiredCapability {
                name: "read",
                version: "1",
            }];
            let verified =
                validate_handshake_response(*family, "test-v1", &required, &bytes, 4096).unwrap();
            assert_eq!(verified.family, *family);
            assert_eq!(verified.supported, vec![("read".into(), "1".into())]);
            assert_eq!(
                validate_handshake_response(*family, "test-v2", &required, &bytes, 4096),
                Err(HandshakeValidationError::VersionMismatch)
            );
            assert_eq!(
                validate_handshake_response(*family, "test-v1", &required, &bytes, bytes.len() - 1),
                Err(HandshakeValidationError::ResponseTooLarge)
            );
            let other = crate::BindingFamily::ALL
                .iter()
                .find(|candidate| *candidate != family)
                .unwrap();
            assert_eq!(
                validate_handshake_response(*other, "test-v1", &required, &bytes, 4096),
                Err(HandshakeValidationError::DescriptorMismatch)
            );
        }
    }

    #[test]
    fn rejects_missing_presence_capability_mismatch_and_duplicate_capabilities() {
        use HandshakeValidationError as E;
        let family = crate::BindingFamily::Actors;
        let required = [RequiredCapability {
            name: "write",
            version: "1",
        }];
        assert_eq!(
            validate_handshake_response(family, "v1", &required, &[], 4096),
            Err(E::MissingIdentity)
        );
        assert_eq!(
            validate_handshake_response(family, "v1", &required, &[0xff], 4096),
            Err(E::MalformedResponse)
        );
        let bytes = response(
            family,
            "v1",
            serde_json::json!([{"name": "write", "version": "2"}]),
        );
        assert_eq!(
            validate_handshake_response(family, "v1", &required, &bytes, 4096),
            Err(E::MissingRequiredCapability)
        );
        let bytes = response(
            family,
            "v1",
            serde_json::json!([{"name": "write", "version": "1"}, {"name": "write", "version": "1"}]),
        );
        assert_eq!(
            validate_handshake_response(family, "v1", &required, &bytes, 4096),
            Err(E::InvalidCapability)
        );
    }
}
