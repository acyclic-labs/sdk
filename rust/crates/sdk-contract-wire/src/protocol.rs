//! Rust-owned Protocol v1 dependency metadata.
//!
//! Protocol is the shared version and capability handshake imported by the
//! Filesystem and Harness contracts.  It is modeled here so descriptor
//! closure never needs to recover the dependency from an active product proto
//! tree.  The checked-in family descriptors remain immutable compatibility
//! fixtures and are used only by tests.

use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, field_descriptor_proto};
use prost_types::{FileDescriptorProto, FileDescriptorSet, FileOptions};

pub const FILE_NAME: &str = "protocol/v1/protocol.proto";
pub const PACKAGE: &str = "acyclic.protocol.v1";
pub const SYNTAX: &str = "proto3";
pub const GO_PACKAGE: &str = "github.com/acyclic-labs/sdk/go/gen/protocol/v1;protocolv1";

fn field(
    name: &'static str,
    number: i32,
    json_name: &'static str,
    kind: field_descriptor_proto::Type,
    type_name: Option<&'static str>,
    label: field_descriptor_proto::Label,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_owned()),
        extendee: None,
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(kind as i32),
        type_name: type_name.map(str::to_owned),
        default_value: None,
        options: None,
        oneof_index: None,
        json_name: Some(json_name.to_owned()),
        proto3_optional: None,
    }
}

fn message(name: &'static str, fields: Vec<FieldDescriptorProto>) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.to_owned()),
        field: fields,
        extension: vec![],
        nested_type: vec![],
        enum_type: vec![],
        extension_range: vec![],
        oneof_decl: vec![],
        options: None,
        reserved_range: vec![],
        reserved_name: vec![],
    }
}

/// The complete Protocol v1 file descriptor, including its package option.
pub fn protocol_file_descriptor() -> FileDescriptorProto {
    let optional_string = field;
    FileDescriptorProto {
        name: Some(FILE_NAME.to_owned()),
        package: Some(PACKAGE.to_owned()),
        dependency: vec![],
        public_dependency: vec![],
        weak_dependency: vec![],
        message_type: vec![
            message(
                "ProtocolIdentity",
                vec![
                    optional_string(
                        "version",
                        1,
                        "version",
                        field_descriptor_proto::Type::String,
                        None,
                        field_descriptor_proto::Label::Optional,
                    ),
                    optional_string(
                        "descriptor_digest",
                        2,
                        "descriptorDigest",
                        field_descriptor_proto::Type::String,
                        None,
                        field_descriptor_proto::Label::Optional,
                    ),
                ],
            ),
            message(
                "Capability",
                vec![
                    optional_string(
                        "name",
                        1,
                        "name",
                        field_descriptor_proto::Type::String,
                        None,
                        field_descriptor_proto::Label::Optional,
                    ),
                    optional_string(
                        "version",
                        2,
                        "version",
                        field_descriptor_proto::Type::String,
                        None,
                        field_descriptor_proto::Label::Optional,
                    ),
                ],
            ),
            message(
                "CapabilitySet",
                vec![optional_string(
                    "capabilities",
                    1,
                    "capabilities",
                    field_descriptor_proto::Type::Message,
                    Some(".acyclic.protocol.v1.Capability"),
                    field_descriptor_proto::Label::Repeated,
                )],
            ),
            message(
                "HandshakeRequest",
                vec![
                    optional_string(
                        "protocol",
                        1,
                        "protocol",
                        field_descriptor_proto::Type::Message,
                        Some(".acyclic.protocol.v1.ProtocolIdentity"),
                        field_descriptor_proto::Label::Optional,
                    ),
                    optional_string(
                        "required",
                        2,
                        "required",
                        field_descriptor_proto::Type::Message,
                        Some(".acyclic.protocol.v1.CapabilitySet"),
                        field_descriptor_proto::Label::Optional,
                    ),
                ],
            ),
            message(
                "HandshakeResponse",
                vec![
                    optional_string(
                        "protocol",
                        1,
                        "protocol",
                        field_descriptor_proto::Type::Message,
                        Some(".acyclic.protocol.v1.ProtocolIdentity"),
                        field_descriptor_proto::Label::Optional,
                    ),
                    optional_string(
                        "supported",
                        2,
                        "supported",
                        field_descriptor_proto::Type::Message,
                        Some(".acyclic.protocol.v1.CapabilitySet"),
                        field_descriptor_proto::Label::Optional,
                    ),
                ],
            ),
        ],
        enum_type: vec![],
        service: vec![],
        extension: vec![],
        options: Some(FileOptions {
            go_package: Some(GO_PACKAGE.to_owned()),
            ..FileOptions::default()
        }),
        source_code_info: None,
        syntax: Some(SYNTAX.to_owned()),
    }
}

/// Return the source-info-free Protocol descriptor set used by dependency
/// closure exporters.
pub fn protocol_descriptor() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![protocol_file_descriptor()],
    }
    .encode_to_vec()
}

/// Describe the shared handshake messages without imposing a family's policy.
pub fn protocol_message_docs(name: &str) -> &'static str {
    match name {
        "ProtocolIdentity" => {
            "Identifies a service family's versioned wire contract and descriptor digest. The accepting service defines its identity matching policy."
        }
        "Capability" => {
            "Identifies a named, versioned capability offered or requested during a handshake."
        }
        "CapabilitySet" => {
            "Groups the named, versioned capabilities exchanged during protocol negotiation."
        }
        "HandshakeRequest" => {
            "Requests protocol negotiation with a service family's identity and required capabilities. The family defines which omissions and capabilities it accepts."
        }
        "HandshakeResponse" => {
            "Reports the service family's protocol identity and supported capabilities after negotiation."
        }
        _ => "",
    }
}

/// Describe each field in the context of its handshake message.
pub fn protocol_field_docs(message: &str, name: &str) -> &'static str {
    match (message, name) {
        ("ProtocolIdentity", "version") => {
            "The version identifier of the service family's wire contract."
        }
        ("ProtocolIdentity", "descriptor_digest") => {
            "The service family's descriptor identity digest. Its canonical bytes and digest convention are defined by that family; generating documentation does not change an archived handshake identity."
        }
        ("Capability", "name") => "The capability name recognized by the service family.",
        ("Capability", "version") => "The version associated with this named capability.",
        ("CapabilitySet", "capabilities") => {
            "The capabilities in this set, each identified by its name and version."
        }
        ("HandshakeRequest", "protocol") => {
            "The protocol identity presented by the caller. Message presence is represented independently from empty identity strings."
        }
        ("HandshakeRequest", "required") => {
            "The capabilities requested by the caller. Acceptance is determined by the service family's negotiation implementation."
        }
        ("HandshakeResponse", "protocol") => {
            "The protocol identity reported by the accepting service."
        }
        ("HandshakeResponse", "supported") => {
            "The capabilities advertised by the accepting service."
        }
        _ => "",
    }
}

/// Render protobuf source and comments from the same Rust-owned descriptor model.
pub fn protocol_proto() -> String {
    let file = protocol_file_descriptor();
    let mut source = format!(
        "syntax = \"{SYNTAX}\";\npackage {PACKAGE};\n\noption go_package = \"{GO_PACKAGE}\";\n\n// Version negotiation shared by every Acyclic service family. Each family\n// names its own contract in ProtocolIdentity; nothing here depends on one.\n\n"
    );
    for message in &file.message_type {
        let name = message.name.as_deref().expect("owned message name");
        source.push_str(&format!(
            "// {}\nmessage {name} {{\n",
            protocol_message_docs(name)
        ));
        for field in &message.field {
            let field_name = field.name.as_deref().expect("owned field name");
            let field_type = match field.r#type() {
                field_descriptor_proto::Type::String => "string",
                field_descriptor_proto::Type::Message => field
                    .type_name
                    .as_deref()
                    .expect("owned message field type")
                    .rsplit('.')
                    .next()
                    .expect("owned type name"),
                _ => unreachable!("Protocol fields are strings or messages"),
            };
            let repeated = if field.label() == field_descriptor_proto::Label::Repeated {
                "repeated "
            } else {
                ""
            };
            source.push_str(&format!(
                "  // {}\n  {repeated}{field_type} {field_name} = {};\n",
                protocol_field_docs(name, field_name),
                field.number()
            ));
        }
        source.push_str("}\n\n");
    }
    source
}
#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    const FILESYSTEM_FIXTURE: &[u8] =
        include_bytes!("../tests/fixtures/filesystem-v2.descriptor.bin");

    #[test]
    fn protocol_descriptor_preserves_inventory_tags_and_option() {
        let file = protocol_file_descriptor();
        assert_eq!(file.name.as_deref(), Some(FILE_NAME));
        assert_eq!(file.package.as_deref(), Some(PACKAGE));
        assert_eq!(file.syntax.as_deref(), Some(SYNTAX));
        assert_eq!(
            file.options.as_ref().and_then(|o| o.go_package.as_deref()),
            Some(GO_PACKAGE)
        );
        assert_eq!(file.message_type.len(), 5);
        assert_eq!(file.message_type[0].field[0].number, Some(1));
        assert_eq!(file.message_type[0].field[1].number, Some(2));
        assert_eq!(
            file.message_type[2].field[0].label,
            Some(field_descriptor_proto::Label::Repeated as i32)
        );
        assert_eq!(
            file.message_type[3].field[1].type_name.as_deref(),
            Some(".acyclic.protocol.v1.CapabilitySet")
        );
    }

    #[test]
    fn protocol_descriptor_matches_immutable_deployed_dependency() {
        let fixture = FileDescriptorSet::decode(FILESYSTEM_FIXTURE).expect("Filesystem fixture");
        let mut expected = fixture
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some(FILE_NAME))
            .expect("Protocol dependency file")
            .clone();
        expected.source_code_info = None;
        assert_eq!(protocol_file_descriptor(), expected);
    }

    #[test]
    fn protocol_source_contains_documented_handshake_role() {
        let source = protocol_proto();
        assert!(source.contains("Version negotiation shared by every Acyclic service family."));
        assert!(source.contains("string descriptor_digest = 2;"));
        assert!(source.contains("repeated Capability capabilities = 1;"));
    }
}
