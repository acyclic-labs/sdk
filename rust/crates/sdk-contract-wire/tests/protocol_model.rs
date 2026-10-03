use acyclic_sdk_contract_wire::{filesystem, harness, protocol};
use prost::Message;
use prost_types::FileDescriptorSet;
use sha2::{Digest, Sha256};

const FILESYSTEM_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/filesystem-v2.descriptor.bin"
));
const INFERENCE_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/inference-v1.descriptor.bin"
));

fn file_without_source_info(bytes: &[u8], name: &str) -> Vec<u8> {
    let set = FileDescriptorSet::decode(bytes).expect("descriptor set");
    let file = set
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(name))
        .expect("descriptor file");
    FileDescriptorSet {
        file: vec![prost_types::FileDescriptorProto {
            source_code_info: None,
            ..file
        }],
    }
    .encode_to_vec()
}

#[test]
fn protocol_model_matches_immutable_deployed_dependency() {
    let model = protocol::protocol_descriptor();
    assert_eq!(
        model,
        file_without_source_info(FILESYSTEM_FIXTURE, protocol::FILE_NAME)
    );
    assert_eq!(
        Sha256::digest(model)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "ce697a9dede342fa869397ca984dd148d6c6375d33a0a633483615a506398660"
    );
}

#[test]
fn filesystem_and_harness_emit_complete_protocol_dependency_closure() {
    for (name, bytes) in [
        ("Filesystem", filesystem::filesystem_descriptor()),
        ("Harness", harness::harness_descriptor()),
    ] {
        let set = FileDescriptorSet::decode(bytes.as_slice()).expect("family descriptor");
        let family = set
            .file
            .iter()
            .find(|file| file.name.as_deref() != Some(protocol::FILE_NAME))
            .expect("family file");
        assert_eq!(
            set.file
                .iter()
                .map(|file| file.name.as_deref().unwrap_or_default())
                .collect::<Vec<_>>(),
            vec![
                protocol::FILE_NAME,
                family.name.as_deref().unwrap_or_default(),
            ]
        );
        assert_eq!(family.dependency, vec![protocol::FILE_NAME.to_owned()]);
        assert!(
            set.file
                .iter()
                .any(|file| file.name.as_deref() == Some(protocol::FILE_NAME)),
            "{name} descriptor omitted Protocol dependency"
        );
    }
}

#[test]
fn protocol_source_retains_handshake_docs_and_wire_names() {
    let source = protocol::protocol_proto();
    assert!(source.contains("Version negotiation shared by every Acyclic service family."));
    assert!(source.contains("descriptor_digest = 2"));
    assert!(source.contains("CapabilitySet required = 2"));
}

#[test]
fn options_closure_pins_upstream_wkt_as_immutable_oss_dependency() {
    let set = FileDescriptorSet::decode(INFERENCE_FIXTURE).expect("Inference fixture");
    let wkt = set
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some("google/protobuf/descriptor.proto"))
        .expect("upstream descriptor.proto dependency");
    assert_eq!(wkt.package.as_deref(), Some("google.protobuf"));
    let mut source_info_free = wkt.clone();
    source_info_free.source_code_info = None;
    let digest = Sha256::digest(
        FileDescriptorSet {
            file: vec![source_info_free],
        }
        .encode_to_vec(),
    );
    let actual = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        actual, "46dc93b4614e090fea8f94c2e6a2c1b8ee894e41a6b6710a6826aca42399c0c4",
        "upstream WKT descriptor changed"
    );
    let validation = set
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some("validation/v1/options.proto"))
        .expect("validation options dependency");
    assert_eq!(
        validation.dependency,
        vec!["google/protobuf/descriptor.proto"]
    );
    let mut extensions = validation.extension.clone();
    extensions.sort_by_key(|extension| extension.number);
    assert_eq!(
        extensions
            .iter()
            .map(|extension| {
                (
                    extension.number.unwrap_or_default(),
                    extension.name.as_deref(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (51001, Some("nonzero_fixed_bytes")),
            (51002, Some("required_message")),
            (51003, Some("positive_uint64")),
            (51004, Some("required_oneof")),
            (51005, Some("min_items")),
            (51006, Some("max_items")),
            (51007, Some("nonempty_max_bytes")),
            (51008, Some("max_uint64")),
            (51009, Some("known_nonzero_enum")),
            (51010, Some("nonempty_max_item_bytes")),
            (51011, Some("partial_terminal")),
            (51012, Some("http_path")),
        ]
    );
}

#[test]
fn protocol_semantic_and_source_drift_cannot_recover_from_authored_proto() {
    let canonical = protocol::protocol_descriptor();
    let mut damaged = protocol::protocol_file_descriptor();
    damaged.message_type[0]
        .field
        .retain(|field| field.name.as_deref() != Some("descriptor_digest"));
    damaged
        .options
        .as_mut()
        .expect("Protocol options")
        .go_package = None;
    assert_ne!(
        FileDescriptorSet {
            file: vec![damaged],
        }
        .encode_to_vec(),
        canonical
    );

    let source = protocol::protocol_proto();
    let without_field = source.replace("  string descriptor_digest = 2;\n", "");
    let without_option = source.replace(
        "option go_package = \"github.com/acyclic-labs/sdk/go/gen/protocol/v1;protocolv1\";\n",
        "",
    );
    assert_ne!(without_field, source);
    assert_ne!(without_option, source);
    assert!(source.contains("descriptor_digest"));
    assert!(source.contains("go_package"));
}
