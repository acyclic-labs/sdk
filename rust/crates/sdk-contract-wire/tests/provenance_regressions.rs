use acyclic_sdk_contract_wire::{
    actors_descriptor, actors_proto,
    filesystem::{ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST, filesystem_descriptor},
    machines::machines_descriptor,
    objects::objects_descriptor,
    stream::stream_descriptor,
};
use prost::Message;
use prost_types::FileDescriptorSet;
use sha2::{Digest, Sha256};
use std::path::Path;

const ACTORS_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/actors-v1.descriptor.bin"
));
const STREAM_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/stream-v2.descriptor.bin"
));
const OBJECTS_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/objects-v2.descriptor.bin"
));
const WORKERS_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/workers-v1.descriptor.bin"
));
const FILESYSTEM_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/filesystem-v2.descriptor.bin"
));
const MACHINES_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/machines-v1.descriptor.bin"
));
const HARNESS_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/harness-v2.descriptor.bin"
));
const ARCHIVED_OBJECTS_V1_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../compatibility/objects/v1/objects_descriptor.bin"
));

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn emitted_family_descriptors_match_pinned_fixture_bytes() {
    let cases = [
        ("actors", actors_descriptor(), ACTORS_FIXTURE),
        ("stream", stream_descriptor(), STREAM_FIXTURE),
        ("objects", objects_descriptor(), OBJECTS_FIXTURE),
    ];
    for (name, emitted, fixture) in cases {
        assert_eq!(emitted.as_slice(), fixture, "{name} fixture drifted");
    }
}

fn descriptor_without_source_info(bytes: &[u8]) -> Vec<u8> {
    let mut set = FileDescriptorSet::decode(bytes).expect("descriptor set");
    for file in &mut set.file {
        file.source_code_info = None;
    }
    set.encode_to_vec()
}

fn descriptor_file_without_source_info(bytes: &[u8], name: &str) -> Vec<u8> {
    let set = FileDescriptorSet::decode(bytes).expect("descriptor set");
    let file = set
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some(name))
        .unwrap_or_else(|| panic!("missing descriptor file {name}"));
    let mut single = FileDescriptorSet {
        file: vec![file.clone()],
    };
    single.file[0].source_code_info = None;
    single.encode_to_vec()
}

#[test]
fn emitted_descriptor_semantics_preserve_options_roles_and_dependencies() {
    let cases = [
        (
            "machines",
            machines_descriptor(),
            MACHINES_FIXTURE,
            "machines/v1/machines.proto",
        ),
        (
            "filesystem",
            filesystem_descriptor(),
            FILESYSTEM_FIXTURE,
            "filesystem/v2/filesystem.proto",
        ),
    ];
    for (name, emitted, fixture, file_name) in cases {
        assert_eq!(
            descriptor_file_without_source_info(&emitted, file_name),
            descriptor_file_without_source_info(fixture, file_name),
            "{name} semantic descriptor drifted"
        );
    }

    let emitted = FileDescriptorSet::decode(filesystem_descriptor().as_slice())
        .expect("filesystem descriptor");
    let filesystem = emitted
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some("filesystem/v2/filesystem.proto"))
        .expect("filesystem file");
    for dependency in &filesystem.dependency {
        assert!(
            emitted
                .file
                .iter()
                .any(|file| file.name.as_deref() == Some(dependency.as_str())),
            "filesystem descriptor omits dependency closure entry {dependency}"
        );
    }
}

#[test]
fn workers_descriptor_semantics_preserve_options_roles_and_fields() {
    assert_eq!(
        descriptor_without_source_info(&acyclic_sdk_contract_wire::workers::workers_descriptor(),),
        descriptor_without_source_info(WORKERS_FIXTURE),
        "workers semantic descriptor drifted"
    );
}

#[test]
fn pinned_runtime_fixture_hashes_and_handshake_role_are_stable() {
    let cases = [
        (
            ACTORS_FIXTURE,
            5234,
            "0515dc7e3f38a5648f85ee52f5bda7e639cc31cf83179a208bb5abbd398a04bf",
        ),
        (
            STREAM_FIXTURE,
            5878,
            "1d311dd12a56de4f04923e4144071c507c6b59b1789955fd8629d09c990abd0c",
        ),
        (
            OBJECTS_FIXTURE,
            7554,
            "1968e12e89d38f7076b9aee559c815748858c156401a56ba53e615def49fe86f",
        ),
        (
            WORKERS_FIXTURE,
            10799,
            "851b6cd37b8cb4baa6d3a111efdad655b89936b2e1057ecb74e62825715bd7d8",
        ),
        (
            FILESYSTEM_FIXTURE,
            54180,
            "105e153060d229569836982527c91bd56a69891691007215fbc599115eca2093",
        ),
        (
            MACHINES_FIXTURE,
            30060,
            "68feb507148fbf798a3e05236a4d93d36d216c260db0a6a339db5919c630e758",
        ),
        (
            HARNESS_FIXTURE,
            53908,
            "b1d721a657f40f652a560769ff440b7a1ca44739765fd3114269b727de5474e4",
        ),
        (
            ARCHIVED_OBJECTS_V1_FIXTURE,
            25221,
            "4701187ac8ca87325d42aee0f99c7826ebb63ec2be0ae98c4768006d4ff31a51",
        ),
    ];
    for (fixture, length, expected) in cases {
        assert_eq!(fixture.len(), length);
        assert_eq!(sha256_hex(fixture), expected);
    }
    assert_eq!(
        ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST,
        "blake3:ece4a6bb58779d216707a426a0ebc5375b5a7a99b14178ce2f9d4f9dd781df60"
    );
}

#[test]
fn removing_model_fields_or_file_options_cannot_be_recovered_from_proto() {
    let canonical_source = actors_proto();
    let without_field = canonical_source.replace("  bytes code_sha256 = 2;\n", "");
    let without_option = canonical_source.replace(
        "option go_package = \"github.com/acyclic-labs/sdk/go/gen/actors/v1;actorsv1\";\n",
        "",
    );
    assert_ne!(without_field, canonical_source);
    assert_ne!(without_option, canonical_source);
    assert!(without_field.len() < canonical_source.len());
    assert!(without_option.len() < canonical_source.len());

    let canonical_descriptor = actors_descriptor();
    let mut damaged = FileDescriptorSet::decode(canonical_descriptor.as_slice())
        .expect("canonical Actors descriptor");
    let file = damaged.file.first_mut().expect("Actors file");
    file.message_type
        .iter_mut()
        .find(|message| message.name.as_deref() == Some("ActorObservation"))
        .expect("ActorObservation message")
        .field
        .retain(|field| field.name.as_deref() != Some("code_sha256"));
    file.options.as_mut().expect("file options").go_package = None;
    assert_ne!(damaged.encode_to_vec(), canonical_descriptor);
    assert!(canonical_source.contains("code_sha256"));
    assert!(canonical_source.contains("go_package"));
}

#[test]
fn generator_source_has_no_active_proto_authority_read() {
    let generator = include_str!("../src/bin/sdk-contract-wire.rs");
    assert!(!generator.contains("../../../proto/"));
    assert!(!generator.contains("proto/actors/"));
    assert!(!generator.contains("proto/validation/"));
    assert!(!generator.contains("Command::new(\"protoc\")"));
    assert!(generator.contains("include_bytes!(\"../lib.rs\")"));
    assert!(generator.contains("include_bytes!(\"../objects.rs\")"));
    assert!(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/bin/sdk-contract-wire.rs")
            .exists()
    );
}

#[test]
fn generator_declares_every_pinned_wire_family() {
    let generator = include_str!("../src/bin/sdk-contract-wire.rs");
    for marker in [
        "harness_descriptor",
        "harness_proto",
        "harness/v2/harness.proto",
        "HARNESS_DESCRIPTOR_PATH",
        "protocol_descriptor",
        "protocol/v1/protocol.proto",
    ] {
        assert!(
            generator.contains(marker),
            "generator does not declare pinned Harness family marker {marker}"
        );
    }
}
