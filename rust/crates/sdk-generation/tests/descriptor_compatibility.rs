//! Actors descriptor compatibility proof.
//!
//! The archived descriptor is a compatibility baseline, not a generation input.
//! The current descriptor is generated into `OUT_DIR` by `acyclic-actors` and is
//! exposed through `FILE_DESCRIPTOR_SET`. The only normalization allowed here
//! is removal of `FileDescriptorProto.source_code_info`; all other descriptor
//! fields, options, and unknown fields remain part of the comparison.

use acyclic_actors::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, UnknownField, Value};

const ARCHIVED_ACTORS_DESCRIPTOR: &[u8] =
    include_bytes!("fixtures/actors/v1/actors_descriptor.bin");
const ADVERSARIAL_DESCRIPTOR: &[u8] =
    include_bytes!("fixtures/actors/v1/adversarial_unknown_option.bin");

fn descriptor_set_message() -> prost_reflect::MessageDescriptor {
    DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .expect("prost-reflect global pool must contain FileDescriptorSet")
}

fn normalize_source_code_info(bytes: &[u8]) -> Vec<u8> {
    let mut descriptor_set = DynamicMessage::decode(descriptor_set_message(), bytes)
        .expect("descriptor set must decode through prost-reflect");
    let files = descriptor_set
        .get_field_by_name_mut("file")
        .expect("FileDescriptorSet.file must exist");
    let Value::List(files) = files else {
        panic!("FileDescriptorSet.file must be repeated");
    };
    for file in files {
        let Value::Message(file) = file else {
            panic!("FileDescriptorSet.file must contain messages");
        };
        file.clear_field_by_name("source_code_info");
    }
    descriptor_set.encode_to_vec()
}

fn first_file<'a>(descriptor_set: &'a mut DynamicMessage) -> &'a mut DynamicMessage {
    let files = descriptor_set
        .get_field_by_name_mut("file")
        .expect("FileDescriptorSet.file must exist");
    let Value::List(files) = files else {
        panic!("FileDescriptorSet.file must be repeated");
    };
    let Some(Value::Message(file)) = files.first_mut() else {
        panic!("descriptor set must contain one file");
    };
    file
}

fn unknown_option_numbers(bytes: &[u8]) -> Vec<u32> {
    let mut descriptor_set = DynamicMessage::decode(descriptor_set_message(), bytes)
        .expect("descriptor set must decode through prost-reflect");
    let file = first_file(&mut descriptor_set);
    let options = file
        .get_field_by_name("options")
        .expect("FileDescriptorProto.options must exist");
    let Value::Message(options) = options.as_ref() else {
        panic!("FileDescriptorProto.options must be a message");
    };
    options.unknown_fields().map(UnknownField::number).collect()
}

#[test]
fn current_actors_descriptor_matches_archived_wire_contract() {
    assert!(!ARCHIVED_ACTORS_DESCRIPTOR.is_empty());
    assert!(!FILE_DESCRIPTOR_SET.is_empty());

    // Validate both sets as descriptors before comparing them. This catches a
    // malformed generated artifact even when its bytes happen to normalize.
    DescriptorPool::decode(ARCHIVED_ACTORS_DESCRIPTOR)
        .expect("archived Actors descriptor must be valid");
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("fresh Actors descriptor must be valid");

    assert_eq!(
        normalize_source_code_info(ARCHIVED_ACTORS_DESCRIPTOR),
        normalize_source_code_info(FILE_DESCRIPTOR_SET),
        "Actors wire descriptor changed outside source_code_info"
    );
}

#[test]
fn source_info_normalization_preserves_unknown_custom_option_fields() {
    let normalized = normalize_source_code_info(ADVERSARIAL_DESCRIPTOR);
    assert_ne!(normalized, ADVERSARIAL_DESCRIPTOR);
    assert_eq!(normalize_source_code_info(&normalized), normalized);
    assert_eq!(unknown_option_numbers(ADVERSARIAL_DESCRIPTOR), vec![50_000]);
    assert_eq!(unknown_option_numbers(&normalized), vec![50_000]);

    let mut descriptor_set = DynamicMessage::decode(descriptor_set_message(), &normalized)
        .expect("normalized adversarial descriptor must decode");
    let file = first_file(&mut descriptor_set);
    assert!(
        !file.has_field_by_name("source_code_info"),
        "normalization may clear only source_code_info"
    );
}

