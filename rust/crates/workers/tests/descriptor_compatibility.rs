//! Workers descriptor compatibility proof.
//!
//! The archived descriptor is a compatibility baseline, not a generation input.
//! The current descriptor is generated into `OUT_DIR` by `acyclic-workers` and is
//! exposed through `FILE_DESCRIPTOR_SET`. The only normalization allowed here
//! is removal of `FileDescriptorProto.source_code_info` plus canonicalization of
//! top-level `message_type` and `enum_type` declaration order by name. Protify
//! sorts those declarations while building the generated descriptor. All other
//! descriptor fields, declaration contents, options, and unknown fields remain
//! part of the comparison. This is a descriptor contract check, not a complete
//! transport handshake proof. The archived baseline bytes and their hash remain
//! immutable.
//!
//! The one legacy metadata exception is Buf's `FileDescriptorProto` build
//! metadata extension 8042, whose exact archived encoding is `08001800`:
//! <https://raw.githubusercontent.com/bufbuild/buf/main/proto/buf/alpha/image/v1/image.proto>.
//! It is filtered only at the `FileDescriptorProto` level and only when the
//! complete encoded unknown field matches that known value.

use acyclic_workers::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, UnknownField, Value};

const ARCHIVED_WORKERS_DESCRIPTOR: &[u8] =
    include_bytes!("fixtures/workers/v1/workers_descriptor.bin");
const ADVERSARIAL_DESCRIPTOR: &[u8] =
    include_bytes!("fixtures/workers/v1/adversarial_unknown_option.bin");
const BUF_BUILD_METADATA_8042: &[u8] = &[0xd2, 0xf6, 0x03, 0x04, 0x08, 0x00, 0x18, 0x00];

fn descriptor_set_message() -> prost_reflect::MessageDescriptor {
    DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .expect("prost-reflect global pool must contain FileDescriptorSet")
}

fn normalize_source_code_info(bytes: &[u8]) -> Vec<u8> {
    let mut descriptor_set = DynamicMessage::decode(descriptor_set_message(), bytes)
        .expect("descriptor set must decode through prost-reflect");
    clear_source_code_info(&mut descriptor_set);
    descriptor_set.encode_to_vec()
}

fn clear_source_code_info(descriptor_set: &mut DynamicMessage) {
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
}

fn declaration_name(value: &Value) -> String {
    let Value::Message(message) = value else {
        panic!("descriptor declaration must be a message");
    };
    let Some(name) = message.get_field_by_name("name") else {
        panic!("descriptor declaration must have a name");
    };
    let Value::String(name) = name.as_ref() else {
        panic!("descriptor declaration name must be a string");
    };
    name.clone()
}

fn sort_top_level_declarations(descriptor_set: &mut DynamicMessage) {
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
        for field_name in ["message_type", "enum_type"] {
            let declarations = file
                .get_field_by_name_mut(field_name)
                .expect("FileDescriptorProto declaration field must exist");
            let Value::List(declarations) = declarations else {
                panic!("FileDescriptorProto declaration field must be repeated");
            };
            declarations.sort_by_key(declaration_name);
        }
    }
}

fn canonical_descriptor(bytes: &[u8]) -> DynamicMessage {
    let mut descriptor_set = DynamicMessage::decode(descriptor_set_message(), bytes)
        .expect("descriptor set must decode through prost-reflect");
    clear_source_code_info(&mut descriptor_set);
    filter_legacy_buf_build_metadata(&mut descriptor_set);
    sort_top_level_declarations(&mut descriptor_set);
    descriptor_set
}

fn filter_legacy_buf_build_metadata(descriptor_set: &mut DynamicMessage) {
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
        let unknown_fields: Vec<_> = file.take_unknown_fields().collect();
        for unknown_field in unknown_fields {
            let mut encoded = Vec::new();
            unknown_field.encode(&mut encoded);
            if encoded == BUF_BUILD_METADATA_8042 {
                continue;
            }
            file.merge(encoded.as_slice())
                .expect("re-encoding an unknown field must succeed");
        }
    }
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

fn options_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut descriptor_set = DynamicMessage::decode(descriptor_set_message(), bytes)
        .expect("descriptor set must decode through prost-reflect");
    let file = first_file(&mut descriptor_set);
    let options = file
        .get_field_by_name("options")
        .expect("FileDescriptorProto.options must exist");
    let Value::Message(options) = options.as_ref() else {
        panic!("FileDescriptorProto.options must be a message");
    };
    options.encode_to_vec()
}

fn first_value_difference(expected: &Value, actual: &Value, path: &str) -> Option<String> {
    match (expected, actual) {
        (Value::Message(expected), Value::Message(actual)) => {
            first_message_difference(expected, actual, path)
        }
        (Value::List(expected), Value::List(actual)) => {
            if expected.len() != actual.len() {
                return Some(format!(
                    "{path} length {} != {}",
                    expected.len(),
                    actual.len()
                ));
            }
            expected
                .iter()
                .zip(actual)
                .enumerate()
                .find_map(|(index, (expected, actual))| {
                    first_value_difference(expected, actual, &format!("{path}[{index}]"))
                })
        }
        _ if expected != actual => Some(path.to_owned()),
        _ => None,
    }
}

fn first_message_difference(
    expected: &DynamicMessage,
    actual: &DynamicMessage,
    path: &str,
) -> Option<String> {
    let expected_fields: Vec<_> = expected.fields().collect();
    let actual_fields: Vec<_> = actual.fields().collect();
    if expected_fields.len() != actual_fields.len() {
        return Some(format!(
            "{path} set-field count {} != {}",
            expected_fields.len(),
            actual_fields.len()
        ));
    }
    for (field, expected_value) in expected_fields {
        let field_path = format!("{path}.{}", field.name());
        let Some((_, actual_value)) = actual
            .fields()
            .find(|(actual_field, _)| actual_field.number() == field.number())
        else {
            return Some(format!("{field_path} missing"));
        };
        if let Some(difference) = first_value_difference(expected_value, actual_value, &field_path)
        {
            return Some(difference);
        }
    }

    let expected_unknown: Vec<_> = expected.unknown_fields().collect();
    let actual_unknown: Vec<_> = actual.unknown_fields().collect();
    if expected_unknown.len() != actual_unknown.len() {
        return Some(format!(
            "{path}.unknown_fields {:?} != {:?}",
            expected_unknown
                .iter()
                .map(|field| (field.number(), field.wire_type()))
                .collect::<Vec<_>>(),
            actual_unknown
                .iter()
                .map(|field| (field.number(), field.wire_type()))
                .collect::<Vec<_>>(),
        ));
    }
    for (index, (expected, actual)) in expected_unknown.iter().zip(actual_unknown).enumerate() {
        if *expected != actual {
            return Some(format!("{path}.unknown_fields[{index}]"));
        }
    }
    None
}

fn assert_descriptor_equivalent(expected: &DynamicMessage, actual: &DynamicMessage) {
    if let Some(path) = first_message_difference(expected, actual, "$") {
        panic!("descriptor mismatch at {path}");
    }
}

#[test]
fn current_workers_descriptor_matches_archived_contract() {
    assert!(!ARCHIVED_WORKERS_DESCRIPTOR.is_empty());
    assert!(!FILE_DESCRIPTOR_SET.is_empty());

    // Validate both sets as descriptors before comparing them. This catches a
    // malformed generated artifact even when its bytes happen to normalize.
    DescriptorPool::decode(ARCHIVED_WORKERS_DESCRIPTOR)
        .expect("archived Workers descriptor must be valid");
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("fresh Workers descriptor must be valid");

    let archived = canonical_descriptor(ARCHIVED_WORKERS_DESCRIPTOR);
    let fresh = canonical_descriptor(FILE_DESCRIPTOR_SET);
    assert_descriptor_equivalent(&archived, &fresh);
}

#[test]
fn source_info_normalization_preserves_unknown_custom_option_fields() {
    let normalized = normalize_source_code_info(ADVERSARIAL_DESCRIPTOR);
    assert_ne!(normalized, ADVERSARIAL_DESCRIPTOR);
    assert_eq!(normalize_source_code_info(&normalized), normalized);
    assert_eq!(
        unknown_option_numbers(ADVERSARIAL_DESCRIPTOR),
        vec![50_000, 50_000, 50_001, 50_002, 50_003, 50_004]
    );
    assert_eq!(
        unknown_option_numbers(&normalized),
        vec![50_000, 50_000, 50_001, 50_002, 50_003, 50_004]
    );
    assert_eq!(
        options_bytes(ADVERSARIAL_DESCRIPTOR),
        options_bytes(&normalized),
        "source-info normalization must preserve every option payload and wire type"
    );

    let mut descriptor_set =
        DynamicMessage::decode(descriptor_set_message(), normalized.as_slice())
            .expect("normalized adversarial descriptor must decode");
    let file = first_file(&mut descriptor_set);
    assert!(
        !file.has_field_by_name("source_code_info"),
        "normalization may clear only source_code_info"
    );
}

#[test]
fn descriptor_comparison_rejects_wire_identity_mutation() {
    let mut descriptor_set =
        DynamicMessage::decode(descriptor_set_message(), ARCHIVED_WORKERS_DESCRIPTOR)
            .expect("archived descriptor must decode");
    let file = first_file(&mut descriptor_set);
    file.set_field_by_name("name", Value::String("workers/v1/changed.proto".to_owned()));
    let mutated = descriptor_set.encode_to_vec();
    assert!(
        first_message_difference(
            &canonical_descriptor(ARCHIVED_WORKERS_DESCRIPTOR),
            &canonical_descriptor(&mutated),
            "$"
        )
        .is_some(),
        "descriptor proof must reject a wire identity mutation"
    );
}

#[test]
fn descriptor_comparison_rejects_option_mutation() {
    let mut descriptor_set =
        DynamicMessage::decode(descriptor_set_message(), ADVERSARIAL_DESCRIPTOR)
            .expect("adversarial descriptor must decode");
    let file = first_file(&mut descriptor_set);
    let options = file
        .get_field_by_name_mut("options")
        .expect("FileDescriptorProto.options must exist");
    let Value::Message(options) = options else {
        panic!("FileDescriptorProto.options must be a message");
    };
    options.set_field_by_name("java_package", Value::String("changed.package".to_owned()));
    let mutated = descriptor_set.encode_to_vec();
    assert!(
        first_message_difference(
            &canonical_descriptor(ADVERSARIAL_DESCRIPTOR),
            &canonical_descriptor(&mutated),
            "$"
        )
        .is_some(),
        "descriptor proof must reject an option mutation"
    );
}

#[test]
fn descriptor_comparison_rejects_mutated_or_unrelated_buf_metadata() {
    let metadata_offset = ARCHIVED_WORKERS_DESCRIPTOR
        .windows(BUF_BUILD_METADATA_8042.len())
        .position(|window| window == BUF_BUILD_METADATA_8042)
        .expect("archived descriptor must contain Buf metadata 8042");
    let mut mutated_payload = ARCHIVED_WORKERS_DESCRIPTOR.to_vec();
    let last_byte = metadata_offset + BUF_BUILD_METADATA_8042.len() - 1;
    mutated_payload[last_byte] ^= 1;
    assert!(
        first_message_difference(
            &canonical_descriptor(ARCHIVED_WORKERS_DESCRIPTOR),
            &canonical_descriptor(&mutated_payload),
            "$"
        )
        .is_some(),
        "a changed 8042 payload must remain a compatibility difference"
    );

    let mut descriptor_set =
        DynamicMessage::decode(descriptor_set_message(), ARCHIVED_WORKERS_DESCRIPTOR)
            .expect("archived descriptor must decode");
    let file = first_file(&mut descriptor_set);
    file.merge(&[0xda, 0xf6, 0x03, 0x04, 0x08, 0x00, 0x18, 0x00][..])
        .expect("unrelated unknown metadata must decode");
    let unrelated_metadata = descriptor_set.encode_to_vec();
    assert!(
        first_message_difference(
            &canonical_descriptor(ARCHIVED_WORKERS_DESCRIPTOR),
            &canonical_descriptor(&unrelated_metadata),
            "$"
        )
        .is_some(),
        "unknown field 8043 must not be hidden by the 8042 exception"
    );
}
