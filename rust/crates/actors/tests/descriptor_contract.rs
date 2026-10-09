//! Actors v1 descriptor contract and adversarial mutation controls.
//!
//! Fixed standard descriptors come independently from vendored protoc and Buf.
//! Comparison removes source locations and canonicalizes top-level declaration
//! order. All other descriptor contents, including unknown fields, compare exactly.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test fixtures use fatal assertions for invalid descriptor setup"
)]

use acyclic_actors::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, UnknownField, Value};

const EXPECTED_ACTORS_DESCRIPTOR: &[u8] = include_bytes!("fixtures/actors/v1/actors-v1.bin");
const BUF_ACTORS_DESCRIPTOR: &[u8] = include_bytes!("fixtures/actors/v1/actors-buf-v1.bin");
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
    sort_top_level_declarations(&mut descriptor_set);
    descriptor_set
}

fn first_file(descriptor_set: &mut DynamicMessage) -> &mut DynamicMessage {
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

fn named_declaration<'a>(
    parent: &'a mut DynamicMessage,
    field_name: &str,
    name: &str,
) -> &'a mut DynamicMessage {
    let declarations = parent
        .get_field_by_name_mut(field_name)
        .unwrap_or_else(|| panic!("{field_name} must exist"));
    let Value::List(declarations) = declarations else {
        panic!("{field_name} must be repeated");
    };
    declarations
        .iter_mut()
        .find(|value| declaration_name(value) == name)
        .and_then(|value| match value {
            Value::Message(message) => Some(message),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{field_name} must contain {name}"))
}

fn named_field<'a>(
    parent: &'a mut DynamicMessage,
    field_name: &str,
    name: &str,
) -> &'a mut DynamicMessage {
    let fields = parent
        .get_field_by_name_mut(field_name)
        .unwrap_or_else(|| panic!("{field_name} must exist"));
    let Value::List(fields) = fields else {
        panic!("{field_name} must be repeated");
    };
    fields
        .iter_mut()
        .find(|value| declaration_name(value) == name)
        .and_then(|value| match value {
            Value::Message(message) => Some(message),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{field_name} must contain {name}"))
}

fn assert_mutation_rejected(mutate: impl FnOnce(&mut DynamicMessage)) {
    let mut descriptor_set =
        DynamicMessage::decode(descriptor_set_message(), EXPECTED_ACTORS_DESCRIPTOR)
            .expect("expected descriptor must decode");
    mutate(&mut descriptor_set);
    let mutated = descriptor_set.encode_to_vec();
    assert!(
        first_message_difference(
            &canonical_descriptor(EXPECTED_ACTORS_DESCRIPTOR),
            &canonical_descriptor(&mutated),
            "$"
        )
        .is_some(),
        "descriptor proof must reject the requested identity mutation"
    );
}

fn assert_field_value(message: &DynamicMessage, field_name: &str, expected: Value) {
    let actual = message
        .get_field_by_name(field_name)
        .map(|value| value.into_owned());
    assert_eq!(actual, Some(expected), "{field_name} descriptor value");
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
fn current_actors_descriptor_matches_expected_contract() {
    assert!(!EXPECTED_ACTORS_DESCRIPTOR.is_empty());
    assert!(!FILE_DESCRIPTOR_SET.is_empty());

    // Validate both sets as descriptors before comparing them. This catches a
    // malformed generated artifact even when its bytes happen to normalize.
    DescriptorPool::decode(EXPECTED_ACTORS_DESCRIPTOR)
        .expect("expected Actors descriptor must be valid");
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).expect("fresh Actors descriptor must be valid");

    let expected = canonical_descriptor(EXPECTED_ACTORS_DESCRIPTOR);
    let fresh = canonical_descriptor(FILE_DESCRIPTOR_SET);
    assert_descriptor_equivalent(&expected, &fresh);
}

#[test]
fn rust_descriptor_matches_buf_rendered_contract() {
    let rust = canonical_descriptor(FILE_DESCRIPTOR_SET);
    let buf = canonical_descriptor(BUF_ACTORS_DESCRIPTOR);
    assert_descriptor_equivalent(&rust, &buf);
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
        DynamicMessage::decode(descriptor_set_message(), EXPECTED_ACTORS_DESCRIPTOR)
            .expect("expected descriptor must decode");
    let file = first_file(&mut descriptor_set);
    file.set_field_by_name("name", Value::String("actors/v1/changed.proto".to_owned()));
    let mutated = descriptor_set.encode_to_vec();
    assert!(
        first_message_difference(
            &canonical_descriptor(EXPECTED_ACTORS_DESCRIPTOR),
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
fn descriptor_comparison_rejects_all_unknown_file_metadata() {
    for metadata in [
        &[0xd2, 0xf6, 0x03, 0x04, 0x08, 0x00, 0x18, 0x00][..],
        &[0xd2, 0xf6, 0x03, 0x04, 0x08, 0x00, 0x18, 0x01][..],
        &[0xda, 0xf6, 0x03, 0x04, 0x08, 0x00, 0x18, 0x00][..],
    ] {
        let mut descriptor =
            DynamicMessage::decode(descriptor_set_message(), EXPECTED_ACTORS_DESCRIPTOR)
                .expect("expected descriptor must decode");
        first_file(&mut descriptor)
            .merge(metadata)
            .expect("unknown metadata must decode");
        assert!(
            first_message_difference(
                &canonical_descriptor(EXPECTED_ACTORS_DESCRIPTOR),
                &canonical_descriptor(&descriptor.encode_to_vec()),
                "$"
            )
            .is_some(),
            "unknown file metadata must never be hidden"
        );
    }
}

#[test]
fn expected_contract_identities_cover_tags_presence_oneofs_enums_and_rpcs() {
    let mut descriptor_set =
        DynamicMessage::decode(descriptor_set_message(), EXPECTED_ACTORS_DESCRIPTOR)
            .expect("expected descriptor must decode");
    let file = first_file(&mut descriptor_set);

    let subscription_start = named_declaration(file, "message_type", "SubscriptionStart");
    let cursor = named_field(subscription_start, "field", "cursor");
    assert_field_value(cursor, "number", Value::I32(1));
    assert_field_value(cursor, "oneof_index", Value::I32(0));
    let start_oneof = named_declaration(subscription_start, "oneof_decl", "start");
    assert_field_value(start_oneof, "name", Value::String("start".to_owned()));

    let observation = named_declaration(file, "message_type", "SubscriptionObservation");
    let failed_cursor = named_field(observation, "field", "failed_cursor");
    assert_field_value(failed_cursor, "number", Value::I32(10));
    assert_field_value(failed_cursor, "proto3_optional", Value::Bool(true));

    let actor_state = named_declaration(file, "enum_type", "ActorState");
    let active = named_field(actor_state, "value", "ACTOR_STATE_ACTIVE");
    assert_field_value(active, "number", Value::I32(1));

    let service = named_declaration(file, "service", "ActorsService");
    let create_actor = named_field(service, "method", "CreateActor");
    assert_field_value(
        create_actor,
        "input_type",
        Value::String(".acyclic.actors.v1.CreateActorRequest".to_owned()),
    );
    assert_field_value(
        create_actor,
        "output_type",
        Value::String(".acyclic.actors.v1.CreateActorResponse".to_owned()),
    );
}

#[test]
fn descriptor_comparison_rejects_field_number_mutation() {
    assert_mutation_rejected(|descriptor_set| {
        let file = first_file(descriptor_set);
        let message = named_declaration(file, "message_type", "SubscriptionStart");
        let cursor = named_field(message, "field", "cursor");
        cursor.set_field_by_name("number", Value::I32(17));
    });
}

#[test]
fn descriptor_comparison_rejects_enum_value_mutation() {
    assert_mutation_rejected(|descriptor_set| {
        let file = first_file(descriptor_set);
        let actor_state = named_declaration(file, "enum_type", "ActorState");
        let active = named_field(actor_state, "value", "ACTOR_STATE_ACTIVE");
        active.set_field_by_name("number", Value::I32(7));
    });
}

#[test]
fn descriptor_comparison_rejects_presence_mutation() {
    assert_mutation_rejected(|descriptor_set| {
        let file = first_file(descriptor_set);
        let message = named_declaration(file, "message_type", "SubscriptionObservation");
        let failed_cursor = named_field(message, "field", "failed_cursor");
        failed_cursor.set_field_by_name("proto3_optional", Value::Bool(false));
    });
}

#[test]
fn descriptor_comparison_rejects_oneof_identity_mutation() {
    assert_mutation_rejected(|descriptor_set| {
        let file = first_file(descriptor_set);
        let message = named_declaration(file, "message_type", "SubscriptionStart");
        let start = named_declaration(message, "oneof_decl", "start");
        start.set_field_by_name("name", Value::String("renamed_start".to_owned()));
    });
}

#[test]
fn descriptor_comparison_rejects_rpc_identity_mutation() {
    assert_mutation_rejected(|descriptor_set| {
        let file = first_file(descriptor_set);
        let service = named_declaration(file, "service", "ActorsService");
        let create_actor = named_field(service, "method", "CreateActor");
        create_actor.set_field_by_name(
            "input_type",
            Value::String(".acyclic.actors.v1.UpdateActorRequest".to_owned()),
        );
    });
}
