use acyclic_sdk_contract_validation::{compare_bytes, DifferenceKind};
use acyclic_sdk_contract_wire::{
    actors_descriptor, filesystem::filesystem_descriptor, harness::harness_descriptor,
    inference::inference_descriptor, machines::machines_descriptor, objects::objects_descriptor,
    stream::stream_descriptor, workers::workers_descriptor,
};
use prost::Message;
use prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorProto, FileDescriptorSet};

fn fixture(name: &str) -> &'static [u8] {
    match name {
        "actors" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/actors-v1.descriptor.bin"
        )),
        "stream" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/stream-v2.descriptor.bin"
        )),
        "objects" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/objects-v2.descriptor.bin"
        )),
        "workers" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/workers-v1.descriptor.bin"
        )),
        "filesystem" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/filesystem-v2.descriptor.bin"
        )),
        "harness" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/harness-v2.descriptor.bin"
        )),
        "inference" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/inference-v1.descriptor.bin"
        )),
        "machines" => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/machines-v1.descriptor.bin"
        )),
        _ => panic!("unknown family fixture {name}"),
    }
}

fn declared_file<'a>(
    set: &'a mut FileDescriptorSet,
    name: &str,
    file_name: &str,
) -> &'a mut FileDescriptorProto {
    set.file
        .iter_mut()
        .find(|file| file.name.as_deref() == Some(file_name))
        .unwrap_or_else(|| panic!("{name} file {file_name}"))
}

fn first_message_with_field(messages: &mut [DescriptorProto]) -> Option<&mut DescriptorProto> {
    for message in messages {
        if !message.field.is_empty() {
            return Some(message);
        }
        if let Some(nested) = first_message_with_field(&mut message.nested_type) {
            return Some(nested);
        }
    }
    None
}

fn first_message_with_oneof(messages: &mut [DescriptorProto]) -> Option<&mut DescriptorProto> {
    for message in messages {
        if !message.oneof_decl.is_empty() {
            return Some(message);
        }
        if let Some(nested) = first_message_with_oneof(&mut message.nested_type) {
            return Some(nested);
        }
    }
    None
}

fn first_nested_enum(messages: &mut [DescriptorProto]) -> Option<&mut EnumDescriptorProto> {
    for message in messages {
        if let Some(enumeration) = message
            .enum_type
            .iter_mut()
            .find(|enumeration| !enumeration.value.is_empty())
        {
            return Some(enumeration);
        }
        if let Some(nested) = first_nested_enum(&mut message.nested_type) {
            return Some(nested);
        }
    }
    None
}

fn first_enum(file: &mut FileDescriptorProto) -> Option<&mut EnumDescriptorProto> {
    if let Some(index) = file
        .enum_type
        .iter()
        .position(|enumeration| !enumeration.value.is_empty())
    {
        return file.enum_type.get_mut(index);
    }
    first_nested_enum(&mut file.message_type)
}

fn assert_rejected(
    name: &str,
    baseline: &[u8],
    mutated: Vec<u8>,
    expected_kind: Option<DifferenceKind>,
) {
    let report = compare_bytes(baseline, &mutated)
        .unwrap_or_else(|error| panic!("{name} descriptor comparison: {error:?}"));
    assert!(
        !report.semantic_compatible,
        "{name} wire-shape mutation was accepted"
    );
    if let Some(expected_kind) = expected_kind {
        assert!(
            report
                .differences
                .iter()
                .any(|difference| difference.kind == expected_kind),
            "{name} mutation did not report {expected_kind:?}: {:?}",
            report.differences
        );
    }
}

#[test]
fn every_declared_family_rejects_wire_shape_mutations() {
    let families = [
        ("actors", "actors/v1/actors.proto", actors_descriptor()),
        ("stream", "stream/v2/stream.proto", stream_descriptor()),
        ("objects", "objects/v2/objects.proto", objects_descriptor()),
        ("workers", "workers/v1/workers.proto", workers_descriptor()),
        (
            "filesystem",
            "filesystem/v2/filesystem.proto",
            filesystem_descriptor(),
        ),
        ("harness", "harness/v2/harness.proto", harness_descriptor()),
        (
            "inference",
            "inference/v1/inference.proto",
            inference_descriptor(),
        ),
        (
            "machines",
            "machines/v1/machines.proto",
            machines_descriptor(),
        ),
    ];
    let mut oneof_families = Vec::new();

    for (name, file_name, canonical) in families {
        let baseline = fixture(name);

        let mut field_tag = FileDescriptorSet::decode(canonical.as_slice())
            .unwrap_or_else(|error| panic!("{name} descriptor: {error}"));
        let file = declared_file(&mut field_tag, name, file_name);
        let message = first_message_with_field(&mut file.message_type)
            .unwrap_or_else(|| panic!("{name} message with a field"));
        message.field[0].number = Some(message.field[0].number.expect("field number") + 1000);
        assert_rejected(
            name,
            baseline,
            field_tag.encode_to_vec(),
            Some(DifferenceKind::FieldTag),
        );

        let mut json_name = FileDescriptorSet::decode(canonical.as_slice())
            .unwrap_or_else(|error| panic!("{name} descriptor: {error}"));
        let file = declared_file(&mut json_name, name, file_name);
        let message = first_message_with_field(&mut file.message_type)
            .unwrap_or_else(|| panic!("{name} message with a field"));
        message.field[0].json_name = Some("wireIdentityChanged".to_owned());
        assert_rejected(name, baseline, json_name.encode_to_vec(), None);

        let mut option = FileDescriptorSet::decode(canonical.as_slice())
            .unwrap_or_else(|error| panic!("{name} descriptor: {error}"));
        let file = declared_file(&mut option, name, file_name);
        file.options
            .as_mut()
            .unwrap_or_else(|| panic!("{name} file options"))
            .go_package = Some("wire.identity.changed".to_owned());
        assert_rejected(
            name,
            baseline,
            option.encode_to_vec(),
            Some(DifferenceKind::CustomOption),
        );

        let mut enum_value = FileDescriptorSet::decode(canonical.as_slice())
            .unwrap_or_else(|error| panic!("{name} descriptor: {error}"));
        let file = declared_file(&mut enum_value, name, file_name);
        let enumeration = first_enum(file).unwrap_or_else(|| panic!("{name} enum with a value"));
        enumeration.value[0].number =
            Some(enumeration.value[0].number.expect("enum value number") + 1000);
        assert_rejected(
            name,
            baseline,
            enum_value.encode_to_vec(),
            Some(DifferenceKind::EnumValue),
        );

        let mut rpc = FileDescriptorSet::decode(canonical.as_slice())
            .unwrap_or_else(|error| panic!("{name} descriptor: {error}"));
        let file = declared_file(&mut rpc, name, file_name);
        let service = file
            .service
            .iter_mut()
            .find(|service| !service.method.is_empty())
            .unwrap_or_else(|| panic!("{name} service with a method"));
        service.method[0].server_streaming =
            Some(!service.method[0].server_streaming.unwrap_or(false));
        assert_rejected(
            name,
            baseline,
            rpc.encode_to_vec(),
            Some(DifferenceKind::ServiceStreaming),
        );

        let mut oneof = FileDescriptorSet::decode(canonical.as_slice())
            .unwrap_or_else(|error| panic!("{name} descriptor: {error}"));
        let file = declared_file(&mut oneof, name, file_name);
        if let Some(message) = first_message_with_oneof(&mut file.message_type) {
            oneof_families.push(name);
            message.oneof_decl[0].name = Some("wireIdentityChanged".to_owned());
            assert_rejected(name, baseline, oneof.encode_to_vec(), None);
        }
    }

    assert!(
        !oneof_families.is_empty(),
        "no family exercised oneof identity mutation"
    );
}
