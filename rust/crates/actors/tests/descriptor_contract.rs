//! Actors v1 descriptor contract.
//!
//! The retained public Buf baseline is compared after removing source locations
//! and canonicalizing top-level declaration order. All other descriptor contents,
//! including unknown fields, compare exactly. Mutation controls live in
//! `rust/fuzz/fuzz_targets/proto_descriptors.rs`, which fuzzes the shared
//! `acyclic_proto_build::canonicalize` identity.

use acyclic_actors::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};

type Error = Box<dyn std::error::Error>;

const EXPECTED_ACTORS_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../compatibility/descriptors/actors-v1.bin"
));
const BUF_ACTORS_DESCRIPTOR: &[u8] = include_bytes!("fixtures/actors/v1/actors-buf-v1.bin");

fn descriptor_set_message() -> Result<prost_reflect::MessageDescriptor, Error> {
    DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .ok_or_else(|| "prost-reflect global pool must contain FileDescriptorSet".into())
}

fn decode(bytes: &[u8]) -> Result<DynamicMessage, Error> {
    Ok(DynamicMessage::decode(descriptor_set_message()?, bytes)?)
}

fn files_mut(descriptor_set: &mut DynamicMessage) -> Result<&mut Vec<Value>, Error> {
    descriptor_set
        .get_field_by_name_mut("file")
        .and_then(Value::as_list_mut)
        .ok_or_else(|| "FileDescriptorSet.file must be repeated".into())
}

fn file_message(value: &mut Value) -> Result<&mut DynamicMessage, Error> {
    value
        .as_message_mut()
        .ok_or_else(|| "FileDescriptorSet.file must contain messages".into())
}

fn clear_source_code_info(descriptor_set: &mut DynamicMessage) -> Result<(), Error> {
    for file in files_mut(descriptor_set)? {
        file_message(file)?.clear_field_by_name("source_code_info");
    }
    Ok(())
}

fn declaration_name(value: &Value) -> Result<String, Error> {
    value
        .as_message()
        .and_then(|message| message.get_field_by_name("name"))
        .and_then(|name| name.as_str().map(str::to_owned))
        .ok_or_else(|| "descriptor declaration must have a string name".into())
}

fn sort_top_level_declarations(descriptor_set: &mut DynamicMessage) -> Result<(), Error> {
    for file in files_mut(descriptor_set)? {
        let file = file_message(file)?;
        for field_name in ["message_type", "enum_type"] {
            let declarations = file
                .get_field_by_name_mut(field_name)
                .and_then(Value::as_list_mut)
                .ok_or_else(|| format!("FileDescriptorProto.{field_name} must be repeated"))?;
            let names = declarations
                .iter()
                .map(declaration_name)
                .collect::<Result<Vec<_>, _>>()?;
            let mut keyed = names
                .into_iter()
                .zip(declarations.drain(..))
                .collect::<Vec<_>>();
            keyed.sort_by(|left, right| left.0.cmp(&right.0));
            declarations.extend(keyed.into_iter().map(|(_, value)| value));
        }
    }
    Ok(())
}

fn canonical_descriptor(bytes: &[u8]) -> Result<DynamicMessage, Error> {
    let mut descriptor_set = decode(bytes)?;
    clear_source_code_info(&mut descriptor_set)?;
    sort_top_level_declarations(&mut descriptor_set)?;
    Ok(descriptor_set)
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

fn assert_descriptor_equivalent(
    expected: &DynamicMessage,
    actual: &DynamicMessage,
) -> Result<(), Error> {
    match first_message_difference(expected, actual, "$") {
        Some(path) => Err(format!("descriptor mismatch at {path}").into()),
        None => Ok(()),
    }
}

#[test]
fn current_actors_descriptor_matches_expected_contract() -> Result<(), Error> {
    assert!(!EXPECTED_ACTORS_DESCRIPTOR.is_empty());
    assert!(!FILE_DESCRIPTOR_SET.is_empty());
    DescriptorPool::decode(EXPECTED_ACTORS_DESCRIPTOR)?;
    DescriptorPool::decode(FILE_DESCRIPTOR_SET)?;
    assert_eq!(
        FILE_DESCRIPTOR_SET, EXPECTED_ACTORS_DESCRIPTOR,
        "the build must emit the retained canonical actors descriptor byte for byte"
    );
    Ok(())
}

#[test]
fn rust_descriptor_matches_buf_rendered_contract() -> Result<(), Error> {
    let rust = canonical_descriptor(FILE_DESCRIPTOR_SET)?;
    let buf = canonical_descriptor(BUF_ACTORS_DESCRIPTOR)?;
    assert_descriptor_equivalent(&rust, &buf)
}
