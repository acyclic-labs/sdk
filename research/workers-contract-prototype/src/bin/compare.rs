use std::{env, fs, io, path::PathBuf};

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};

const BUF_BUILD_METADATA_8042: &[u8] = &[0xd2, 0xf6, 0x03, 0x04, 0x08, 0x00, 0x18, 0x00];

fn descriptor_set_message() -> prost_reflect::MessageDescriptor {
    DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .expect("prost-reflect global pool must contain FileDescriptorSet")
}

fn normalize(mut descriptor_set: DynamicMessage) -> DynamicMessage {
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
        let unknown_fields: Vec<_> = file.take_unknown_fields().collect();
        for unknown_field in unknown_fields {
            let mut encoded = Vec::new();
            unknown_field.encode(&mut encoded);
            if encoded != BUF_BUILD_METADATA_8042 {
                file.merge(encoded.as_slice())
                    .expect("unknown field must re-encode");
            }
        }
        for field_name in ["message_type", "enum_type"] {
            let declarations = file
                .get_field_by_name_mut(field_name)
                .expect("FileDescriptorProto declaration field must exist");
            let Value::List(declarations) = declarations else {
                panic!("declaration field must be repeated");
            };
            declarations.sort_by_key(|value| {
                let Value::Message(message) = value else {
                    panic!("declaration must be a message");
                };
                let Some(name) = message.get_field_by_name("name") else {
                    panic!("declaration must have a name");
                };
                let Value::String(name) = name.as_ref() else {
                    panic!("declaration name must be a string");
                };
                name.clone()
            });
        }
    }
    descriptor_set
}

fn first_difference(
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
        match (expected_value, actual_value) {
            (Value::Message(expected), Value::Message(actual)) => {
                if let Some(diff) = first_difference(expected, actual, &field_path) {
                    return Some(diff);
                }
            }
            (Value::List(expected), Value::List(actual)) => {
                if expected.len() != actual.len() {
                    return Some(format!("{field_path} list length"));
                }
                for (index, (expected, actual)) in expected.iter().zip(actual.iter()).enumerate() {
                    match (expected, actual) {
                        (Value::Message(expected), Value::Message(actual)) => {
                            if let Some(diff) = first_difference(
                                expected,
                                actual,
                                &format!("{field_path}[{index}]"),
                            ) {
                                return Some(diff);
                            }
                        }
                        _ if expected != actual => return Some(format!("{field_path}[{index}]")),
                        _ => {}
                    }
                }
            }
            _ if expected_value != actual_value => return Some(field_path),
            _ => {}
        }
    }
    let expected_unknown: Vec<_> = expected.unknown_fields().collect();
    let actual_unknown: Vec<_> = actual.unknown_fields().collect();
    if expected_unknown != actual_unknown {
        return Some(format!("{path}.unknown_fields"));
    }
    None
}

fn main() -> io::Result<()> {
    let mut args = env::args_os().skip(1);
    let expected = PathBuf::from(args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: compare <expected.bin> <actual.bin>",
        )
    })?);
    let actual = PathBuf::from(args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: compare <expected.bin> <actual.bin>",
        )
    })?);
    let expected_bytes = fs::read(expected)?;
    let actual_bytes = fs::read(actual)?;
    DescriptorPool::decode(expected_bytes.as_slice())
        .map_err(|e| io::Error::other(format!("expected descriptor invalid: {e}")))?;
    DescriptorPool::decode(actual_bytes.as_slice())
        .map_err(|e| io::Error::other(format!("actual descriptor invalid: {e}")))?;
    let expected = normalize(
        DynamicMessage::decode(descriptor_set_message(), expected_bytes.as_slice())
            .map_err(|e| io::Error::other(e.to_string()))?,
    );
    let actual = normalize(
        DynamicMessage::decode(descriptor_set_message(), actual_bytes.as_slice())
            .map_err(|e| io::Error::other(e.to_string()))?,
    );
    if let Some(path) = first_difference(&expected, &actual, "$") {
        return Err(io::Error::other(format!("descriptor mismatch at {path}")));
    }
    println!("canonical descriptor equivalence: pass");
    Ok(())
}
