//! Fuzz descriptor identity canonicalization and protobuf round-trip stability.
#![no_main]
use libfuzzer_sys::fuzz_target;
use prost::Message;
use prost::encoding::{WireType, encode_key, encode_varint};
use prost_reflect::{DescriptorPool, DynamicMessage, Value};

macro_rules! descriptors {
    ($($file:literal),* $(,)?) => {
        [$(($file, include_bytes!(concat!("../../../compatibility/descriptors/", $file)).as_slice())),*]
    };
}

const WRITERS: &[(&str, &[u8])] = &descriptors![
    "auth-v1.bin",
    "actors-v1.bin",
    "filesystem-v2.bin",
    "inference-v1.bin",
    "machines-v1.bin",
    "objects-v1.bin",
    "stream-v1.bin",
    "harness-v2.bin",
];

fn decode(bytes: &[u8]) -> DynamicMessage {
    let descriptor = DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .unwrap_or_else(|| unreachable!("descriptor schema missing"));
    DynamicMessage::decode(descriptor, bytes)
        .unwrap_or_else(|error| panic!("admitted descriptor cannot be reflected: {error}"))
}

fn unknowns(file: &DynamicMessage) -> Vec<Vec<u8>> {
    let mut fields: Vec<_> = file
        .unknown_fields()
        .filter(|field| field.number() != 8042)
        .map(|field| {
            let mut bytes = Vec::new();
            field.encode(&mut bytes);
            bytes
        })
        .collect();
    fields.sort();
    fields
}

fn check_unknowns(input: &DynamicMessage, canonical: &[u8]) {
    let output = decode(canonical);
    let input_files = input
        .get_field_by_name("file")
        .unwrap_or_else(|| unreachable!("descriptor files missing"));
    let output_files = output
        .get_field_by_name("file")
        .unwrap_or_else(|| unreachable!("descriptor files missing"));
    let input_files = input_files.as_list().unwrap_or_else(|| unreachable!());
    let output_files = output_files.as_list().unwrap_or_else(|| unreachable!());
    assert_eq!(input_files.len(), output_files.len());
    for (input, output) in input_files.iter().zip(output_files) {
        let input = input.as_message().unwrap_or_else(|| unreachable!());
        let output = output.as_message().unwrap_or_else(|| unreachable!());
        let name = input
            .get_field_by_name("name")
            .unwrap_or_else(|| unreachable!("descriptor name missing"));
        assert_eq!(Some(&name), output.get_field_by_name("name").as_ref());
        if !name
            .as_str()
            .unwrap_or_default()
            .starts_with("google/protobuf/")
        {
            assert_eq!(
                unknowns(input),
                unknowns(output),
                "canonical descriptor discarded or changed unknown file fields"
            );
        }
    }
}

fuzz_target!(|bytes: &[u8]| {
    assert_eq!(WRITERS.len(), acyclic_proto_build::FAMILIES.len());
    let index = usize::from(bytes.first().copied().unwrap_or_default()) % WRITERS.len();
    let ((file, writer), (_, expected_file, roots)) = WRITERS
        .iter()
        .zip(acyclic_proto_build::FAMILIES)
        .nth(index)
        .unwrap_or_else(|| unreachable!("descriptor family missing"));
    assert_eq!(file, expected_file);
    let service = match *file {
        "actors-v1.bin" => Some("acyclic.actors.v1.ActorsService"),
        "filesystem-v2.bin" => Some("acyclic.filesystem.v2.FilesystemService"),
        "machines-v1.bin" => Some("acyclic.machines.v1.MachinesService"),
        "objects-v1.bin" => Some("acyclic.objects.v1.BlobsService"),
        "stream-v1.bin" => Some("acyclic.stream.v1.StreamService"),
        _ => None,
    };
    if let Some(service) = service {
        acyclic_proto_build::authorization(writer, service, "Method")
            .expect("captured production method admission must compile");
        // The same public metadata decoder receives arbitrary inputs. A
        // successful Rust/TypeScript projection must be deterministic.
        if let Ok(first) = acyclic_proto_build::authorization(bytes, service, "Method") {
            assert_eq!(
                acyclic_proto_build::authorization(bytes, service, "Method").ok(),
                Some(first)
            );
        }
    }
    assert_eq!(
        acyclic_proto_build::canonicalize(writer, roots)
            .ok()
            .as_deref(),
        Some(*writer),
        "canonical production writer descriptor refused or changed"
    );
    // Mutate captured compiler output while preserving protobuf framing.
    // Contracts without authorization metadata do not carry this catalog.
    let action = b"actors/invoke";
    if let Some(position) = writer
        .windows(action.len())
        .position(|value| value == action)
    {
        let mut undeclared = writer.to_vec();
        undeclared[position + action.len() - 1] = b'!';
        assert!(
            acyclic_proto_build::canonicalize(&undeclared, roots).is_err(),
            "descriptor must refuse an undeclared action"
        );
        if let Some(service) = service {
            assert!(
                acyclic_proto_build::authorization(&undeclared, service, "Method").is_err(),
                "method admission must refuse an undeclared action"
            );
        }
    }
    let mut challenge = decode(writer);
    let files = challenge
        .get_field_by_name_mut("file")
        .and_then(Value::as_list_mut)
        .unwrap_or_else(|| unreachable!());
    let file = files
        .iter_mut()
        .filter_map(Value::as_message_mut)
        .find(|file| {
            file.get_field_by_name("name").is_some_and(|name| {
                !name
                    .as_str()
                    .unwrap_or_default()
                    .starts_with("google/protobuf/")
            })
        })
        .unwrap_or_else(|| unreachable!("compiler descriptor has no original file"));
    // Extend the original compiler message through the protobuf wire writer.
    // Repetition also checks multiplicity, independently of field ordering.
    let number = 10_000 + u32::from(bytes.get(1).copied().unwrap_or_default());
    let mut extension = Vec::new();
    encode_key(number, WireType::LengthDelimited, &mut extension);
    encode_varint(bytes.len() as u64, &mut extension);
    extension.extend_from_slice(bytes);
    for _ in 0..2 {
        file.merge(extension.as_slice())
            .unwrap_or_else(|error| panic!("writer extension refused: {error}"));
    }
    let canonical = acyclic_proto_build::canonicalize(&challenge.encode_to_vec(), roots)
        .unwrap_or_else(|error| panic!("extended production writer descriptor refused: {error}"));
    check_unknowns(&challenge, &canonical);
    if let Ok(canonical) = acyclic_proto_build::canonicalize(bytes, roots) {
        check_unknowns(&decode(bytes), &canonical);
        assert_eq!(
            acyclic_proto_build::canonicalize(&canonical, roots)
                .ok()
                .as_deref(),
            Some(canonical.as_slice())
        );
    }
});
