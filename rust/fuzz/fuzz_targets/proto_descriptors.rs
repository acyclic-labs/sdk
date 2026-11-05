//! Fuzz descriptor identity canonicalization and protobuf round-trip stability.
#![no_main]
use libfuzzer_sys::fuzz_target;

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
    if let Ok(canonical) = acyclic_proto_build::canonicalize(bytes, roots) {
        assert_eq!(
            acyclic_proto_build::canonicalize(&canonical, roots)
                .ok()
                .as_deref(),
            Some(canonical.as_slice())
        );
    }
});
