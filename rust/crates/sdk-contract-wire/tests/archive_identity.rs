//! Regression checks for the distinction between canonical Rust schema bytes
//! and the immutable descriptor used by a deployed runtime handshake.

use acyclic_sdk_contract_wire::{
    FacadeLanguage, all_facade_operations, bindings::BindingFamily, generate_remote_facades,
    protocol,
};
use sha2::{Digest, Sha256};

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn every_runtime_handshake_uses_its_explicit_archived_fixture() {
    let families = [
        (
            "actors",
            BindingFamily::Actors,
            include_bytes!("../../actors/src/generated/acyclic-actors-v1.bin").as_slice(),
            "70720491f34232b4b7e424a17f8383ad5a69b1018460e8fff7a62600fb6ec16c",
        ),
        (
            "workers",
            BindingFamily::Workers,
            include_bytes!("../../workers/src/generated/acyclic-workers-v1.bin").as_slice(),
            "851b6cd37b8cb4baa6d3a111efdad655b89936b2e1057ecb74e62825715bd7d8",
        ),
        (
            "objects",
            BindingFamily::Objects,
            include_bytes!("../../objects/src/generated/acyclic-objects-v2.bin").as_slice(),
            "21cb9f4893ce487716645e2814ffc680b9b6db8e0f23a9ea6867851100861d6b",
        ),
        (
            "stream",
            BindingFamily::Stream,
            include_bytes!("../../stream/proto/stream/v2/stream_descriptor.bin").as_slice(),
            "f7b25aa49d033bf9300c517b940263c9ad14d1db7fbfdb6a4a9e73b5ec44c58e",
        ),
        (
            "inference",
            BindingFamily::Inference,
            include_bytes!("../../inference/inference_descriptor.bin").as_slice(),
            "21c35707beb7d3aa8c87f63ceb129083ad092010a64d9b9e82924a0f5661bf15",
        ),
        (
            "machines",
            BindingFamily::Machines,
            include_bytes!("../../machines/src/generated/acyclic-machines-v1.bin").as_slice(),
            "68feb507148fbf798a3e05236a4d93d36d216c260db0a6a339db5919c630e758",
        ),
        (
            "filesystem",
            BindingFamily::Filesystem,
            include_bytes!("../../filesystem/src/generated/acyclic-filesystem-v2.bin").as_slice(),
            "105e153060d229569836982527c91bd56a69891691007215fbc599115eca2093",
        ),
        (
            "harness",
            BindingFamily::Harness,
            include_bytes!("../../harness/src/generated/harness-archived-v2.bin").as_slice(),
            "b1d721a657f40f652a560769ff440b7a1ca44739765fd3114269b727de5474e4",
        ),
    ];

    for (name, family, expected, digest) in families {
        assert_eq!(
            family.archived_runtime_descriptor(),
            expected,
            "{name} runtime identity is not bound to its explicit archive"
        );
        assert_eq!(sha256(expected), digest, "{name} archive bytes changed");
        assert!(
            !family.model_descriptor().is_empty(),
            "{name} model is empty"
        );
    }
}

#[test]
fn protocol_handshake_fixture_is_separate_from_the_eight_service_families() {
    let expected = include_bytes!("fixtures/protocol-v1.descriptor.bin").as_slice();
    assert_eq!(protocol::protocol_descriptor(), expected);
    assert_eq!(
        sha256(expected),
        "ce697a9dede342fa869397ca984dd148d6c6375d33a0a633483615a506398660"
    );
}

#[test]
fn generated_facades_serialize_every_rust_operation_policy() {
    let operations = all_facade_operations();
    assert_eq!(operations.len(), 106);

    let outputs = generate_remote_facades();
    let targets = outputs
        .iter()
        .map(|output| output.language.name())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        targets,
        ["python", "go", "ruby", "php", "dart", "java", "csharp"]
            .into_iter()
            .collect()
    );
    assert_eq!(outputs.len(), targets.len(), "duplicate facade targets");
    for output in outputs {
        assert!(
            matches!(
                output.language,
                FacadeLanguage::Python
                    | FacadeLanguage::Go
                    | FacadeLanguage::Ruby
                    | FacadeLanguage::Php
                    | FacadeLanguage::Dart
                    | FacadeLanguage::Java
                    | FacadeLanguage::Csharp
            ),
            "unexpected facade target"
        );
        for operation in &operations {
            let mut offset = 0;
            let mut serialized_entries = 0;
            while let Some(relative) = output.source[offset..].find(operation.rpc) {
                let index = offset + relative;
                if matches!(
                    output.source.as_bytes().get(index + operation.rpc.len()),
                    Some(b'"' | b'\'')
                ) {
                    serialized_entries += 1;
                }
                offset = index + operation.rpc.len();
            }
            assert_eq!(
                serialized_entries,
                1,
                "{} does not serialize exactly one entry for {}",
                output.language.name(),
                operation.rpc
            );
            for metadata in operation
                .capabilities
                .iter()
                .chain(operation.errors)
                .chain(operation.validations)
            {
                assert!(
                    output.source.contains(metadata),
                    "{} omitted {metadata} for {}",
                    output.language.name(),
                    operation.rpc
                );
            }
        }
    }
}
