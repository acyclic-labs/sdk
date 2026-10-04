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
            "0c01551deb2a5413c338b5c0bab7249d90b40b04c7b6829d90cb44bb0027e4d7",
        ),
        (
            "workers",
            BindingFamily::Workers,
            include_bytes!("../../workers/src/generated/acyclic-workers-v1.bin").as_slice(),
            "ec65edb7279ea99de68945e776628c7ba842be0a81a4fb872876d8f82db2f147",
        ),
        (
            "objects",
            BindingFamily::Objects,
            include_bytes!("../../objects/src/generated/acyclic-objects-v2.bin").as_slice(),
            "601b092b87d702b9aad1c0fa1615af623f19c59067ea9c52e2cb61db34994b1b",
        ),
        (
            "stream",
            BindingFamily::Stream,
            include_bytes!("../../stream/proto/stream/v2/stream_descriptor.bin").as_slice(),
            "0253a46e0e6f565aae140bbe7237581dd45c959c2d0882c822e3bff6a58769fe",
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
            "3baefd3633485a2eaa989f02fb24af04c02764abb7fc215c42971311ab4b8fdd",
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
        assert!(!family.model_descriptor().is_empty(), "{name} model is empty");
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

    for output in generate_remote_facades() {
        assert!(
            matches!(
                output.language,
                FacadeLanguage::Ruby
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
