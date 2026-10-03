//! Bounded authority-migration proof for the Inference and Machines bindings.
//!
//! This test deliberately invokes the Rust model generator in an isolated
//! output directory. It never reads an authored `proto/` tree. The package
//! descriptor inputs are the boundary currently consumed by the public Rust
//! bindings; semantic comparison keeps compiler field ordering and image
//! metadata out of the migration decision while retaining custom options.

use acyclic_sdk_contract_validation::compare_bytes;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const INFERENCE_ARCHIVED: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/inference-v1.descriptor.bin"
));
const MACHINES_ARCHIVED: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/machines-v1.descriptor.bin"
));
const INFERENCE_PACKAGE_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../inference/inference_descriptor.bin"
));
const INFERENCE_MODEL_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../inference/inference_model_descriptor.bin"
));
const MACHINES_PACKAGE_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../machines/src/generated/acyclic-machines-v1.bin"
));
const MACHINES_MODEL_DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../machines/src/generated/acyclic-machines-v1.model.bin"
));

fn temporary_output() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!("acyclic-authority-migration-{nonce}"))
}

fn run_generator(command: &str, output: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_sdk-contract-wire"))
        .args([command, "--out"])
        .arg(output)
        .output()
        .expect("run Rust authority generator")
}

fn assert_success(output: Output, command: &str) {
    assert!(
        output.status.success(),
        "generator {command} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_drift(output: Output, expected: &str) {
    assert!(
        !output.status.success(),
        "generator unexpectedly accepted drift"
    );
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        report.contains(expected),
        "missing {expected:?} in {report}"
    );
}

fn assert_semantic_compatibility(name: &str, baseline: &[u8], candidate: &[u8]) {
    let report = compare_bytes(baseline, candidate)
        .unwrap_or_else(|error| panic!("{name} descriptor comparison failed: {error}"));
    assert!(
        report.semantic_compatible,
        "{name} descriptor drift: {:?}",
        report.differences
    );
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn model_descriptors_match_public_binding_inputs_and_archived_handshakes() {
    let output = temporary_output();
    assert_success(run_generator("generate", &output), "generate");

    let inference = fs::read(output.join("inference/v1/inference.fds.bin"))
        .expect("generated Inference descriptor");
    let machines = fs::read(output.join("machines/v1/machines.fds.bin"))
        .expect("generated Machines descriptor");

    for (name, generated, model, package, archived) in [
        (
            "Inference",
            inference.as_slice(),
            INFERENCE_MODEL_DESCRIPTOR,
            INFERENCE_PACKAGE_DESCRIPTOR,
            INFERENCE_ARCHIVED,
        ),
        (
            "Machines",
            machines.as_slice(),
            MACHINES_MODEL_DESCRIPTOR,
            MACHINES_PACKAGE_DESCRIPTOR,
            MACHINES_ARCHIVED,
        ),
    ] {
        assert_eq!(generated, model, "{name} product model descriptor drifted");
        assert_semantic_compatibility(name, package, generated);
        assert_semantic_compatibility(name, archived, generated);
    }

    assert_eq!(
        sha256(INFERENCE_ARCHIVED),
        "21c35707beb7d3aa8c87f63ceb129083ad092010a64d9b9e82924a0f5661bf15"
    );
    assert_eq!(
        sha256(MACHINES_ARCHIVED),
        "68feb507148fbf798a3e05236a4d93d36d216c260db0a6a339db5919c630e758"
    );

    let _ = fs::remove_dir_all(output);
}

#[test]
fn source_mutation_regeneration_restores_public_binding_inputs() {
    let output = temporary_output();
    assert_success(run_generator("generate", &output), "generate");

    let inference_proto = output.join("inference/v1/inference.proto");
    let inference_descriptor = output.join("inference/v1/inference.fds.bin");
    let machines_descriptor = output.join("machines/v1/machines.fds.bin");
    let original_proto = fs::read(&inference_proto).expect("generated Inference source");
    let original_inference_descriptor =
        fs::read(&inference_descriptor).expect("generated Inference descriptor");
    let original_machines_descriptor =
        fs::read(&machines_descriptor).expect("generated Machines descriptor");

    let mut mutated_proto = original_proto.clone();
    mutated_proto.extend_from_slice(b"\n// migration mutation\n");
    fs::write(&inference_proto, mutated_proto).expect("mutate generated source");
    assert_drift(
        run_generator("check", &output),
        "generated protobuf source drifted",
    );
    assert_success(
        run_generator("generate", &output),
        "regenerate source mutation",
    );
    assert_eq!(
        fs::read(&inference_proto).expect("restored Inference source"),
        original_proto
    );

    let mut mutated_descriptor = original_machines_descriptor.clone();
    mutated_descriptor.push(0);
    fs::write(&machines_descriptor, mutated_descriptor).expect("mutate generated descriptor");
    assert_drift(
        run_generator("check", &output),
        "generated descriptor drifted",
    );
    assert_success(
        run_generator("generate", &output),
        "regenerate descriptor mutation",
    );
    assert_eq!(
        fs::read(&inference_descriptor).expect("restored Inference descriptor"),
        original_inference_descriptor
    );
    assert_eq!(
        fs::read(&machines_descriptor).expect("restored Machines descriptor"),
        original_machines_descriptor
    );
    assert_success(
        run_generator("check", &output),
        "check regenerated authority",
    );

    let _ = fs::remove_dir_all(output);
}
