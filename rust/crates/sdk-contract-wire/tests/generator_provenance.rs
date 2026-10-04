use acyclic_sdk_contract_options::options_proto;
use acyclic_sdk_contract_wire::{
    actors_descriptor, actors_proto,
    filesystem::{filesystem_descriptor, filesystem_proto},
    harness::{harness_descriptor, harness_proto},
    inference::{inference_descriptor, inference_proto},
    machines::{machines_descriptor, machines_proto},
    objects::{objects_descriptor, objects_proto},
    protocol::{protocol_descriptor, protocol_proto},
    stream::{stream_descriptor, stream_proto},
    workers::{workers_descriptor, workers_proto},
};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use sha2::{Digest, Sha256};

fn temporary_output(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("acyclic-sdk-wire-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("temporary generator output");
    path
}

fn run_generator(command: &str, output: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sdk-contract-wire"))
        .args([command, "--out"])
        .arg(output)
        .output()
        .expect("sdk-contract-wire process")
}

fn generated_files(root: &Path) -> BTreeSet<String> {
    fn visit(root: &Path, current: &Path, files: &mut BTreeSet<String>) {
        for entry in fs::read_dir(current).expect("generated output directory") {
            let entry = entry.expect("generated output entry");
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .expect("generated path under output")
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    let mut files = BTreeSet::new();
    visit(root, root, &mut files);
    files
}

fn expected_artifacts() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("validation/v1/options.proto", options_proto().into_bytes()),
        ("actors/v1/actors.proto", actors_proto().into_bytes()),
        ("actors/v1/actors.fds.bin", actors_descriptor()),
        ("stream/v2/stream.proto", stream_proto().into_bytes()),
        ("stream/v2/stream.fds.bin", stream_descriptor()),
        ("objects/v2/objects.proto", objects_proto().into_bytes()),
        ("objects/v2/objects.fds.bin", objects_descriptor()),
        ("workers/v1/workers.proto", workers_proto().into_bytes()),
        ("workers/v1/workers.fds.bin", workers_descriptor()),
        (
            "filesystem/v2/filesystem.proto",
            filesystem_proto().into_bytes(),
        ),
        ("filesystem/v2/filesystem.fds.bin", filesystem_descriptor()),
        ("harness/v2/harness.proto", harness_proto().into_bytes()),
        ("harness/v2/harness.fds.bin", harness_descriptor()),
        ("protocol/v1/protocol.proto", protocol_proto().into_bytes()),
        ("protocol/v1/protocol.fds.bin", protocol_descriptor()),
        (
            "inference/v1/inference.proto",
            inference_proto().into_bytes(),
        ),
        ("inference/v1/inference.fds.bin", inference_descriptor()),
        ("machines/v1/machines.proto", machines_proto().into_bytes()),
        ("machines/v1/machines.fds.bin", machines_descriptor()),
        ("rust-authority.json", Vec::new()),
        ("rust-family-goldens.json", Vec::new()),
    ]
}

fn assert_clean_output(root: &Path) {
    let expected = expected_artifacts();
    let expected_names = expected
        .iter()
        .map(|(path, _)| (*path).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        generated_files(root),
        expected_names,
        "generated artifact set drifted"
    );
    for (relative, expected_bytes) in expected {
        if relative == "rust-authority.json" {
            let manifest = fs::read_to_string(root.join(relative)).expect("authority manifest");
            assert!(manifest.contains("\"authority\": \"rust\""));
            assert!(manifest.contains("\"source_revision\""));
        } else if relative == "rust-family-goldens.json" {
            let manifest = fs::read_to_string(root.join("rust-authority.json"))
                .expect("authority manifest for family goldens");
            let manifest_hash = format!("{:x}", Sha256::digest(manifest.as_bytes()));
            let goldens = fs::read_to_string(root.join(relative)).expect("family goldens");
            assert!(goldens.starts_with("[\n") && goldens.ends_with("]\n"));
            assert!(goldens.contains(&format!(
                "\"authority_manifest_sha256\":\"{manifest_hash}\""
            )));
            for family in [
                "actors", "stream", "objects", "workers", "filesystem", "harness",
                "inference", "machines", "protocol",
            ] {
                assert!(goldens.contains(&format!("\"family\":\"{family}\"")));
            }
        } else {
            assert_eq!(
                fs::read(root.join(relative)).expect("generated artifact"),
                expected_bytes,
                "generated artifact drifted: {relative}"
            );
        }
    }
}

#[test]
fn clean_generation_has_exact_family_set_and_model_bytes() {
    let output = temporary_output("clean");
    let generated = run_generator("generate", &output);
    assert!(
        generated.status.success(),
        "clean generation failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    assert_clean_output(&output);
    let checked = run_generator("check", &output);
    assert!(
        checked.status.success(),
        "clean output failed check: {}",
        String::from_utf8_lossy(&checked.stderr)
    );
    let _ = fs::remove_dir_all(output);
}

#[test]
fn check_rejects_missing_extra_and_edited_artifacts() {
    let output = temporary_output("drift");
    let generated = run_generator("generate", &output);
    assert!(
        generated.status.success(),
        "clean generation failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );

    fs::remove_file(output.join("actors/v1/actors.proto")).expect("remove generated source");
    let missing = run_generator("check", &output);
    assert!(
        !missing.status.success(),
        "check recovered a removed source"
    );
    assert!(String::from_utf8_lossy(&missing.stderr).contains("cannot read"));

    let regenerated = run_generator("generate", &output);
    assert!(regenerated.status.success(), "restore generation failed");
    fs::write(
        output.join("actors/v1/actors.fds.bin"),
        [actors_descriptor(), vec![0x00]].concat(),
    )
    .expect("edit generated descriptor");
    let edited = run_generator("check", &output);
    assert!(
        !edited.status.success(),
        "check recovered an edited descriptor"
    );
    assert!(String::from_utf8_lossy(&edited.stderr).contains("descriptor drifted"));

    let regenerated = run_generator("generate", &output);
    assert!(regenerated.status.success(), "restore generation failed");
    fs::write(output.join("unexpected.bin"), b"extra").expect("add extra artifact");
    let extra = run_generator("check", &output);
    assert!(!extra.status.success(), "check accepted an extra artifact");
    assert!(String::from_utf8_lossy(&extra.stderr).contains("extra"));

    let _ = fs::remove_dir_all(output);
}

#[test]
fn authority_manifest_binds_rust_models_without_authored_proto_inputs() {
    let output = temporary_output("authority");
    let generated = run_generator("generate", &output);
    assert!(
        generated.status.success(),
        "generation failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );

    let manifest = fs::read_to_string(output.join("rust-authority.json"))
        .expect("authority manifest from Rust model generation");
    assert!(manifest.contains("\"authority\": \"rust\""));
    assert!(manifest.contains("rust/crates/sdk-contract-wire/src/inference.rs"));
    assert!(manifest.contains("rust/crates/sdk-contract-wire/src/machines.rs"));
    // Runtime credential policy and registry selection must participate in
    // provenance even when their edits leave canonical descriptor bytes intact.
    for (relative, bytes) in [
        (
            "rust/crates/sdk-contract-wire/src/family_registry.rs",
            include_bytes!("../src/family_registry.rs").as_slice(),
        ),
        (
            "rust/crates/sdk-contract-wire/src/credential.rs",
            include_bytes!("../src/credential.rs").as_slice(),
        ),
    ] {
        use sha2::{Digest, Sha256};
        let expected = format!("{:x}", Sha256::digest(bytes));
        assert!(
            manifest.contains(&format!("\"{relative}\": \"{expected}\"")),
            "missing policy source identity: {relative}"
        );
    }
    assert!(manifest.contains("\"descriptor_role\": \"canonical_schema\""));
    assert!(
        !manifest.contains("\"handshake_descriptor\": null"),
        "every family must bind an explicit immutable handshake descriptor"
    );
    for (path, bytes) in [
        (
            "rust/crates/actors/src/generated/acyclic-actors-v1.bin",
            include_bytes!("../../actors/src/generated/acyclic-actors-v1.bin").as_slice(),
        ),
        (
            "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
            include_bytes!("../../stream/proto/stream/v2/stream_descriptor.bin").as_slice(),
        ),
        (
            "rust/crates/objects/src/generated/acyclic-objects-v2.bin",
            include_bytes!("../../objects/src/generated/acyclic-objects-v2.bin").as_slice(),
        ),
        (
            "rust/crates/workers/src/generated/acyclic-workers-v1.bin",
            include_bytes!("../../workers/src/generated/acyclic-workers-v1.bin").as_slice(),
        ),
        (
            "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
            include_bytes!("../../filesystem/src/generated/acyclic-filesystem-v2.bin").as_slice(),
        ),
        (
            "rust/crates/harness/src/generated/harness-archived-v2.bin",
            include_bytes!("../../harness/src/generated/harness-archived-v2.bin").as_slice(),
        ),
        (
            "rust/crates/sdk-contract-wire/tests/fixtures/protocol-v1.descriptor.bin",
            include_bytes!("fixtures/protocol-v1.descriptor.bin").as_slice(),
        ),
        (
            "rust/crates/inference/inference_descriptor.bin",
            include_bytes!("../../inference/inference_descriptor.bin").as_slice(),
        ),
        (
            "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
            include_bytes!("../../machines/src/generated/acyclic-machines-v1.bin").as_slice(),
        ),
    ] {
        let digest = format!("{:x}", Sha256::digest(bytes));
        assert!(
            manifest.contains(&format!("\"handshake_descriptor\": \"{path}\"")),
            "missing handshake descriptor identity: {path}"
        );
        assert!(
            manifest.contains(&format!("\"handshake_descriptor_sha256\": \"{digest}\"")),
            "missing handshake descriptor digest: {path}"
        );
    }
    assert!(
        !manifest.contains("proto/inference") && !manifest.contains("proto/machines"),
        "generation authority must not consult authored proto paths"
    );
    let _ = fs::remove_dir_all(output);
}
