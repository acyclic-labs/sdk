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
    transport_control::{control_descriptor, control_proto},
    type_policy::TypePolicyLanguage,
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
        ("transport/v1/transport.proto", control_proto().into_bytes()),
        ("transport/v1/transport.fds.bin", control_descriptor()),
        (
            "inference/v1/inference.proto",
            inference_proto().into_bytes(),
        ),
        ("inference/v1/inference.fds.bin", inference_descriptor()),
        ("machines/v1/machines.proto", machines_proto().into_bytes()),
        ("machines/v1/machines.fds.bin", machines_descriptor()),
        ("rust-authority.json", Vec::new()),
        ("rust-family-goldens.json", Vec::new()),
        ("type-policy.json", Vec::new()),
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
            assert_rust_family_goldens(root, &fs::read(root.join(relative)).expect("family goldens"));
        } else if relative == "type-policy.json" {
            assert_type_policy(&fs::read(root.join(relative)).expect("type policy"));
        } else {
            assert_eq!(
                fs::read(root.join(relative)).expect("generated artifact"),
                expected_bytes,
                "generated artifact drifted: {relative}"
            );
        }
    }
}

fn assert_type_policy(bytes: &[u8]) {
    let document: serde_json::Value =
        serde_json::from_slice(bytes).expect("Rust type policy must be valid JSON");
    assert_eq!(document["schema"], "acyclic.sdk.type-policy.v1");
    assert_eq!(
        document["source"],
        "rust/crates/sdk-contract-wire/src/type_policy.rs"
    );
    let languages = document["languages"]
        .as_array()
        .expect("type policy languages array");
    let actual_languages = languages
        .iter()
        .map(|language| {
            language["language"]
                .as_str()
                .expect("type policy language id")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    let expected_languages = TypePolicyLanguage::ALL
        .iter()
        .map(|language| language.id().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        languages.len(),
        actual_languages.len(),
        "type policy language inventory contains duplicates"
    );
    assert_eq!(actual_languages, expected_languages);
    for section in [
        "semantic_types",
        "field_mappings",
        "public_field_bindings",
        "union_variants",
        "jvm_union_variants",
        "rpc_methods",
        "enum_fields",
        "oneof_members",
        "presence_fields",
        "operation_rules",
        "public_nested_routes",
    ] {
        assert!(
            document[section].as_array().is_some(),
            "type policy is missing Rust-owned section {section}"
        );
    }
}

fn assert_rust_family_goldens(root: &Path, bytes: &[u8]) {
    let authority = fs::read(root.join("rust-authority.json")).expect("authority manifest");
    let authority_hash = format!("{:x}", Sha256::digest(authority));
    let goldens: Vec<serde_json::Value> =
        serde_json::from_slice(bytes).expect("Rust family goldens must be valid JSON");
    let expected = [
        (
            "actors",
            "acyclic.actors.v1.ActorLimits",
            "handlerTimeoutMillis",
            "08ffffffffffffffffff01",
            r#"{"handlerTimeoutMillis":"18446744073709551615"}"#,
        ),
        (
            "stream",
            "acyclic.stream.v2.Record",
            "sequence",
            "08ffffffffffffffffff01",
            r#"{"sequence":"18446744073709551615"}"#,
        ),
        (
            "objects",
            "acyclic.objects.v2.ObjectInfo",
            "size",
            "10ffffffffffffffffff01",
            r#"{"size":"18446744073709551615"}"#,
        ),
        (
            "workers",
            "acyclic.workers.v1.CodeVersion",
            "sizeBytes",
            "10ffffffffffffffffff01",
            r#"{"sizeBytes":"18446744073709551615"}"#,
        ),
        (
            "filesystem",
            "acyclic.filesystem.v2.WorkspaceContextSnapshot",
            "revision",
            "10ffffffffffffffffff01",
            r#"{"revision":"18446744073709551615"}"#,
        ),
        (
            "harness",
            "acyclic.harness.v2.OperationStatus",
            "revision",
            "38ffffffffffffffffff01",
            r#"{"revision":"18446744073709551615"}"#,
        ),
        (
            "inference",
            "inference.customer.v1.ModelCapability",
            "maximumContext",
            "18ffffffffffffffffff01",
            r#"{"maximumContext":"18446744073709551615"}"#,
        ),
        (
            "machines",
            "acyclic.machines.v1.SuspensionPolicy",
            "afterIdleMs",
            "10ffffffffffffffffff01",
            r#"{"afterIdleMs":"18446744073709551615"}"#,
        ),
        (
            "protocol",
            "acyclic.protocol.v1.ProtocolIdentity",
            "version",
            "0a0b727573742d676f6c64656e",
            r#"{"version":"rust-golden"}"#,
        ),
    ];
    assert_eq!(goldens.len(), expected.len(), "Rust family golden count drifted");
    for (golden, (family, message, field, wire_hex, json)) in goldens.iter().zip(expected) {
        assert_eq!(golden["family"], family);
        assert_eq!(
            golden["kind"],
            if family == "protocol" {
                "string"
            } else {
                "uint64"
            }
        );
        assert_eq!(golden["authority_manifest_sha256"], authority_hash);
        assert_eq!(golden["message"], message);
        assert_eq!(golden["field"], field);
        assert_eq!(
            golden["value"],
            if family == "protocol" {
                "rust-golden"
            } else {
                "18446744073709551615"
            }
        );
        assert_eq!(golden["wire_hex"], wire_hex);
        assert_eq!(golden["json"], json);
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
        !manifest.contains("proto/inference") && !manifest.contains("proto/machines"),
        "generation authority must not consult authored proto paths"
    );
    let _ = fs::remove_dir_all(output);
}
