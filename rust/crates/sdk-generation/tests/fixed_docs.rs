use sdk_docs::{BuildInput, Channel, build_data, write_bundle};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn temp(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("{name}-{nonce}"))
}

fn git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run(
    binary: &Path,
    operation: &str,
    root: &Path,
    rustdoc: &Path,
    output: &Path,
) -> std::process::Output {
    Command::new(binary)
        .args([
            operation,
            "--root",
            root.to_str().unwrap(),
            "--rustdoc-json",
            rustdoc.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--version",
            "0.2.0",
            "--channel",
            "release",
        ])
        .output()
        .unwrap()
}

#[test]
fn fixed_docs_stage_binds_git_source_and_rejects_drift() {
    let root = temp("sdk-generation-root");
    let rustdoc = temp("sdk-generation-rustdoc");
    let first = temp("sdk-generation-first");
    let second = temp("sdk-generation-second");
    for path in [
        "rust/crates/actors/src",
        "rust/crates/actors/examples",
        "rust/crates/sdk-docs/src",
        "rust/crates/sdk-generation/src",
        "rust/crates/sdk-generation/tests",
        "docs",
    ] {
        fs::create_dir_all(root.join(path)).unwrap();
    }
    for path in [
        "rust/crates/actors/src/lib.rs",
        "rust/crates/actors/examples/example.rs",
        "rust/crates/sdk-docs/src/lib.rs",
        "rust/crates/sdk-generation/src/main.rs",
        "rust/crates/sdk-generation/tests/fixed_docs.rs",
        "docs/objects-v2-http.md",
        "docs/rust-source-generation.md",
    ] {
        fs::write(root.join(path), "rust-owned source\n").unwrap();
    }
    fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
    fs::write(root.join("rust/crates/actors/Cargo.toml"), "[package]\n").unwrap();
    fs::write(root.join("rust/crates/actors/README.md"), "Actors\n").unwrap();
    fs::write(root.join("rust/crates/sdk-docs/Cargo.toml"), "[package]\n").unwrap();
    fs::write(
        root.join("rust/crates/sdk-generation/Cargo.toml"),
        "[package]\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/sdk-generation/README.md"),
        "generation launcher\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/sdk-generation/rust-toolchain.toml"),
        "[toolchain]\nchannel = \"1.98.1\"\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/sdk-generation/Cargo.lock"),
        "generation lock\n",
    )
    .unwrap();
    fs::write(root.join("Cargo.lock"), "root lock\n").unwrap();
    fs::write(root.join("rust/crates/sdk-docs/Cargo.lock"), "docs lock\n").unwrap();
    fs::write(
        root.join("rust-toolchain.toml"),
        "[toolchain]\nchannel = \"1.98.1\"\n",
    )
    .unwrap();
    fs::write(
        root.join("docs/rust-source-generation.md"),
        "generation guide\n",
    )
    .unwrap();
    fs::write(root.join(".gitignore"), "rust/crates/sdk-docs/target/\n").unwrap();
    fs::create_dir_all(&rustdoc).unwrap();
    let fixture = json!({
        "root": 0,
        "crate_version": null,
        "includes_private": false,
        "index": {
            "0": {"id": 0, "crate_id": 0, "name": "demo", "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": true, "items": [1], "is_stripped": false}}},
            "1": {"id": 1, "crate_id": 0, "name": "visible", "span": null, "visibility": "public", "docs": "visible", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}}
        },
        "paths": {"1": {"crate_id": 0, "path": ["demo", "visible"], "kind": "function"}},
        "external_crates": {},
        "target": {"triple": "x86_64-pc-windows-msvc", "target_features": []},
        "format_version": 60
    });
    fs::write(
        rustdoc.join("actors.json"),
        serde_json::to_vec(&fixture).unwrap(),
    )
    .unwrap();
    git(&root, &["init", "--quiet"]);
    git(&root, &["config", "user.email", "fixture@example.invalid"]);
    git(&root, &["config", "user.name", "fixture"]);
    git(&root, &["add", "."]);
    git(&root, &["commit", "--quiet", "-m", "fixture"]);
    fs::write(
        root.join("rust/crates/actors/src/lib.rs"),
        "dirty release source\n",
    )
    .unwrap();
    let binary = Path::new(env!("CARGO_BIN_EXE_sdk-generation"));
    assert!(
        !run(binary, "generate", &root, &rustdoc, &first)
            .status
            .success()
    );
    fs::write(
        root.join("rust/crates/actors/src/lib.rs"),
        "rust-owned source\n",
    )
    .unwrap();
    let duplicate = Command::new(binary)
        .args([
            "generate",
            "--root",
            root.to_str().unwrap(),
            "--rustdoc-json",
            rustdoc.to_str().unwrap(),
            "--output",
            first.to_str().unwrap(),
            "--version",
            "0.2.0",
            "--channel",
            "release",
            "--channel",
            "release",
        ])
        .output()
        .unwrap();
    assert!(!duplicate.status.success());
    for output in [&first, &second] {
        let result = run(binary, "generate", &root, &rustdoc, output);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert_eq!(
        fs::read(first.join("generation-manifest.json")).unwrap(),
        fs::read(second.join("generation-manifest.json")).unwrap()
    );
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(first.join("generation-manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["tool"]["id"], "sdk-docs-library");
    assert_eq!(manifest["tool"]["version"], "0.2.0");
    assert_eq!(manifest["tool"]["channel"], "release");
    assert!(manifest["tool"].get("args").is_none());
    assert_eq!(
        fs::read(first.join("releases/0.2.0/sdk-docs-data.v1.json")).unwrap(),
        fs::read(second.join("releases/0.2.0/sdk-docs-data.v1.json")).unwrap()
    );
    assert_eq!(
        fs::read(first.join("releases/0.2.0/sdk-docs-data.v1.schema.json")).unwrap(),
        fs::read(second.join("releases/0.2.0/sdk-docs-data.v1.schema.json")).unwrap()
    );
    let index: serde_json::Value =
        serde_json::from_slice(&fs::read(first.join("sdk-docs-versions.v1.json")).unwrap())
            .unwrap();
    assert_eq!(index["releases"][0]["version"], "0.2.0");
    let data = fs::read_to_string(first.join("releases/0.2.0/sdk-docs-data.v1.json")).unwrap();
    assert!(data.contains("sdk-docs-data.v1"));
    let guide_path = root.join("docs/objects-v2-http.md");
    let guide = fs::read(&guide_path).unwrap();
    fs::write(&guide_path, b"tampered guide\n").unwrap();
    assert!(
        !run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    fs::write(&guide_path, guide).unwrap();
    assert!(
        run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    let data_path = first.join("releases/0.2.0/sdk-docs-data.v1.json");
    let data_bytes = fs::read(&data_path).unwrap();
    fs::write(&data_path, b"tampered generated data\n").unwrap();
    assert!(
        !run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    fs::write(&data_path, data_bytes).unwrap();
    assert!(
        run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    let schema_path = first.join("releases/0.2.0/sdk-docs-data.v1.schema.json");
    let schema_bytes = fs::read(&schema_path).unwrap();
    fs::write(&schema_path, b"tampered generated schema\n").unwrap();
    assert!(
        !run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    fs::write(&schema_path, schema_bytes).unwrap();
    assert!(
        run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    let history = build_data(&BuildInput {
        version: "0.3.0".into(),
        channel: Channel::Release,
        revision: "0123456789abcdef0123456789abcdef01234567".into(),
        source_state: "captured-snapshot".into(),
        repository_root: root.clone(),
        rustdoc_files: vec![rustdoc.join("actors.json")],
        mark_latest: true,
    })
    .unwrap();
    write_bundle(&history, &first, true).unwrap();
    let history_index: serde_json::Value =
        serde_json::from_slice(&fs::read(first.join("sdk-docs-versions.v1.json")).unwrap())
            .unwrap();
    assert_eq!(history_index["releases"].as_array().unwrap().len(), 2);
    assert_eq!(history_index["latest"]["version"], "0.3.0");
    let conflicting_history = build_data(&BuildInput {
        version: "0.3.0".into(),
        channel: Channel::Release,
        revision: "fedcba9876543210fedcba9876543210fedcba98".into(),
        source_state: "captured-snapshot".into(),
        repository_root: root.clone(),
        rustdoc_files: vec![rustdoc.join("actors.json")],
        mark_latest: true,
    })
    .unwrap();
    assert!(write_bundle(&conflicting_history, &first, true).is_err());
    let guarded_index: serde_json::Value =
        serde_json::from_slice(&fs::read(first.join("sdk-docs-versions.v1.json")).unwrap())
            .unwrap();
    assert_eq!(guarded_index["releases"].as_array().unwrap().len(), 2);
    assert_eq!(guarded_index["latest"]["version"], "0.3.0");
    fs::write(root.join("rust/crates/actors/src/lib.rs"), "tampered\n").unwrap();
    assert!(
        !run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    fs::write(
        root.join("rust/crates/actors/src/lib.rs"),
        "rust-owned source\n",
    )
    .unwrap();
    fs::write(rustdoc.join("actors.json"), b"tampered\n").unwrap();
    assert!(
        !run(binary, "drift", &root, &rustdoc, &first)
            .status
            .success()
    );
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(rustdoc);
    let _ = fs::remove_dir_all(first);
    let _ = fs::remove_dir_all(second);
}
