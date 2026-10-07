use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    root: PathBuf,
    rustdoc: PathBuf,
    output: PathBuf,
}

fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn hash_file(path: &Path) -> String {
    hash_bytes(&fs::read(path).unwrap())
}

fn source_files(root: &Path, include_plugin: bool) -> BTreeMap<String, String> {
    let paths = [
        "Cargo.lock",
        "Cargo.toml",
        "release/cargo-crates.json",
        "rust/crates/demo/Cargo.toml",
        "rust/crates/demo/src/lib.rs",
    ];
    let mut files = paths
        .into_iter()
        .map(|relative| (relative.to_string(), hash_file(&root.join(relative))))
        .collect::<BTreeMap<_, _>>();
    if include_plugin {
        files.insert(
            "plugin/src/main.rs".to_string(),
            hash_file(&root.join("plugin/src/main.rs")),
        );
    }
    files
}

fn source_digest(files: &BTreeMap<String, String>) -> String {
    let mut bytes = Vec::new();
    for (path, digest) in files {
        bytes.extend_from_slice(path.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(digest.as_bytes());
        bytes.push(0);
    }
    hash_bytes(&bytes)
}

fn git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fixture(include_plugin: bool) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "sdk-generation-cli-regression-{}-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        if include_plugin { "plugin" } else { "base" }
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("rust/crates/demo/src")).unwrap();
    fs::create_dir_all(root.join("src")).unwrap();
    fs::create_dir_all(root.join("release")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = \"demo-docs\"\nversion = \"1.0.0\"\nedition = \"2021\"\npublish = true\n\n[workspace]\n",
    )
    .unwrap();
    fs::write(root.join("Cargo.lock"), "version = 3\n").unwrap();
    fs::write(root.join("src/lib.rs"), "pub fn fixture() {}\n").unwrap();
    fs::write(root.join("release/cargo-crates.json"), "[\"demo-docs\"]\n").unwrap();
    fs::write(
        root.join("rust/crates/demo/Cargo.toml"),
        "[package]\nname = \"demo\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/demo/src/lib.rs"),
        "pub fn demo() -> u64 { 7 }\n",
    )
    .unwrap();
    if include_plugin {
        fs::create_dir_all(root.join("plugin/src")).unwrap();
        fs::write(root.join("plugin/src/main.rs"), "fn main() {}\n").unwrap();
    }
    git(&root, &["init", "--quiet"]);
    git(
        &root,
        &[
            "config",
            "user.email",
            "sdk-generation-test@example.invalid",
        ],
    );
    git(&root, &["config", "user.name", "sdk-generation-test"]);
    git(&root, &["add", "."]);
    git(&root, &["commit", "--quiet", "-m", "fixture"]);

    let rustdoc = root.join("rustdoc");
    fs::create_dir_all(&rustdoc).unwrap();
    fs::write(rustdoc.join("demo_docs.json"), minimal_rustdoc()).unwrap();
    let output = root.join("output");
    let files = source_files(&root, include_plugin);
    fs::write(
        rustdoc.join("demo_docs.source.sha256"),
        format!("{}\n", source_digest(&files)),
    )
    .unwrap();
    Fixture {
        root,
        rustdoc,
        output,
    }
}

fn minimal_rustdoc() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "root": 0,
        "crate_version": "1.0.0",
        "includes_private": false,
        "index": {
            "0": {
                "id": 0,
                "crate_id": 0,
                "name": "demo_docs",
                "span": null,
                "visibility": "public",
                "docs": null,
                "links": {},
                "attrs": [],
                "deprecation": null,
                "stability": null,
                "const_stability": null,
                "inner": {
                    "module": {
                        "is_crate": true,
                        "items": [],
                        "is_stripped": false
                    }
                }
            }
        },
        "paths": {},
        "external_crates": {},
        "target": {"triple": "x86_64-unknown-linux-gnu", "target_features": []},
        "format_version": 60
    }))
    .unwrap()
}

fn manifest(root: &Path, include_plugin: bool, source_state: &str) -> Value {
    let files = source_files(root, include_plugin);
    let rustdoc = root.join("rustdoc/demo_docs.json");
    let output = root.join("output");
    fs::create_dir_all(&output).unwrap();
    let artifact = output.join("artifact.txt");
    fs::write(&artifact, b"artifact\n").unwrap();
    json!({
        "schema": "sdk-generation.manifest.v1",
        "generator": "sdk-generation-test",
        "rustdocTool": "rustdoc-test",
        "cargoMetadata": "cargo-metadata-test",
        "version": "probe",
        "channel": "preview",
        "revision": git_revision(root),
        "sourceState": source_state,
        "sourceSha256": source_digest(&files),
        "sourceFiles": files,
        "rustdocRoot": root.join("rustdoc").to_string_lossy(),
        "rustdocFiles": {"rustdoc/demo_docs.json": hash_file(&rustdoc)},
        "profileAvailability": "sdk-docs-profile-availability.v1.json",
        "scenarios": "sdk-docs-scenarios.v1.json",
        "artifacts": {"artifact.txt": hash_file(&artifact)}
    })
}

fn git_revision(root: &Path) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn write_manifest(output: &Path, value: &Value) {
    fs::create_dir_all(output).unwrap();
    let bytes = serde_json::to_vec_pretty(value).unwrap();
    fs::write(output.join("generation-manifest.v1.json"), &bytes).unwrap();
    fs::write(
        output.join("generation-manifest.v1.sha256"),
        format!("{}\n", hash_bytes(&bytes)),
    )
    .unwrap();
}

fn run_drift(fixture: &Fixture) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sdk-generation"))
        .args([
            "drift",
            "--root",
            fixture.root.to_str().unwrap(),
            "--output",
            fixture.output.to_str().unwrap(),
            "--rustdoc-dir",
            fixture.rustdoc.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

fn run_generate(
    root: &Path,
    rustdoc: &Path,
    output: &Path,
    version: &str,
    channel: &str,
    skip_scenarios: bool,
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sdk-generation"));
    command.args([
        "generate",
        "--root",
        root.to_str().unwrap(),
        "--output",
        output.to_str().unwrap(),
        "--rustdoc-dir",
        rustdoc.to_str().unwrap(),
        "--version",
        version,
        "--channel",
        channel,
    ]);
    if skip_scenarios {
        command.arg("--skip-scenarios");
    }
    command.output().unwrap()
}

#[test]
fn source_state_tamper_is_rejected() {
    let fixture = fixture(false);
    let mut value = manifest(&fixture.root, false, "working-tree");
    write_manifest(&fixture.output, &value);
    assert!(run_drift(&fixture).status.success());

    value["sourceState"] = Value::String("captured-snapshot".to_string());
    write_manifest(&fixture.output, &value);
    let result = run_drift(&fixture);
    assert!(!result.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(message.contains("source state"), "{message}");
}

#[test]
fn plugin_source_is_part_of_manifest_closure() {
    let fixture = fixture(true);
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "plugin-source-closure",
        "preview",
        true,
    );
    assert!(
        result.status.success(),
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let generated: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    assert!(generated["sourceFiles"]
        .get("plugin/src/main.rs")
        .and_then(Value::as_str)
        .is_some());
    assert!(run_drift(&fixture).status.success());
}

#[test]
fn stale_source_attestation_is_rejected_before_generate() {
    let fixture = fixture(false);
    let first = Command::new(env!("CARGO_BIN_EXE_sdk-generation"))
        .args([
            "generate",
            "--root",
            fixture.root.to_str().unwrap(),
            "--output",
            fixture.output.to_str().unwrap(),
            "--rustdoc-dir",
            fixture.rustdoc.to_str().unwrap(),
            "--version",
            "stale-attestation",
            "--channel",
            "preview",
            "--skip-scenarios",
        ])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );

    fs::write(
        fixture.root.join("rust/crates/demo/src/lib.rs"),
        "pub fn demo() -> u64 { 8 }\n",
    )
    .unwrap();
    let second_output = fixture.root.join("stale-output");
    let second = Command::new(env!("CARGO_BIN_EXE_sdk-generation"))
        .args([
            "generate",
            "--root",
            fixture.root.to_str().unwrap(),
            "--output",
            second_output.to_str().unwrap(),
            "--rustdoc-dir",
            fixture.rustdoc.to_str().unwrap(),
            "--version",
            "stale-attestation",
            "--channel",
            "preview",
            "--skip-scenarios",
        ])
        .output()
        .unwrap();
    assert!(!second.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(message.contains("matching source attestation"), "{message}");
    assert!(!second_output.join("generation-manifest.v1.json").exists());
}

#[test]
fn release_generation_rejects_scenario_bypass() {
    let fixture = fixture(false);
    git(&fixture.root, &["add", "rustdoc"]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "rustdoc receipt"],
    );
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "release-scenario-bypass",
        "release",
        true,
    );
    assert!(!result.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        message.contains("cannot skip the registered scenario source closure"),
        "{message}"
    );
    assert!(!fixture.output.join("generation-manifest.v1.json").exists());
}

#[test]
fn concurrent_preview_publications_are_idempotent() {
    let fixture = fixture(false);
    let root_a = fixture.root.clone();
    let rustdoc_a = fixture.rustdoc.clone();
    let output_a = fixture.output.clone();
    let first = std::thread::spawn(move || {
        run_generate(
            &root_a,
            &rustdoc_a,
            &output_a,
            "concurrent-preview",
            "preview",
            true,
        )
    });
    let root_b = fixture.root.clone();
    let rustdoc_b = fixture.rustdoc.clone();
    let output_b = fixture.output.clone();
    let second = std::thread::spawn(move || {
        run_generate(
            &root_b,
            &rustdoc_b,
            &output_b,
            "concurrent-preview",
            "preview",
            true,
        )
    });
    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert!(first.status.success() || second.status.success());
    assert!(run_drift(&fixture).status.success());
}
