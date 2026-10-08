use sdk_docs::rustdoc_digest;
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
        "docs/guide.md",
        "rust/crates/demo/Cargo.toml",
        "rust/crates/demo/README.md",
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
    fs::create_dir_all(root.join("docs")).unwrap();
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
        root.join("docs/guide.md"),
        "# Fixture guide\n\nThe guide is part of the docs source closure.\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/demo/Cargo.toml"),
        "[package]\nname = \"demo\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/demo/README.md"),
        "# Demo crate\n\nThis crate guide is part of the source closure.\n",
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

/// A real Cargo workspace whose `extra` feature exposes an API item that the
/// default Rustdoc family cannot contain.  The execute-profiles path must
/// merge that profile-only item into the catalog with the profile's exact
/// source and receipt identities; silently dropping it would make the sidecar
/// claim coverage for an item that no catalog row can represent.
fn feature_profile_fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "sdk-generation-feature-profile-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("rust/crates/demo/src")).unwrap();
    fs::create_dir_all(root.join("release")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"rust/crates/demo\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    fs::write(
        root.join("rust-toolchain.toml"),
        "[toolchain]\nchannel = \"1.98.1\"\nprofile = \"minimal\"\n",
    )
    .unwrap();
    fs::write(root.join("release/cargo-crates.json"), "[\"demo\"]\n").unwrap();
    fs::write(
        root.join("rust/crates/demo/Cargo.toml"),
        "[package]\nname = \"demo\"\nversion = \"1.0.0\"\nedition = \"2021\"\n\n[lib]\npath = \"src/lib.rs\"\n\n[features]\ndefault = [\"base\"]\nbase = []\nextra = []\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/demo/src/lib.rs"),
        "pub struct Always;\n\n#[cfg(not(feature = \"extra\"))]\npub fn overloaded(value: u64) -> u64 { value }\n\n#[cfg(feature = \"extra\")]\npub fn overloaded(value: &str) -> usize { value.len() }\n\n#[cfg(feature = \"extra\")]\npub struct FeatureOnly;\n",
    )
    .unwrap();
    // This dependency-free lockfile is the exact lock input for the fixture;
    // no network or workspace checkout is involved in the producer run.
    fs::write(
        root.join("Cargo.lock"),
        "version = 3\n\n[[package]]\nname = \"demo\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
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
    git(
        &root,
        &["commit", "--quiet", "-m", "feature profile fixture"],
    );
    let output = std::env::temp_dir().join(format!(
        "sdk-generation-feature-profile-output-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&output);
    Fixture {
        root,
        rustdoc: output.join(".rustdoc"),
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

fn generated_span_fixture_pair() -> (Fixture, Fixture) {
    let first = fixture(false);
    let generated_relative = "target/generated/wire.rs";
    let generated = first.root.join(generated_relative);
    fs::create_dir_all(generated.parent().unwrap()).unwrap();
    fs::write(&generated, b"pub struct Wire;\n").unwrap();

    let receipt_path = first.rustdoc.join("demo_docs.json");
    let mut receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt["index"]["0"]["span"] = json!({
        "filename": generated.canonicalize().unwrap().to_string_lossy(),
        "begin": [1, 0],
        "end": [1, 16]
    });
    fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();

    let second_root = std::env::temp_dir().join(format!(
        "sdk-generation-relocated-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&second_root);
    let clone = Command::new("git")
        .args([
            "clone",
            "--quiet",
            first.root.to_str().unwrap(),
            second_root.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        clone.status.success(),
        "git clone failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    fs::create_dir_all(second_root.join("rustdoc")).unwrap();
    fs::copy(
        first.rustdoc.join("demo_docs.json"),
        second_root.join("rustdoc/demo_docs.json"),
    )
    .unwrap();
    fs::copy(
        first.rustdoc.join("demo_docs.source.sha256"),
        second_root.join("rustdoc/demo_docs.source.sha256"),
    )
    .unwrap();
    let second_generated = second_root.join(generated_relative);
    fs::create_dir_all(second_generated.parent().unwrap()).unwrap();
    fs::write(&second_generated, b"pub struct Wire;\n").unwrap();
    let second_receipt = second_root.join("rustdoc/demo_docs.json");
    let mut second_json: Value =
        serde_json::from_slice(&fs::read(&second_receipt).unwrap()).unwrap();
    second_json["index"]["0"]["span"] = json!({
        "filename": second_generated
            .canonicalize()
            .unwrap()
            .to_string_lossy(),
        "begin": [1, 0],
        "end": [1, 16]
    });
    fs::write(&second_receipt, serde_json::to_vec(&second_json).unwrap()).unwrap();

    let second = Fixture {
        root: second_root.clone(),
        rustdoc: second_root.join("rustdoc"),
        output: second_root.join("output"),
    };
    (first, second)
}

#[test]
fn relocated_checkouts_produce_identical_artifacts_and_source_digest() {
    let (first, second) = generated_span_fixture_pair();
    for fixture in [&first, &second] {
        let result = run_generate(
            &fixture.root,
            &fixture.rustdoc,
            &fixture.output,
            "relocation-invariant",
            "preview",
            true,
        );
        assert!(
            result.status.success(),
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }

    let first_manifest: Value = serde_json::from_slice(
        &fs::read(first.output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    let second_manifest: Value = serde_json::from_slice(
        &fs::read(second.output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        first_manifest["sourceSha256"], second_manifest["sourceSha256"],
        "source digest must survive checkout relocation"
    );
    assert_eq!(
        first_manifest["sourceFiles"], second_manifest["sourceFiles"],
        "source closure must survive checkout relocation"
    );
    assert_eq!(
        first_manifest["artifacts"], second_manifest["artifacts"],
        "every generated artifact hash must survive checkout relocation"
    );

    let _ = fs::remove_dir_all(first.root);
    let _ = fs::remove_dir_all(second.root);
}

fn run_generate_execute_profiles(fixture: &Fixture) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_sdk-generation"))
        .args([
            "generate",
            "--root",
            fixture.root.to_str().unwrap(),
            "--output",
            fixture.output.to_str().unwrap(),
            "--version",
            "feature-profile-catalog",
            "--channel",
            "preview",
            "--execute-profiles",
            "--skip-scenarios",
        ])
        .output()
        .unwrap()
}

fn run_drift_execute_profiles(fixture: &Fixture) -> std::process::Output {
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

fn output_message(result: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    )
}

#[test]
fn scenario_failure_leaves_docs_unadmitted() {
    let fixture = fixture(false);
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "unadmitted-scenario-failure",
        "preview",
        false,
    );
    assert!(!result.status.success(), "{}", output_message(&result));
    assert!(
        output_message(&result).contains("scenario"),
        "{}",
        output_message(&result)
    );
    assert!(fixture
        .output
        .join("preview/unadmitted-scenario-failure/sdk-docs-data.v2.json")
        .exists());
    assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
}

#[test]
fn source_mutation_during_profiles_leaves_docs_unadmitted() {
    let fixture = feature_profile_fixture();
    fs::write(fixture.root.join("rust/crates/demo/build.rs"), r#"
fn main() {
    use std::io::Write;
    let path = std::path::Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/lib.rs");
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(file, "// source mutated during profile execution").unwrap();
}
"#).unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "source mutation fixture"],
    );
    let result = run_generate_execute_profiles(&fixture);
    assert!(!result.status.success(), "{}", output_message(&result));
    assert!(
        output_message(&result).contains("source digest changed"),
        "{}",
        output_message(&result)
    );
    assert!(fixture.output.join("generation-manifest.v1.json").exists());
    assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
}

fn assert_source_edit_invalidates_output(relative: &str, changed: &str, include_plugin: bool) {
    let fixture = fixture(include_plugin);
    let first = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "source-closure-baseline",
        "preview",
        true,
    );
    assert!(
        first.status.success(),
        "{}{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );

    let source = fixture.root.join(relative);
    let original = fs::read(&source).unwrap();
    let original_hash = hash_bytes(&original);
    fs::write(&source, changed).unwrap();
    let changed_hash = hash_file(&source);
    assert_ne!(
        original_hash, changed_hash,
        "test edit did not change {relative}"
    );

    let drift = run_drift(&fixture);
    assert!(!drift.status.success());
    let drift_message = format!(
        "{}{}",
        String::from_utf8_lossy(&drift.stdout),
        String::from_utf8_lossy(&drift.stderr)
    );
    assert!(
        drift_message.contains("source digest changed"),
        "{drift_message}"
    );
    assert!(drift_message.contains(relative), "{drift_message}");

    let stale_output = fixture.root.join("stale-after-source-edit");
    let stale = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &stale_output,
        "source-closure-stale",
        "preview",
        true,
    );
    assert!(!stale.status.success());
    let stale_message = format!(
        "{}{}",
        String::from_utf8_lossy(&stale.stdout),
        String::from_utf8_lossy(&stale.stderr)
    );
    assert!(
        stale_message.contains("matching source attestation"),
        "{stale_message}"
    );
    assert!(!stale_output.join("generation-manifest.v1.json").exists());

    fs::write(&source, &original).unwrap();
    assert_eq!(hash_file(&source), original_hash);
    let drift = run_drift(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));

    let refreshed_output = fixture.root.join("refreshed-after-source-restore");
    let refreshed = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &refreshed_output,
        "source-closure-restored",
        "preview",
        true,
    );
    assert!(
        refreshed.status.success(),
        "{}{}",
        String::from_utf8_lossy(&refreshed.stdout),
        String::from_utf8_lossy(&refreshed.stderr)
    );
    let manifest: Value = serde_json::from_slice(
        &fs::read(refreshed_output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["sourceFiles"][relative], original_hash);
}

#[test]
fn crate_rust_comment_and_markdown_guide_edits_invalidate_output() {
    assert_source_edit_invalidates_output(
        "rust/crates/demo/src/lib.rs",
        "pub fn demo() -> u64 { 7 }\n// changed crate comment\n",
        false,
    );
    assert_source_edit_invalidates_output(
        "rust/crates/demo/README.md",
        "# Demo crate\n\nChanged crate guide.\n",
        false,
    );
    assert_source_edit_invalidates_output(
        "docs/guide.md",
        "# Fixture guide\n\nChanged workspace guide.\n",
        false,
    );
}

#[test]
fn plugin_rust_edits_invalidate_output() {
    assert_source_edit_invalidates_output(
        "plugin/src/main.rs",
        "fn main() {}\n// changed plugin source\n",
        true,
    );
}

#[test]
fn unknown_generate_option_is_rejected_before_writes() {
    let fixture = fixture(false);
    let result = Command::new(env!("CARGO_BIN_EXE_sdk-generation"))
        .args([
            "generate",
            "--root",
            fixture.root.to_str().unwrap(),
            "--output",
            fixture.output.to_str().unwrap(),
            "--rustdoc-dir",
            fixture.rustdoc.to_str().unwrap(),
            "--version",
            "unknown-option",
            "--unknown",
            "accepted",
        ])
        .output()
        .unwrap();
    assert!(!result.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(message.contains("unknown option `--unknown`"), "{message}");
    assert!(
        !fixture.output.exists(),
        "unknown options must fail before generation writes output"
    );
}

#[test]
fn unknown_drift_option_is_rejected_before_reads() {
    let fixture = fixture(false);
    let result = Command::new(env!("CARGO_BIN_EXE_sdk-generation"))
        .args([
            "drift",
            "--root",
            fixture.root.to_str().unwrap(),
            "--output",
            fixture.output.to_str().unwrap(),
            "--rustdoc-dir",
            fixture.rustdoc.to_str().unwrap(),
            "--unknown",
            "accepted",
        ])
        .output()
        .unwrap();
    assert!(!result.status.success());
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(message.contains("unknown option `--unknown`"), "{message}");
    assert!(
        !fixture.output.exists(),
        "unknown options must fail before drift reads or writes output"
    );
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
    let drift = run_drift(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
}

#[test]
fn versioned_release_manifest_binds_source_and_scenario_artifacts() {
    let fixture = fixture(false);
    let result = run_generate(
        &fixture.root,
        &fixture.rustdoc,
        &fixture.output,
        "versioned-provenance",
        "preview",
        true,
    );
    assert!(result.status.success(), "{}", output_message(&result));
    let generation: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("generation-manifest.v1.json")).unwrap(),
    )
    .unwrap();
    let release_relative = generation["releaseManifest"].as_str().unwrap();
    let release_path = fixture.output.join(release_relative.replace('/', "\\"));
    let release_bytes = fs::read(&release_path).unwrap();
    let release: Value = serde_json::from_slice(&release_bytes).unwrap();
    assert_eq!(release["schema"], "sdk-generation-release-manifest.v1");
    assert_eq!(release["version"], "versioned-provenance");
    assert_eq!(release["revision"], generation["revision"]);
    assert_eq!(release["sourceSha256"], generation["sourceSha256"]);
    assert_eq!(release["sourceFiles"], generation["sourceFiles"]);
    let catalog = release["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|artifact| artifact["kind"] == "scenario-catalog")
        .expect("release manifest must bind the scenario catalog artifact");
    let catalog_path = fixture
        .output
        .join(catalog["path"].as_str().unwrap().replace('/', "\\"));
    assert_eq!(catalog["sha256"], hash_file(&catalog_path));
    assert_eq!(
        fs::read_to_string(release_path.with_extension("sha256"))
            .unwrap()
            .trim(),
        hash_bytes(&release_bytes)
    );
    assert!(run_drift(&fixture).status.success());

    let original_catalog = fs::read(&catalog_path).unwrap();
    fs::write(&catalog_path, b"tampered scenario catalog\n").unwrap();
    let drift = run_drift(&fixture);
    assert!(!drift.status.success());
    assert!(output_message(&drift).contains("generated artifact digest changed"));
    fs::write(&catalog_path, original_catalog).unwrap();
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
fn release_generation_requires_executed_profiles() {
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
        message.contains("release generation requires --execute-profiles"),
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
    let drift = run_drift(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
}

#[test]
fn feature_profile_api_is_joined_with_catalog_and_source_provenance() {
    let fixture = feature_profile_fixture();
    let result = run_generate_execute_profiles(&fixture);
    assert!(
        result.status.success(),
        "feature-only profile API must be joined into the docs catalog:\n{}",
        output_message(&result)
    );

    let data_path = fixture
        .output
        .join("preview/feature-profile-catalog/sdk-docs-data.v2.json");
    let data: Value = serde_json::from_slice(&fs::read(&data_path).unwrap()).unwrap();
    assert!(data.to_string().contains("FeatureOnly"));

    let availability: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-profile-availability.v1.json")).unwrap(),
    )
    .unwrap();
    let feature_entry = availability["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["path"] == "demo::FeatureOnly")
        .expect("feature-only item must retain a sidecar entry");
    let feature_profile = feature_entry["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|profile| {
            profile["features"]
                .as_array()
                .is_some_and(|features| features.iter().any(|feature| feature == "extra"))
        })
        .expect("FeatureOnly must be attributed to the extra profile");

    let variant_entries = availability["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["path"] == "demo::overloaded")
        .collect::<Vec<_>>();
    assert!(
        variant_entries.len() >= 2,
        "profile merge must retain same-path items with distinct signatures"
    );
    let signatures = variant_entries
        .iter()
        .filter_map(|entry| entry["signature"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        signatures.len() >= 2,
        "same-path profile variants must remain signature-distinct"
    );
    assert!(variant_entries.iter().any(|entry| {
        entry["profiles"].as_array().is_some_and(|profiles| {
            profiles.iter().any(|profile| {
                profile["features"]
                    .as_array()
                    .is_some_and(|features| features.iter().any(|feature| feature == "extra"))
            })
        })
    }));

    let receipts: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
    )
    .unwrap();
    let receipt = receipts
        .as_array()
        .unwrap()
        .iter()
        .find(|receipt| receipt["profile"] == feature_profile["profile"])
        .expect("sidecar profile must have an executed receipt row");
    let receipt_path = fixture.output.join(".rustdoc").join(
        receipt["rustdocFile"]
            .as_str()
            .unwrap()
            .strip_prefix("rustdoc/")
            .unwrap()
            .replace('/', "\\"),
    );
    assert_eq!(
        receipt["rustdocSha256"],
        rustdoc_digest(&receipt_path, &fixture.root).unwrap()
    );
    assert_eq!(
        fs::read_to_string(receipt_path.with_extension("source.sha256"))
            .unwrap()
            .trim(),
        data["source"]["sourceSha256"].as_str().unwrap()
    );
    assert_eq!(data["source"]["revision"], git_revision(&fixture.root));

    let availability_hash =
        hash_file(&fixture.output.join("sdk-docs-profile-availability.v1.json"));
    let second = run_generate_execute_profiles(&fixture);
    assert!(
        second.status.success(),
        "replaying the same producer inputs must be deterministic:\n{}",
        output_message(&second)
    );
    assert_eq!(
        hash_file(&fixture.output.join("sdk-docs-profile-availability.v1.json")),
        availability_hash
    );

    let source = fixture.root.join("rust/crates/demo/src/lib.rs");
    let original = fs::read(&source).unwrap();
    let mut changed = original.clone();
    changed.extend_from_slice(b"// source API closure mutation\n");
    fs::write(&source, changed).unwrap();
    let drift = run_drift_execute_profiles(&fixture);
    assert!(!drift.status.success());
    assert!(output_message(&drift).contains("rust/crates/demo/src/lib.rs"));
    fs::write(&source, original).unwrap();
    assert!(
        run_drift_execute_profiles(&fixture).status.success(),
        "restoring the exact Rust source must restore drift validity"
    );
}

#[test]
fn clean_relocated_profile_builds_share_generated_source_content_identity() {
    let fixture = feature_profile_fixture();
    fs::write(fixture.root.join("rust/crates/demo/build.rs"),
        "fn main() { std::fs::write(std::path::PathBuf::from(std::env::var(\"OUT_DIR\").unwrap()).join(\"wire.rs\"), \"pub struct GeneratedWire;\\n\").unwrap(); }\n").unwrap();
    fs::write(
        fixture.root.join("rust/crates/demo/src/lib.rs"),
        "include!(concat!(env!(\"OUT_DIR\"), \"/wire.rs\"));\n",
    )
    .unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &[
            "commit",
            "--quiet",
            "-m",
            "generated source relocation fixture",
        ],
    );
    let relocated_root = fixture.root.with_extension("relocated");
    let relocated_output = fixture.output.with_extension("relocated");
    assert!(Command::new("git")
        .args([
            "clone",
            "--quiet",
            fixture.root.to_str().unwrap(),
            relocated_root.to_str().unwrap()
        ])
        .status()
        .unwrap()
        .success());
    let relocated = Fixture {
        root: relocated_root,
        rustdoc: relocated_output.join(".rustdoc"),
        output: relocated_output,
    };
    for checkout in [&fixture, &relocated] {
        assert!(
            !checkout.output.exists(),
            "each profile build starts with a fresh output directory"
        );
        let result = run_generate_execute_profiles(checkout);
        assert!(result.status.success(), "{}", output_message(&result));
        assert!(run_drift_execute_profiles(checkout).status.success());
    }
    let path = "preview/feature-profile-catalog/sdk-docs-data.v2.json";
    assert_eq!(
        fs::read(fixture.output.join(path)).unwrap(),
        fs::read(relocated.output.join(path)).unwrap(),
        "independent clean profile outputs must publish identical content identities and bytes"
    );
    let data: Value =
        serde_json::from_slice(&fs::read(fixture.output.join(path)).unwrap()).unwrap();
    let mut spans = Vec::new();
    fn source_paths(value: &Value, paths: &mut Vec<String>) {
        if let Some(object) = value.as_object() {
            if let Some(path) = object
                .get("path")
                .and_then(Value::as_str)
                .filter(|path| path.starts_with("generated/rustdoc/"))
            {
                paths.push(path.to_string());
            }
            for child in object.values() {
                source_paths(child, paths);
            }
        } else if let Some(array) = value.as_array() {
            for child in array {
                source_paths(child, paths);
            }
        }
    }
    source_paths(&data, &mut spans);
    assert!(
        !spans.is_empty(),
        "generated source spans must appear in the actual docs data"
    );
    for path in spans {
        assert_eq!(
            fs::read(fixture.output.join(&path)).unwrap(),
            b"pub struct GeneratedWire;\n"
        );
        assert_eq!(
            fs::read(fixture.output.join(&path)).unwrap(),
            fs::read(relocated.output.join(&path)).unwrap()
        );
    }
    assert_eq!(
        fs::read(fixture.output.join("sdk-docs-profile-availability.v1.json")).unwrap(),
        fs::read(
            relocated
                .output
                .join("sdk-docs-profile-availability.v1.json")
        )
        .unwrap()
    );
}
fn historical_fixture(mutation: Option<&str>) -> (Fixture, PathBuf) {
    let fixture = feature_profile_fixture();
    let package = fixture.root.join("rust/crates/demo");
    fs::create_dir_all(package.join("examples")).unwrap();
    fs::write(package.join("README.md"), "# Historical source guide\n").unwrap();
    for name in ["descriptor.bin", "input.json", "schema.proto"] {
        fs::write(package.join(name), b"original archived input").unwrap();
    }
    fs::write(fixture.root.join(".gitignore"), "target/\nrust/crates/demo/descriptor.bin\nrust/crates/demo/input.json\nrust/crates/demo/schema.proto\n").unwrap();
    let body = match mutation {
        Some(name) => format!(
            "std::fs::write(\"rust/crates/demo/{name}\", \"changed archived input\").unwrap();"
        ),
        None => String::new(),
    };
    fs::write(
        package.join("examples/historical.rs"),
        format!("fn main() {{ {body} println!(\"executed-historical-example\"); }}\n"),
    )
    .unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "historical docs fixture"],
    );
    let support = fixture.output.join("test-support");
    let archives = support.join("registry");
    let archive_source = support.join("archive-source/demo-1.0.0");
    fs::create_dir_all(&archives).unwrap();
    fs::create_dir_all(archive_source.join("src")).unwrap();
    fs::create_dir_all(archive_source.join("examples")).unwrap();
    for name in [
        "src/lib.rs",
        "examples/historical.rs",
        "README.md",
        "descriptor.bin",
        "input.json",
        "schema.proto",
    ] {
        fs::copy(package.join(name), archive_source.join(name)).unwrap();
    }
    fs::copy(
        package.join("Cargo.toml"),
        archive_source.join("Cargo.toml.orig"),
    )
    .unwrap();
    fs::write(
        archive_source.join("Cargo.toml"),
        "# Cargo-produced normalized manifest fixture\n",
    )
    .unwrap();
    fs::write(
        archive_source.join("Cargo.lock"),
        "# Cargo-produced standalone package lock fixture\n",
    )
    .unwrap();
    fs::write(archive_source.join(".cargo_vcs_info.json"), serde_json::to_vec(&json!({"git":{"sha1":git_revision(&fixture.root),"dirty":false},"path_in_vcs":"rust/crates/demo"})).unwrap()).unwrap();
    let archive = archives.join("demo-1.0.0.crate");
    let packed = Command::new("tar")
        .args(["-cf"])
        .arg(&archive)
        .arg("-C")
        .arg(support.join("archive-source"))
        .arg("demo-1.0.0")
        .output()
        .unwrap();
    assert!(
        packed.status.success(),
        "{}",
        String::from_utf8_lossy(&packed.stderr)
    );
    write_registry_transport(&support, &archive);
    (fixture, support)
}

fn write_registry_transport(support: &Path, archive: &Path) {
    let response = serde_json::to_string(&json!({"version":{"crate":"demo","num":"1.0.0","checksum":hash_file(archive).trim_start_matches("sha256:"),"yanked":false}})).unwrap();
    // Subprocess fixture for the real registry command boundary. No network
    // requests or authored documentation JSON enter this qualification test.
    let curl_source = support.join("fixture-curl.rs");
    fs::write(
        &curl_source,
        format!("fn main() {{ let args: Vec<_> = std::env::args().collect(); assert!(args.windows(2).any(|pair| pair[0] == \"--user-agent\" && pair[1].starts_with(\"acyclic-sdk-docs/\") && pair[1].contains(\"https://github.com/acyclic-labs/sdk\"))); print!(\"{{}}\\n200\", {response:?}); }}"),
    )
    .unwrap();
    let curl = support.join(if cfg!(windows) { "curl.exe" } else { "curl" });
    let compiled = Command::new("rustc")
        .arg(&curl_source)
        .arg("--crate-name")
        .arg("fixture_curl")
        .arg("-o")
        .arg(&curl)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
}

fn run_historical_generate(fixture: &Fixture, support: &Path) -> std::process::Output {
    run_historical_generate_mode(fixture, support, None)
}

fn run_historical_generate_mode(
    fixture: &Fixture,
    support: &Path,
    mode: Option<&str>,
) -> std::process::Output {
    let path = std::env::join_paths(
        std::iter::once(support.to_owned())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_sdk-generation"));
    command
        .env("PATH", path)
        .args(["generate", "--root"])
        .arg(&fixture.root)
        .arg("--output")
        .arg(&fixture.output)
        .args([
            "--version",
            "1.0.0",
            "--channel",
            "release",
            "--execute-profiles",
            "--historical-archives",
        ])
        .arg(support.join("registry"));
    if let Some(mode) = mode {
        command.args(["--historical-source", mode]);
    }
    command.output().unwrap()
}

fn registry_archive_fixture(
    dirty: bool,
    published_lock: bool,
    mutation: Option<&str>,
    publisher_vcs: bool,
) -> (Fixture, PathBuf) {
    let (fixture, support) = historical_fixture(None);
    let package = fixture.root.join("rust/crates/demo");
    let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap();
    fs::write(package.join("Cargo.toml"), manifest.replace("[package]\n", "[package]\ninclude = [\"Cargo.toml\", \"README.md\", \"src/**\", \"examples/**\", \"descriptor.bin\", \"input.json\", \"schema.proto\"]\n")).unwrap();
    let original = fs::read_to_string(package.join("src/lib.rs")).unwrap();
    fs::write(
        package.join("src/lib.rs"),
        format!(
            "{original}\n/// API present only in the published archive.\npub struct ArchiveOnly;\n"
        ),
    )
    .unwrap();
    let action = mutation.map(|path| format!("std::fs::write(std::path::Path::new(env!(\"CARGO_MANIFEST_DIR\")).join({path:?}), \"changed imported input\").unwrap();")).unwrap_or_default();
    fs::write(
        package.join("examples/historical.rs"),
        format!("fn main() {{ {action} println!(\"archive-example-executed\"); }}\n"),
    )
    .unwrap();
    // Cargo itself creates the normalized manifest and published package lock.
    let packaged = Command::new("cargo")
        .current_dir(&fixture.root)
        .args(["package", "--allow-dirty", "--no-verify", "--manifest-path"])
        .arg(package.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(support.join("package-target"))
        .output()
        .unwrap();
    assert!(packaged.status.success(), "{}", output_message(&packaged));
    let unpack = support.join("cargo-packaged-source");
    fs::create_dir_all(&unpack).unwrap();
    let extracted = Command::new("tar")
        .arg("-xf")
        .arg(support.join("package-target/package/demo-1.0.0.crate"))
        .arg("-C")
        .arg(&unpack)
        .output()
        .unwrap();
    assert!(extracted.status.success(), "{}", output_message(&extracted));
    let source = unpack.join("demo-1.0.0");
    fs::write(source.join(".cargo_vcs_info.json"), serde_json::to_vec(&json!({"git":{"sha1":git_revision(&fixture.root),"dirty":dirty},"path_in_vcs":"rust/crates/demo"})).unwrap()).unwrap();
    if !publisher_vcs {
        fs::remove_file(source.join(".cargo_vcs_info.json")).unwrap();
    }
    if !published_lock {
        fs::remove_file(source.join("Cargo.lock")).unwrap();
    }
    let archive = support.join("registry/demo-1.0.0.crate");
    let packed = Command::new("tar")
        .args(["-cf"])
        .arg(&archive)
        .arg("-C")
        .arg(&unpack)
        .arg("demo-1.0.0")
        .output()
        .unwrap();
    assert!(packed.status.success(), "{}", output_message(&packed));
    write_registry_transport(&support, &archive);
    (fixture, support)
}

#[test]
fn registry_archive_native_source_is_not_a_publisher_git_retag() {
    for (dirty, published_lock, publisher_vcs) in [
        (true, true, true),
        (false, false, true),
        (false, true, false),
    ] {
        let (fixture, support) =
            registry_archive_fixture(dirty, published_lock, None, publisher_vcs);
        let publisher = git_revision(&fixture.root);
        let result = run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
        assert!(result.status.success(), "{}", output_message(&result));
        let output = fixture.output.join("releases/1.0.0");
        let data: Value =
            serde_json::from_slice(&fs::read(output.join("sdk-docs-data.v2.json")).unwrap())
                .unwrap();
        assert_eq!(data["source"]["capturedSource"]["kind"], "registryArchives");
        assert_ne!(data["source"]["revision"], publisher);
        assert_eq!(data["source"]["sourceState"], "registry-archives");
        let archive = &data["source"]["capturedSource"]["archives"][0];
        if publisher_vcs {
            assert_eq!(archive["publisherVcs"]["revision"], publisher);
            assert_eq!(archive["publisherVcs"]["dirty"], dirty);
        } else {
            assert!(archive["publisherVcs"].is_null());
            assert_eq!(data["source"]["releasedPackages"][0]["sourceRevision"], "");
            assert_eq!(data["source"]["releasedPackages"][0]["pathInVcs"], "");
        }
        assert_eq!(
            archive["resolutionLock"]["kind"],
            if published_lock {
                "published"
            } else {
                "docsProducer"
            }
        );
        let item = data["families"][0]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["name"] == "ArchiveOnly")
            .unwrap();
        let source_path = item["source"]["path"].as_str().unwrap();
        assert!(source_path.starts_with("releases/1.0.0/sources/demo-1.0.0/"));
        assert!(fixture.output.join(source_path).exists());
        if !published_lock {
            let lock = output.join("sources/demo-1.0.0/Cargo.lock");
            let captured_lock = fs::read(&lock).unwrap();
            let repeated =
                run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
            assert!(repeated.status.success(), "{}", output_message(&repeated));
            assert_eq!(fs::read(&lock).unwrap(), captured_lock);
        }
        let imported = Fixture {
            root: fixture.root.clone(),
            rustdoc: output.join(".rustdoc"),
            output: output.clone(),
        };
        for path in [
            "Cargo.lock",
            "Cargo.toml",
            "descriptor.bin",
            "input.json",
            "schema.proto",
        ] {
            let source = output.join("sources/demo-1.0.0").join(path);
            let original = fs::read(&source).unwrap();
            fs::write(&source, b"tampered imported source").unwrap();
            let drift = run_drift(&imported);
            assert!(!drift.status.success(), "{path} drift was accepted");
            fs::write(&source, original).unwrap();
        }
        fs::write(output.join("sources/demo-1.0.0/stale-extra.bin"), b"stale").unwrap();
        assert!(!run_drift(&imported).status.success());
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
}

#[test]
fn historical_source_help_and_flag_contract_are_explicit() {
    let binary = env!("CARGO_BIN_EXE_sdk-generation");
    let help = Command::new(binary).arg("help").output().unwrap();
    assert!(help.status.success());
    let text = output_message(&help);
    assert!(text.contains("--historical-archives DIR"));
    assert!(text.contains("--historical-source git|registry-archives"));
    for (mode, expected) in [
        ("registry-archives", "requires --historical-archives"),
        ("unverified", "unknown historical source"),
    ] {
        let rejected = Command::new(binary)
            .args([
                "generate",
                "--root",
                "missing",
                "--output",
                "missing",
                "--version",
                "1.0.0",
                "--execute-profiles",
                "--channel",
                "release",
                "--historical-source",
                mode,
            ])
            .output()
            .unwrap();
        assert!(!rejected.status.success());
        assert!(output_message(&rejected).contains(expected));
    }
}

#[test]
fn archive_example_mutation_cannot_admit_its_native_release_catalog() {
    let (fixture, support) = registry_archive_fixture(true, true, Some("input.json"), true);
    let result = run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
    assert!(!result.status.success());
    assert!(
        output_message(&result).contains("source closure changed"),
        "{}",
        output_message(&result)
    );
    assert_eq!(
        fs::read(
            fixture
                .output
                .join("releases/1.0.0/sources/demo-1.0.0/input.json")
        )
        .unwrap(),
        b"changed imported input"
    );
    assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}

#[test]
fn historical_native_pipeline_admits_only_executed_source_owned_registry_docs() {
    let (fixture, support) = historical_fixture(None);
    let result = run_historical_generate(&fixture, &support);
    assert!(result.status.success(), "{}", output_message(&result));
    let version = fixture.output.join("releases/1.0.0");
    let data: Value =
        serde_json::from_slice(&fs::read(version.join("sdk-docs-data.v2.json")).unwrap()).unwrap();
    assert_eq!(data["source"]["publicationStatus"], "registryReleased");
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(version.join("generation-manifest.v1.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["scope"]["kind"], "historicalRelease");
    assert!(manifest["sourceFiles"]["rust/crates/demo/descriptor.bin"].is_string());
    assert!(manifest["declaredSourceToolchain"]
        .as_str()
        .unwrap()
        .contains("1.98.1"));
    let scenarios: Value =
        serde_json::from_slice(&fs::read(version.join("sdk-docs-scenarios.v1.json")).unwrap())
            .unwrap();
    assert_eq!(scenarios.as_array().unwrap().len(), 1);
    assert_eq!(scenarios[0]["target"], "historical");
    let index: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-versions.v1.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(index["latest"]["version"], "1.0.0");
    assert_eq!(index["releases"].as_array().unwrap().len(), 1);
    // Resealing a manifest after omitting an ignored archive member must not
    // make the archive's authoritative input disappear from final validation.
    let omitted = "rust/crates/demo/descriptor.bin";
    manifest["scope"]["archive_source_paths"]
        .as_array_mut()
        .unwrap()
        .retain(|path| path.as_str() != Some(omitted));
    manifest["sourceFiles"]
        .as_object_mut()
        .unwrap()
        .remove(omitted);
    let files: BTreeMap<String, String> =
        serde_json::from_value(manifest["sourceFiles"].clone()).unwrap();
    manifest["sourceSha256"] = json!(source_digest(&files));
    write_manifest(&version, &manifest);
    let altered = Fixture {
        root: fixture.root.clone(),
        rustdoc: version.join(".rustdoc"),
        output: version.clone(),
    };
    let drift = run_drift(&altered);
    assert!(!drift.status.success());
    assert!(
        output_message(&drift).contains("retained registry archive closure differs from scope"),
        "{}",
        output_message(&drift)
    );
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}

#[test]
fn successful_historical_example_mutating_archived_inputs_cannot_admit_catalog() {
    for mutation in ["descriptor.bin", "input.json", "schema.proto"] {
        let (fixture, support) = historical_fixture(Some(mutation));
        let result = run_historical_generate(&fixture, &support);
        assert!(!result.status.success(), "{mutation} was admitted");
        assert!(
            output_message(&result).contains("source digest changed since generation"),
            "{mutation}: {}",
            output_message(&result)
        );
        assert_eq!(
            fs::read(fixture.root.join("rust/crates/demo").join(mutation)).unwrap(),
            b"changed archived input"
        );
        let executions: Value = serde_json::from_slice(
            &fs::read(
                fixture
                    .output
                    .join("releases/1.0.0/sdk-docs-scenario-executions.v1.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            executions.as_array().unwrap().len(),
            1,
            "example must really execute successfully before outer drift rejects"
        );
        assert!(
            !fixture.output.join("sdk-docs-versions.v1.json").exists(),
            "mutated historical source must remain unadmitted"
        );
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
}
