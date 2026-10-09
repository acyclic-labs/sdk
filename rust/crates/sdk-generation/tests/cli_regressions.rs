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
        "scope": {"kind": "currentSdk"},
        "registryCorpus": null,
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

fn drift_command(fixture: &Fixture) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sdk-generation"));
    command.args([
        "drift",
        "--root",
        fixture.root.to_str().unwrap(),
        "--output",
        fixture.output.to_str().unwrap(),
        "--rustdoc-dir",
        fixture.rustdoc.to_str().unwrap(),
    ]);
    command
}

fn run_drift(fixture: &Fixture) -> std::process::Output {
    drift_command(fixture).output().unwrap()
}

fn fixture_path(support: &Path) -> std::ffi::OsString {
    std::env::join_paths(
        std::iter::once(support.to_owned())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap()
}

fn run_historical_drift(fixture: &Fixture, support: &Path) -> std::process::Output {
    drift_command(fixture)
        .env("PATH", fixture_path(support))
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
        .join("preview/unadmitted-scenario-failure/sdk-docs-data.v1.json")
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
        output_message(&result)
            .contains("source closure changed during qualification before manifest admission"),
        "{}",
        output_message(&result)
    );
    assert!(!fixture.output.join("generation-manifest.v1.json").exists());
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
    let release_path = fixture.output.join(release_relative);
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
    let catalog_path = fixture.output.join(catalog["path"].as_str().unwrap());
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
fn long_feature_identities_use_portable_receipt_paths_and_pass_drift() {
    let fixture = feature_profile_fixture();
    let feature = "native-execution-observation-".repeat(8);
    let manifest = fixture.root.join("rust/crates/demo/Cargo.toml");
    let original = fs::read_to_string(&manifest).unwrap();
    fs::write(&manifest, format!("{original}\n{feature} = []\n")).unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "long feature identity fixture"],
    );
    let generated = run_generate_execute_profiles(&fixture);
    assert!(generated.status.success(), "{}", output_message(&generated));
    let receipts: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
    )
    .unwrap();
    let long_profiles = receipts
        .as_array()
        .unwrap()
        .iter()
        .filter(|receipt| {
            receipt["features"]
                .as_array()
                .is_some_and(|features| features.iter().any(|value| value == &feature))
        })
        .collect::<Vec<_>>();
    assert!(
        !long_profiles.is_empty(),
        "the long Cargo feature must be executed"
    );
    for receipt in long_profiles {
        assert!(receipt["profile"].as_str().unwrap().len() > 255);
        let relative = receipt["rustdocFile"]
            .as_str()
            .unwrap()
            .strip_prefix("rustdoc/")
            .unwrap();
        let path = fixture.rustdoc.join(relative);
        assert!(path.file_name().unwrap().to_string_lossy().len() < 255);
        assert!(path.is_file(), "the executed receipt must be retained");
        assert!(path.with_extension("source.sha256").is_file());
    }
    let drift = run_drift_execute_profiles(&fixture);
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
        .join("preview/feature-profile-catalog/sdk-docs-data.v1.json");
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
            .unwrap(),
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
fn current_binary_only_owner_emits_guides_without_private_api_or_registry_claims() {
    let fixture = feature_profile_fixture();
    fs::write(fixture.root.join("rust/crates/demo/Cargo.toml"),
        "[package]\nname = 'demo'\nversion = '1.0.0'\nedition = '2021'\n[[bin]]\nname = 'demo-cli'\npath = 'src/main.rs'\n").unwrap();
    fs::remove_file(fixture.root.join("rust/crates/demo/src/lib.rs")).unwrap();
    fs::write(fixture.root.join("rust/crates/demo/src/main.rs"),
        "//! Run the source-owned command guide.\n/// PRIVATE CLI INTERNALS\nfn hidden() {}\nfn main() { hidden(); }\n").unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "binary-only fixture"],
    );
    let result = run_generate_execute_profiles(&fixture);
    assert!(result.status.success(), "{}", output_message(&result));
    let data: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .output
                .join("preview/feature-profile-catalog/sdk-docs-data.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(data["source"]["publicationStatus"], "candidate");
    assert!(data["source"]["releasedPackages"].is_null());
    assert!(data["families"][0]["items"].as_array().unwrap().is_empty());
    assert!(data.to_string().contains("source-owned command guide"));
    assert!(!data.to_string().contains("PRIVATE CLI INTERNALS"));
    let drift = run_drift_execute_profiles(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
}

#[test]
fn clean_relocated_profile_builds_share_generated_source_content_identity() {
    let fixture = feature_profile_fixture();
    fs::create_dir_all(fixture.root.join("rust/crates/helper/src")).unwrap();
    fs::write(
        fixture.root.join("rust/crates/helper/Cargo.toml"),
        "[package]\nname = \"relocation-helper\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("rust/crates/helper/src/lib.rs"),
        "pub struct Witness;\n",
    )
    .unwrap();
    let manifest = fixture.root.join("rust/crates/demo/Cargo.toml");
    let mut contents = fs::read_to_string(&manifest).unwrap();
    contents.push_str("\n[dependencies]\nrelocation-helper = { path = \"../helper\" }\n");
    fs::write(manifest, contents).unwrap();
    assert!(Command::new("cargo")
        .args(["generate-lockfile", "--offline"])
        .current_dir(&fixture.root)
        .status()
        .unwrap()
        .success());
    fs::write(fixture.root.join("rust/crates/demo/build.rs"),
        "fn main() { std::fs::write(std::path::PathBuf::from(std::env::var(\"OUT_DIR\").unwrap()).join(\"wire.rs\"), \"pub struct GeneratedWire;\\n\").unwrap(); }\n").unwrap();
    fs::write(
        fixture.root.join("rust/crates/demo/src/lib.rs"),
        "include!(concat!(env!(\"OUT_DIR\"), \"/wire.rs\"));\npub fn dependency_witness() -> relocation_helper::Witness { relocation_helper::Witness }\n",
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
    let dependency_paths = |checkout: &Fixture| {
        let mut paths = Vec::new();
        for entry in fs::read_dir(&checkout.rustdoc).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let raw: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
            for record in raw["external_crates"].as_object().unwrap().values() {
                if record["name"] == "relocation_helper" {
                    let physical = record["path"].as_str().unwrap();
                    assert!(physical.contains(".profile-build"), "{physical}");
                    paths.push(physical.to_string());
                }
            }
        }
        paths.sort();
        paths.dedup();
        assert!(
            !paths.is_empty(),
            "actual dependency load paths must be captured"
        );
        paths
    };
    assert_ne!(
        dependency_paths(&fixture),
        dependency_paths(&relocated),
        "raw Rustdoc must retain the independently relocated dependency artifacts"
    );
    let path = "preview/feature-profile-catalog/sdk-docs-data.v1.json";
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
    write_registry_transport(&support);
    (fixture, support)
}

fn write_registry_transport(support: &Path) {
    let mut responses = fs::read_dir(support.join("registry")).unwrap().flat_map(|entry| {
        let archive = entry.unwrap().path();
        let package = archive.file_stem().unwrap().to_str().unwrap().strip_suffix("-1.0.0").unwrap().to_owned();
        let version = json!({"crate":package,"num":"1.0.0","checksum":hash_file(&archive).trim_start_matches("sha256:"),"yanked":false});
        [
            (format!("/{package}/1.0.0"), serde_json::to_string(&json!({"version":version})).unwrap()),
            (format!("/{package}/versions"), serde_json::to_string(&json!({"versions":[version],"meta":{"total":1,"next_page":null}})).unwrap()),
        ]
    }).collect::<Vec<_>>();
    responses.sort();
    // Subprocess fixture for the real registry command boundary. No network
    // requests or authored documentation JSON enter this qualification test.
    let curl_source = support.join("fixture-curl.rs");
    fs::write(
        &curl_source,
        format!("fn main() {{ let args: Vec<_> = std::env::args().collect(); assert!(args.windows(2).any(|pair| pair[0] == \"--user-agent\" && pair[1].starts_with(\"acyclic-sdk-docs/\") && pair[1].contains(\"https://github.com/acyclic-labs/sdk\"))); let responses = {responses:?}; match responses.iter().find(|(suffix, _)| args.last().unwrap().ends_with(*suffix)) {{ Some((_, response)) => print!(\"{{}}\\n200\", response), None => print!(\"\\n404\") }} }}"),
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
    run_historical_generate_mode_with_env(fixture, support, mode, None)
}

fn run_historical_generate_mode_with_env(
    fixture: &Fixture,
    support: &Path,
    mode: Option<&str>,
    override_lock: Option<&Path>,
) -> std::process::Output {
    run_historical_generate_mode_with_authority(fixture, support, mode, override_lock, None)
}

fn run_historical_generate_mode_with_authority(
    fixture: &Fixture,
    support: &Path,
    mode: Option<&str>,
    override_lock: Option<&Path>,
    automatic_config: Option<(&Path, bool)>,
) -> std::process::Output {
    let path = fixture_path(support);
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
        if mode == "registry-archives" {
            let anchors = support.join("registry-frontier-revisions");
            let revisions = if anchors.exists() {
                fs::read_to_string(anchors).unwrap()
            } else {
                git_revision(&fixture.root)
            };
            command
                .arg("--registry-frontier-repository")
                .arg(&fixture.root)
                .arg("--registry-frontier-revisions")
                .arg(revisions.trim());
        }
    }
    if let Some(lock) = override_lock {
        command.env("CARGO_RESOLVER_LOCKFILE_PATH", lock);
    }
    if let Some((directory, cargo_home)) = automatic_config {
        if cargo_home {
            command.env("CARGO_HOME", directory);
        } else {
            command.current_dir(directory);
        }
    }
    command.output().unwrap()
}

fn registry_archive_fixture(
    dirty: bool,
    published_lock: bool,
    mutation: Option<&str>,
    publisher_vcs: bool,
) -> (Fixture, PathBuf) {
    registry_archive_fixture_named(dirty, published_lock, mutation, publisher_vcs, "demo")
}

fn registry_archive_fixture_named(
    dirty: bool,
    published_lock: bool,
    mutation: Option<&str>,
    publisher_vcs: bool,
    package_name: &str,
) -> (Fixture, PathBuf) {
    registry_archive_fixture_configured(
        dirty,
        published_lock,
        mutation,
        publisher_vcs,
        package_name,
        None,
    )
}

fn registry_archive_fixture_configured(
    dirty: bool,
    published_lock: bool,
    mutation: Option<&str>,
    publisher_vcs: bool,
    package_name: &str,
    setup: Option<fn(&Path)>,
) -> (Fixture, PathBuf) {
    let (fixture, support) = historical_fixture(None);
    let package = fixture.root.join("rust/crates/demo");
    let manifest = fs::read_to_string(package.join("Cargo.toml"))
        .unwrap()
        .replace("name = \"demo\"", &format!("name = {package_name:?}"));
    fs::write(package.join("Cargo.toml"), manifest.replace("[package]\n", "[package]\ninclude = [\"Cargo.toml\", \"README.md\", \"src/**\", \"examples/**\", \"descriptor.bin\", \"input.json\", \"schema.proto\"]\n")).unwrap();
    // Original Cargo and release manifests establish each renamed owner's
    // authority. Archive-only API bytes below remain divergent from this root.
    fs::write(
        fixture.root.join("release/cargo-crates.json"),
        serde_json::to_vec(&vec![package_name]).unwrap(),
    )
    .unwrap();
    let lock = fs::read_to_string(fixture.root.join("Cargo.lock")).unwrap();
    fs::write(
        fixture.root.join("Cargo.lock"),
        lock.replace("name = \"demo\"", &format!("name = {package_name:?}")),
    )
    .unwrap();
    git(
        &fixture.root,
        &[
            "add",
            "rust/crates/demo/Cargo.toml",
            "release/cargo-crates.json",
            "Cargo.lock",
        ],
    );
    git(
        &fixture.root,
        &[
            "commit",
            "--quiet",
            "-m",
            "original registry owner authority",
        ],
    );
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
    if let Some(setup) = setup {
        setup(&package);
    }
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
        .arg(support.join(format!("package-target/package/{package_name}-1.0.0.crate")))
        .arg("-C")
        .arg(&unpack)
        .output()
        .unwrap();
    assert!(extracted.status.success(), "{}", output_message(&extracted));
    let source = unpack.join(format!("{package_name}-1.0.0"));
    fs::write(source.join(".cargo_vcs_info.json"), serde_json::to_vec(&json!({"git":{"sha1":git_revision(&fixture.root),"dirty":dirty},"path_in_vcs":"rust/crates/demo"})).unwrap()).unwrap();
    if !publisher_vcs {
        fs::remove_file(source.join(".cargo_vcs_info.json")).unwrap();
    }
    if !published_lock {
        fs::remove_file(source.join("Cargo.lock")).unwrap();
    }
    let archive = support.join(format!("registry/{package_name}-1.0.0.crate"));
    let packed = Command::new("tar")
        .args(["-cf"])
        .arg(&archive)
        .arg("-C")
        .arg(&unpack)
        .arg(format!("{package_name}-1.0.0"))
        .output()
        .unwrap();
    assert!(packed.status.success(), "{}", output_message(&packed));
    if package_name != "demo" {
        fs::remove_file(support.join("registry/demo-1.0.0.crate")).unwrap();
    }
    write_registry_transport(&support);
    (fixture, support)
}

#[test]
fn registry_published_and_missing_locks_ignore_ambient_lock_selection() {
    for published in [true, false] {
        let (fixture, support) = registry_archive_fixture(false, published, None, true);
        let foreign = support.join("foreign/Cargo.lock");
        fs::create_dir_all(foreign.parent().unwrap()).unwrap();
        fs::write(&foreign, "invalid foreign lock must never be selected\n").unwrap();
        let archive = support.join("registry/demo-1.0.0.crate");
        let original_archive = fs::read(&archive).unwrap();
        let result = run_historical_generate_mode_with_env(
            &fixture,
            &support,
            Some("registry-archives"),
            Some(&foreign),
        );
        assert!(result.status.success(), "{}", output_message(&result));
        assert_eq!(
            fs::read(&foreign).unwrap(),
            b"invalid foreign lock must never be selected\n"
        );
        assert_eq!(fs::read(&archive).unwrap(), original_archive);
        let data: Value = serde_json::from_slice(
            &fs::read(fixture.output.join("releases/1.0.0/sdk-docs-data.v1.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            data["source"]["capturedSource"]["archives"][0]["resolutionLock"]["kind"],
            if published {
                "published"
            } else {
                "docsProducer"
            }
        );
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
}

#[test]
fn archive_locks_override_foreign_automatic_cargo_configuration() {
    for published in [true, false] {
        for cargo_home in [true, false] {
            let (fixture, support) = registry_archive_fixture(false, published, None, true);
            let directory = support.join("automatic-config");
            let config_directory = if cargo_home {
                directory.clone()
            } else {
                directory.join(".cargo")
            };
            fs::create_dir_all(&config_directory).unwrap();
            let foreign = support.join("foreign-automatic/Cargo.lock");
            fs::create_dir_all(foreign.parent().unwrap()).unwrap();
            let foreign = foreign
                .parent()
                .unwrap()
                .canonicalize()
                .unwrap()
                .join("Cargo.lock");
            let foreign_bytes = b"invalid foreign automatic lock must not be selected\n";
            fs::write(&foreign, foreign_bytes).unwrap();
            let config = format!(
                "[resolver]\nlockfile-path = {}\n",
                serde_json::to_string(foreign.to_str().unwrap()).unwrap()
            );
            fs::write(config_directory.join("config.toml"), &config).unwrap();
            // Negative control: maintained Cargo really discovers this automatic config.
            let mut control = Command::new("cargo");
            control
                .args([
                    "metadata",
                    "--locked",
                    "--offline",
                    "--format-version",
                    "1",
                    "--manifest-path",
                ])
                .arg(support.join("cargo-packaged-source/demo-1.0.0/Cargo.toml"))
                .env_remove("CARGO_RESOLVER_LOCKFILE_PATH");
            if cargo_home {
                control.env("CARGO_HOME", &directory);
            } else {
                control.current_dir(&directory);
            }
            let rejected = control.output().unwrap();
            assert!(
                !rejected.status.success(),
                "automatic config negative control did not select foreign lock"
            );
            assert!(
                output_message(&rejected).contains("lock"),
                "{}",
                output_message(&rejected)
            );
            let archive = support.join("registry/demo-1.0.0.crate");
            let original_archive = fs::read(&archive).unwrap();
            let result = run_historical_generate_mode_with_authority(
                &fixture,
                &support,
                Some("registry-archives"),
                None,
                Some((&directory, cargo_home)),
            );
            assert!(result.status.success(), "{}", output_message(&result));
            assert_eq!(fs::read(&foreign).unwrap(), foreign_bytes);
            assert_eq!(fs::read(&archive).unwrap(), original_archive);
            assert_eq!(
                fs::read_to_string(config_directory.join("config.toml")).unwrap(),
                config
            );
            let data: Value = serde_json::from_slice(
                &fs::read(fixture.output.join("releases/1.0.0/sdk-docs-data.v1.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                data["source"]["capturedSource"]["archives"][0]["resolutionLock"]["kind"],
                if published {
                    "published"
                } else {
                    "docsProducer"
                }
            );
            let _ = fs::remove_dir_all(fixture.root);
            let _ = fs::remove_dir_all(fixture.output);
        }
    }
}

#[test]
fn imported_initial_extras_and_profile_added_files_cannot_admit_catalog() {
    fn add_input_during_profile(package: &Path) {
        let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap();
        fs::write(
            package.join("Cargo.toml"),
            manifest.replace(
                "include = [",
                "build = \"build.rs\"\ninclude = [\"build.rs\", ",
            ),
        )
        .unwrap();
        fs::write(package.join("build.rs"), "fn main() { std::fs::write(std::path::Path::new(&std::env::var(\"CARGO_MANIFEST_DIR\").unwrap()).join(\"unverified-added.json\"), \"{}\").unwrap(); }\n").unwrap();
    }
    for during_profile in [false, true] {
        let (fixture, support) = registry_archive_fixture_configured(
            false,
            true,
            None,
            true,
            "demo",
            if during_profile {
                Some(add_input_during_profile)
            } else {
                None
            },
        );
        if !during_profile {
            let unknown = fixture
                .output
                .join("releases/1.0.0/sources/.docs-producer/unknown-owner/config.toml");
            fs::create_dir_all(unknown.parent().unwrap()).unwrap();
            fs::write(unknown, "unknown producer input").unwrap();
        }
        let result = run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
        assert!(!result.status.success());
        assert!(
            output_message(&result).contains(if during_profile {
                "source closure changed during qualification before manifest admission"
            } else {
                "initial imported source closure differs from verified archive inputs"
            }),
            "{}",
            output_message(&result)
        );
        assert!(!fixture.output.join("sdk-docs-versions.v1.json").exists());
        if during_profile {
            assert!(fixture
                .output
                .join("releases/1.0.0/sources/demo-1.0.0/unverified-added.json")
                .exists());
        }
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
}

#[test]
fn archive_feature_profiles_bind_all_generated_aliases_and_keep_required_failures_fatal() {
    fn setup(package: &Path) {
        let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap();
        fs::write(
            package.join("Cargo.toml"),
            manifest.replace(
                "include = [",
                "build = \"build.rs\"\ninclude = [\"build.rs\", ",
            ),
        )
        .unwrap();
        fs::write(package.join("src/lib.rs"), "#[cfg(not(feature=\"base\"))]\ncompile_error!(\"original package requires base\");\ninclude!(concat!(env!(\"OUT_DIR\"), \"/wire.rs\"));\n").unwrap();
        fs::write(package.join("build.rs"), "fn main() { let name = if std::env::var_os(\"CARGO_FEATURE_EXTRA\").is_some() { \"ExtraWire\" } else { \"DefaultWire\" }; std::fs::write(std::path::PathBuf::from(std::env::var(\"OUT_DIR\").unwrap()).join(\"wire.rs\"), format!(\"/// Actual feature-generated API.\\npub struct {name};\\n\")).unwrap(); }\n").unwrap();
    }
    fn required_no_defaults(package: &Path) {
        setup(package);
        let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap();
        fs::write(package.join("Cargo.toml"), format!("{manifest}\n[package.metadata.docs.rs]\nno-default-features=true\nfeatures=['extra']\n")).unwrap();
    }
    for required_failure in [false, true] {
        let (fixture, support) = registry_archive_fixture_configured(
            false,
            true,
            None,
            true,
            "demo",
            Some(if required_failure {
                required_no_defaults
            } else {
                setup
            }),
        );
        let result = run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
        if required_failure {
            assert!(!result.status.success());
            assert!(output_message(&result).contains("original package requires base"));
            assert!(
                !fixture.output.join("sdk-docs-versions.v1.json").exists(),
                "required upstream profile failure cannot admit the version"
            );
        } else {
            assert!(result.status.success(), "{}", output_message(&result));
            let output = fixture.output.join("releases/1.0.0");
            let profiles: Value = serde_json::from_slice(
                &fs::read(output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
            )
            .unwrap();
            assert!(profiles
                .as_array()
                .unwrap()
                .iter()
                .all(|profile| profile["defaultFeatures"] == true));
            let data: Value =
                serde_json::from_slice(&fs::read(output.join("sdk-docs-data.v1.json")).unwrap())
                    .unwrap();
            for name in ["DefaultWire", "ExtraWire"] {
                let item = data["families"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|family| family["items"].as_array().unwrap())
                    .find(|item| item["name"] == name)
                    .unwrap();
                let path = item["source"]["path"].as_str().unwrap();
                assert!(path.starts_with("releases/1.0.0/generated/rustdoc/"));
                assert!(fs::read_to_string(fixture.output.join(path))
                    .unwrap()
                    .contains(name));
            }
            let aliases: Value =
                serde_json::from_slice(
                    &fs::read(output.join(
                        "sources/target/sdk-generation-generated-sources/retained/aliases.json",
                    ))
                    .unwrap(),
                )
                .unwrap();
            assert!(
                aliases
                    .as_object()
                    .unwrap()
                    .keys()
                    .filter(|path| path.ends_with("wire.rs"))
                    .count()
                    >= 2,
                "actual Cargo profiles must use distinct OUT_DIR aliases"
            );
            let drift = run_historical_drift(
                &Fixture {
                    root: fixture.root.clone(),
                    rustdoc: output.join(".rustdoc"),
                    output,
                },
                &support,
            );
            assert!(drift.status.success(), "{}", output_message(&drift));
        }
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
}

#[test]
fn three_archive_owners_bind_identical_relative_spans_to_their_own_source() {
    let fixtures = ["demo-a", "demo-b", "demo-c"]
        .map(|name| registry_archive_fixture_named(false, true, None, true, name));
    let (fixture, support) = &fixtures[0];
    let mut anchors = vec![git_revision(&fixture.root)];
    for (index, (other_fixture, other)) in fixtures.iter().enumerate().skip(1) {
        let name = ["demo-a", "demo-b", "demo-c"][index];
        fs::copy(
            other.join(format!("registry/{name}-1.0.0.crate")),
            support.join(format!("registry/{name}-1.0.0.crate")),
        )
        .unwrap();
        let revision = git_revision(&other_fixture.root);
        git(
            &fixture.root,
            &[
                "fetch",
                "--quiet",
                other_fixture.root.to_str().unwrap(),
                &revision,
            ],
        );
        anchors.push(revision);
    }
    fs::write(
        support.join("registry-frontier-revisions"),
        anchors.join(","),
    )
    .unwrap();
    write_registry_transport(support);
    let generated = run_historical_generate_mode(fixture, support, Some("registry-archives"));
    assert!(generated.status.success(), "{}", output_message(&generated));
    let output = fixture.output.join("releases/1.0.0");
    let data: Value =
        serde_json::from_slice(&fs::read(output.join("sdk-docs-data.v1.json")).unwrap()).unwrap();
    assert_eq!(
        data["source"]["releasedPackages"].as_array().unwrap().len(),
        3
    );
    for (family, package) in data["families"]
        .as_array()
        .unwrap()
        .iter()
        .zip(["demo-a", "demo-b", "demo-c"])
    {
        let item = family["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["name"] == "ArchiveOnly")
            .unwrap();
        assert_eq!(
            item["source"]["path"],
            format!("releases/1.0.0/sources/{package}-1.0.0/src/lib.rs")
        );
        assert!(fixture
            .output
            .join(item["source"]["path"].as_str().unwrap())
            .is_file());
    }
    let drift = run_historical_drift(
        &Fixture {
            root: fixture.root.clone(),
            rustdoc: output.join(".rustdoc"),
            output: output.clone(),
        },
        support,
    );
    assert!(drift.status.success(), "{}", output_message(&drift));
    for (fixture, _) in fixtures {
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
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
            serde_json::from_slice(&fs::read(output.join("sdk-docs-data.v1.json")).unwrap())
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
            let drift = run_historical_drift(&imported, &support);
            assert!(!drift.status.success(), "{path} drift was accepted");
            fs::write(&source, original).unwrap();
        }
        fs::write(output.join("sources/demo-1.0.0/stale-extra.bin"), b"stale").unwrap();
        assert!(!run_historical_drift(&imported, &support).status.success());
        let _ = fs::remove_dir_all(fixture.root);
        let _ = fs::remove_dir_all(fixture.output);
    }
}

#[test]
fn covered_publisher_history_keeps_expanded_anchors_and_rejects_export_drift() {
    let (fixture, support) = registry_archive_fixture(true, true, None, true);
    let publisher = git_revision(&fixture.root);
    git(
        &fixture.root,
        &[
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "newer declared history",
        ],
    );
    let declared = git_revision(&fixture.root);
    assert_ne!(publisher, declared);
    let generated = run_historical_generate_mode(&fixture, &support, Some("registry-archives"));
    assert!(generated.status.success(), "{}", output_message(&generated));
    let output = fixture.output.join("releases/1.0.0");
    let manifest: Value =
        serde_json::from_slice(&fs::read(output.join("generation-manifest.v1.json")).unwrap())
            .unwrap();
    let corpus = &manifest["registryCorpus"];
    assert_eq!(corpus["declared_anchors"], json!([declared]));
    let anchors = corpus["anchors"].as_array().unwrap();
    assert_eq!(anchors.len(), 2);
    assert!(anchors.contains(&json!(publisher)));
    assert!(anchors.contains(&json!(declared)));
    let imported = Fixture {
        root: fixture.root.clone(),
        rustdoc: output.join(".rustdoc"),
        output: output.clone(),
    };
    let changed_context = run_drift(&imported);
    assert!(!changed_context.status.success());
    assert!(
        output_message(&changed_context)
            .contains("historical source frontier or discovery context changed"),
        "{}",
        output_message(&changed_context)
    );
    let drift = run_historical_drift(&imported, &support);
    assert!(drift.status.success(), "{}", output_message(&drift));
    fs::write(
        output
            .join("registry-frontier")
            .join(&publisher)
            .join("rust/crates/demo/src/lib.rs"),
        b"tampered original publisher export",
    )
    .unwrap();
    let drift = run_historical_drift(&imported, &support);
    assert!(
        !drift.status.success(),
        "changed original publisher export was admitted"
    );
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}

#[test]
fn historical_source_help_and_flag_contract_are_explicit() {
    let binary = env!("CARGO_BIN_EXE_sdk-generation");
    let help = Command::new(binary).arg("help").output().unwrap();
    assert!(help.status.success());
    let text = output_message(&help);
    assert!(text.contains("--historical-archives DIR"));
    assert!(text.contains("--historical-source git|registry-archives"));
    let missing_frontier = Command::new(binary)
        .args([
            "generate",
            "--root",
            "missing-root",
            "--output",
            "missing-output",
            "--version",
            "1.0.0",
            "--execute-profiles",
            "--channel",
            "release",
            "--historical-source",
            "registry-archives",
            "--historical-archives",
            "missing-archives",
        ])
        .output()
        .unwrap();
    assert!(!missing_frontier.status.success());
    assert!(output_message(&missing_frontier).contains("requires a complete registry corpus"));
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
        serde_json::from_slice(&fs::read(version.join("sdk-docs-data.v1.json")).unwrap()).unwrap();
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
            output_message(&result)
                .contains("source closure changed during qualification before manifest admission"),
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

#[test]
fn excluded_published_path_owners_have_executed_api_and_drift() {
    let fixture = feature_profile_fixture();
    let manifest = fixture.root.join("rust/vendor/macros/Cargo.toml");
    fs::create_dir_all(manifest.parent().unwrap().join("src")).unwrap();
    let demo_manifest = fixture.root.join("rust/crates/demo/Cargo.toml");
    let original = fs::read_to_string(&demo_manifest)
        .unwrap()
        .replace("name = \"demo\"", "name = \"demo-macros\"");
    fs::write(
        &demo_manifest,
        format!("{}\n[dependencies]\ndemo-macros = {{ path = \"../../vendor/macros\" }}\ndemo-helper = {{ path = \"../../vendor/helper\" }}\n", fs::read_to_string(&demo_manifest).unwrap()),
    ).unwrap();
    fs::write(fixture.root.join("Cargo.toml"), "[workspace]\nmembers = [\"rust/crates/demo\"]\nexclude = [\"rust/vendor/macros\", \"rust/vendor/helper\", \"rust/vendor/macro-dev-helper\"]\nresolver = \"2\"\n").unwrap();
    fs::create_dir_all(fixture.root.join("rust/vendor/helper/src")).unwrap();
    fs::write(
        fixture.root.join("rust/vendor/helper/Cargo.toml"),
        "[package]\nname = \"demo-helper\"\nversion = \"1.0.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("rust/vendor/helper/src/lib.rs"),
        "/// Published excluded library API.\npub struct ExcludedOwnedApi;\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("release/cargo-crates.json"),
        "[\"demo\",\"demo-macros\",\"demo-helper\"]\n",
    )
    .unwrap();
    fs::write(fixture.root.join("Cargo.lock"), "version = 3\n\n[[package]]\nname = \"demo\"\nversion = \"1.0.0\"\ndependencies = [\"demo-helper\", \"demo-macros\"]\n\n[[package]]\nname = \"demo-helper\"\nversion = \"1.0.0\"\n\n[[package]]\nname = \"demo-macros\"\nversion = \"1.0.0\"\n").unwrap();
    fs::write(
        &manifest,
        original.replace(
            "[lib]\npath = \"src/lib.rs\"",
            "[lib]\nname = \"renamed_macro_target\"\nproc-macro = true\npath = \"src/lib.rs\"",
        ),
    )
    .unwrap();
    let macro_manifest = fs::read_to_string(&manifest).unwrap();
    fs::write(&manifest, format!("{macro_manifest}\n[dev-dependencies]\nmacro-dev-helper = {{ path = \"../macro-dev-helper\" }}\n")).unwrap();
    fs::create_dir_all(fixture.root.join("rust/vendor/macro-dev-helper/src")).unwrap();
    fs::write(fixture.root.join("rust/vendor/macro-dev-helper/Cargo.toml"), "[package]\nname = \"macro-dev-helper\"\nversion = \"1.0.0\"\nedition = \"2021\"\npublish = false\n").unwrap();
    fs::write(
        fixture.root.join("rust/vendor/macro-dev-helper/src/lib.rs"),
        "pub struct DevOnly;\n",
    )
    .unwrap();
    fs::write(fixture.root.join("rust/vendor/macros/Cargo.lock"), "version = 3\n\n[[package]]\nname = \"demo-macros\"\nversion = \"1.0.0\"\ndependencies = [\"macro-dev-helper\"]\n\n[[package]]\nname = \"macro-dev-helper\"\nversion = \"1.0.0\"\n").unwrap();
    fs::write(
        fixture.root.join("rust/vendor/macros/src/lib.rs"),
        r#"//! Public procedural macro documentation.
extern crate proc_macro;
use proc_macro::TokenStream;
/// Return the supplied tokens unchanged.
#[proc_macro]
pub fn demo_macro(input: TokenStream) -> TokenStream { input }
/// Preserve an attributed item.
#[proc_macro_attribute]
pub fn demo_attribute(_args: TokenStream, input: TokenStream) -> TokenStream { input }
/// Add no derived implementation.
#[proc_macro_derive(DemoDerive)]
pub fn demo_derive(_input: TokenStream) -> TokenStream { TokenStream::new() }
"#,
    )
    .unwrap();
    git(&fixture.root, &["add", "."]);
    git(
        &fixture.root,
        &["commit", "--quiet", "-m", "procedural macro owner fixture"],
    );

    let workspace_only =
        sdk_docs::rustdoc_profiles::load_metadata(fixture.root.join("Cargo.toml")).unwrap();
    assert!(!workspace_only
        .packages
        .iter()
        .any(|package| matches!(package.name.as_ref(), "demo-macros" | "demo-helper")));
    let locked_bytes = fs::read(fixture.root.join("Cargo.lock")).unwrap();
    let macro_locked_bytes = fs::read(fixture.root.join("rust/vendor/macros/Cargo.lock")).unwrap();
    let generated = run_generate_execute_profiles(&fixture);
    assert!(generated.status.success(), "{}", output_message(&generated));
    assert!(fixture.rustdoc.join("renamed_macro_target.json").is_file());
    let availability: Value = serde_json::from_slice(
        &fs::read(fixture.output.join("sdk-docs-profile-availability.v1.json")).unwrap(),
    )
    .unwrap();
    for name in ["demo_macro", "demo_attribute", "DemoDerive"] {
        let expected = format!("renamed_macro_target::{name}");
        assert!(
            availability["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry["path"].as_str() == Some(expected.as_str())
                        && entry["profiles"]
                            .as_array()
                            .is_some_and(|profiles| !profiles.is_empty())
                }),
            "the actual procedural macro API {expected} must have executed profile coverage"
        );
    }
    assert!(availability["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["path"] == "demo_helper::ExcludedOwnedApi"));
    let data: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .output
                .join("preview/feature-profile-catalog/sdk-docs-data.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    for (path, source) in [
        (
            "renamed_macro_target::demo_macro",
            "rust/vendor/macros/src/lib.rs",
        ),
        (
            "demo_helper::ExcludedOwnedApi",
            "rust/vendor/helper/src/lib.rs",
        ),
    ] {
        let item = data["families"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|family| family["items"].as_array().unwrap())
            .find(|item| item["path"] == path)
            .unwrap();
        assert_eq!(item["source"]["path"], source);
    }
    let drift = run_drift_execute_profiles(&fixture);
    assert!(drift.status.success(), "{}", output_message(&drift));
    assert_eq!(
        fs::read(fixture.root.join("Cargo.lock")).unwrap(),
        locked_bytes
    );
    assert_eq!(
        fs::read(fixture.root.join("rust/vendor/macros/Cargo.lock")).unwrap(),
        macro_locked_bytes
    );
    assert!(!fixture.root.join("rust/vendor/helper/Cargo.lock").exists());
    let _ = fs::remove_dir_all(fixture.root);
    let _ = fs::remove_dir_all(fixture.output);
}
