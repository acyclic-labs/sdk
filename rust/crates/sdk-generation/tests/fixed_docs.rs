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

fn command(
    binary: &Path,
    operation: &str,
    root: &Path,
    rustdoc: Option<&Path>,
    output: &Path,
    channel: &str,
) -> Command {
    let mut command = Command::new(binary);
    command.args([
        operation,
        "--root",
        root.to_str().unwrap(),
        "--output",
        output.to_str().unwrap(),
        "--version",
        "0.2.0",
        "--channel",
        channel,
    ]);
    if let Some(rustdoc) = rustdoc {
        command.args(["--rustdoc-json", rustdoc.to_str().unwrap()]);
    }
    command
}

fn run(
    binary: &Path,
    operation: &str,
    root: &Path,
    rustdoc: Option<&Path>,
    output: &Path,
    channel: &str,
) -> std::process::Output {
    command(binary, operation, root, rustdoc, output, channel)
        .output()
        .unwrap()
}

#[test]
fn fixed_docs_stage_binds_git_source_and_rejects_drift() {
    let root = temp("sdk-generation-root");
    let rustdoc = temp("sdk-generation-rustdoc");
    let first = temp("sdk-generation-first");
    let second = temp("sdk-generation-second");
    let foreign = temp("sdk-generation-foreign");
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
            "0": {"id": 0, "crate_id": 0, "name": "acyclic_actors", "span": null, "visibility": "public", "docs": null, "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"module": {"is_crate": true, "items": [1], "is_stripped": false}}},
            "1": {"id": 1, "crate_id": 0, "name": "visible", "span": null, "visibility": "public", "docs": "visible", "links": {}, "attrs": [], "deprecation": null, "stability": null, "const_stability": null, "inner": {"function": {"sig": {"inputs": [], "output": null, "is_c_variadic": false}, "generics": {"params": [], "where_predicates": []}, "header": {"is_const": false, "is_unsafe": false, "is_async": false, "abi": "Rust"}, "has_body": true, "default_unstable": null}}}
        },
        "paths": {"1": {"crate_id": 0, "path": ["acyclic_actors", "visible"], "kind": "function"}},
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
        !run(binary, "generate", &root, Some(&rustdoc), &first, "release")
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
            "preview",
            "--channel",
            "preview",
        ])
        .output()
        .unwrap();
    assert!(!duplicate.status.success());
    for output in [&first, &second] {
        let result = run(binary, "generate", &root, Some(&rustdoc), output, "preview");
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
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(first.join("generation-manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["tool"]["id"], "sdk-docs-library");
    assert_eq!(manifest["family"], "acyclic_actors");
    assert_eq!(manifest["tool"]["version"], "0.2.0");
    assert_eq!(manifest["tool"]["channel"], "preview");
    assert!(manifest["tool"].get("args").is_none());
    assert_eq!(
        fs::read(first.join("preview/0.2.0/sdk-docs-data.v1.json")).unwrap(),
        fs::read(second.join("preview/0.2.0/sdk-docs-data.v1.json")).unwrap()
    );
    assert_eq!(
        fs::read(first.join("preview/0.2.0/sdk-docs-data.v1.schema.json")).unwrap(),
        fs::read(second.join("preview/0.2.0/sdk-docs-data.v1.schema.json")).unwrap()
    );
    let index: serde_json::Value =
        serde_json::from_slice(&fs::read(first.join("sdk-docs-versions.v1.json")).unwrap())
            .unwrap();
    assert_eq!(index["preview"]["version"], "0.2.0");
    let data = fs::read_to_string(first.join("preview/0.2.0/sdk-docs-data.v1.json")).unwrap();
    assert!(data.contains("sdk-docs-data.v1"));
    let guide_path = root.join("docs/objects-v2-http.md");
    let guide = fs::read(&guide_path).unwrap();
    fs::write(&guide_path, b"tampered guide\n").unwrap();
    assert!(
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    fs::write(&guide_path, guide).unwrap();
    assert!(
        run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    let data_path = first.join("preview/0.2.0/sdk-docs-data.v1.json");
    let data_bytes = fs::read(&data_path).unwrap();
    fs::write(&data_path, b"tampered generated data\n").unwrap();
    assert!(
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    fs::write(&data_path, data_bytes).unwrap();
    assert!(
        run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    let schema_path = first.join("preview/0.2.0/sdk-docs-data.v1.schema.json");
    let schema_bytes = fs::read(&schema_path).unwrap();
    fs::write(&schema_path, b"tampered generated schema\n").unwrap();
    assert!(
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    fs::write(&schema_path, schema_bytes).unwrap();
    assert!(
        run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    let mut foreign_fixture = fixture.clone();
    foreign_fixture["index"]["0"]["name"] = json!("foreign_crate");
    foreign_fixture["paths"]["1"]["path"] = json!(["foreign_crate", "visible"]);
    fs::write(
        rustdoc.join("actors.json"),
        serde_json::to_vec(&foreign_fixture).unwrap(),
    )
    .unwrap();
    assert!(
        !run(
            binary,
            "generate",
            &root,
            Some(&rustdoc),
            &foreign,
            "preview"
        )
        .status
        .success()
    );
    fs::write(
        rustdoc.join("actors.json"),
        serde_json::to_vec(&fixture).unwrap(),
    )
    .unwrap();
    fs::write(root.join("rust/crates/actors/src/lib.rs"), "tampered\n").unwrap();
    assert!(
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
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
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(rustdoc);
    let _ = fs::remove_dir_all(first);
    let _ = fs::remove_dir_all(second);
    let _ = fs::remove_dir_all(foreign);
}

#[test]
fn release_generation_builds_rustdoc_from_the_pinned_workspace() {
    let probe = Command::new("rustup")
        .args(["run", "1.98.1", "rustdoc", "--version"])
        .output()
        .unwrap();
    if !probe.status.success()
        && String::from_utf8_lossy(&probe.stderr).contains("rustdoc.exe' is not installed")
    {
        eprintln!("skipping pinned Rustdoc integration test: Rustdoc is not installed");
        return;
    }
    let sandbox = temp("sdk-generation-rustdoc-stage");
    let root = sandbox.join("root");
    let output = sandbox.join("bundle");
    fs::create_dir_all(root.join("rust/crates/actors/src")).unwrap();
    fs::create_dir_all(root.join("rust/crates/actors/examples")).unwrap();
    fs::create_dir_all(root.join("rust/crates/sdk-docs/src")).unwrap();
    fs::create_dir_all(root.join("rust/crates/sdk-generation/src")).unwrap();
    fs::create_dir_all(root.join("rust/crates/sdk-generation/tests")).unwrap();
    fs::create_dir_all(root.join("docs")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"rust/crates/actors\"]\nresolver = \"2\"\n\n[workspace.package]\nversion = \"0.2.0\"\nedition = \"2024\"\nrust-version = \"1.98.1\"\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/actors/Cargo.toml"),
        "[package]\nname = \"acyclic-actors\"\nversion.workspace = true\nedition.workspace = true\nrust-version.workspace = true\npublish = false\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/actors/src/lib.rs"),
        "//! The executable Rust source for the Actors family.\n\npub fn visible() {}\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/actors/examples/example.rs"),
        "fn main() {}\n",
    )
    .unwrap();
    for (path, contents) in [
        ("rust/crates/actors/README.md", "Actors\n"),
        (
            "rust/crates/sdk-docs/Cargo.toml",
            "[package]\nname = \"sdk-docs\"\nversion = \"0.2.0\"\nedition = \"2024\"\n",
        ),
        ("rust/crates/sdk-docs/Cargo.lock", "docs lock\n"),
        ("rust/crates/sdk-docs/src/lib.rs", "pub fn docs() {}\n"),
        (
            "rust/crates/sdk-generation/Cargo.toml",
            "[package]\nname = \"sdk-generation\"\nversion = \"0.2.0\"\nedition = \"2024\"\n",
        ),
        ("rust/crates/sdk-generation/Cargo.lock", "generation lock\n"),
        (
            "rust/crates/sdk-generation/README.md",
            "generation launcher\n",
        ),
        (
            "rust/crates/sdk-generation/rust-toolchain.toml",
            "[toolchain]\nchannel = \"1.98.1\"\n",
        ),
        ("rust/crates/sdk-generation/src/main.rs", "fn main() {}\n"),
        (
            "rust/crates/sdk-generation/tests/fixed_docs.rs",
            "fixture test\n",
        ),
        ("docs/objects-v2-http.md", "objects guide\n"),
        ("docs/rust-source-generation.md", "generation guide\n"),
        ("rust-toolchain.toml", "[toolchain]\nchannel = \"1.98.1\"\n"),
        (".gitignore", "target/\n"),
    ] {
        fs::write(root.join(path), contents).unwrap();
    }
    let lock = Command::new("cargo")
        .args([
            "+1.98.1",
            "generate-lockfile",
            "--manifest-path",
            root.join("Cargo.toml").to_str().unwrap(),
            "--offline",
        ])
        .output()
        .unwrap();
    assert!(
        lock.status.success(),
        "cargo generate-lockfile: {}",
        String::from_utf8_lossy(&lock.stderr)
    );
    git(&root, &["init", "--quiet"]);
    git(&root, &["config", "user.email", "fixture@example.invalid"]);
    git(&root, &["config", "user.name", "fixture"]);
    git(&root, &["add", "."]);
    git(&root, &["commit", "--quiet", "-m", "fixture"]);

    let binary = Path::new(env!("CARGO_BIN_EXE_sdk-generation"));
    let result = command(binary, "generate", &root, None, &output, "release")
        .env("RUSTC", root.join("missing-rustc.exe"))
        .env("RUSTDOC", root.join("missing-rustdoc.exe"))
        .env("RUSTFLAGS", "--cfg injected_compiler_override")
        .env("RUSTDOCFLAGS", "--cfg injected_rustdoc_override")
        .env("CARGO_ENCODED_RUSTFLAGS", "--cfg injected")
        .env("RUSTC_WRAPPER", root.join("missing-wrapper.exe"))
        .env(
            "RUSTC_WORKSPACE_WRAPPER",
            root.join("missing-workspace-wrapper.exe"),
        )
        .env("CARGO_BUILD_TARGET", "injected-target")
        .env(
            "CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_RUSTFLAGS",
            "--cfg injected_target_override",
        )
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let index: serde_json::Value =
        serde_json::from_slice(&fs::read(output.join("sdk-docs-versions.v1.json")).unwrap())
            .unwrap();
    assert_eq!(index["latest"]["version"], "0.2.0");
    let drift = run(binary, "drift", &root, None, &output, "release");
    assert!(
        drift.status.success(),
        "{}",
        String::from_utf8_lossy(&drift.stderr)
    );

    let _ = fs::remove_dir_all(sandbox);
}
