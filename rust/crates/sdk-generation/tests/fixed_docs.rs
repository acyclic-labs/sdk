use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[path = "../src/compiled_generator_inputs.rs"]
mod compiled_generator_inputs;

const FIXTURE_OWNERS: &[(&str, &str)] = &[
    ("acyclic-inference", "rust/crates/inference"),
    (
        "acyclic-inference-contract",
        "rust/crates/inference-contract",
    ),
    ("acyclic-machines", "rust/crates/machines"),
    ("acyclic-native-runtime", "rust/crates/native-runtime"),
    ("acyclic-objects", "rust/crates/objects"),
    ("acyclic-actors", "rust/crates/actors"),
    ("acyclic-workers", "rust/crates/workers"),
    ("acyclic-stream", "rust/crates/stream"),
    ("acyclic-fs", "rust/crates/filesystem"),
    ("acyclic-harness", "rust/crates/harness"),
    ("acyclic-plugin", "plugin"),
];
const FIXTURE_PRIVATE_PACKAGES: &[(&str, &str)] =
    &[("acyclic-actors-napi", "rust/crates/actors-napi")];
const WORKERS_PROTO_HEADER: &str = "// Generated from Rust-owned Workers contract. Do not edit.\n";
const NATIVE_TARGETS: &[&str] = &[
    "x86_64-unknown-linux-gnu",
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-gnu",
    "aarch64-unknown-linux-musl",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "aarch64-pc-windows-msvc",
];

fn temp(_name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join(format!("sg-{nonce:x}"))
}

fn copy_compiled_generator_sources(root: &Path) {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    for relative in compiled_generator_inputs::PATHS {
        let destination = root.join(relative);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(source_root.join(relative), destination).unwrap();
    }
}

fn fixture_workspace_manifest(source_root: &Path) -> String {
    let source = fs::read_to_string(source_root.join("Cargo.toml"))
        .unwrap()
        .replace("\r\n", "\n");
    let members_start = source.find("members = [").unwrap();
    let members_close = source[members_start..]
        .find("]\nresolver")
        .map(|offset| members_start + offset)
        .unwrap();
    let mut manifest = String::with_capacity(source.len());
    manifest.push_str(&source[..members_start]);
    manifest.push_str("members = [\n");
    for (_, relative) in FIXTURE_OWNERS {
        manifest.push_str(&format!("    \"{relative}\",\n"));
    }
    for (_, relative) in FIXTURE_PRIVATE_PACKAGES {
        manifest.push_str(&format!("    \"{relative}\",\n"));
    }
    manifest.push_str("]\n");
    manifest.push_str(&source[members_close + 1..]);
    manifest
}

fn write_fixture_owner_packages(root: &Path) {
    for (package, relative) in FIXTURE_OWNERS {
        if *package == "acyclic-actors" {
            continue;
        }
        let package_root = root.join(relative);
        fs::create_dir_all(package_root.join("src")).unwrap();
        fs::write(
            package_root.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{package}\"\nversion.workspace = true\nedition.workspace = true\nrust-version.workspace = true\npublish.workspace = true\n"
            ),
        )
        .unwrap();
        fs::write(
            package_root.join("src/lib.rs"),
            format!("//! Fixture owner for {package}.\n\npub fn visible() {{}}\n"),
        )
        .unwrap();
    }
}

fn write_fixture_private_packages(root: &Path) {
    let package_root = root.join("rust/crates/actors-napi");
    fs::create_dir_all(package_root.join("src")).unwrap();
    fs::create_dir_all(package_root.join("qualification")).unwrap();
    fs::write(
        package_root.join("Cargo.toml"),
        format!(
            "[package]\nname = \"acyclic-actors-napi\"\nversion.workspace = true\nedition.workspace = true\nrust-version.workspace = true\npublish = false\nbuild = \"build.rs\"\n\n[package.metadata.napi]\ntargets = [{}]\n",
            NATIVE_TARGETS
                .iter()
                .map(|target| format!("\"{target}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
    .unwrap();
    fs::write(package_root.join("build.rs"), "fn main() {}\n").unwrap();
    fs::write(package_root.join("src/lib.rs"), "pub fn bridge() {}\n").unwrap();
    fs::write(package_root.join("README.md"), "Actors N-API fixture\n").unwrap();
    fs::write(
        package_root.join("qualification/fixture.json"),
        "{\"target\":\"fixture\"}\n",
    )
    .unwrap();
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
    command.current_dir(root.parent().unwrap());
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
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let root = temp("sdk-generation-root");
    let rustdoc = temp("sdk-generation-rustdoc");
    let first = temp("sdk-generation-first");
    let second = temp("sdk-generation-second");
    let foreign = temp("sdk-generation-foreign");
    let foreign_docs = temp("sdk-generation-foreign-docs");
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
        "rust/crates/actors/build.rs",
        "rust/crates/actors/examples/example.rs",
        "rust/crates/sdk-docs/src/lib.rs",
        "rust/crates/sdk-generation/src/main.rs",
        "rust/crates/sdk-generation/build.rs",
        "rust/crates/sdk-generation/tests/fixed_docs.rs",
        "docs/objects-v2-http.md",
        "docs/rust-source-generation.md",
    ] {
        fs::write(root.join(path), "rust-owned source\n").unwrap();
    }
    fs::write(
        root.join("Cargo.toml"),
        fixture_workspace_manifest(&source_root),
    )
    .unwrap();
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
    fs::write(
        root.join("docs/unlisted-tracked-guide.md"),
        "tracked guide discovered from the source closure\n",
    )
    .unwrap();
    write_fixture_owner_packages(&root);
    write_fixture_private_packages(&root);
    copy_compiled_generator_sources(&root);
    fs::copy(source_root.join("Cargo.lock"), root.join("Cargo.lock")).unwrap();
    let actors_lib_path = root.join("rust/crates/actors/src/lib.rs");
    let actors_lib = fs::read(&actors_lib_path).unwrap();
    fs::write(root.join(".gitignore"), "rust/crates/sdk-docs/target/\n").unwrap();
    fs::create_dir_all(&rustdoc).unwrap();
    let fixture = json!({
        "root": 0,
        "crate_version": "0.2.0",
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
    for (package, _) in FIXTURE_OWNERS {
        if *package == "acyclic-actors" {
            continue;
        }
        let crate_name = package.replace('-', "_");
        let mut owner_fixture = fixture.clone();
        owner_fixture["index"]["0"]["name"] = json!(crate_name);
        owner_fixture["paths"]["1"]["path"] = json!([crate_name, "visible"]);
        fs::write(
            rustdoc.join(format!("{crate_name}.json")),
            serde_json::to_vec(&owner_fixture).unwrap(),
        )
        .unwrap();
    }
    git(&root, &["init", "--quiet"]);
    git(&root, &["config", "user.email", "fixture@example.invalid"]);
    git(&root, &["config", "user.name", "fixture"]);
    git(&root, &["add", "."]);
    git(&root, &["commit", "--quiet", "-m", "fixture"]);
    fs::write(&actors_lib_path, "dirty release source\n").unwrap();
    let binary = Path::new(env!("CARGO_BIN_EXE_sdk-generation"));
    assert!(
        !run(binary, "generate", &root, Some(&rustdoc), &first, "release")
            .status
            .success()
    );
    fs::write(&actors_lib_path, actors_lib).unwrap();
    let source_binding_path = root.join("rust/crates/actors/src/domain.rs");
    let source_binding = fs::read(&source_binding_path).unwrap();
    fs::write(&source_binding_path, b"foreign semantic source\n").unwrap();
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
    fs::write(&source_binding_path, source_binding).unwrap();
    let docs_source_path = root.join("rust/crates/sdk-docs/src/lib.rs");
    let docs_source = fs::read(&docs_source_path).unwrap();
    fs::write(&docs_source_path, b"foreign docs implementation\n").unwrap();
    assert!(
        !run(
            binary,
            "generate",
            &root,
            Some(&rustdoc),
            &foreign_docs,
            "preview"
        )
        .status
        .success()
    );
    fs::write(&docs_source_path, docs_source).unwrap();
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
    assert_eq!(manifest["tool"]["version"], "0.1.0");
    assert_eq!(manifest["tool"]["channel"], "preview");
    assert!(manifest["tool"].get("args").is_none());
    assert!(
        manifest["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|artifact| { artifact["path"] == "generated/typescript/actors/types.ts" })
    );
    assert!(
        manifest["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|artifact| {
                artifact["path"] == "generated/workers/proto/workers/v1/workers.proto"
            })
    );
    let availability_path = first.join("sdk-docs-profile-availability.v1.json");
    let availability: serde_json::Value =
        serde_json::from_slice(&fs::read(&availability_path).unwrap()).unwrap();
    assert_eq!(availability["schema"], "sdk-docs-profile-availability.v1");
    assert!(!availability["entries"].as_array().unwrap().is_empty());
    assert_eq!(
        fs::read(&availability_path).unwrap(),
        fs::read(second.join("sdk-docs-profile-availability.v1.json")).unwrap()
    );
    assert!(
        manifest["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|artifact| { artifact["path"] == "generated/workers/acyclic-workers-v1.bin" })
    );
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
    let typescript = first.join("generated/typescript/actors/InvokeActorRequest.ts");
    assert!(typescript.is_file());
    assert!(
        fs::read_to_string(&typescript)
            .unwrap()
            .contains("InvokeActorRequest")
    );
    assert_eq!(
        fs::read(&typescript).unwrap(),
        fs::read(second.join("generated/typescript/actors/InvokeActorRequest.ts")).unwrap()
    );
    let types_barrel = first.join("generated/typescript/actors/types.ts");
    assert!(types_barrel.is_file());
    let types_barrel_source = fs::read_to_string(&types_barrel).unwrap();
    assert!(types_barrel_source.contains("export * from \"./InvokeActorRequest.js\";"));
    for entry in fs::read_dir(first.join("generated/typescript/actors")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "ts")
            && path.file_stem().is_some_and(|stem| stem != "types")
        {
            let stem = path.file_stem().unwrap().to_string_lossy();
            assert!(
                types_barrel_source.contains(&format!("export * from \"./{stem}.js\";")),
                "generated module {stem} is missing from the Typescript barrel"
            );
        }
    }
    assert_eq!(
        fs::read(&types_barrel).unwrap(),
        fs::read(second.join("generated/typescript/actors/types.ts")).unwrap()
    );
    let workers_proto = first.join("generated/workers/proto/workers/v1/workers.proto");
    assert!(workers_proto.is_file());
    assert!(
        fs::read_to_string(&workers_proto)
            .unwrap()
            .starts_with(WORKERS_PROTO_HEADER)
    );
    assert!(
        first
            .join("generated/workers/acyclic-workers-v1.bin")
            .is_file()
    );
    let native_targets: serde_json::Value =
        serde_json::from_slice(&fs::read(first.join("generated/native-targets.json")).unwrap())
            .unwrap();
    assert_eq!(native_targets["schema"], "acyclic.actors.native-targets.v1");
    assert_eq!(native_targets["package"], "acyclic-actors-napi");
    assert_eq!(native_targets["version"], "0.2.0");
    assert_eq!(native_targets["source_revision"], manifest["revision"]);
    assert_eq!(native_targets["source_sha256"], manifest["source_sha256"]);
    assert_eq!(native_targets["targets"], serde_json::json!(NATIVE_TARGETS));
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
    let availability_bytes = fs::read(&availability_path).unwrap();
    fs::write(&availability_path, b"tampered profile availability\n").unwrap();
    assert!(
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    fs::write(&availability_path, availability_bytes).unwrap();
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
    let tracked_guide_path = root.join("docs/unlisted-tracked-guide.md");
    let tracked_guide = fs::read(&tracked_guide_path).unwrap();
    fs::write(&tracked_guide_path, b"tampered tracked guide\n").unwrap();
    assert!(
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    fs::write(&tracked_guide_path, tracked_guide).unwrap();
    fs::write(
        rustdoc.join("actors.json"),
        serde_json::to_vec(&fixture).unwrap(),
    )
    .unwrap();
    let domain_path = root.join("rust/crates/actors/src/domain.rs");
    let domain_bytes = fs::read(&domain_path).unwrap();
    fs::write(&domain_path, "tampered\n").unwrap();
    assert!(
        !run(binary, "drift", &root, Some(&rustdoc), &first, "preview")
            .status
            .success()
    );
    fs::write(domain_path, domain_bytes).unwrap();
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
    let _ = fs::remove_dir_all(foreign_docs);
}

#[test]
#[ignore = "release-only pinned Rustdoc integration; run explicitly during release qualification"]
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
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    fs::create_dir_all(root.join("rust/crates/actors/src")).unwrap();
    fs::create_dir_all(root.join("rust/crates/actors/examples")).unwrap();
    fs::create_dir_all(root.join("rust/crates/workers/src")).unwrap();
    fs::create_dir_all(root.join("rust/crates/sdk-docs/src")).unwrap();
    fs::create_dir_all(root.join("rust/crates/sdk-generation/src")).unwrap();
    fs::create_dir_all(root.join("rust/crates/sdk-generation/tests")).unwrap();
    fs::create_dir_all(root.join("docs")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        fixture_workspace_manifest(&source_root),
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/actors/Cargo.toml"),
        "[package]\nname = \"acyclic-actors\"\nversion.workspace = true\nedition.workspace = true\nrust-version.workspace = true\npublish = false\nbuild = \"build.rs\"\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/actors/src/lib.rs"),
        "//! The executable Rust source for the Actors family.\n\ninclude!(concat!(env!(\"OUT_DIR\"), \"/rust/fixture.generated.rs\"));\n\npub static GENERATED_DESCRIPTOR: &[u8] = include_bytes!(concat!(env!(\"OUT_DIR\"), \"/rust/fixture.generated.bin\"));\n\npub fn visible() {}\n",
    )
    .unwrap();
    fs::write(
        root.join("rust/crates/actors/examples/example.rs"),
        "fn main() {}\n",
    )
    .unwrap();
    for (path, contents) in [
        (
            "rust/crates/actors/build.rs",
            "use std::{env, fs, path::PathBuf};\n\nfn main() {\n    let out = PathBuf::from(env::var_os(\"OUT_DIR\").unwrap()).join(\"rust\");\n    fs::create_dir_all(&out).unwrap();\n    fs::write(out.join(\"fixture.generated.rs\"), \"pub fn generated_visible() {}\\n\").unwrap();\n    fs::write(out.join(\"fixture.generated.bin\"), b\"fixture descriptor\\n\").unwrap();\n}\n",
        ),
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
        ("rust/crates/sdk-generation/build.rs", "fn main() {}\n"),
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
    write_fixture_owner_packages(&root);
    fs::write(root.join("rust/crates/workers/README.md"), "Workers\n").unwrap();
    write_fixture_private_packages(&root);
    copy_compiled_generator_sources(&root);
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
    let cache = sandbox
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| sandbox.parent().unwrap())
        .join("sdkgen-fixtures")
        .join(sandbox.file_name().unwrap());
    let result = command(binary, "generate", &root, None, &output, "release")
        .env("CARGO_TARGET_DIR", &cache)
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
    assert!(
        output
            .join("generated/typescript/actors/InvokeActorRequest.ts")
            .is_file()
    );
    assert!(
        output
            .join("generated/typescript/actors/types.ts")
            .is_file()
    );
    assert!(
        output
            .join("generated/acyclic_actors/rust/acyclic.actors.v1.rs")
            .is_file()
    );
    assert!(
        output
            .join("generated/acyclic_actors/acyclic-actors-v1.bin")
            .is_file()
    );
    let generated_source = output.join("generated/acyclic_actors/rust/acyclic.actors.v1.rs");
    let generated_source_bytes = fs::read(&generated_source).unwrap();
    fs::write(&generated_source, b"tampered generated source\n").unwrap();
    let drift = command(binary, "drift", &root, None, &output, "release")
        .env("CARGO_TARGET_DIR", &cache)
        .output()
        .unwrap();
    assert!(!drift.status.success());
    assert_eq!(
        fs::read(&generated_source).unwrap(),
        b"tampered generated source\n"
    );
    fs::write(&generated_source, generated_source_bytes).unwrap();
    let drift = command(binary, "drift", &root, None, &output, "release")
        .env("CARGO_TARGET_DIR", &cache)
        .output()
        .unwrap();
    assert!(
        drift.status.success(),
        "{}",
        String::from_utf8_lossy(&drift.stderr)
    );

    let _ = fs::remove_dir_all(sandbox);
}
