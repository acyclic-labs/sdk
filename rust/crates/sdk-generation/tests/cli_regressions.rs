use sdk_docs::rustdoc_digest;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

struct Fixture {
    directory: Arc<tempfile::TempDir>,
    root: PathBuf,
    rustdoc: PathBuf,
    output: PathBuf,
}

fn fixture_directory() -> Arc<tempfile::TempDir> {
    Arc::new(
        tempfile::Builder::new()
            .prefix("sdk-generation-cli-")
            .tempdir_in(runtime::temp_dir())
            .unwrap(),
    )
}

fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(digest))
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
    let output = Command::new(runtime::git())
        .current_dir(root)
        // Identical fixture branches must not create extra frontier revisions
        // merely because setup crossed a wall-clock second.
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_git(root: &Path, message: &str) {
    git(
        root,
        &[
            "init",
            "--quiet",
            "--template=",
            "--initial-branch=sdk-fixture",
        ],
    );
    use std::io::Write;
    fs::OpenOptions::new().append(true).open(root.join(".git/config")).unwrap()
        .write_all(b"\n[user]\nname = sdk-generation-test\nemail = sdk-generation-test@example.invalid\n[core]\nautocrlf = false\nexcludesFile =\n[commit]\ngpgSign = false\n[maintenance]\nauto = false\n").unwrap();
    git(root, &["add", "."]);
    git(root, &["commit", "--quiet", "-m", message]);
}

fn copy_fixture(template: &Fixture) -> Fixture {
    let directory = fixture_directory();
    let root = directory.path().join("source");
    replay::copy_tree(&template.root, &root);
    assert_ne!(
        sdk_file_identity::stamp(&template.root.join(".git/config")).unwrap(),
        sdk_file_identity::stamp(&root.join(".git/config")).unwrap(),
        "mutable Git configuration must remain private to each control"
    );
    let output = template
        .output
        .strip_prefix(&template.root)
        .map(|path| root.join(path))
        .unwrap_or_else(|_| directory.path().join("output"));
    let rustdoc = template
        .rustdoc
        .strip_prefix(&template.root)
        .map(|path| root.join(path))
        .unwrap_or_else(|_| output.join(template.rustdoc.strip_prefix(&template.output).unwrap()));
    Fixture {
        directory,
        rustdoc,
        output,
        root,
    }
}

fn fixture(include_plugin: bool) -> Fixture {
    let directory = fixture_directory();
    let root = directory.path().join("source");
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
    init_git(&root, "fixture");

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
        directory,
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
    let fixture = feature_profile_source();
    init_git(&fixture.root, "feature profile fixture");
    fixture
}

fn feature_profile_source() -> Fixture {
    profile_source(
        include_str!("fixtures/feature-profile/rust/crates/demo/Cargo.toml"),
        "src/lib.rs",
        include_str!("fixtures/feature-profile/rust/crates/demo/src/lib.rs"),
    )
}

fn profile_source(manifest: &str, entrypoint: &str, source: &str) -> Fixture {
    let directory = fixture_directory();
    let root = directory.path().join("source");
    fs::create_dir_all(root.join("rust/crates/demo/src")).unwrap();
    fs::create_dir_all(root.join("release")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        include_str!("fixtures/feature-profile/Cargo.toml"),
    )
    .unwrap();
    fs::write(
        root.join("rust-toolchain.toml"),
        include_str!("fixtures/feature-profile/rust-toolchain.toml"),
    )
    .unwrap();
    fs::write(
        root.join("release/cargo-crates.json"),
        include_str!("fixtures/feature-profile/release/cargo-crates.json"),
    )
    .unwrap();
    let package = root.join("rust/crates/demo");
    fs::write(package.join("Cargo.toml"), manifest).unwrap();
    fs::write(package.join(entrypoint), source).unwrap();
    // This dependency-free lockfile is the exact lock input for the fixture;
    // no network or workspace checkout is involved in the producer run.
    fs::write(
        root.join("Cargo.lock"),
        include_str!("fixtures/feature-profile/Cargo.lock"),
    )
    .unwrap();
    let output = directory.path().join("output");
    Fixture {
        directory,
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
    if let (Ok(head), Ok(revision)) = (
        fs::read(root.join(".git/HEAD")),
        fs::read(root.join(".git/refs/heads/sdk-fixture")),
    ) {
        if let Some(revision) = paths::fixed_revision(&head, &revision) {
            return revision.to_owned();
        }
    }
    let output = Command::new(runtime::git())
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

fn configure_cli(command: &mut Command) -> &mut Command {
    runtime::configure(command);
    if !command
        .get_envs()
        .any(|(key, _)| key.as_encoded_bytes().eq_ignore_ascii_case(b"PATH"))
    {
        command.env("PATH", runtime::path());
    }
    command
}

fn invoke(command: &mut Command) -> std::process::Output {
    configure_cli(command);
    let mut stdout = Vec::new();
    let result = commands::execute(|| {
        sdk_docs::with_command_environment(command, || {
            sdk_generation::run(
                command
                    .get_args()
                    .map(|arg| arg.to_str().unwrap().to_owned()),
                &mut stdout,
            )
        })
    });
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    #[cfg(windows)]
    use std::os::windows::process::ExitStatusExt;
    std::process::Output {
        status: std::process::ExitStatus::from_raw(if result.is_ok() {
            0
        } else if cfg!(unix) {
            1 << 8
        } else {
            1
        }),
        stdout,
        stderr: result
            .err()
            .map(|error| format!("sdk-generation: {error}\n").into_bytes())
            .unwrap_or_default(),
    }
}

fn drift_command(fixture: &Fixture) -> Command {
    let mut command = Command::new(runtime::binary("sdk-generation"));
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
    invoke(&mut drift_command(fixture))
}

fn fixture_path(support: &Path) -> std::ffi::OsString {
    std::env::join_paths(
        std::iter::once(support.to_owned()).chain(std::env::split_paths(&runtime::path())),
    )
    .unwrap()
}

fn run_historical_drift(fixture: &Fixture, support: &Path) -> std::process::Output {
    invoke(
        drift_command(fixture)
            .env("PATH", fixture_path(support))
            .env("CARGO_HOME", fixture_cargo_home(fixture)),
    )
}

fn generate_command(
    root: &Path,
    rustdoc: &Path,
    output: &Path,
    version: &str,
    channel: &str,
    skip_scenarios: bool,
) -> Command {
    let mut command = Command::new(runtime::binary("sdk-generation"));
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
    command
}

fn run_generate(
    root: &Path,
    rustdoc: &Path,
    output: &Path,
    version: &str,
    channel: &str,
    skip_scenarios: bool,
) -> std::process::Output {
    invoke(&mut generate_command(
        root,
        rustdoc,
        output,
        version,
        channel,
        skip_scenarios,
    ))
}

fn clone_source(source: &Path, destination: &Path) {
    let clone = Command::new(runtime::git())
        .args(["clone", "--quiet"])
        .arg(source)
        .arg(destination)
        .output()
        .unwrap();
    assert!(
        clone.status.success(),
        "git clone failed: {}",
        output_message(&clone)
    );
}

fn generated_span_fixture_pair() -> (Fixture, Fixture) {
    fn place_span(root: &Path, receipt: &mut Value) {
        let generated = root.join("target/generated/wire.rs");
        fs::create_dir_all(generated.parent().unwrap()).unwrap();
        fs::write(&generated, b"pub struct Wire;\n").unwrap();
        receipt["index"]["0"]["span"] = json!({
            "filename": generated.canonicalize().unwrap().to_string_lossy(),
            "begin": [1, 0],
            "end": [1, 16]
        });
    }
    let first = fixture(false);

    let receipt_path = first.rustdoc.join("demo_docs.json");
    let mut receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    place_span(&first.root, &mut receipt);
    fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();

    let directory = fixture_directory();
    let second_root = directory.path().join("source");
    clone_source(&first.root, &second_root);
    fs::create_dir_all(second_root.join("rustdoc")).unwrap();
    fs::copy(
        first.rustdoc.join("demo_docs.source.sha256"),
        second_root.join("rustdoc/demo_docs.source.sha256"),
    )
    .unwrap();
    place_span(&second_root, &mut receipt);
    fs::write(
        second_root.join("rustdoc/demo_docs.json"),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();

    let second = Fixture {
        directory,
        root: second_root.clone(),
        rustdoc: second_root.join("rustdoc"),
        output: second_root.join("output"),
    };
    (first, second)
}

#[test]
fn relocated_checkouts_produce_identical_artifacts_and_source_digest() {
    runtime::time_control();
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
}

fn fixture_cargo_home(fixture: &Fixture) -> PathBuf {
    fixture.directory.path().join("cargo-home")
}

fn run_generate_execute_profiles(fixture: &Fixture) -> std::process::Output {
    invoke(
        Command::new(runtime::binary("sdk-generation"))
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
            .env("CARGO_HOME", fixture_cargo_home(fixture)),
    )
}

fn run_drift_execute_profiles(fixture: &Fixture) -> std::process::Output {
    invoke(
        Command::new(runtime::binary("sdk-generation"))
            .args([
                "drift",
                "--root",
                fixture.root.to_str().unwrap(),
                "--output",
                fixture.output.to_str().unwrap(),
                "--rustdoc-dir",
                fixture.rustdoc.to_str().unwrap(),
            ])
            .env("CARGO_HOME", fixture_cargo_home(fixture)),
    )
}

fn output_message(result: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    )
}

fn assert_executed_profiles(output: &Path, rustdoc: &Path, expected: &[(&str, usize)]) {
    let rows: Value = serde_json::from_slice(
        &fs::read(output.join("sdk-docs-rustdoc-profiles.v1.json")).unwrap(),
    )
    .unwrap();
    let rows = rows.as_array().unwrap();
    let mut counts = BTreeMap::new();
    let mut identities = std::collections::BTreeSet::new();
    for row in rows {
        assert_eq!(row["rustdocProfilesCovered"], true);
        *counts.entry(row["package"].as_str().unwrap()).or_insert(0) += 1;
        assert!(
            identities.insert(row["profile"].as_str().unwrap()),
            "duplicate execution receipt"
        );
        let relative = row["rustdocFile"]
            .as_str()
            .unwrap()
            .strip_prefix("rustdoc/")
            .unwrap();
        assert!(
            rustdoc.join(relative).is_file(),
            "every execution must retain its Rustdoc output"
        );
        assert!(rustdoc
            .join(relative)
            .with_extension("source.sha256")
            .is_file());
    }
    assert_eq!(
        rows.len(),
        expected.iter().map(|(_, count)| count).sum::<usize>()
    );
    assert_eq!(counts, expected.iter().copied().collect::<BTreeMap<_, _>>());
}

fn historical_source_fixture(mutation: Option<&str>) -> (Fixture, PathBuf) {
    let fixture = feature_profile_source();
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
    let support = fixture.output.join("test-support");
    fs::create_dir_all(support.join("registry")).unwrap();
    (fixture, support)
}

fn historical_fixture(mutation: Option<&str>) -> (Fixture, PathBuf) {
    let (fixture, support) = historical_source_fixture(mutation);
    init_git(&fixture.root, "historical docs fixture");
    let package = fixture.root.join("rust/crates/demo");
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
    packaging::write_publisher_vcs(&fixture.root, &archive_source, false);
    let archive = archives.join("demo-1.0.0.crate");
    archives::pack(&archive_source, &archive);
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
    let records = responses
        .iter()
        .map(|(suffix, response)| format!("{suffix}\n{response}\n"))
        .collect::<String>();
    fs::write(support.join("registry-responses.txt"), records).unwrap();
    let destination = support.join(if cfg!(windows) { "curl.exe" } else { "curl" });
    if !destination.exists() {
        let source = runtime::binary("registry-transport-fixture");
        fs::hard_link(&source, &destination)
            .or_else(|_| fs::copy(&source, &destination).map(|_| ()))
            .unwrap();
    }
}

fn run_historical_generate(fixture: &Fixture, support: &Path) -> std::process::Output {
    run_historical_generate_mode(fixture, support, None)
}

fn run_historical_generate_mode(
    fixture: &Fixture,
    support: &Path,
    mode: Option<&str>,
) -> std::process::Output {
    run_historical_generation(fixture, support, mode, None, &[])
}

enum HistoricalAuthority<'a> {
    Lock(&'a Path),
    CargoHome(&'a Path),
    WorkingDirectory(&'a Path),
}

impl HistoricalAuthority<'_> {
    fn configure(&self, command: &mut Command) {
        match self {
            Self::Lock(lock) => command.env("CARGO_RESOLVER_LOCKFILE_PATH", lock),
            Self::CargoHome(home) => command.env("CARGO_HOME", home),
            Self::WorkingDirectory(directory) => command.current_dir(directory),
        };
    }
}

fn run_historical_generation(
    fixture: &Fixture,
    support: &Path,
    mode: Option<&str>,
    authority: Option<HistoricalAuthority<'_>>,
    expected_owners: &[&str],
) -> std::process::Output {
    let path = fixture_path(support);
    let mut command = Command::new(runtime::binary("sdk-generation"));
    command
        .env("PATH", path)
        .env("CARGO_HOME", fixture_cargo_home(fixture))
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
    if let Some(authority) = authority {
        authority.configure(&mut command);
    }
    let result = invoke(&mut command);
    let version = fixture.output.join("releases/1.0.0");
    if result.status.success() || version.join("sdk-docs-rustdoc-profiles.v1.json").is_file() {
        let releases: Value = serde_json::from_slice(
            &fs::read(version.join("sdk-docs-registry-releases.v1.json")).unwrap(),
        )
        .unwrap();
        let expected = releases
            .as_array()
            .unwrap()
            .iter()
            .map(|release| (release["crate"].as_str().unwrap(), 4))
            .collect::<Vec<_>>();
        if !expected_owners.is_empty() {
            assert_eq!(
                expected
                    .iter()
                    .map(|&(owner, _)| owner)
                    .collect::<BTreeSet<_>>(),
                expected_owners.iter().copied().collect::<BTreeSet<_>>(),
            );
        }
        assert_executed_profiles(&version, &version.join(".rustdoc"), &expected);
    }
    result
}

fn registry_archive_fixture(
    dirty: bool,
    published_lock: bool,
    mutation: Option<&str>,
    publisher_vcs: bool,
) -> (Fixture, PathBuf) {
    registry_archive_fixture_configured(
        dirty,
        published_lock,
        mutation,
        publisher_vcs,
        "demo",
        None,
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
    prepared: Option<(Fixture, PathBuf)>,
) -> (Fixture, PathBuf) {
    let needs_commit = prepared.is_some() || package_name != "demo";
    let (fixture, support) = prepared.unwrap_or_else(packaging::source_fixture);
    let package = fixture.root.join("rust/crates/demo");
    if needs_commit {
        let manifest = fs::read_to_string(package.join("Cargo.toml"))
            .unwrap()
            .replace("name = \"demo\"", &format!("name = {package_name:?}"));
        fs::write(package.join("Cargo.toml"), manifest).unwrap();
        packaging::include_sources(&package);
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
    }
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
    let source = packaging::materialize(&package, &support, package_name);
    if publisher_vcs {
        packaging::write_publisher_vcs(&fixture.root, &source, dirty);
    }
    if !published_lock {
        fs::remove_file(source.join("Cargo.lock")).unwrap();
    }
    let archive = support.join(format!("registry/{package_name}-1.0.0.crate"));
    archives::pack(&source, &archive);
    write_registry_transport(&support);
    (fixture, support)
}

#[path = "cli_regressions/current.rs"]
mod current;
#[path = "cli_regressions/registry.rs"]
mod registry;

#[path = "cli_regressions/replay.rs"]
mod replay;

#[path = "cli_regressions/archives.rs"]
mod archives;

#[path = "cli_regressions/cache.rs"]
mod cache;

#[path = "cli_regressions/paths.rs"]
mod paths;

#[path = "cli_regressions/package_manifest.rs"]
mod package_manifest;
#[path = "cli_regressions/packaging.rs"]
mod packaging;

#[path = "cli_regressions/runtime.rs"]
mod runtime;

#[path = "cli_regressions/commands.rs"]
mod commands;
