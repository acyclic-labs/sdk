//! Historical planning uses released archives and Cargo-owned sources, not legacy docs JSON.
use cargo_metadata::{Metadata, TargetKind};
use sdk_docs::historical::ReleasedPackage;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path};
use std::process::Command;

/// The current SDK registry remains the default qualification scope.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Scope {
    #[default]
    CurrentSdk,
    HistoricalRelease {
        released_packages: Vec<ReleasedPackage>,
    },
}

/// The fields read from the authoritative crates.io version endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryVersion {
    #[serde(rename = "crate")]
    pub package: String,
    pub num: String,
    pub checksum: String,
    pub yanked: bool,
}

/// Fetch an actual registry version, retaining yanked historical releases.
pub fn registry_version(package: &str, version: &str) -> Result<RegistryVersion, String> {
    validate_name(package)?;
    validate_version(version)?;
    let url = format!("https://crates.io/api/v1/crates/{package}/{version}");
    let bytes = command_output(Command::new("curl").args([
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--max-time",
        "60",
        &url,
    ]))?;
    #[derive(Deserialize)]
    struct Response {
        version: RegistryVersion,
    }
    let response: Response = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if response.version.package != package || response.version.num != version {
        return Err("registry response identity differs from requested release".into());
    }
    Ok(response.version)
}

/// Verify archive bytes, VCS provenance and exact historical Cargo identity.
pub fn verify_archive(
    metadata: &Metadata,
    root: &Path,
    revision: &str,
    release: &RegistryVersion,
    archive: &Path,
) -> Result<ReleasedPackage, String> {
    validate_name(&release.package)?;
    validate_version(&release.num)?;
    if release.checksum.len() != 64
        || !release
            .checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("invalid registry archive checksum".into());
    }
    let actual = super::sha256_file(archive).map_err(|e| e.to_string())?;
    if actual != format!("sha256:{}", release.checksum) {
        return Err("released registry archive checksum differs".into());
    }
    let entry = format!("{}-{}/.cargo_vcs_info.json", release.package, release.num);
    let bytes = command_output(Command::new("tar").arg("-xOf").arg(archive).arg(entry))?;
    #[derive(Deserialize)]
    struct Git {
        sha1: String,
        #[serde(default)]
        dirty: bool,
    }
    #[derive(Deserialize)]
    struct Vcs {
        git: Git,
        path_in_vcs: String,
    }
    let vcs: Vcs = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if vcs.git.dirty {
        return Err(
            "released archive records dirty source; clean Git provenance is not valid".into(),
        );
    }
    if vcs.git.sha1 != revision
        || revision.len() != 40
        || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("released archive VCS revision differs from historical source".into());
    }
    let path = Path::new(&vcs.path_in_vcs);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err("released archive VCS path escapes source root".into());
    }
    let package = metadata
        .packages
        .iter()
        .find(|package| {
            package.name.as_ref() == release.package && package.version.to_string() == release.num
        })
        .ok_or("released archive has no matching historical Cargo package/version")?;
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let source = root.join(path).canonicalize().map_err(|e| e.to_string())?;
    if !source.starts_with(&root)
        || package
            .manifest_path
            .parent()
            .ok_or("package manifest has no parent")?
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?
            != source
    {
        return Err("released archive VCS path differs from Cargo package source".into());
    }
    verify_archive_sources(&root, package, &source, release, archive)?;
    if super::sha256_file(archive).map_err(|e| e.to_string())? != actual {
        return Err("released archive changed during verification".into());
    }
    Ok(ReleasedPackage {
        package: release.package.clone(),
        version: release.num.clone(),
        yanked: release.yanked,
        registry_checksum: release.checksum.clone(),
        source_revision: revision.into(),
        path_in_vcs: vcs.path_in_vcs,
    })
}

// Read entries without extracting or rewriting historical sources. Every retained
// native Rust/Markdown byte must agree with the claimed clean checkout.
fn verify_archive_sources(
    root: &Path,
    package: &cargo_metadata::Package,
    source: &Path,
    release: &RegistryVersion,
    archive: &Path,
) -> Result<(), String> {
    let prefix = format!("{}-{}/", release.package, release.num);
    let listing = String::from_utf8(command_output(Command::new("tar").arg("-tf").arg(archive))?)
        .map_err(|e| e.to_string())?;
    let mut retained = BTreeSet::new();
    let mut has_manifest = false;
    for entry in listing.lines() {
        let relative = entry
            .strip_prefix(&prefix)
            .ok_or("released archive entry is outside package prefix")?;
        let path = Path::new(relative);
        if path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
        {
            return Err("released archive entry escapes package source".into());
        }
        if entry.ends_with('/') {
            continue;
        }
        let original_manifest = relative == "Cargo.toml.orig";
        if !original_manifest
            && !matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("rs" | "md")
            )
        {
            continue;
        }
        let relative = if original_manifest {
            "Cargo.toml"
        } else {
            relative
        };
        if !retained.insert(relative.to_owned()) {
            return Err("released archive repeats an authoritative source entry".into());
        }
        let checkout = source.join(relative).canonicalize().map_err(|e| {
            format!("released archive source is absent from checkout: {relative}: {e}")
        })?;
        if !checkout.starts_with(source) {
            return Err("released archive source escapes checkout package".into());
        }
        let released = command_output(Command::new("tar").arg("-xOf").arg(archive).arg(entry))?;
        if fs::read(checkout).map_err(|e| e.to_string())? != released {
            return Err(format!(
                "released archive source bytes differ from checkout: {relative}"
            ));
        }
        has_manifest |= original_manifest;
    }
    if !has_manifest {
        return Err("released archive has no original Cargo manifest".into());
    }
    // Cargo's explicit sources and the maintained docs source closure must be
    // present too: matching a subset of the archive cannot prove a checkout.
    for target in &package.targets {
        let target_source = target
            .src_path
            .as_std_path()
            .canonicalize()
            .map_err(|e| e.to_string())?;
        let relative = target_source
            .strip_prefix(source)
            .map_err(|_| "historical Cargo target escapes package")?;
        if !retained.contains(&super::path_string(relative)) {
            return Err(format!(
                "released archive is missing Cargo Rust source: {}",
                relative.display()
            ));
        }
    }
    let mut source_files = Vec::new();
    super::collect_source_files(root, source, archive, &mut source_files)
        .map_err(|e| e.to_string())?;
    for file in source_files {
        if matches!(
            file.extension().and_then(|ext| ext.to_str()),
            Some("rs" | "md")
        ) {
            let relative =
                super::path_string(file.strip_prefix(source).map_err(|e| e.to_string())?);
            if !retained.contains(&relative) {
                return Err(format!(
                    "released archive is missing native docs source: {relative}"
                ));
            }
        }
    }
    Ok(())
}

/// A source-owned example and its exact Cargo invocation inputs.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Example {
    pub package: String,
    pub version: String,
    pub target: String,
    pub source_path: String,
    pub required_features: Vec<String>,
    pub source_sha256: String,
}

pub fn examples(
    metadata: &Metadata,
    root: &Path,
    owners: &BTreeSet<String>,
) -> Result<Vec<Example>, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    if owners.iter().any(|owner| {
        !metadata
            .packages
            .iter()
            .any(|package| package.name.as_ref() == owner)
    }) {
        return Err("historical example owner is absent from Cargo metadata".into());
    }
    let mut examples = Vec::new();
    for package in metadata
        .packages
        .iter()
        .filter(|package| owners.contains(package.name.as_ref()))
    {
        for target in package
            .targets
            .iter()
            .filter(|target| target.kind.contains(&TargetKind::Example))
        {
            let path = target
                .src_path
                .as_std_path()
                .canonicalize()
                .map_err(|e| e.to_string())?;
            let relative = path
                .strip_prefix(&root)
                .map_err(|_| "historical example source escapes source root")?;
            examples.push(Example {
                package: package.name.to_string(),
                version: package.version.to_string(),
                target: target.name.clone(),
                source_path: super::path_string(relative),
                required_features: target.required_features.clone(),
                source_sha256: super::sha256_file(&path).map_err(|e| e.to_string())?,
            });
        }
    }
    examples.sort_by(|a, b| (&a.package, &a.target).cmp(&(&b.package, &b.target)));
    Ok(examples)
}

/// Run only the actual historical targets; no modern scenario sources are copied.
pub fn execute_examples(
    root: &Path,
    cargo: &Path,
    target_dir: &Path,
    examples: &[Example],
) -> Result<Vec<ExampleExecution>, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let mut results = Vec::new();
    for example in examples {
        let source = root
            .join(&example.source_path)
            .canonicalize()
            .map_err(|e| e.to_string())?;
        if !source.starts_with(&root) {
            return Err("historical example source escapes source root".into());
        }
        if super::sha256_file(&root.join(&example.source_path)).map_err(|e| e.to_string())?
            != example.source_sha256
        {
            return Err("historical example source changed before execution".into());
        }
        let mut command = Command::new(cargo);
        command
            .current_dir(&root)
            .args(["run", "--locked", "--quiet", "--manifest-path"])
            .arg(root.join("Cargo.toml"))
            .args(["--package", &example.package, "--example", &example.target])
            .arg("--target-dir")
            .arg(target_dir);
        if !example.required_features.is_empty() {
            command
                .arg("--features")
                .arg(example.required_features.join(","));
        }
        if let Some(directory) = cargo.parent() {
            command.env(
                "RUSTC",
                directory.join(if cfg!(windows) { "rustc.exe" } else { "rustc" }),
            );
        }
        command
            .env_remove("RUSTC_WRAPPER")
            .env_remove("RUSTC_WORKSPACE_WRAPPER");
        let output = command.output().map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "historical example {}::{} failed: {}",
                example.package,
                example.target,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        if super::sha256_file(&root.join(&example.source_path)).map_err(|e| e.to_string())?
            != example.source_sha256
        {
            return Err("historical example source changed during execution".into());
        }
        results.push(ExampleExecution {
            example: example.clone(),
            stdout_sha256: super::sha256_bytes(&output.stdout),
            stderr_sha256: super::sha256_bytes(&output.stderr),
        });
    }
    Ok(results)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExampleExecution {
    pub example: Example,
    pub stdout_sha256: String,
    pub stderr_sha256: String,
}

fn validate_version(version: &str) -> Result<(), String> {
    if version.is_empty()
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
    {
        return Err("invalid registry version".into());
    }
    Ok(())
}
fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("invalid registry package name".into());
    }
    Ok(())
}
fn command_output(command: &mut Command) -> Result<Vec<u8>, String> {
    let output = command.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    Ok(output.stdout)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "sdk-historical-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(root.join("src")).unwrap();
            fs::create_dir_all(root.join("examples")).unwrap();
            fs::write(root.join("Cargo.toml"), "[package]\nname='historical-fixture'\nversion='0.1.0'\nedition='2021'\n[workspace]\n[features]\nold-feature=[]\n[[example]]\nname='old-example'\nrequired-features=['old-feature']\n").unwrap();
            fs::write(
                root.join("src/lib.rs"),
                "//! Historical source.\npub fn value() -> u8 { 7 }\n",
            )
            .unwrap();
            fs::write(
                root.join("examples/old-example.rs"),
                "fn main() { println!(\"{}\", historical_fixture::value()); }\n",
            )
            .unwrap();
            fs::write(root.join("Cargo.lock"), "# This file is automatically @generated by Cargo.\nversion = 4\n[[package]]\nname = \"historical-fixture\"\nversion = \"0.1.0\"\n").unwrap();
            Self(root)
        }
        fn metadata(&self) -> Metadata {
            sdk_docs::rustdoc_profiles::load_metadata_with_cargo(
                self.0.join("Cargo.toml"),
                Some(&cargo()),
            )
            .unwrap()
        }
        fn archive(&self, revision: &str, vcs_path: &str) -> (RegistryVersion, PathBuf) {
            self.archive_with_dirty(revision, vcs_path, false)
        }
        fn archive_with_dirty(
            &self,
            revision: &str,
            vcs_path: &str,
            dirty: bool,
        ) -> (RegistryVersion, PathBuf) {
            let entry = self.0.join("historical-fixture-0.1.0");
            fs::create_dir_all(&entry).unwrap();
            fs::write(
                entry.join(".cargo_vcs_info.json"),
                serde_json::to_vec(
                    &serde_json::json!({"git":{"sha1": revision,"dirty":dirty}, "path_in_vcs": vcs_path}),
                )
                .unwrap(),
            )
            .unwrap();
            fs::create_dir_all(entry.join("src")).unwrap();
            fs::create_dir_all(entry.join("examples")).unwrap();
            fs::copy(self.0.join("src/lib.rs"), entry.join("src/lib.rs")).unwrap();
            fs::copy(
                self.0.join("examples/old-example.rs"),
                entry.join("examples/old-example.rs"),
            )
            .unwrap();
            fs::copy(self.0.join("Cargo.toml"), entry.join("Cargo.toml.orig")).unwrap();
            let archive = self.0.join("release.crate");
            command_output(
                Command::new("tar")
                    .arg("-cf")
                    .arg(&archive)
                    .arg("-C")
                    .arg(&self.0)
                    .arg("historical-fixture-0.1.0"),
            )
            .unwrap();
            let checksum = super::super::sha256_file(&archive)
                .unwrap()
                .trim_start_matches("sha256:")
                .to_owned();
            (
                RegistryVersion {
                    package: "historical-fixture".into(),
                    num: "0.1.0".into(),
                    checksum,
                    yanked: true,
                },
                archive,
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn cargo() -> PathBuf {
        std::env::var_os("CARGO")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("cargo"))
    }
    #[test]
    fn archive_requires_checksum_vcs_and_exact_cargo_identity() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata();
        let revision = "a".repeat(40);
        let (release, archive) = fixture.archive(&revision, "");
        let accepted =
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive).unwrap();
        assert!(accepted.yanked);
        fs::write(
            fixture.0.join("src/lib.rs"),
            "//! Divergent checkout Rust bytes.\n",
        )
        .unwrap();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("source bytes differ")
        );
        fs::copy(
            fixture.0.join("historical-fixture-0.1.0/src/lib.rs"),
            fixture.0.join("src/lib.rs"),
        )
        .unwrap();
        let (dirty, dirty_archive) = fixture.archive_with_dirty(&revision, "", true);
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &dirty, &dirty_archive)
                .unwrap_err()
                .contains("dirty source")
        );
        let (release, archive) = fixture.archive(&revision, "");
        let mut wrong = release.clone();
        wrong.checksum = "b".repeat(64);
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &wrong, &archive)
                .unwrap_err()
                .contains("checksum differs")
        );
        assert!(
            verify_archive(&metadata, &fixture.0, &"c".repeat(40), &release, &archive)
                .unwrap_err()
                .contains("revision differs")
        );
        let (escaping, archive) = fixture.archive(&revision, "../outside");
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &escaping, &archive)
                .unwrap_err()
                .contains("escapes")
        );
        let (release, archive) = fixture.archive(&revision, "examples");
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("differs from Cargo")
        );
        let mut metadata = metadata;
        metadata.packages[0].version = "0.1.1".parse().unwrap();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("matching historical Cargo")
        );
        let mut invalid = release;
        invalid.num = "../0.1.0".into();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &invalid, &archive)
                .unwrap_err()
                .contains("invalid registry version")
        );
    }
    #[test]
    fn archive_rejects_missing_target_source_and_changed_original_manifest() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata();
        let revision = "a".repeat(40);
        let (mut release, archive) = fixture.archive(&revision, "");
        fs::remove_file(fixture.0.join("historical-fixture-0.1.0/src/lib.rs")).unwrap();
        command_output(
            Command::new("tar")
                .arg("-cf")
                .arg(&archive)
                .arg("-C")
                .arg(&fixture.0)
                .arg("historical-fixture-0.1.0"),
        )
        .unwrap();
        release.checksum = super::super::sha256_file(&archive)
            .unwrap()
            .trim_start_matches("sha256:")
            .into();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("missing Cargo Rust source")
        );
        let (release, archive) = fixture.archive(&revision, "");
        fs::write(fixture.0.join("Cargo.toml"), "different manifest").unwrap();
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("source bytes differ")
        );
    }
    #[test]
    fn archive_rejects_unarchived_module_in_maintained_native_source_closure() {
        let fixture = Fixture::new();
        let package = fixture.0.join("rust/crates/historical-fixture");
        fs::create_dir_all(package.join("src")).unwrap();
        fs::create_dir_all(package.join("examples")).unwrap();
        fs::copy(fixture.0.join("Cargo.toml"), package.join("Cargo.toml")).unwrap();
        fs::copy(fixture.0.join("src/lib.rs"), package.join("src/lib.rs")).unwrap();
        fs::copy(
            fixture.0.join("examples/old-example.rs"),
            package.join("examples/old-example.rs"),
        )
        .unwrap();
        fs::write(
            package.join("src/unarchived.rs"),
            "//! Unarchived native source.\n",
        )
        .unwrap();
        let metadata = sdk_docs::rustdoc_profiles::load_metadata_with_cargo(
            package.join("Cargo.toml"),
            Some(&cargo()),
        )
        .unwrap();
        let revision = "a".repeat(40);
        let (release, archive) = fixture.archive(&revision, "rust/crates/historical-fixture");
        assert!(
            verify_archive(&metadata, &fixture.0, &revision, &release, &archive)
                .unwrap_err()
                .contains("missing native docs source: src/unarchived.rs")
        );
    }
    #[test]
    fn actual_cargo_examples_keep_required_features_execute_and_reject_drift_failure() {
        let fixture = Fixture::new();
        let metadata = fixture.metadata();
        let owners = BTreeSet::from(["historical-fixture".into()]);
        let discovered = examples(&metadata, &fixture.0, &owners).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].target, "old-example");
        assert_eq!(discovered[0].required_features, vec!["old-feature"]);
        let execution =
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &discovered).unwrap();
        assert_eq!(
            execution[0].stdout_sha256,
            super::super::sha256_bytes(b"7\n")
        );
        fs::write(
            fixture.0.join("examples/old-example.rs"),
            "fn main() { panic!(\"failure\"); }\n",
        )
        .unwrap();
        assert!(
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &discovered)
                .unwrap_err()
                .contains("changed before execution")
        );
        let failing = examples(&metadata, &fixture.0, &owners).unwrap();
        assert!(
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &failing)
                .unwrap_err()
                .contains("failed")
        );
        fs::write(fixture.0.join("examples/old-example.rs"), r#"fn main() { std::fs::write("examples/old-example.rs", "fn main() {}\n").unwrap(); }"#).unwrap();
        let mutating = examples(&metadata, &fixture.0, &owners).unwrap();
        assert!(
            execute_examples(&fixture.0, &cargo(), &fixture.0.join("target"), &mutating)
                .unwrap_err()
                .contains("changed during execution")
        );
        assert!(examples(
            &metadata,
            &fixture.0,
            &BTreeSet::from(["modern-absent-owner".into()])
        )
        .unwrap_err()
        .contains("absent"));
    }
    #[test]
    fn cargo_binary_target_produces_native_receipt_without_public_api_admission() {
        use sdk_docs::rustdoc_profiles::{
            execute_target_profile_with_cargo, observe_rustdoc, ProfileSpec, RustdocTarget,
        };
        let fixture = Fixture::new();
        fs::write(
            fixture.0.join("src/main.rs"),
            "//! Source-owned historical command instructions.\nfn main() {}\n",
        )
        .unwrap();
        let metadata = fixture.metadata();
        let rustc =
            cargo()
                .parent()
                .unwrap()
                .join(if cfg!(windows) { "rustc.exe" } else { "rustc" });
        let output = command_output(Command::new(rustc).arg("-vV")).unwrap();
        let info = String::from_utf8(output).unwrap();
        let host = info
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .unwrap()
            .to_owned();
        let profile = ProfileSpec {
            package: "historical-fixture".into(),
            target: host.clone(),
            default_features: true,
            features: BTreeSet::new(),
        };
        let output = fixture.0.join("binary.json");
        let target_dir = fixture.0.join("native-rustdoc");
        let absent = execute_target_profile_with_cargo(
            fixture.0.join("Cargo.toml"),
            &metadata,
            &profile,
            &BTreeSet::from([host.clone()]),
            &target_dir,
            &output,
            Some(&cargo()),
            &RustdocTarget::Binary("absent".into()),
        );
        assert!(absent
            .unwrap_err()
            .to_string()
            .contains("absent from Cargo"));
        assert!(!target_dir.exists());
        let observation = execute_target_profile_with_cargo(
            fixture.0.join("Cargo.toml"),
            &metadata,
            &profile,
            &BTreeSet::from([host]),
            &target_dir,
            &output,
            Some(&cargo()),
            &RustdocTarget::Binary("historical-fixture".into()),
        )
        .unwrap();
        assert_eq!(observation.format_version, 60);
        assert_eq!(observation.crate_version.as_deref(), Some("0.1.0"));
        assert!(observation.includes_private);
        assert!(observe_rustdoc(&output).is_err());
        let native: serde_json::Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
        assert_eq!(
            native["index"][native["root"].to_string()]["docs"],
            "Source-owned historical command instructions."
        );
    }
}
