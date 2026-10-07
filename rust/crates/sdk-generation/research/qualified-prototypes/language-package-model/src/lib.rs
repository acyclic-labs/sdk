//! Rust-owned package metadata for generated language artifacts.
//!
//! The Cargo package and target identities in this crate are derived from
//! `cargo_metadata`. JSON is an evidence projection; it is never an input
//! authority for package names, crate names, or versions.

use cargo_metadata::{MetadataCommand, TargetKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Language {
    Rust,
    TypeScript,
    Python,
    Go,
    Jvm,
    Dotnet,
    Swift,
    Cpp,
    Ruby,
    Php,
    Dart,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneratorIdentity {
    pub family: String,
    pub version: String,
    pub source: String,
    pub source_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustPackageIdentity {
    pub package_name: String,
    pub crate_name: String,
    pub version: String,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactIdentity {
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
}

/// A deterministic inventory of the Rust-owned inputs used to produce an
/// artifact. The git revision remains useful for provenance, but it cannot
/// detect an uncommitted edit (or a new untracked Rust source file) by itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceFileIdentity {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceInventory {
    pub files: Vec<SourceFileIdentity>,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum QualificationStatus {
    Passed,
    Blocked,
    Diagnostic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QualificationMetadata {
    pub status: QualificationStatus,
    pub operations: Vec<String>,
    pub checks: Vec<String>,
    /// Hash of the external qualification receipt that justifies the status
    /// and scope. A caller cannot claim passed without a real receipt.
    pub receipt: ArtifactIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LanguagePackageArtifact {
    pub schema: String,
    pub language: Language,
    pub package: RustPackageIdentity,
    pub source_revision: String,
    pub source_inventory: SourceInventory,
    pub generator: GeneratorIdentity,
    pub artifact: ArtifactIdentity,
    pub qualification: QualificationMetadata,
}

/// The existing `sdk-generation` manifest already owns this exact wire shape
/// for source file hashes. Keep the prototype's evidence model as an adapter
/// over that vector instead of introducing another source-manifest format.
#[derive(Debug, Deserialize)]
struct ExistingGenerationManifest {
    schema: String,
    source: Vec<SourceFileIdentity>,
    source_sha256: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cargo metadata failed: {0}")]
    Cargo(#[from] cargo_metadata::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("package `{0}` was not found in Cargo metadata")]
    MissingPackage(String),
    #[error("package `{package}` has no library or sole binary target")]
    MissingTarget { package: String },
    #[error("source revision is empty")]
    EmptyRevision,
    #[error("source revision mismatch: expected {expected}, record has {actual}")]
    SourceRevisionMismatch { expected: String, actual: String },
    #[error("Rust source inventory mismatch: expected {expected}, got {actual}")]
    SourceInventoryMismatch { expected: String, actual: String },
    #[error(
        "artifact identity mismatch for {path}: expected sha256:{expected}, got sha256:{actual}"
    )]
    ArtifactMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },
    #[error("artifact byte length mismatch for {path}: expected {expected}, got {actual}")]
    ArtifactLengthMismatch {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },
    #[error(
        "qualification receipt {path} does not contain the required evidence marker {marker:?}"
    )]
    ReceiptEvidenceMissing { path: PathBuf, marker: String },
    #[error(
        "qualification receipt {path} does not bind the requested source, generator, artifact, or language"
    )]
    ReceiptBindingMismatch { path: PathBuf },
    #[error("qualification receipt {path} is not a typed JSON receipt")]
    ReceiptMalformed { path: PathBuf },
    #[error("qualification receipt {0} is empty")]
    EmptyReceipt(PathBuf),
    #[error("git revision lookup failed: {0}")]
    Git(String),
    #[error("sdk-generation manifest {0} is malformed or uses an unsupported schema")]
    GenerationManifestMalformed(PathBuf),
    #[error(
        "sdk-generation manifest {path} source digest mismatch: expected {expected}, got {actual}"
    )]
    GenerationManifestDigestMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },
}

pub fn source_revision(root: &Path) -> Result<String, Error> {
    let output = Command::new("git")
        .args(["-C", root.to_str().unwrap_or_default(), "rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err(Error::Git(
            String::from_utf8_lossy(&output.stderr).trim().into(),
        ));
    }
    let revision = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if revision.is_empty() {
        return Err(Error::EmptyRevision);
    }
    Ok(revision)
}

fn is_rust_owned_input(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    let lower = normalized.to_ascii_lowercase();
    if normalized == "Cargo.toml"
        || normalized == "Cargo.lock"
        || lower.starts_with("rust-toolchain")
    {
        return true;
    }
    if normalized.starts_with("rust/") {
        return true;
    }
    matches!(
        Path::new(&normalized)
            .extension()
            .and_then(|ext| ext.to_str()),
        Some("rs" | "udl" | "proto")
    )
}

/// Hash all tracked and non-ignored untracked Rust-owned inputs in a stable
/// path order. Missing tracked files fail through the normal file read, while
/// new source files are included by git's --others output.
fn inventory_for_paths(root: &Path, package_roots: &[PathBuf]) -> Result<SourceInventory, Error> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z", "-c", "-o", "--exclude-standard", "--"])
        .output()?;
    if !output.status.success() {
        return Err(Error::Git(
            String::from_utf8_lossy(&output.stderr).trim().into(),
        ));
    }
    let mut paths = String::from_utf8_lossy(&output.stdout)
        .split('\0')
        .filter(|path| {
            if path.is_empty() || !is_rust_owned_input(path) {
                return false;
            }
            if *path == "Cargo.toml"
                || *path == "Cargo.lock"
                || path.to_ascii_lowercase().starts_with("rust-toolchain")
                || path.starts_with("proto/")
                || path.starts_with("generated/rust/")
            {
                return true;
            }
            package_roots.iter().any(|package_root| {
                package_root
                    .strip_prefix(root)
                    .ok()
                    .map(|relative| {
                        if relative.as_os_str().is_empty() {
                            true
                        } else {
                            relative
                                .to_str()
                                .map(|relative| {
                                    let relative = relative.replace('\\', "/");
                                    *path == relative || path.starts_with(&(relative + "/"))
                                })
                                .unwrap_or(false)
                        }
                    })
                    .unwrap_or(false)
            })
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();

    let mut files = Vec::with_capacity(paths.len());
    for relative in paths {
        let path = root.join(&relative);
        // A sparse checkout can legitimately omit generated Rust outputs.
        // Keep an explicit missing sentinel in the inventory so the omission
        // is provenance-visible and a later materialization changes the digest.
        let identity = if path.is_file() {
            sha256_file(&path)?
        } else {
            ArtifactIdentity {
                path: path.clone(),
                sha256: "MISSING".into(),
                bytes: 0,
            }
        };
        files.push(SourceFileIdentity {
            path: relative.replace('\\', "/"),
            sha256: identity.sha256,
            bytes: identity.bytes,
        });
    }
    Ok(inventory_from_files(files))
}

fn inventory_from_files(files: Vec<SourceFileIdentity>) -> SourceInventory {
    let mut digest = Sha256::new();
    for file in &files {
        digest.update(file.path.as_bytes());
        digest.update([0]);
        digest.update(file.sha256.as_bytes());
        digest.update([0]);
        digest.update(file.bytes.to_string().as_bytes());
        digest.update([b'\n']);
    }
    SourceInventory {
        files,
        sha256: format!("{:x}", digest.finalize()),
    }
}

/// Adapt the source vector from the existing sdk-generation manifest. The
/// manifest remains the authority for generation output; this only checks its
/// integrity before binding a language artifact to the same source inputs.
pub fn source_inventory_from_generation_manifest(
    manifest: &Path,
) -> Result<SourceInventory, Error> {
    let bytes = fs::read(manifest)?;
    let record: ExistingGenerationManifest = serde_json::from_slice(&bytes)
        .map_err(|_| Error::GenerationManifestMalformed(manifest.to_owned()))?;
    if record.schema != "acyclic.sdk.generation.v1" {
        return Err(Error::GenerationManifestMalformed(manifest.to_owned()));
    }
    let inventory = inventory_from_files(record.source);
    if inventory.sha256 != record.source_sha256 {
        return Err(Error::GenerationManifestDigestMismatch {
            path: manifest.to_owned(),
            expected: record.source_sha256,
            actual: inventory.sha256,
        });
    }
    Ok(inventory)
}

/// Hash the Rust package and local path-dependency closure, plus workspace
/// manifests and generated protocol inputs. This avoids making every package
/// qualification rehash unrelated crates in a monorepo.
pub fn source_inventory_for_manifest(
    root: &Path,
    manifest: &Path,
) -> Result<SourceInventory, Error> {
    let metadata = MetadataCommand::new().manifest_path(manifest).exec()?;
    let package_name = metadata
        .packages
        .iter()
        .find(|package| package.manifest_path.clone().into_std_path_buf() == manifest)
        .map(|package| package.name.clone())
        .ok_or_else(|| Error::MissingPackage(manifest.display().to_string()))?;
    let mut wanted = HashSet::from([package_name]);
    let mut changed = true;
    while changed {
        changed = false;
        for package in &metadata.packages {
            if !wanted.contains(package.name.as_str()) {
                continue;
            }
            for dependency in &package.dependencies {
                if dependency.source.is_none() && wanted.insert(dependency.name.clone()) {
                    changed = true;
                }
            }
        }
    }
    let package_roots = metadata
        .packages
        .iter()
        .filter(|package| wanted.contains(package.name.as_str()))
        .filter_map(|package| {
            package
                .manifest_path
                .clone()
                .into_std_path_buf()
                .parent()
                .map(Path::to_owned)
        })
        .collect::<Vec<_>>();
    inventory_for_paths(root, &package_roots)
}

/// Broad inventory helper for callers that need to attest an entire checkout.
pub fn source_inventory(root: &Path) -> Result<SourceInventory, Error> {
    inventory_for_paths(root, &[])
}

pub fn cargo_package(manifest: &Path, package_name: &str) -> Result<RustPackageIdentity, Error> {
    let metadata = MetadataCommand::new()
        .manifest_path(manifest)
        .no_deps()
        .exec()?;
    let package = metadata
        .packages
        .iter()
        .find(|package| package.name.as_str() == package_name)
        .ok_or_else(|| Error::MissingPackage(package_name.into()))?;
    let target = package
        .targets
        .iter()
        .find(|target| {
            target.kind.iter().any(|kind| {
                matches!(
                    kind,
                    TargetKind::Lib
                        | TargetKind::RLib
                        | TargetKind::CDyLib
                        | TargetKind::DyLib
                        | TargetKind::StaticLib
                        | TargetKind::ProcMacro
                )
            })
        })
        .or_else(|| {
            (package.targets.len() == 1)
                .then(|| package.targets.first())
                .flatten()
        })
        .ok_or_else(|| Error::MissingTarget {
            package: package_name.into(),
        })?;
    Ok(RustPackageIdentity {
        package_name: package.name.to_string(),
        crate_name: target.name.to_string(),
        version: package.version.to_string(),
        manifest_path: package.manifest_path.clone().into_std_path_buf(),
    })
}

pub fn sha256_file(path: &Path) -> Result<ArtifactIdentity, Error> {
    let bytes = fs::read(path)?;
    let digest = Sha256::digest(&bytes);
    Ok(ArtifactIdentity {
        path: path.to_owned(),
        sha256: format!("{digest:x}"),
        bytes: bytes.len() as u64,
    })
}

#[derive(Debug, Deserialize)]
struct ReceiptFile {
    path: PathBuf,
    sha256: String,
    bytes: u64,
}

#[derive(Debug, Deserialize)]
struct ReceiptToolchain {
    uniffi_bindgen: Option<String>,
    uniffi_source_sha256: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReceiptDocument {
    source_revision: Option<String>,
    source_inventory_sha256: Option<String>,
    toolchain: Option<ReceiptToolchain>,
    source: Option<BTreeMap<String, ReceiptFile>>,
    artifacts: Option<BTreeMap<String, ReceiptFile>>,
}

fn receipt_artifact_key(language: Language) -> &'static str {
    match language {
        Language::Python => "wheel",
        Language::Jvm => "kotlin",
        Language::Swift => "swift",
        Language::Ruby => "gem",
        Language::Dotnet => "nuget",
        Language::Go => "go",
        Language::Cpp => "cpp",
        Language::Dart => "dart",
        Language::Php => "php",
        Language::Rust => "rust",
        Language::TypeScript => "typescript",
    }
}

fn validate_receipt(
    receipt: &Path,
    status: QualificationStatus,
    marker: &str,
    source_revision: &str,
    source_inventory: &SourceInventory,
    generator: &GeneratorIdentity,
    language: Language,
    artifact: &ArtifactIdentity,
) -> Result<ArtifactIdentity, Error> {
    let identity = sha256_file(receipt)?;
    let bytes = fs::read(receipt)?;
    if identity.bytes == 0 {
        return Err(Error::EmptyReceipt(receipt.to_owned()));
    }
    if status == QualificationStatus::Passed
        && (marker.is_empty()
            || !bytes
                .windows(marker.len())
                .any(|window| window == marker.as_bytes()))
    {
        return Err(Error::ReceiptEvidenceMissing {
            path: receipt.to_owned(),
            marker: marker.to_owned(),
        });
    }
    if status == QualificationStatus::Passed {
        // This receipt schema binds the UniFFI producer. Other producers need
        // their own typed toolchain evidence before they can claim a pass.
        if generator.family != "uniffi" {
            return Err(Error::ReceiptBindingMismatch {
                path: receipt.to_owned(),
            });
        }
        let document: ReceiptDocument =
            serde_json::from_slice(&bytes).map_err(|_| Error::ReceiptMalformed {
                path: receipt.to_owned(),
            })?;
        if document.source_revision.as_deref() != Some(source_revision)
            || document.source_inventory_sha256.as_deref() != Some(source_inventory.sha256.as_str())
        {
            return Err(Error::ReceiptBindingMismatch {
                path: receipt.to_owned(),
            });
        }
        if generator.family == "uniffi"
            && document
                .toolchain
                .as_ref()
                .and_then(|toolchain| toolchain.uniffi_bindgen.as_deref())
                != Some(generator.version.as_str())
        {
            return Err(Error::ReceiptBindingMismatch {
                path: receipt.to_owned(),
            });
        }
        if generator.family == "uniffi"
            && document
                .toolchain
                .as_ref()
                .and_then(|toolchain| toolchain.uniffi_source_sha256.as_deref())
                != Some(generator.source_sha256.as_str())
        {
            return Err(Error::ReceiptBindingMismatch {
                path: receipt.to_owned(),
            });
        }
        let Some(receipt_artifact) = document
            .artifacts
            .as_ref()
            .and_then(|artifacts| artifacts.get(receipt_artifact_key(language)))
        else {
            return Err(Error::ReceiptBindingMismatch {
                path: receipt.to_owned(),
            });
        };
        if receipt_artifact.path != artifact.path
            || receipt_artifact.sha256.to_ascii_lowercase() != artifact.sha256
            || receipt_artifact.bytes != artifact.bytes
        {
            return Err(Error::ReceiptBindingMismatch {
                path: receipt.to_owned(),
            });
        }
        if let Some(source_files) = document.source.as_ref() {
            for source in source_files.values() {
                let normalized = source.path.to_string_lossy().replace('\\', "/");
                let Some(current) = source_inventory
                    .files
                    .iter()
                    .find(|file| file.path == normalized)
                else {
                    return Err(Error::ReceiptBindingMismatch {
                        path: receipt.to_owned(),
                    });
                };
                if current.sha256 != source.sha256.to_ascii_lowercase()
                    || current.bytes != source.bytes
                {
                    return Err(Error::ReceiptBindingMismatch {
                        path: receipt.to_owned(),
                    });
                }
            }
        }
    }
    Ok(identity)
}

fn build_record_with_identity(
    manifest: &Path,
    package_name: &str,
    language: Language,
    source_revision: String,
    source_inventory: SourceInventory,
    generator: GeneratorIdentity,
    artifact: &Path,
    receipt: &Path,
    receipt_marker: &str,
    mut qualification: QualificationMetadata,
) -> Result<LanguagePackageArtifact, Error> {
    if source_revision.is_empty() {
        return Err(Error::EmptyRevision);
    }
    let artifact_identity = sha256_file(artifact)?;
    qualification.receipt = validate_receipt(
        receipt,
        qualification.status,
        receipt_marker,
        &source_revision,
        &source_inventory,
        &generator,
        language,
        &artifact_identity,
    )?;
    Ok(LanguagePackageArtifact {
        schema: "acyclic.language-package-artifact/v2".into(),
        language,
        package: cargo_package(manifest, package_name)?,
        source_revision,
        source_inventory,
        generator,
        artifact: artifact_identity,
        qualification,
    })
}

/// Build a record while deriving the source revision from the checkout itself.
/// Callers may still use [`build_record`] for isolated fixture tests where a
/// synthetic revision is intentional.
pub fn build_record_from_source(
    source_root: &Path,
    manifest: &Path,
    package_name: &str,
    language: Language,
    generator: GeneratorIdentity,
    artifact: &Path,
    receipt: &Path,
    receipt_marker: &str,
    qualification: QualificationMetadata,
) -> Result<LanguagePackageArtifact, Error> {
    build_record_with_identity(
        manifest,
        package_name,
        language,
        source_revision(source_root)?,
        source_inventory_for_manifest(source_root, manifest)?,
        generator,
        artifact,
        receipt,
        receipt_marker,
        qualification,
    )
}

impl LanguagePackageArtifact {
    pub fn verify_source_revision(&self, expected: &str) -> Result<(), Error> {
        if self.source_revision == expected {
            Ok(())
        } else {
            Err(Error::SourceRevisionMismatch {
                expected: expected.into(),
                actual: self.source_revision.clone(),
            })
        }
    }

    pub fn verify_source(&self, source_root: &Path) -> Result<(), Error> {
        self.verify_source_revision(&source_revision(source_root)?)?;
        let actual = source_inventory_for_manifest(source_root, &self.package.manifest_path)?;
        if actual.sha256 == self.source_inventory.sha256 {
            Ok(())
        } else {
            Err(Error::SourceInventoryMismatch {
                expected: self.source_inventory.sha256.clone(),
                actual: actual.sha256,
            })
        }
    }

    pub fn verify_artifact(&self, artifact: &Path) -> Result<(), Error> {
        let actual = sha256_file(artifact)?;
        if actual.sha256 != self.artifact.sha256 {
            return Err(Error::ArtifactMismatch {
                path: artifact.into(),
                expected: self.artifact.sha256.clone(),
                actual: actual.sha256,
            });
        }
        if actual.bytes != self.artifact.bytes {
            return Err(Error::ArtifactLengthMismatch {
                path: artifact.into(),
                expected: self.artifact.bytes,
                actual: actual.bytes,
            });
        }
        Ok(())
    }

    pub fn verify_qualification_receipt(&self, receipt: &Path) -> Result<(), Error> {
        let actual = sha256_file(receipt)?;
        if actual.sha256 != self.qualification.receipt.sha256 {
            return Err(Error::ArtifactMismatch {
                path: receipt.into(),
                expected: self.qualification.receipt.sha256.clone(),
                actual: actual.sha256,
            });
        }
        if actual.bytes != self.qualification.receipt.bytes {
            return Err(Error::ArtifactLengthMismatch {
                path: receipt.into(),
                expected: self.qualification.receipt.bytes,
                actual: actual.bytes,
            });
        }
        Ok(())
    }

    pub fn evidence_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_root() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("language-package-model-{stamp}"));
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            "[package]\nname = \"acyclic-actors-uniffi\"\nversion = \"0.2.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn probe() {}\n").unwrap();
        root
    }

    fn git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git command failed: {args:?}");
    }

    fn committed_fixture() -> PathBuf {
        let root = fixture_root();
        git(&root, &["init", "-q"]);
        git(
            &root,
            &["config", "user.email", "language-package-model@test"],
        );
        git(&root, &["config", "user.name", "language-package-model"]);
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "fixture"]);
        root
    }

    fn generator() -> GeneratorIdentity {
        GeneratorIdentity {
            family: "uniffi".into(),
            version: "0.31.0".into(),
            source: "mozilla/uniffi-rs".into(),
            source_sha256: "generator-source-sha256".into(),
        }
    }

    fn qualification(receipt: &Path) -> QualificationMetadata {
        QualificationMetadata {
            status: QualificationStatus::Passed,
            operations: vec!["all-eight-actors".into()],
            checks: vec!["install".into(), "cancellation".into()],
            receipt: sha256_file(receipt).unwrap(),
        }
    }

    fn typed_receipt(root: &Path, artifact: &Path, language: &str, version: &str) -> PathBuf {
        let revision = source_revision(root).unwrap();
        let inventory = source_inventory_for_manifest(root, &root.join("Cargo.toml")).unwrap();
        let artifact = sha256_file(artifact).unwrap();
        let key = match language {
            "python" => "wheel",
            "swift" => "swift",
            _ => panic!("unsupported fixture language"),
        };
        let receipt = root.join("receipt.json");
        let artifact_path = artifact.path.to_string_lossy().replace('\\', "\\\\");
        let text = format!(
            r#"{{"source_revision":"{revision}","source_inventory_sha256":"{inventory}","toolchain":{{"uniffi_bindgen":"{version}","uniffi_source_sha256":"generator-source-sha256"}},"artifacts":{{"{key}":{{"path":"{}","sha256":"{}","bytes":{}}}}},"status":"PASS"}}"#,
            artifact_path,
            artifact.sha256,
            artifact.bytes,
            inventory = inventory.sha256,
        );
        fs::write(&receipt, text).unwrap();
        receipt
    }

    #[test]
    fn cargo_metadata_is_authoritative_for_package_and_crate_identity() {
        let root = fixture_root();
        let package = cargo_package(&root.join("Cargo.toml"), "acyclic-actors-uniffi").unwrap();
        assert_eq!(package.package_name, "acyclic-actors-uniffi");
        assert_eq!(package.crate_name, "acyclic_actors_uniffi");
        assert_eq!(package.version, "0.2.0");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn artifact_and_source_identity_reject_drift() {
        let root = committed_fixture();
        let artifact = root.join("artifact.whl");
        fs::write(&artifact, b"qualified artifact\n").unwrap();
        let receipt = typed_receipt(&root, &artifact, "python", "0.31.0");
        let record = build_record_from_source(
            &root,
            &root.join("Cargo.toml"),
            "acyclic-actors-uniffi",
            Language::Python,
            generator(),
            &artifact,
            &receipt,
            "PASS",
            qualification(&receipt),
        )
        .unwrap();
        record.verify_source(&root).unwrap();
        record.verify_artifact(&artifact).unwrap();
        record.verify_qualification_receipt(&receipt).unwrap();
        fs::write(&artifact, b"tampered artifact\n").unwrap();
        assert!(matches!(
            record.verify_artifact(&artifact),
            Err(Error::ArtifactMismatch { .. })
        ));
        fs::write(root.join("src/lib.rs"), "pub fn probe() { 1 }\n").unwrap();
        assert!(matches!(
            record.verify_source(&root),
            Err(Error::SourceInventoryMismatch { .. })
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sdk_generation_manifest_source_vector_is_reused_and_integrity_checked() {
        let root = fixture_root();
        let source = vec![SourceFileIdentity {
            path: "rust/crates/example/src/lib.rs".into(),
            sha256: "abc123".into(),
            bytes: 7,
        }];
        let digest = inventory_from_files(source.clone()).sha256;
        let manifest = root.join("generation-manifest.json");
        fs::write(
            &manifest,
            serde_json::json!({
                "schema": "acyclic.sdk.generation.v1",
                "source": source,
                "source_sha256": digest,
            })
            .to_string(),
        )
        .unwrap();
        let adapted = source_inventory_from_generation_manifest(&manifest).unwrap();
        assert_eq!(adapted.files[0].path, "rust/crates/example/src/lib.rs");

        fs::write(
            &manifest,
            serde_json::json!({
                "schema": "acyclic.sdk.generation.v1",
                "source": adapted.files,
                "source_sha256": "changed-without-source-change",
            })
            .to_string(),
        )
        .unwrap();
        assert!(matches!(
            source_inventory_from_generation_manifest(&manifest),
            Err(Error::GenerationManifestDigestMismatch { .. })
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn passed_status_rejects_forged_receipt() {
        let root = committed_fixture();
        let artifact = root.join("artifact.whl");
        let receipt = root.join("receipt.json");
        fs::write(&artifact, b"qualified artifact\n").unwrap();
        fs::write(&receipt, br#"{"status":"passed","scope":"invented"}"#).unwrap();
        let result = build_record_from_source(
            &root,
            &root.join("Cargo.toml"),
            "acyclic-actors-uniffi",
            Language::Python,
            generator(),
            &artifact,
            &receipt,
            "PASS",
            qualification(&receipt),
        );
        assert!(matches!(
            result,
            Err(Error::ReceiptEvidenceMissing { .. })
                | Err(Error::ReceiptBindingMismatch { .. })
                | Err(Error::ReceiptMalformed { .. })
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn receipt_rejects_changed_generator_artifact_and_language() {
        let root = committed_fixture();
        let artifact = root.join("artifact.whl");
        fs::write(&artifact, b"qualified artifact\n").unwrap();
        let receipt = typed_receipt(&root, &artifact, "python", "0.31.0");

        let mut unsupported_generator = generator();
        unsupported_generator.family = "unrecognized".into();
        let result = build_record_from_source(
            &root,
            &root.join("Cargo.toml"),
            "acyclic-actors-uniffi",
            Language::Python,
            unsupported_generator,
            &artifact,
            &receipt,
            "PASS",
            qualification(&receipt),
        );
        assert!(matches!(result, Err(Error::ReceiptBindingMismatch { .. })));

        let changed_generator = build_record_from_source(
            &root,
            &root.join("Cargo.toml"),
            "acyclic-actors-uniffi",
            Language::Python,
            GeneratorIdentity {
                family: "uniffi".into(),
                version: "0.32.0".into(),
                source: "mozilla/uniffi-rs".into(),
                source_sha256: "generator-source-sha256".into(),
            },
            &artifact,
            &receipt,
            "PASS",
            qualification(&receipt),
        );
        assert!(matches!(
            changed_generator,
            Err(Error::ReceiptBindingMismatch { .. })
        ));

        let changed_generator_source = build_record_from_source(
            &root,
            &root.join("Cargo.toml"),
            "acyclic-actors-uniffi",
            Language::Python,
            GeneratorIdentity {
                family: "uniffi".into(),
                version: "0.31.0".into(),
                source: "mozilla/uniffi-rs".into(),
                source_sha256: "different-generator-source-sha256".into(),
            },
            &artifact,
            &receipt,
            "PASS",
            qualification(&receipt),
        );
        assert!(matches!(
            changed_generator_source,
            Err(Error::ReceiptBindingMismatch { .. })
        ));

        fs::write(&artifact, b"other artifact\n").unwrap();
        let wrong_artifact = build_record_from_source(
            &root,
            &root.join("Cargo.toml"),
            "acyclic-actors-uniffi",
            Language::Python,
            generator(),
            &artifact,
            &receipt,
            "PASS",
            qualification(&receipt),
        );
        assert!(matches!(
            wrong_artifact,
            Err(Error::ReceiptBindingMismatch { .. })
        ));

        fs::write(&artifact, b"qualified artifact\n").unwrap();
        let wrong_language = build_record_from_source(
            &root,
            &root.join("Cargo.toml"),
            "acyclic-actors-uniffi",
            Language::Swift,
            generator(),
            &artifact,
            &receipt,
            "PASS",
            qualification(&receipt),
        );
        assert!(matches!(
            wrong_language,
            Err(Error::ReceiptBindingMismatch { .. })
        ));
        let _ = fs::remove_dir_all(root);
    }
}
