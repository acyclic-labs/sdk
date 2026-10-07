//! Rust-owned package metadata for generated language artifacts.
//!
//! The Cargo package and target identities in this crate are derived from
//! `cargo_metadata`. JSON is an evidence projection; it is never an input
//! authority for package names, crate names, or versions.

use cargo_metadata::{MetadataCommand, TargetKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
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
    #[error("qualification receipt {0} is empty")]
    EmptyReceipt(PathBuf),
    #[error("git revision lookup failed: {0}")]
    Git(String),
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
pub fn source_inventory(root: &Path) -> Result<SourceInventory, Error> {
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
        .filter(|path| !path.is_empty() && is_rust_owned_input(path))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();

    let mut files = Vec::with_capacity(paths.len());
    for relative in paths {
        let path = root.join(&relative);
        let identity = sha256_file(&path)?;
        files.push(SourceFileIdentity {
            path: relative.replace('\\', "/"),
            sha256: identity.sha256,
            bytes: identity.bytes,
        });
    }
    let mut digest = Sha256::new();
    for file in &files {
        digest.update(file.path.as_bytes());
        digest.update([0]);
        digest.update(file.sha256.as_bytes());
        digest.update([0]);
        digest.update(file.bytes.to_string().as_bytes());
        digest.update([b'\n']);
    }
    Ok(SourceInventory {
        files,
        sha256: format!("{:x}", digest.finalize()),
    })
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

fn validate_receipt(
    receipt: &Path,
    status: QualificationStatus,
    marker: &str,
) -> Result<ArtifactIdentity, Error> {
    let identity = sha256_file(receipt)?;
    if identity.bytes == 0 {
        return Err(Error::EmptyReceipt(receipt.to_owned()));
    }
    if status == QualificationStatus::Passed
        && (marker.is_empty()
            || !fs::read(receipt)?
                .windows(marker.len())
                .any(|window| window == marker.as_bytes()))
    {
        return Err(Error::ReceiptEvidenceMissing {
            path: receipt.to_owned(),
            marker: marker.to_owned(),
        });
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
    qualification.receipt = validate_receipt(receipt, qualification.status, receipt_marker)?;
    Ok(LanguagePackageArtifact {
        schema: "acyclic.language-package-artifact/v2".into(),
        language,
        package: cargo_package(manifest, package_name)?,
        source_revision,
        source_inventory,
        generator,
        artifact: sha256_file(artifact)?,
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
        source_inventory(source_root)?,
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
        let actual = source_inventory(source_root)?;
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
        let receipt = root.join("receipt.json");
        fs::write(&receipt, br#"{"status":"PASS","scope":"all-eight-actors"}"#).unwrap();
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
        assert!(matches!(result, Err(Error::ReceiptEvidenceMissing { .. })));
        let _ = fs::remove_dir_all(root);
    }
}
