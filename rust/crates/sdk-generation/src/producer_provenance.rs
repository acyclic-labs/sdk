//! Per-file provenance for Rust-owned language producer outputs.
//!
//! A directory digest is useful for a quick comparison, but it does not give
//! consumers an auditable file set.  Producer manifests therefore record each
//! relative path, byte count, and content hash.  Validation compares both the
//! declared set and every file on disk, so a facade copied from another
//! generation cannot hide inside an otherwise current producer directory.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Component, Path, PathBuf};

pub const SCHEMA: &str = "acyclic.sdk.language-producer-manifest.v1";
const MANIFEST_FILE: &str = "producer-manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerFile {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerManifest {
    pub schema: String,
    pub target: String,
    pub source_revision: String,
    pub source_digest: String,
    pub rust_model_digest: String,
    pub files: Vec<ProducerFile>,
    pub artifact_digest: String,
}

/// Write a manifest beside a producer's configured output directory.
///
/// The manifest itself is deliberately excluded from `files`, which avoids a
/// self-referential hash while keeping it in the outer generation artifact set.
pub fn emit(
    target_root: &Path,
    target: &str,
    source_revision: &str,
    source_digest: &str,
    rust_model_digest: &str,
) -> Result<(PathBuf, ProducerManifest), String> {
    let manifest_path = target_root.join(MANIFEST_FILE);
    let files = collect_files(target_root, Some(&manifest_path))?;
    if files.is_empty() {
        return Err(format!(
            "producer {target} emitted no files under {}",
            target_root.display()
        ));
    }
    let manifest = ProducerManifest {
        schema: SCHEMA.to_owned(),
        target: target.to_owned(),
        source_revision: source_revision.to_owned(),
        source_digest: source_digest.to_owned(),
        rust_model_digest: rust_model_digest.to_owned(),
        artifact_digest: artifact_digest(&files),
        files,
    };
    let bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
    fs::write(&manifest_path, [bytes.as_slice(), b"\n"].concat())
        .map_err(|error| format!("write {}: {error}", manifest_path.display()))?;
    Ok((manifest_path, manifest))
}

/// Validate a producer manifest against its output directory and generation
/// source identity.  This rejects modified, missing, and unexpected files.
pub fn verify(
    manifest_path: &Path,
    expected_target: &str,
    expected_source_revision: &str,
    expected_source_digest: &str,
    expected_rust_model_digest: &str,
) -> Result<ProducerManifest, String> {
    let bytes = fs::read(manifest_path)
        .map_err(|error| format!("read {}: {error}", manifest_path.display()))?;
    let manifest: ProducerManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", manifest_path.display()))?;
    if manifest.schema != SCHEMA {
        return Err(format!(
            "producer manifest {} has unsupported schema {}",
            manifest_path.display(),
            manifest.schema
        ));
    }
    if manifest.target != expected_target {
        return Err(format!(
            "producer manifest target differs: expected {expected_target}, got {}",
            manifest.target
        ));
    }
    if manifest.source_revision != expected_source_revision {
        return Err(format!(
            "producer {expected_target} source revision differs: expected {expected_source_revision}, got {}",
            manifest.source_revision
        ));
    }
    if manifest.source_digest != expected_source_digest {
        return Err(format!(
            "producer {expected_target} source digest differs: expected {expected_source_digest}, got {}",
            manifest.source_digest
        ));
    }
    if manifest.rust_model_digest != expected_rust_model_digest {
        return Err(format!(
            "producer {expected_target} Rust model digest differs: expected {expected_rust_model_digest}, got {}",
            manifest.rust_model_digest
        ));
    }
    let target_root = manifest_path
        .parent()
        .ok_or_else(|| "producer manifest has no parent directory".to_owned())?;
    let actual = collect_files(target_root, Some(manifest_path))?;
    if actual != manifest.files {
        return Err(format!(
            "producer {expected_target} file manifest differs from output directory"
        ));
    }
    let actual_digest = artifact_digest(&actual);
    if actual_digest != manifest.artifact_digest {
        return Err(format!(
            "producer {expected_target} artifact digest differs: expected {}, got {actual_digest}",
            manifest.artifact_digest
        ));
    }
    Ok(manifest)
}

/// Validate all successful producer entries recorded in a generated plan.
/// Pending entries have no generated surface to validate and remain pending
/// under the existing language qualification rules.
pub fn verify_plan(
    output: &Path,
    expected_source_revision: &str,
    expected_source_digest: &str,
    expected_rust_model_digest: &str,
) -> Result<(), String> {
    let plan_path = output.join("language-producers/plan.json");
    if !plan_path.is_file() {
        return Ok(());
    }
    let plan: Value = serde_json::from_slice(
        &fs::read(&plan_path).map_err(|error| format!("read {}: {error}", plan_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", plan_path.display()))?;
    let targets = plan
        .get("targets")
        .and_then(Value::as_array)
        .ok_or_else(|| "language producer plan has no targets array".to_owned())?;
    for entry in targets {
        if entry
            .pointer("/execution/status")
            .and_then(Value::as_str)
            != Some("passed")
        {
            continue;
        }
        let target = entry
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| "passed language producer plan entry has no id".to_owned())?;
        let manifest_path = output
            .join("language-producers")
            .join(target)
            .join(MANIFEST_FILE);
        verify(
            &manifest_path,
            target,
            expected_source_revision,
            expected_source_digest,
            expected_rust_model_digest,
        )?;
    }
    Ok(())
}

fn collect_files(root: &Path, excluded: Option<&Path>) -> Result<Vec<ProducerFile>, String> {
    let mut paths = Vec::new();
    collect_paths(root, root, excluded, &mut paths)?;
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let relative = path.to_string_lossy().replace('\\', "/");
            let bytes = fs::read(root.join(&path))
                .map_err(|error| format!("read producer output {relative}: {error}"))?;
            Ok(ProducerFile {
                path: relative,
                sha256: hash_bytes(&bytes),
                bytes: bytes.len() as u64,
            })
        })
        .collect()
}

fn collect_paths(
    root: &Path,
    directory: &Path,
    excluded: Option<&Path>,
    paths: &mut Vec<PathBuf>,
) -> Result<(), String> {
    for entry in fs::read_dir(directory)
        .map_err(|error| format!("read producer output {}: {error}", directory.display()))?
    {
        let path = entry
            .map_err(|error| error.to_string())?
            .path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("inspect producer output {}: {error}", path.display()))?;
        if excluded.is_some_and(|excluded| path == excluded) {
            continue;
        }
        if metadata.file_type().is_symlink() {
            return Err(format!("producer output contains a symlink: {}", path.display()));
        }
        if metadata.is_dir() {
            collect_paths(root, &path, excluded, paths)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| "producer output path escaped its root".to_owned())?;
            if !is_safe_relative(relative) {
                return Err(format!(
                    "producer output path is not portable: {}",
                    relative.display()
                ));
            }
            paths.push(relative.to_owned());
        }
    }
    Ok(())
}

fn is_safe_relative(path: &Path) -> bool {
    path.components().all(|component| {
        !matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))
    })
}

fn artifact_digest(files: &[ProducerFile]) -> String {
    let mut canonical = String::new();
    for file in files {
        canonical.push_str(&file.path);
        canonical.push('\0');
        canonical.push_str(&file.sha256);
        canonical.push('\0');
        canonical.push_str(&file.bytes.to_string());
        canonical.push('\0');
    }
    hash_bytes(canonical.as_bytes())
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(0);

    fn test_root(label: &str) -> PathBuf {
        let nonce = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "acyclic-producer-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(root.join("package/src")).expect("create output");
        root
    }

    fn cleanup(root: &Path) {
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn manifest_records_relative_file_hashes() {
        let root = test_root("manifest");
        fs::write(root.join("package/src/client.rs"), b"pub struct Client;\n").expect("write");
        let revision = "a".repeat(40);
        let (manifest_path, manifest) =
            emit(
                &root,
                "rust",
                &revision,
                "sha256:source",
                &"a".repeat(64),
            )
                .expect("emit manifest");
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].path, "package/src/client.rs");
        assert_eq!(manifest.files[0].bytes, 19);
        verify(
            &manifest_path,
            "rust",
            &revision,
            "sha256:source",
            &"a".repeat(64),
        )
        .expect("verify manifest");
        assert_eq!(manifest.rust_model_digest, "a".repeat(64));
        cleanup(&root);
    }

    #[test]
    fn modified_facade_is_rejected_by_per_file_provenance() {
        let root = test_root("modified");
        let file = root.join("package/client.rs");
        fs::write(&file, b"pub struct Client;\n").expect("write");
        let revision = "b".repeat(40);
        let (manifest_path, _) =
            emit(
                &root,
                "rust",
                &revision,
                "sha256:source",
                &"b".repeat(64),
            )
                .expect("emit manifest");
        fs::write(&file, b"pub struct Client<T>;\n").expect("mutate");
        let error = verify(
            &manifest_path,
            "rust",
            &revision,
            "sha256:source",
            &"b".repeat(64),
        )
            .expect_err("modified file must fail");
        assert!(error.contains("file manifest differs"));
        cleanup(&root);
    }

    #[test]
    fn stale_rust_model_binding_is_rejected() {
        let root = test_root("model-digest");
        fs::write(root.join("package/client.rs"), b"pub struct Client;\n").expect("write");
        let revision = "c".repeat(40);
        let (manifest_path, _) = emit(
            &root,
            "rust",
            &revision,
            "sha256:source",
            &"c".repeat(64),
        )
        .expect("emit manifest");
        let mut document: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        document["rust_model_digest"] = Value::String("d".repeat(64));
        fs::write(&manifest_path, serde_json::to_vec(&document).unwrap()).unwrap();
        let error = verify(
            &manifest_path,
            "rust",
            &revision,
            "sha256:source",
            &"c".repeat(64),
        )
        .expect_err("stale Rust model binding must fail");
        assert!(error.contains("Rust model digest differs"));
        cleanup(&root);
    }
}
