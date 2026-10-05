//! Rust-owned policies for generated language package staging.
//!
//! Language launchers may invoke a pinned generator, but they must not decide
//! which generated files are portable package content.  This module keeps
//! those decisions in the Rust generation entrypoint and returns the exact
//! files removed from a staged package for the receipt.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const OPENAPI_GENERATOR_VERSION: &str = "7.25.0";
const OPENAPI_GENERATOR_COMMIT: &str = "ef964b04480889ef86b56cfae84ade8ad4c91c41";
const OPENAPI_GENERATOR_JAR_SHA256: &str =
    "41ce4f6b07f196676439d710759fa1ced7a08066d06ff1bf314681470289efae";

/// The immutable generator identity and portable staging policy for one
/// additional target.  The language process remains an external compiler or
/// package tool; the policy itself is Rust-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPackagePolicy {
    pub target_id: &'static str,
    pub generator: &'static str,
    pub generator_version: &'static str,
    pub generator_commit: &'static str,
    pub generator_jar_sha256: &'static str,
    pub portable_excluded_files: &'static [&'static str],
}

const R_PORTABLE_EXCLUDED_FILES: &[&str] = &[".travis.yml"];
const NO_PORTABLE_EXCLUDED_FILES: &[&str] = &[];

/// Returns the pinned policy for the additional OpenAPI targets that are
/// produced by the Rust-owned generation lane.
pub fn target_package_policy(target_id: &str) -> Option<TargetPackagePolicy> {
    let (canonical_target_id, generator) = match target_id {
        "ada" => ("ada", "OpenAPI Generator ada"),
        "clojure" => ("clojure", "OpenAPI Generator clojure"),
        "crystal" => ("crystal", "OpenAPI Generator crystal"),
        "elm" => ("elm", "OpenAPI Generator elm"),
        "gdscript" => ("gdscript", "OpenAPI Generator gdscript"),
        "nim" => ("nim", "OpenAPI Generator nim"),
        "r" => ("r", "OpenAPI Generator r"),
        _ => return None,
    };
    Some(TargetPackagePolicy {
        target_id: canonical_target_id,
        generator,
        generator_version: OPENAPI_GENERATOR_VERSION,
        generator_commit: OPENAPI_GENERATOR_COMMIT,
        generator_jar_sha256: OPENAPI_GENERATOR_JAR_SHA256,
        portable_excluded_files: if target_id == "r" {
            R_PORTABLE_EXCLUDED_FILES
        } else {
            NO_PORTABLE_EXCLUDED_FILES
        },
    })
}

/// Apply the Rust-owned portable staging policy to one generated package.
///
/// The return value is relative to `package_root`, sorted by traversal order
/// and suitable for inclusion in a generation receipt.  Unknown targets are
/// deliberately a no-op so existing non-OpenAPI producers remain unchanged.
pub fn sanitize_generated_package(
    target_id: &str,
    package_root: &Path,
) -> io::Result<Vec<PathBuf>> {
    let Some(policy) = target_package_policy(target_id) else {
        return Ok(Vec::new());
    };
    if !package_root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "generated package root is not a directory: {}",
                package_root.display()
            ),
        ));
    }
    let mut removed = Vec::new();
    remove_portable_excluded_files(package_root, package_root, policy, &mut removed)?;
    removed.sort();
    Ok(removed)
}

/// Rebuild the deterministic archive after Rust has applied the package
/// policy.  The archiver is a pinned generic utility; target selection,
/// filtering, and the archive location remain owned by this Rust module.
pub fn write_deterministic_archive(
    source_root: &Path,
    target_id: &str,
    package_root: &Path,
    target_output: &Path,
) -> io::Result<PathBuf> {
    if target_package_policy(target_id).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no Rust package policy for target {target_id}"),
        ));
    }
    let archiver = source_root
        .join("research/additional-languages/openapi-targets/write-deterministic-zip.ps1");
    if !archiver.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "deterministic archive writer is missing: {}",
                archiver.display()
            ),
        ));
    }
    let archive = target_output.join(format!("acyclic-http-{target_id}-0.1.0.zip"));
    let result = Command::new("pwsh")
        .args([
            "-NoProfile",
            "-File",
            &archiver.to_string_lossy(),
            "-Root",
            &package_root.to_string_lossy(),
            "-Archive",
            &archive.to_string_lossy(),
        ])
        .output()?;
    if !result.status.success() {
        return Err(io::Error::other(format!(
            "deterministic archive writer failed for {target_id}: {}",
            String::from_utf8_lossy(&result.stderr).trim()
        )));
    }
    if !archive.is_file() {
        return Err(io::Error::other(format!(
            "deterministic archive writer produced no archive for {target_id}: {}",
            archive.display()
        )));
    }
    Ok(archive)
}

fn remove_portable_excluded_files(
    root: &Path,
    current: &Path,
    policy: TargetPackagePolicy,
    removed: &mut Vec<PathBuf>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            remove_portable_excluded_files(root, &path, policy, removed)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if policy
            .portable_excluded_files
            .iter()
            .any(|excluded| *excluded == name)
        {
            fs::remove_file(&path)?;
            let relative = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
            removed.push(relative);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("acyclic-sdk-generation-{label}-{nonce}"))
    }

    #[test]
    fn policies_pin_all_additional_targets_to_one_generator() {
        for target in ["ada", "clojure", "crystal", "elm", "gdscript", "nim", "r"] {
            let policy = target_package_policy(target).expect("target policy");
            assert_eq!(policy.target_id, target);
            assert_eq!(policy.generator_version, "7.25.0");
            assert_eq!(policy.generator_commit, OPENAPI_GENERATOR_COMMIT);
            assert_eq!(policy.generator_jar_sha256, OPENAPI_GENERATOR_JAR_SHA256);
        }
        assert!(target_package_policy("python").is_none());
    }

    #[test]
    fn r_policy_removes_machine_local_travis_files_before_packaging() {
        let root = temp_root("r-portable");
        fs::create_dir_all(root.join("actors/nested")).expect("create package");
        fs::write(root.join("actors/.travis.yml"), b"cache: /machine").expect("travis");
        fs::write(root.join("actors/nested/.travis.yml"), b"cache: /machine").expect("travis");
        fs::write(root.join("actors/README.md"), b"portable").expect("readme");

        let removed = sanitize_generated_package("r", &root).expect("sanitize");
        assert_eq!(removed.len(), 2);
        assert!(!root.join("actors/.travis.yml").exists());
        assert!(!root.join("actors/nested/.travis.yml").exists());
        assert!(root.join("actors/README.md").exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn non_r_policies_preserve_generator_files() {
        let root = temp_root("nim-portable");
        fs::create_dir_all(&root).expect("create package");
        fs::write(root.join(".travis.yml"), b"generator metadata").expect("travis");

        let removed = sanitize_generated_package("nim", &root).expect("sanitize");
        assert!(removed.is_empty());
        assert!(root.join(".travis.yml").exists());
        fs::remove_dir_all(root).expect("cleanup");
    }
}
