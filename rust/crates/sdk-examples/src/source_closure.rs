//! Authoritative source closure for the example producer.
//!
//! The producer is compiled from a snapshot, but it is asked at runtime to
//! read a source tree.  Both sides therefore derive the same closure from
//! Cargo's resolved local path dependency graph instead of maintaining a
//! hand-written list of files.  This includes source files, build inputs,
//! manifests, lockfiles, and generated inputs in every local package that
//! the examples package resolves.

use sdk_source_identity::normalized_build_recipe;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const EXAMPLES_PACKAGE: &str = "rust/crates/sdk-examples/";
const ROOT_INPUTS: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain",
    "rust-toolchain.toml",
];

pub fn closure_files(source_root: &Path) -> Result<Vec<String>, String> {
    let source_root = source_root.canonicalize().map_err(|error| {
        format!(
            "canonicalize source root {}: {error}",
            source_root.display()
        )
    })?;
    let manifest = source_root.join(EXAMPLES_PACKAGE).join("Cargo.toml");
    if !manifest.is_file() {
        return Err(format!(
            "sdk-examples manifest is missing: {}",
            manifest.display()
        ));
    }
    let metadata = cargo_metadata(&manifest)?;
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or("cargo metadata packages are missing")?;
    let package_by_id = packages
        .iter()
        .filter_map(|package| Some((package.get("id")?.as_str()?.to_owned(), package.clone())))
        .collect::<BTreeMap<_, _>>();
    let root_id = metadata
        .pointer("/resolve/root")
        .and_then(Value::as_str)
        .ok_or("cargo metadata resolve.root is missing")?;
    let nodes = metadata
        .pointer("/resolve/nodes")
        .and_then(Value::as_array)
        .ok_or("cargo metadata resolve.nodes are missing")?;
    let node_by_id = nodes
        .iter()
        .filter_map(|node| Some((node.get("id")?.as_str()?.to_owned(), node)))
        .collect::<BTreeMap<_, _>>();

    let mut package_ids = BTreeSet::new();
    let mut pending = VecDeque::from([root_id.to_owned()]);
    while let Some(id) = pending.pop_front() {
        if !package_ids.insert(id.clone()) {
            continue;
        }
        let node = node_by_id
            .get(&id)
            .ok_or_else(|| format!("cargo metadata node is missing for {id}"))?;
        if let Some(dependencies) = node.get("dependencies").and_then(Value::as_array) {
            for dependency in dependencies {
                if let Some(dependency_id) = dependency.as_str() {
                    pending.push_back(dependency_id.to_owned());
                }
            }
        }
    }

    let mut files = BTreeSet::new();
    for package_id in package_ids {
        let package = package_by_id
            .get(&package_id)
            .ok_or_else(|| format!("cargo metadata package is missing for {package_id}"))?;
        let manifest_path = PathBuf::from(
            package
                .get("manifest_path")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("manifest path is missing for {package_id}"))?,
        );
        // Registry packages are resolved dependencies, but they are not
        // source owned by this producer. Cargo's null `source` field is the
        // authoritative local path marker; package location is not inferred
        // from a repository directory convention.
        if package
            .get("source")
            .is_some_and(|source| !source.is_null())
        {
            continue;
        }
        let manifest_path = manifest_path.canonicalize().map_err(|error| {
            format!(
                "canonicalize package manifest {}: {error}",
                manifest_path.display()
            )
        })?;
        let Some(_) = relative_path(&source_root, &manifest_path) else {
            return Err(format!(
                "resolved local dependency escapes source root: {}",
                manifest_path.display()
            ));
        };
        let package_dir = manifest_path.parent().ok_or_else(|| {
            format!(
                "package manifest has no parent: {}",
                manifest_path.display()
            )
        })?;
        collect_package_files(&source_root, package_dir, &mut files)?;
    }
    collect_root_inputs(&source_root, &mut files)?;
    if files.is_empty() {
        return Err("resolved Cargo source closure is empty".to_owned());
    }
    Ok(files.into_iter().collect())
}

pub fn source_digest(source_root: &Path, build_target: Option<&str>) -> Result<String, String> {
    let files = closure_files(source_root)?;
    digest_files(source_root, &files, build_target)
}

pub fn model_digest(source_root: &Path, build_target: Option<&str>) -> Result<String, String> {
    let files = closure_files(source_root)?;
    let model_files = files
        .into_iter()
        .filter(|relative| !relative.starts_with(EXAMPLES_PACKAGE))
        .collect::<Vec<_>>();
    if model_files.is_empty() {
        return Err("resolved model source closure is empty".to_owned());
    }
    digest_files(source_root, &model_files, build_target)
}

pub fn digest_files(
    source_root: &Path,
    files: &[String],
    build_target: Option<&str>,
) -> Result<String, String> {
    let source_root = source_root
        .canonicalize()
        .map_err(|error| format!("canonicalize source root: {error}"))?;
    let metadata = cargo_metadata(&source_root.join(EXAMPLES_PACKAGE).join("Cargo.toml"))?;
    let recipe = normalized_build_recipe(&source_root, &metadata, build_target)?;
    let mut digest = Sha256::new();
    digest.update(b"cargo-build-recipe\0");
    digest.update(recipe);
    digest.update([0]);
    for relative in files {
        let bytes = fs::read(source_root.join(relative)).map_err(|error| {
            format!(
                "read source closure file {}: {error}",
                source_root.join(relative).display()
            )
        })?;
        digest.update(relative.as_bytes());
        digest.update([0]);
        digest.update(bytes);
        digest.update([0]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

pub fn recipe_digest(source_root: &Path, build_target: Option<&str>) -> Result<String, String> {
    let source_root = source_root
        .canonicalize()
        .map_err(|error| format!("canonicalize source root: {error}"))?;
    let metadata = cargo_metadata(&source_root.join(EXAMPLES_PACKAGE).join("Cargo.toml"))?;
    let recipe = normalized_build_recipe(&source_root, &metadata, build_target)?;
    Ok(format!("sha256:{:x}", Sha256::digest(recipe)))
}

fn cargo_metadata(manifest: &Path) -> Result<Value, String> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args([
            "metadata",
            "--manifest-path",
            &manifest.to_string_lossy(),
            "--locked",
            "--format-version",
            "1",
        ])
        .output()
        .map_err(|error| format!("run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode cargo metadata: {error}"))
}

fn collect_package_files(
    source_root: &Path,
    package_dir: &Path,
    files: &mut BTreeSet<String>,
) -> Result<(), String> {
    let mut pending = vec![package_dir.to_owned()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory)
            .map_err(|error| format!("read source package {}: {error}", directory.display()))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("read source package entry: {error}"))?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                format!("inspect source package entry {}: {error}", path.display())
            })?;
            if file_type.is_symlink() {
                return Err(format!(
                    "symlink source input is unsupported: {}",
                    path.display()
                ));
            }
            if file_type.is_dir() {
                let name = entry.file_name();
                if matches!(
                    name.to_string_lossy().as_ref(),
                    "target" | ".git" | "node_modules" | "examples" | "tests" | "benches"
                ) {
                    continue;
                }
                pending.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let relative = relative_path(source_root, &path).ok_or_else(|| {
                format!(
                    "source package file escapes source root: {}",
                    path.display()
                )
            })?;
            let relative = relative.to_string_lossy().replace('\\', "/");
            files.insert(relative);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{closure_files, source_digest};
    use std::env;
    use std::fs;
    use std::process::Command;

    #[test]
    fn crate_owned_markdown_mutation_changes_source_digest() {
        let root =
            env::temp_dir().join(format!("sdk-source-closure-guide-{}", std::process::id(),));
        let _ = fs::remove_dir_all(&root);
        let package = root.join("rust/crates/sdk-examples");
        fs::create_dir_all(package.join("src")).expect("create source package");
        fs::create_dir_all(package.join("docs")).expect("create guide directory");
        fs::write(
            root.join("Cargo.toml"),
            b"[workspace]\nmembers = [\"rust/crates/sdk-examples\"]\nresolver = \"2\"\n",
        )
        .expect("write fixture workspace manifest");
        fs::write(
            package.join("Cargo.toml"),
            b"[package]\nname = \"sdk-examples\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .expect("write fixture package manifest");
        fs::write(package.join("src/lib.rs"), b"pub fn scenario() {}\n")
            .expect("write fixture source");
        let guide = package.join("docs/guide.md");
        fs::write(&guide, b"# Baseline guide\n").expect("write baseline guide");

        let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let lock = Command::new(&cargo)
            .args(["generate-lockfile", "--manifest-path"])
            .arg(root.join("Cargo.toml"))
            .env("CARGO_NET_OFFLINE", "true")
            .current_dir(&root)
            .output()
            .expect("start fixture lockfile generation");
        assert!(
            lock.status.success(),
            "fixture lockfile generation failed: {}",
            String::from_utf8_lossy(&lock.stderr)
        );

        let files = closure_files(&root).expect("collect source closure");
        assert!(
            files
                .iter()
                .any(|path| path == "rust/crates/sdk-examples/docs/guide.md"),
            "guide Markdown must be retained in the source closure: {files:?}"
        );
        let baseline = source_digest(&root, None).expect("digest baseline closure");
        fs::write(&guide, b"# Mutated guide\n").expect("mutate guide");
        let mutated = source_digest(&root, None).expect("digest mutated closure");
        assert_ne!(
            baseline, mutated,
            "guide mutation must change source identity"
        );

        let _ = fs::remove_dir_all(root);
    }
}

/// Return a path relative to `root` while tolerating Windows' extended path
/// prefix and case-insensitive drive/path spelling. Cargo metadata may emit a
/// lexical `Q:\...` path while `canonicalize` returns `\\?\Q:\...`; using
/// `Path::strip_prefix` directly would incorrectly classify that input as an
/// escape from the captured source root.
fn relative_path(root: &Path, candidate: &Path) -> Option<PathBuf> {
    let root = normalized_path_text(root);
    let candidate = normalized_path_text(candidate);
    let root_lower = root.to_ascii_lowercase();
    let candidate_lower = candidate.to_ascii_lowercase();
    if candidate_lower == root_lower {
        return Some(PathBuf::new());
    }
    let prefix = format!("{root_lower}\\");
    candidate_lower
        .strip_prefix(&prefix)
        .map(|_| PathBuf::from(&candidate[root.len() + 1..]))
}

fn normalized_path_text(path: &Path) -> String {
    let mut text = path.to_string_lossy().replace('/', "\\");
    while let Some(stripped) = text.strip_prefix("\\\\?\\") {
        text = stripped.to_owned();
    }
    while text.ends_with('\\') && text.len() > 3 {
        text.pop();
    }
    text
}

fn collect_root_inputs(source_root: &Path, files: &mut BTreeSet<String>) -> Result<(), String> {
    for relative in ROOT_INPUTS {
        let path = source_root.join(relative);
        if let Ok(metadata) = fs::symlink_metadata(&path) {
            if metadata.file_type().is_symlink() {
                return Err(format!(
                    "symlink source input is unsupported: {}",
                    path.display()
                ));
            }
        }
        if path.is_file() {
            files.insert((*relative).to_owned());
        }
    }
    let cargo_config = source_root.join(".cargo");
    if let Ok(metadata) = fs::symlink_metadata(&cargo_config) {
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "symlink workspace Cargo config is unsupported: {}",
                cargo_config.display()
            ));
        }
    }
    if cargo_config.is_dir() {
        collect_package_files(source_root, &cargo_config, files)?;
    } else if cargo_config.exists() {
        return Err(format!(
            "workspace Cargo config is not a directory: {}",
            cargo_config.display()
        ));
    }
    Ok(())
}
