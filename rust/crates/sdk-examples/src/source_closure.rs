//! Runtime source-closure digests for the examples producer.

#[path = "source_closure_common.rs"]
mod common;

pub use common::{closure_files, digest_files};

use sdk_source_identity::normalized_build_recipe;
use sha2::{Digest, Sha256};
use std::path::Path;

const EXAMPLES_PACKAGE: &str = "rust/crates/sdk-examples/";

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

pub fn recipe_digest(source_root: &Path, build_target: Option<&str>) -> Result<String, String> {
    let source_root = source_root
        .canonicalize()
        .map_err(|error| format!("canonicalize source root: {error}"))?;
    let metadata = common::cargo_metadata(&source_root.join(EXAMPLES_PACKAGE).join("Cargo.toml"))?;
    let recipe = normalized_build_recipe(&source_root, &metadata, build_target)?;
    Ok(format!("sha256:{:x}", Sha256::digest(recipe)))
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
