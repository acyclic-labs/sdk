//! Source policies inspect precisely the repository inputs admitted at build.
//!
//! Copying thousands of files onto a target and reopening them is unnecessary
//! for a policy on the compiled source, and makes its cost depend on filesystem
//! caches. The build host enumerates once; Cargo tracks additions and changes.
//! Runtime data fixtures continue to use source_path, independently of this
//! immutable catalog. No runtime filesystem lookup or fallback is possible here.
use std::path::{Component, Path};

// A static owns the catalog once. A promoted array in the accessor may be
// duplicated across codegen units, including its source strings.
static SOURCES: &[(&str, &str)] = include!(concat!(env!("OUT_DIR"), "/repository-rust-sources.rs"));

/// Every Rust source under the repository's seven source roots, ordered by its
/// repository-relative slash-separated path. Includes tests and build scripts.
/// Build/dependency cache directories are excluded outside Rust crate src trees;
/// inside src, every directory remains visible to its crate's source policies.
#[must_use]
pub fn repository_rust_sources() -> &'static [(&'static str, &'static str)] {
    SOURCES
}

/// Looks up a repository-relative Rust module without consulting the target's
/// filesystem. Lexical parent components are permitted only inside the root.
#[must_use]
pub fn repository_rust_source(path: &Path) -> Option<&'static str> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(name.to_str()?),
            Component::CurDir => {}
            Component::ParentDir => {
                components.pop()?;
            }
            Component::Prefix(_) | Component::RootDir => return None,
        }
    }
    let name = components.join("/");
    let sources = repository_rust_sources();
    sources
        .binary_search_by_key(&name.as_str(), |(path, _)| *path)
        .ok()
        .map(|index| sources[index].1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_is_complete_ordered_and_independent_of_runtime_source_paths() {
        let sources = repository_rust_sources();
        assert!(sources.len() > 3000, "source catalog lost its roots");
        assert!(sources.windows(2).all(|pair| pair[0].0 < pair[1].0));
        for (path, text) in sources {
            assert_eq!(repository_rust_source(Path::new(path)), Some(*text));
        }
        let path = Path::new("rust/crates/test-support/src/../src/source_catalog.rs");
        assert_eq!(
            repository_rust_source(path),
            Some(include_str!("source_catalog.rs"))
        );
        assert!(
            repository_rust_source(Path::new("../rust/crates/test-support/src/lib.rs")).is_none()
        );
        assert!(repository_rust_source(Path::new("rust/crates/test-support/absent.rs")).is_none());
        assert!(repository_rust_source(&std::env::temp_dir().join("absent.rs")).is_none());
    }
}
