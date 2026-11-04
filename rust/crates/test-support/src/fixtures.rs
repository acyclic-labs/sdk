use std::path::{Path, PathBuf};

/// Locates a checked-in fixture relative to the SDK repository.
///
/// Cross-built test executables run against an explicitly supplied source
/// snapshot: set `ACYCLIC_TEST_SOURCE_ROOT` to its absolute repository root.
/// Ordinary Cargo tests use the source tree they were compiled from.
///
/// # Panics
///
/// If the explicit source root is relative, or the fixture path is absolute.
pub fn source_path(relative: impl AsRef<Path>) -> PathBuf {
    resolve(
        std::env::var_os("ACYCLIC_TEST_SOURCE_ROOT").map(PathBuf::from),
        relative.as_ref(),
    )
}

/// Locates `relative` inside the crate whose compile-time manifest directory is
/// `manifest`, under the same source root as [`source_path`]. Use
/// [`crate_path!`](crate::crate_path) rather than calling this directly.
///
/// # Panics
///
/// If `manifest` lies outside the repository this crate was compiled in.
pub fn crate_source_path(manifest: &str, relative: impl AsRef<Path>) -> PathBuf {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("test-support lives at rust/crates/test-support");
    let package = Path::new(manifest)
        .strip_prefix(repository)
        .expect("crate manifest lies inside the SDK repository");
    source_path(package.join(relative))
}

/// Locates the Cargo binary target whose runtime variable is `variable`.
///
/// Test runners that execute relocated test binaries (nextest archives) set
/// `CARGO_BIN_EXE_<name>` at run time; ordinary Cargo runs use the path the
/// test was compiled with. Use [`binary_path!`](crate::binary_path).
pub fn binary_target(variable: &str, compiled: &str) -> PathBuf {
    std::env::var_os(variable).map_or_else(|| PathBuf::from(compiled), PathBuf::from)
}

/// The path of a file inside the calling crate's source directory.
#[macro_export]
macro_rules! crate_path {
    ($relative:expr) => {
        $crate::crate_source_path(env!("CARGO_MANIFEST_DIR"), $relative)
    };
}

/// The path of the calling package's Cargo binary target `$name`.
#[macro_export]
macro_rules! binary_path {
    ($name:literal) => {
        $crate::binary_target(
            concat!("CARGO_BIN_EXE_", $name),
            env!(concat!("CARGO_BIN_EXE_", $name)),
        )
    };
}

fn resolve(root: Option<PathBuf>, relative: &Path) -> PathBuf {
    let root = root.unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.."));
    assert!(
        root.is_absolute(),
        "test source root must be absolute: {}",
        root.display()
    );
    assert!(
        relative.is_relative(),
        "fixture path must be repository-relative"
    );
    root.join(relative)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_source_snapshot_replaces_the_build_hosts_path() {
        let root = std::env::temp_dir().join("cross-built-fixtures");
        let source = resolve(Some(root.clone()), Path::new("plugin/src/main.rs"));
        assert_eq!(source, root.join("plugin/src/main.rs"));
        assert!(source.is_absolute());
    }
}
