use std::path::{Path, PathBuf};

/// Locates the original named Cargo executable after runtime relocation.
///
/// Runtime Cargo metadata wins over the compiled path. Nextest's portable
/// executable variable is used when Cargo metadata is absent. An optional
/// explicitly selected executable takes precedence over both.
///
/// # Panics
///
/// If the selected executable path is not absolute.
#[macro_export]
macro_rules! cargo_binary {
    ($name:literal) => {
        $crate::cargo_binary!($name, None::<::std::ffi::OsString>)
    };
    ($name:literal, $selected:expr) => {{
        let selected = ($selected)
            .or_else(|| ::std::env::var_os(concat!("CARGO_BIN_EXE_", $name)))
            .or_else(|| ::std::env::var_os(concat!("NEXTEST_BIN_EXE_", $name).replace('-', "_")));
        let path = selected.map_or_else(
            || ::std::path::PathBuf::from(env!(concat!("CARGO_BIN_EXE_", $name))),
            ::std::path::PathBuf::from,
        );
        assert!(
            path.is_absolute(),
            "test binary path must be absolute: {}",
            path.display()
        );
        path
    }};
}

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
