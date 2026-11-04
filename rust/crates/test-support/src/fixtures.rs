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
    let root = std::env::var_os("ACYCLIC_TEST_SOURCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.."));
    let relative = relative.as_ref();
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

/// Locates an artifact whose runtime path variable is `variable`.
///
/// Relocated tests supply paths to executables and their include directories
/// at run time. Ordinary Cargo runs use the original compiled path. Cargo
/// binary targets use [`cargo_binary!`](crate::cargo_binary).
/// The compiled path is requested only when no runtime input is present.
///
/// # Errors
///
/// Returns the compiled path provider's error when no runtime input is present.
pub fn runtime_path<E>(
    variable: &str,
    compiled: impl FnOnce() -> Result<PathBuf, E>,
) -> Result<PathBuf, E> {
    std::env::var_os(variable).map_or_else(compiled, |path| Ok(PathBuf::from(path)))
}

/// The path of a file inside the calling crate's source directory.
#[macro_export]
macro_rules! crate_path {
    ($relative:expr) => {
        $crate::crate_source_path(env!("CARGO_MANIFEST_DIR"), $relative)
    };
}
