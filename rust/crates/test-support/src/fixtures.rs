use std::path::{Path, PathBuf};

/// Locates the original named Cargo executable after runtime relocation.
///
/// Runtime Cargo metadata wins over the compiled path. Nextest's portable
/// executable variable is used when Cargo metadata is absent. An optional
/// explicitly selected executable takes precedence over both. Checking a consumer
/// does not require Cargo to have built its executable: the compiled path is
/// consulted only when no explicit or runtime executable was selected.
///
/// # Panics
///
/// If no executable metadata is available, or its selected path is not absolute.
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
            || {
                ::std::path::PathBuf::from(
                    option_env!(concat!("CARGO_BIN_EXE_", $name)).expect(concat!(
                        "test binary metadata absent for ",
                        $name,
                        "; select its executable explicitly or supply Cargo/Nextest runtime metadata"
                    )),
                )
            },
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

#[cfg(test)]
mod tests {
    #[test]
    fn selected_external_runner_executes_without_compiled_cargo_metadata() {
        let executable = crate::cargo_binary!(
            "external-native-test-consumer",
            Some(
                std::env::current_exe()
                    .expect("current test executable")
                    .into_os_string()
            )
        );
        let output = std::process::Command::new(executable)
            .arg("--list")
            .output()
            .expect("execute explicitly selected original runner");
        assert!(
            output.status.success(),
            "selected runner failed: {output:?}"
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("selected_external_runner_executes_without_compiled_cargo_metadata"),
            "selected runner did not expose its original property: {output:?}"
        );
    }
}
