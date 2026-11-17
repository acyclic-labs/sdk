use std::path::{Path, PathBuf};

/// Writes a runnable fixture at `path` for the host that runs the test.
///
/// Windows admits a real image or a batch script, while Unix admits a file
/// with its execute bit. Fixtures that only need to satisfy executable search
/// use this instead of writing empty bytes under a Windows image name.
pub fn executable_file(path: &Path) {
    #[cfg(windows)]
    if path.extension().is_some_and(|extension| {
        extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
    }) {
        std::fs::write(path, b"@echo off\r\n").expect("write batch fixture");
    } else {
        std::fs::copy(std::env::current_exe().expect("test executable"), path)
            .expect("copy executable fixture");
    }
    #[cfg(not(windows))]
    {
        std::fs::write(path, []).expect("write executable fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
                .expect("mark executable fixture");
        }
    }
}

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
/// If no executable is supplied, or its selected path is not absolute.
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
                option_env!(concat!("CARGO_BIN_EXE_", $name))
                    .map_or_else(::std::path::PathBuf::new, ::std::path::PathBuf::from)
            },
            ::std::path::PathBuf::from,
        );
        // An executable missing from Cargo and nextest leaves an empty path.
        assert!(
            path.is_absolute(),
            concat!(
                "test binary ",
                $name,
                " must be supplied by Cargo or nextest as an absolute path: {}"
            ),
            path.display()
        );
        path
    }};
}

/// Locates a host tool that a test starts by name.
///
/// A Bazel test supplies the tool as a declared executable through Cargo's
/// `CARGO_BIN_EXE_<name>` variable, as [`cargo_binary!`](crate::cargo_binary)
/// does. An ordinary Cargo run has no such variable, so the tool is resolved
/// from an environment variable of the same name (Cargo sets `CARGO`), or from
/// the host's executable search path by its bare name.
#[macro_export]
macro_rules! host_tool {
    ($name:literal) => {{
        ::std::env::var_os(concat!("CARGO_BIN_EXE_", $name))
            .or_else(|| ::std::env::var_os(concat!("NEXTEST_BIN_EXE_", $name).replace('-', "_")))
            .or_else(|| ::std::env::var_os($name))
            .map_or_else(
                || ::std::ffi::OsString::from($name),
                ::std::ffi::OsString::from,
            )
    }};
}

/// Locates a checked-in fixture relative to the SDK repository.
///
/// Cross-built test executables run against an explicitly supplied source
/// snapshot: set `ACYCLIC_TEST_SOURCE_ROOT` to its absolute repository root.
/// A Bazel `isolated_test` sets it to the SDK's repository in the runfiles, in
/// the SDK's own workspace and in any workspace that depends on the SDK, so a
/// consumer reads the SDK files it declares as test data. Ordinary Cargo tests
/// use the source tree they were compiled from.
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
/// `manifest` within the compile-time repository root `repository`, under the
/// same source root as [`source_path`]. Both paths come from the one compile of
/// the calling crate, so they share a root wherever that compile ran. Use
/// [`crate_path!`](crate::crate_path) rather than calling this directly.
///
/// # Panics
///
/// If `manifest` lies outside `repository`.
pub fn crate_source_path(repository: &str, manifest: &str, relative: impl AsRef<Path>) -> PathBuf {
    let package = Path::new(manifest)
        .strip_prefix(repository)
        .expect("crate manifest lies inside the repository it was compiled in");
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
        $crate::crate_source_path(
            env!("ACYCLIC_BUILD_SOURCE_ROOT"),
            env!("CARGO_MANIFEST_DIR"),
            $relative,
        )
    };
}

/// [`crate_path!`](crate::crate_path) as a `&'static str`, for APIs that keep the
/// path, such as proptest's `FileFailurePersistence::Direct`.
///
/// # Panics
///
/// If the path is not UTF-8.
#[macro_export]
macro_rules! crate_path_str {
    ($relative:expr) => {{
        static PATH: ::std::sync::OnceLock<::std::string::String> = ::std::sync::OnceLock::new();
        PATH.get_or_init(|| {
            $crate::crate_path!($relative)
                .into_os_string()
                .into_string()
                .expect("source paths are UTF-8")
        })
        .as_str()
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_tool_uses_the_declared_executable_or_the_host_search_path() {
        let child = |declared: Option<&str>| {
            let mut command = std::process::Command::new(
                std::env::current_exe().expect("current test executable"),
            );
            command
                .args(["--exact", "fixtures::tests::host_tool_child", "--nocapture"])
                .env("ACYCLIC_HOST_TOOL_CHILD", "1")
                .env_remove("CARGO_BIN_EXE_node")
                .env_remove("NEXTEST_BIN_EXE_node")
                .env_remove("node");
            if let Some(declared) = declared {
                command.env("CARGO_BIN_EXE_node", declared);
            }
            let output = command.output().expect("run host tool child");
            assert!(
                output.status.success(),
                "host tool child failed: {output:?}"
            );
        };
        child(None);
        child(Some("/declared/node"));
    }

    #[test]
    fn host_tool_child() {
        if std::env::var_os("ACYCLIC_HOST_TOOL_CHILD").is_none() {
            return;
        }
        let expected = std::env::var_os("CARGO_BIN_EXE_node")
            .unwrap_or_else(|| std::ffi::OsString::from("node"));
        assert_eq!(crate::host_tool!("node"), expected);
    }

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
