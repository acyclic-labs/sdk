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
/// Runtime Cargo metadata wins over nextest's portable executable variable. An
/// optional explicitly selected executable takes precedence over both. A test
/// binary is supplied by Cargo or nextest at run time; a consumer that does not
/// build it fails the assertion rather than reading a path compiled into it.
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
        let path = selected.map_or_else(::std::path::PathBuf::new, ::std::path::PathBuf::from);
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
/// consumer reads the SDK files it declares as test data. A Bazel test that runs
/// the binary directly reads the SDK's repository from the runfiles. Ordinary
/// Cargo tests use the workspace they run in.
///
/// # Panics
///
/// If the explicit source root is relative, or the fixture path is absolute.
pub fn source_path(relative: impl AsRef<Path>) -> PathBuf {
    let relative = relative.as_ref();
    assert!(
        relative.is_relative(),
        "fixture path must be repository-relative"
    );
    let root = source_root(relative);
    assert!(
        root.is_absolute(),
        "test source root must be absolute: {}",
        root.display()
    );
    root.join(relative)
}

/// The repository root a test reads checked-in files from, resolved at run time.
fn source_root(relative: &Path) -> PathBuf {
    std::env::var_os("ACYCLIC_TEST_SOURCE_ROOT")
        .map(PathBuf::from)
        .or_else(|| runfiles_source_root(relative))
        .or_else(bazel_exec_root)
        .unwrap_or_else(|| {
            workspace_root(&std::env::current_dir().expect("test working directory"))
        })
}

/// The exec root of a Bazel test, whose source tree the test binary's runfiles
/// path names. A test that runs directly rather than through `isolated_test`
/// reads checked-in files from the exec root, which holds the source tree.
fn bazel_exec_root() -> Option<PathBuf> {
    let runfiles = std::env::var_os("TEST_SRCDIR")?;
    let components: Vec<_> = Path::new(&runfiles).components().collect();
    let bazel_out = components
        .iter()
        .position(|component| component.as_os_str() == "bazel-out")?;
    Some(components.get(..bazel_out)?.iter().collect())
}

/// The workspace root above `package`, the directory whose manifest declares the workspace.
fn workspace_root(package: &Path) -> PathBuf {
    package
        .ancestors()
        .find(|directory| {
            std::fs::read_to_string(directory.join("Cargo.toml"))
                .is_ok_and(|manifest| manifest.contains("[workspace]"))
        })
        .expect("crate directory lies inside a Cargo workspace")
        .to_owned()
}

/// The SDK repository in a Bazel test's runfiles when the test runs directly
/// rather than through `isolated_test`, which supplies `ACYCLIC_TEST_SOURCE_ROOT`.
/// A consumer sees the SDK as an external repository, so only a path that exists
/// in the main workspace's runfiles selects it.
fn runfiles_source_root(relative: &Path) -> Option<PathBuf> {
    let runfiles = std::env::var_os("TEST_SRCDIR")?;
    let workspace = std::env::var_os("TEST_WORKSPACE")?;
    let root = Path::new(&runfiles).join(workspace);
    root.join(relative).exists().then_some(root)
}

/// The repository-relative directory of the calling crate, resolved at run time.
///
/// A Bazel `isolated_test` runs each case in its package directory under the
/// source root it supplies. A Bazel test that runs its binary directly carries
/// the package in the binary's path under `bazel-out/<config>/bin`. An ordinary
/// Cargo test runs in its package directory, which is the workspace root's
/// descendant.
fn crate_package() -> PathBuf {
    let package = std::env::current_dir().expect("test working directory");
    if let Some(root) = std::env::var_os("ACYCLIC_TEST_SOURCE_ROOT").map(PathBuf::from) {
        return package
            .strip_prefix(&root)
            .expect("crate directory lies inside the test source root")
            .to_owned();
    }
    if let Some(package) = bazel_package() {
        return package;
    }
    let root = workspace_root(&package);
    package
        .strip_prefix(&root)
        .expect("crate directory lies inside the Cargo workspace")
        .to_owned()
}

/// The package of a Bazel test binary, from its path under `bazel-out/<config>/bin`.
fn bazel_package() -> Option<PathBuf> {
    let executable = std::env::current_exe().ok()?;
    let components: Vec<_> = executable.components().collect();
    let bazel_out = components
        .iter()
        .position(|component| component.as_os_str() == "bazel-out")?;
    let package = components.get(bazel_out + 3..components.len().checked_sub(1)?)?;
    Some(package.iter().collect())
}

/// Locates `relative` inside the calling crate's source directory, under the
/// same source root as [`source_path`]. Use [`crate_path!`](crate::crate_path)
/// rather than calling this directly.
pub fn crate_source_path(relative: impl AsRef<Path>) -> PathBuf {
    source_path(crate_package().join(relative))
}

/// The path of a file inside the calling crate's source directory.
#[macro_export]
macro_rules! crate_path {
    ($relative:expr) => {
        $crate::crate_source_path($relative)
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
