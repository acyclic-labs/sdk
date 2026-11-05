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
/// `ACYCLIC_TEST_SOURCE_ROOT` supplies the absolute source snapshot root at
/// execution time. The repository's Cargo configuration supplies its own root
/// by default; an explicit environment value takes precedence. Set the variable
/// before launching a test executable directly or after relocation.
///
/// # Panics
///
/// If the source root is missing or relative, or the fixture path is absolute.
pub fn source_path(relative: impl AsRef<Path>) -> PathBuf {
    let root = PathBuf::from(std::env::var_os("ACYCLIC_TEST_SOURCE_ROOT").expect(
        "step=fixture-source-root: test source root is missing; recovery command: cargo test --locked -p acyclic-test-support --lib (from the SDK source root)",
    ));
    assert!(
        root.is_absolute(),
        "test source root must be absolute: {}",
        root.display()
    );
    let relative = relative.as_ref();
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

/// The path of a file inside the calling crate's source directory.
#[macro_export]
macro_rules! crate_path {
    ($relative:expr) => {
        $crate::crate_source_path(env!("CARGO_MANIFEST_DIR"), $relative)
    };
}

#[cfg(test)]
mod tests {
    use super::source_path;
    use std::{path::Path, process::Command};

    #[test]
    fn runtime_source_snapshot_is_an_execution_input() {
        if let Some(fixture) = std::env::var_os("ACYCLIC_FIXTURE_PROBE") {
            assert_eq!(
                std::fs::read(source_path(fixture)).unwrap(),
                b"runtime source"
            );
            return;
        }
        assert_eq!(
            std::fs::read(crate::crate_path!("Cargo.toml")).unwrap(),
            include_bytes!("../Cargo.toml")
        );
        let root = tempfile::tempdir().unwrap();
        let fixture = root.path().join("fixture");
        std::fs::write(&fixture, b"runtime source").unwrap();
        let test = concat!(
            module_path!(),
            "::runtime_source_snapshot_is_an_execution_input"
        );
        let test = test.split_once("::").unwrap().1;
        for (supplied, relative, refusal) in [
            (Some(root.path()), Path::new("fixture"), None),
            (
                None,
                Path::new("fixture"),
                Some("test source root is missing"),
            ),
            (
                Some(Path::new("relative")),
                Path::new("fixture"),
                Some("test source root must be absolute"),
            ),
            (
                Some(root.path()),
                fixture.as_path(),
                Some("fixture path must be repository-relative"),
            ),
        ] {
            let mut child = Command::new(std::env::current_exe().unwrap());
            child
                .args(["--exact", test, "--nocapture"])
                .env_remove("ACYCLIC_TEST_SOURCE_ROOT")
                .env("ACYCLIC_FIXTURE_PROBE", relative);
            if let Some(supplied) = supplied {
                child.env("ACYCLIC_TEST_SOURCE_ROOT", supplied);
            }
            let output = child.output().expect("step=fixture-source-control launch; recovery command: cargo test --locked -p acyclic-test-support --lib runtime_source_snapshot_is_an_execution_input");
            let log = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(log.contains("running 1 test"), "{log}");
            if let Some(refusal) = refusal {
                assert!(!output.status.success() && log.contains(refusal), "{log}");
            } else {
                assert!(output.status.success(), "{log}");
            }
        }
    }
}
