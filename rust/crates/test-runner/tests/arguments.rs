//! The runner binary, started as Bazel starts it, selects cases from its own command-line
//! arguments. The `fixture_` cases act only in a working directory holding `MARKER`, which only
//! this test creates; anywhere else they pass at once.

use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

const MARKER: &str = "acyclic-test-runner-arguments-fixture";

fn acting() -> bool {
    Path::new(MARKER).exists()
}

#[test]
fn fixture_selected() {}

#[test]
fn fixture_excluded() {
    assert!(!acting(), "an excluded case ran");
}

/// `fixture_ --skip fixture_excluded`, given as arguments, runs only `fixture_selected`: a
/// runner that dropped its arguments would also run `fixture_excluded`, which fails here.
#[test]
fn the_runner_selects_cases_from_its_arguments() {
    // Run as a fixture by a runner that ignored its selection, it does not start another runner.
    if acting() {
        return;
    }
    let runner = env::var_os("CARGO_BIN_EXE_acyclic-test-runner")
        .expect("the runner executable is supplied as CARGO_BIN_EXE_acyclic-test-runner");
    let dir = env::temp_dir().join(format!(
        "acyclic-test-runner-arguments-{}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(MARKER), "").unwrap();
    let output = Command::new(runner)
        .args(["fixture_", "--skip", "fixture_excluded"])
        .env("ACYCLIC_TEST_BINARY", env::current_exe().unwrap())
        .env("ACYCLIC_TEST_ID", "arguments")
        .env("ACYCLIC_TEST_PACKAGE", &dir)
        .env("ACYCLIC_TEST_LIMIT_SECONDS", "10")
        .env("TEST_TMPDIR", &dir)
        .env_remove("TESTBRIDGE_TEST_ONLY")
        .current_dir(&dir)
        .output()
        .unwrap();
    fs::remove_dir_all(&dir).unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report = format!("{stdout}{}", String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success(), "{report}");
    let cases: Vec<&str> = stdout
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("PASS"))
        .filter_map(|line| line.rsplit(' ').next())
        .collect();
    assert_eq!(cases, ["fixture_selected"], "{report}");
}
