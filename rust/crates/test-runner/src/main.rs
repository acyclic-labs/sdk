//! Runs every libtest case of one test executable in its own process under a time limit,
//! as cargo-nextest does, so a Bazel test action keeps nextest's per-test isolation.
//!
//! The Bazel `isolated_test` rule supplies, relative to the runfiles workspace directory:
//! `ACYCLIC_TEST_BINARY` (the libtest executable), `ACYCLIC_TEST_PACKAGE` (the Cargo
//! package directory, each case's working directory as under Cargo) and
//! `ACYCLIC_TEST_LIMIT_SECONDS` (the per-case limit). Bazel's `--test_filter` arrives as
//! `TESTBRIDGE_TEST_ONLY` and selects cases as `cargo test <filter>` does.
//!
//! Ignored cases are reported as skipped and not run, as under Cargo. Each case runs in an
//! owned process tree, so a case past its limit is stopped with every process it started.

use std::env;
use std::fs::{self, File};
use std::io::{self, Write};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use acyclic_native_runtime::{ProcessCommand, ProcessCustody, ProcessStdio, ProcessTree};

/// How one case ended.
enum Outcome {
    Passed,
    Failed(Option<i32>),
    TimedOut,
}

/// One test executable and the limits each of its cases runs under.
struct Suite {
    /// The source tree the cases read checked-in files from, given to them as
    /// `ACYCLIC_TEST_SOURCE_ROOT`: the test's runfiles root.
    source: PathBuf,
    /// Cargo's `CARGO_BIN_EXE_<name>` variables, as absolute paths under the runfiles root.
    executables: Vec<(String, PathBuf)>,
    binary: PathBuf,
    package: PathBuf,
    logs: PathBuf,
    limit: Duration,
}

fn main() -> ExitCode {
    match run() {
        Ok(0) => ExitCode::SUCCESS,
        Ok(failed) => {
            eprintln!("acyclic-test-runner: {failed} case(s) failed");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("acyclic-test-runner: {error}");
            ExitCode::from(2)
        }
    }
}

/// The runner's own settings, from its rule. Cases never see them: a case may read a variable of
/// the same name as its own input.
const SETTINGS: [&str; 3] = [
    "ACYCLIC_TEST_BINARY",
    "ACYCLIC_TEST_LIMIT_SECONDS",
    "ACYCLIC_TEST_PACKAGE",
];

fn setting(name: &str) -> Result<String, String> {
    env::var(name).map_err(|error| format!("{name}: {error}"))
}

fn run() -> Result<usize, String> {
    let root = env::current_dir().map_err(|error| format!("working directory: {error}"))?;
    let limit = setting("ACYCLIC_TEST_LIMIT_SECONDS")?
        .parse()
        .map_err(|error| format!("ACYCLIC_TEST_LIMIT_SECONDS: {error}"))?;
    let executables = env::vars_os()
        .filter_map(|(name, path)| {
            let name = name.into_string().ok()?;
            name.starts_with("CARGO_BIN_EXE_")
                .then(|| (name, root.join(path)))
        })
        .collect();
    let suite = Suite {
        source: root.clone(),
        executables,
        binary: root.join(setting("ACYCLIC_TEST_BINARY")?),
        package: root.join(setting("ACYCLIC_TEST_PACKAGE")?),
        logs: PathBuf::from(setting("TEST_TMPDIR")?),
        limit: Duration::from_secs(limit),
    };
    let filter = env::var("TESTBRIDGE_TEST_ONLY").ok();
    run_suite(&suite, filter.as_deref(), &mut io::stdout().lock())
}

/// The cases `filter` selects, with `--ignored` only the ignored ones.
fn list(suite: &Suite, filter: Option<&str>, ignored: bool) -> Result<Vec<String>, String> {
    let listing = Command::new(&suite.binary)
        .args(["--list", "--format", "terse"])
        .args(ignored.then_some("--ignored"))
        .args(filter)
        .env("ACYCLIC_TEST_SOURCE_ROOT", &suite.source)
        .env_remove(SETTINGS[0])
        .env_remove(SETTINGS[1])
        .env_remove(SETTINGS[2])
        .envs(suite.executables.iter().map(|(name, path)| (name, path)))
        .current_dir(&suite.package)
        .output()
        .map_err(|error| format!("{}: {error}", suite.binary.display()))?;
    if !listing.status.success() {
        return Err(format!(
            "listing {} failed: {}",
            suite.binary.display(),
            String::from_utf8_lossy(&listing.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&listing.stdout)
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .map(str::to_owned)
        .collect())
}

/// Runs each case `filter` selects in its own process and reports every outcome to `out`.
/// Ignored cases are reported as skipped. Returns how many cases did not pass.
fn run_suite(suite: &Suite, filter: Option<&str>, out: &mut dyn Write) -> Result<usize, String> {
    let ignored = list(suite, filter, true)?;
    for name in &ignored {
        writeln!(out, "SKIP [ ignored] {name}").map_err(|error| format!("report: {error}"))?;
    }
    let cases: Vec<String> = list(suite, filter, false)?
        .into_iter()
        .filter(|name| !ignored.contains(name))
        .collect();
    // Every case tree is held in custody, so a timeout stops descendants that left its process
    // group (setsid, nested trees) too.
    let custody = ProcessCustody::acquire(&suite.logs.join("processes"))
        .map_err(|error| format!("process custody: {error}"))?;
    let queue = Mutex::new(cases.iter().enumerate());
    let workers = thread::available_parallelism()
        .map_or(1, NonZero::get)
        .min(cases.len());
    let (sender, results) = mpsc::channel();
    thread::scope(|scope| {
        for _ in 0..workers {
            let (sender, queue, custody) = (sender.clone(), &queue, &custody);
            scope.spawn(move || {
                while let Some((index, name)) = queue.lock().ok().and_then(|mut queue| queue.next())
                {
                    let log = suite.logs.join(format!("case-{index}.log"));
                    let started = Instant::now();
                    let outcome = case(suite, custody, name, &log);
                    if sender
                        .send((name.as_str(), outcome, started.elapsed(), log))
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        drop(sender);
        let mut failed = 0;
        for (name, outcome, elapsed, log) in results {
            let verdict = match outcome {
                Ok(Outcome::Passed) => {
                    writeln!(out, "PASS [{:>7.3}s] {name}", elapsed.as_secs_f64())
                        .map_err(|error| format!("report: {error}"))?;
                    continue;
                }
                Ok(Outcome::Failed(code)) => format!("FAIL exit={code:?}"),
                Ok(Outcome::TimedOut) => format!("TIMEOUT after {:?}", suite.limit),
                Err(error) => format!("ERROR {error}"),
            };
            failed += 1;
            writeln!(
                out,
                "{verdict} [{:>7.3}s] {name}\n{}",
                elapsed.as_secs_f64(),
                fs::read_to_string(&log).unwrap_or_default()
            )
            .map_err(|error| format!("report: {error}"))?;
        }
        Ok(failed)
    })
}

/// Runs one case in its own custodied process tree. A case past the limit fails, and the
/// tree is stopped with every process the case started, including ones that left its group.
fn case(suite: &Suite, custody: &ProcessCustody, name: &str, log: &Path) -> io::Result<Outcome> {
    let output = File::create(log)?;
    let mut command = ProcessCommand::new(&suite.binary);
    command
        .args([name, "--exact", "--nocapture", "--test-threads", "1"])
        .env("ACYCLIC_TEST_SOURCE_ROOT", &suite.source);
    for setting in SETTINGS {
        command.env_remove(setting);
    }
    for (variable, path) in &suite.executables {
        command.env(variable, path);
    }
    command
        .current_dir(&suite.package)
        .stdin(ProcessStdio::null())
        .stderr(output.try_clone()?)
        .stdout(output);
    let mut tree = ProcessTree::spawn_in_custody(&mut command, custody)?;
    Ok(match tree.wait(suite.limit) {
        Ok(status) if status.success() => Outcome::Passed,
        Ok(status) => Outcome::Failed(status.code()),
        Err(error) if error.kind() == io::ErrorKind::TimedOut => {
            tree.terminate(None)?;
            Outcome::TimedOut
        }
        Err(error) => return Err(error),
    })
}

#[cfg(test)]
mod tests {
    //! The runner drives this test executable itself. The `fixture_` cases act out each
    //! outcome only in a working directory holding `MARKER`, which only these tests create;
    //! anywhere else they pass at once.

    use super::*;

    const MARKER: &str = "acyclic-test-runner-fixture";

    fn acting() -> bool {
        Path::new(MARKER).exists()
    }

    #[test]
    fn fixture_pass() {}

    #[test]
    fn fixture_fail() {
        assert!(!acting(), "fixture failure");
    }

    #[test]
    #[ignore = "the runner reports ignored cases as skipped without running them"]
    fn fixture_ignored() {
        assert!(!acting(), "ignored fixture ran");
    }

    #[test]
    fn fixture_hang() {
        if acting() {
            thread::sleep(Duration::from_secs(3600));
        }
    }

    fn suite(name: &str) -> Suite {
        let dir =
            env::temp_dir().join(format!("acyclic-test-runner-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(MARKER), "").unwrap();
        Suite {
            source: dir.clone(),
            executables: Vec::new(),
            binary: env::current_exe().unwrap(),
            package: dir.clone(),
            logs: dir,
            limit: Duration::from_secs(2),
        }
    }

    fn report(suite: &Suite, filter: &str) -> (usize, String) {
        let mut out = Vec::new();
        let failed = run_suite(suite, Some(filter), &mut out).unwrap();
        fs::remove_dir_all(&suite.package).unwrap();
        (failed, String::from_utf8(out).unwrap())
    }

    #[test]
    fn reports_each_outcome_and_counts_failures() {
        let (failed, out) = report(&suite("outcomes"), "tests::fixture_");
        assert_eq!(failed, 2, "{out}");
        assert!(
            out.contains("PASS ") && out.contains("tests::fixture_pass"),
            "{out}"
        );
        assert!(
            out.contains("FAIL exit=Some(101)") && out.contains("fixture failure"),
            "{out}"
        );
        assert!(
            out.contains("TIMEOUT after 2s") && out.contains("tests::fixture_hang"),
            "{out}"
        );
        assert!(
            out.contains("SKIP [ ignored] tests::fixture_ignored")
                && !out.contains("ignored fixture ran"),
            "{out}"
        );
    }

    #[test]
    fn filter_selects_cases() {
        let (failed, out) = report(&suite("filter"), "tests::fixture_pass");
        assert_eq!((failed, out.lines().count()), (0, 1), "{out}");
    }
}
