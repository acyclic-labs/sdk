//! Runs every libtest case of one test executable in its own process under a time limit,
//! as cargo-nextest does, so a Bazel test action keeps nextest's per-test isolation.
//!
//! The Bazel `isolated_test` rule supplies, relative to the runfiles workspace directory:
//! `ACYCLIC_TEST_BINARY` (the libtest executable), `ACYCLIC_TEST_ID` (its nextest binary id),
//! `ACYCLIC_TEST_PACKAGE` (the Cargo package directory, each case's working directory as under
//! Cargo) and `ACYCLIC_TEST_LIMIT_SECONDS` (the per-case limit). Bazel's `--test_filter` arrives as
//! `TESTBRIDGE_TEST_ONLY` and selects cases as `cargo test <filter>` does.
//!
//! Each outcome is reported as nextest reports it, `STATUS [ seconds] <binary id> <case>`, so a
//! case keeps one identity whichever runner ran it. Ignored cases are reported as skipped and
//! not run, as under Cargo. Each case runs in an
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
    /// The executable's nextest binary id: `<package>`, `<package>::<test>`, `<package>::bin/<name>`.
    id: String,
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
const SETTINGS: [&str; 4] = [
    "ACYCLIC_TEST_BINARY",
    "ACYCLIC_TEST_ID",
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
        id: setting("ACYCLIC_TEST_ID")?,
        package: root.join(setting("ACYCLIC_TEST_PACKAGE")?),
        logs: PathBuf::from(setting("TEST_TMPDIR")?),
        limit: Duration::from_secs(limit),
    };
    let filter = env::var("TESTBRIDGE_TEST_ONLY").ok();
    run_suite(&suite, filter.as_deref(), &mut io::stdout().lock())
}

/// The cases `filter` selects, with `--ignored` only the ignored ones.
fn list(suite: &Suite, filter: Option<&str>, ignored: bool) -> Result<Vec<String>, String> {
    let mut command = Command::new(&suite.binary);
    command
        .args(["--list", "--format", "terse"])
        .args(ignored.then_some("--ignored"))
        .args(filter)
        .env("ACYCLIC_TEST_SOURCE_ROOT", &suite.source);
    for setting in SETTINGS {
        command.env_remove(setting);
    }
    let listing = command
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
        writeln!(out, "{:>12} [         ] {} {name}", "SKIP", suite.id)
            .map_err(|error| format!("report: {error}"))?;
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
            let (status, detail) = match outcome {
                Ok(Outcome::Passed) => ("PASS", None),
                Ok(Outcome::Failed(code)) => ("FAIL", Some(format!("exit={code:?}"))),
                Ok(Outcome::TimedOut) => ("TIMEOUT", Some(format!("after {:?}", suite.limit))),
                Err(error) => ("FAIL", Some(format!("runner error: {error}"))),
            };
            let line = format!(
                "{status:>12} [{:>8.3}s] {} {name}",
                elapsed.as_secs_f64(),
                suite.id
            );
            let Some(detail) = detail else {
                writeln!(out, "{line}").map_err(|error| format!("report: {error}"))?;
                continue;
            };
            failed += 1;
            writeln!(
                out,
                "{line}\n{detail}\n{}",
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

    /// A case started by a runner whose own settings are set: it sees its source root and
    /// executables, and none of the runner's settings.
    #[test]
    fn environment_case() {
        if acting() {
            for setting in SETTINGS {
                assert_eq!(env::var_os(setting), None, "{setting} reached the case");
            }
            assert!(env::var_os("ACYCLIC_TEST_SOURCE_ROOT").is_some());
            assert_eq!(
                env::var_os("CARGO_BIN_EXE_fixture").map(PathBuf::from),
                Some(env::current_exe().unwrap())
            );
        }
    }

    /// Acts as a runner started with its settings set, running `environment_case`.
    #[test]
    fn environment_runner() {
        if acting()
            && SETTINGS
                .iter()
                .all(|setting| env::var_os(setting).is_some())
        {
            let root = env::current_dir().unwrap();
            let suite = Suite {
                source: root.clone(),
                executables: vec![("CARGO_BIN_EXE_fixture".into(), env::current_exe().unwrap())],
                binary: env::current_exe().unwrap(),
                id: "fixture".into(),
                package: root.clone(),
                logs: root,
                limit: Duration::from_secs(10),
            };
            let mut out = Vec::new();
            let failed = run_suite(&suite, Some("tests::environment_case"), &mut out).unwrap();
            let out = String::from_utf8(out).unwrap();
            assert_eq!((failed, out.lines().count()), (0, 1), "{out}");
        }
    }

    #[test]
    fn cases_see_their_inputs_and_none_of_the_runner_settings() {
        let dir = suite("environment").package;
        let mut runner = Command::new(env::current_exe().unwrap());
        runner
            .args(["tests::environment_runner", "--exact", "--nocapture"])
            .current_dir(&dir);
        for setting in SETTINGS {
            runner.env(setting, "runner setting");
        }
        let output = runner.output().unwrap();
        fs::remove_dir_all(&dir).unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
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
            id: "acyclic-test-runner".into(),
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
        let status = |status: &str, case: &str| {
            let rest = format!("acyclic-test-runner tests::{case}");
            out.lines()
                .any(|line| nextest_line(line) == Some((status, rest.as_str())))
        };
        assert!(status("PASS", "fixture_pass"), "{out}");
        assert!(
            status("FAIL", "fixture_fail")
                && out.contains("exit=Some(101)")
                && out.contains("fixture failure"),
            "{out}"
        );
        assert!(
            status("TIMEOUT", "fixture_hang") && out.contains("after 2s"),
            "{out}"
        );
        assert!(
            status("SKIP", "fixture_ignored") && !out.contains("ignored fixture ran"),
            "{out}"
        );
    }

    /// A status line exactly as nextest prints it, `{status:>12} [{seconds:>8.3}s] <id> <case>`
    /// (a skip's bracket is blank), as its status and `<id> <case>`; anything else is refused.
    fn nextest_line(line: &str) -> Option<(&str, &str)> {
        let status = line.get(..12)?.trim_start();
        let rest = line.get(12..)?.strip_prefix(" [")?;
        let (duration, rest) = (rest.get(..9)?, rest.get(9..)?.strip_prefix("] ")?);
        let seconds = |text: &str| {
            let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
            text.strip_suffix('s')
                .and_then(|text| text.trim_start().split_once('.'))
                .is_some_and(|(whole, fraction)| {
                    !whole.is_empty() && digits(whole) && fraction.len() == 3 && digits(fraction)
                })
        };
        let timed = if status == "SKIP" {
            duration.bytes().all(|byte| byte == b' ')
        } else {
            seconds(duration)
        };
        (!status.is_empty() && status.bytes().all(|byte| byte.is_ascii_uppercase()) && timed)
            .then_some((status, rest))
    }

    #[test]
    fn status_lines_parse_only_as_nextest_prints_them() {
        let line = |bracket: &str| format!("{:>12} [{bracket}] id case", "PASS");
        assert_eq!(nextest_line(&line("   0.027s")), Some(("PASS", "id case")));
        for refused in ["invalid", "  0.027s ", "   0.02s", "   0.0275", "         "] {
            assert_eq!(nextest_line(&line(refused)), None, "[{refused}]");
        }
        let skip = format!("{:>12} [         ] id case", "SKIP");
        assert_eq!(nextest_line(&skip), Some(("SKIP", "id case")));
        assert_eq!(
            nextest_line("PASS [   0.027s] id case"),
            None,
            "unaligned status"
        );
    }

    #[test]
    fn filter_selects_cases() {
        let (failed, out) = report(&suite("filter"), "tests::fixture_pass");
        assert_eq!((failed, out.lines().count()), (0, 1), "{out}");
    }
}
