//! Runs every libtest case of one test executable in its own process under a time limit,
//! as cargo-nextest does, so a Bazel test action keeps nextest's per-test isolation.
//!
//! The Bazel `isolated_test` rule supplies, relative to the runfiles workspace directory:
//! `ACYCLIC_TEST_BINARY` (the libtest executable), `ACYCLIC_TEST_ID` (its nextest binary id),
//! `ACYCLIC_TEST_PACKAGE` (the Cargo package directory, each case's working directory as under
//! Cargo), `ACYCLIC_TEST_SOURCE` (this SDK's own repository in the runfiles, which a consuming
//! workspace sees as an external repository) and `ACYCLIC_TEST_LIMIT_SECONDS` (the per-case
//! limit). A nextest profile's cases
//! select through `ACYCLIC_TEST_ONLY` (run only cases whose name contains it) and
//! `ACYCLIC_TEST_EXCLUDE` (newline-separated; skip cases whose name contains any), as
//! nextest's `test(...)` default filters do; a profile whose cases nextest runs under a wrapper
//! script names it as `ACYCLIC_TEST_WRAPPER`. Within those, cases are selected as libtest
//! selects them, from Bazel's `--test_filter` (which arrives as `TESTBRIDGE_TEST_ONLY`) followed by
//! every `--test_arg`, as `cargo test -- <args>` selects them.
//!
//! Each outcome is reported as nextest reports it, `STATUS [ seconds] <binary id> <case>`, so a
//! case keeps one identity whichever runner ran it. Ignored cases are reported as skipped and
//! not run, as under Cargo, unless the arguments carry libtest's `--ignored`: then exactly the
//! selected ignored cases run, as `cargo test -- --ignored` runs them. Each case runs in an
//! owned process tree, so a case past its limit is stopped with every process it started.

use std::env;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// The SDK source tree the cases read checked-in SDK files from, given to them as
    /// `ACYCLIC_TEST_SOURCE_ROOT`: the SDK repository's directory in the runfiles, which is
    /// the workspace directory only when the SDK is the main workspace.
    source: PathBuf,
    /// Cargo's `CARGO_BIN_EXE_<name>` variables, as absolute paths under the runfiles root.
    executables: Vec<(String, PathBuf)>,
    binary: PathBuf,
    /// A nextest profile's run wrapper, if its cases run under one: each case runs as
    /// `<wrapper> <binary> <arguments>`, as nextest runs it.
    wrapper: Option<PathBuf>,
    /// The executable's nextest binary id: `<package>`, `<package>::<test>`, `<package>::bin/<name>`.
    id: String,
    /// The profile cases this target runs alone, if it is a profile's target.
    only: Option<String>,
    /// The profile cases the default target leaves to their own profile targets.
    exclude: Vec<String>,
    package: PathBuf,
    logs: PathBuf,
    limit: Duration,
}

impl Suite {
    /// Whether this target runs `name`: within its profile, and outside every profile it leaves.
    fn selects(&self, name: &str) -> bool {
        self.only
            .as_ref()
            .is_none_or(|only| name.contains(only.as_str()))
            && !self
                .exclude
                .iter()
                .any(|pattern| name.contains(pattern.as_str()))
    }
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
const SETTINGS: [&str; 9] = [
    "ACYCLIC_TEST_BINARY",
    "ACYCLIC_TEST_EXCLUDE",
    "ACYCLIC_TEST_ID",
    "ACYCLIC_TEST_LIMIT_SECONDS",
    "ACYCLIC_TEST_ONLY",
    "ACYCLIC_TEST_PACKAGE",
    "ACYCLIC_TEST_SOURCE",
    "ACYCLIC_TEST_WORKERS",
    "ACYCLIC_TEST_WRAPPER",
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
        source: root.join(setting("ACYCLIC_TEST_SOURCE")?),
        executables,
        binary: root.join(setting("ACYCLIC_TEST_BINARY")?),
        wrapper: env::var_os("ACYCLIC_TEST_WRAPPER").map(|path| root.join(path)),
        id: setting("ACYCLIC_TEST_ID")?,
        only: env::var("ACYCLIC_TEST_ONLY").ok(),
        exclude: env::var("ACYCLIC_TEST_EXCLUDE")
            .map(|patterns| patterns.lines().map(str::to_owned).collect())
            .unwrap_or_default(),
        package: root.join(setting("ACYCLIC_TEST_PACKAGE")?),
        logs: PathBuf::from(setting("TEST_TMPDIR")?),
        limit: Duration::from_secs(limit),
    };
    let selection: Vec<OsString> = env::var_os("TESTBRIDGE_TEST_ONLY")
        .into_iter()
        .chain(env::args_os().skip(1))
        .collect();
    run_suite(&suite, &selection, &mut io::stdout().lock())
}

/// The cases libtest's `selection` arguments select, with `--ignored` only the ignored ones.
fn list(suite: &Suite, selection: &[OsString], ignored: bool) -> Result<Vec<String>, String> {
    let mut command = Command::new(&suite.binary);
    command
        .args(["--list", "--format", "terse"])
        .args(ignored.then_some("--ignored"))
        .args(selection)
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
        .filter(|name| suite.selects(name))
        .map(str::to_owned)
        .collect())
}

/// Runs each case `selection` selects in its own process and reports every outcome to `out`.
/// Ignored cases are reported as skipped; with `--ignored` in `selection` only they run. Returns
/// how many cases did not pass.
fn run_suite(suite: &Suite, selection: &[OsString], out: &mut dyn Write) -> Result<usize, String> {
    let only_ignored = selection.iter().any(|argument| argument == "--ignored");
    let filters: Vec<OsString> = selection
        .iter()
        .filter(|argument| *argument != "--ignored")
        .cloned()
        .collect();
    let ignored = list(suite, &filters, true)?;
    let (cases, skipped) = if only_ignored {
        (ignored, Vec::new())
    } else {
        let cases = list(suite, &filters, false)?
            .into_iter()
            .filter(|name| !ignored.contains(name))
            .collect();
        (cases, ignored)
    };
    for name in &skipped {
        writeln!(out, "{:>12} [         ] {} {name}", "SKIP", suite.id)
            .map_err(|error| format!("report: {error}"))?;
    }
    if !selection.is_empty() && cases.is_empty() && skipped.is_empty() {
        return Err(format!("{selection:?} selects no case of {}", suite.id));
    }
    // Every case tree is held in custody, so a timeout stops descendants that left its process
    // group (setsid, nested trees) too.
    let custody = ProcessCustody::acquire(&suite.logs.join("processes"))
        .map_err(|error| format!("process custody: {error}"))?;
    let queue = Mutex::new(cases.iter().enumerate());
    let workers = env::var("ACYCLIC_TEST_WORKERS")
        .ok()
        .and_then(|workers| workers.parse().ok())
        .filter(|workers: &usize| *workers > 0)
        .unwrap_or_else(|| thread::available_parallelism().map_or(1, NonZero::get))
        .min(cases.len());
    let (sender, results) = mpsc::channel();
    thread::scope(|scope| {
        for _ in 0..workers {
            let (sender, queue, custody) = (sender.clone(), &queue, &custody);
            scope.spawn(move || {
                while let Some((index, name)) = queue.lock().ok().and_then(|mut queue| queue.next())
                {
                    let log = suite.logs.join(format!("case-{index}.log"));
                    let temporary = temporary_for(
                        &suite.logs,
                        std::process::id(),
                        CASES.fetch_add(1, Ordering::Relaxed),
                    );
                    let started = Instant::now();
                    let outcome = case(suite, custody, name, &log, &temporary);
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

/// Numbers every case this process runs, across all suites, so suites run at once (as `cargo
/// test` runs this crate's own tests) never share a temporary directory.
static CASES: AtomicUsize = AtomicUsize::new(0);

/// A case's own temporary directory, short by construction so a socket bound in it fits
/// `sun_path`: Bazel's test directory alone can exceed it. On Unix it sits under `/tmp`, which
/// the Linux sandbox makes private to the test, named by the runner's process and the case's
/// number in it. Windows keeps it under the test's directory.
fn temporary_for(logs: &Path, runner: u32, number: usize) -> PathBuf {
    let name = format!("acyclic-{runner:x}-{number:x}");
    if cfg!(unix) {
        Path::new("/tmp").join(name)
    } else {
        logs.join(name)
    }
}

/// Runs one case in its own custodied process tree. A case past the limit fails, and the
/// tree is stopped with every process the case started, including ones that left its group.
///
/// The case's temporary directory is its own (`temporary_for`), created empty and removed after
/// the case, and on Unix is named by its resolved path: Bazel gives a test no TMPDIR, and
/// macOS's `/tmp` is a symlink, which every case that checks its paths for links refuses. A
/// directory left by an earlier runner of the same process id is stale and is replaced.
/// Windows names it by its absolute path, never a resolved one: `canonicalize` there returns a
/// `\\?\` verbatim path, which programs a case starts (the vendored protoc) cannot open.
fn case(
    suite: &Suite,
    custody: &ProcessCustody,
    name: &str,
    log: &Path,
    temporary: &Path,
) -> io::Result<Outcome> {
    let output = File::create(log)?;
    match fs::remove_dir_all(temporary) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
        _ => fs::create_dir(temporary)?,
    }
    let named = if cfg!(windows) {
        std::path::absolute(temporary)?
    } else {
        temporary.canonicalize()?
    };
    let outcome = run_case(suite, custody, name, output, &named);
    fs::remove_dir_all(temporary)?;
    outcome
}

fn run_case(
    suite: &Suite,
    custody: &ProcessCustody,
    name: &str,
    output: File,
    temporary: &Path,
) -> io::Result<Outcome> {
    let mut command = ProcessCommand::new(suite.wrapper.as_ref().unwrap_or(&suite.binary));
    if suite.wrapper.is_some() {
        command.arg(&suite.binary);
    }
    command
        // Only selected cases reach here, so an ignored one runs because it was selected.
        .args([
            name,
            "--exact",
            "--include-ignored",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env("ACYCLIC_TEST_SOURCE_ROOT", &suite.source);
    for variable in ["TMPDIR", "TMP", "TEMP"] {
        command.env(variable, temporary);
    }
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

    /// The longest file name a case may bind as a Unix socket inside its temporary directory:
    /// `sun_path` holds 107 bytes on Linux (103 on macOS, whose root is also the longer).
    const SOCKET_NAME_ROOM: usize = 56;

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
    #[ignore = "the runner runs an ignored case only when its arguments carry --ignored"]
    fn fixture_ignored() {
        assert!(!acting(), "ignored fixture ran");
    }

    #[test]
    fn fixture_hang() {
        if acting() {
            thread::sleep(Duration::from_secs(3600));
        }
    }

    /// A case started by a runner whose own settings are set: it sees its source root,
    /// executables and own resolved temporary directory, and none of the runner's settings.
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
            let temporary = env::temp_dir();
            // The runner's process id is not the case's: the directory is checked by shape.
            let name = temporary.file_name().unwrap().to_str().unwrap();
            assert!(
                name.starts_with("acyclic-") && name.ends_with("-0"),
                "{} is not case 0's own directory",
                temporary.display()
            );
            if cfg!(windows) {
                assert!(
                    !temporary.as_os_str().to_string_lossy().starts_with(r"\\?\"),
                    "{} is a verbatim path",
                    temporary.display()
                );
                assert!(
                    temporary.is_absolute(),
                    "{} is relative",
                    temporary.display()
                );
            } else {
                assert_eq!(temporary.canonicalize().unwrap(), temporary, "unresolved");
            }
            assert_eq!(fs::read_dir(&temporary).unwrap().count(), 0, "not empty");
        }
    }

    /// The longest per-case directory (any runner, any case) leaves a socket name of
    /// `SOCKET_NAME_ROOM` bytes inside Linux's 107-byte `sun_path`.
    #[cfg(unix)]
    #[test]
    fn every_case_directory_leaves_room_for_a_socket() {
        let root = Path::new("/tmp").canonicalize().unwrap();
        let longest = temporary_for(Path::new("/unused"), u32::MAX, usize::MAX);
        let resolved = root.join(longest.file_name().unwrap());
        let length = resolved.as_os_str().len() + 1 + SOCKET_NAME_ROOM;
        assert!(
            length <= 107,
            "{} leaves {length} > 107",
            resolved.display()
        );
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
                wrapper: None,
                id: "fixture".into(),
                only: None,
                exclude: Vec::new(),
                package: root.clone(),
                logs: root,
                limit: Duration::from_secs(10),
            };
            let mut out = Vec::new();
            let failed = run_suite(
                &suite,
                &["tests::environment_case".into(), "--exact".into()],
                &mut out,
            )
            .unwrap();
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
            wrapper: None,
            id: "acyclic-test-runner".into(),
            only: None,
            exclude: Vec::new(),
            package: dir.clone(),
            logs: dir,
            limit: Duration::from_secs(2),
        }
    }

    fn report(suite: &Suite, selection: &[&str]) -> (usize, String) {
        let selection: Vec<OsString> = selection.iter().map(OsString::from).collect();
        let mut out = Vec::new();
        let failed = run_suite(suite, &selection, &mut out).unwrap();
        fs::remove_dir_all(&suite.package).unwrap();
        (failed, String::from_utf8(out).unwrap())
    }

    #[test]
    fn reports_each_outcome_and_counts_failures() {
        let (failed, out) = report(&suite("outcomes"), &["tests::fixture_"]);
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

    /// A wrapped profile's case runs as `<wrapper> <binary> <arguments>`: a wrapper that passes
    /// them through keeps the case's own outcome, and one that runs nothing decides it instead.
    #[cfg(unix)]
    #[test]
    fn a_wrapper_runs_each_case_with_the_binary_first() {
        use std::os::unix::fs::PermissionsExt;
        for (name, body, failed) in [("through", "exec \"$@\"", 1), ("instead", "exit 0", 0)] {
            let mut wrapped = suite(&format!("wrapper-{name}"));
            let wrapper = wrapped.package.join("wrapper.sh");
            let binary = wrapped.binary.display().to_string();
            fs::write(
                &wrapper,
                format!("#!/bin/sh\n[ \"$1\" = '{binary}' ] || exit 9\n{body}\n"),
            )
            .unwrap();
            fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
            wrapped.wrapper = Some(wrapper);
            let (count, out) = report(&wrapped, &["tests::fixture_fail"]);
            assert_eq!(count, failed, "{name}: {out}");
        }
    }

    /// A profile's target runs only its cases; the default target runs every other case.
    #[test]
    fn profiles_split_cases_between_their_targets() {
        let mut profile = suite("profile");
        profile.only = Some("fixture_pa".into());
        let (failed, out) = report(&profile, &["tests::fixture_"]);
        assert_eq!((failed, out.lines().count()), (0, 1), "{out}");
        assert!(out.ends_with("tests::fixture_pass\n"), "{out}");
        let mut default = suite("default");
        default.exclude = vec!["fixture_pa".into(), "fixture_hang".into()];
        let (failed, out) = report(&default, &["tests::fixture_"]);
        assert_eq!(failed, 1, "{out}");
        assert!(
            !out.contains("fixture_pass") && !out.contains("fixture_hang"),
            "{out}"
        );
    }

    /// The filter and every argument after it select as libtest does: `--skip` here leaves only
    /// the passing and ignored fixtures of the four `tests::fixture_` cases.
    #[test]
    fn libtest_arguments_select_cases() {
        let (failed, out) = report(
            &suite("selection"),
            &[
                "tests::fixture_",
                "--skip",
                "fixture_hang",
                "--skip",
                "fixture_fail",
            ],
        );
        assert_eq!((failed, out.lines().count()), (0, 2), "{out}");
    }

    /// `--ignored` runs exactly the selected ignored cases, and no other, as
    /// `cargo test -- --ignored` does; an ignored selection with no case is refused.
    #[test]
    fn ignored_argument_runs_only_ignored_cases() {
        let (failed, out) = report(&suite("ignored"), &["tests::fixture_", "--ignored"]);
        assert_eq!(failed, 1, "{out}");
        assert!(
            !out.contains("fixture_pass") && !out.contains("SKIP"),
            "{out}"
        );
        assert!(
            out.starts_with(&format!("{:>12}", "FAIL"))
                && out.contains("acyclic-test-runner tests::fixture_ignored")
                && out.contains("ignored fixture ran"),
            "{out}"
        );
        let suite = suite("ignored-unmatched");
        let refused = run_suite(
            &suite,
            &["fixture_pass".into(), "--ignored".into()],
            &mut Vec::new(),
        );
        fs::remove_dir_all(&suite.package).unwrap();
        assert_eq!(
            refused,
            Err(r#"["fixture_pass", "--ignored"] selects no case of acyclic-test-runner"#.into())
        );
    }

    /// A selection that names no case fails the run instead of passing with nothing run.
    #[test]
    fn a_selection_matching_no_case_is_refused() {
        let suite = suite("unmatched");
        let refused = run_suite(&suite, &["tests::no_such_case".into()], &mut Vec::new());
        fs::remove_dir_all(&suite.package).unwrap();
        assert_eq!(
            refused,
            Err(r#"["tests::no_such_case"] selects no case of acyclic-test-runner"#.into())
        );
    }
}
