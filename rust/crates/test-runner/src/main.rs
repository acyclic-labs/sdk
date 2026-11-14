//! Runs every libtest case of one test executable in its own process under a time limit,
//! as cargo-nextest does, so a Bazel test action keeps nextest's per-test isolation.
//!
//! The Bazel `isolated_test` rule supplies, relative to the runfiles workspace directory:
//! `ACYCLIC_TEST_BINARY` (the libtest executable), `ACYCLIC_TEST_PACKAGE` (the Cargo
//! package directory, each case's working directory as under Cargo) and
//! `ACYCLIC_TEST_LIMIT_SECONDS` (the per-case limit).

use std::env;
use std::fs::{self, File};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use wait_timeout::ChildExt;

/// How one case ended.
enum Outcome {
    Passed,
    Failed(Option<i32>),
    TimedOut,
}

/// One test executable and the limits each of its cases runs under.
struct Suite {
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

fn setting(name: &str) -> Result<String, String> {
    env::var(name).map_err(|error| format!("{name}: {error}"))
}

fn run() -> Result<usize, String> {
    let root = env::current_dir().map_err(|error| format!("working directory: {error}"))?;
    let limit = setting("ACYCLIC_TEST_LIMIT_SECONDS")?
        .parse()
        .map_err(|error| format!("ACYCLIC_TEST_LIMIT_SECONDS: {error}"))?;
    let suite = Suite {
        binary: root.join(setting("ACYCLIC_TEST_BINARY")?),
        package: root.join(setting("ACYCLIC_TEST_PACKAGE")?),
        logs: PathBuf::from(setting("TEST_TMPDIR")?),
        limit: Duration::from_secs(limit),
    };
    let listing = Command::new(&suite.binary)
        .args(["--list", "--format", "terse"])
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
    let listing = String::from_utf8_lossy(&listing.stdout);
    let cases: Vec<&str> = listing
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .collect();
    let queue = Mutex::new(cases.iter().enumerate());
    let workers = thread::available_parallelism()
        .map_or(1, NonZero::get)
        .min(cases.len());
    let (sender, results) = mpsc::channel();
    thread::scope(|scope| {
        for _ in 0..workers {
            let (sender, queue, suite) = (sender.clone(), &queue, &suite);
            scope.spawn(move || {
                while let Some((index, name)) = queue.lock().ok().and_then(|mut queue| queue.next())
                {
                    let log = suite.logs.join(format!("case-{index}.log"));
                    let started = Instant::now();
                    let outcome = case(suite, name, &log);
                    if sender
                        .send((*name, outcome, started.elapsed(), log))
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
                    println!("PASS [{:>7.3}s] {name}", elapsed.as_secs_f64());
                    continue;
                }
                Ok(Outcome::Failed(code)) => format!("FAIL exit={code:?}"),
                Ok(Outcome::TimedOut) => format!("TIMEOUT after {}s", suite.limit.as_secs()),
                Err(error) => format!("ERROR {error}"),
            };
            failed += 1;
            println!("{verdict} [{:>7.3}s] {name}", elapsed.as_secs_f64());
            println!("{}", fs::read_to_string(&log).unwrap_or_default());
        }
        Ok(failed)
    })
}

/// Runs one case in its own process; a case past the limit is killed and fails.
fn case(suite: &Suite, name: &str, log: &Path) -> std::io::Result<Outcome> {
    let output = File::create(log)?;
    let mut child = Command::new(&suite.binary)
        .args([name, "--exact", "--nocapture", "--test-threads", "1"])
        .current_dir(&suite.package)
        .stdin(Stdio::null())
        .stderr(output.try_clone()?)
        .stdout(output)
        .spawn()?;
    Ok(match child.wait_timeout(suite.limit)? {
        Some(status) if status.success() => Outcome::Passed,
        Some(status) => Outcome::Failed(status.code()),
        None => {
            child.kill()?;
            child.wait()?;
            Outcome::TimedOut
        }
    })
}
