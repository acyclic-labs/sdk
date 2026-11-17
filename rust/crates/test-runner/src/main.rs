//! Runs every libtest case of one test executable in its own process under a time limit,
//! as cargo-nextest does, so a Bazel test action keeps nextest's per-test isolation.
//!
//! The Bazel `isolated_test` rule supplies, relative to the runfiles workspace directory:
//! `ACYCLIC_TEST_BINARY` (the libtest executable), `ACYCLIC_TEST_ID` (its nextest binary id),
//! `ACYCLIC_TEST_PACKAGE` (the Cargo package directory, each case's working directory as under
//! Cargo), `ACYCLIC_TEST_SOURCE` (this SDK's own repository in the runfiles, which a consuming
//! workspace sees as an external repository), `ACYCLIC_TEST_LIMIT_SECONDS` (the per-case
//! limit) and `ACYCLIC_TEST_CPUS` (the CPUs the test reserved from Bazel as its `cpu:N` tag, and
//! so the most cases it runs at once: never the host's count, which every test Bazel runs
//! together would claim at once). `ACYCLIC_TEST_TEMP_ROOT` selects `memory`, or `disk` for a
//! target tagged `requires-disk-tmp`; see `temporary_root`. A nextest profile's cases select
//! through `ACYCLIC_TEST_ONLY` (run only cases whose name contains it) and
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
use std::hash::BuildHasher as _;
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
    /// This runner's own directory, where each case's temporary directory is made.
    temporary: RunnerDirectory,
    limit: Duration,
    /// The CPUs the test reserved, and so the most cases it runs at once.
    cpus: NonZero<usize>,
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
const SETTINGS: [&str; 10] = [
    "ACYCLIC_TEST_BINARY",
    "ACYCLIC_TEST_CPUS",
    "ACYCLIC_TEST_EXCLUDE",
    "ACYCLIC_TEST_ID",
    "ACYCLIC_TEST_LIMIT_SECONDS",
    "ACYCLIC_TEST_ONLY",
    "ACYCLIC_TEST_PACKAGE",
    "ACYCLIC_TEST_SOURCE",
    "ACYCLIC_TEST_TEMP_ROOT",
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
    let logs = PathBuf::from(setting("TEST_TMPDIR")?);
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
        temporary: RunnerDirectory::claim(&temporary_root(
            &setting("ACYCLIC_TEST_TEMP_ROOT")?,
            Path::new(MEMORY_ROOT),
            &logs,
        )?)?,
        logs,
        limit: Duration::from_secs(limit),
        cpus: setting("ACYCLIC_TEST_CPUS")?
            .parse()
            .map_err(|error| format!("ACYCLIC_TEST_CPUS: {error}"))?,
    };
    let selection: Vec<OsString> = env::var_os("TESTBRIDGE_TEST_ONLY")
        .into_iter()
        .chain(env::args_os().skip(1))
        .collect();
    let failed = run_suite(&suite, &selection, &mut io::stdout().lock())?;
    suite.temporary.release()?;
    Ok(failed)
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
        .unwrap_or_else(|| suite.cpus.get())
        .min(suite.cpus.get())
        .min(cases.len());
    let (sender, results) = mpsc::channel();
    thread::scope(|scope| {
        for _ in 0..workers {
            let (sender, queue, custody) = (sender.clone(), &queue, &custody);
            scope.spawn(move || {
                while let Some((index, name)) = queue.lock().ok().and_then(|mut queue| queue.next())
                {
                    let log = suite.logs.join(format!("case-{index}.log"));
                    let temporary =
                        temporary_for(&suite.temporary.path, CASES.fetch_add(1, Ordering::Relaxed));
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

/// Linux's RAM-backed directory: a case's durability syncs there cost no device flush, so a
/// test's wall time does not follow the load on the host's disk. The product still syncs.
const MEMORY_ROOT: &str = "/dev/shm";

/// The root of every case's temporary directory, by the rule's `ACYCLIC_TEST_TEMP_ROOT`. On Linux
/// `memory` is `memory_root`, which must exist: there is no silent fallback to a disk. A target
/// that needs a real disk's semantics (`O_DIRECT`, a block device behind `statfs`) is tagged
/// `requires-disk-tmp` and gets `disk`. Elsewhere, and for `disk`, the root is `/tmp` on Unix
/// (short, so a socket path fits) and the test's own directory on Windows.
fn temporary_root(kind: &str, memory_root: &Path, logs: &Path) -> Result<PathBuf, String> {
    let memory = match kind {
        "memory" => true,
        "disk" => false,
        other => {
            return Err(format!(
                "ACYCLIC_TEST_TEMP_ROOT: {other:?} is neither \"memory\" nor \"disk\""
            ));
        }
    };
    if cfg!(target_os = "linux") && memory {
        if !memory_root.is_dir() {
            return Err(format!(
                "{} is not a directory, so cases cannot have RAM-backed temporary directories; \
                 recovery: mount it, or tag a target that needs a real disk requires-disk-tmp",
                memory_root.display()
            ));
        }
        let mounts = fs::read_to_string("/proc/self/mountinfo")
            .map_err(|error| format!("/proc/self/mountinfo: {error}"))?;
        if !executes(memory_root, &mounts) {
            return Err(format!(
                "{} is mounted noexec, so a case cannot run a script it writes there; \
                 recovery: remount it exec, or tag a target that needs a real disk requires-disk-tmp",
                memory_root.display()
            ));
        }
        return Ok(memory_root.to_path_buf());
    }
    Ok(if cfg!(unix) {
        PathBuf::from("/tmp")
    } else {
        logs.to_path_buf()
    })
}

/// Whether the mount holding `path` lets a case execute files, by its innermost entry in
/// `mountinfo` (the format of `/proc/self/mountinfo`: the mount point is the fifth field and its
/// options the sixth). Cases write and run scripts in their temporary directories.
fn executes(path: &Path, mountinfo: &str) -> bool {
    mountinfo
        .lines()
        .filter_map(|line| {
            let mut fields = line.split(' ').skip(4);
            Some((Path::new(fields.next()?), fields.next()?))
        })
        .filter(|(point, _)| path.starts_with(point))
        .max_by_key(|(point, _)| point.as_os_str().len())
        .is_some_and(|(_, options)| !options.split(',').any(|option| option == "noexec"))
}

/// The prefix of a runner's directory, followed by exactly eight lowercase hex digits.
const RUNNER_PREFIX: &str = "acyclic-";

/// A runner's own directory under the shared temporary root, holding its cases' directories.
///
/// A root such as `/dev/shm` or `/tmp` outlives every sandbox, and Bazel may kill a runner before
/// it removes what its cases left. So on Unix the runner holds an exclusive `flock` on its
/// directory for its whole life, which the kernel releases however the process ends, and every
/// new runner reclaims each runner directory of its own user whose lock it can take: that runner
/// is gone. The directory is created owner-only under a random name, then locked; a reclaimer
/// that takes it before its owner locks it removes it, and the owner sees the unlinked inode and
/// retries. Another user's directories are left alone.
struct RunnerDirectory {
    path: PathBuf,
    #[cfg(unix)]
    _lock: File,
}

fn failed(action: &str, path: &Path, error: io::Error) -> String {
    format!("{action} {}: {error}", path.display())
}

impl RunnerDirectory {
    fn claim(root: &Path) -> Result<Self, String> {
        // Abandoned directories go before this runner needs a new one: they may hold the very
        // space or inodes its creation needs.
        #[cfg(unix)]
        reclaim(root, acyclic_native_runtime::private_file::effective_user())?;
        let mut attempt = 0_u64;
        loop {
            let name = std::collections::hash_map::RandomState::new()
                .hash_one((std::process::id(), attempt))
                .to_le_bytes();
            let name = u32::from_le_bytes([name[0], name[1], name[2], name[3]]);
            let path = root.join(format!("{RUNNER_PREFIX}{name:08x}"));
            let directory = &mut fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                directory.mode(0o700);
            }
            match directory.create(&path) {
                Ok(()) => {}
                // A live runner already has this name; take another.
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    attempt += 1;
                    continue;
                }
                Err(error) => return Err(failed("create", &path, error)),
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                let lock = match File::open(&path) {
                    Ok(lock) => lock,
                    // A reclaimer took the still-unlocked directory first.
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        attempt += 1;
                        continue;
                    }
                    Err(error) => return Err(failed("open", &path, error)),
                };
                lock.lock().map_err(|error| failed("lock", &path, error))?;
                let held = lock
                    .metadata()
                    .map_err(|error| failed("inspect", &path, error))?;
                match fs::symlink_metadata(&path) {
                    Ok(visible) if (visible.dev(), visible.ino()) == (held.dev(), held.ino()) => {
                        return Ok(Self { path, _lock: lock });
                    }
                    // The directory this runner locked is no longer the one at its path.
                    Ok(_) => {
                        attempt += 1;
                        continue;
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        attempt += 1;
                        continue;
                    }
                    Err(error) => return Err(failed("inspect", &path, error)),
                }
            }
            #[cfg(not(unix))]
            return Ok(Self { path });
        }
    }

    /// Removes the directory once every case has ended.
    fn release(&self) -> Result<(), String> {
        remove(&self.path)
    }
}

/// Removes a runner directory and everything in it. A case may leave a directory it made
/// unwritable; the runner owns the whole tree, so it restores its own access to each directory
/// first, and no leftover outlives its runner's reclaim.
fn remove(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    restore_access(path)?;
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        // A reclaimer may have removed the same directory first.
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(failed("remove", path, error)),
    }
}

/// Gives the owner full access to `path` and every directory under it, following no link.
#[cfg(unix)]
fn restore_access(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt as _;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(failed("inspect", path, error)),
    };
    if !metadata.is_dir() {
        return Ok(());
    }
    if metadata.permissions().mode() & 0o700 != 0o700 {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| failed("restore access to", path, error))?;
    }
    for entry in fs::read_dir(path).map_err(|error| failed("list", path, error))? {
        restore_access(&entry.map_err(|error| failed("list", path, error))?.path())?;
    }
    Ok(())
}

/// Removes every runner directory of `user` under `root` whose lock is free.
#[cfg(unix)]
fn reclaim(root: &Path, user: u32) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt as _;
    for entry in fs::read_dir(root).map_err(|error| failed("list", root, error))? {
        let entry = entry.map_err(|error| failed("list", root, error))?;
        let path = entry.path();
        if !entry.file_name().to_str().is_some_and(is_runner_name) {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(failed("inspect", &path, error)),
        };
        if !metadata.is_dir() || metadata.uid() != user {
            continue;
        }
        let abandoned = match File::open(&path) {
            Ok(abandoned) => abandoned,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(failed("open", &path, error)),
        };
        match abandoned.try_lock() {
            Ok(()) => remove(&path)?,
            Err(fs::TryLockError::WouldBlock) => {}
            Err(fs::TryLockError::Error(error)) => return Err(failed("lock", &path, error)),
        }
    }
    Ok(())
}

/// Whether `name` is a runner directory's: the prefix and exactly eight lowercase hex digits.
fn is_runner_name(name: &str) -> bool {
    name.strip_prefix(RUNNER_PREFIX).is_some_and(|digits| {
        digits.len() == 8
            && digits
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// A case's own temporary directory in its runner's directory, short by construction so a socket
/// bound in it fits `sun_path`: Bazel's test directory alone can exceed it.
fn temporary_for(runner: &Path, number: usize) -> PathBuf {
    runner.join(format!("{number:x}"))
}

/// Runs one case in its own custodied process tree. A case past the limit fails, and the
/// tree is stopped with every process the case started, including ones that left its group.
///
/// The case's temporary directory is its own (`temporary_for`), created new and empty and removed
/// after the case, and on Unix is named by its resolved path: Bazel gives a test no TMPDIR, and
/// macOS's `/tmp` is a symlink, which every case that checks its paths for links refuses.
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
    fs::create_dir(temporary)?;
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

    /// The longest file name a case may bind as a Unix socket inside its temporary directory.
    const SOCKET_NAME_ROOM: usize = 56;

    /// The longest path `sun_path` holds, without its terminating NUL.
    const SUN_PATH: usize = if cfg!(target_os = "macos") { 103 } else { 107 };

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

    /// The two ends of a FIFO named `meet`, which only `cases_run_at_once_up_to_the_reservation`
    /// creates: opening either end blocks until the other is open, so both pass only when the
    /// runner runs them at once.
    #[cfg(unix)]
    #[test]
    fn rendezvous_reader() {
        if acting() && Path::new("meet").exists() {
            File::open("meet").unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn rendezvous_writer() {
        if acting() && Path::new("meet").exists() {
            fs::OpenOptions::new().write(true).open("meet").unwrap();
        }
    }

    /// A test runs as many cases at once as it reserved CPUs, and never more: the rendezvous
    /// meets under a reservation of two, and under one each end waits out its limit alone.
    #[cfg(unix)]
    #[test]
    fn cases_run_at_once_up_to_the_reservation() {
        for (cpus, failed) in [(2, 0), (1, 2)] {
            let mut reserved = suite(&format!("reservation-{cpus}"));
            reserved.cpus = NonZero::new(cpus).unwrap();
            reserved.limit = Duration::from_secs(1);
            let fifo = Command::new("mkfifo")
                .arg(reserved.package.join("meet"))
                .status()
                .unwrap();
            assert!(fifo.success());
            let (count, out) = report(&reserved, &["tests::rendezvous_"]);
            assert_eq!(count, failed, "{cpus} CPU(s): {out}");
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
            // The runner's name is random: the directory is checked by shape.
            let runner = temporary.parent().and_then(Path::file_name);
            assert!(
                temporary.file_name() == Some("0".as_ref())
                    && runner
                        .and_then(|name| name.to_str())
                        .is_some_and(is_runner_name),
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
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let permissions = fs::metadata(temporary.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode();
                assert_eq!(permissions & 0o077, 0, "another user can open the runner");
            }
        }
    }

    /// The longest per-case directory (any runner, any case) leaves a socket name of
    /// `SOCKET_NAME_ROOM` bytes inside this platform's `sun_path`.
    #[cfg(unix)]
    #[test]
    fn every_case_directory_leaves_room_for_a_socket() {
        for kind in ["memory", "disk"] {
            let root = temporary_root(kind, Path::new(MEMORY_ROOT), Path::new("/unused"))
                .unwrap()
                .canonicalize()
                .unwrap();
            let runner = root.join(format!("{RUNNER_PREFIX}{:08x}", u32::MAX));
            assert!(is_runner_name(&format!("{RUNNER_PREFIX}{:08x}", u32::MAX)));
            let longest = temporary_for(&runner, usize::MAX);
            let length = longest.as_os_str().len() + 1 + SOCKET_NAME_ROOM;
            assert!(
                length <= SUN_PATH,
                "{} leaves {length} > {SUN_PATH}",
                longest.display()
            );
        }
    }

    /// A runner reclaims the directory of a runner that is gone, even one its cases left unwritable,
    /// keeps a live runner's, and never touches another program's entry of a similar name.
    #[cfg(unix)]
    #[test]
    fn a_runner_reclaims_only_directories_whose_runner_is_gone() {
        let root = env::temp_dir().join(format!("acyclic-reclaim-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let live = RunnerDirectory::claim(&root).unwrap();
        let gone = RunnerDirectory::claim(&root).unwrap();
        fs::write(gone.path.join("left-behind"), "").unwrap();
        // A case may end with a directory it made unwritable, its entries still in it.
        let sealed = gone.path.join("sealed");
        fs::create_dir(&sealed).unwrap();
        fs::write(sealed.join("entry"), "").unwrap();
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&sealed, fs::Permissions::from_mode(0o555)).unwrap();
        }
        let gone_path = gone.path.clone();
        drop(gone);
        let other = root.join(format!("{RUNNER_PREFIX}0000000g"));
        fs::create_dir(&other).unwrap();
        let next = RunnerDirectory::claim(&root).unwrap();
        assert!(
            !gone_path.exists(),
            "{} was not reclaimed",
            gone_path.display()
        );
        assert!(live.path.is_dir() && next.path.is_dir() && other.is_dir());
        live.release().unwrap();
        next.release().unwrap();
        fs::remove_dir_all(&root).unwrap();
    }

    /// A pre-created entry at the old predictable lock path in the shared root is ignored: the
    /// lock lives in a directory only this user can write, so another user cannot block a runner
    /// by claiming a name there.
    #[cfg(unix)]
    #[test]
    fn a_foreign_entry_at_the_old_lock_path_does_not_block_a_runner() {
        let root = env::temp_dir().join(format!("acyclic-lock-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let user = acyclic_native_runtime::private_file::effective_user();
        let old = root.join(format!("{RUNNER_PREFIX}{user}.lock"));
        // A directory is not a file of the user, so the old lock would refuse it.
        fs::create_dir(&old).unwrap();
        let runner = RunnerDirectory::claim(&root).unwrap();
        assert!(runner.path.is_dir());
        runner.release().unwrap();
        fs::remove_dir_all(&root).unwrap();
    }

    /// A lock file in the process's `HOME` does not block a runner: the lock is the runner's own
    /// directory in the shared root, so jobs with different `HOME` values share one lock.
    #[cfg(unix)]
    #[test]
    fn a_home_lock_file_does_not_block_a_runner() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = env::temp_dir().join(format!("acyclic-home-root-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let home = env::temp_dir().join(format!("acyclic-home-{}", std::process::id()));
        fs::create_dir(&home).unwrap();
        let lock_directory = home.join(".acyclic-test-runner");
        fs::create_dir(&lock_directory).unwrap();
        fs::set_permissions(&lock_directory, fs::Permissions::from_mode(0o700)).unwrap();
        let user = acyclic_native_runtime::private_file::effective_user();
        // A directory at the lock path is not a file the old lock code would accept.
        fs::create_dir(lock_directory.join(format!("{RUNNER_PREFIX}{user}.lock"))).unwrap();
        let output = Command::new(env::current_exe().unwrap())
            .args(["tests::home_lock_runner", "--exact", "--nocapture"])
            .env("ACYCLIC_HOME_LOCK_ROOT", &root)
            .env("HOME", &home)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        fs::remove_dir_all(&root).unwrap();
        fs::remove_dir_all(&home).unwrap();
    }

    /// Acts as a runner whose `HOME` holds a lock file the old lock code would refuse.
    #[cfg(unix)]
    #[test]
    fn home_lock_runner() {
        if let Some(root) = env::var_os("ACYCLIC_HOME_LOCK_ROOT") {
            let runner = RunnerDirectory::claim(Path::new(&root)).unwrap();
            assert!(runner.path.is_dir());
            runner.release().unwrap();
        }
    }

    /// The innermost mount decides, and only its own `noexec` refuses execution.
    #[test]
    fn a_noexec_mount_is_refused_by_its_innermost_entry() {
        let mounts = concat!(
            "22 1 0:21 / / rw,relatime shared:1 - ext4 /dev/root rw\n",
            "25 22 0:23 / /dev/shm rw,nosuid,nodev,noexec shared:3 - tmpfs tmpfs rw\n",
        );
        assert!(!executes(Path::new("/dev/shm"), mounts));
        assert!(executes(Path::new("/tmp"), mounts));
        let remounted =
            format!("{mounts}26 22 0:24 / /dev/shm rw,nosuid shared:4 - tmpfs tmpfs rw\n");
        assert!(executes(Path::new("/dev/shm"), &remounted));
    }

    /// Cases get RAM-backed directories on Linux and a disk only when the target declares one;
    /// a missing RAM root or an unknown kind refuses the suite instead of falling back.
    #[test]
    fn the_temporary_root_is_memory_on_linux_unless_a_disk_is_declared() {
        let logs = Path::new("/unused");
        let memory = temporary_root("memory", Path::new(MEMORY_ROOT), logs).unwrap();
        let disk = temporary_root("disk", Path::new(MEMORY_ROOT), logs).unwrap();
        if cfg!(target_os = "linux") {
            assert_eq!(memory, Path::new(MEMORY_ROOT));
            let missing = env::temp_dir().join(format!("acyclic-no-root-{}", std::process::id()));
            let refused = temporary_root("memory", &missing, logs).unwrap_err();
            assert!(refused.contains("requires-disk-tmp"), "{refused}");
        } else {
            assert_eq!(memory, disk);
        }
        assert_ne!(disk, Path::new(MEMORY_ROOT));
        assert!(temporary_root("ram", Path::new(MEMORY_ROOT), logs).is_err());
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
                temporary: RunnerDirectory::claim(
                    &temporary_root("memory", Path::new(MEMORY_ROOT), &root).unwrap(),
                )
                .unwrap(),
                logs: root,
                limit: Duration::from_secs(10),
                cpus: NonZero::<usize>::MIN,
            };
            let mut out = Vec::new();
            let failed = run_suite(
                &suite,
                &["tests::environment_case".into(), "--exact".into()],
                &mut out,
            )
            .unwrap();
            suite.temporary.release().unwrap();
            let out = String::from_utf8(out).unwrap();
            assert_eq!((failed, out.lines().count()), (0, 1), "{out}");
        }
    }

    #[test]
    fn cases_see_their_inputs_and_none_of_the_runner_settings() {
        let environment = suite("environment");
        environment.temporary.release().unwrap();
        let dir = environment.package;
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
            temporary: RunnerDirectory::claim(
                &temporary_root("memory", Path::new(MEMORY_ROOT), &dir).unwrap(),
            )
            .unwrap(),
            logs: dir,
            limit: Duration::from_secs(2),
            cpus: NonZero::new(2).unwrap(),
        }
    }

    fn report(suite: &Suite, selection: &[&str]) -> (usize, String) {
        let selection: Vec<OsString> = selection.iter().map(OsString::from).collect();
        let mut out = Vec::new();
        let failed = run_suite(suite, &selection, &mut out).unwrap();
        suite.temporary.release().unwrap();
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
        suite.temporary.release().unwrap();
        fs::remove_dir_all(&suite.package).unwrap();
        assert_eq!(
            refused,
            Err(r#"["tests::no_such_case"] selects no case of acyclic-test-runner"#.into())
        );
    }
}
