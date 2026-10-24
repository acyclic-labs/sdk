//! A hook or stdio MCP server that the host starts must not hold the host's
//! own output: a reader of that output sees its end when the host closes
//! it, even while the started process still runs.
//!
//! The test runs an owner (this test executable) whose output is a pipe
//! the test reads. The owner starts a parked child through the entry point,
//! closes its output, and waits; the parked child holds an exclusive lock
//! while it runs. The negative control starts the same child through
//! `std`, which hands it every inheritable handle, the owner's output among
//! them.
//!
//! The owner and the parked child are each tethered to the test: they
//! report over a pipe the test waits on, so a failure or an early end is
//! seen at once, and they end when the test releases them, however it ends.

use super::native::run_hook;
use super::{HookFailure, HookRun, HookSpec};
use crate::native_environment::variable;
use acyclic_native_runtime::command::{Child, Command, Stdio};
use acyclic_native_runtime::testing::{Tether, TetherEvent, Tethered, close_standard_output};
use acyclic_native_runtime::{FileLock, FileLockMode};
use std::fs::File;
use std::io::Read as _;
use std::os::windows::io::AsHandle as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const MODE: &str = "ACYCLIC_RELEASE_TEST_MODE";
const ROOT: &str = "ACYCLIC_RELEASE_TEST_ROOT";
/// The tether the parked child reports over and waits on.
const PARK: &str = "ACYCLIC_RELEASE_TEST_PARK";
/// The tether the owner reports over and waits on.
const REPORTS: &str = "ACYCLIC_RELEASE_TEST_REPORTS";
const OWNER: &str = "hooks::release_tests::release_owner";
const PARKED: &str = "hooks::release_tests::release_parked_child";
/// How long one release test may take before it fails, as a backstop:
/// every wait in it ends at once on the event it waits for or on a
/// helper's end.
const BACKSTOP: Duration = Duration::from_secs(60);

fn exe() -> PathBuf {
    std::env::current_exe().expect("test executable")
}

fn lock_path(root: &Path) -> PathBuf {
    root.join("parked.lock")
}

/// The parked child's arguments: this executable, running only
/// [`release_parked_child`].
fn parked_arguments() -> [&'static str; 3] {
    ["--exact", PARKED, "--nocapture"]
}

/// Serves the parked child: holds the lock, reports its PID, and runs
/// until the test releases it.
#[test]
fn release_parked_child() {
    let Some(tether) = variable(PARK) else {
        return;
    };
    let tether = Tethered::connect(&tether).expect("tether");
    let root = PathBuf::from(variable(ROOT).expect("root"));
    let lock = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path(&root))
        .expect("lock file");
    let _held = FileLock::acquire(lock, FileLockMode::Exclusive).expect("hold lock");
    tether
        .send(&std::process::id().to_string())
        .expect("report");
    tether.released();
    std::process::exit(0);
}

/// Serves the owner: starts the parked child as its mode says and, once
/// the test has seen the child run, closes its own output. Reports the
/// child's end, or its own failure, over its tether at once.
#[test]
fn release_owner() {
    let Some(mode) = variable(MODE) else {
        return;
    };
    let mode = mode.into_string().expect("UTF-8 mode");
    let reports =
        Arc::new(Tethered::connect(&variable(REPORTS).expect("reports")).expect("reports"));
    let reporter = Arc::clone(&reports);
    std::panic::set_hook(Box::new(move |panic| {
        let _ = reporter.send(&format!("panicked: {panic}"));
    }));
    let root = PathBuf::from(variable(ROOT).expect("owner root"));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (ended, end) = tokio::sync::oneshot::channel::<String>();
    match mode.as_str() {
        "hook" => {
            let hook = Arc::new(HookSpec {
                run: HookRun::Executable {
                    executable: exe().to_str().expect("UTF-8 executable").to_owned(),
                    argv: parked_arguments().map(str::to_owned).to_vec(),
                },
                timeout_ms: 600_000,
                failure: HookFailure::Report,
            });
            let request = Arc::new((root.clone(), root, b"{}".to_vec()));
            runtime.spawn(async move {
                let run = run_hook(hook, request, Arc::new(|| Ok(())), 65_536).await;
                let _ = ended.send(format!("{:?}", run.map(drop)));
            });
        }
        #[cfg(feature = "mcp-stdio")]
        "mcp" => {
            // The test harness reports on its output, which an MCP client
            // reads as protocol; the server's own child writes elsewhere.
            let mut command = Command::new("cmd.exe");
            command.raw_arg(format!(
                "/d /c \"\"{}\" {} > NUL\"",
                exe().display(),
                parked_arguments().join(" ")
            ));
            runtime.spawn(async move {
                let started =
                    crate::mcp::stdio::StdioMcpTransport::spawn(command, 65_536, 600_000).await;
                let _ = ended.send(format!("{:?}", started.map(drop)));
            });
        }
        "control" => {
            let mut child = std::process::Command::new(exe())
                .args(parked_arguments())
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("std child");
            thread::spawn(move || {
                let _ = ended.send(format!("{:?}", child.wait()));
            });
        }
        other => panic!("unknown mode {other}"),
    }
    let (went, go) = tokio::sync::oneshot::channel();
    let receiver = Arc::clone(&reports);
    thread::spawn(move || {
        let _ = went.send(receiver.receive());
    });
    let next = runtime.block_on(async {
        tokio::select! {
            ended = end => Err(ended),
            go = go => Ok(go),
        }
    });
    match next {
        Ok(Ok(Ok(Some(go)))) if go == "go" => {}
        Err(ended) => {
            let _ = reports.send(&format!("the child ended: {ended:?}"));
            std::process::exit(1);
        }
        Ok(other) => panic!("expected go, received {other:?}"),
    }
    close_standard_output().expect("close output");
    reports.send("ready").expect("ready");
    reports.released();
    std::process::exit(0);
}

/// Whether the parked child still holds its lock.
fn held(root: &Path) -> bool {
    let file = File::open(lock_path(root)).expect("lock file");
    matches!(
        FileLock::try_acquire(file, FileLockMode::Exclusive),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    )
}

/// One owner run. The owner is killed and reaped however the test ends;
/// the tether the parked child waits on releases the child when dropped.
struct Run {
    owner: Child,
    parked: Tether,
    deadline: Instant,
    root: tempfile::TempDir,
}

impl Drop for Run {
    fn drop(&mut self) {
        let _ = self.owner.kill();
        let _ = self.owner.wait();
    }
}

/// Starts an owner in `mode` and reports whether its output ended within
/// `bound` after it closed it, and whether the parked child still ran when
/// the result was read. Each wait ends on the event it waits for, or at
/// once when the owner or the child fails.
fn output_ends_while_the_child_runs(mode: &str, bound: Duration) -> (bool, bool, Run) {
    let deadline = Instant::now() + BACKSTOP;
    let root = tempfile::tempdir().expect("root");
    let mut reports = Tether::new().expect("reports");
    let parked = Tether::new().expect("parked");
    let mut command = Command::new(exe());
    command
        .args(["--exact", OWNER, "--nocapture"])
        .env(MODE, mode)
        .env(ROOT, root.path())
        .env(REPORTS, reports.name())
        .env(PARK, parked.name())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let owner = command.spawn().expect("owner");
    let mut run = Run {
        owner,
        parked,
        deadline,
        root,
    };
    let mut output = run.owner.stdout.take().expect("owner output");
    let (ended, end) = mpsc::channel();
    thread::spawn(move || {
        let _ = ended.send(output.read_to_end(&mut Vec::new()).map(|_| ()));
    });
    let started = Tether::wait(
        &mut [&mut run.parked, &mut reports],
        &[run.owner.as_handle()],
        deadline,
    );
    assert!(
        matches!(started, Ok(TetherEvent::Line(0, _))),
        "the {mode} child did not start: {started:?}"
    );
    reports.send("go").expect("go");
    let ready = Tether::wait(
        &mut [&mut reports, &mut run.parked],
        &[run.owner.as_handle()],
        deadline,
    );
    assert_eq!(
        ready.ok(),
        Some(TetherEvent::Line(0, "ready".to_owned())),
        "the owner did not close its output"
    );
    let reached = end.recv_timeout(bound.min(deadline.saturating_duration_since(Instant::now())));
    let running = held(run.root.path());
    (matches!(reached, Ok(Ok(()))), running, run)
}

/// Asserts the parked child dies with its owner: its end of the tether
/// closes once the owner is killed.
fn dies_with_its_owner(mut run: Run) {
    run.owner.kill().expect("kill owner");
    run.owner.wait().expect("reap owner");
    let deadline = run.deadline;
    let end = Tether::wait(&mut [&mut run.parked], &[], deadline);
    assert_eq!(
        end.ok(),
        Some(TetherEvent::Closed(0)),
        "the started tree outlived its owner"
    );
    assert!(!held(run.root.path()), "the child's lock outlived it");
}

fn releases_the_owners_output(mode: &str) {
    // The bound only detects an output the child still holds.
    let (ended, running, run) = output_ends_while_the_child_runs(mode, Duration::from_secs(30));
    assert!(ended, "the {mode} child held the owner's output");
    assert!(running, "the {mode} child ended before the output did");
    dies_with_its_owner(run);
}

/// Negative control: a child started through `std` holds the output. It
/// outlives its owner until the run releases it.
fn std_holds_the_owners_output() {
    let (ended, running, _run) =
        output_ends_while_the_child_runs("control", Duration::from_secs(2));
    assert!(!ended, "a std-started child released the owner's output");
    assert!(running, "the std-started child ended");
}

#[test]
fn a_hook_releases_the_owners_output() {
    releases_the_owners_output("hook");
    std_holds_the_owners_output();
}

#[cfg(feature = "mcp-stdio")]
#[test]
fn a_stdio_mcp_server_releases_the_owners_output() {
    releases_the_owners_output("mcp");
    std_holds_the_owners_output();
}
