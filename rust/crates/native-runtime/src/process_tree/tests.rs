use super::ProcessTree;

use std::io;

#[test]
fn early_stdin_closure_preserves_literal_capture_but_rejects_incomplete_exchange() {
    use std::process::Stdio;
    let input = vec![b'x'; 1_048_576];
    for literal in [true, false] {
        #[cfg(windows)]
        let mut command = crate::shell_command(
            "echo fixture-output&echo fixture-error 1>&2&exit /b 7",
            Some("cmd"),
        )
        .expect("resolve fixture shell");
        #[cfg(not(windows))]
        let mut command = crate::shell_command(
            "exec 0<&-; printf fixture-output; printf fixture-error >&2; exit 7",
            Some("sh"),
        )
        .expect("resolve fixture shell");
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn early-closing child");
        let input = if literal {
            crate::CaptureInput::Literal(&input)
        } else {
            crate::CaptureInput::Exchange {
                initial: &input,
                maximum: input.len(),
            }
        };
        let result = tree.wait_with_output(
            std::time::Duration::from_secs(5),
            4096,
            Some(input),
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        );
        if literal {
            let output = result.expect("literal input may be closed early");
            assert_eq!(output.status.code(), Some(7));
            assert!(String::from_utf8_lossy(&output.stdout).starts_with("fixture-output"));
            assert!(String::from_utf8_lossy(&output.stderr).starts_with("fixture-error"));
        } else {
            assert_eq!(
                result
                    .expect_err("exchange input must be delivered")
                    .error
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        }
        assert!(tree.is_reaped());
    }
}
/// The child exits after a write found its stdin full, and the same capture
/// pass sees its output end and its exit before any later write could meet
/// the closed pipe. The exchange still fails: its input never reached the
/// child.
#[cfg(unix)]
#[test]
fn an_exchange_fails_when_the_child_exits_before_taking_its_input() -> io::Result<()> {
    use std::process::Stdio;
    let mut command = std::process::Command::new("/bin/sh");
    command
        .args(["-c", "exec sleep 60"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let pid = tree
        .child
        .as_ref()
        .map(std::process::Child::id)
        .ok_or(io::ErrorKind::NotFound)?;
    let input = vec![b'x'; 1 << 20];
    let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    meter.blocked_write = Some(Box::new(move || {
        // SAFETY: the unreaped direct child reserves this pid.
        assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) }, 0);
        while super::platform::observe_exit_id(pid, None)
            .expect("observe the killed child")
            .is_none()
        {
            std::thread::yield_now();
        }
    }));
    let failure = tree
        .wait_with_output(
            std::time::Duration::from_secs(5),
            16,
            Some(crate::CaptureInput::Exchange {
                initial: &input,
                maximum: input.len(),
            }),
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            Some(&meter),
            Some([
                Vec::with_capacity(16),
                Vec::with_capacity(16),
                Vec::with_capacity(input.len()),
            ]),
            None,
        )
        .expect_err("undelivered exchange input must fail");
    assert_eq!(failure.error.kind(), io::ErrorKind::BrokenPipe);
    assert!(failure.status.is_some());
    assert!(failure.cleanup_completed);
    Ok(())
}

#[cfg(unix)]
#[test]
fn prepared_closed_spawn_uses_owned_birth_environment_and_no_late_allocation() -> std::io::Result<()>
{
    use std::sync::Arc;
    let sizing = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    let frame = ProcessTree::prepare_closed_process(2, sizing.clone())?;
    let bytes = sizing.memory.load(std::sync::atomic::Ordering::Acquire);
    assert!(bytes > 0);
    drop(frame);
    assert_eq!(sizing.memory.load(std::sync::atomic::Ordering::Acquire), 0);
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(u64::MAX, 0));
    let mut frame = Some(ProcessTree::prepare_closed_process(2, original.clone())?);
    assert_eq!(
        original.memory.load(std::sync::atomic::Ordering::Acquire),
        bytes
    );
    original
        .deny_allocations
        .store(true, std::sync::atomic::Ordering::Release);
    let prepared = frame.as_mut().expect("genuine funded birth frame");
    let path = prepared
        .environment
        .data
        .iter_mut()
        .find(|entry| entry.data.starts_with(b"PATH="))
        .ok_or(std::io::ErrorKind::NotFound)?;
    let end = path.data.len() - 1;
    path.data
        .get_mut(5..end)
        .filter(|value| !value.is_empty())
        .ok_or(std::io::ErrorKind::InvalidInput)?
        .fill(b'x');
    let mut custody = None;
    ProcessTree::spawn_closed_prepared(
        c"/bin/sh",
        &[c"-c", c"case \"$PATH\" in x*) exit 7;; *) exit 9;; esac"],
        &mut frame,
        original.as_ref(),
        &mut custody,
    )?;
    assert!(frame.is_none());
    let mut tree = custody.take().expect("same actual leader owns its frame");
    assert!(tree.prepared_spawn.is_some());
    assert_eq!(
        original.memory.load(std::sync::atomic::Ordering::Acquire),
        bytes
    );
    assert_eq!(
        tree.wait(std::time::Duration::from_secs(5))?.code(),
        Some(7)
    );
    assert!(!tree.is_reaped());
    assert_eq!(
        original.memory.load(std::sync::atomic::Ordering::Acquire),
        bytes,
        "observed exit is not physical reap"
    );
    tree.terminate(None)?;
    assert!(tree.is_reaped() && tree.prepared_spawn.is_none());
    assert_eq!(
        original.memory.load(std::sync::atomic::Ordering::Acquire),
        0
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn prepared_closed_spawn_denial_retains_original_frame() -> io::Result<()> {
    let mut limits = crate::tests::ReadBudgetMeter::new(u64::MAX, 0);
    limits.cancel_after_operation = true;
    let meter = std::sync::Arc::new(limits);
    let mut frame = Some(ProcessTree::prepare_closed_process(2, meter.clone())?);
    let bytes = meter.memory.load(std::sync::atomic::Ordering::Acquire);
    let mut custody = None;
    assert_eq!(
        ProcessTree::spawn_closed_prepared(
            c"/bin/sh",
            &[c"-c"],
            &mut frame,
            meter.as_ref(),
            &mut custody
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(custody.is_none() && frame.is_some());
    assert!(
        ProcessTree::spawn_closed_prepared(
            c"/bin/sh",
            &[c"-c", c"exit 7"],
            &mut frame,
            meter.as_ref(),
            &mut custody
        )
        .is_err()
    );
    assert!(custody.is_none() && frame.is_some());
    assert_eq!(
        meter.memory.load(std::sync::atomic::Ordering::Acquire),
        bytes
    );
    drop(frame);
    assert_eq!(meter.memory.load(std::sync::atomic::Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn measured_kill_with_denied_observation_keeps_owned_child() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let mut tree = ProcessTree::spawn(&mut command("child", temporary.path()))?;
    let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, 0);
    meter.operation_denial = Some(crate::NativeOperationKind::Read);
    assert!(tree.terminate(Some(&meter)).is_err());
    assert!(!tree.is_reaped());
    tree.wait(Duration::from_secs(5))?;
    tree.terminate(None)?;
    assert!(tree.is_reaped());
    Ok(())
}

#[cfg(windows)]
#[test]
fn suspended_child_launch_fits_its_input_item_allowance() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let mut command = command("exit-code", temporary.path());
    let items = 1
        + command.get_args().count()
        + command.get_envs().count()
        + usize::from(command.get_current_dir().is_some());
    let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, 0);
    meter.item_limit = Some(u64::try_from(items).map_err(|_| io::ErrorKind::InvalidInput)?);
    let mut custody = None;
    let spawned = ProcessTree::spawn_controlled(&mut command, Some(&meter), &mut custody);
    let mut tree = custody.take().ok_or(io::ErrorKind::NotFound)?;
    let status = spawned.and_then(|()| tree.wait(Duration::from_secs(5)));
    tree.terminate(None)?;
    assert_eq!(status?.code(), Some(7));
    assert!(tree.is_reaped());
    Ok(())
}

use std::fs;
use std::io::{Read as _, Write as _};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const MODE: &str = "ACYCLIC_PROCESS_TREE_TEST_MODE";
const ROOT: &str = "ACYCLIC_PROCESS_TREE_TEST_ROOT";
const BULK_BYTES: usize = 4 << 20;

pub(super) fn command(mode: &str, root: &std::path::Path) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("test executable"));
    command
        .args([
            "--exact",
            "process_tree::tests::process_tree_helper",
            "--nocapture",
        ])
        .env_clear()
        .env(MODE, mode)
        .env(ROOT, root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Waits for a helper's readiness marker.
pub(super) fn ready(root: &std::path::Path, name: &str, mut exited: impl FnMut() -> bool) {
    while !root.join(name).exists() {
        assert!(
            !exited() || root.join(name).exists(),
            "helper exited before it was ready"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn tree_exited(tree: &mut ProcessTree) -> bool {
    tree.try_wait(None).map_or(true, |status| status.is_some())
}

/// Asks any descendant that outlived its tree's cleanup to mark its escape, then checks that none did.
pub(super) fn assert_no_escape(root: &std::path::Path) {
    fs::write(root.join("probe"), b"probe").expect("escape probe");
    thread::sleep(Duration::from_millis(500));
    assert!(!root.join("escaped").exists());
}

#[test]
#[allow(
    clippy::zombie_processes,
    reason = "exit-parent deliberately tests parent-before-descendant exit"
)]
fn process_tree_helper() {
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    let root = std::path::PathBuf::from(std::env::var_os(ROOT).expect("helper root"));
    match mode.as_str() {
        "grandchild" => {
            fs::write(root.join("grandchild-ready"), b"ready").expect("grandchild ready");
            let deadline = Instant::now() + Duration::from_secs(60);
            while !root.join("probe").exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            if root.join("probe").exists() {
                fs::write(root.join("escaped"), b"descendant survived").expect("escaped marker");
            }
        }
        "child" | "child-cancel" | "exit-parent" => {
            let mut grandchild = command("grandchild", &root);
            grandchild.stdout(Stdio::inherit()).stderr(Stdio::inherit());
            let mut grandchild = grandchild.spawn().expect("spawn grandchild");
            ready(&root, "grandchild-ready", || {
                grandchild
                    .try_wait()
                    .map_or(true, |status| status.is_some())
            });
            fs::write(root.join("tree-ready"), b"ready").expect("tree ready");
            if mode == "child-cancel" {
                std::io::stdout()
                    .write_all(b"tree-cancel-ready")
                    .expect("tree readiness");
                std::io::stdout().flush().expect("flush tree readiness");
            }
            if mode != "exit-parent" {
                let _ = grandchild.wait();
            }
        }
        "stderr-bulk" => stderr_bulk_helper(),
        "flood" => loop {
            std::io::stdout()
                .write_all(&[b'o'; 8192])
                .expect("stdout flood");
            std::io::stderr()
                .write_all(&[b'e'; 8192])
                .expect("stderr flood");
        },
        "partial" => {
            std::io::stdout()
                .write_all(b"stdout-before-stop")
                .expect("partial stdout");
            std::io::stderr()
                .write_all(b"stderr-before-stop")
                .expect("partial stderr");
            std::io::stdout().flush().expect("flush partial stdout");
            std::io::stderr().flush().expect("flush partial stderr");
            fs::write(root.join("partial-ready"), b"ready").expect("partial ready");
            thread::sleep(Duration::from_secs(30));
        }
        "output" => {
            assert_eq!(
                std::env::var("EXPLICIT_PROCESS_INPUT").expect("explicit input"),
                "allowed"
            );
            assert!(std::env::var_os("PATH").is_none());
            assert!(std::env::var_os("HOME").is_none());
            assert!(std::env::var_os("CODEX_HOME").is_none());
            std::io::stdout()
                .write_all(b"stdout-marker")
                .expect("stdout");
            std::io::stderr()
                .write_all(b"stderr-marker")
                .expect("stderr");
            let mut input = Vec::new();
            std::io::stdin()
                .read_to_end(&mut input)
                .expect("closed stdin");
            assert!(input.is_empty());
            std::process::exit(0);
        }
        "bulk" => {
            let chunk = [b'b'; 64 * 1024];
            for _ in 0..BULK_BYTES / chunk.len() {
                std::io::stdout().write_all(&chunk).expect("bulk stdout");
            }
            std::process::exit(0);
        }
        "stdin-stall" | "stdin-handshake" | "stdin-cancel" | "stdin-save" => {
            stdin_helper(&mode, &root)
        }
        "stream-notify" => stream_notify_helper(&root),
        "exit-code" => std::process::exit(7),
        _ => assert_eq!(mode, "known helper mode"),
    }
}

fn stream_notify_helper(root: &std::path::Path) {
    let mut peer = std::net::TcpStream::connect(root.to_string_lossy().as_ref())
        .expect("connect stream observer");
    peer.set_read_timeout(Some(Duration::from_secs(2)))
        .expect("stream handshake deadline");
    std::io::stdout()
        .write_all(b"stream-stdout")
        .expect("stream stdout");
    std::io::stdout().flush().expect("flush stream stdout");
    std::io::stderr()
        .write_all(b"stream-stderr")
        .expect("stream stderr");
    std::io::stderr().flush().expect("flush stream stderr");
    let mut acknowledgement = [0];
    peer.read_exact(&mut acknowledgement)
        .expect("observer acknowledged both pipes before exit");
    assert_eq!(acknowledgement, [1]);
    std::process::exit(0);
}

fn stderr_bulk_helper() -> ! {
    for _ in 0..32 {
        std::io::stderr()
            .write_all(&[b'e'; 8192])
            .expect("stderr bulk");
    }
    std::io::stderr()
        .write_all(b"stderr-tail-marker")
        .expect("stderr tail");
    std::process::exit(7);
}

fn stdin_helper(mode: &str, root: &std::path::Path) {
    match mode {
        "stdin-cancel" => {
            let mut first = [0];
            std::io::stdin()
                .read_exact(&mut first)
                .expect("partial input");
            assert_eq!(first, [b'i']);
            std::io::stdout()
                .write_all(b"stdin-partial-marker")
                .expect("partial marker");
            std::io::stdout().flush().expect("flush partial marker");
            // No further input is consumed. The owner must observe the
            // cancellation notification and physically reap this child.
            loop {
                thread::park();
            }
        }
        "stdin-save" => {
            let mut input = Vec::new();
            std::io::stdin()
                .read_to_end(&mut input)
                .expect("prompt EOF");
            crate::private_file::publish(
                &root.join("stdin-captured"),
                &root.join("stdin-captured.next"),
                crate::RenameMode::NoReplace,
                |file| file.write_all(&input),
                |_| Ok(()),
            )
            .expect("atomically captured prompt");
        }
        "stdin-stall" => {
            std::io::stdout()
                .write_all(b"stdin-stall-marker")
                .expect("stall marker");
            std::io::stdout().flush().expect("flush stall marker");
            fs::write(root.join("stdin-stall-ready"), b"ready").expect("stall ready");
            thread::sleep(Duration::from_secs(30));
        }
        "stdin-handshake" => {
            let mut first = [0; 6];
            std::io::stdin()
                .read_exact(&mut first)
                .expect("first input");
            assert_eq!(&first, b"first\n");
            std::io::stdout()
                .write_all(b"first-ack\n")
                .expect("first ack");
            std::io::stdout().flush().expect("flush first ack");
            let mut second = [0; 7];
            std::io::stdin()
                .read_exact(&mut second)
                .expect("second input");
            assert_eq!(&second, b"second\n");
            std::io::stderr()
                .write_all(b"exchange-stderr\n")
                .expect("exchange stderr");
            std::io::stderr().flush().expect("flush exchange stderr");
            std::io::stdout()
                .write_all(b"second-ack\n")
                .expect("second ack");
            std::io::stdout().flush().expect("flush second ack");
            thread::sleep(Duration::from_secs(30));
        }
        _ => panic!("unknown stdin helper mode"),
    }
}

#[test]
fn spawn_failure_does_not_return_an_unowned_child() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let error = ProcessTree::spawn(&mut Command::new(temporary.path().join("missing")))
        .err()
        .expect("missing executable must not spawn");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn cleanup_is_idempotent_and_drop_contains_descendants() {
    for explicit in [false, true] {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree =
            ProcessTree::spawn(&mut command("child", temporary.path())).expect("spawn tree");
        ready(temporary.path(), "tree-ready", || tree_exited(&mut tree));
        if explicit {
            tree.terminate(None).expect("terminate tree");
            tree.terminate(None).expect("repeat termination");
            assert!(tree.child.is_none());
        }
        drop(tree);
        assert_no_escape(temporary.path());
    }
}

#[test]
fn capture_check_failure_cleans_up_the_owned_tree() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut tree =
        ProcessTree::spawn(&mut command("child-cancel", temporary.path())).expect("spawn tree");
    let cancelled = std::cell::Cell::new(false);
    let wake = super::wake::ProcessWake::new().expect("cancellation wake");
    let mut observed = Vec::new();
    let started = Instant::now();
    let error = tree
        .wait_with_output(
            Duration::from_secs(5),
            4096,
            None,
            || {
                if cancelled.get() {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "cancelled",
                    ))
                } else {
                    Ok(())
                }
            },
            |chunk| {
                if let crate::ProcessOutput::Stdout(bytes) = chunk {
                    observed.extend_from_slice(bytes);
                    if observed
                        .windows(b"tree-cancel-ready".len())
                        .any(|bytes| bytes == b"tree-cancel-ready")
                    {
                        cancelled.set(true);
                        wake.notify()?;
                    }
                }
                Ok(crate::ProcessInput::default())
            },
            None,
            None,
            Some(std::slice::from_ref(&wake)),
        )
        .expect_err("cancellation must stop capture");
    assert_eq!(error.error.kind(), std::io::ErrorKind::Interrupted);
    assert_eq!(error.error.to_string(), "cancelled");
    assert!(
        cancelled.get(),
        "the complete live tree acknowledged readiness"
    );
    assert!(error.cleanup_completed && tree.is_reaped());
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "notification must interrupt capture before its deadline"
    );
    assert_no_escape(temporary.path());
}

#[test]
fn owned_streaming_stdout_rejects_empty_reads_and_reaches_eof() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("output", temporary.path());
    command.env("EXPLICIT_PROCESS_INPUT", "allowed");
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn streaming tree");
    assert_eq!(
        tree.read_stdout(&mut []).expect_err("empty read").kind(),
        std::io::ErrorKind::InvalidInput
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut bytes = Vec::new();
    loop {
        let mut buffer = [0; 128];
        match tree.read_stdout(&mut buffer).expect("read owned stdout") {
            Some(0) => break,
            Some(read) => bytes.extend_from_slice(buffer.get(..read).expect("valid pipe read")),
            None => thread::sleep(Duration::from_millis(1)),
        }
        assert!(Instant::now() < deadline, "streaming output deadline");
    }
    tree.terminate(None).expect("clean streaming tree");
    assert!(bytes.ends_with(b"stdout-marker"));
}

#[test]
fn graceful_cleanup_handles_an_exited_leader_and_its_descendants() {
    for mode in ["exit-code", "exit-parent", "child"] {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree =
            ProcessTree::spawn(&mut command(mode, temporary.path())).expect("spawn tree");
        if mode != "exit-code" {
            ready(temporary.path(), "tree-ready", || tree_exited(&mut tree));
        }
        if mode != "child" {
            tree.wait(Duration::from_secs(5))
                .expect("observe leader exit");
        }
        let status = tree
            .terminate_after(Duration::from_secs(1))
            .expect("graceful tree cleanup");
        if mode == "exit-code" {
            assert_eq!(status.code(), Some(7));
        }
        assert!(tree.child.is_none());
        assert_no_escape(temporary.path());
    }
}

#[test]
fn collection_closes_stdin_and_preserves_explicit_environment() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("output", temporary.path());
    command
        .env("EXPLICIT_PROCESS_INPUT", "allowed")
        .stdin(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
    let output = tree
        .wait_with_output(
            Duration::from_secs(5),
            4096,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)
        .expect("collect output");
    assert!(output.status.success(), "{output:?}");
    assert!(output.stdout.ends_with(b"stdout-marker"));
    assert_eq!(output.stderr, b"stderr-marker");
    assert!(tree.child.is_none());
    assert!(
        tree.wait_with_output(
            Duration::ZERO,
            0,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None
        )
        .map_err(|failure| failure.error)
        .is_err()
    );
    tree.terminate(None).expect("repeat cleanup");
    let mut command = self::command("output", temporary.path());
    command.env("EXPLICIT_PROCESS_INPUT", "allowed");
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
    assert_eq!(
        tree.wait_with_output(
            Duration::from_secs(5),
            output.stdout.len() + output.stderr.len() - 1,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None
        )
        .map_err(|failure| failure.error)
        .expect_err("combined pipes exceed budget")
        .kind(),
        std::io::ErrorKind::FileTooLarge
    );
}

#[test]
fn stdin_backpressure_preserves_intent_checks_and_cleanup() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("stdin-cancel", temporary.path());
    command.stdin(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn stalled reader");
    let input = vec![b'i'; BULK_BYTES];
    let cancelled = std::cell::Cell::new(false);
    let wake = super::wake::ProcessWake::new().expect("cancellation wake");
    let mut observed = Vec::new();
    let failure = tree
        .wait_with_output(
            Duration::from_secs(5),
            4096,
            Some(crate::CaptureInput::Literal(&input)),
            || {
                if cancelled.get() {
                    Err(std::io::ErrorKind::Interrupted.into())
                } else {
                    Ok(())
                }
            },
            |chunk| {
                if let crate::ProcessOutput::Stdout(bytes) = chunk {
                    observed.extend_from_slice(bytes);
                    if observed
                        .windows(b"stdin-partial-marker".len())
                        .any(|bytes| bytes == b"stdin-partial-marker")
                    {
                        cancelled.set(true);
                        wake.notify()?;
                    }
                }
                Ok(crate::ProcessInput::default())
            },
            None,
            None,
            Some(std::slice::from_ref(&wake)),
        )
        .expect_err("intent stops stalled exchange after actual partial input");
    assert_eq!(failure.error.kind(), std::io::ErrorKind::Interrupted);
    assert!(failure.stdout.ends_with(b"stdin-partial-marker"));
    assert!(failure.cleanup_completed && tree.is_reaped());
}

#[test]
fn queued_stdin_delivers_exact_bytes_and_eof_through_the_capture_pump() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("stdin-save", temporary.path());
    command.stdin(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn prompt reader");
    let input: Vec<u8> = (0..BULK_BYTES)
        .map(|n| u8::try_from(n % 251).unwrap())
        .collect();
    tree.queue_stdin(input.clone()).expect("queue prompt");
    assert!(
        tree.take_stdin().is_none(),
        "queued pipe cannot be transferred"
    );
    assert_eq!(
        tree.queue_stdin(vec![b'x']).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    while !tree.pump_stdin().expect("nonblocking prompt progress") {
        thread::yield_now();
    }
    tree.require_stdin_complete().expect("prompt delivered");
    ready(temporary.path(), "stdin-captured", || {
        tree_exited(&mut tree)
    });
    assert_eq!(
        fs::read(temporary.path().join("stdin-captured")).unwrap(),
        input
    );
    tree.terminate(None).expect("reap prompt reader");
}

#[test]
fn queued_stdin_backpressure_keeps_cancellation_and_exclusive_custody() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("stdin-stall", temporary.path());
    command.stdin(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn stalled reader");
    ready(temporary.path(), "stdin-stall-ready", || {
        tree_exited(&mut tree)
    });
    tree.queue_stdin(vec![b'x'; BULK_BYTES])
        .expect("queue blocked prompt");
    assert!(!tree.pump_stdin().expect("one bounded nonblocking write"));
    assert_eq!(
        tree.require_stdin_complete().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    let failure = tree
        .wait_with_output(
            Duration::from_secs(5),
            4096,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .expect_err("capture cannot replace a queued-input owner");
    assert_eq!(failure.error.kind(), io::ErrorKind::AlreadyExists);
    assert!(!failure.cleanup_completed && !tree.is_reaped());
    tree.terminate(None)
        .expect("the queued owner cancels and reaps");
    assert!(tree.is_reaped());
}

#[test]
fn queued_stdin_refuses_a_second_capture_before_any_capture_turn() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("stdin-stall", temporary.path());
    command.stdin(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn queued owner");
    tree.queue_stdin(vec![b'x'; BULK_BYTES])
        .expect("queue prompt");
    let result = super::pump::Pump::new(
        &mut tree,
        Duration::from_secs(5).into(),
        4096,
        None,
        None,
        [Vec::new(), Vec::new()],
    );
    let Err(failure) = result else {
        panic!("capture replaced the queued owner")
    };
    assert_eq!(failure.error.kind(), io::ErrorKind::AlreadyExists);
    tree.terminate(None).expect("reap queued owner");
}

#[test]
fn queued_stdin_rejects_early_closure_that_optional_literal_capture_accepts() {
    for required in [true, false] {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("exit-code", temporary.path());
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn early exit");
        while !tree_exited(&mut tree) {
            thread::yield_now();
        }
        let input = vec![b'x'; BULK_BYTES];
        tree.queue_stdin(input.clone()).expect("queue prompt");
        if !required {
            // Actual optional capture semantics are the negative control:
            // permitting partial delivery must not settle a host prompt.
            tree.queued_stdin = Some(
                super::PendingStdin::owned(crate::OwnedCaptureInput::Literal(input))
                    .expect("optional input control"),
            );
        }
        let result = loop {
            match tree.pump_stdin() {
                Ok(false) => thread::yield_now(),
                result => break result,
            }
        };
        if required {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        } else {
            assert!(result.expect("optional capture accepts the same closed peer"));
        }
        tree.terminate(None).expect("reap closed reader");
    }
}

#[test]
fn partial_stdin_writes_preserve_exact_bytes_and_explicit_eof() {
    #[cfg(windows)]
    let mut command = {
        use std::os::windows::process::CommandExt as _;
        let mut command = Command::new("powershell.exe");
        command
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::OpenStandardInput().CopyTo([Console]::OpenStandardOutput())",
            ])
            .creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
        command
    };
    #[cfg(not(windows))]
    let mut command = Command::new("cat");
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn input echo");
    let input: Vec<u8> = (0..BULK_BYTES)
        .map(|index| u8::try_from(index % 251).expect("byte pattern"))
        .collect();
    let output = tree
        .wait_with_output(
            Duration::from_secs(60),
            2 * BULK_BYTES,
            Some(crate::CaptureInput::Literal(&input)),
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .expect("collect echoed input");
    assert!(output.status.success());
    assert_eq!(output.stdout, input);
    assert!(output.stderr.is_empty() && tree.is_reaped());
}

#[test]
fn streaming_exchange_uses_the_same_bounded_capture_and_cleanup_owner() {
    use super::ProcessInput;
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("stdin-handshake", temporary.path());
    command.stdin(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn exchange");
    let mut seen = Vec::new();
    let mut phase = 0;
    let output = tree
        .wait_with_output(
            Duration::from_secs(5),
            4096,
            Some(crate::CaptureInput::Exchange {
                initial: b"first\n",
                maximum: 13,
            }),
            || Ok(()),
            |chunk| {
                let crate::ProcessOutput::Stdout(chunk) = chunk else {
                    return Ok(ProcessInput::default());
                };
                seen.extend_from_slice(chunk);
                if phase == 0 && seen.ends_with(b"first-ack\n") {
                    seen.clear();
                    phase = 1;
                    Ok(ProcessInput {
                        bytes: b"second\n".to_vec(),
                        complete: false,
                    })
                } else if phase == 1 && seen.ends_with(b"second-ack\n") {
                    phase = 2;
                    Ok(ProcessInput {
                        complete: true,
                        ..crate::ProcessInput::default()
                    })
                } else {
                    Ok(crate::ProcessInput::default())
                }
            },
            None,
            None,
            None,
        )
        .expect("complete streaming exchange");
    assert_eq!(phase, 2);
    assert!(output.stdout.ends_with(b"first-ack\nsecond-ack\n"));
    assert_eq!(output.stderr, b"exchange-stderr\n");
    assert!(
        !output.status.success(),
        "protocol completion terminates the stalled server"
    );
    assert!(tree.is_reaped());
}

#[test]
fn streaming_output_observes_both_pipes_before_exit_without_an_exchange() -> io::Result<()> {
    for input in [None, Some(crate::CaptureInput::Literal(&[]))] {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?.to_string();
        let mut command = command("stream-notify", std::path::Path::new(&address));
        #[cfg(windows)]
        command.env(
            "SystemRoot",
            std::env::var_os("SystemRoot").ok_or(io::ErrorKind::NotFound)?,
        );
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command)?;
        let mut observed_stdout = Vec::new();
        let mut observed_stderr = Vec::new();
        let mut acknowledged = false;
        let output = tree
            .wait_with_output(
                Duration::from_secs(2),
                4096,
                input,
                || Ok(()),
                |chunk| {
                    match chunk {
                        crate::ProcessOutput::Stdout(bytes) => {
                            observed_stdout.extend_from_slice(bytes);
                        }
                        crate::ProcessOutput::Stderr(bytes) => {
                            observed_stderr.extend_from_slice(bytes);
                        }
                    }
                    if !acknowledged
                        && observed_stdout.ends_with(b"stream-stdout")
                        && observed_stderr.ends_with(b"stream-stderr")
                    {
                        let (mut peer, _) = listener.accept()?;
                        peer.set_write_timeout(Some(Duration::from_secs(2)))?;
                        peer.write_all(&[1])?;
                        acknowledged = true;
                    }
                    Ok(crate::ProcessInput::default())
                },
                None,
                None,
                None,
            )
            .map_err(|failure| failure.error)?;
        assert!(
            acknowledged && output.status.success(),
            "status: {}, stdout: {}, stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, observed_stdout);
        assert_eq!(output.stderr, observed_stderr);
        assert!(tree.is_reaped());
    }
    Ok(())
}

/// Holds this thread's capture clock at a chosen instant until dropped.
struct FrozenCaptureClock;

impl FrozenCaptureClock {
    fn at(now: std::time::Instant) -> Self {
        super::set_capture_clock(Some(now));
        Self
    }

    fn advance_to(&self, now: std::time::Instant) {
        super::set_capture_clock(Some(now));
    }
}

impl Drop for FrozenCaptureClock {
    fn drop(&mut self) {
        super::set_capture_clock(None);
    }
}

#[test]
fn streaming_input_overflow_parser_failure_and_deadline_preserve_cleanup() {
    use super::ProcessInput;
    for parser_failure in [false, true] {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = command("stdin-handshake", temporary.path());
        command.stdin(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn exchange");
        let mut seen = Vec::new();
        let failure = tree
            .wait_with_output(
                Duration::from_secs(5),
                4096,
                Some(crate::CaptureInput::Exchange {
                    initial: b"first\n",
                    maximum: 6,
                }),
                || Ok(()),
                |chunk| {
                    let crate::ProcessOutput::Stdout(chunk) = chunk else {
                        return Ok(ProcessInput::default());
                    };
                    seen.extend_from_slice(chunk);
                    if seen.ends_with(b"first-ack\n") {
                        if parser_failure {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "invalid protocol",
                            ))
                        } else {
                            Ok(ProcessInput {
                                bytes: b"second\n".to_vec(),
                                complete: false,
                            })
                        }
                    } else {
                        Ok(crate::ProcessInput::default())
                    }
                },
                None,
                None,
                None,
            )
            .expect_err("stop streaming exchange");
        assert_eq!(
            failure.error.kind(),
            if parser_failure {
                std::io::ErrorKind::InvalidData
            } else {
                std::io::ErrorKind::FileTooLarge
            }
        );
        assert!(failure.stdout.ends_with(b"first-ack\n"));
        assert!(failure.cleanup_completed);
        assert!(tree.is_reaped());
    }
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("stdin-stall", temporary.path());
    command.stdin(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn stalled exchange");
    ready(temporary.path(), "stdin-stall-ready", || {
        tree_exited(&mut tree)
    });
    // The capture clock is frozen and only reaches the deadline inside the
    // second check, so expiry is ordered after two full capture iterations no
    // matter how late the scheduler runs this thread.
    let timeout = Duration::from_millis(20);
    let started = std::time::Instant::now();
    let clock = FrozenCaptureClock::at(started);
    let mut checks = 0;
    let failure = tree
        .wait_with_output(
            timeout,
            4096,
            Some(crate::CaptureInput::Exchange {
                initial: &vec![b'i'; BULK_BYTES],
                maximum: BULK_BYTES,
            }),
            || {
                checks += 1;
                if checks == 2 {
                    clock.advance_to(started + timeout);
                }
                Ok(())
            },
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .expect_err("deadline stops stalled input");
    assert_eq!(failure.error.kind(), std::io::ErrorKind::TimedOut);
    assert!(failure.stdout.ends_with(b"stdin-stall-marker"));
    drop(clock);
    assert_eq!(
        checks, 2,
        "the deadline expired only once the clock reached it"
    );
    assert!(failure.cleanup_completed);
    assert!(tree.is_reaped());
}

#[test]
fn stopped_capture_preserves_output_prefix_and_cleanup_observation() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut tree = ProcessTree::spawn(&mut command("partial", temporary.path()))
        .expect("spawn partial output");
    ready(temporary.path(), "partial-ready", || tree_exited(&mut tree));
    let failure = tree
        .wait_with_output(
            Duration::from_millis(20),
            4096,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .expect_err("partial process times out");
    assert_eq!(failure.error.kind(), std::io::ErrorKind::TimedOut);
    assert!(failure.stdout.ends_with(b"stdout-before-stop"));
    assert_eq!(failure.stderr, b"stderr-before-stop");
    assert!(failure.cleanup_completed);
    assert!(failure.status.is_none());
    assert!(tree.is_reaped());
}

#[test]
fn capture_timeout_and_output_overflow_reap_owned_tree() {
    for (mode, timeout, limit, kind) in [
        (
            "child",
            Duration::from_millis(20),
            4096,
            std::io::ErrorKind::TimedOut,
        ),
        (
            "flood",
            Duration::from_secs(5),
            16384,
            std::io::ErrorKind::FileTooLarge,
        ),
    ] {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut tree =
            ProcessTree::spawn(&mut command(mode, temporary.path())).expect("spawn tree");
        if mode == "child" {
            ready(temporary.path(), "tree-ready", || tree_exited(&mut tree));
            assert_eq!(
                tree.wait(Duration::ZERO).expect_err("wait deadline").kind(),
                std::io::ErrorKind::TimedOut
            );
            assert!(tree.child.is_some());
        }
        let started = Instant::now();
        let error = tree
            .wait_with_output(
                timeout,
                limit,
                None,
                || Ok(()),
                |_| Ok(crate::ProcessInput::default()),
                None,
                None,
                None,
            )
            .expect_err("capture must fail");
        assert_eq!(error.error.kind(), kind);
        assert!(error.cleanup_completed);
        assert!(error.stdout.len() + error.stderr.len() <= limit);
        if mode == "flood" {
            assert_eq!(error.stdout.len() + error.stderr.len(), limit);
        }
        assert!(started.elapsed() < Duration::from_secs(7));
        assert!(tree.child.is_none());
        tree.terminate(None).expect("repeat cleanup");
        assert_no_escape(temporary.path());
    }
}

#[test]
fn bulk_output_is_collected_fully_within_its_exact_bound() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let output = ProcessTree::spawn(&mut command("bulk", temporary.path()))
        .expect("spawn bulk writer")
        .wait_with_output(
            Duration::from_secs(60),
            2 * BULK_BYTES,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)
        .expect("collect bulk output");
    assert!(output.status.success());
    let header = output.stdout.len() - BULK_BYTES;
    assert!(
        output
            .stdout
            .get(header..)
            .expect("bulk bytes")
            .iter()
            .all(|byte| *byte == b'b')
    );
    assert!(output.stderr.is_empty());
    let error = ProcessTree::spawn(&mut command("bulk", temporary.path()))
        .expect("spawn bulk writer")
        .wait_with_output(
            Duration::from_secs(60),
            output.stdout.len() - 1,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)
        .expect_err("bulk output exceeds bound");
    assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
}

#[test]
fn exited_parent_does_not_leave_descendant_holding_output_open() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut tree =
        ProcessTree::spawn(&mut command("exit-parent", temporary.path())).expect("spawn tree");
    let output = tree
        .wait_with_output(
            Duration::from_secs(5),
            4096,
            None,
            || Ok(()),
            |_| Ok(crate::ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)
        .expect("collect exited parent");
    assert!(output.status.success());
    ready(temporary.path(), "tree-ready", || true);
    assert_no_escape(temporary.path());
}

#[cfg(unix)]
#[test]
fn exit_observation_retains_group_leader_until_cleanup() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let mut command = command("output", temporary.path());
    command.env("EXPLICIT_PROCESS_INPUT", "allowed");
    let mut tree = ProcessTree::spawn(&mut command).expect("spawn tree");
    let deadline = Instant::now() + Duration::from_secs(5);
    while tree.try_wait(None).expect("poll exit").is_none() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        tree.wait(Duration::from_secs(5))
            .expect("wait exit")
            .success()
    );
    assert!(
        super::platform::observe_exit(tree.child.as_ref().expect("retained child"))
            .expect("leader remains waitable")
            .expect("exited leader")
            .success()
    );
    tree.terminate(None).expect("terminate and reap");
    assert!(tree.child.is_none());
    let mut tree = ProcessTree::spawn(&mut self::command("exit-code", temporary.path()))
        .expect("spawn nonzero exit");
    assert_eq!(
        tree.wait(Duration::from_secs(5))
            .expect("nonzero exit")
            .code(),
        Some(7)
    );
    tree.terminate(None).expect("reap nonzero exit");

    let mut tree = ProcessTree::spawn(&mut self::command("child", temporary.path()))
        .expect("spawn signalled child");
    tree.terminate_descendants().expect("signal tree");
    use std::os::unix::process::ExitStatusExt as _;
    assert_eq!(
        tree.wait(Duration::from_secs(5))
            .expect("signal exit")
            .signal(),
        Some(libc::SIGKILL)
    );
    tree.terminate(None).expect("reap signal exit");
}

#[cfg(all(feature = "test-support", unix))]
mod seeded_capture {
    use super::ProcessTree;
    use crate::testing::Schedule;
    use crate::{CaptureHandler, CaptureSchedule, ProcessInput, ProcessOutput};
    use std::{io, process::Stdio, time::Duration};

    struct Collect(Vec<u8>);

    impl CaptureHandler for Collect {
        fn check(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn observe(&mut self, chunk: ProcessOutput<'_>) -> io::Result<ProcessInput> {
            if let ProcessOutput::Stdout(bytes) = chunk {
                self.0.extend_from_slice(bytes);
            }
            Ok(ProcessInput::default())
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Observed {
        trace: Vec<String>,
        boundaries: Vec<&'static str>,
        polls: Vec<(u64, &'static str)>,
        stdout: Vec<u8>,
    }

    /// Captures `script` in a seeded scope; `scheduled` forces the turns
    /// the scope would otherwise see as one physical submission.
    fn seeded(script: &str, scheduled: bool) -> io::Result<Observed> {
        let mut command = crate::shell_command(script, Some("sh"))?;
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let tree = ProcessTree::spawn(&mut command)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            tokio::time::pause();
            let (schedule, mut boundaries) = Schedule::seeded(71);
            let capture = CaptureSchedule::new(Duration::from_secs(30));
            let collect = Collect(Vec::new());
            let operation = async move {
                if scheduled {
                    tree.capture_scheduled(capture, 64, None, collect).await
                } else {
                    tree.capture(capture, 64, None, collect).await
                }
            };
            let mut operation = Box::pin(schedule.scope(operation));
            let mut phases = Vec::new();
            let outcome = loop {
                tokio::select! {
                    biased;
                    boundary = boundaries.recv() => {
                        let boundary = boundary.expect("lost process boundary");
                        phases.push(boundary.name);
                        boundary.release();
                    },
                    outcome = &mut operation => break outcome?,
                }
            };
            schedule.drained().await;
            let output = outcome.result.map_err(|failure| failure.error)?;
            assert!(output.status.success());
            assert_eq!(output.stdout, outcome.handler.0);
            Ok(Observed {
                trace: schedule.trace(),
                boundaries: phases,
                polls: schedule.polls(),
                stdout: output.stdout,
            })
        })
    }

    fn one_submission(observed: &Observed) -> bool {
        observed
            .trace
            .iter()
            .filter(|event| event.starts_with("native:"))
            .count()
            == 1
    }

    #[test]
    fn seeded_capture_is_one_submission_however_output_arrives() -> io::Result<()> {
        let steady = seeded("printf dst", false)?;
        assert_eq!(steady.stdout, b"dst");
        assert!(one_submission(&steady), "{steady:?}");
        // The same output split across readiness events, so a turn at a time
        // takes at least as many turns. The seeded schedule must not see it.
        let split = seeded("printf d; sleep 0.05; printf st", false)?;
        assert_eq!(steady, split, "OS readiness reached the seeded schedule");
        // Negative control: scheduling the same capture a turn at a time
        // exposes its turns, which the oracle rejects.
        let scheduled = seeded("printf dst", true)?;
        assert_eq!(scheduled.stdout, b"dst");
        assert!(!one_submission(&scheduled), "{scheduled:?}");
        Ok(())
    }
}
