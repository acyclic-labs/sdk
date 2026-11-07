//! Hold a real stderr writer outside child containment: the same EOF condition
//! as a detached macOS descendant, without waiting for a watchdog to detect it.
use super::*;
use crate::local::{LocalHarness, LocalHarnessConfig};
use acyclic_native_runtime::ProcessStdio;
use std::io::Write as _;
use std::{fs, sync::Arc};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn failed(step: &str, cause: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    format!("step={step}: {cause}; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_process::children::tests").into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_exited_session_settles_with_an_external_stderr_writer() -> Result {
    served("codex", exercise).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_exited_session_settles_with_an_external_stderr_writer() -> Result {
    served("claude-code", exercise).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pi_exited_session_settles_with_an_external_stderr_writer() -> Result {
    served("pi", exercise).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_child_runs_a_follow_up_after_its_view_is_released() -> Result {
    served("codex", follow_up).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exited_pid_producer_refuses_with_a_retained_stdout_writer() -> Result {
    served("codex", |host, cwd| {
        let (mut sessions, _) = children(host, cwd, "closed-pid")?;
        let (mut running, view) = claim(
            &mut sessions,
            cwd,
            "closed",
            SessionAction::Spawn {
                prompt: "closed producer".into(),
            },
        )?;
        let mut command = native_fixture(&view, "", "0");
        running
            .process
            .spawn(&mut command, running.claim.operation)?;
        let pid = shell_pid(&mut running)?;
        let writer = fs::OpenOptions::new()
            .write(true)
            .open(format!("/proc/{pid}/fd/1"))?;
        running.process.queue_stdin(b"exit\n".to_vec())?;
        assert!(
            running.process.pump_stdin()?,
            "the empty pipe accepts five bytes"
        );
        let error = shell_pid(&mut running).expect_err("an exited producer has no next PID");
        assert!(
            error
                .to_string()
                .contains("read native fixture PID: process exited before PID")
        );
        assert!(
            running.process.read_stdout(&mut [0])?.is_none(),
            "the external writer withholds stdout EOF"
        );
        wait_exit(&mut running)?;
        let completed = sessions
            .refresh("closed", &mut running, &mut [0; 8192])?
            .ok_or("exited child did not settle")?;
        let failure = completed["results"][0]["outcome"]["Failed"]["message"]
            .as_str()
            .ok_or("missing reply was reported successful")?;
        assert!(
            failure.contains("process exited without a completed native reply"),
            "{failure}"
        );
        drop(writer);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exited_child_requeues_captured_output_without_waiting_for_stdout_eof() -> Result {
    served("codex", |host, cwd| {
        let (mut sessions, _) = children(host, cwd, "exit-output")?;
        let (mut running, view) = claim(
            &mut sessions,
            cwd,
            "captured",
            SessionAction::Spawn {
                prompt: "captured reply".into(),
            },
        )?;
        let mut command = native_fixture(&view, "", "0");
        running
            .process
            .spawn(&mut command, running.claim.operation)?;
        let pid = shell_pid(&mut running)?;
        let mut writer = fs::OpenOptions::new()
            .write(true)
            .open(format!("/proc/{pid}/fd/1"))?;
        let mut diagnostic_writer = fs::OpenOptions::new()
            .write(true)
            .open(format!("/proc/{pid}/fd/2"))?;
        running.process.queue_stdin(b"exit\n".to_vec())?;
        wait_exit(&mut running)?;
        // Replay the actual diagnostic with stdout quiescent and both writers
        // retained. Progress must schedule another pass without requiring EOF.
        let diagnostic = running.process.failure_message("");
        assert!(diagnostic.contains("native-diagnostic"));
        diagnostic_writer.write_all(diagnostic.as_bytes())?;
        assert!(
            output(&mut running, &mut [0; 8])?,
            "consumed stderr must schedule the next bounded output pass"
        );
        assert!(running.process.take_output_progress());
        // Replay after direct-child exit. Derive one invalid UTF-8 frame from
        // the captured start, between that start and its original reply, so
        // framing loss cannot be reset by another turn-start. Keeping this
        // writer proves settlement cannot require EOF.
        let captured = include_bytes!("../fixtures/codex-completed.jsonl");
        let first = captured
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or("captured start absent")?
            + 1;
        let mut corrupted = captured[..first].to_vec();
        corrupted[0] = 0xff;
        writer.write_all(&captured[..first])?;
        writer.write_all(&corrupted)?;
        writer.write_all(&captured[first..])?;
        sessions.children.insert("captured".into(), running);
        sessions.ready.push("captured");
        // Small scratch pages force both real output passes to spend all
        // their credit while the captured terminal is still unread.
        let mut page = [0; 8];
        sessions.pump_output(&mut page)?;
        assert!(sessions.children.contains_key("captured"));
        assert!(
            sessions.children["captured"].answer.outcome.is_none(),
            "terminal remains unread after the full passes"
        );
        assert_eq!(
            sessions
                .ready
                .queue()
                .keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["captured"],
            "actual output must schedule exactly its next bounded pass"
        );
        // The original exit boundary is already fixed. A detached writer can
        // keep both pipes active, but its later bytes must neither extend the
        // capture nor postpone settlement of the captured terminal reply.
        writer.write_all(captured)?;
        diagnostic_writer.write_all(diagnostic.as_bytes())?;
        assert_eq!(
            sessions
                .children
                .get_mut("captured")
                .ok_or("original child absent")?
                .process
                .drain_stderr()?
                .read_bytes,
            0,
            "terminal capture must exclude later detached stderr writes"
        );
        sessions.pump_output(&mut page)?;
        assert!(!sessions.children.contains_key("captured"));
        assert!(sessions.ready.queue().keys.is_empty());
        let completed = sessions
            .wait("captured")?
            .ok_or("captured child is still running")?;
        assert_eq!(
            completed["results"][0]["outcome"]["Succeeded"]["text"],
            "PAIRED_NATIVE_DONE"
        );
        drop(writer);
        drop(diagnostic_writer);
        Ok(())
    })
    .await
}

/// Runs `body` against a live workspace server for `host`.
async fn served(host: &str, body: fn(&str, &Path) -> Result) -> Result {
    let checkout = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    let cwd = checkout.path().canonicalize()?;
    let local = Arc::new(
        LocalHarness::open(LocalHarnessConfig::native_host(&cwd).state_directory(state.path()))
            .await?,
    );
    let (ready, readiness) = tokio::sync::oneshot::channel();
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        host_files::serve_until(
            local,
            move || {
                ready.send(()).map_err(|()| {
                    crate::Error::Storage("native child workspace readiness receiver closed".into())
                })
            },
            async move {
                stopped.await.map_err(|cause| {
                    crate::Error::Storage(format!("native child workspace shutdown: {cause}"))
                })
            },
        )
        .await
    });
    let readiness = tokio::select! {
        biased;
        result = &mut server => return Err(failed(
            "start native child workspace", format!("owner ended: {result:?}")
        )),
        ready = readiness => ready.map_err(|cause| failed("start native child workspace", cause)),
    };
    let result = readiness.and_then(|()| body(host, &cwd));
    let _ = shutdown.send(());
    let retirement = server.await;
    assert!(
        matches!(retirement, Ok(Ok(()))),
        "{}",
        failed(
            "retire native child workspace",
            format!("owner={retirement:?}; fixture={result:?}")
        )
    );
    result
}

/// A root session for `host` and its children, as the root's native host
/// holds them.
fn children(host: &str, cwd: &Path, session: &str) -> Result<(NativeChildren, AgentPath)> {
    let root = host_files::request(
        cwd,
        HostAction::Open {
            host: host.into(),
            session: session.into(),
        },
    )?;
    let parent: AgentPath = serde_json::from_value(root["task"].clone())?;
    let held = acyclic_fs::hold_root(cwd)?.create_directories(
        &PathBuf::from(".acyclic-sdk/host-processes").join(parent.id().as_str()),
        acyclic_fs::NativeDurability::Durable,
    )?;
    let ownership = FileLock::try_acquire(
        held.open_file(Path::new("fixture.lock"), true)?,
        FileLockMode::Exclusive,
    )?;
    let sessions = NativeChildren::with(
        host,
        cwd.to_path_buf(),
        parent.clone(),
        Vec::new(),
        ownership,
        held,
    );
    Ok((sessions, parent))
}

/// Sends `action` to `reference` claiming its input, and admits the claimed
/// turn: its process slot and the view its exchange mounted.
fn claim(
    sessions: &mut NativeChildren,
    cwd: &Path,
    reference: &str,
    action: SessionAction,
) -> Result<(Running, PathBuf)> {
    // The accepting exchange itself carries the claimed input: no separate
    // observation or claim precedes the launch.
    let mut accepted = sessions.request(reference, action, true)?;
    let claimed = accepted
        .as_object_mut()
        .and_then(|reply| reply.remove("next"))
        .filter(Value::is_object)
        .ok_or("fixture child input was not claimed")?;
    let receipt = sessions.record(reference, &accepted)?.receipt.clone();
    let view = receipt.confirm(&claimed)?;
    let lease = HostProcessLease::admit(cwd, &receipt, &claimed)?;
    let running = Running {
        process: lease.funded_process()?,
        claim: serde_json::from_value(claimed["claim"].clone())?,
        failure: None,
        events: acyclic_native_runtime::lines::LineBuffer::new(EVENT_LIMIT),
        answer: NativeAnswer::default(),
        settlement: None,
        wake: std::task::Waker::noop().clone(),
        armed: None,
    };
    Ok((running, view))
}

/// An idle child's view is released once its turn settles with nothing
/// queued; a follow-up mounts it again, and its turn runs and settles under
/// the child's original receipt.
fn follow_up(host: &str, cwd: &Path) -> Result {
    let (mut sessions, parent) = children(host, cwd, "follow-up")?;
    let prompts = [
        SessionAction::Spawn {
            prompt: "first".into(),
        },
        SessionAction::Send {
            prompt: "follow-up".into(),
        },
    ];
    for (turn, action) in prompts.into_iter().enumerate() {
        let (mut running, view) = claim(&mut sessions, cwd, "idle", action)?;
        let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
        command
            .args([
                "-c",
                "printf turn > \"turn-$1\"; printf '%s' \"$2\"",
                "fixture",
            ])
            .arg(turn.to_string())
            .arg(include_str!("../fixtures/codex-completed.jsonl"))
            .current_dir(&view)
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        let operation = running.claim.operation;
        running.process.spawn(&mut command, operation)?;
        wait_exit(&mut running)?;
        let completed = sessions
            .refresh("idle", &mut running, &mut [0; 8192])?
            .ok_or_else(|| failed("settle idle child", "exited turn did not settle"))?;
        assert!(completed["next"].is_null(), "turn {turn} queued nothing");
        assert!(completed["status"]["settled"].as_bool().unwrap_or(false));
        let results = completed["results"]
            .as_array()
            .ok_or("settled results absent")?;
        assert!(
            results.last().is_some_and(
                |result| result["outcome"]["Succeeded"]["text"] == "PAIRED_NATIVE_DONE"
            ),
            "turn {turn} recorded its reply: {results:?}"
        );
    }
    host_files::request(
        cwd,
        HostAction::Fs {
            task: parent,
            command: "merge agents/idle".into(),
        },
    )?;
    assert_eq!(fs::read(cwd.join("turn-0"))?, b"turn");
    assert_eq!(fs::read(cwd.join("turn-1"))?, b"turn");
    Ok(())
}

/// Drain actual output before parking on the original process-event producer.
/// Direct-child exit owns this wait; another stderr writer cannot delay it.
fn wait_exit(running: &mut Running) -> Result {
    running.process.take_output_progress();
    let mut page = [0; 8192];
    loop {
        running.process.pump_stdin()?;
        output(running, &mut page)?;
        if running.process.try_wait()?.is_some() {
            running.process.drain_stderr()?;
            running.process.take_output_progress();
            return Ok(());
        }
        if !running.process.take_output_progress() {
            futures::executor::block_on(running.process.readiness()?).into_result()?;
        }
    }
}

/// Read the real shell's PID before releasing its stdin gate. EOF is a
/// terminal refusal rather than a missing file to keep checking.
fn shell_pid(running: &mut Running) -> Result<u32> {
    let mut digits = [0; 11];
    let mut length = 0;
    loop {
        match running.process.read_stdout(&mut digits[length..])? {
            Some(count) if count > 0 => {
                length += count;
                if digits[length - 1] == b'\n' {
                    return std::str::from_utf8(&digits[..length - 1])
                        .ok()
                        .and_then(|pid| pid.parse().ok())
                        .ok_or_else(|| failed("read native fixture PID", "invalid PID"));
                }
                if length == digits.len() {
                    return Err(failed("read native fixture PID", "unterminated PID"));
                }
            }
            None => {
                running.process.drain_stderr()?;
                if running.process.try_wait()?.is_some() {
                    return Err(failed(
                        "read native fixture PID",
                        "process exited before PID",
                    ));
                }
                futures::executor::block_on(running.process.readiness()?).into_result()?;
            }
            _ => return Err(failed("read native fixture PID", "closed producer")),
        }
    }
}

fn native_fixture(view: &Path, events: &str, exit: &str) -> acyclic_native_runtime::ProcessCommand {
    let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
    command.args(["-c", "printf '%s\\n' \"$$\"; IFS= read -r release || { printf 'step=release native fixture: stdin closed; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_process::children::tests\\n' >&2; exit 125; }; printf captured > partial.txt; printf '%s' \"$1\"; printf native-diagnostic >&2; exit \"$2\"", "fixture"])
        .arg(events).arg(exit).current_dir(view).stdin(ProcessStdio::piped()).stdout(ProcessStdio::piped()).stderr(ProcessStdio::piped());
    command
}

fn exercise(host: &str, cwd: &Path) -> Result {
    let (mut sessions, parent) = children(host, cwd, "held-stderr")?;
    let (mut running, view) = claim(
        &mut sessions,
        cwd,
        "held",
        SessionAction::Spawn {
            prompt: "held stderr".into(),
        },
    )?;
    assert!(
        sessions
            .request("held", SessionAction::Next {}, false)
            .is_err(),
        "a competitor cannot claim the handed-off input"
    );
    // The Pi adapter must retain the native assistant refusal even when its
    // process reports success and another writer still holds stderr open.
    let (events, exit) = if host == "pi" {
        (include_str!("../fixtures/pi-assistant-error.jsonl"), "0")
    } else {
        ("", "17")
    };
    let mut command = native_fixture(&view, events, exit);
    let claim = running.claim.operation;
    running.process.spawn(&mut command, claim)?;
    let pid = shell_pid(&mut running)?;
    // A real duplicate writer held by this test process, outside child custody.
    let writer = fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/{pid}/fd/2"))?;
    running.process.queue_stdin(b"exit\n".to_vec())?;
    wait_exit(&mut running)?;
    assert!(
        !running.process.drain_stderr()?.closed,
        "negative input must withhold stderr EOF"
    );
    let completed = sessions
        .refresh("held", &mut running, &mut [0; 8192])?
        .ok_or("exited native session still waits for inherited stderr EOF")?;
    assert!(completed["next"].is_null(), "no further input was queued");
    assert!(completed["status"]["settled"].as_bool().unwrap_or(false));
    let failure = completed["results"][0]["outcome"]["Failed"]["message"]
        .as_str()
        .ok_or("failed result absent")?;
    if host == "pi" {
        assert!(
            failure.starts_with("No API key for provider: fixture"),
            "native semantic refusal was lost: {failure}"
        );
    }
    assert!(failure.ends_with("native-diagnostic"), "{failure}");
    host_files::request(
        cwd,
        HostAction::Fs {
            task: parent,
            command: "merge agents/held".into(),
        },
    )?;
    assert_eq!(fs::read(cwd.join("partial.txt"))?, b"captured");
    drop(writer);
    Ok(())
}
