//! Hold a real stderr writer outside child containment: the same EOF condition
//! as a detached macOS descendant, without waiting for a watchdog to detect it.
use super::*;
use crate::local::{LocalHarness, LocalHarnessConfig};
use acyclic_native_runtime::ProcessStdio;
use notify::Watcher;
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
async fn a_closed_pid_producer_ends_admission() -> Result {
    served("codex", |host, cwd, _| {
        let (mut sessions, _) = children(host, cwd, "closed-pid")?;
        let (mut running, view) = claim(
            &mut sessions,
            cwd,
            "closed",
            SessionAction::Spawn {
                prompt: "closed producer".into(),
            },
        )?;
        let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
        command
            .args(["-c", "exit 0"])
            .current_dir(&view)
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        running
            .process
            .spawn(&mut command, running.claim.operation)?;
        let error = shell_pid(&mut running).expect_err("a closed producer has no PID");
        assert!(
            error
                .to_string()
                .contains("read native fixture PID: closed producer")
        );
        wait_exit(&mut running)?;
        assert!(sessions.refresh("closed", &mut running)?.is_some());
        Ok(())
    })
    .await
}

/// Runs `body` against a live workspace server for `host`.
async fn served(host: &str, body: fn(&str, &Path, &Path) -> Result) -> Result {
    let checkout = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    let cwd = checkout.path().canonicalize()?;
    let local = Arc::new(
        LocalHarness::open(LocalHarnessConfig::native_host(&cwd).state_directory(state.path()))
            .await?,
    );
    let (changed, mut changes) = tokio::sync::mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if !matches!(&event, Ok(event) if event.kind.is_access()) {
            let _ = changed.try_send(event);
        }
    })?;
    watcher.watch(&cwd, notify::RecursiveMode::Recursive)?;
    let server_local = Arc::clone(&local);
    let mut server = tokio::spawn(async move { host_files::serve(server_local).await });
    let result = async {
        while !cwd.join(".acyclic-sdk/host-files/owner.json").is_file() {
            tokio::select! {
                result = &mut server => return Err(failed(
                    "start native child workspace", format!("owner ended: {result:?}")
                )),
                event = changes.recv() => match event {
                    Some(event) => { event.map_err(|cause| failed("watch native child workspace", cause))?; }
                    None => return Err(failed("start native child workspace", "notification producer closed")),
                },
            }
        }
        drop(watcher);
        body(host, &cwd, state.path())
    }.await;
    server.abort();
    if !server.is_finished() {
        let _ = server.await;
    }
    Arc::try_unwrap(local)
        .map_err(|_| "fixture retained host server")?
        .disconnect()
        .await?;
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
        answer: Outcome::Succeeded(String::new()),
        settlement: None,
        wake: std::task::Waker::noop().clone(),
        armed: None,
    };
    Ok((running, view))
}

/// An idle child's view is released once its turn settles with nothing
/// queued; a follow-up mounts it again, and its turn runs and settles under
/// the child's original receipt.
fn follow_up(host: &str, cwd: &Path, _: &Path) -> Result {
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
            .args(["-c", "printf turn > \"turn-$1\"", "fixture"])
            .arg(turn.to_string())
            .current_dir(&view)
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        let operation = running.claim.operation;
        running.process.spawn(&mut command, operation)?;
        wait_exit(&mut running)?;
        let completed = sessions
            .refresh("idle", &mut running)?
            .ok_or_else(|| failed("settle idle child", "exited turn did not settle"))?;
        assert!(completed["next"].is_null(), "turn {turn} queued nothing");
        assert!(completed["status"]["settled"].as_bool().unwrap_or(false));
        let results = completed["results"]
            .as_array()
            .ok_or("settled results absent")?;
        assert!(
            results
                .last()
                .is_some_and(|result| result["outcome"].get("Succeeded").is_some()),
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
    loop {
        running.process.pump_stdin()?;
        output(running)?;
        if running.process.try_wait()?.is_some() {
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
                futures::executor::block_on(running.process.readiness()?).into_result()?;
            }
            _ => return Err(failed("read native fixture PID", "closed producer")),
        }
    }
}

fn exercise(host: &str, cwd: &Path, _: &Path) -> Result {
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
            .request("held", SessionAction::Next, false)
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
    let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
    command.args(["-c", "printf '%s\\n' \"$$\"; IFS= read -r release || { printf 'step=release native fixture: stdin closed; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_process::children::tests\\n' >&2; exit 125; }; printf captured > partial.txt; printf '%s' \"$1\"; printf native-diagnostic >&2; exit \"$2\"", "fixture"])
        .arg(events).arg(exit).current_dir(&view).stdin(ProcessStdio::piped()).stdout(ProcessStdio::piped()).stderr(ProcessStdio::piped());
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
        !running.process.drain_stderr()?,
        "negative input must withhold stderr EOF"
    );
    let completed = sessions
        .refresh("held", &mut running)?
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
