//! Hold a real stderr writer outside child containment: the same EOF condition
//! as a detached macOS descendant, without waiting for a watchdog to detect it.
use super::*;
use crate::local::{LocalHarness, LocalHarnessConfig};
use acyclic_native_runtime::ProcessStdio;
use notify::Watcher;
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
async fn nested_children_preserve_unmerged_work_across_follow_up_and_merge() -> Result {
    served("codex", follow_up).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exited_pid_producer_refuses_with_a_retained_stdout_writer() -> Result {
    served("codex", |host, cwd| {
        let (mut sessions, _) = children(host, cwd, "closed-pid")?;
        let (mut running, view) = claim(
            &mut sessions,
            "closed",
            SessionAction::Spawn {
                prompt: "closed producer".into(),
            },
        )?;
        let mut command = native_fixture(&view, Path::new("/dev/null"), "0");
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
        let captured = include_bytes!("../fixtures/codex-completed.jsonl");
        let mut events = captured.split_inclusive(|byte| *byte == b'\n');
        let start = events.next().ok_or("captured start absent")?;
        let preview = events.next().ok_or("captured preview absent")?;
        let (mut sessions, _) = children(host, cwd, "exit-output")?;
        for (reference, recovered) in [("recovered", true), ("stale", false)] {
            let (mut running, view) = claim(
                &mut sessions,
                reference,
                SessionAction::Spawn {
                    prompt: "captured reply".into(),
                },
            )?;
            let mut command = native_fixture(&view, Path::new("/dev/null"), "0");
            running
                .process
                .spawn(&mut command, running.claim.operation)?;
            let pid = shell_pid(&mut running)?;
            let mut writer = fs::OpenOptions::new()
                .write(true)
                .open(format!("/proc/{pid}/fd/1"))?;
            running.process.queue_stdin(b"exit\n".to_vec())?;
            wait_exit(&mut running)?;
            // Replay after direct-child exit, keeping the writer to withhold EOF.
            // Derived negative inputs corrupt one original frame: invalid UTF-8
            // before a fresh preview can recover, while malformed JSON after the
            // preview must invalidate it before the original completed terminal.
            let boundary = if recovered {
                start.len()
            } else {
                start.len() + preview.len()
            };
            let mut corrupted = if recovered { start } else { preview }.to_vec();
            corrupted[0] = if recovered { 0xff } else { b'!' };
            writer.write_all(&captured[..boundary])?;
            writer.write_all(&corrupted)?;
            writer.write_all(&captured[boundary..])?;
            sessions.children.insert(reference.into(), running);
            sessions.ready.push(reference);
            // Small scratch pages force both real output passes to spend all
            // their credit while the captured terminal is still unread.
            let mut page = [0; 8];
            sessions.pump_output(&mut page)?;
            assert!(sessions.children.contains_key(reference));
            assert!(
                sessions.children[reference].answer.outcome.is_none(),
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
                [reference],
                "actual output must schedule exactly its next bounded pass"
            );
            sessions.pump_output(&mut page)?;
            assert!(!sessions.children.contains_key(reference));
            assert!(sessions.ready.queue().keys.is_empty());
            let completed = sessions
                .wait(reference)?
                .ok_or("captured child is still running")?;
            if recovered {
                assert_eq!(
                    completed["results"][0]["outcome"]["Succeeded"]["text"],
                    "PAIRED_NATIVE_DONE"
                );
            } else {
                let failure = completed["results"][0]["outcome"]["Failed"]["message"]
                    .as_str()
                    .ok_or("malformed native event retained a stale successful reply")?;
                assert!(failure.contains("malformed JSON"), "{failure}");
            }
            drop(writer);
        }
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
    let owner_directory = cwd.join(".acyclic-sdk/host-files");
    fs::create_dir_all(&owner_directory)?;
    let (changed, mut changes) = tokio::sync::mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if !matches!(&event, Ok(event) if event.kind.is_access()
            || !event.paths.iter().any(|path| path.file_name().is_some_and(|name| name == "owner.json")))
        {
            let _ = changed.try_send(event);
        }
    })?;
    watcher.watch(&owner_directory, notify::RecursiveMode::NonRecursive)?;
    let server_local = Arc::clone(&local);
    let mut server = tokio::spawn(async move { host_files::serve(server_local).await });
    let result = async {
        while !owner_directory.join("owner.json").is_file() {
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
        body(host, &cwd)
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
    let policy: &[&str] = match host {
        "codex" => &[
            "--sandbox",
            "workspace-write",
            "--ask-for-approval",
            "never",
        ],
        "claude-code" => &["--settings", "{}", "--permission-mode", "default"],
        _ => &[],
    };
    let policy = policy.iter().map(|value| (*value).to_owned()).collect();
    let sessions = NativeChildren::own(host, cwd, parent.clone(), policy)?;
    Ok((sessions, parent))
}

/// Sends `action` to `reference` claiming its input, and admits the claimed
/// turn: its process slot and the view its exchange mounted.
fn claim(
    sessions: &mut NativeChildren,
    reference: &str,
    action: SessionAction,
) -> Result<(Running, PathBuf)> {
    // The accepting exchange itself carries the claimed input: no separate
    // observation or claim precedes the launch.
    let mut accepted = sessions.request(reference, action, true)?;
    let original = accepted["next"]["prompt"]
        .as_str()
        .ok_or("fixture child prompt absent")?;
    let (allocation, length) = (original.as_ptr(), original.len());
    let ClaimedInput {
        lease,
        claim,
        view,
        prompt,
        ..
    } = sessions
        .claim(reference, &mut accepted)?
        .ok_or("fixture child input was not claimed")?;
    assert!(accepted.get("next").is_none());
    assert_eq!(prompt.len(), length);
    assert_eq!(
        prompt.as_ptr(),
        allocation,
        "native claim copied the admitted prompt instead of transferring its allocation"
    );
    let running = Running::new(lease.funded_process()?, claim, reference, &sessions.ready);
    Ok((running, view))
}

/// An idle child's view is released once its turn settles with nothing
/// queued; a follow-up mounts it again, and its turn runs and settles under
/// the child's original receipt. A nested child inherits the unmerged parent
/// cut, and each merge changes only its own parent until the outer merge.
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
    let mut previous: Option<TurnClaim> = None;
    for (turn, action) in prompts.into_iter().enumerate() {
        let (mut running, view) = claim(&mut sessions, "idle", action)?;
        if let Some(previous) = &previous {
            assert_eq!(running.claim.agent, previous.agent);
            assert_ne!(running.claim.operation, previous.operation);
            assert_eq!(running.claim.turn, previous.turn + 1);
            assert_eq!(fs::read(view.join("nested.txt"))?, b"turn");
        }
        let mut command = completed_write(&view, &format!("turn-{turn}"));
        let operation = running.claim.operation;
        running.process.spawn(&mut command, operation)?;
        wait_exit(&mut running)?;
        if turn == 0 {
            // Reusing the local name must still create a distinct child of
            // this parent, using the production ownership entrance.
            let mut nested = NativeChildren::own(
                host,
                &view,
                running.claim.agent.clone(),
                sessions.flags.clone(),
            )?;
            let (mut inner, inner_view) = claim(
                &mut nested,
                "idle",
                SessionAction::Spawn {
                    prompt: "nested work".into(),
                },
            )?;
            assert_eq!(
                inner.claim.agent.parent(),
                Some(running.claim.agent.clone())
            );
            assert_eq!(fs::read(inner_view.join("turn-0"))?, b"turn");
            let mut command = completed_write(&inner_view, "nested.txt");
            inner.process.spawn(&mut command, inner.claim.operation)?;
            wait_exit(&mut inner)?;
            finish_turn(&nested, "idle", &mut inner)?;
            let foreign = sessions
                .request(
                    "idle",
                    SessionAction::Complete {
                        claim: inner.claim.clone(),
                        outcome: inner
                            .settlement
                            .clone()
                            .ok_or("nested child retained no settlement")?,
                    },
                    false,
                )
                .expect_err("a parent-local name cannot authorize another child's reply");
            assert!(
                matches!(foreign, Error::Conflict(ref message) if message.contains("native completion selects another child")),
                "{foreign}"
            );
            assert!(!view.join("nested.txt").exists(), "nested work is private");
            host_files::request(
                &view,
                HostAction::Fs {
                    task: running.claim.agent.clone(),
                    command: "merge agents/idle".into(),
                },
            )?;
            assert_eq!(fs::read(view.join("nested.txt"))?, b"turn");
            assert!(!cwd.join("nested.txt").exists(), "outer work is private");
        }
        finish_turn(&sessions, "idle", &mut running)?;
        previous = Some(running.claim.clone());
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
    assert_eq!(fs::read(cwd.join("nested.txt"))?, b"turn");
    Ok(())
}

fn completed_write(view: &Path, file: &str) -> acyclic_native_runtime::ProcessCommand {
    let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
    command
        .args(["-c", "printf turn > \"$1\" || { printf 'step=write native fixture: write failed; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_process::children::tests\\n' >&2; exit 125; }; printf '%s' \"$2\"", "fixture"])
        .arg(file)
        .arg(include_str!("../fixtures/codex-completed.jsonl"))
        .current_dir(view)
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    command
}

fn finish_turn(sessions: &NativeChildren, reference: &str, running: &mut Running) -> Result {
    let completed = sessions
        .refresh(reference, running, &mut [0; 8192])?
        .ok_or_else(|| failed("settle native child", "exited turn did not settle"))?;
    assert!(completed["next"].is_null(), "nothing else was queued");
    assert!(completed["status"]["settled"].as_bool().unwrap_or(false));
    let results = completed["results"]
        .as_array()
        .ok_or("settled results absent")?;
    assert!(
        results
            .last()
            .is_some_and(|result| result["outcome"]["Succeeded"]["text"] == "PAIRED_NATIVE_DONE"),
        "{reference} recorded its reply: {results:?}"
    );
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

fn native_fixture(
    view: &Path,
    events: &Path,
    exit: &str,
) -> acyclic_native_runtime::ProcessCommand {
    let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
    command.args(["-c", "printf '%s\\n' \"$$\"; IFS= read -r release || { printf 'step=release native fixture: stdin closed; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_process::children::tests\\n' >&2; exit 125; }; printf captured > partial.txt; cat \"$1\" || { printf 'step=replay recorded native output: source read failed; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_process::children::tests\\n' >&2; exit 125; }; printf native-diagnostic >&2; exit \"$2\"", "fixture"])
        .arg(events).arg(exit).current_dir(view).stdin(ProcessStdio::piped()).stdout(ProcessStdio::piped()).stderr(ProcessStdio::piped());
    command
}

fn exercise(host: &str, cwd: &Path) -> Result {
    let (mut sessions, parent) = children(host, cwd, "held-stderr")?;
    let (mut running, view) = claim(
        &mut sessions,
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
    // Replay original Claude tool failures before its successful parent result.
    // Both semantic refusals must survive a zero exit and an external writer.
    let claude = if host == "claude-code" {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../../../../plugin/tests/hosts/fixtures/native-claude-workflow-refusal.json"
        ))?;
        let mut events = String::new();
        // Derived scale input repeats an unchanged captured frame. The real
        // tool refusals follow the former cumulative log bound.
        let terminal = serde_json::to_string(
            fixture["events"]
                .as_array()
                .and_then(|events| events.last())
                .ok_or("original terminal absent")?,
        )?;
        while events.len() <= 4 * 1024 * 1024 {
            events.push_str(&terminal);
            events.push('\n');
        }
        for event in fixture["events"]
            .as_array()
            .ok_or("original Claude events absent")?
        {
            events.push_str(&serde_json::to_string(event)?);
            events.push('\n');
        }
        events
    } else {
        String::new()
    };
    let (events, exit) = if host == "pi" {
        (include_str!("../fixtures/pi-assistant-error.jsonl"), "0")
    } else if host == "claude-code" {
        (claude.as_str(), "0")
    } else {
        ("", "17")
    };
    // Keep large captures out of the OS argument vector and outside the view.
    let mut source = tempfile::NamedTempFile::new()?;
    source.write_all(events.as_bytes())?;
    let mut command = native_fixture(&view, source.path(), exit);
    let claim = running.claim.operation;
    running.process.spawn(&mut command, claim)?;
    let pid = shell_pid(&mut running)?;
    // A real duplicate writer held by this test process, outside child custody.
    let writer = fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/{pid}/fd/2"))?;
    // Reject a forged/noop control owner before parking on its real process.
    running.wake.wake_by_ref();
    assert_eq!(sessions.ready.take(), BTreeSet::from(["held".into()]));
    running.process.take_output_progress();
    sessions.arm("held", &mut running);
    assert!(running.armed.is_some() || sessions.ready.queue().keys.contains("held"));
    running.process.queue_stdin(b"exit\n".to_vec())?;
    assert!(running.process.pump_stdin()?);
    sessions.children.insert("held".into(), running);
    // The same owner registration and pump as the deployed coordination
    // server. Every pass waits for the native producer or actual I/O progress.
    while sessions.children.contains_key("held") {
        futures::executor::block_on(std::future::poll_fn(|cx| sessions.ready.poll(cx)));
        sessions.pump()?;
    }
    let completed = sessions
        .wait("held")?
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
    } else if host == "claude-code" {
        assert!(
            failure.contains("toolu_01VERw7PfxSLcASXXwP4rH3W"),
            "{failure}"
        );
        assert!(
            failure.contains("The command line is too long."),
            "{failure}"
        );
    }
    if !events.is_empty() {
        let file: crate::conversation::FileRef =
            serde_json::from_value(completed["events"].clone())?;
        assert_eq!(
            file.descriptor().byte_length(),
            events.len() as u64,
            "native event log discarded original captured bytes after cumulative cap"
        );
        file.descriptor().verify(events.as_bytes(), &mut None)?;
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
