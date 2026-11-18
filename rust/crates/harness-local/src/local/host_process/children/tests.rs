//! Hold a real stderr writer outside child containment: the same EOF condition
//! as a detached macOS descendant, without waiting for a watchdog to detect it.
use super::*;
use crate::local::{LocalHarness, LocalHarnessConfig};
use acyclic_native_runtime::ProcessStdio;
use std::io::Write as _;
use std::{fs, sync::Arc};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn failed(step: &str, cause: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    format!("step={step}: {cause}; recovery=cargo test --locked -p acyclic-harness --no-default-features --lib local::host_process::children::tests").into()
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
async fn native_reference_aliases_preserve_process_and_parent_ownership() -> Result {
    for host in ["codex", "claude-code", "pi"] {
        served(host, |host, cwd| {
            references(host, cwd)?;
            shared_references(host, cwd)
        })
        .await?;
    }
    Ok(())
}

/// Parent-local names remain independent across roots and a nested fork,
/// even though both roots deliberately share the original checkout.
fn shared_references(host: &str, cwd: &Path) -> Result {
    let (mut left, left_parent) = children(host, cwd, "shared-left")?;
    let (mut right, right_parent) = children(host, cwd, "shared-right")?;
    let spawn = || SessionAction::Spawn {
        prompt: "shared child name".into(),
    };
    let (left_run, left_view) = claim(&mut left, cwd, "shared", spawn())?;
    let left_task = left_run.claim.agent.clone();
    let (right_run, right_view) = claim(&mut right, cwd, "shared", spawn())
        .map_err(|cause| failed("parent-scoped child reference", cause))?;
    let right_task = right_run.claim.agent.clone();
    assert_ne!(left_task, right_run.claim.agent);
    let mut nested = NativeChildren::own(host, cwd, left_task.clone(), left.flags.clone())?;
    let (nested_run, nested_view) = claim(&mut nested, cwd, "shared", spawn())?;
    let nested_task = nested_run.claim.agent.clone();
    assert!(nested_run.claim.agent.is_child_of(&left_task));
    let events = match host {
        "codex" => include_str!("../fixtures/codex-completed.jsonl"),
        "claude-code" => include_str!("../fixtures/claude-code-completed.jsonl"),
        "pi" => include_str!("../fixtures/pi-assistant-error.jsonl"),
        _ => return Err(failed("select captured native output", host)),
    };
    for (sessions, mut running, view, name) in [
        (&mut nested, nested_run, nested_view, "nested"),
        (&mut left, left_run, left_view, "left"),
        (&mut right, right_run, right_view, "right"),
    ] {
        let mut command = native_fixture(&view, events, "0");
        running
            .process
            .spawn(&mut command, running.claim.operation, None)?;
        shell_pid(&mut running)?;
        running.process.queue_stdin(b"release\n".to_vec())?;
        wait_exit(&mut running)?;
        fs::rename(view.join("partial.txt"), view.join(format!("{name}.txt")))?;
        let completed = sessions
            .refresh("shared", &mut running, &mut [0; 8192])?
            .ok_or("shared child did not settle")?;
        assert_eq!(completed["status"]["settled"], true);
        assert!(
            !cwd.join(format!("{name}.txt")).exists(),
            "child write escaped its fork"
        );
    }
    for (name, parent, branch) in [
        ("nested", left_task.clone(), nested_task.to_string()),
        ("left", left_parent.clone(), "agents/shared".into()),
        ("right", right_parent.clone(), right_task.to_string()),
    ] {
        let inspect = |command: &str| {
            host_files::request(
                cwd,
                HostAction::Fs {
                    task: parent.clone(),
                    command: command.into(),
                },
            )
        };
        let diff = inspect(&format!("diff {branch}"))
            .map_err(|cause| failed(&format!("{host} inspect {name} child"), cause))?;
        inspect(&format!("merge {branch}")).map_err(|cause| {
            failed(
                &format!("{host} merge {name} child"),
                format!("{cause}; parent={parent}; diff={diff}"),
            )
        })?;
    }
    for name in ["nested", "left", "right"] {
        assert_eq!(fs::read(cwd.join(format!("{name}.txt")))?, b"captured");
    }
    host_files::request(
        cwd,
        HostAction::Fs {
            task: left_parent,
            command: format!("discard {left_task}"),
        },
    )?;
    host_files::request(
        cwd,
        HostAction::Fs {
            task: right_parent,
            command: "diff agents/shared".into(),
        },
    )?;
    Ok(())
}

fn references(host: &str, cwd: &Path) -> Result {
    let reference = "a".repeat(255);
    let (mut sessions, parent) = children(host, cwd, "reference-custody")?;
    let (mut running, view) = claim(
        &mut sessions,
        cwd,
        &reference,
        SessionAction::Spawn {
            prompt: "work".into(),
        },
    )?;
    let task = running.claim.agent.clone();
    let key = task.to_string();
    let mut command = native_fixture(&view, "", "0");
    running
        .process
        .spawn(&mut command, running.claim.operation, None)?;
    shell_pid(&mut running)?;
    sessions.children.insert(key.clone(), running);
    let replay = sessions.spawn(&reference, "work".into())?;
    assert_eq!(replay["task"], key);
    assert_eq!(sessions.children.len(), 1);
    assert!(sessions.wait(&reference)?.is_none());
    assert!(sessions.wait(&key)?.is_none());
    assert_eq!(sessions.accepted.len(), 1);
    for branch in [key.clone(), format!("agents/{reference}")] {
        let refused = host_files::request(
            cwd,
            HostAction::Fs {
                task: parent.clone(),
                command: format!("merge {branch}"),
            },
        )
        .expect_err("canonical child merge bypassed running-session custody");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Conflict);
        assert!(
            refused
                .to_string()
                .contains("native session is still running"),
            "{refused}"
        );
    }
    let grandchild = crate::local::host_owner::child(&task, "grandchild")?;
    for other in [&parent, &grandchild] {
        assert!(
            matches!(sessions.wait(other.as_str()), Err(Error::Invalid(message))
                if message.contains("only direct children can be waited on")),
            "a non-child path must fail before a storage lookup"
        );
    }
    for other in [&parent, &grandchild] {
        for operation in ["diff", "merge", "discard"] {
            let refused = host_files::request(
                cwd,
                HostAction::Fs {
                    task: parent.clone(),
                    command: format!("{operation} {other}"),
                },
            )
            .expect_err("a full non-child address bypassed parent ownership");
            assert_eq!(refused.code(), crate::wire::ErrorCode::Invalid);
            assert!(
                refused.to_string().contains("only direct children"),
                "{refused}"
            );
        }
    }
    let refused = host_files::request(
        cwd,
        HostAction::Fs {
            task,
            command: format!("merge agents/{reference}"),
        },
    )
    .expect_err("a leaf cannot merge its own changes");
    assert_eq!(refused.code(), crate::wire::ErrorCode::Invalid);
    assert!(
        refused.to_string().contains("only a parent can merge"),
        "{refused}"
    );
    sessions.cancel(&key)?;
    assert!(!sessions.running());
    let local = sessions
        .wait(&reference)?
        .ok_or("cancelled child is still running")?;
    let full = sessions
        .wait(&key)?
        .ok_or("cancelled path is still running")?;
    assert_eq!(local["task"], full["task"]);
    assert_eq!(local["control"], "interrupt");
    sessions.archive(&reference)?;
    for result in [
        sessions.resume(&key, "next".into()),
        sessions.spawn(&reference, "work".into()),
        sessions.spawn(&reference, "different".into()),
        sessions.send(&reference, "next".into()),
        sessions.send(&key, "next".into()),
    ] {
        let refused = result.expect_err("an archived reference cannot resume");
        assert_eq!(refused.code(), crate::wire::ErrorCode::Conflict);
        assert!(
            refused.to_string().contains("spawn a new reference"),
            "{refused}"
        );
    }
    let fresh = sessions.request(
        "fresh",
        SessionAction::Spawn {
            prompt: "new work".into(),
        },
        false,
    )?;
    assert_ne!(fresh["task"], full["task"]);
    sessions.archive("fresh")?;
    Ok(())
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
            .spawn(&mut command, running.claim.operation, None)?;
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
                cwd,
                reference,
                SessionAction::Spawn {
                    prompt: "captured reply".into(),
                },
            )?;
            let mut command = native_fixture(&view, "", "0");
            running
                .process
                .spawn(&mut command, running.claim.operation, None)?;
            let pid = shell_pid(&mut running)?;
            let mut writer = fs::OpenOptions::new()
                .write(true)
                .open(format!("/proc/{pid}/fd/1"))?;
            let mut diagnostic_writer = fs::OpenOptions::new()
                .write(true)
                .open(format!("/proc/{pid}/fd/2"))?;
            running.process.queue_stdin(b"exit\n".to_vec())?;
            wait_exit(&mut running)?;
            let diagnostic = running.process.failure_message("");
            assert!(diagnostic.contains("native-diagnostic"));
            diagnostic_writer.write_all(diagnostic.as_bytes())?;
            assert!(
                output(&mut running, &mut [0; 8])?,
                "consumed stderr must schedule the next bounded output pass"
            );
            assert!(running.process.take_output_progress());
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
            // The original terminal is complete JSON without its final delimiter.
            // Direct-child exit supplies the capture cut while this writer keeps
            // the pipe open, so EOF is not needed to finish that frame.
            writer.write_all(
                captured[boundary..]
                    .strip_suffix(b"\n")
                    .ok_or("captured terminal delimiter absent")?,
            )?;
            let key = sessions.key(reference)?;
            sessions.children.insert(key.clone(), running);
            sessions.ready.push(&key);
            // Small scratch pages force both real output passes to spend all
            // their credit while the captured terminal is still unread.
            let mut page = [0; 8];
            sessions.pump_output(&mut page)?;
            assert!(sessions.children.contains_key(&key));
            assert!(
                sessions.children[&key].answer.outcome.is_none(),
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
                [key.as_str()],
                "actual output must schedule exactly its next bounded pass"
            );
            // Exit already selected both capture boundaries. Detached writers
            // can retain the pipes but cannot extend this original observation.
            writer.write_all(captured)?;
            diagnostic_writer.write_all(diagnostic.as_bytes())?;
            assert_eq!(
                sessions
                    .children
                    .get_mut(&key)
                    .ok_or("original child absent")?
                    .process
                    .drain_stderr()?
                    .read_bytes,
                0,
                "terminal capture must exclude later detached stderr writes"
            );
            sessions.pump_output(&mut page)?;
            assert!(!sessions.children.contains_key(&key));
            assert!(sessions.ready.queue().keys.is_empty());
            let completed = sessions
                .wait(reference)?
                .ok_or("captured child is still running")?;
            if recovered {
                assert_eq!(
                    completed["results"][0]["outcome"]["Succeeded"]["text"], "PAIRED_NATIVE_DONE",
                    "exited native terminal without LF was discarded"
                );
            } else {
                let failure = completed["results"][0]["outcome"]["Failed"]["message"]
                    .as_str()
                    .ok_or("malformed native event retained a stale successful reply")?;
                assert!(failure.contains("malformed JSON"), "{failure}");
            }
            drop(writer);
            drop(diagnostic_writer);
        }
        Ok(())
    })
    .await
}

/// Runs `body` against a live workspace server for `host`.
pub(super) async fn served(host: &str, body: fn(&str, &Path) -> Result) -> Result {
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
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        readiness.and_then(|()| body(host, &cwd))
    }));
    let _ = shutdown.send(());
    let retirement = server.await;
    assert!(
        matches!(retirement, Ok(Ok(()))),
        "{}",
        failed(
            "retire native child workspace",
            format!("owner={retirement:?}")
        )
    );
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// A root session for `host` and its children, as the root's native host
/// holds them.
pub(super) fn children(
    host: &str,
    cwd: &Path,
    session: &str,
) -> Result<(NativeChildren, AgentPath)> {
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
        "pi" => &[],
        _ => return Err(failed("select native host policy", host)),
    };
    let policy = policy
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    let flags = launch::parent_flags(host, &policy, cwd)?;
    let sessions = NativeChildren::own(host, cwd, parent.clone(), flags)?;
    Ok((sessions, parent))
}

/// Sends `action` to `reference` claiming its input, and admits the claimed
/// turn: its process slot and the view its exchange mounted.
pub(super) fn claim(
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
    let key = sessions.key(reference)?;
    let receipt = sessions.record(&key, &accepted)?.receipt.clone();
    let view = receipt.confirm(&claimed)?;
    let lease = HostProcessLease::admit(cwd, &receipt, &claimed)?;
    let running = Running::new(
        lease.funded_process()?,
        serde_json::from_value(claimed["claim"].clone())?,
        &key,
        &sessions.ready,
    );
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
        let (mut running, view) = claim(&mut sessions, cwd, "idle", action)?;
        if let Some(previous) = &previous {
            assert_eq!(running.claim.agent, previous.agent);
            assert_ne!(running.claim.operation, previous.operation);
            assert_eq!(running.claim.turn, previous.turn + 1);
            assert_eq!(fs::read(view.join("nested.txt"))?, b"turn");
        }
        let events = include_str!("../fixtures/codex-completed.jsonl");
        let events = if turn == 0 {
            events
        } else {
            events
                .strip_suffix('\n')
                .ok_or("captured terminal delimiter absent")?
        };
        crate::local::host_owner::check_native_event_resume(events.as_bytes());
        let mut command = completed_write(&view, &format!("turn-{turn}"), events);
        let operation = running.claim.operation;
        running.process.spawn(&mut command, operation, None)?;
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
                &view,
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
            let mut command = completed_write(
                &inner_view,
                "nested.txt",
                include_str!("../fixtures/codex-completed.jsonl"),
            );
            inner
                .process
                .spawn(&mut command, inner.claim.operation, None)?;
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
            assert_eq!(foreign.code(), crate::wire::ErrorCode::Conflict);
            assert!(
                foreign
                    .to_string()
                    .contains("native completion selects another child"),
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

fn completed_write(
    view: &Path,
    file: &str,
    events: &str,
) -> acyclic_native_runtime::ProcessCommand {
    let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
    command
        .args(["-c", "printf turn > \"$1\" || { printf 'step=write native fixture: write failed; recovery=cargo test --locked -p acyclic-harness --no-default-features --lib local::host_process::children::tests\\n' >&2; exit 125; }; printf '%s' \"$2\"", "fixture"])
        .arg(file)
        .arg(events)
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
pub(super) fn shell_pid(running: &mut Running) -> Result<u32> {
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

pub(super) fn native_fixture(
    view: &Path,
    events: &str,
    exit: &str,
) -> acyclic_native_runtime::ProcessCommand {
    let mut command = acyclic_native_runtime::ProcessCommand::new("/bin/sh");
    command.args(["-c", "printf '%s\\n' \"$$\"; IFS= read -r release || { printf 'step=release native fixture: stdin closed; recovery=cargo test --locked -p acyclic-harness --no-default-features --lib local::host_process::children::tests\\n' >&2; exit 125; }; printf captured > partial.txt; printf '%s' \"$1\"; printf native-diagnostic >&2; exit \"$2\"", "fixture"])
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
    running.process.spawn(&mut command, claim, None)?;
    let lease = running
        .process
        .lease
        .as_ref()
        .ok_or("started native lease absent")?;
    let mut log = lease.root.open_file(&event_log(claim), false)?;
    let mut context = String::new();
    std::io::Read::read_to_string(&mut log, &mut context)?;
    let first: serde_json::Value =
        serde_json::from_str(context.lines().next().ok_or("native context absent")?)?;
    assert_eq!(
        first,
        serde_json::json!({"type":"acyclic.host_context", "version":1, "task":lease.task, "cwd":view}),
        "the original native transcript must identify its admitted view before child output"
    );
    let pid = shell_pid(&mut running)?;
    // A real duplicate writer held by this test process, outside child custody.
    let writer = fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/{pid}/fd/2"))?;
    let key = running.claim.agent.to_string();
    // Reject a forged/noop control owner before parking on its real process.
    running.wake.wake_by_ref();
    assert_eq!(sessions.ready.take(), HashSet::from([key.clone()]));
    running.process.take_output_progress();
    sessions.arm(&key, &mut running);
    assert!(running.armed.is_some() || sessions.ready.queue().keys.contains(&key));
    running.process.queue_stdin(b"exit\n".to_vec())?;
    assert!(running.process.pump_stdin()?);
    // The production owner already holds the sole readiness observation.
    // Hand it the process without installing a competing test-side wait.
    // `writer` stays live through settlement, so stderr cannot reach EOF.
    sessions.children.insert(key.clone(), running);
    // The same owner registration and pump as the deployed coordination
    // server. Every pass waits for the native producer or actual I/O progress.
    while sessions.children.contains_key(&key) {
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
        let file: crate::conversation::FileRef =
            serde_json::from_value(completed["events"].clone())?;
        context.push_str(events);
        file.descriptor().verify(context.as_bytes(), &mut None)?;
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
