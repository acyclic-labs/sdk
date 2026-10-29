//! Hold a real stderr writer outside child containment: the same EOF condition
//! as a detached macOS descendant, without waiting for a watchdog to detect it.
use super::*;
use crate::local::{LocalHarness, LocalHarnessConfig};
use std::{fs, process::Stdio, sync::Arc};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

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

/// Runs `body` against a live workspace server for `host`.
async fn served(host: &str, body: fn(&str, &Path, &Path) -> Result) -> Result {
    let checkout = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    let cwd = checkout.path().canonicalize()?;
    let local = Arc::new(
        LocalHarness::open(LocalHarnessConfig::native_host(&cwd).state_directory(state.path()))
            .await?,
    );
    let server_local = Arc::clone(&local);
    let server = tokio::spawn(async move { host_files::serve(server_local).await });
    while !cwd.join(".acyclic-sdk/host-files/owner.json").is_file() {
        assert!(
            !server.is_finished(),
            "fixture workspace server exited before admission"
        );
        tokio::task::yield_now().await;
    }
    let result = body(host, &cwd, state.path());
    server.abort();
    let _ = server.await;
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
        answer: String::new(),
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
        let mut command = std::process::Command::new("/bin/sh");
        command
            .args(["-c", "printf turn > \"turn-$1\"", "fixture"])
            .arg(turn.to_string())
            .current_dir(&view)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let operation = running.claim.operation;
        running.process.spawn(&mut command, operation)?;
        let completed = loop {
            if let Some(completed) = sessions.refresh("idle", &mut running)? {
                break completed;
            }
            std::thread::yield_now();
        };
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

fn exercise(host: &str, cwd: &Path, state: &Path) -> Result {
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
    let mut command = std::process::Command::new("/bin/sh");
    command.args(["-c", "echo $$ > \"$1/pid\"; while test ! -f \"$1/exit\"; do :; done; printf captured > partial.txt; printf native-diagnostic >&2; exit 17", "fixture"])
        .arg(state).current_dir(&view).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let claim = running.claim.operation;
    running.process.spawn(&mut command, claim)?;
    let pid = loop {
        if let Ok(pid) = fs::read_to_string(state.join("pid"))
            && let Ok(pid) = pid.trim().parse::<u32>()
        {
            break pid;
        }
        assert!(
            running.process.try_wait()?.is_none(),
            "fixture exited before exporting its pipe"
        );
        std::thread::yield_now();
    };
    // A real duplicate writer held by this test process, outside child custody.
    let writer = fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/{pid}/fd/2"))?;
    fs::write(state.join("exit"), b"exit")?;
    while running.process.try_wait()?.is_none() {
        std::thread::yield_now();
    }
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
