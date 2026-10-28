//! Hold a real stderr writer outside child containment: the same EOF condition
//! as a detached macOS descendant, without waiting for a watchdog to detect it.
use super::*;
use crate::local::{LocalHarness, LocalHarnessConfig};
use std::{fs, process::Stdio, sync::Arc};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_exited_session_settles_with_an_external_stderr_writer() -> Result {
    held_stderr("codex").await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_exited_session_settles_with_an_external_stderr_writer() -> Result {
    held_stderr("claude-code").await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pi_exited_session_settles_with_an_external_stderr_writer() -> Result {
    held_stderr("pi").await
}

async fn held_stderr(host: &str) -> Result {
    let checkout = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    let cwd = checkout.path().canonicalize()?;
    let local = Arc::new(
        LocalHarness::open(LocalHarnessConfig::native_host(&cwd).state_directory(state.path()))
            .await?,
    );
    let server_local = Arc::clone(&local);
    let mut serving = Box::pin(async move { host_files::serve(server_local).await });
    // Construction publishes the endpoint and arms its request watcher before
    // this first Pending. Later progress is driven by the ordinary owner task.
    assert!(futures::poll!(serving.as_mut()).is_pending());
    let server = tokio::spawn(serving);
    let result = exercise(host, &cwd);
    server.abort();
    let _ = server.await;
    Arc::try_unwrap(local)
        .map_err(|_| "fixture retained host server")?
        .disconnect()
        .await?;
    result
}

fn exercise(host: &str, cwd: &Path) -> Result {
    let root = host_files::request(
        cwd,
        HostAction::Open {
            host: host.into(),
            session: "held-stderr".into(),
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
    let sessions = NativeChildren {
        host: host.into(),
        cwd: cwd.to_path_buf(),
        parent: parent.clone(),
        flags: Vec::new(),
        children: BTreeMap::new(),
        prompts: BTreeMap::new(),
        _ownership: ownership,
        _root: held,
    };
    let observed = sessions.request(
        "held",
        SessionAction::Spawn {
            prompt: "held stderr".into(),
        },
    )?;
    let view = PathBuf::from(observed["cwd"].as_str().ok_or("child cwd absent")?);
    let (lease, claimed) = HostProcessLease::claim_child(cwd, &parent, "held")?
        .ok_or("fixture child input was not claimed")?;
    let mut running = Running {
        process: lease.funded_process()?,
        claim: serde_json::from_value(claimed["claim"].clone())?,
        failure: None,
        events: acyclic_native_runtime::lines::LineBuffer::new(EVENT_LIMIT),
        answer: String::new(),
        session: None,
    };
    let mut command = std::process::Command::new("/bin/sh");
    command.args(["-c", "printf '%s\\n' \"$$\"; IFS= read -r release; printf captured > partial.txt; printf native-diagnostic >&2; exit 17"])
        .current_dir(&view).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let claim = running.claim.operation;
    running.process.spawn(&mut command, claim)?;
    let mut pid = Vec::new();
    let pid = loop {
        // Arm before checking so publication cannot fall between inspection
        // and registration. The shared hub parks this thread until an edge.
        let ready = running.process.readiness()?;
        let mut bytes = [0; 32];
        match running.process.read_stdout(&mut bytes)? {
            Some(0) => return Err("fixture stdout closed before its PID frame".into()),
            Some(count) => {
                pid.extend_from_slice(&bytes[..count]);
                if pid.last() == Some(&b'\n') {
                    break std::str::from_utf8(&pid)?.trim_end().parse::<u32>()?;
                }
                continue;
            }
            None => {}
        }
        assert!(
            running.process.try_wait()?.is_none(),
            "fixture exited before exporting its pipe"
        );
        futures::executor::block_on(ready);
    };
    // A real duplicate writer held by this test process, outside child custody.
    let writer = fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/{pid}/fd/2"))?;
    running.process.queue_stdin(b"exit\n".to_vec())?;
    loop {
        let ready = running.process.readiness()?;
        running.process.pump_stdin()?;
        // Consume actual pipe readiness before waiting again. In particular,
        // a diagnostic cannot repeatedly wake an exit wait while left unread.
        running.process.read_stdout(&mut [0; 32])?;
        running.process.drain_stderr()?;
        if running.process.try_wait()?.is_some() {
            break;
        }
        futures::executor::block_on(ready);
    }
    running.process.require_stdin_complete()?;
    assert!(
        !running.process.drain_stderr()?,
        "negative input must withhold stderr EOF"
    );
    assert!(
        sessions.refresh("held", &mut running)?,
        "exited native session still waits for inherited stderr EOF"
    );
    let completed = sessions.request("held", SessionAction::Observe)?;
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
