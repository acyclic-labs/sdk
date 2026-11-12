//! Real process and filesystem events control every ordering in this test.
use super::*;
use crate::{
    conversation::ContentGrant,
    runtime::{ContentBindings, ToolContext},
};
use acyclic_stream::{StreamProvider, StreamProviderExt as _};
use futures::{FutureExt, TryStreamExt};
use std::io::{Read, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn child_executable(harness: &LocalHarness) -> Result<std::path::PathBuf> {
    let scratch = harness
        .workspace
        .as_ref()
        .unwrap()
        .mounts
        .scratch_root
        .as_ref()
        .unwrap();
    let child = scratch.join(format!("shell-job-child{}", std::env::consts::EXE_SUFFIX));
    // The command's grant includes scratch, while the host test image is outside it.
    std::fs::copy(std::env::current_exe().map_err(storage)?, &child).map_err(storage)?;
    Ok(child)
}
async fn listener_executor(
    harness: &LocalHarness,
    ports: Vec<u16>,
) -> Result<Arc<dyn crate::tool::ToolExecutor>> {
    use acyclic_native_runtime::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork};
    let workspace = harness.workspace.as_ref().unwrap();
    // Select the fixture's held temporary home through MountOptions::view_root,
    // as native-host admission does; never use the ambient shared-view home.
    let view_home = Arc::new(workspace.held_task_mount_root()?);
    let view_home = workspace.task_view_admission.get_or_init(|| view_home);
    let mut read = vec![executable()?.parent().unwrap().to_path_buf()];
    #[cfg(target_os = "linux")]
    read.extend(
        ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
            .map(std::path::PathBuf::from)
            .into_iter()
            .filter(|path| path.exists()),
    );
    #[cfg(target_os = "macos")]
    read.extend(
        ["/System/Library", "/usr", "/bin", "/lib"]
            .map(std::path::PathBuf::from)
            .into_iter()
            .filter(|path| path.exists()),
    );
    #[cfg(windows)]
    read.push(
        std::env::var_os("SystemRoot")
            .ok_or_else(|| Error::Invalid("SystemRoot is absent".into()))?
            .into(),
    );
    let write = workspace
        .mounts
        .native_mounts()
        .filter_map(|(_, root, mount)| {
            (!mount.mode.read_only()).then(|| {
                // These fixture-owned parents survive projection retirement;
                // their volume directories are created only during execution.
                mount.view.source.as_ref().map_or_else(
                    || root.to_path_buf(),
                    |source| source.binding_root.path().to_path_buf(),
                )
            })
        })
        .chain(workspace.mounts.scratch_root.iter().cloned())
        .chain(std::iter::once(view_home.path().to_path_buf()))
        .collect::<Vec<_>>();
    let grant = ExecutionGrant::composed(
        &workspace.path,
        ExecutionAccess::WorkspaceWrite,
        write,
        read,
        ExecutionNetwork::TcpPorts(ports.into_iter().collect()),
    )
    .map_err(storage)?;
    Ok(workspace
        .executor(
            (&harness.project, &harness.files, 1 << 20),
            harness.clock.clone(),
            &harness.signed,
            &harness.verifier,
            Some(grant),
        )
        .await?
        .0)
}
#[test]
fn shell_job_child() {
    if let Ok(address) = std::fs::read_to_string("shell-job.exit") {
        let mut release = std::net::TcpStream::connect(address).unwrap();
        let mut byte = [0];
        release.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [b'E']);
        return;
    }
    let Ok(address) = std::fs::read_to_string("shell-job.addr") else {
        return;
    };
    if let Ok(home) = std::fs::read_to_string("shell-job.timeout") {
        std::fs::write("retained", b"before-stop").unwrap();
        std::fs::write(
            std::path::PathBuf::from(home).join("timeout-retained"),
            b"before-stop",
        )
        .unwrap();
    }
    let mut gate = std::net::TcpStream::connect(address).unwrap();
    println!("JOB_READY");
    std::io::stdout().flush().unwrap();
    gate.write_all(b"R").unwrap();
    let mut release = [0];
    gate.read_exact(&mut release).unwrap();
    assert_eq!(release, [b'G']);
    std::io::stdout().write_all(&vec![b'x'; 80_000]).unwrap();
    eprintln!("JOB_STDERR");
    println!("JOB_DONE");
    std::fs::write("completed", b"committed").unwrap();
}

async fn terminal(harness: &LocalHarness, directory: &str) -> Result<serde_json::Value> {
    let physical = harness.workspace.as_ref().unwrap();
    let volume = &harness.scratch;
    let id = physical
        .filesystem
        .workspace_id(&volume.storage_name(&mut None)?)
        .map_err(storage)?;
    let path =
        acyclic_fs::session::changes_path(&physical.filesystem, id.volume_id()).map_err(storage)?;
    let streams = harness.agent_stream.as_ref().unwrap();
    let tail = streams.tail(path.clone()).await.map_err(storage)?;
    let mut changes = acyclic_stream::records_only(
        streams
            .follow(acyclic_stream::FollowRequest::new(path, tail))
            .await
            .map_err(storage)?,
    );
    let grant = ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        volume,
        "",
        &mut None,
    )?;
    loop {
        if let Some((_, bytes, _)) = harness
            .files
            .read_private_path_indexed(
                volume,
                &grant,
                &format!("{directory}/result.json"),
                None,
                Some(1 << 20),
                Default::default(),
            )
            .await?
        {
            return serde_json::from_slice(&bytes).map_err(storage);
        }
        changes
            .try_next()
            .await
            .map_err(storage)?
            .ok_or_else(|| Error::Storage("job output follow closed".into()))?;
        // Every re-read follows a committed filesystem event, never a timer.
    }
}

async fn ready_connection(
    harness: &LocalHarness,
    listener: &tokio::net::TcpListener,
    directory: &str,
) -> Result<tokio::net::TcpStream> {
    tokio::select! {
        connection = listener.accept() => connection.map(|(socket, _)| socket).map_err(storage),
        ended = terminal(harness, directory) => Err(Error::Conflict(format!(
            "background shell ended before child readiness: {}", ended?
        ))),
    }
}

#[tokio::test]
async fn cancelled_foreground_call_retains_capture_and_publication_after_cleanup() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let model = crate::model::Model::new("scripted".into(), "test".into(), "1".into())?;
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (
            model,
            Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ),
    )?)
    .await?;
    let writer = crate::filesystem::FilesystemContentPublisher::new(
        harness.files.clone(),
        &harness.scratch,
        &harness.verifier,
        &harness.signed,
        1 << 20,
        None,
    )?;
    let task = Arc::new(harness.harness.agent_context(
        &crate::Capabilities::new(Vec::<String>::new()),
        ContentBindings {
            volume: harness.scratch.clone(),
            reader: harness.client_reader()?,
            writer: Some(writer),
        },
    )?);
    let context = ToolContext::new(task, crate::OperationId::new(), "foreground-cancellation")?;
    let output_directory = format!("tools/{}/{}", context.turn_id(), context.operation_id());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(storage)?;
    std::fs::write(
        directory.path().join("shell-job.addr"),
        listener.local_addr().map_err(storage)?.to_string(),
    )
    .map_err(storage)?;
    let physical = harness.workspace.as_ref().unwrap();
    let (_, home) = physical.mounts.named(Some("home"))?;
    let home_volume = home.view.volume.clone();
    let home_root = physical
        .mounts
        .native_root_for(home)
        .ok_or_else(|| Error::Invalid("home has no native root".into()))?;
    std::fs::write(
        directory.path().join("shell-job.timeout"),
        home_root.to_str().unwrap(),
    )
    .map_err(storage)?;
    let invocation = ToolInvocation {
        operation_id: context.operation_id(),
        call_id: "foreground-cancellation".into(),
        name: "shell".into(),
        arguments: serde_json::json!({"command": [child_executable(&harness)?, "--exact", "local::checkout_tools::shell::job::tests::shell_job_child", "--nocapture"]}),
    };
    let executor = listener_executor(
        &harness,
        vec![listener.local_addr().map_err(storage)?.port()],
    )
    .await?;
    let mut call = executor.execute(invocation, Some(context));
    let mut gate = tokio::select! {
        connection = listener.accept() => connection.map(|(socket, _)| socket).map_err(storage)?,
        result = &mut call => panic!("shell returned before its writes: {result:?}"),
    };
    let mut byte = [0];
    gate.read_exact(&mut byte).await.map_err(storage)?;
    assert_eq!(byte, [b'R']);
    drop(call);
    let published = terminal(&harness, &output_directory).await?;
    assert_eq!(published["stop"]["cleanup_completed"], true, "{published}");
    let physical = harness.workspace.as_ref().unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let head = physical
        .view()
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    let retained = head
        .read(
            "/retained",
            64,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    assert_eq!(retained.as_ref(), b"before-stop");
    let grant = ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        &home_volume,
        "",
        &mut None,
    )?;
    let (_, retained) = harness
        .files
        .read_private_path(
            &home_volume,
            &grant,
            "timeout-retained",
            None,
            64,
            Default::default(),
        )
        .await?;
    assert_eq!(retained.as_ref(), b"before-stop");
    assert_eq!(gate.read(&mut byte).await.map_err(storage)?, 0);

    harness.disconnect().await
}

#[tokio::test]
async fn background_startup_deadline_bounds_volume_admission() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let model = crate::model::Model::new("scripted".into(), "test".into(), "1".into())?;
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (
            model,
            Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ),
    )?)
    .await?;
    let writer = crate::filesystem::FilesystemContentPublisher::new(
        harness.files.clone(),
        &harness.scratch,
        &harness.verifier,
        &harness.signed,
        1 << 20,
        None,
    )?;
    let task = Arc::new(harness.harness.agent_context(
        &crate::Capabilities::new(Vec::<String>::new()),
        ContentBindings {
            volume: harness.scratch.clone(),
            reader: harness.client_reader()?,
            writer: Some(writer),
        },
    )?);
    let context = ToolContext::new(task, crate::OperationId::new(), "background-admission")?;
    let output_directory = format!("tools/{}/{}", context.turn_id(), context.operation_id());
    let physical = harness.workspace.as_ref().unwrap();
    let (_, home) = physical.mounts.named(Some("home"))?;
    let source = home.view.source.as_ref().unwrap();
    let held = crate::sync::owned::lock_owned(StateArc::clone(&source.execution)).await;
    let invocation = ToolInvocation {
        operation_id: context.operation_id(),
        call_id: "background-admission".into(),
        name: "shell".into(),
        arguments: serde_json::json!({
            "command": [child_executable(&harness)?, "--exact", "local::checkout_tools::shell::job::tests::shell_job_child"],
            "timeout_ms": 20,
            "background": true,
        }),
    };
    let (schedule, boundaries) = acyclic_native_runtime::testing::Schedule::seeded(83);
    schedule.set_unix_millis(acyclic_stream::UnixMillisClock::now_unix_millis(
        &acyclic_stream::SystemUnixMillisClock,
    ));
    crate::test_support::run_native_schedule(&schedule, boundaries, async {
        tokio::time::pause();
        let executor = &harness.harness.target(None)?.executor;
        let result = executor.execute(invocation, Some(context)).await;
        assert!(
            matches!(
                result,
                Err(Error::Process(crate::ProcessError::Timeout { .. }))
            ),
            "{result:?}"
        );
        assert!(
            StateArc::clone(&physical.state).try_lock_owned().is_err(),
            "worker did not retain its admitted checkout"
        );
        drop(held);
        let published = terminal(&harness, &output_directory).await?;
        assert!(published.get("error").is_some(), "{published}");
        Ok::<_, Error>(())
    })
    .await?;
    harness.disconnect().await
}

#[tokio::test]
async fn shell_background_jobs_keep_capture_and_cancel_independently() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let model = crate::model::Model::new("scripted".into(), "test".into(), "1".into())?;
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (
            model,
            Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ),
    )?)
    .await?;
    let writer = crate::filesystem::FilesystemContentPublisher::new(
        harness.files.clone(),
        &harness.scratch,
        &harness.verifier,
        &harness.signed,
        1 << 20,
        None,
    )?;
    let task = Arc::new(harness.harness.agent_context(
        &crate::Capabilities::new(Vec::<String>::new()),
        ContentBindings {
            volume: harness.scratch.clone(),
            reader: harness.client_reader()?,
            writer: Some(writer),
        },
    )?);
    let executor = &harness.harness.target(None)?.executor;
    let child = child_executable(&harness)?;
    let selector = "local::checkout_tools::shell::job::tests::shell_job_child";
    let mut jobs = Vec::new();
    let mut gates = Vec::new();
    for (name, requires_ready) in [
        ("early", false),
        ("one", true),
        ("two", true),
        ("disconnect", true),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(storage)?;
        std::fs::create_dir(directory.path().join(name)).map_err(storage)?;
        let exit = if requires_ready {
            std::fs::write(
                directory.path().join(name).join("shell-job.addr"),
                listener.local_addr().map_err(storage)?.to_string(),
            )
            .map_err(storage)?;
            None
        } else {
            let exit = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(storage)?;
            std::fs::write(
                directory.path().join(name).join("shell-job.exit"),
                exit.local_addr().map_err(storage)?.to_string(),
            )
            .map_err(storage)?;
            Some(exit)
        };
        let context = ToolContext::new(task.clone(), crate::OperationId::new(), name)?;
        let invocation = ToolInvocation {
            operation_id: context.operation_id(),
            call_id: name.into(),
            name: "shell".into(),
            arguments: serde_json::json!({"command": [child, "--exact", selector, "--nocapture"], "cwd": name, "background": true}),
        };
        let mut ports = vec![listener.local_addr().map_err(storage)?.port()];
        if let Some(exit) = &exit {
            ports.push(exit.local_addr().map_err(storage)?.port());
        }
        let executor = listener_executor(&harness, ports).await?;
        eprintln!("shell job {name}: executing");
        let result = executor
            .execute(invocation.clone(), Some(context.clone()))
            .await?;
        assert_eq!(result.value["background"], true, "{result:?}");
        eprintln!("shell job {name}: awaiting child readiness");
        if let Some(exit) = exit {
            // Keep the child alive until admission has returned. Releasing it
            // then proves terminal delivery ends a wait for a connection it
            // will never make, without depending on scheduling or a timer.
            let mut release = ready_connection(
                &harness,
                &exit,
                result.value["output_directory"].as_str().unwrap(),
            )
            .await?;
            release.write_all(b"E").await.map_err(storage)?;
        }
        let connection = ready_connection(
            &harness,
            &listener,
            result.value["output_directory"].as_str().unwrap(),
        )
        .await;
        if !requires_ready {
            // A real child exits without connecting. Its committed result must
            // end readiness even when no connection can ever be produced.
            assert!(
                matches!(&connection, Err(Error::Conflict(_))),
                "{connection:?}"
            );
            continue;
        }
        let mut gate = connection?;
        let mut ready = [0];
        gate.read_exact(&mut ready).await.map_err(storage)?;
        assert_eq!(ready, [b'R']);
        eprintln!("shell job {name}: ready");
        // Reconciliation recovers the original durable admission, without
        // restarting the command or waiting for its lifetime to end.
        assert_eq!(
            executor
                .reconcile(invocation, Some(context.clone()))
                .await?
                .unwrap()
                .value,
            result.value
        );
        jobs.push((context, result.value));
        gates.push(gate);
    }
    // A foreground command can run while both independent jobs are waiting.
    eprintln!("shell jobs ready: executing foreground command");
    let context = ToolContext::new(task, crate::OperationId::new(), "foreground")?;
    let result = executor
        .execute(
            ToolInvocation {
                operation_id: context.operation_id(),
                call_id: "foreground".into(),
                name: "shell".into(),
                arguments: serde_json::json!({"command": "echo foreground"}),
            },
            Some(context),
        )
        .await?;
    assert_eq!(result.value["exit_code"], 0);
    eprintln!("foreground complete: cancelling first shell job");
    std::fs::write(
        directory.path().join(jobs[0].1["cancel"].as_str().unwrap()),
        b"cancel",
    )
    .map_err(storage)?;
    let cancelled = terminal(&harness, jobs[0].1["output_directory"].as_str().unwrap()).await?;
    assert_eq!(cancelled["stop"]["kind"], "control_stopped", "{cancelled}");
    assert_eq!(cancelled["stop"]["cleanup_completed"], true);
    assert!(!directory.path().join("one/completed").exists());
    eprintln!("first job cancelled: releasing second shell job");
    let mut byte = [0];
    assert!(
        gates[1].read(&mut byte).now_or_never().is_none(),
        "cancelling one job closed the other"
    );
    gates[1].write_all(b"G").await.map_err(storage)?;
    let finished = terminal(&harness, jobs[1].1["output_directory"].as_str().unwrap()).await?;
    assert_eq!(finished["exit_code"], 0, "{finished}");
    assert!(finished["omitted_bytes"].as_u64().unwrap() > 0);
    eprintln!("second job completed: inspecting captured output");
    let grant = ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        &harness.scratch,
        "",
        &mut None,
    )?;
    let spill_path = finished["spill"]
        .as_str()
        .unwrap()
        .strip_prefix("volume:///output/")
        .unwrap();
    let (_, spill) = harness
        .files
        .read_private_path(
            &harness.scratch,
            &grant,
            spill_path,
            None,
            1 << 20,
            Default::default(),
        )
        .await?;
    // Stdout and stderr are independent pipes. Their observed chunks may
    // interleave without losing a byte of either stream.
    assert_eq!(spill.iter().filter(|byte| **byte == b'x').count(), 80_000);
    assert!(spill.windows(10).any(|bytes| bytes == b"JOB_STDERR"));
    assert!(spill.windows(8).any(|bytes| bytes == b"JOB_DONE"));
    assert_eq!(
        std::fs::read(directory.path().join("two/completed")).map_err(storage)?,
        b"committed"
    );
    drop(jobs);
    eprintln!("disconnecting shell workspace owner");
    harness.disconnect().await?;
    eprintln!("workspace owner disconnected: awaiting child socket closure");
    let mut byte = [0];
    assert_eq!(gates[2].read(&mut byte).await.map_err(storage)?, 0);
    assert!(!directory.path().join("disconnect/completed").exists());
    Ok(())
}

#[tokio::test]
async fn cancellation_watches_the_target_of_a_home_alias() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let target = directory.path().join("mounted");
    let alias = directory.path().join("home");
    std::fs::create_dir(&target).map_err(storage)?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &alias).map_err(storage)?;
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(&target, &alias).map_err(storage)?;
    let operation = crate::OperationId::new();
    let cancellation = Cancellation::open(&alias, operation)?;
    // The wait is pending on its watch before the control file appears.
    let (waited, written) = futures::future::join(cancellation.wait(), async {
        std::fs::write(alias.join(format!("shell-{operation}.cancel")), b"cancel")
    })
    .await;
    written.map_err(storage)?;
    waited?;

    // An already observed invalidation is not a permanently ready tree batch.
    // The next wait parks until the selected input actually appears.
    let operation = crate::OperationId::new();
    let cancellation = Cancellation::open(&alias, operation)?;
    cancellation.watcher.invalidate();
    let mut waiting = std::pin::pin!(cancellation.wait());
    assert!(futures::poll!(&mut waiting).is_pending());
    std::fs::write(alias.join(format!("shell-{operation}.cancel")), b"cancel").map_err(storage)?;
    waiting.await?;

    // An alias retarget after registration must not leave cancellation bound
    // to the old home. The original wait observes both the move and new input.
    let replacement = directory.path().join("replacement");
    std::fs::create_dir(&replacement).map_err(storage)?;
    let operation = crate::OperationId::new();
    let cancellation = Cancellation::open(&alias, operation)?;
    let mut waiting = std::pin::pin!(cancellation.wait());
    assert!(futures::poll!(&mut waiting).is_pending());
    #[cfg(unix)]
    std::fs::remove_file(&alias).map_err(storage)?;
    #[cfg(windows)]
    std::fs::remove_dir(&alias).map_err(storage)?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(&replacement, &alias).map_err(storage)?;
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(&replacement, &alias).map_err(storage)?;
    std::fs::write(alias.join(format!("shell-{operation}.cancel")), b"cancel").map_err(storage)?;
    waiting.await?;
    Ok(())
}
