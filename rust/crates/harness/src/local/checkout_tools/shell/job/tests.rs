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
                1 << 20,
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
    let child = std::env::current_exe().map_err(storage)?;
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
        .strip_prefix(".volumes/home/")
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
    Ok(())
}
