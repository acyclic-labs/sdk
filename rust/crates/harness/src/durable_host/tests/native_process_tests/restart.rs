use super::*;

#[cfg(feature = "filesystem-local")]
#[tokio::test]
async fn actual_host_termination_reopens_launch_only_receipt_without_replay_or_slot_release()
-> Result<()> {
    use std::process::{Command, Stdio};
    let storage = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let mut child =
        Command::new(std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?)
            .args([
                "--exact",
                "durable_host::tests::native_process_tests::native_host_child",
                "--nocapture",
            ])
            .env_clear()
            .env("NATIVE_HOST_STORAGE", storage.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Error::Storage(error.to_string()))?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(bytes) = std::fs::read(storage.path().join("restart.json"))
                && let Ok(seed) = crate::contract::json_from_slice::<NativeRestart>(&bytes)
                && seed.root.join("host-death-ready").exists()
            {
                break seed;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    child
        .kill()
        .map_err(|error| Error::Storage(error.to_string()))?;
    child
        .wait()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let seed = ready
        .map_err(|error| Error::Storage(format!("native host did not reach launch: {error}")))?;
    tokio::time::sleep(std::time::Duration::from_millis(3200)).await;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = test_work(
        acyclic_fs::performance::work_budget(1_000_000_000, 1_000_000_000),
        &cancellation,
    )?;
    let LocalNativeRecovery {
        files,
        task,
        owner,
        process,
        effects,
    } = reopen_native(storage.path(), &seed, &work).await?;
    let attempt = crate::EffectAttemptId::from_bytes(
        task_effect_id(seed.task, seed.command, &mut None)?.into_bytes(),
    );
    let path = acyclic_stream::StreamPath::new(format!("harness/v2/native-process/{attempt}"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        effects
            .run_task_effect(&owner, seed.command, &seed.plan, Some(&work))
            .await?
            .0,
        EffectStatus::Indeterminate
    );
    assert!(process.output(attempt, &work).await?.is_none());
    let receipt = process
        .receipt(attempt, &work, None)
        .await?
        .ok_or_else(|| Error::Storage("missing launch-only native receipt".into()))?;
    assert_native_receipt_binding(
        &owner,
        seed.command,
        &seed.plan,
        &receipt,
        &EffectStatus::Indeterminate,
        false,
    )?;
    assert_eq!(receipt.original_fence, seed.fence);
    assert_eq!(receipt.diagnostic, None);
    assert_eq!(receipt.publication_failure, None);
    assert_eq!(receipt.invocation_failure, None);
    assert_eq!(receipt.work, acyclic_fs::WorkCounters::default());
    let output = seed.root.join("dst/output.txt");
    if output.exists() {
        std::fs::remove_file(&output).map_err(|error| Error::Storage(error.to_string()))?;
    }
    task.task_host().cancel(seed.task, None).await?;
    assert!(owner.verify(false, &mut None).await.is_err());
    assert!(owner.verify(true, &mut None).await.is_ok());
    assert_eq!(
        effects
            .reconcile_task_effect(&owner, seed.command, &seed.plan, Some(&work))
            .await?
            .0,
        EffectStatus::Indeterminate
    );
    assert_eq!(process.receipt(attempt, &work, None).await?, Some(receipt));
    assert_eq!(task.task_host().outcome(seed.task).await?, None);
    assert!(!output.exists());
    let destination = workspace_ref(
        seed.destination.provider().clone(),
        &seed.destination.storage_name(&mut None)?,
    )?;
    assert!(
        files
            .read(&destination, None, "/output.txt", 64)
            .await
            .is_err()
    );
    let coordinator = owner.host.coordinator.lock().await;
    let operation = coordinator
        .scheduler()
        .operation(owner.operation_id())
        .ok_or_else(|| Error::NotFound("native operation after host death".into()))?;
    assert!(operation.reservation.is_some());
    assert_ne!(operation.phase, crate::scheduler::OperationPhase::Terminal);
    Ok(())
}
