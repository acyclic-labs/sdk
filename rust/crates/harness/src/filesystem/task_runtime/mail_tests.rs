//! Real MemoryFS/SDK mailbox controls; no replacement reader or mailbox engine.

use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

#[tokio::test]
async fn native_stock_mail_command_uses_original_atomic_v3_send_and_custody() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let task = TaskId::from_bytes([104; 16]);
    let payloads = FilesystemContentPublisher::new(
        fixture.filesystem.clone(),
        &(fixture.volume),
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.signed,
        65_536,
        None,
    )?;
    let payload = payloads
        .stage(
            OperationId::from_bytes([106; 16]),
            "native-stock-mail-body",
            b"7",
            None,
        )?
        .await?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let runtime = fixture.runtime(Some(&io)).await?;
    let (lease, custody) = fixture
        .native_lease_command(
            &runtime,
            super::super::super::MAIL_SEND_TASK_COMMAND_KIND,
            serde_json::to_value(super::super::super::MailSendTaskCommand {
                recipient: task,
                payload: payload.clone(),
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
            &work,
        )
        .await?;
    let reservation = lease.reservation.clone();
    let (lease, custody) = match runtime
        .resume_task_with_native_work(lease, custody, &runtime.commands(), 1, &work)
        .await?
    {
        (TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }), custody) => {
            (lease, custody)
        }
        (TaskWorkerAttempt::Unresolved { error, .. }, _) => return Err(error),
        _ => return Err(Error::Invalid("mail outbox did not yield".into())),
    };
    let fence = LeaseFence::from(&lease.reservation);
    let context = runtime
        .task_context_with_native_work(task, &fence, &work)
        .await?;
    let session = runtime
        .open_task_with_native_work(task, &fence, Arc::clone(&context), &work)
        .await?;
    let command =
        crate::filesystem::retain_decoded(io.value_decode_preparation::<WorkflowCommand>(
            &session.checkpoint().state["command"],
            crate::wire::ErrorCode::Storage,
        )?)?;
    drop(session);
    let body = context.read_file(&command.payload, Some(&io)).await?;
    let command_payload = crate::filesystem::retain_decoded(io.decode(
        std::marker::PhantomData::<Value>,
        &body,
        crate::wire::ErrorCode::Storage,
    )?)?;
    drop(body);
    let progress = runtime
        .commands()
        .execute_with_native_work(context, fence, command, command_payload, &work)?
        .await?;
    assert!(matches!(
        &progress,
        TaskCommandProgress::ReadyWithNativeCustody { custody, .. }
            if custody.work().shares_ledger(&work)
    ));
    drop(progress);
    assert!(custody.work().shares_ledger(&work));
    assert!(work.receipt().backend_write_operations > 0);
    let inbox = runtime
        .task_host()
        .inbox_with_native_work(task, 0, 8, &work)
        .await?;
    assert_eq!(inbox.0.len(), 1);
    assert_eq!(inbox.0[0].payload, payload);
    assert_eq!(inbox.0[0].task_id, task);
    let inbox_before = inbox.0[0].message_id.clone();
    drop(inbox);
    assert_eq!(lease.reservation, reservation);
    let (attempt, custody) = runtime
        .reconcile_task_with_native_work(lease, custody, &runtime.commands(), 1, 1_000, &work)
        .await?;
    assert!(matches!(
        attempt,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { .. })
    ));
    let inbox = runtime
        .task_host()
        .inbox_with_native_work(task, 0, 8, &work)
        .await?;
    assert_eq!(inbox.0.len(), 1);
    assert_eq!(inbox.0[0].message_id, inbox_before);
    assert!(custody.work().shares_ledger(&work));
    drop(inbox);
    drop(attempt);
    drop(custody);
    Ok(())
}
