//! Real MemoryFS/SDK mailbox controls; no replacement reader or mailbox engine.

use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

fn finite_mail_budget() -> Result<WorkBudget> {
    let mut fields = serde_json::to_value(WorkBudget::default())
        .map_err(|cause| Error::Invalid(cause.to_string()))?;
    for value in fields.as_object_mut().ok_or_else(|| Error::Invalid("work counter fixture is not an object".into()))?.values_mut() {
        *value = json!(64 * 1024 * 1024_u64);
    }
    serde_json::from_value(fields).map_err(|cause| Error::Invalid(cause.to_string()))
}



#[tokio::test]
async fn native_stock_mail_command_uses_original_atomic_v3_send_and_custody() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture.machine.native.store(true, Ordering::SeqCst);
    let runtime = fixture.runtime().await?;
    let task = TaskId::from_bytes([104; 16]);
    let payloads = FilesystemSchedulerPayloadStore::new(fixture.filesystem.clone(), &(fixture.volume), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.signed, 65_536, None)?;
    let payload = payloads.stage(OperationId::from_bytes([106; 16]), "native-stock-mail-body", b"7").await?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(finite_mail_budget()?, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let (lease, custody) = fixture.native_lease_command(
        &runtime, super::super::super::MAIL_SEND_TASK_COMMAND_KIND,
        serde_json::to_value(super::super::super::MailSendTaskCommand { recipient: task, payload: payload.clone() })?,
        &work,
    ).await?;
    let reservation = lease.reservation.clone();
    let (attempt, custody) = runtime.resume_task_with_native_work(lease, custody, &runtime.commands(), 1, &work).await?;
    assert!(matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })));
    assert!(custody.work().shares_ledger(&work));
    assert!(work.receipt().backend_write_operations > 0);
    let inbox = runtime.task_host().inbox_with_native_work(task, 0, 8, &work).await?;
    assert_eq!(inbox.0.len(), 1);
    assert_eq!(inbox.0[0].payload, payload);
    assert_eq!(inbox.0[0].task_id, task);
    let inbox_before = inbox.0[0].message_id.clone();
    drop(inbox);
    let lease = match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => lease,
        _ => unreachable!(),
    };
    assert_eq!(lease.reservation, reservation);
    let (attempt, custody) = runtime.reconcile_task_with_native_work(lease, custody, &runtime.commands(), 1, 1_000, &work).await?;
    assert!(matches!(attempt, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { .. })));
    let inbox = runtime.task_host().inbox_with_native_work(task, 0, 8, &work).await?;
    assert_eq!(inbox.0.len(), 1);
    assert_eq!(inbox.0[0].message_id, inbox_before);
    assert!(custody.work().shares_ledger(&work));
    drop(inbox);
    drop(attempt);
    drop(custody);
    Ok(())
}
