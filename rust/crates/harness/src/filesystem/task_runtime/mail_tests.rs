//! Real MemoryFS/SDK mailbox controls; no replacement reader or mailbox engine.

use super::*;
use crate::conversation::FileRef;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest};
use bytes::Bytes;

fn finite_mail_budget() -> Result<WorkBudget> {
    // Spell the finite fixture across the canonical counter schema so a global
    // counter cutover cannot leave a silently unbounded or omitted field.
    let mut fields = serde_json::to_value(WorkBudget::default())
        .map_err(|cause| Error::Invalid(cause.to_string()))?;
    for value in fields.as_object_mut().ok_or_else(|| Error::Invalid("work counter fixture is not an object".into()))?.values_mut() {
        *value = json!(64 * 1024 * 1024_u64);
    }
    serde_json::from_value(fields).map_err(|cause| Error::Invalid(cause.to_string()))
}

fn original_mail_work(budget: WorkBudget, cancellation: &CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

async fn retained_mail_fixture() -> Result<(Fixture, Runtime, TaskId, FileRef)> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime().await?;
    let task = TaskId::from_bytes([104; 16]);
    let payloads = FilesystemSchedulerPayloadStore::new(
        fixture.filesystem.clone(), fixture.volume.clone(), &fixture.issuer.verifier(), &fixture.signed, 65_536,
    )?;
    let payload = payloads.stage(OperationId::from_bytes([106; 16]), "native-mail-body", b"7").await?;
    let lease = fixture.admit_command(&runtime, super::super::super::MAIL_SEND_TASK_COMMAND_KIND,
        serde_json::to_value(&super::super::super::MailSendTaskCommand { recipient: task, payload: payload.clone() })
            .map_err(|cause| Error::Invalid(cause.to_string()))?).await?;
    assert!(matches!(runtime.resume_task(lease, &runtime.commands(), 1).await,
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })));
    Ok((fixture, runtime, task, payload))
}

#[tokio::test]
async fn native_mailbox_page_empty_precancel_and_denial_keep_original_ledger() -> Result<()> {
    let (fixture, runtime, task, _) = retained_mail_fixture().await?;
    // Passive recipient observation is not a new lease or dispatch. Advancing
    // the deadline does not give this read authority to cancel or publish.
    fixture.clock.0.store(20, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let work = original_mail_work(finite_mail_budget()?, &cancellation)?;
    let (items, custody) = runtime.task_host().inbox_with_native_work(task, 0, 1, &work).await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].sequence, 1);
    assert!(work.shares_ledger(custody.work()));
    assert!(custody.bytes() >= (items.capacity() * std::mem::size_of::<crate::InboxItem>()) as u64);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(work.receipt().authority_records_read > 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    let live = work.live_allocation_bytes();
    let returned = custody.bytes();
    drop(items);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), live - returned);
    let (empty, custody) = runtime.task_host().inbox_with_native_work(task, 1, 1, &work).await?;
    assert!(empty.is_empty());
    assert_eq!(custody.bytes(), 0);
    assert!(work.shares_ledger(custody.work()));
    drop(empty);
    drop(custody);

    let cancellation = CancellationToken::new();
    let cancelled = original_mail_work(finite_mail_budget()?, &cancellation)?;
    cancellation.cancel();
    let prefix = cancelled.receipt();
    assert!(matches!(runtime.task_host().inbox_with_native_work(task, 0, 1, &cancelled).await,
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(cancelled.receipt(), prefix);

    let mut budget = finite_mail_budget()?;
    budget.backend_read_operations = 0;
    let denied = original_mail_work(budget, &CancellationToken::new())?;
    assert!(matches!(runtime.task_host().inbox_with_native_work(task, 0, 1, &denied).await,
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(_), .. })));
    assert_eq!(denied.receipt().backend_read_operations, 0);
    assert_eq!(denied.receipt().backend_write_operations, 0);

    let mut budget = finite_mail_budget()?;
    budget.allocation_operations = 1;
    let denied_allocation = original_mail_work(budget, &CancellationToken::new())?;
    let prefix = denied_allocation.receipt();
    assert!(matches!(
        runtime.task_host().inbox_with_native_work(task, 0, 1, &denied_allocation).await,
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(_), .. })
    ));
    assert_eq!(denied_allocation.receipt().allocation_operations, prefix.allocation_operations);
    assert_eq!(denied_allocation.receipt().backend_write_operations, 0);
    assert_eq!(runtime.task_host().inbox(task, 0, 8).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn native_mailbox_rejects_hostile_v3_records_with_real_atomic_sdk_locations() -> Result<()> {
    let (fixture, runtime, task, payload) = retained_mail_fixture().await?;
    let mailbox = acyclic_stream::StreamPath::new(format!("harness/v3/mail/by-recipient/{task}"))?;
    let mut variants = Vec::new();
    let envelope = json!({"sender":task,"recipient":task,"message_id":OperationId::from_bytes([160;16]),
        "schema_revision":1,"route_revision":1,"payload":payload});
    for field in ["schema_revision", "route_revision"] {
        for revision in [0_u32, 2, u32::MAX] {
            let mut value = envelope.clone();
            value[field] = json!(revision);
            variants.push(value);
        }
    }
    let mut destination = envelope.clone();
    destination["recipient"] = json!(TaskId::from_bytes([199;16]));
    variants.push(destination);
    for field in ["sender", "recipient", "message_id", "schema_revision", "route_revision", "payload"] {
        let mut value = envelope.clone();
        value.as_object_mut().expect("envelope fixture").remove(field);
        variants.push(value);
    }
    let mut unknown = envelope;
    unknown["unexpected"] = json!(true);
    variants.push(unknown);
    for (index, mut value) in variants.into_iter().enumerate() {
        let message = OperationId::from_bytes([160 + index as u8;16]);
        if value.get("message_id").is_some() { value["message_id"] = json!(message); }
        let bytes = Bytes::from(crate::contract::canonical_json_bytes(&value)?);
        let sequence = fixture.stream.bounds(mailbox.as_str()).await?.tail;
        let location = acyclic_stream::StreamPath::new(format!("harness/v3/mail/by-intent/{task}/{message}"))?;
        let receipt = Bytes::from(crate::contract::canonical_json_bytes(&json!({
            "sender":task,"recipient":task,"message_id":message,"schema_revision":1,"route_revision":1,
            "sequence":sequence,"value_digest":*blake3::hash(&bytes).as_bytes(),
        }))?);
        assert!(matches!(fixture.stream.commit(CommitRequest {
            conditions:vec![CommitCondition::Tail {path:mailbox.clone(),expected:sequence},CommitCondition::Absent {path:location.clone()}],
            mutations:vec![CommitMutation::Append {path:mailbox.clone(),records:vec![bytes]},
                CommitMutation::Append {path:location,records:vec![receipt]}],
            idempotency_key:acyclic_stream::IdempotencyKey::new(Bytes::from(vec![index as u8 + 1]))?,
        }).await?, CommitOutcome::Committed(_)));
        let work = original_mail_work(finite_mail_budget()?, &CancellationToken::new())?;
        let error = match runtime.task_host().inbox_with_native_work(task, sequence, 1, &work).await {
            Err(error) => error,
            Ok((items, custody)) => { drop(items); drop(custody); return Err(Error::Invalid("hostile native mail escaped".into())); }
        };
        assert_eq!(error.code(), crate::wire::ErrorCode::Storage);
        assert!(error.is_native_failure());
        assert_eq!(work.receipt().backend_write_operations, 0);
        drop(error);
    }
    Ok(())
}
