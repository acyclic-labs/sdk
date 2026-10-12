use super::*;
use crate::runtime::TaskStateProvider;
use crate::conversation::FileRef;
use crate::test_stream::LostSessionAck;
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_stream::{OwnedBytes};

type MailRuntime = FilesystemTaskRuntime<LostSessionAck<MemoryStream>, MemoryAuthorityBackend, MemoryObjectBackend>;

const BUDGET: WorkBudget = acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024);
fn work(budget: WorkBudget, cancellation: &CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

struct MailFixture {
    fixture: Fixture,
    sdk: Arc<LostSessionAck<MemoryStream>>,
    runtime: MailRuntime,
    tasks: Vec<(TaskId, LeaseFence)>,
    payload: FileRef,
}
impl MailFixture {
    async fn new() -> Result<Self> {
        let fixture = Fixture::new().await?;
        fixture.machine.native.store(true, Ordering::SeqCst);
        let sdk = Arc::new(LostSessionAck::default());
        let runtime = FilesystemTaskRuntime::open(
            Arc::clone(&sdk), fixture.filesystem.clone(), &fixture.volume,
            &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.signed, &fixture.scope,
            &fixture.tasks, &fixture.machines, &fixture.tools,
            SessionLimits { active_tasks: 8, total_tasks: 8, depth: 4, model_steps: 8 },
            8, 65_536, fixture.clock.clone(), None, &[], &ResumableToolRegistry::default(), None,
        ).await?;
        let payloads = FilesystemSchedulerPayloadStore::new(fixture.filesystem.clone(), &(fixture.volume), &fixture.issuer.verifier(&mut None).expect("verifier copy"), &fixture.signed, 65_536, None)?;
        let definition = fixture.tasks.get_version::<Value, Value>("test.deadline", "1", #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
        let mut tasks = Vec::new();
        for byte in 190..193 {
            let operation = OperationId::from_bytes([byte; 16]);
            let payload = payloads.stage(operation, "command", &crate::contract::canonical_json_bytes(
                &super::super::super::ToolTaskCommand { name: "test.deadline".into(), revision: "1".into(), arguments: Value::Null },
            )?).await?;
            assert!(matches!(runtime.harness().admit(operation, &definition,
                json!({"command": WorkflowCommand { operation_id: operation,
                    kind: super::super::super::TOOL_TASK_COMMAND_KIND.into(), payload }}), None).await?,
                Admission::Accepted(_)));
            let lease = match runtime.task_host().pull_work(&Worker {
                id: "native-mail-control".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new(),
            }).await? {
                WorkPull::Claimed(lease) => lease,
                _ => return Err(Error::NotFound("native mail original lease".into())),
            };
            runtime.task_host().start_task(&lease, None).await?;
            tasks.push((TaskId::from_bytes(lease.operation.operation_id.into_bytes()), LeaseFence::from(&lease.reservation)));
        }
        let payload = payloads.stage(OperationId::from_bytes([193; 16]), "mail-source", b"actual native mailbox source").await?;
        Ok(Self { fixture, sdk, runtime, tasks, payload })
    }

    async fn send(&self, recipient: usize, operation: OperationId, io: &PreparationIo) -> Result<crate::conversation::FileRef> {
        let payload = self.payload.clone_with_io(io)?;
        self.runtime.task_host().send_owned_with_io(self.tasks[0].0, &self.tasks[0].1,
            self.tasks[recipient].0, operation, payload, Some(io)).await
    }
}

#[tokio::test]
async fn native_mail_sdk_cursor_atomic_lost_ack_and_multi_recipient_are_original() -> Result<()> {
    let fixture = MailFixture::new().await?;
    assert_eq!(fixture.payload.volume(), &fixture.fixture.volume);
    let work = work(BUDGET, &CancellationToken::new())?;
    fixture.sdk.native_only.store(true, Ordering::SeqCst);
    fixture.sdk.commit_lose_ack.store(true, Ordering::SeqCst);
    fixture.sdk.reset_io();
    let io = PreparationIo::from_native_work(&work)?;
    let operation = OperationId::from_bytes([194; 16]);
    let ack = fixture.send(1, operation, &io).await?;
    assert_eq!(ack, fixture.payload);
    assert!(fixture.sdk.io_snapshot().inspections > 0);
    let committed = fixture.sdk.io_snapshot().records;
    assert_eq!(committed, 2, "one original atomic intent/envelope pair");
    let (items, guard) = fixture.runtime.task_host().inbox_with_native_work(fixture.tasks[1].0, 0, 1, &work).await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].payload, fixture.payload);
    assert_eq!(items[0].sequence, 1);
    assert_eq!(items[0].message_id, operation.to_string());
    assert!(guard.matches_work(&work));
    drop(items);
    drop(guard);
    let retry = fixture.send(1, operation, &io).await?;
    assert_eq!(retry, ack);
    assert_eq!(fixture.sdk.io_snapshot().records, committed);
    assert!(fixture.runtime.task_host().reconcile_sent_mail(fixture.tasks[0].0, &fixture.tasks[0].1,
        fixture.tasks[1].0, operation, fixture.payload.clone_with_io(&io)?, Some(&io)).await?);
    assert_eq!(fixture.sdk.io_snapshot().records, committed);
    let rejected = fixture.send(2, operation, &io).await;
    assert!(matches!(rejected, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, .. })));
    assert_eq!(fixture.sdk.io_snapshot().records, committed);
    fixture.send(2, OperationId::from_bytes([195; 16]), &io).await?;
    let (items, guard) = fixture.runtime.task_host().inbox_with_native_work(fixture.tasks[2].0, 0, 1, &work).await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].task_id, fixture.tasks[2].0);
    assert!(guard.matches_work(&work));
    let (empty, guard) = fixture.runtime.task_host().inbox_with_native_work(fixture.tasks[2].0, 1, 1, &work).await?;
    assert!(empty.is_empty());
    assert_eq!(empty.capacity(), 0);
    assert_eq!(guard.bytes(), 0);
    assert!(guard.matches_work(&work));
    Ok(())
}

#[tokio::test]
async fn native_mail_cancellation_and_page_quota_deny_before_more_sdk_io() -> Result<()> {
    let fixture = MailFixture::new().await?;
    let cancellation = CancellationToken::new();
    let original = work(BUDGET, &cancellation)?;
    fixture.sdk.native_only.store(true, Ordering::SeqCst);
    let io = PreparationIo::from_native_work(&original)?;
    let payload = fixture.payload.clone_with_io(&io)?;
    cancellation.cancel();
    fixture.sdk.reset_io();
    let before = original.receipt();
    assert!(matches!(fixture.runtime.task_host().send_owned_with_io(fixture.tasks[0].0, &fixture.tasks[0].1,
        fixture.tasks[1].0, OperationId::from_bytes([196; 16]), payload, Some(&io)).await,
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert!(matches!(fixture.runtime.task_host().inbox_with_native_work(fixture.tasks[1].0, 0, 1, &original).await,
        Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(original.receipt(), before);
    assert_eq!(fixture.sdk.io_snapshot().reads, 0);
    assert_eq!(fixture.sdk.io_snapshot().writes, 0);
    let mut allowance = BUDGET;
    allowance.allocation_operations = 0;
    let denied = work(allowance, &CancellationToken::new())?;
    fixture.sdk.reset_io();
    assert!(matches!(fixture.runtime.task_host().inbox_with_native_work(fixture.tasks[1].0, 0, 1, &denied).await,
        Err(Error::NativeWork { .. })));
    assert_eq!(fixture.sdk.io_snapshot().reads, 0);
    assert_eq!(fixture.sdk.io_snapshot().writes, 0);
    Ok(())
}

#[tokio::test]
async fn native_mail_sdk_cursor_rejects_actual_hostile_version_and_recipient_records() -> Result<()> {
    let fixture = MailFixture::new().await?;
    let sender = fixture.tasks[0].0;
    let recipient = fixture.tasks[1].0;
    for (index, (field, replacement)) in [
        ("recipient", None), ("schema_revision", None), ("route_revision", None),
        ("recipient", Some(json!(fixture.tasks[2].0))),
        ("schema_revision", Some(json!(2))), ("route_revision", Some(json!(2))),
    ].into_iter().enumerate() {
        let message = OperationId::from_bytes([197 + index as u8; 16]);
        let host = fixture.runtime.task_host();
        let mut event = json!({ "sender": sender, "recipient": recipient, "message_id": message,
            "schema_revision": 1, "route_revision": 1, "payload": fixture.payload });
        let object = event.as_object_mut().expect("actual wire object");
        if let Some(replacement) = replacement { object.insert(field.into(), replacement); }
        else { object.remove(field); }
        let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&event)?);
        let mailbox = acyclic_stream::StreamPath::new(format!("harness/v3/mail/by-recipient/{recipient}"))?;
        let intent = acyclic_stream::StreamPath::new(format!("harness/v3/mail/by-intent/{sender}/{message}"))?;
        let location = OwnedBytes::from(crate::contract::canonical_json_bytes(&json!({
            "sender": sender, "recipient": recipient, "message_id": message,
            "schema_revision": 1, "route_revision": 1, "sequence": index as u64,
            "value_digest": *blake3::hash(&bytes).as_bytes(),
        }))?);
        let target = if index == 0 { acyclic_stream::CommitCondition::Absent { path: mailbox.clone() } }
            else { acyclic_stream::CommitCondition::Tail { path: mailbox.clone(), expected: index as u64 } };
        let committed = Arc::clone(&fixture.sdk).commit(acyclic_stream::CommitRequest {
            conditions: vec![target, acyclic_stream::CommitCondition::Absent { path: intent.clone() }],
            mutations: vec![
                acyclic_stream::CommitMutation::Append { path: mailbox, records: vec![bytes] },
                acyclic_stream::CommitMutation::Append { path: intent, records: vec![location] },
            ],
            idempotency_key: acyclic_stream::IdempotencyKey::new(OwnedBytes::copy_from_slice(&message.into_bytes()))?,
        }).await?;
        assert!(matches!(committed, acyclic_stream::CommitOutcome::Committed(_)));
        fixture.sdk.native_only.store(true, Ordering::SeqCst);
        let original = work(BUDGET, &CancellationToken::new())?;
        assert!(matches!(host.inbox_with_native_work(recipient, index as u64, 1, &original).await,
            Err(Error::NativeDecode { .. }) | Err(Error::NativeDiagnostic { .. })
                | Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, .. })));
        fixture.sdk.native_only.store(false, Ordering::SeqCst);
    }
    Ok(())
}
