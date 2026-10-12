use super::*;
use serde_json::json;
use records::validate_commands;

struct Counter { identity: MachineIdentity, schema: Value }
impl ResumableMachine for Counter {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> { Ok(input.clone()) }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition { state: json!(state.as_u64().unwrap_or(0) + input.as_u64().unwrap_or(0)), commands: vec![],
            status: if input.is_null() { MachineStatus::Completed { value: state.clone() } } else { MachineStatus::Suspended } })
    }
}

#[test]
fn workflow_history_and_command_counts_have_no_fixed_ceiling() -> Result<()> {
    use crate::{AgentId, conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef}, resources::ProviderRef};
    let payload = FileRef::new(VolumeRef::new(ProviderRef::new("workflow-test".into(), "filesystem".into(), "2".into(), &mut None)?,
        "private".into(), VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([4; 16])), &mut None)?,
        "commands/first.json".into(), "immutable-version".into(), FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
        "first.json".into(), &mut None)?;
    let prior = MachineCheckpoint { machine: MachineIdentity { name: "test.large".into(), version: "1".into(), digest: [1; 32] },
        revision: 1_000_001, state: json!(0) };
    let mut record = WorkflowRecord {
        operation_id: OperationId::from_bytes(1_000_000_u128.to_be_bytes()), idempotency_key: IdempotencyKey::new("large-history")?,
        input_digest: crate::contract::canonical_json_digest(&Value::Null)?, prior: prior.clone(), input: Value::Null,
        transition: MachineTransition { state: json!(1), commands: (100_000_u128..101_025).map(|id| WorkflowCommand {
            operation_id: OperationId::from_bytes(id.to_be_bytes()), kind: "test.publish".into(), payload: payload.clone(),
        }).collect(), status: MachineStatus::Suspended },
        next: MachineCheckpoint { revision: prior.revision + 1, state: json!(1), ..prior.clone() },
    };
    let reserved = (1_u128..=65_536).map(|id| OperationId::from_bytes(id.to_be_bytes())).collect();
    validate_workflow_commit(prior.revision, &record.idempotency_key, &record, None)?;
    validate_workflow_next(&prior, false, &reserved, &BTreeSet::new(), &record, None)?;
    let mut unsafe_command = serde_json::to_value(&record.transition.commands[0]).map_err(|error| Error::Invalid(error.to_string()))?;
    unsafe_command["body"] = json!("inline attachment bytes");
    assert!(serde_json::from_value::<WorkflowCommand>(unsafe_command).is_err());
    assert!(validate_commands(&[record.transition.commands[0].clone(), record.transition.commands[0].clone()], None).is_err());
    record.transition.commands[0].operation_id = OperationId::from_bytes(1_u128.to_be_bytes());
    assert!(matches!(validate_workflow_next(&prior, false, &reserved, &BTreeSet::new(), &record, None), Err(Error::Conflict(_))));
    Ok(())
}

#[tokio::test]
async fn workflow_pages_preserve_old_retries_and_reject_cross_page_corruption() -> Result<()> {
    let identity = MachineIdentity { name: "test.paged".into(), version: "1".into(), digest: [1; 32] };
    let mut registry = MachineRegistry::default();
    registry.register(Arc::new(Counter { identity: identity.clone(), schema: json!({"type":"integer","minimum":0}) }), &mut None)?;
    let initial = MachineCheckpoint { machine: identity, revision: 0, state: json!(0) };
    let journal = Arc::new(MemoryWorkflowJournal::default());
    assert!(matches!(DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await, Err(Error::Conflict(_))));
    let admission = WorkflowAdmission { operation_id: OperationId::new(), request_digest: [2; 32], initial: initial.clone() };
    assert_eq!(journal.admit(admission.clone()).await?, admission);
    assert_eq!(journal.admit(admission.clone()).await?, admission);
    let mut changed = admission.clone();
    changed.request_digest = [8; 32];
    assert!(matches!(journal.admit(changed).await, Err(Error::Conflict(_))));
    let mut host = DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    assert!(matches!(host.step(OperationId::new(), IdempotencyKey::new("rejected")?, json!(2),
        |_| Err(Error::Invalid("output schema rejected transition".into()))).await, Err(Error::Invalid(_))));
    assert_eq!(host.checkpoint().revision, 0);
    assert!(journal.replay(0, WORKFLOW_REPLAY_PAGE_RECORDS).await?.is_empty());
    let first = OperationId::new();
    let key = IdempotencyKey::new("paged-first")?;
    let transition = host.step(first, key.clone(), json!(1), |_| Ok(())).await?;
    for index in 1..=WORKFLOW_REPLAY_PAGE_RECORDS {
        host.step(OperationId::new(), IdempotencyKey::new(format!("paged-{index}"))?, json!(1), |_| Ok(())).await?;
    }
    assert_eq!(journal.replay(0, WORKFLOW_REPLAY_PAGE_RECORDS).await?.len(), WORKFLOW_REPLAY_PAGE_RECORDS as usize);
    assert_eq!(journal.replay(0, u32::MAX).await?.len(), (WORKFLOW_REPLAY_PAGE_RECORDS + 1) as usize);
    assert_eq!(journal.replay(u64::from(WORKFLOW_REPLAY_PAGE_RECORDS), WORKFLOW_REPLAY_PAGE_RECORDS).await?.len(), 1);
    assert!(journal.replay(0, 0).await.is_err());
    let mut reopened = DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    assert_eq!(reopened.checkpoint().revision, u64::from(WORKFLOW_REPLAY_PAGE_RECORDS) + 1);
    assert_eq!(reopened.step(first, key.clone(), json!(1), |_| Ok(())).await?, transition);
    assert!(reopened.step(first, key.clone(), json!(2), |_| Ok(())).await.is_err());
    let original_key = {
        let mut state = journal.state.lock().unwrap();
        let last = state.records.last_mut().unwrap();
        let original = last.idempotency_key.clone();
        last.idempotency_key = key.clone();
        original
    };
    assert!(DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await.is_err());
    {
        let mut state = journal.state.lock().unwrap();
        let last = state.records.last_mut().unwrap();
        last.idempotency_key = original_key;
        last.operation_id = first;
    }
    assert!(DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await.is_err());
    journal.state.lock().unwrap().records[0].idempotency_key = IdempotencyKey::new("changed-first")?;
    assert!(matches!(reopened.step(first, key, json!(1), |_| Ok(())).await, Err(Error::Storage(_))));
    let journal = Arc::new(MemoryWorkflowJournal::default());
    journal.admit(admission).await?;
    let mut terminal = DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    let operation = OperationId::new();
    let key = IdempotencyKey::new("complete")?;
    let completed = terminal.step(operation, key.clone(), Value::Null, |_| Ok(())).await?;
    assert!(matches!(completed.status, MachineStatus::Completed { .. }));
    assert!(matches!(terminal.step(OperationId::new(), IdempotencyKey::new("after-terminal")?, json!(1), |_| Ok(())).await, Err(Error::Conflict(_))));
    let mut recovered = DurableWorkflowHost::open(registry, initial, journal).await?;
    assert_eq!(recovered.checkpoint(), terminal.checkpoint());
    assert_eq!(recovered.step(operation, key, Value::Null, |_| Ok(())).await?, completed);
    Ok(())
}
