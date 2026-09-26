//! Durable workflow journal qualification.
#![allow(clippy::indexing_slicing)]

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    resources::ProviderRef,
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineIdentity, MachineRegistry, MachineStatus,
        MachineTransition, ResumableMachine, WorkflowAdmission, WorkflowCommand,
        WorkflowCommitOutcome, WorkflowJournal,
    },
};
use acyclic_harness_filesystem::{FilesystemHost, FilesystemWorkflowJournal};
use acyclic_stream::{MemoryStream, StreamClient};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde_json::{Value, json};
use std::sync::Arc;

struct Counter {
    identity: MachineIdentity,
    schema: Value,
    command_payload: FileRef,
}

impl ResumableMachine for Counter {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, _input: &Value) -> Result<Value> {
        Ok(json!(0))
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let next = state.as_i64().unwrap_or_default() + input.as_i64().unwrap_or_default();
        let mut operation_bytes = [0_u8; 16];
        operation_bytes[..8].copy_from_slice(&next.to_le_bytes());
        operation_bytes[8..].copy_from_slice(&next.to_le_bytes());
        Ok(MachineTransition {
            state: json!(next),
            commands: vec![WorkflowCommand {
                operation_id: OperationId::from_bytes(operation_bytes),
                kind: "counter.publish".into(),
                payload: self.command_payload.clone(),
            }],
            status: MachineStatus::Suspended,
        })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn workflow_reopens_from_ref_only_stream_with_pinned_machine_and_exact_retry() -> Result<()> {
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
    let provider = ProviderRef::new("workflow-e2e", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let volume = VolumeRef::new(
        provider,
        "counter-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "workflow-e2e",
        [6; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "counter".into(),
        },
    );
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([4; 16]),
        "owner",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let write = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
    let command_payload = host
        .put_content(
            &volume,
            &write,
            "commands/count.json",
            b"3",
            "application/json",
            "count.json",
            65_536,
            &IdempotencyKey::new("command-payload")?,
        )
        .await?;
    let journal = Arc::new(FilesystemWorkflowJournal::new(
        stream.clone(),
        host.clone(),
        "counter",
        volume.clone(),
        issuer.verifier(),
        scope.clone(),
        65_536,
    )?);
    let machine = Arc::new(Counter {
        identity: MachineIdentity {
            name: "test.counter".into(),
            version: "1".into(),
            digest: [7; 32],
        },
        schema: json!({"type": "integer"}),
        command_payload: command_payload.clone(),
    });
    let initial = MachineCheckpoint {
        machine: machine.identity().clone(),
        revision: 0,
        state: json!(0),
    };
    let mut registry = MachineRegistry::default();
    registry.register(machine)?;
    let admission = WorkflowAdmission {
        operation_id: OperationId::from_bytes([9; 16]),
        request_digest: [5; 32],
        initial: initial.clone(),
    };
    stream
        .stream("harness/v2/workflows/orphan")
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .append_batch(
            vec![Bytes::from_static(b"unadmitted-transition")],
            Some(0),
            None,
        )
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    let orphan = FilesystemWorkflowJournal::new(
        stream.clone(),
        host.clone(),
        "orphan",
        volume.clone(),
        issuer.verifier(),
        scope.clone(),
        65_536,
    )?;
    assert!(matches!(
        orphan.admit(admission.clone()).await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    assert_eq!(journal.admit(admission.clone()).await?, admission);
    assert_eq!(journal.admission().await?, Some(admission.clone()));
    let mut changed_admission = admission.clone();
    changed_admission.request_digest = [6; 32];
    assert!(matches!(
        journal.admit(changed_admission).await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    let mut changed_initial = initial.clone();
    changed_initial.state = json!(1);
    assert!(matches!(
        DurableWorkflowHost::open(registry.clone(), changed_initial, journal.clone()).await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    let mut workflow =
        DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    let operation = OperationId::from_bytes([8; 16]);
    let retry = IdempotencyKey::new("counter-step-1")?;
    let transition = workflow.step(operation, retry.clone(), json!(3)).await?;
    assert_eq!(transition.state, json!(3));
    assert_eq!(workflow.step(operation, retry, json!(3)).await?, transition);
    let second = OperationId::from_bytes([11; 16]);
    assert_eq!(
        workflow
            .step(second, IdempotencyKey::new("counter-step-2")?, json!(1))
            .await?
            .state,
        json!(4)
    );
    let committed = journal.replay().await?;
    assert_eq!(committed.len(), 2);
    assert!(matches!(
        journal
            .commit(
                0,
                IdempotencyKey::new("counter-step-1")?,
                committed[0].clone()
            )
            .await?,
        WorkflowCommitOutcome::Replayed(_)
    ));
    let mut reused_command = committed[1].clone();
    reused_command.operation_id = OperationId::from_bytes([12; 16]);
    reused_command.idempotency_key = IdempotencyKey::new("reused-command")?;
    reused_command.prior = committed[1].next.clone();
    reused_command.next.revision = 3;
    reused_command.next.state = json!(5);
    reused_command.transition.state = json!(5);
    reused_command.transition.commands[0].operation_id =
        committed[0].transition.commands[0].operation_id;
    assert!(matches!(
        journal
            .commit(2, reused_command.idempotency_key.clone(), reused_command)
            .await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    let mut forged = committed[1].clone();
    forged.operation_id = OperationId::from_bytes([12; 16]);
    forged.idempotency_key = IdempotencyKey::new("forged-step")?;
    forged.prior.revision = 2;
    forged.prior.state = json!(999);
    forged.next.revision = 3;
    assert!(matches!(
        journal
            .commit(2, forged.idempotency_key.clone(), forged)
            .await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    let reopened = DurableWorkflowHost::open(registry, initial, journal.clone()).await?;
    assert_eq!(reopened.checkpoint().state, json!(4));
    let admission_entries = stream
        .stream("harness/v2/workflow-admissions/counter")
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .read(0, 2)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(admission_entries.len(), 1);
    let admission_ref: FileRef = serde_json::from_slice(&admission_entries[0].value)
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(admission_ref.descriptor().media_type(), "application/json");
    let records = stream
        .stream("harness/v2/workflows/counter")
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .read(0, 2)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(records.len(), 2);
    let reference: FileRef = serde_json::from_slice(&records[0].value)
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(reference.descriptor().media_type(), "application/json");
    let other = FilesystemWorkflowJournal::new(
        stream,
        host,
        "counter",
        volume,
        issuer.verifier(),
        scope,
        65_536,
    )?;
    let candidate = |operation, key: &str, command| -> Result<_> {
        let mut record = committed[1].clone();
        record.operation_id = OperationId::from_bytes([operation; 16]);
        record.idempotency_key = IdempotencyKey::new(key)?;
        record.prior = committed[1].next.clone();
        record.next.revision = 3;
        record.next.state = json!(5);
        record.transition.state = json!(5);
        record.transition.commands[0].operation_id = OperationId::from_bytes([command; 16]);
        Ok(record)
    };
    let first = candidate(13, "concurrent-first", 15)?;
    // Both independent hosts race the same CAS revision and emit the same
    // command identity. Exactly one may retain that command operation.
    let second = candidate(14, "concurrent-second", 15)?;
    let (first_result, second_result) = tokio::join!(
        journal.commit(2, first.idempotency_key.clone(), first.clone()),
        other.commit(2, second.idempotency_key.clone(), second.clone()),
    );
    assert!(
        matches!(&first_result, Ok(WorkflowCommitOutcome::Applied(_)))
            ^ matches!(&second_result, Ok(WorkflowCommitOutcome::Applied(_)))
    );
    assert!(
        matches!(&first_result, Err(acyclic_harness::Error::Conflict(_)))
            ^ matches!(&second_result, Err(acyclic_harness::Error::Conflict(_)))
    );
    let tail = journal.replay().await?;
    assert_eq!(tail.len(), 3);
    assert!(tail[2] == first || tail[2] == second);
    Ok(())
}
