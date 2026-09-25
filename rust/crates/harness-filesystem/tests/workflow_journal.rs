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
        MachineTransition, ResumableMachine, WorkflowCommand, WorkflowJournal,
    },
};
use acyclic_harness_filesystem::{FilesystemHost, FilesystemWorkflowJournal};
use acyclic_stream::{MemoryStream, StreamClient};
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
        Ok(MachineTransition {
            state: json!(next),
            commands: vec![WorkflowCommand {
                operation_id: OperationId::from_bytes([10; 16]),
                kind: "counter.publish".into(),
                payload: self.command_payload.clone(),
            }],
            status: MachineStatus::Suspended,
        })
    }
}

#[tokio::test]
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
        host,
        "counter",
        volume,
        issuer.verifier(),
        scope,
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
    let mut workflow =
        DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    let operation = OperationId::from_bytes([8; 16]);
    let retry = IdempotencyKey::new("counter-step-1")?;
    let transition = workflow.step(operation, retry.clone(), json!(3)).await?;
    assert_eq!(transition.state, json!(3));
    assert_eq!(workflow.step(operation, retry, json!(3)).await?, transition);
    assert_eq!(journal.replay().await?.len(), 1);
    let reopened = DurableWorkflowHost::open(registry, initial, journal).await?;
    assert_eq!(reopened.checkpoint().state, json!(3));
    let records = stream
        .stream("harness/v2/workflows/counter")
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .read(0, 2)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(records.len(), 1);
    let reference: FileRef = serde_json::from_slice(&records[0].value)
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    assert_eq!(reference.descriptor().media_type(), "application/json");
    Ok(())
}
