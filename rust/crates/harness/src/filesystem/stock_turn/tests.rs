use super::*;
use crate::{filesystem::{MODEL_TASK_COMMAND_KIND, MAIL_RECEIVE_TASK_COMMAND_KIND}, workflow::WorkflowCommand};
use crate::{
    AgentId, IdempotencyKey, OperationId,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineRegistry, MemoryWorkflowJournal,
        WorkflowAdmission, WorkflowJournal,
    },
};

fn command() -> Result<WorkflowCommand> {
    Ok(WorkflowCommand {
        operation_id: OperationId::from_bytes([9; 16]),
        kind: MODEL_TASK_COMMAND_KIND.into(),
        payload: FileRef::new(
            VolumeRef::new(
                ProviderRef::new("stock-turn-test", "filesystem", "2")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
            )?,
            "commands/model.json",
            "immutable-command",
            FileDescriptor::from_bytes(b"{}", "application/json")?,
            "model.json",
        )?,
    })
}

#[test]
fn stock_turn_runtime_requires_original_model_grant_before_work() -> Result<()> {
    use crate::{
        Capabilities,
        conversation::Limits,
        runtime::{Bindings, RuntimeScope},
    };

    let machine = Arc::new(StockTurnMachine::new());
    let mut tasks = crate::runtime::TaskRegistry::default();
    tasks.register(machine.definition()?)?;
    let mut missing = Bindings::local();
    missing.tasks = tasks.clone();
    assert!(matches!(missing.build(), Err(Error::Invalid(message))
        if message == "unsatisfied task requirement: model"));

    let mut available = Bindings::local();
    available.tasks = tasks;
    available.scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
    let runtime = available.build()?;
    runtime.task::<Value, TurnOutput>("acyclic.stock_turn@1")?;
    Ok(())
}

#[tokio::test]
async fn stock_turn_outbox_survives_reopen_and_rejects_foreign_or_uncertain_results() -> Result<()>
{
    let machine = Arc::new(StockTurnMachine::new());
    let mut tasks = crate::runtime::TaskRegistry::default();
    tasks.register(machine.definition()?)?;
    tasks.get::<Value, TurnOutput>("acyclic.stock_turn@1")?;
    let mut registry = MachineRegistry::default();
    registry.register(machine.clone())?;
    let command = command()?;
    let receive = WorkflowCommand { operation_id: OperationId::from_bytes([10;16]),
        kind: MAIL_RECEIVE_TASK_COMMAND_KIND.into(), payload: command.payload.clone() };
    let initial = MachineCheckpoint {
        machine: machine.identity().clone(),
        revision: 0,
        state: machine.initialize(
            &json!({"receive":receive,"model":command}),
        )?,
    };
    let journal = Arc::new(MemoryWorkflowJournal::default());
    journal
        .admit(WorkflowAdmission {
            operation_id: OperationId::from_bytes([1; 16]),
            request_digest: [7; 32],
            initial: initial.clone(),
        })
        .await?;
    let mut host =
        DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    let emitted = host
        .step(
            OperationId::from_bytes([2; 16]),
            IdempotencyKey::new("emit")?,
            Value::Null,
        )
        .await?;
    assert_eq!(emitted.commands, vec![receive.clone()]);
    assert_eq!(emitted.status, MachineStatus::Suspended);
    drop(host);
    let mut host =
        DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    assert_eq!(host.latest_transition().await?, Some(emitted.clone()));
    assert_eq!(
        host.step(
            OperationId::from_bytes([2; 16]),
            IdempotencyKey::new("emit")?,
            Value::Null
        )
        .await?,
        emitted
    );
    let selected = host.step(OperationId::from_bytes([11;16]), IdempotencyKey::new("select")?,
        json!({"commands":[{"operation_id":receive.operation_id,"value":{"Succeeded":null}}]})).await?;
    assert_eq!(selected.commands, vec![command.clone()]);
    drop(host);
    let mut host = DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    assert_eq!(host.latest_transition().await?, Some(selected));
    let output = TurnOutput {
        text: "original answer".into(),
        attachments: Vec::new(),
        metadata: json!({"provider":"fixture"}),
        steps: 1,
    };
    let value = serde_json::to_value(Outcome::Succeeded(output.clone()))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let result = json!({"operation_id":command.operation_id,"value":value});
    let foreign = json!({"operation_id":OperationId::from_bytes([8;16]),"value":value});
    for input in [
        Value::Null,
        json!({"commands":[]}),
        json!({"commands":[foreign]}),
        json!({"commands":[result.clone(),result.clone()]}),
        json!({"commands":[{"operation_id":command.operation_id,"value":"Cancelled"}]}),
        json!({"commands":[{"operation_id":command.operation_id,
            "value":{"Indeterminate":{"operation_id":command.operation_id}}}]}),
    ] {
        assert!(
            host.step(
                OperationId::new(),
                IdempotencyKey::new("invalid-result")?,
                input
            )
            .await
            .is_err()
        );
        assert_eq!(host.checkpoint().revision, 2);
        assert_eq!(journal.replay(0, 8).await?.len(), 2);
    }
    let completed = host
        .step(
            OperationId::from_bytes([3; 16]),
            IdempotencyKey::new("settle")?,
            json!({"commands":[result]}),
        )
        .await?;
    assert!(completed.commands.is_empty());
    assert_eq!(
        completed.status,
        MachineStatus::Completed {
            value: serde_json::to_value(&output)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        }
    );
    drop(host);
    let host = DurableWorkflowHost::open(registry, initial, journal.clone()).await?;
    assert_eq!(host.latest_transition().await?, Some(completed));
    assert_eq!(journal.replay(0, 8).await?.len(), 3);
    Ok(())
}

#[test]
fn stock_turn_preserves_failure_and_rejects_another_command_kind() -> Result<()> {
    let machine = StockTurnMachine::new();
    let mut command = command()?;
    let receive = WorkflowCommand { operation_id: OperationId::from_bytes([10;16]),
        kind: MAIL_RECEIVE_TASK_COMMAND_KIND.into(), payload: command.payload.clone() };
    command.kind = super::super::TOOL_TASK_COMMAND_KIND.into();
    assert!(machine.initialize(&json!({"receive":receive,"model":command})).is_err());
    command.kind = MODEL_TASK_COMMAND_KIND.into();
    let initial = machine.initialize(&json!({"receive":receive,"model":command}))?;
    assert!(
        machine
            .transition(&initial, &json!({"wake":"unrelated"}))
            .is_err()
    );
    let receiving = machine.transition(&initial, &Value::Null)?;
    let waiting = machine.transition(&receiving.state, &json!({"commands":[{
        "operation_id":receive.operation_id,"value":{"Succeeded":null}
    }]}))?;
    let failed = machine.transition(&waiting.state, &json!({"commands":[{
        "operation_id":command.operation_id,"value":{"Failed":{"message":"denied by original policy"}}
    }]}))?;
    assert_eq!(
        failed.status,
        MachineStatus::Failed {
            message: "denied by original policy".into()
        }
    );
    assert!(failed.commands.is_empty());
    assert!(machine.transition(&failed.state, &Value::Null).is_err());
    Ok(())
}
