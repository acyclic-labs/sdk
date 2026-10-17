use super::*;
use acyclic_harness::{
    AgentId, IdempotencyKey, OperationId,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineRegistry, MachineStatus,
        MemoryWorkflowJournal, ResumableMachine, WorkflowAdmission, WorkflowCommand,
        WorkflowJournal,
    },
};
use proptest::prelude::*;
use std::sync::Arc;

fn tool_command(id: [u8; 16]) -> Result<WorkflowCommand> {
    Ok(WorkflowCommand {
        operation_id: OperationId::from_bytes(id),
        kind: "acyclic.tool.v1".into(),
        payload: FileRef::new(
            VolumeRef::new(
                ProviderRef::new(
                    "session-test".into(),
                    "filesystem".into(),
                    "2".into(),
                    &mut None,
                )?,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
                &mut None,
            )?,
            "commands/tool.json".into(),
            "immutable-command".into(),
            FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
            "tool.json".into(),
            &mut None,
        )?,
    })
}

fn result(operation_id: OperationId, value: Value) -> Value {
    json!({"commands":[{"operation_id":operation_id,"value":value}]})
}

#[test]
fn session_fuzz_seeds_reach_every_stage_and_the_real_command_codec() -> Result<()> {
    let machine = CommandSessionMachine::new();
    for seed in [
        include_str!("../../../harness-fuzz/corpus/external-hooks/session-ready"),
        include_str!("../../../harness-fuzz/corpus/external-hooks/session-next"),
        include_str!("../../../harness-fuzz/corpus/external-hooks/session-idle"),
        include_str!("../../../harness-fuzz/corpus/external-hooks/session-result"),
        include_str!("../../../harness-fuzz/corpus/external-hooks/session-close"),
    ] {
        let (state, input): (Value, Value) = serde_json::from_str(seed).map_err(|e| invalid(&e))?;
        machine.transition(&state, &input)?;
        if !input.is_null() && state["stage"] == "idle" {
            let initial = machine.initialize(&input)?;
            assert_eq!(
                machine.transition(&initial, &Value::Null)?.commands.len(),
                1
            );
        }
    }
    Ok(())
}

#[test]
fn invalid_session_commands_never_enter_the_outbox() -> Result<()> {
    let machine = CommandSessionMachine::new();
    let mut command = tool_command([9; 16])?;
    command.kind = "invalid command kind".into();
    let input = json!({"command":command,"terminal":false});
    assert!(machine.initialize(&input).is_err());
    assert!(
        machine
            .transition(&json!({"stage":"idle"}), &input)
            .is_err()
    );
    assert!(
        machine
            .transition(&json!({"stage":"ready","input":input}), &Value::Null)
            .is_err()
    );
    let mut input = json!({"command":tool_command([9; 16])?,"terminal":false,"unexpected":true});
    assert!(machine.initialize(&input).is_err());
    input
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("input is not an object".into()))?
        .remove("unexpected");
    input
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("input is not an object".into()))?
        .remove("terminal");
    assert!(machine.initialize(&input).is_err());
    Ok(())
}

proptest! {
    // Property: each admitted command occupies one slot until its exact result;
    // foreign results and malformed envelopes cannot advance the session.
    #[test]
    fn session_slot_retains_identity_and_round_trips(
        id in any::<[u8; 16]>(), terminal in any::<bool>(), rejected in any::<bool>(), text in ".{0,128}",
    ) {
        let machine = CommandSessionMachine::new();
        let command = tool_command(id)?;
        let input = json!({"command":command,"terminal":terminal});
        let ready = machine.initialize(&input)?;
        prop_assert!(machine.transition(&ready, &input).is_err());
        let emitted = machine.transition(&ready, &Value::Null)?;
        prop_assert_eq!(&emitted.commands, &vec![command.clone()]);
        prop_assert_eq!(&emitted, &machine.transition(&ready, &Value::Null)?);
        let encoded = serde_json::to_vec(&emitted.state).map_err(|e| invalid(&e))?;
        let waiting = serde_json::from_slice::<Value>(&encoded).map_err(|e| invalid(&e))?;
        prop_assert_eq!(&waiting, &emitted.state);
        let foreign = OperationId::from_bytes(id.map(|byte| byte ^ 1));
        let value = if rejected {json!({"Failed":{"message":text}})} else {json!({"answer":text})};
        let valid = result(command.operation_id, value);
        for invalid in [Value::Null, input, result(foreign, Value::Null),
            json!({"commands":[]}), json!({"commands":valid["commands"],"extra":true}),
            json!({"commands":[valid["commands"][0],valid["commands"][0]]}),
            json!({"commands":[{"operation_id":command.operation_id}]}),
            json!({"commands":[{"operation_id":command.operation_id,"value":null,"extra":true}]}),
            result(command.operation_id,json!({"Failed":{"message":42}})),
            result(command.operation_id,json!({"Failed":{"message":"rejected"},"Succeeded":null})),
        ] {
            prop_assert!(machine.transition(&waiting, &invalid).is_err());
        }
        let settled = machine.transition(&waiting, &valid)?;
        prop_assert!(settled.commands.is_empty());
        if terminal && !rejected {
            prop_assert_eq!(&settled.status, &MachineStatus::Completed {value:Value::Null});
            prop_assert!(machine.transition(&settled.state, &Value::Null).is_err());
        } else {
            prop_assert_eq!(&settled.status, &MachineStatus::Suspended);
            let idle = machine.transition(&settled.state, &Value::Null)?;
            prop_assert!(idle.commands.is_empty());
            prop_assert_eq!(&idle.state, &settled.state);
            let next = tool_command(foreign.into_bytes())?;
            let next = machine.transition(&settled.state,&json!({"command":next,"terminal":true}))?;
            prop_assert_eq!(next.commands, vec![tool_command(foreign.into_bytes())?]);
            let closed = machine.transition(&next.state,&result(foreign,Value::Null))?;
            prop_assert_eq!(closed.status,MachineStatus::Completed{value:Value::Null});
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Only an idle session accepts its owned native outcome. No command is
    // emitted by completion; an outstanding hook must settle first.
    #[test]
    fn native_completion_waits_for_hooks_and_preserves_the_outcome(
        id in any::<[u8;16]>(), failed in any::<bool>(), text in ".{0,128}",
    ) {
        let machine=CommandSessionMachine::new();
        let command=tool_command(id)?;
        let ready=machine.initialize(&json!({"command":command,"terminal":false}))?;
        let waiting=machine.transition(&ready,&Value::Null)?;
        let outcome=if failed {json!({"Failed":{"message":text}})} else {json!({"Succeeded":{"success":true,"exit_code":0,"native":text}})};
        let finished=json!({"native_exit":outcome});
        prop_assert!(machine.transition(&waiting.state,&finished).is_err());
        let idle=machine.transition(&waiting.state,&result(command.operation_id,Value::Null))?;
        let completed=machine.transition(&idle.state,&finished)?;
        prop_assert!(completed.commands.is_empty());
        let expected=if failed {MachineStatus::Failed {message:text}} else {MachineStatus::Completed {value:json!({"success":true,"exit_code":0,"native":text})}};
        prop_assert_eq!(completed.status,expected);
        prop_assert!(machine.transition(&completed.state,&finished).is_err());
        for invalid in [json!({"native_exit":outcome,"extra":true}),json!({"native_exit":"Cancelled"}),
            json!({"native_exit":{"Failed":{"message":42}}})] {
            prop_assert!(machine.transition(&idle.state,&invalid).is_err());
        }
        let mut command=command;
        command.kind="acyclic.native_process.v1".into();
        let ready=machine.initialize(&json!({"command":command,"terminal":true}))?;
        let waiting=machine.transition(&ready,&Value::Null)?;
        let value=json!({"success":true,"exit_code":0});
        let settled=machine.transition(&waiting.state,&result(command.operation_id,value.clone()))?;
        prop_assert_eq!(settled.status,MachineStatus::Completed{value});
    }
}

#[tokio::test]
async fn command_session_reopens_the_existing_outbox_without_reemitting_effects() -> Result<()> {
    let machine = Arc::new(CommandSessionMachine::new());
    let mut tasks = acyclic_harness::runtime::TaskRegistry::default();
    tasks.register(machine.definition()?)?;
    tasks.get::<Value, Value>("acyclic.command_session@1")?;
    let mut registry = MachineRegistry::default();
    registry.register(machine.clone(), &mut None)?;
    let command = tool_command([9; 16])?;
    let initial = MachineCheckpoint {
        machine: machine.identity().clone(),
        revision: 0,
        state: machine.initialize(&json!({"command":command,"terminal":false}))?,
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
    let dispatch_id = OperationId::from_bytes([2; 16]);
    let dispatch_key = IdempotencyKey::new("dispatch")?;
    let emitted = host
        .step(dispatch_id, dispatch_key.clone(), Value::Null, |_| Ok(()))
        .await?;
    assert_eq!(emitted.commands, vec![command.clone()]);
    drop(host);
    let mut host =
        DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    assert_eq!(
        host.step(dispatch_id, dispatch_key, Value::Null, |_| Ok(()))
            .await?,
        emitted
    );
    assert_eq!(host.checkpoint().revision, 1);
    host.step(
        OperationId::from_bytes([3; 16]),
        IdempotencyKey::new("settle")?,
        result(command.operation_id, Value::Null),
        |_| Ok(()),
    )
    .await?;
    drop(host);
    let mut host =
        DurableWorkflowHost::open(registry.clone(), initial.clone(), journal.clone()).await?;
    let terminal = tool_command([10; 16])?;
    let dispatched = host
        .step(
            OperationId::from_bytes([4; 16]),
            IdempotencyKey::new("detach")?,
            json!({"command":terminal,"terminal":true}),
            |_| Ok(()),
        )
        .await?;
    assert_eq!(dispatched.commands, vec![terminal.clone()]);
    let completed = host
        .step(
            OperationId::from_bytes([5; 16]),
            IdempotencyKey::new("detached")?,
            result(terminal.operation_id, Value::Null),
            |_| Ok(()),
        )
        .await?;
    assert_eq!(
        completed.status,
        MachineStatus::Completed { value: Value::Null }
    );
    drop(host);
    let host = DurableWorkflowHost::open(registry, initial, journal.clone()).await?;
    assert_eq!(host.latest_transition().await?, Some(completed));
    assert_eq!(journal.replay(0, 8, None)?.await?.0.len(), 4);
    Ok(())
}
