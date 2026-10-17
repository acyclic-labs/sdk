use super::*;
use crate::{
    AgentId, TaskId,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use proptest::prelude::*;

fn commands() -> (WorkflowCommand, WorkflowCommand) {
    let payload = FileRef::new(
        VolumeRef::new(
            ProviderRef::new(
                "child-agent-test".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )
            .unwrap(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
            &mut None,
        )
        .unwrap(),
        "commands/model.json".into(),
        "immutable-command".into(),
        FileDescriptor::from_bytes(b"{}", "application/json", &mut None).unwrap(),
        "model.json".into(),
        &mut None,
    )
    .unwrap();
    (
        WorkflowCommand {
            operation_id: OperationId::from_bytes([10; 16]),
            kind: super::super::MAIL_RECEIVE_TASK_COMMAND_KIND.into(),
            payload: payload.clone(),
        },
        WorkflowCommand {
            operation_id: OperationId::from_bytes([9; 16]),
            kind: super::super::MODEL_TASK_COMMAND_KIND.into(),
            payload,
        },
    )
}

fn result(command: &WorkflowCommand, value: Value) -> Value {
    json!({"commands":[{"operation_id":command.operation_id,"value":value}]})
}

#[test]
fn checked_child_turn_seed_advances_to_the_next_gapless_receive() {
    let seed: Value = serde_json::from_slice(include_bytes!(
        "../../../../harness-fuzz/corpus/child_control/child-agent-turn"
    ))
    .unwrap();
    let machine = ChildAgentMachine::new();
    let state = machine.initialize(&seed["initial"]).unwrap();
    let waiting = machine.transition(&state, &Value::Null).unwrap();
    let active = machine.transition(&waiting.state, &seed["input"]).unwrap();
    let next = machine
        .transition(&active.state, &seed["model_result"])
        .unwrap();
    assert_eq!(mail_cursor(&next, &next.commands[0]).unwrap(), 1);
    assert_eq!(next.status, MachineStatus::Suspended);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Every accepted message advances exactly once. Retry, stale/foreign mail,
    // foreign commands and uncertain model results cannot advance the cursor.
    #[test]
    fn repeated_turns_keep_gapless_mail_and_distinct_replayable_commands(
        task in any::<[u8;16]>(), turns in 1u64..9, outcome in 0u8..3,
    ) {
        let machine = ChildAgentMachine::new();
        let (receive, model) = commands();
        let initial = machine.initialize(&json!({"receive":receive,"model":model})).unwrap();
        let mut waiting = machine.transition(&initial, &Value::Null).unwrap();
        let mut operations = std::collections::BTreeSet::new();
        prop_assert!(operations.insert(receive.operation_id));
        for sequence in 1..=turns {
            let receive = waiting.commands[0].clone();
            prop_assert_eq!(mail_cursor(&waiting, &receive).unwrap(), sequence - 1);
            let mut foreign = receive.clone();
            foreign.operation_id = OperationId::from_bytes([80;16]);
            prop_assert!(mail_cursor(&waiting, &foreign).is_err());
            let item = InboxItem { task_id: TaskId::from_bytes(task), sequence,
                message_id: format!("message-{sequence}"), payload: receive.payload.clone() };
            for invalid in [0, sequence - 1, sequence + 1] {
                let mut bad = item.clone();
                bad.sequence = invalid;
                prop_assert!(machine.transition(&waiting.state, &result(&receive, json!({"Succeeded":bad}))).is_err(), "invalid sequence accepted");
            }
            if sequence > 1 {
                let mut other = task;
                other[0] ^= 1;
                let mut bad = item.clone();
                bad.task_id = TaskId::from_bytes(other);
                prop_assert!(machine.transition(&waiting.state, &result(&receive, json!({"Succeeded":bad}))).is_err(), "foreign owner accepted");
            }
            let incoming = result(&receive, json!({"Succeeded":item}));
            let active = machine.transition(&waiting.state, &incoming).unwrap();
            prop_assert_eq!(machine.transition(&waiting.state, &incoming).unwrap(), active.clone());
            let model = active.commands[0].clone();
            prop_assert!(operations.insert(model.operation_id));
            prop_assert!(mail_cursor(&active, &model).is_err());
            prop_assert!(machine.transition(&active.state, &result(&model,
                json!({"Indeterminate":{"operation_id":model.operation_id}}))).is_err(), "uncertain turn advanced");
            prop_assert!(machine.transition(&active.state, &result(&foreign, Value::Null)).is_err());
            let output = match outcome {
                0 => json!({"Succeeded":{"text":"done","metadata":null,"steps":1,"attachments":[]}}),
                1 => json!({"Failed":{"message":"rejected"}}),
                _ => json!("Cancelled"),
            };
            let completed = result(&model, output.clone());
            let next = machine.transition(&active.state, &completed).unwrap();
            let record = crate::workflow::WorkflowRecord {
                operation_id: model.operation_id,
                idempotency_key: crate::IdempotencyKey::new(format!("turn-{sequence}")).unwrap(),
                prior: crate::workflow::MachineCheckpoint {
                    machine: machine.identity().clone(), revision: sequence * 2,
                    state: active.state.clone(),
                },
                input: completed.clone(), transition: next.clone(),
            };
            let projected = completed_turn(TaskId::from_bytes(task), &record).unwrap();
            prop_assert_eq!(projected.turn, sequence);
            prop_assert_eq!(serde_json::to_value(&projected.outcome).unwrap(), output);
            let mut other = task; other[0] ^= 1;
            prop_assert!(completed_turn(TaskId::from_bytes(other), &record).is_err());
            let mut wrong_command = record.clone();
            wrong_command.input["commands"][0]["operation_id"] = json!(foreign.operation_id);
            prop_assert!(completed_turn(TaskId::from_bytes(task), &wrong_command).is_err());

            prop_assert_eq!(machine.transition(&active.state, &completed).unwrap(), next.clone());
            prop_assert_eq!(&next.status, &MachineStatus::Suspended);
            prop_assert_eq!(mail_cursor(&next, &next.commands[0]).unwrap(), sequence);
            prop_assert!(operations.insert(next.commands[0].operation_id));
            waiting = next;
        }
    }
}
