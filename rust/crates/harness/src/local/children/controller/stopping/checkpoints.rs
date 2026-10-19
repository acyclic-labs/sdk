use super::*;
use crate::{
    scheduler::WorkflowSuspension,
    workflow::{MachineCheckpoint, MachineRegistry, ResumableMachine},
};
use proptest::prelude::*;
use std::borrow::Cow;

type Predicate = fn(Option<ChildStatus>, TaskId, u64, Option<&WorkflowSuspension>) -> bool;

fn checkpoint_cuts(predicate: Predicate, task: TaskId, turns: u64, assert_property: bool) {
    let seed: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../../harness-fuzz/corpus/child_control/child-agent-turn"
    ))
    .unwrap();
    let machine = Arc::new(crate::filesystem::ChildAgentMachine::new());
    let mut registry = MachineRegistry::default();
    registry.register(machine.clone(), &mut None).unwrap();
    let initial = MachineCheckpoint {
        machine: machine.identity().clone(),
        revision: 0,
        state: machine.initialize(&seed["initial"]).unwrap(),
    };
    let (mut checkpoint, mut transition, _, _) = registry
        .step(&initial, Cow::Owned(serde_json::Value::Null), None)
        .unwrap();
    let (mut actual, mut expected) = (Vec::new(), Vec::new());
    actual.push(predicate(None, task, 0, None));
    expected.push(true);
    let unstarted = ChildStatus {
        task,
        state: ChildState::Cancelled,
        settled: true,
        inputs: 1,
    };
    actual.push(predicate(Some(unstarted), task, 0, None));
    expected.push(true);
    actual.push(predicate(
        Some(ChildStatus {
            state: ChildState::Running,
            ..unstarted
        }),
        task,
        0,
        None,
    ));
    expected.push(false);
    actual.push(predicate(
        Some(ChildStatus {
            settled: false,
            ..unstarted
        }),
        task,
        0,
        None,
    ));
    expected.push(false);
    actual.push(predicate(Some(unstarted), task, 1, None));
    expected.push(false);

    for turn in 1..=turns {
        let previous = Some(ChildStatus {
            task,
            state: ChildState::Running,
            settled: false,
            inputs: turn,
        });
        let slot = |revision, command| WorkflowSuspension {
            revision,
            waiting_command: Some(command),
            input: None,
        };
        let receive = slot(checkpoint.revision, transition.commands[0].operation_id);
        actual.push(predicate(previous, task, turn - 1, Some(&receive)));
        expected.push(true);
        let mut incoming = seed["input"].clone();
        incoming["commands"][0]["operation_id"] =
            checkpoint.state["receive"]["operation_id"].clone();
        incoming["commands"][0]["value"]["Succeeded"]["task_id"] =
            serde_json::to_value(task).unwrap();
        incoming["commands"][0]["value"]["Succeeded"]["sequence"] = serde_json::json!(turn);
        (checkpoint, transition, _, _) = registry
            .step(&checkpoint, Cow::Owned(incoming), None)
            .unwrap();
        let model = slot(checkpoint.revision, transition.commands[0].operation_id);
        actual.push(predicate(previous, task, turn - 1, Some(&model)));
        expected.push(false);
        let mut completed = seed["model_result"].clone();
        completed["commands"][0]["operation_id"] =
            checkpoint.state["model"]["operation_id"].clone();
        completed["commands"][0]["value"] = serde_json::json!("Cancelled");
        (checkpoint, transition, _, _) = registry
            .step(&checkpoint, Cow::Owned(completed), None)
            .unwrap();
        let receive = slot(checkpoint.revision, transition.commands[0].operation_id);
        actual.push(predicate(previous, task, turn, Some(&receive)));
        expected.push(true);
        let mut foreign = task.into_bytes();
        foreign[0] ^= 1;
        actual.push(predicate(
            previous,
            TaskId::from_bytes(foreign),
            turn,
            Some(&receive),
        ));
        expected.push(false);
        actual.push(predicate(previous, task, turn + 1, Some(&receive)));
        expected.push(false);
        actual.push(predicate(previous, task, turn, None));
        expected.push(false);
        let missing = WorkflowSuspension {
            waiting_command: None,
            ..receive
        };
        actual.push(predicate(previous, task, turn, Some(&missing)));
        expected.push(false);
    }
    if assert_property {
        assert_eq!(
            actual, expected,
            "cancelled child acknowledged an in-flight model or foreign cut"
        );
    }
}

fn missing_cursor_fence(
    previous: Option<ChildStatus>,
    task: TaskId,
    published: u64,
    slot: Option<&WorkflowSuspension>,
) -> bool {
    let slot = slot.cloned().map(|slot| WorkflowSuspension {
        revision: published * 2 + 1,
        ..slot
    });
    cancelled_turn_is_quiet(previous, task, published, slot.as_ref())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn cancelled_ack_uses_only_the_real_receive_checkpoint(task in any::<[u8;16]>(), turns in 1u64..9) {
        checkpoint_cuts(cancelled_turn_is_quiet, TaskId::from_bytes(task), turns, true);
    }
}

#[test]
fn cancelled_ack_negative_control_depends_on_the_cursor_property() {
    let task = TaskId::from_bytes([8; 16]);
    std::panic::catch_unwind(|| checkpoint_cuts(missing_cursor_fence, task, 2, true))
        .expect_err("missing receive fence was not detected");
    checkpoint_cuts(missing_cursor_fence, task, 2, false);
}
