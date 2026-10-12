//! Bounded checkpoint exploration of the production registered Pi machine.

use super::*;
use acyclic_harness::workflow::{MachineCheckpoint, MachineTransition};
use std::collections::BTreeSet;

const DEPTH: usize = 8;

struct PathState {
    checkpoint: MachineCheckpoint,
    emitted: usize,
    pinned: Option<Value>,
}

impl PathState {
    fn advance(
        &self,
        checkpoint: MachineCheckpoint,
        transition: &MachineTransition,
        command: &WorkflowCommand,
    ) -> Self {
        let emitted = self.emitted + transition.commands.len();
        assert!(emitted <= 1);
        assert!(transition.commands.iter().all(|actual| actual == command));
        let pinned = checkpoint
            .state
            .get("allocation")
            .or_else(|| checkpoint.state.get("result"))
            .cloned();
        if self.pinned.is_some() { assert_eq!(self.pinned, pinned); }
        if pinned.is_some() { assert_eq!(emitted, 1); }
        Self { checkpoint, emitted, pinned }
    }

}

fn registry() -> MachineRegistry {
    let mut registry = MachineRegistry::default();
    registry.register(Arc::new(PiTaskMachine::new())).unwrap();
    registry
}

fn inputs(command: &WorkflowCommand) -> Vec<Value> {
    let completion =
        |operation_id, value| json!({"commands":[{"operation_id":operation_id,"value":value}]});
    let success = completion(
        command.operation_id,
        Outcome::Succeeded(command.payload.clone()),
    );
    vec![
        Value::Null,
        json!({}),
        json!({"commands":[]}),
        json!({"commands":[{},{}]}),
        json!({"commands":[],"unexpected":true}),
        success,
        completion(
            command.operation_id,
            Outcome::Succeeded(alternative_result(command)),
        ),
        completion(
            OperationId::from_bytes([99; 16]),
            Outcome::Succeeded(command.payload.clone()),
        ),
        completion(
            command.operation_id,
            Outcome::Failed {
                message: "known failure".to_owned(),
            },
        ),
        completion(command.operation_id, Outcome::<FileRef>::Cancelled),
        completion(
            command.operation_id,
            Outcome::<FileRef>::Indeterminate {
                operation_id: command.operation_id,
            },
        ),
    ]
}

fn alternative_result(command: &WorkflowCommand) -> FileRef {
    // A second valid provider result must never replace the first settlement.
    FileRef::new(
        command.payload.volume().clone(),
        "different-result.json",
        "different",
        acyclic_harness::conversation::FileDescriptor::from_bytes(b"[]", "application/json")
            .unwrap(),
        "different-result.json",
    )
    .unwrap()
}

fn replay_step(
    path: &PathState,
    input: &Value,
) -> acyclic_harness::Result<(MachineCheckpoint, MachineTransition)> {
    let before = registry().step(&path.checkpoint, input);
    let bytes = serde_json::to_vec(&path.checkpoint)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let restored = serde_json::from_slice(&bytes)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let after = registry().step(&restored, input);
    match (&before, &after) {
        (Ok(first), Ok(second)) => assert_eq!(first, second),
        (Err(first), Err(second)) => assert_eq!(first.code(), second.code()),
        _ => panic!("crash replay changed transition acceptance"),
    }
    after
}

fn explore(execution: PiTaskExecution, command: &WorkflowCommand) {
    let machine = PiTaskMachine::new();
    let state = machine
        .initialize(
            &serde_json::to_value(PiTaskInput {
                binding: binding(),
                execution,
            })
            .unwrap(),
        )
        .unwrap();
    let mut frontier = vec![PathState {
        checkpoint: MachineCheckpoint {
            machine: machine.identity().clone(),
            revision: 0,
            state,
        },
        emitted: 0,
        pinned: None,
    }];
    let mut visited = BTreeSet::new();
    let mut settled = 0;
    for _ in 0..DEPTH {
        let mut next = Vec::new();
        for path in frontier {
            let key = serde_json::to_string(&(&path.checkpoint, path.emitted, &path.pinned)).unwrap();
            if !visited.insert(key) {
                continue;
            }
            settled += usize::from(path.pinned.is_some());
            for input in inputs(command) {
                if let Ok((checkpoint, transition)) = replay_step(&path, &input) {
                    let advanced = path.advance(checkpoint, &transition, command);
                    assert_eq!(advanced.checkpoint.revision, path.checkpoint.revision + 1);
                    registry()
                        .validate_checkpoint(&advanced.checkpoint)
                        .unwrap();
                    next.push(advanced);
                }
            }
        }
        frontier = next;
    }
    assert!(
        settled > 0,
        "exploration never reached a successful settlement"
    );
}

#[test]
fn committed_pi_histories_preserve_one_command_and_first_result_across_crash_replay() {
    let command = preparation();
    for execution in [
        PiTaskExecution::Host {
            root: PiRootIntent::ScratchOnly {},
            preparation: command.clone(),
        },
        PiTaskExecution::Child {
            launch: command.clone(),
        },
    ] {
        explore(execution, &command);
    }
}
