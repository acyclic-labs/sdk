
use super::*;
use acyclic_harness::workflow::{MachineCheckpoint, MachineRegistry, MachineTransition};

const DEPTH: usize = 8;

fn inputs(command: &WorkflowCommand) -> Vec<Value> {
    let success = result(
        command.operation_id,
        Outcome::Succeeded(command.payload.clone()),
    );
    let alternative = FileRef::new(
        command.payload.volume().clone(),
        "different-result.json",
        "different",
        acyclic_harness::conversation::FileDescriptor::from_bytes(b"[]", "application/json")
            .unwrap(),
        "different-result.json",
    )
    .unwrap();
    vec![
        Value::Null,
        json!({}),
        json!({"commands":[]}),
        json!({"commands":[{},{}]}),
        json!({"commands":[],"unexpected":true}),
        success,
        result(
            command.operation_id,
            Outcome::Succeeded(alternative),
        ),
        result(
            OperationId::from_bytes([99; 16]),
            Outcome::Succeeded(command.payload.clone()),
        ),
        result(
            command.operation_id,
            Outcome::Failed {
                message: "known failure".to_owned(),
            },
        ),
        result(command.operation_id, Outcome::<FileRef>::Cancelled),
        result(
            command.operation_id,
            Outcome::<FileRef>::Indeterminate {
                operation_id: command.operation_id,
            },
        ),
    ]
}

fn replay_step(
    registry: &MachineRegistry,
    checkpoint: &MachineCheckpoint,
    input: &Value,
) -> acyclic_harness::Result<(MachineCheckpoint, MachineTransition, acyclic_fs::NativeAllocation)> {
    let original = work();
    let mut guarded = crate::pi_root_provider::guarded_pi_value(input, NATIVE_METADATA_MAXIMUM, &original)?;
    let mut before = registry.step_with_native_work(checkpoint, guarded.0, &original);
    if let Ok(first) = &mut before { first.2.absorb(&mut guarded.1).unwrap(); }
    let bytes = serde_json::to_vec(checkpoint)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let restored = serde_json::from_slice(&bytes)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let mut guarded = crate::pi_root_provider::guarded_pi_value(input, NATIVE_METADATA_MAXIMUM, &original)?;
    let mut after = registry.step_with_native_work(&restored, guarded.0, &original);
    if let Ok(second) = &mut after { second.2.absorb(&mut guarded.1).unwrap(); }
    match (&before, &after) {
        (Ok(first), Ok(second)) => assert_eq!((&first.0, &first.1), (&second.0, &second.1)),
        (Err(first), Err(second)) => assert_eq!(first.code(), second.code()),
        _ => panic!("crash replay changed transition acceptance"),
    }
    after
}

fn explore(execution: PiTaskExecution, command: &WorkflowCommand) {
    let inputs = inputs(command);
    let original = work();
    let machine = PiTaskMachine::new(&original).unwrap();
    let state = machine
        .initialize_with_native_work(
            &serde_json::to_value(PiTaskInput {
                binding: binding(),
                execution,
            })
            .unwrap(), &original,
        )
        .unwrap();
    let mut registry = MachineRegistry::default();
    registry.register(machine.clone(), &mut None).unwrap();
    let mut frontier = vec![(MachineCheckpoint {
            machine: machine.identity().clone(),
            revision: 0,
            state: state.0,
        }, state.1)];
    let mut settled = false;
    for _ in 0..DEPTH {
        let mut next = Vec::new();
        for (checkpoint, _custody) in frontier {
            let pinned = checkpoint.state.get("allocation").or_else(|| checkpoint.state.get("result"));
            settled |= pinned.is_some();
            for input in &inputs {
                if let Ok((advanced, transition, custody)) = replay_step(&registry, &checkpoint, input) {
                    assert_eq!(transition.commands.as_slice(),
                        if checkpoint.revision == 0 { std::slice::from_ref(command) } else { &[] });
                    let next_pinned = advanced.state.get("allocation").or_else(|| advanced.state.get("result"));
                    if pinned.is_some() { assert_eq!(pinned, next_pinned); }
                    assert_eq!(advanced.revision, checkpoint.revision + 1);
                    registry.validate_checkpoint_with_native_work(&advanced, custody.work()).unwrap();
                    drop(transition);
                    next.push((advanced, custody));
                }
            }
        }
        frontier = next;
    }
    assert!(
        settled,
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
