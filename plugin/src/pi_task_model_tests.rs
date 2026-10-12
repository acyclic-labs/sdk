use super::*;
use acyclic_harness::workflow::{MachineCheckpoint, MachineRegistry};
#[test]
fn committed_pi_histories_preserve_one_command_and_first_result_across_crash_replay() {
    let command = preparation();
    let alternative = FileRef::new(command.payload.volume().clone(), "different-result.json", "different", acyclic_harness::conversation::FileDescriptor::from_bytes(b"[]", "application/json").unwrap(), "different-result.json").unwrap();
    let inputs = [Value::Null, json!({}), json!({"commands":[]}), json!({"commands":[{},{}]}), json!({"commands":[],"unexpected":true}),
        result(command.operation_id, Outcome::Succeeded(command.payload.clone())), result(command.operation_id, Outcome::Succeeded(alternative)),
        result(OperationId::from_bytes([99; 16]), Outcome::Succeeded(command.payload.clone())), result(command.operation_id, Outcome::Failed { message: "known failure".into() }),
        result(command.operation_id, Outcome::<FileRef>::Cancelled), result(command.operation_id, Outcome::<FileRef>::Indeterminate { operation_id: command.operation_id })];
    for execution in [PiTaskExecution::Host { root: PiRootIntent::ScratchOnly {}, preparation: command.clone() }, PiTaskExecution::Child { launch: command.clone() }] {
        let original = work(); let machine = PiTaskMachine::new(&original).unwrap(); let mut registry = MachineRegistry::default(); registry.register(machine.clone(), &mut None).unwrap();
        let state = machine.initialize_with_native_work(&serde_json::to_value(PiTaskInput { binding: binding(), execution }).unwrap(), &original).unwrap();
        let mut frontier = vec![(MachineCheckpoint { machine: machine.identity().clone(), revision: 0, state: state.0 }, state.1)]; let mut settled = false;
        for _ in 0..8 { let mut next = Vec::new();
            for (checkpoint, _custody) in frontier {
                let pinned = checkpoint.state.get("allocation").or_else(|| checkpoint.state.get("result")); settled |= pinned.is_some();
                for input in &inputs {
                    let restored: MachineCheckpoint = serde_json::from_slice(&serde_json::to_vec(&checkpoint).unwrap()).unwrap(); let work = work(); let [before, after] = [&checkpoint, &restored].map(|cut| {
                        let mut admitted = crate::pi_root_provider::guarded_pi_value(input, NATIVE_METADATA_MAXIMUM, &work)?;
                        let mut step = registry.step_with_native_work(cut, admitted.0, &work); if let Ok(step) = &mut step { step.2.absorb(&mut admitted.1).unwrap(); } step
                    });
                    match (&before, &after) { (Ok(a), Ok(b)) => assert_eq!((&a.0, &a.1), (&b.0, &b.1)), (Err(a), Err(b)) => assert_eq!(a.code(), b.code()), _ => panic!("crash replay changed acceptance") }; drop(before);
                    if let Ok((advanced, transition, custody)) = after {
                        assert_eq!(transition.commands.as_slice(), if checkpoint.revision == 0 { std::slice::from_ref(&command) } else { &[] }); if pinned.is_some() { assert_eq!(pinned, advanced.state.get("allocation").or_else(|| advanced.state.get("result"))); }
                        assert_eq!(advanced.revision, checkpoint.revision + 1); registry.validate_checkpoint_with_native_work(&advanced, custody.work()).unwrap(); drop(transition); next.push((advanced, custody));
                    }
                }
            }
            frontier = next;
        }
        assert!(settled, "exploration never reached successful settlement");
    }
}
