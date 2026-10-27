use acyclic_machines::provider_core::{Admission, OperationRegistry, operation_id};
use acyclic_machines::{
    EventFact, IdempotencyKey, MachineId, MachineState, MutationOutcome, OperationPhase,
    ProviderError,
};

fn key(suffix: u8) -> Result<IdempotencyKey, acyclic_machines::IdentityError> {
    IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}"))
}

#[test]
fn admission_replays_success_and_rejects_rebinding() -> Result<(), Box<dyn std::error::Error>> {
    let registry = OperationRegistry::default();
    let Admission::Fresh(id) = registry.admit(key(1)?, &"intent-a")? else {
        return Err("expected fresh admission".into());
    };
    assert!(matches!(
        registry.admit(key(1)?, &"intent-a"),
        Err(ProviderError::Indeterminate(_))
    ));
    assert!(matches!(
        registry.admit(key(1)?, &"intent-b"),
        Err(ProviderError::Conflict(_))
    ));
    let machine = MachineId::new();
    registry.complete(id, MutationOutcome::Suspended(machine));
    let Admission::Replay(outcome) = registry.admit(key(1)?, &"intent-a")? else {
        return Err("expected replay admission".into());
    };
    assert_eq!(*outcome, MutationOutcome::Suspended(machine));
    assert_eq!(operation_id(key(1)?), id);
    assert_ne!(operation_id(key(1)?), operation_id(key(2)?));
    // Pinned so recovery keeps finding operations admitted by earlier releases.
    assert_eq!(
        operation_id(key(1)?).to_string(),
        "5826b47d-5e02-5c80-a94c-145737646d7f"
    );
    Ok(())
}

#[test]
fn cancel_returns_bound_sandboxes_once() -> Result<(), Box<dyn std::error::Error>> {
    let registry = OperationRegistry::default();
    let Admission::Fresh(id) = registry.admit(key(3)?, &"x")? else {
        return Err("expected fresh admission".into());
    };
    assert!(registry.bind_created(id, "sb-1"));
    registry.bind_target(id, "existing");
    let (observation, sandboxes) = registry.cancel(id).ok_or("missing operation")?;
    assert_eq!(observation.phase, OperationPhase::Cancelled);
    assert_eq!(sandboxes, vec!["sb-1".to_owned()]);
    assert!(registry.cancel(id).ok_or("missing operation")?.1.is_empty());
    assert!(
        !registry.bind_created(id, "sb-late"),
        "a sandbox created after cancellation is handed back to the caller to delete"
    );
    assert_eq!(
        registry.record(id).ok_or("missing record")?.created,
        Vec::<String>::new()
    );
    registry.complete(id, MutationOutcome::Woken(MachineId::new()));
    assert_eq!(
        registry.inspect(id).ok_or("missing observation")?.phase,
        OperationPhase::Cancelled
    );
    Ok(())
}

#[test]
fn cancel_finishes_undoing_indeterminate_and_failed_operations()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = OperationRegistry::default();
    let Admission::Fresh(id) = registry.admit(key(5)?, &"fork")? else {
        return Err("expected fresh admission".into());
    };
    assert!(registry.bind_created(id, "child-0"));
    registry.fail(id, &ProviderError::Unavailable);
    let (observation, sandboxes) = registry.cancel(id).ok_or("missing operation")?;
    assert_eq!(observation.phase, OperationPhase::Cancelled);
    assert_eq!(sandboxes, ["child-0"]);

    let Admission::Fresh(id) = registry.admit(key(6)?, &"fork")? else {
        return Err("expected fresh admission".into());
    };
    assert!(registry.bind_created(id, "child-1"));
    registry.fail(id, &ProviderError::Failed);
    assert_eq!(registry.take_created(id), ["child-1"]);
    registry.restore_created(id, vec!["child-1".to_owned()]);
    let (observation, sandboxes) = registry.cancel(id).ok_or("missing operation")?;
    assert_eq!(observation.phase, OperationPhase::Failed);
    assert_eq!(sandboxes, ["child-1"]);
    assert!(registry.cancel(id).ok_or("missing operation")?.1.is_empty());
    Ok(())
}

#[test]
fn cancel_never_returns_the_machine_an_operation_acts_on() -> Result<(), Box<dyn std::error::Error>>
{
    let registry = OperationRegistry::default();
    let Admission::Fresh(id) = registry.admit(key(4)?, &"suspend")? else {
        return Err("expected fresh admission".into());
    };
    registry.bind_target(id, "existing-machine");
    let (observation, sandboxes) = registry.cancel(id).ok_or("missing operation")?;
    assert_eq!(observation.phase, OperationPhase::Cancelled);
    assert!(sandboxes.is_empty(), "{sandboxes:?}");
    Ok(())
}

#[test]
fn observations_append_events_only_on_state_change() -> Result<(), Box<dyn std::error::Error>> {
    let registry = OperationRegistry::default();
    let sandbox: crate::api::Sandbox =
        serde_json::from_str(include_str!("../tests/fixtures/sandbox_started.json"))?;
    let mut observation = crate::map::sandbox_to_observation(&sandbox, None, None, 0)?;
    registry.observe_machine(observation.clone(), 1);
    registry.observe_machine(observation.clone(), 2);
    observation.state = MachineState::Suspended;
    registry.observe_machine(observation.clone(), 3);
    observation.state = MachineState::Running;
    registry.observe_machine(observation.clone(), 4);
    let (events, next) = registry.events(observation.id, None, 2);
    assert_eq!(events.len(), 2);
    assert_eq!(next, Some(2));
    let (rest, next) = registry.events(observation.id, next, 16);
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].fact, EventFact::State(MachineState::Running));
    assert_eq!(next, None);
    registry.mark_destroyed(observation.id, 5);
    assert_eq!(
        registry
            .machine(observation.id)
            .ok_or("missing machine")?
            .state,
        MachineState::Destroyed
    );
    Ok(())
}
