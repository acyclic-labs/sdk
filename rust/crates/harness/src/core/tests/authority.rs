use super::*;

#[test]
fn committed_events_attest_admission_without_persisting_bearer_scopes() -> Result<()> {
    let owner = issuer();
    let mut reducer = reducer_fixture();
    let agent = AgentId::from_bytes([19; 16]);
    let mut command = command(operation(19), 0, Action::BindConversation { agent });
    owner.attenuate(
        &command.scope,
        "child",
        Capabilities::new(["conversation:bind"]),
    )?;
    assert!(
        owner
            .attenuate(&command.scope, "child", Capabilities::new(["admin"]))
            .is_err()
    );
    let ApplyResult::Applied { event } = reducer.plan(&command)? else {
        panic!("fresh event replayed")
    };
    let wire = serde_json::to_value(&event).unwrap();
    assert!(wire["scope"].get("proof").is_none());
    assert!(wire["scope"].get("parent_proof").is_none());
    assert!(serde_json::from_value::<Scope>(wire["scope"].clone()).is_err());
    let mut tampered = event.clone();
    tampered.scope.capabilities = Capabilities::new(["conversation:bind", "effect:run"]);
    assert!(reducer.apply_committed(tampered, &mut None).is_err());
    let foreign = AuthorityIssuer::new(
        "test",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "task-1".into(),
        },
    );
    let verifier = foreign.verifier(&mut None)?;
    assert!(
        Reducer::new(foreign.audience, verifier, schemas())
            .plan(&command)
            .is_err()
    );
    command.scope.capabilities = Capabilities::new(["conversation:bind", "effect:run"]);
    assert!(reducer.plan(&command).is_err());
    reducer.apply_committed(event, &mut None)?;
    assert_eq!(reducer.revision(), 1);
    Ok(())
}
