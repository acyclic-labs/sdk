use super::*;

#[test]
fn committed_events_attest_admission_without_persisting_bearer_scopes() -> Result<()> {
    assert!(
        !format!("{:?}", issuer().verifier(&mut None).expect("verifier copy")).contains("[7, 7, 7")
    );
    let mut reducer = reducer_fixture();
    let ApplyResult::Applied { event } = reducer.plan(&command(
        operation(19),
        0,
        Action::BindConversation {
            agent: AgentId::from_bytes([19; 16]),
        },
    ))?
    else {
        panic!("fresh event replayed")
    };
    let json = serde_json::to_value(&event).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(json["scope"].get("proof").is_none());
    assert!(json["scope"].get("parent_proof").is_none());
    assert!(serde_json::from_value::<Scope>(json["scope"].clone()).is_err());
    let mut tampered = event.clone();
    tampered.scope.capabilities = Capabilities::new(["conversation:bind", "effect:run"]);
    assert!(matches!(
        reducer.apply_committed(tampered, &mut None),
        Err(Error::Unauthorized(_))
    ));
    reducer.apply_committed(event, &mut None)?;
    assert_eq!(reducer.revision(), 1);
    Ok(())
}

#[test]
fn scope_attenuation_cannot_expand_authority() {
    let owner = issuer();
    let root = owner
        .issue(
            "root",
            &Capabilities::new(["read", "write"]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(
        owner
            .attenuate(&root, "child", Capabilities::new(["read"]))
            .is_ok()
    );
    assert!(matches!(
        owner.attenuate(&root, "child", Capabilities::new(["read", "admin"])),
        Err(Error::Unauthorized(_))
    ));
}

#[test]
fn mutated_or_foreign_scopes_are_rejected() -> Result<()> {
    let mut command = command(
        operation(1),
        0,
        Action::AppendCustom {
            schema: "example.message".into(),
            version: 1,
            content: request_file(b"{}")?,
        },
    );
    command.scope = issuer()
        .issue(
            "root",
            &Capabilities::new(["event:append"]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    let foreign = AuthorityIssuer::new(
        "test",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "task-1".into(),
        },
    );
    let reducer = Reducer::new(
        foreign.audience.clone(),
        foreign.verifier(&mut None).expect("verifier copy"),
        schemas(),
    );
    assert!(matches!(
        reducer.plan(&command),
        Err(Error::Unauthorized(_))
    ));
    command.scope.capabilities = Capabilities::new(["event:append", "effect:run"]);
    assert!(matches!(
        reducer_fixture().plan(&command),
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}
