use super::*;

fn dependency(name: &str, version: u32) -> ExtensionDependency {
    ExtensionDependency {
        name: name.into(),
        version,
    }
}

fn agent_fixture(registry: SchemaRegistry) -> (Reducer, AuthorityIssuer) {
    let owner = AuthorityIssuer::new(
        "extension-host",
        [44; 32],
        Authority {
            kind: AggregateKind::Agent,
            id: "agent-extensions".into(),
        },
    );
    (
        Reducer::new(
            owner.audience.clone(),
            owner.verifier(&mut None).expect("verifier copy"),
            registry,
        ),
        owner,
    )
}

fn agent_command(owner: &AuthorityIssuer, id: u8, revision: u64, action: Action) -> Command {
    let mut command = command(operation(id), revision, action);
    command.scope = owner
        .issue(
            "owner",
            &Capabilities::new([
                "extension:configure",
                "extension:activate",
                "lifecycle:manage",
            ]),
            None,
            None,
            &mut None,
        )
        .expect("scope proof");
    command
}

#[test]
fn extension_activation_requires_validated_configuration_and_pins_revision() -> Result<()> {
    let extension = dependency("example.configured", 1);
    let mut registry = SchemaRegistry::new();
    registry.register(
        extension.name.clone(),
        extension.version,
        json!({"type":"object"}),
        [1; 32],
        ExtensionForkPolicy::Inherit,
        [],
        Some(json!({"type":"object","required":["enabled"],
            "properties":{"enabled":{"type":"boolean"}},"additionalProperties":false})),
    )?;
    let (mut reducer, owner) = agent_fixture(registry.clone());
    let select = |id, revision| {
        agent_command(
            &owner,
            id,
            revision,
            Action::SelectExtensions {
                roots: vec![extension.clone()],
            },
        )
    };
    assert!(matches!(
        reducer.apply(select(71, 0)),
        Err(Error::Conflict(_))
    ));
    let first = request_file(br#"{"enabled":true}"#)?;
    let invalid = request_file(br#"{"enabled":false,"extra":1}"#)?;
    assert!(
        reducer
            .schemas()
            .validate_configuration_bytes(
                &extension,
                &invalid,
                br#"{"enabled":false,"extra":1}"#,
                None
            )
            .is_err()
    );
    reducer.schemas().validate_configuration_bytes(
        &extension,
        &first,
        br#"{"enabled":true}"#,
        None,
    )?;
    reducer.apply(agent_command(
        &owner,
        72,
        0,
        Action::ConfigureExtension {
            extension: extension.clone(),
            content: first.clone(),
        },
    ))?;
    reducer.apply(select(73, 1))?;
    assert_eq!(reducer.active_configurations()[0].content, first);
    let admitted = reducer.extension_admission()?.expect("committed selection");
    assert_eq!(admitted.source().revision, 2);
    assert_eq!(admitted.configurations()[0].content, first);
    let runtime = crate::runtime::RuntimeScope::new(
        Capabilities::new(["extension:activate"]),
        crate::conversation::Limits::default(),
    )?
    .with_extensions_from(&reducer)?;
    assert_eq!(runtime.extensions(), Some(&admitted));
    assert!(
        runtime
            .narrow(
                Capabilities::new(std::iter::empty::<&str>()),
                crate::conversation::Limits::default()
            )?
            .with_extensions_from(&reducer)
            .is_err()
    );
    let second = request_file(br#"{"enabled":false}"#)?;
    reducer.schemas().validate_configuration_bytes(
        &extension,
        &second,
        br#"{"enabled":false}"#,
        None,
    )?;
    reducer.apply(agent_command(
        &owner,
        74,
        2,
        Action::ConfigureExtension {
            extension,
            content: second,
        },
    ))?;
    reducer.set_resident_event_limit(1)?;
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    assert_eq!(snapshot.events.len(), 1);
    assert_eq!(reducer.archived_through_revision(), 2);
    assert_eq!(reducer.active_configurations()[0].content, first);
    assert_eq!(reducer.extension_admission()?, Some(admitted));
    let wire = crate::contract::canonical_json_bytes(&snapshot)?;
    let restored = Reducer::restore(
        crate::contract::json_from_slice(&wire)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        owner.verifier(&mut None).expect("verifier copy"),
        registry,
        &mut None,
    )?;
    assert_eq!(
        restored.active_configurations(),
        reducer.active_configurations()
    );
    assert_eq!(
        restored.extension_admission()?,
        reducer.extension_admission()?
    );
    Ok(())
}

#[test]
fn extension_registry_and_records_share_a_bounded_name_policy() -> Result<()> {
    for name in [
        "plain",
        "example.",
        ".example",
        "example..state",
        "example. state",
        "example.\0state",
        "example../private",
        "example.state/path",
        "example.state\\path",
        "example.state:other",
    ] {
        assert!(validate_extension_name(name).is_err());
        assert!(
            SchemaRegistry::new()
                .register(
                    name,
                    1,
                    json!({"type":"object"}),
                    [1; 32],
                    ExtensionForkPolicy::Inherit,
                    [],
                    None
                )
                .is_err()
        );
    }
    validate_extension_name(&format!("example.{}", "x".repeat(256)))?;
    validate_extension_name("example.state")
}

#[test]
fn extension_activation_requires_exact_acyclic_dependencies() -> Result<()> {
    let alpha = dependency("example.alpha", 1);
    let beta = dependency("example.beta", 2);
    let mut registry = SchemaRegistry::new();
    registry.register(
        alpha.name.clone(),
        alpha.version,
        json!({"type":"object"}),
        [1; 32],
        ExtensionForkPolicy::Inherit,
        [beta.clone()],
        None,
    )?;
    assert!(matches!(
        registry.activation_order(&[alpha.clone()], &mut None),
        Err(Error::Unsupported(_))
    ));
    registry.register(
        beta.name.clone(),
        beta.version,
        json!({"type":"object"}),
        [2; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    assert_eq!(
        registry.activation_order(&[alpha.clone()], &mut None)?.0,
        vec![beta.clone(), alpha.clone()]
    );
    let other = dependency(&beta.name, 3);
    registry.register(
        other.name.clone(),
        other.version,
        json!({"type":"object"}),
        [3; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    assert!(matches!(
        registry.activation_order(&[alpha.clone(), other], &mut None),
        Err(Error::Conflict(_))
    ));
    let mut cyclic = SchemaRegistry::new();
    cyclic.register(
        alpha.name.clone(),
        alpha.version,
        json!({"type":"object"}),
        [1; 32],
        ExtensionForkPolicy::Inherit,
        [beta.clone()],
        None,
    )?;
    cyclic.register(
        beta.name.clone(),
        beta.version,
        json!({"type":"object"}),
        [2; 32],
        ExtensionForkPolicy::Reset,
        [alpha],
        None,
    )?;
    assert!(matches!(
        cyclic.activation_order(&[beta], &mut None),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[test]
fn agent_extension_selection_replays_and_does_not_retarget_old_events() -> Result<()> {
    let base = dependency("example.base", 1);
    let feature = dependency("example.feature", 2);
    let mut registry = SchemaRegistry::new();
    registry.register(
        base.name.clone(),
        base.version,
        json!({"type":"object"}),
        [1; 32],
        ExtensionForkPolicy::Inherit,
        [],
        None,
    )?;
    registry.register(
        feature.name.clone(),
        feature.version,
        json!({"type":"object"}),
        [2; 32],
        ExtensionForkPolicy::Reset,
        [base.clone()],
        None,
    )?;
    let (mut reducer, owner) = agent_fixture(registry.clone());
    let select = agent_command(
        &owner,
        61,
        0,
        Action::SelectExtensions {
            roots: vec![feature.clone()],
        },
    );
    assert!(matches!(
        reducer.apply(select.clone())?,
        ApplyResult::Applied { .. }
    ));
    assert_eq!(reducer.active_extensions(), &[base, feature]);
    let mut restored = Reducer::restore(
        reducer.snapshot(usize::MAX, &mut None)?.0,
        owner.verifier(&mut None).expect("verifier copy"),
        registry,
        &mut None,
    )?;
    assert_eq!(restored.active_extensions(), reducer.active_extensions());
    assert!(matches!(
        restored.apply(select.clone())?,
        ApplyResult::Replayed { .. }
    ));
    restored.apply(agent_command(
        &owner,
        62,
        1,
        Action::SelectExtensions { roots: Vec::new() },
    ))?;
    assert!(restored.active_extensions().is_empty());
    assert!(matches!(
        restored.apply(select)?,
        ApplyResult::Replayed { .. }
    ));
    assert!(restored.active_extensions().is_empty());
    restored.apply(agent_command(
        &owner,
        63,
        2,
        Action::TransitionLifecycle {
            to: LifecycleState::Active,
            reason: None,
        },
    ))?;
    assert!(matches!(
        restored.apply(agent_command(
            &owner,
            64,
            3,
            Action::SelectExtensions { roots: Vec::new() }
        )),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[test]
fn extension_state_version_change_requires_exact_migration_event() -> Result<()> {
    let mut registry = schemas();
    registry.register(
        "example.message",
        2,
        json!({"type":"object","required":["text"],
        "properties":{"text":{"type":"string"}}}),
        [10; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let owner = issuer();
    let mut reducer = Reducer::new(
        owner.audience.clone(),
        owner.verifier(&mut None).expect("verifier copy"),
        registry.clone(),
    );
    let append = |version, body| -> Result<Action> {
        Ok(Action::AppendCustom {
            schema: "example.message".into(),
            version,
            content: request_file(body)?,
        })
    };
    reducer.apply(command(operation(110), 0, append(1, br#"{"text":"old"}"#)?))?;
    assert!(matches!(
        reducer.apply(command(operation(111), 1, append(2, br#"{"text":"new"}"#)?)),
        Err(Error::Conflict(_))
    ));
    let mut migrate = command(
        operation(112),
        1,
        Action::MigrateExtensionState {
            name: "example.message".into(),
            previous: EventReference {
                authority: owner.audience.clone(),
                revision: 1,
            },
            to_version: 2,
            content: request_file(br#"{"text":"new"}"#)?,
        },
    );
    migrate.scope = owner
        .issue(
            "owner",
            &Capabilities::new([
                "extension:migrate".to_owned(),
                request_file(b"null")?
                    .volume()
                    .capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([8; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let mut stale = migrate.clone();
    if let Action::MigrateExtensionState { previous, .. } = &mut stale.action {
        previous.revision = 0;
    }
    assert!(matches!(
        reducer.plan_with_migration_boundary_with_work(&stale, true, false, None, &mut None),
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        reducer.apply(migrate.clone()),
        Err(Error::Unsupported(_))
    ));
    let (ApplyResult::Applied { event }, _) =
        reducer.plan_with_migration_boundary_with_work(&migrate, true, false, None, &mut None)?
    else {
        panic!("fresh migration replayed")
    };
    reducer.apply_committed(event, &mut None)?;
    assert!(matches!(
        reducer.apply(migrate)?,
        ApplyResult::Replayed { .. }
    ));
    assert_eq!(
        reducer
            .extension_state("example.message")
            .map(|(_, state)| state.version),
        Some(2)
    );
    let restored = Reducer::restore(
        reducer.snapshot(usize::MAX, &mut None)?.0,
        owner.verifier(&mut None).expect("verifier copy"),
        registry,
        &mut None,
    )?;
    assert_eq!(
        restored.extension_state("example.message"),
        reducer.extension_state("example.message")
    );
    assert_eq!(restored.events_after(0, 10)?.len(), 2);
    Ok(())
}

#[test]
fn extension_replay_requires_exact_implementation_and_fork_policy() -> Result<()> {
    let mut original = reducer_fixture();
    let ApplyResult::Applied { event } = original.apply(command(
        operation(70),
        0,
        Action::AppendCustom {
            schema: "example.message".into(),
            version: 1,
            content: request_file(br#"{"text":"hello"}"#)?,
        },
    ))?
    else {
        panic!("fresh append replayed")
    };
    let schema = json!({"type":"object","properties":{"text":{"type":"string"}},"additionalProperties":true});
    for (digest, policy) in [
        ([8; 32], ExtensionForkPolicy::Inherit),
        ([9; 32], ExtensionForkPolicy::Reset),
    ] {
        let mut bindings = SchemaRegistry::new();
        bindings.register(
            "example.message",
            1,
            schema.clone(),
            digest,
            policy,
            [],
            None,
        )?;
        let mut candidate = Reducer::new(
            issuer().audience.clone(),
            issuer().verifier(&mut None).expect("verifier copy"),
            bindings,
        );
        assert!(matches!(
            candidate.apply_committed(event.clone(), &mut None),
            Err(Error::Conflict(_))
        ));
    }
    let mut missing = Reducer::new(
        issuer().audience.clone(),
        issuer().verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    assert!(matches!(
        missing.apply_committed(event.clone(), &mut None),
        Err(Error::Unsupported(_))
    ));
    let mut exact = reducer_fixture();
    assert!(matches!(
        exact.apply_committed(event, &mut None)?,
        ApplyResult::Applied { .. }
    ));
    assert!(matches!(
        exact.schemas.register(
            "example.message",
            1,
            schema,
            [8; 32],
            ExtensionForkPolicy::Inherit,
            [],
            None
        ),
        Err(Error::Conflict(_))
    ));
    Ok(())
}
