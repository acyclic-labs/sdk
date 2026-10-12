use super::*;

fn dependency(name: &str, version: u32) -> ExtensionDependency {
    ExtensionDependency {
        name: name.into(),
        version,
    }
}

fn install(
    registry: &mut SchemaRegistry,
    extension: &ExtensionDependency,
    dependencies: Vec<ExtensionDependency>,
) -> Result<()> {
    registry.register(
        extension.name.clone(),
        extension.version,
        json!({"type":"object"}),
        [1; 32],
        ExtensionForkPolicy::Inherit,
        dependencies,
        None,
    )
}

#[test]
fn extension_activation_requires_validated_configuration_and_pins_revision() -> Result<()> {
    let extension = dependency("example.configured", 1);
    let mut registry = SchemaRegistry::new();
    let configuration = json!({"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"}},"additionalProperties":false});
    registry.register(
        extension.name.clone(),
        1,
        json!({"type":"object"}),
        [1; 32],
        ExtensionForkPolicy::Inherit,
        [],
        Some(configuration),
    )?;
    let mut owner = issuer();
    owner.audience.kind = AggregateKind::Agent;
    let verifier = owner.verifier(&mut None)?;
    let mut reducer = Reducer::new(owner.audience.clone(), verifier, registry);
    let scope = owner.issue(
        "owner",
        &Capabilities::new([
            "extension:configure",
            "extension:activate",
            "lifecycle:manage",
        ]),
        None,
        None,
        &mut None,
    )?;
    let apply = |state: &mut Reducer, id, action| {
        let mut command = command(operation(id), state.revision(), action);
        command.scope = scope.clone();
        state.apply(command)
    };
    let roots = vec![extension.clone()];
    let mut select = command(operation(73), 0, Action::SelectExtensions { roots });
    select.scope = scope.clone();
    assert!(reducer.apply(select.clone()).is_err());
    for (bytes, valid) in [
        (br#"{"enabled":true}"#.as_slice(), true),
        (br#"{"enabled":false,"extra":1}"#.as_slice(), false),
    ] {
        let file = request_file(bytes)?;
        assert_eq!(
            reducer
                .schemas()
                .validate_configuration_bytes(&extension, &file, bytes, None)
                .is_ok(),
            valid
        );
    }
    let first = request_file(br#"{"enabled":true}"#)?;
    let configure = Action::ConfigureExtension {
        extension: extension.clone(),
        content: first.clone(),
    };
    apply(&mut reducer, 72, configure)?;
    select.expected_revision = 1;
    reducer.apply(select.clone())?;
    let admitted = reducer.extension_admission()?.unwrap();
    assert_eq!(admitted.configurations()[0].content, first);
    let configure = Action::ConfigureExtension {
        extension,
        content: request_file(br#"{"enabled":false}"#)?,
    };
    apply(&mut reducer, 74, configure)?;
    let mut restored = restore(&reducer, &owner)?;
    assert_eq!(restored.extension_admission()?, Some(admitted.clone()));
    assert!(matches!(
        restored.apply(select.clone())?,
        ApplyResult::Replayed { .. }
    ));
    let clear = Action::SelectExtensions { roots: Vec::new() };
    apply(&mut restored, 75, clear.clone())?;
    restored.apply(select)?;
    assert!(restored.active_extensions().is_empty());
    let active = Action::TransitionLifecycle {
        to: LifecycleState::Active,
        reason: None,
    };
    apply(&mut restored, 76, active)?;
    assert!(apply(&mut restored, 77, clear).is_err());
    reducer.set_resident_event_limit(1)?;
    assert_eq!(
        restore(&reducer, &owner)?.extension_admission()?,
        Some(admitted)
    );
    Ok(())
}

#[test]
fn extension_activation_requires_exact_acyclic_dependencies() -> Result<()> {
    for name in [
        "plain",
        "example..state",
        "example.state/path",
        "example.\0state",
    ] {
        assert!(install(&mut SchemaRegistry::new(), &dependency(name, 1), vec![]).is_err());
    }
    validate_extension_name(&format!("example.{}", "x".repeat(256)))?;
    let alpha = dependency("example.alpha", 1);
    let beta = dependency("example.beta", 2);
    let mut registry = SchemaRegistry::new();
    install(&mut registry, &alpha, vec![beta.clone()])?;
    assert!(
        registry
            .activation_order(&[alpha.clone()], &mut None)
            .is_err()
    );
    install(&mut registry, &beta, vec![])?;
    assert_eq!(
        registry.activation_order(&[alpha.clone()], &mut None)?.0,
        vec![beta.clone(), alpha.clone()]
    );
    let other = dependency(&beta.name, 3);
    install(&mut registry, &other, vec![])?;
    assert!(
        registry
            .activation_order(&[alpha.clone(), other], &mut None)
            .is_err()
    );
    let mut cyclic = SchemaRegistry::new();
    install(&mut cyclic, &alpha, vec![beta.clone()])?;
    install(&mut cyclic, &beta, vec![alpha])?;
    assert!(cyclic.activation_order(&[beta], &mut None).is_err());
    Ok(())
}

#[test]
fn extension_state_version_change_requires_exact_migration_event() -> Result<()> {
    let mut reducer = reducer_fixture();
    reducer.schemas.register(
        "example.message",
        2,
        json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
        [10; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let append = |version, body| -> Result<Action> {
        Ok(Action::AppendCustom {
            schema: "example.message".into(),
            version,
            content: request_file(body)?,
        })
    };
    apply(&mut reducer, 110, append(1, br#"{"text":"old"}"#)?)?;
    assert!(apply(&mut reducer, 111, append(2, br#"{"text":"new"}"#)?).is_err());
    let migration = Action::MigrateExtensionState {
        name: "example.message".into(),
        previous: EventReference {
            authority: issuer().audience,
            revision: 1,
        },
        to_version: 2,
        content: request_file(br#"{"text":"new"}"#)?,
    };
    let mut migrate = command(operation(112), 1, migration);
    migrate.scope = issuer().issue(
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
    )?;
    let mut stale = migrate.clone();
    if let Action::MigrateExtensionState { previous, .. } = &mut stale.action {
        previous.revision = 0;
    }
    assert!(
        reducer
            .plan_with_migration_boundary_with_work(&stale, true, false, None, &mut None)
            .is_err()
    );
    assert!(reducer.apply(migrate.clone()).is_err());
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
    let state = reducer.extension_state("example.message").unwrap();
    assert_eq!(state.1.version, 2);
    assert_eq!(
        restore(&reducer, &issuer())?.extension_state("example.message"),
        reducer.extension_state("example.message")
    );
    Ok(())
}
