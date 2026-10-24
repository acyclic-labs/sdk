use super::*;

#[test]
fn actor_ids_follow_the_live_ascii_grammar_without_normalization()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(ActorId::new(String::new()), Err(DomainError::EmptyActorId));
    for raw in ["  ", "cafe\u{301}", "caf\u{e9}", "parent/actor:1"] {
        assert!(ActorId::new(raw.into()).is_err());
    }
    for raw in ["Actor_1", "actor-1", "dot.name"] {
        let id = ActorId::new(raw.into())?;
        let view = id.identity(
            b"account",
            &crate::addressing::NamespaceId::new(
                crate::addressing::DEFAULT_ACTOR_NAMESPACE.as_bytes(),
            )?,
        )?;
        let retained = crate::addressing::ActorReference::decode(&view.encode())?;
        assert_eq!(retained.id(), raw);
        assert_eq!(
            retained.namespace().as_str(),
            crate::addressing::DEFAULT_ACTOR_NAMESPACE
        );
    }
    assert_ne!(ActorId::new("Actor".into())?, ActorId::new("actor".into())?);
    Ok(())
}

#[test]
fn typescript_export_includes_error_roots_with_esm_imports() {
    let output = std::env::temp_dir().join(format!(
        "acyclic-actors-typescript-export-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&output);

    export::export_typescript(&output).expect("Actors TypeScript export should succeed");

    let service_error =
        std::fs::read_to_string(output.join("actors/ServiceError.ts")).expect("ServiceError");
    assert!(service_error.contains("from \"./ErrorCode.js\""));
    for name in ["ActorState", "SubscriptionState", "ErrorCode"] {
        let declaration = std::fs::read_to_string(output.join(format!("actors/{name}.ts")))
            .expect("numeric enum declaration");
        assert!(declaration.contains(&format!("export type {name} = ")));
        assert!(!declaration.contains("export enum "));
    }
    let current_head = std::fs::read_to_string(output.join("actors/CurrentHeadMarker.ts"))
        .expect("CurrentHeadMarker");
    assert!(current_head.contains("true"));

    std::fs::remove_dir_all(output).expect("remove temporary export");
}

#[test]
fn typescript_export_names_follow_nested_ts_rs_metadata() {
    let names = export::typescript_export_names(&Config::default());

    assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
    for name in [
        "ActorObservation",
        "ActorState",
        "CurrentHeadMarker",
        "SubscriptionObservation",
        "SubscriptionState",
    ] {
        assert!(names.iter().any(|candidate| candidate == name), "{name}");
    }
}

#[test]
fn digest_matches_existing_create_predicate() {
    assert_eq!(
        CodeSha256::new(vec![0; 31]),
        Err(DomainError::InvalidCodeSha256)
    );
    assert_eq!(
        CodeSha256::new(vec![0; 32]),
        Err(DomainError::InvalidCodeSha256)
    );
    assert!(CodeSha256::new(vec![1; 32]).is_ok());
}

#[test]
fn positive_u64_is_the_single_limits_admission_predicate() {
    let invalid = DomainError::Contract(crate::ContractError::InvalidArgument);
    assert_eq!(PositiveU64::new(0), Err(invalid));
    assert_eq!(PositiveU64::try_from(0), Err(invalid));

    let one = PositiveU64::new(1).expect("one is positive");
    let max = PositiveU64::new(u64::MAX).expect("u64::MAX is representable");
    assert_eq!(one.get(), 1);
    assert_eq!(max.get(), u64::MAX);

    let wire = wire::ActorLimits {
        handler_timeout_millis: 1,
        memory_bytes: u64::MAX,
        checkpoint_bytes: 4096,
    };
    let limits = ActorLimits::try_from(wire.clone()).expect("positive wire limits");
    assert_eq!(limits.handler_timeout_millis(), 1);
    assert_eq!(limits.memory_bytes(), u64::MAX);
    assert_eq!(limits.checkpoint_bytes(), 4096);
    assert_eq!(wire::ActorLimits::from(limits), wire);

    let invalid_wire = wire::ActorLimits {
        handler_timeout_millis: 0,
        memory_bytes: 1,
        checkpoint_bytes: 1,
    };
    assert_eq!(ActorLimits::try_from(invalid_wire), Err(invalid));
}

#[test]
fn invoke_round_trips_without_rewriting_wire_fields() {
    let wire = wire::InvokeActorRequest {
        invocation_order: None,
        namespace: None,
        account_id: Vec::new(),
        location_hint: None,
        binding_name: None,
        rpc_method: None,
        actor_id: "actor-1".into(),
        method: String::new(),
        url: String::new(),
        body: vec![0, 1, 2],
        headers: vec![wire::Header {
            name: "x-test".into(),
            value: "ok".into(),
        }],
    };
    let typed = InvokeActorRequest::try_from(wire.clone()).ok();
    assert!(typed.is_some());
    let Some(typed) = typed else {
        return;
    };
    let round_trip: wire::InvokeActorRequest = typed.into();
    assert_eq!(round_trip, wire);
}

#[test]
fn invoke_rejects_only_the_existing_actor_id_presence_violation() {
    let wire = wire::InvokeActorRequest {
        invocation_order: None,
        namespace: None,
        account_id: Vec::new(),
        location_hint: None,
        binding_name: None,
        rpc_method: None,
        actor_id: String::new(),
        method: String::new(),
        url: String::new(),
        body: Vec::new(),
        headers: Vec::new(),
    };
    assert_eq!(
        InvokeActorRequest::try_from(wire),
        Err(DomainError::EmptyActorId)
    );
}

#[test]
fn response_enum_unknown_values_are_not_normalized() {
    let observation = wire::ActorObservation {
        observation_stream_path: None,
        namespace: None,
        account_id: Vec::new(),
        actor_id: "actor-1".into(),
        code_sha256: vec![1; 32],
        home_region: "eu".into(),
        state: 99,
        subscriptions: Vec::new(),
        checkpoint_unix_millis: None,
        checkpoint_epoch: 0,
        configuration_revision: 0,
    };
    assert_eq!(
        ActorObservation::try_from(observation),
        Err(DomainError::UnknownActorState(99))
    );
    assert_eq!(
        SubscriptionState::try_from(99),
        Err(DomainError::UnknownSubscriptionState(99))
    );
    assert_eq!(
        ErrorCode::try_from(99),
        Err(DomainError::UnknownErrorCode(99))
    );
}

#[test]
fn response_digest_keeps_the_nonzero_admission_rule() {
    let observation = wire::ActorObservation {
        observation_stream_path: None,
        namespace: None,
        account_id: Vec::new(),
        actor_id: "actor-1".into(),
        code_sha256: vec![0; 32],
        home_region: "eu".into(),
        state: 1,
        subscriptions: Vec::new(),
        checkpoint_unix_millis: None,
        checkpoint_epoch: 0,
        configuration_revision: 0,
    };
    assert_eq!(
        ActorObservation::try_from(observation),
        Err(DomainError::InvalidCodeSha256)
    );
}

#[test]
fn subscription_projection_keeps_canonical_current_head_rule() {
    let invalid_start = wire::SubscriptionStart {
        start: Some(wire::subscription_start::Start::CurrentHead(false)),
    };
    assert_eq!(
        SubscriptionStart::try_from(invalid_start),
        Err(DomainError::InvalidSubscription)
    );

    let invalid = wire::SubscriptionSpec {
        subscription_id: "events".into(),
        stream_path: "agents/a/events".into(),
        start: Some(wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::CurrentHead(false)),
        }),
        placement_anchor: false,
    };
    assert_eq!(
        SubscriptionSpec::try_from(invalid),
        Err(DomainError::InvalidSubscription)
    );
}

#[test]
fn wire_ingress_rechecks_container_constructor_predicates() {
    let invalid_binding = wire::Binding {
        target_account: Vec::new(),
        actor_namespace: None,
        worker_service: None,
        name: String::new(),
        capability: "read".into(),
        resource: "bucket/a".into(),
    };
    assert_eq!(
        Binding::try_from(invalid_binding),
        Err(DomainError::InvalidBinding)
    );

    let invalid_subscription = wire::SubscriptionSpec {
        subscription_id: String::new(),
        stream_path: "events/input".into(),
        start: Some(wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::Cursor(0)),
        }),
        placement_anchor: false,
    };
    assert_eq!(
        SubscriptionSpec::try_from(invalid_subscription),
        Err(DomainError::InvalidSubscription)
    );
}

#[test]
fn wire_ingress_rechecks_request_admission_predicates() {
    let subscription = wire::SubscriptionSpec {
        subscription_id: "events".into(),
        stream_path: "agents/a/events".into(),
        start: Some(wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::Cursor(0)),
        }),
        placement_anchor: false,
    };
    let binding = wire::Binding {
        target_account: Vec::new(),
        actor_namespace: None,
        worker_service: None,
        name: "storage".into(),
        capability: "read".into(),
        resource: "bucket/a".into(),
    };
    let create = wire::DeploymentRequest {
        worker_limits: None,
        name: "example".into(),
        account_id: Vec::new(),
        jurisdiction: String::new(),
        expected_revision: 0,
        code_sha256: vec![1; 32],

        bindings: vec![binding.clone()],
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 2,
            checkpoint_bytes: 3,
        }),
        subscriptions: vec![subscription.clone()],
        idempotency_key: "create-1".into(),
    };

    let mut duplicate = create.clone();
    duplicate.subscriptions.push(subscription.clone());
    assert_eq!(
        DeploymentRequest::try_from(duplicate),
        Err(DomainError::Contract(crate::ContractError::DuplicateName))
    );

    let mut multiple_anchors = create.clone();
    multiple_anchors.subscriptions[0].placement_anchor = true;
    let mut second_anchor = subscription;
    second_anchor.subscription_id = "other-events".into();
    second_anchor.placement_anchor = true;
    multiple_anchors.subscriptions.push(second_anchor);
    assert_eq!(
        DeploymentRequest::try_from(multiple_anchors),
        Err(DomainError::Contract(crate::ContractError::InvalidArgument))
    );

    let mut invalid_idempotency = create.clone();
    invalid_idempotency.idempotency_key.clear();
    assert_eq!(
        DeploymentRequest::try_from(invalid_idempotency),
        Err(DomainError::Contract(crate::ContractError::InvalidArgument))
    );

    let mut over_limit = create;
    over_limit
        .bindings
        .extend((0..=crate::MAX_BINDINGS).map(|index| wire::Binding {
            target_account: Vec::new(),
            actor_namespace: None,
            worker_service: None,
            name: format!("binding-{index}"),
            capability: "read".into(),
            resource: "bucket/a".into(),
        }));
    assert_eq!(
        DeploymentRequest::try_from(over_limit),
        Err(DomainError::Contract(crate::ContractError::LimitExceeded))
    );

    let update = wire::UpdateNamespaceRequest {
        account_id: Vec::new(),
        namespace_id: "actor-1".into(),
        code_sha256: vec![1; 32],
        bindings: vec![binding.clone()],
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 2,
            checkpoint_bytes: 3,
        }),
        expected_configuration_revision: 1,
        idempotency_key: "update-1".into(),
    };
    let mut duplicate_bindings = update.clone();
    duplicate_bindings.bindings.push(binding);
    assert_eq!(
        UpdateNamespaceRequest::try_from(duplicate_bindings),
        Err(DomainError::Contract(crate::ContractError::DuplicateName))
    );

    let mut invalid_update_idempotency = update;
    invalid_update_idempotency.idempotency_key.clear();
    assert_eq!(
        UpdateNamespaceRequest::try_from(invalid_update_idempotency),
        Err(DomainError::Contract(crate::ContractError::InvalidArgument))
    );

    let add = wire::AddSubscriptionRequest {
        namespace: None,
        account_id: Vec::new(),
        actor_id: "actor-1".into(),
        subscription: Some(wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::CurrentHead(true)),
            }),
            placement_anchor: false,
        }),
        idempotency_key: "add-1".into(),
    };
    let mut invalid_add_idempotency = add;
    invalid_add_idempotency.idempotency_key.clear();
    assert_eq!(
        AddSubscriptionRequest::try_from(invalid_add_idempotency),
        Err(DomainError::Contract(crate::ContractError::InvalidArgument))
    );
}

#[test]
fn public_create_constructor_round_trips_and_uses_canonical_validation() {
    let digest = CodeSha256::new(vec![1; 32]).expect("valid digest");
    let binding =
        Binding::new("storage".into(), "read".into(), "bucket/a".into()).expect("valid binding");
    let limits = ActorLimits::new(1, u64::MAX, 4096).expect("positive limits");
    let subscription = SubscriptionSpec::new(
        "events".into(),
        "agents/a/events".into(),
        SubscriptionStart::cursor(u64::MAX),
        false,
    )
    .expect("valid subscription");
    let request = DeploymentRequest::new(
        ActorNamespace::new("ClassOne".into()).expect("namespace"),
        digest.clone(),
        vec![binding.clone()],
        limits,
        vec![subscription.clone()],
        "create-1".into(),
    )
    .expect("valid create request");
    let wire: wire::DeploymentRequest = request.clone().into();
    let decoded = DeploymentRequest::try_from(wire).expect("canonical decode");
    assert_eq!(decoded, request);
    assert_eq!(decoded.code_sha256(), &digest);
    assert_eq!(decoded.bindings(), &[binding]);
    assert_eq!(decoded.subscriptions(), std::slice::from_ref(&subscription));

    let duplicate = DeploymentRequest::new(
        ActorNamespace::new("ClassOne".into()).expect("namespace"),
        digest,
        Vec::new(),
        ActorLimits::new(1, 1, 1).expect("positive limits"),
        vec![subscription.clone(), subscription],
        "create-2".into(),
    );
    assert_eq!(
        duplicate,
        Err(DomainError::Contract(crate::ContractError::DuplicateName))
    );
}

#[test]
fn public_update_and_add_constructors_preserve_wire_inverse() {
    let actor_id = ActorId::new("actor-1".into()).expect("valid actor id");
    let digest = CodeSha256::new(vec![2; 32]).expect("valid digest");
    let limits = ActorLimits::new(1, 2, 3).expect("positive limits");
    let update = UpdateNamespaceRequest::new(
        ActorNamespace::new("ClassOne".into()).expect("namespace"),
        digest,
        Vec::new(),
        limits,
        u64::MAX,
        "update-1".into(),
    )
    .expect("valid update request");
    let update_wire: wire::UpdateNamespaceRequest = update.clone().into();
    assert_eq!(UpdateNamespaceRequest::try_from(update_wire), Ok(update));

    let subscription = SubscriptionSpec::new(
        "events".into(),
        "agents/a/events".into(),
        SubscriptionStart::current_head(),
        false,
    )
    .expect("valid subscription");
    let add = AddSubscriptionRequest::new(actor_id, subscription, "add-1".into())
        .expect("valid add request");
    let add_wire: wire::AddSubscriptionRequest = add.clone().into();
    assert_eq!(AddSubscriptionRequest::try_from(add_wire), Ok(add));

    assert_eq!(
        SubscriptionStart::try_from(wire::SubscriptionStart {
            start: Some(wire::subscription_start::Start::CurrentHead(false)),
        }),
        Err(DomainError::InvalidSubscription)
    );
}

#[test]
fn typed_bindings_reject_opaque_or_inconsistent_targets_at_every_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let namespace =
        Binding::actor_namespace("OBJECTS".into(), "grant".into(), b"account", "ClassOne")?;
    let service = Binding::worker_service("SERVICE".into(), "grant".into(), b"other", "worker")?;
    for binding in [namespace, service] {
        let wire = wire::Binding::from(binding.clone());
        assert_eq!(Binding::try_from(wire.clone())?, binding);
        for mutation in 0..5 {
            let mut invalid = wire.clone();
            match mutation {
                0 => invalid.target_account.clear(),
                1 => invalid.resource.push_str("-other"),
                2 => {
                    invalid.actor_namespace = Some("ClassOne".into());
                    invalid.worker_service = Some("worker".into());
                }
                3 => invalid.capability.clear(),
                _ => invalid.name.clear(),
            }
            // Negative control: the raw protobuf admits the inconsistent shape.
            assert!(Binding::try_from(invalid).is_err());
        }
    }
    assert!(Binding::new("OBJECTS".into(), "grant".into(), "actors:/ClassOne".into()).is_err());
    assert!(Binding::new("SERVICE".into(), "grant".into(), "workers:/worker".into()).is_err());
    assert!(
        Binding::worker_service("SERVICE".into(), "grant".into(), b"account", "Upper").is_err()
    );
    Ok(())
}

#[test]
fn wire_scope_validates_namespaces_and_preserves_opaque_accounts()
-> Result<(), Box<dyn std::error::Error>> {
    let legacy = wire::InspectActorRequest {
        actor_id: "same".into(),
        namespace: None,
        account_id: vec![],
    };
    assert_eq!(
        InspectActorRequest::try_from(legacy.clone())?.namespace(),
        crate::addressing::DEFAULT_ACTOR_NAMESPACE
    );
    let mut invalid = legacy.clone();
    invalid.namespace = Some(String::new());
    assert!(InspectActorRequest::try_from(invalid).is_err());
    let mut opaque_account = legacy;
    opaque_account.account_id = vec![1; 65536];
    let reference = InspectActorRequest::try_from(opaque_account)?.reference(b"fallback")?;
    assert_eq!(reference.account().as_bytes(), vec![1; 65536]);
    for (namespace, id) in [
        (Some("".into()), "same"),
        (None, "not valid"),
        (Some("ClassOne".into()), "a/b"),
    ] {
        let raw = wire::DeleteAllRequest {
            actor_id: id.into(),
            namespace,
            account_id: vec![],
            idempotency_key: "clear".into(),
        };
        assert!(crate::validate_delete(&raw).is_err());
        assert!(DeleteAllRequest::try_from(raw).is_err());
    }
    Ok(())
}

#[test]
fn deployment_limit_alternatives_are_exclusive_and_operation_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let worker = DeploymentRequest::worker(
        ActorNamespace::new("worker".into())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        WorkerLimits::new(2, 3, 4)?,
        "deploy".into(),
    )?;
    let raw: wire::DeploymentRequest = worker.into();
    assert!(crate::client::validate_operation("DeployWorker", &raw).is_ok());
    assert!(crate::client::validate_operation("DeployNamespace", &raw).is_err());
    let mut neither = raw.clone();
    neither.worker_limits = None;
    assert!(DeploymentRequest::try_from(neither).is_err());
    let mut both = raw.clone();
    both.limits = Some(ActorLimits::new(1, 2, 3)?.into());
    assert!(DeploymentRequest::try_from(both).is_err());
    let mut namespace = raw;
    namespace.worker_limits = None;
    namespace.limits = Some(ActorLimits::new(1, 2, 3)?.into());
    assert!(crate::client::validate_operation("DeployNamespace", &namespace).is_ok());
    assert!(crate::client::validate_operation("DeployWorker", &namespace).is_err());
    Ok(())
}
#[test]
fn invocation_order_is_transport_metadata_with_strict_session_validation() -> Result<(), DomainError>
{
    for session in [
        String::new(),
        "a".repeat(31),
        "A".repeat(32),
        "g".repeat(32),
        "a".repeat(33),
    ] {
        assert!(InvocationOrder::new(session.clone(), 0).is_err());
        assert!(
            InvocationOrder::try_from(wire::InvocationOrder {
                session,
                sequence: 0
            })
            .is_err()
        );
    }
    for sequence in [0, 1, u64::MAX] {
        let order = InvocationOrder::new("01".repeat(16), sequence)?;
        assert_eq!(
            InvocationOrder::try_from(wire::InvocationOrder::from(order.clone()))?,
            order
        );
        assert_eq!(order.session(), "01".repeat(16));
        assert_eq!(order.sequence(), sequence);
    }
    Ok(())
}
