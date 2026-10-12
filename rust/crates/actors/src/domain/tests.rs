use super::*;

#[test]
fn actor_id_matches_existing_presence_rule() {
    assert_eq!(ActorId::new(String::new()), Err(DomainError::EmptyActorId));
    assert_eq!(
        ActorId::new("  ".into()).map(|value| value.as_str().to_owned()),
        Ok(String::from("  "))
    );
}

#[test]
fn typescript_export_includes_error_roots_with_esm_imports() {
    let output = std::env::temp_dir().join(format!(
        "acyclic-actors-typescript-export-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&output);

    export_typescript(&output).expect("Actors TypeScript export should succeed");

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
    let names = typescript_export_names(&Config::default());

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
        name: "storage".into(),
        capability: "read".into(),
        resource: "bucket/a".into(),
    };
    let create = wire::CreateActorRequest {
        code_sha256: vec![1; 32],
        home_region: "eu".into(),
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
        CreateActorRequest::try_from(duplicate),
        Err(DomainError::Contract(crate::ContractError::DuplicateName))
    );

    let mut multiple_anchors = create.clone();
    multiple_anchors.subscriptions[0].placement_anchor = true;
    let mut second_anchor = subscription;
    second_anchor.subscription_id = "other-events".into();
    second_anchor.placement_anchor = true;
    multiple_anchors.subscriptions.push(second_anchor);
    assert_eq!(
        CreateActorRequest::try_from(multiple_anchors),
        Err(DomainError::Contract(crate::ContractError::InvalidArgument))
    );

    let mut invalid_idempotency = create.clone();
    invalid_idempotency.idempotency_key.clear();
    assert_eq!(
        CreateActorRequest::try_from(invalid_idempotency),
        Err(DomainError::Contract(crate::ContractError::InvalidArgument))
    );

    let mut over_limit = create;
    over_limit
        .bindings
        .extend((0..=crate::MAX_BINDINGS).map(|index| wire::Binding {
            name: format!("binding-{index}"),
            capability: "read".into(),
            resource: "bucket/a".into(),
        }));
    assert_eq!(
        CreateActorRequest::try_from(over_limit),
        Err(DomainError::Contract(crate::ContractError::LimitExceeded))
    );

    let update = wire::UpdateActorRequest {
        actor_id: "actor-1".into(),
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
        UpdateActorRequest::try_from(duplicate_bindings),
        Err(DomainError::Contract(crate::ContractError::DuplicateName))
    );

    let mut invalid_update_idempotency = update;
    invalid_update_idempotency.idempotency_key.clear();
    assert_eq!(
        UpdateActorRequest::try_from(invalid_update_idempotency),
        Err(DomainError::Contract(crate::ContractError::InvalidArgument))
    );

    let add = wire::AddSubscriptionRequest {
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
    let request = CreateActorRequest::new(
        digest.clone(),
        "eu".into(),
        vec![binding.clone()],
        limits,
        vec![subscription.clone()],
        "create-1".into(),
    )
    .expect("valid create request");
    let wire: wire::CreateActorRequest = request.clone().into();
    let decoded = CreateActorRequest::try_from(wire).expect("canonical decode");
    assert_eq!(decoded, request);
    assert_eq!(decoded.code_sha256(), &digest);
    assert_eq!(decoded.bindings(), &[binding]);
    assert_eq!(decoded.subscriptions(), std::slice::from_ref(&subscription));

    let duplicate = CreateActorRequest::new(
        digest,
        "eu".into(),
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
    let update = UpdateActorRequest::new(
        actor_id.clone(),
        digest,
        Vec::new(),
        limits,
        u64::MAX,
        "update-1".into(),
    )
    .expect("valid update request");
    let update_wire: wire::UpdateActorRequest = update.clone().into();
    assert_eq!(UpdateActorRequest::try_from(update_wire), Ok(update));

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
