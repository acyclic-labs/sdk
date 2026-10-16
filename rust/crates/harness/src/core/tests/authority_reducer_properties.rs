use super::*;
use proptest::prelude::*;

const KINDS: [AggregateKind; 5] = [
    AggregateKind::Agent,
    AggregateKind::Conversation,
    AggregateKind::Session,
    AggregateKind::Turn,
    AggregateKind::Task,
];

fn audience_binding(id: &str, key: [u8; 32], agent: AgentId, use_original: bool) {
    for kind in KINDS {
        let owner = AuthorityIssuer::new(
            "issuer",
            key,
            Authority {
                kind,
                id: id.into(),
            },
        );
        let scope = owner
            .issue(
                "grant",
                &Capabilities::new(["lifecycle:manage"]),
                Some(agent),
                None,
                &mut None,
            )
            .unwrap();
        let mut cmd = command(
            operation(1),
            0,
            Action::TransitionLifecycle {
                to: LifecycleState::Active,
                reason: None,
            },
        );
        cmd.scope = scope.clone();
        for target in KINDS {
            let foreign = AuthorityIssuer::new(
                "issuer",
                key,
                Authority {
                    kind: target,
                    id: id.into(),
                },
            );
            let verifier = if use_original {
                owner.verifier(&mut None)
            } else {
                foreign.verifier(&mut None)
            }
            .unwrap();
            assert_eq!(
                verifier.verify(&scope, &mut None).is_ok(),
                kind == target,
                "scope crossed an aggregate-kind boundary"
            );
            let mut reducer = Reducer::new(foreign.audience.clone(), verifier, schemas());
            let before = reducer.copy(&mut None).unwrap();
            assert_eq!(reducer.plan(&cmd).is_ok(), kind == target);
            assert_eq!(reducer, before, "planning changed authoritative state");
            assert_eq!(reducer.apply(&cmd).is_ok(), kind == target);
            if kind == target {
                assert_eq!(reducer.revision(), 1);
                assert_eq!(reducer.lifecycle(), LifecycleState::Active);
                assert_eq!(restore(&reducer, &foreign).unwrap(), reducer);
            } else {
                assert_eq!(reducer, before, "foreign admission changed state");
            }
        }
    }
}

fn guarantee(index: u8) -> EffectGuarantee {
    match index % 3 {
        0 => EffectGuarantee::IdempotentRetry,
        1 => EffectGuarantee::AtMostOnce,
        _ => EffectGuarantee::ExactlyOnce,
    }
}

fn commit(reducer: &mut Reducer, replay: &mut Reducer, next: &mut u8, action: Action) -> Command {
    let cmd = command(operation(*next), reducer.revision(), action);
    *next += 1;
    let before = reducer.copy(&mut None).unwrap();
    let planned = reducer.plan(&cmd).unwrap();
    assert_eq!(*reducer, before, "effect planning changed state");
    let applied = reducer.apply(&cmd).unwrap();
    assert_eq!(applied, planned);
    let ApplyResult::Applied { event } = applied else {
        panic!("fresh effect operation replayed")
    };
    replay.apply_committed(event, &mut None).unwrap();
    assert_eq!(reducer, replay, "committed effect replay diverged");
    assert_eq!(restore(reducer, &issuer()).unwrap(), *reducer);
    let settled = reducer.copy(&mut None).unwrap();
    assert!(matches!(
        reducer.apply(&cmd).unwrap(),
        ApplyResult::Replayed { .. }
    ));
    assert_eq!(*reducer, settled, "exact effect retry changed state");
    cmd
}

fn plan_effect(
    reducer: &mut Reducer,
    replay: &mut Reducer,
    next: &mut u8,
    id: EffectId,
    guarantee: EffectGuarantee,
    payload: &[u8],
) {
    commit(
        reducer,
        replay,
        next,
        Action::PlanEffect {
            effect_id: id,
            provider: "example.provider".into(),
            guarantee,
            effect_kind: "example.send".into(),
            request: request_file(payload).unwrap(),
            result_schema: schema_file(&json!({"type":"object"})).unwrap(),
        },
    );
}

fn rejected(reducer: &mut Reducer, id: u8, action: Action) {
    let before = reducer.copy(&mut None).unwrap();
    let cmd = command(operation(id), reducer.revision(), action);
    assert!(reducer.plan(&cmd).is_err(), "invalid effect was planned");
    assert_eq!(*reducer, before);
    assert!(reducer.apply(&cmd).is_err(), "invalid effect was applied");
    assert_eq!(*reducer, before, "effect rejection changed state");
    let mut event = reducer.events.back().unwrap().clone();
    event.operation_id = cmd.operation_id;
    event.revision = reducer.revision() + 1;
    event.intent_digest = crate::executor::json::canonical_json_digest(
        &cmd,
        crate::store::default_projection_read_limits().maximum_bytes,
        &mut None,
    )
    .unwrap();
    event.payload = match &cmd.action {
        Action::ResolveEffect { observation } => EventPayload::EffectResolved {
            observation: observation.clone(),
        },
        Action::MarkEffectDispatched {
            effect_id,
            attempt_id,
        } => EventPayload::EffectDispatched {
            effect_id: *effect_id,
            attempt_id: *attempt_id,
        },
        _ => unreachable!("only effect transitions are tested"),
    };
    let verifier = issuer().verifier(&mut None).unwrap();
    event.attestation = verifier.attest_event(&event, &mut None).unwrap();
    verifier.verify_event(&event, &mut None).unwrap();
    assert!(
        reducer.apply_committed(event, &mut None).is_err(),
        "invalid authenticated effect event was applied"
    );
    assert_eq!(*reducer, before, "committed effect rejection changed state");
}

fn attestation_fields(payload: &[u8], key_byte: usize, bit: u8, use_original: bool) {
    let owner = issuer();
    let verifier = owner.verifier(&mut None).unwrap();
    let id = EffectId::from_bytes([9; 16]);
    let attempt = EffectAttemptId::from_bytes([10; 16]);
    let mut reducer = reducer_fixture();
    let mut replay = reducer_fixture();
    let mut next = 1;
    plan_effect(
        &mut reducer,
        &mut replay,
        &mut next,
        id,
        EffectGuarantee::IdempotentRetry,
        payload,
    );
    commit(
        &mut reducer,
        &mut replay,
        &mut next,
        Action::MarkEffectDispatched {
            effect_id: id,
            attempt_id: attempt,
        },
    );
    let original = owner
        .attest_effect(
            id,
            reducer.effect(id).unwrap(),
            attempt,
            EffectStatus::Indeterminate,
            &mut None,
        )
        .unwrap();
    verifier.verify_effect(&original, &mut None).unwrap();
    for field in 0..8 {
        let mut changed = original.clone();
        match field {
            0 => changed.provider.push('x'),
            1 => changed.effect_id = EffectId::from_bytes([11; 16]),
            2 => changed.attempt_id = EffectAttemptId::from_bytes([12; 16]),
            3 => changed.request_digest[key_byte] ^= 1 << bit,
            4 => changed.guarantee = EffectGuarantee::AtMostOnce,
            5 => changed.result_schema_digest[key_byte] ^= 1 << bit,
            6 => {
                changed.status = EffectStatus::Failed {
                    message: "changed".into(),
                }
            }
            _ => changed.proof[key_byte] ^= 1 << bit,
        }
        let candidate = if use_original {
            original.clone()
        } else {
            changed
        };
        assert!(
            verifier.verify_effect(&candidate, &mut None).is_err(),
            "effect field was not authenticated"
        );
        rejected(
            &mut reducer,
            200,
            Action::ResolveEffect {
                observation: candidate,
            },
        );
    }
    commit(
        &mut reducer,
        &mut replay,
        &mut next,
        Action::ResolveEffect {
            observation: original,
        },
    );
}

fn effect_history(index: u8, retries: usize, payload: &[u8], success: bool, widen_retry: bool) {
    let guarantee = guarantee(index);
    let id = EffectId::from_bytes([9; 16]);
    let mut reducer = reducer_fixture();
    let mut replay = reducer_fixture();
    let mut next = 1;
    plan_effect(&mut reducer, &mut replay, &mut next, id, guarantee, payload);
    let mut attempt = EffectAttemptId::from_bytes([10; 16]);
    commit(
        &mut reducer,
        &mut replay,
        &mut next,
        Action::MarkEffectDispatched {
            effect_id: id,
            attempt_id: attempt,
        },
    );
    for round in 0..retries {
        let observation = issuer()
            .attest_effect(
                id,
                reducer.effect(id).unwrap(),
                attempt,
                EffectStatus::Indeterminate,
                &mut None,
            )
            .unwrap();
        commit(
            &mut reducer,
            &mut replay,
            &mut next,
            Action::ResolveEffect { observation },
        );
        let before = reducer.copy(&mut None).unwrap();
        if widen_retry {
            reducer.projection.effects.get_mut(&id).unwrap().guarantee =
                EffectGuarantee::IdempotentRetry;
        }
        let previous_attempt = attempt;
        let next_attempt = EffectAttemptId::from_bytes([20 + round as u8; 16]);
        let cmd = command(
            operation(next),
            reducer.revision(),
            Action::MarkEffectDispatched {
                effect_id: id,
                attempt_id: next_attempt,
            },
        );
        let retry = reducer.plan(&cmd);
        assert_eq!(
            retry.is_ok(),
            guarantee == EffectGuarantee::IdempotentRetry,
            "dispatch crossed a delivery-guarantee boundary"
        );
        if guarantee != EffectGuarantee::IdempotentRetry {
            assert_eq!(reducer, before);
            rejected(&mut reducer, next, cmd.action);
            break;
        }
        commit(&mut reducer, &mut replay, &mut next, cmd.action);
        attempt = next_attempt;
        let stale = issuer()
            .attest_effect(
                id,
                reducer.effect(id).unwrap(),
                previous_attempt,
                EffectStatus::Failed {
                    message: "stale attempt".into(),
                },
                &mut None,
            )
            .unwrap();
        issuer()
            .verifier(&mut None)
            .unwrap()
            .verify_effect(&stale, &mut None)
            .unwrap();
        rejected(
            &mut reducer,
            200,
            Action::ResolveEffect { observation: stale },
        );
        rejected(
            &mut reducer,
            200,
            Action::MarkEffectDispatched {
                effect_id: id,
                attempt_id: attempt,
            },
        );
    }
    let status = if success {
        EffectStatus::Succeeded {
            result: request_file(br#"{"receipt":true}"#).unwrap(),
        }
    } else {
        EffectStatus::Failed {
            message: "definite failure".into(),
        }
    };
    let observation = issuer()
        .attest_effect(
            id,
            reducer.effect(id).unwrap(),
            attempt,
            status.clone(),
            &mut None,
        )
        .unwrap();
    commit(
        &mut reducer,
        &mut replay,
        &mut next,
        Action::ResolveEffect {
            observation: observation.clone(),
        },
    );
    assert_eq!(reducer.effect(id).unwrap().status, status);
    rejected(&mut reducer, 200, Action::ResolveEffect { observation });
    rejected(
        &mut reducer,
        200,
        Action::MarkEffectDispatched {
            effect_id: id,
            attempt_id: EffectAttemptId::from_bytes([99; 16]),
        },
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn aggregate_kinds_are_distinct_authority_audiences(
        id in "[a-zA-Z0-9_-]{1,32}", key in any::<[u8; 32]>(), agent in any::<[u8; 16]>(),
    ) { audience_binding(&id, key, AgentId::from_bytes(agent), false); }
    #[test]
    fn every_effect_attestation_field_is_authenticated(
        payload in prop::collection::vec(any::<u8>(), 0..128), byte in 0..32_usize, bit in 0..8_u8,
    ) { attestation_fields(&payload, byte, bit, false); }
    #[test]
    fn effect_retry_replay_and_restore_preserve_guarantees(
        retries in 0..7_usize, payload in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        for index in 0..3 {
            for success in [false, true] {
                effect_history(index, retries, &payload, success, false);
            }
        }
    }
}

#[test]
#[should_panic(expected = "scope crossed an aggregate-kind boundary")]
fn negative_control_detects_using_the_grants_original_audience() {
    audience_binding("same-id", [7; 32], AgentId::from_bytes([1; 16]), true);
}

#[test]
#[should_panic(expected = "effect field was not authenticated")]
fn negative_control_detects_authenticating_the_original_observation() {
    attestation_fields(b"{}", 0, 0, true);
}

#[test]
#[should_panic(expected = "dispatch crossed a delivery-guarantee boundary")]
fn negative_control_detects_widening_a_nonretryable_guarantee() {
    effect_history(1, 1, b"{}", false, true);
}
