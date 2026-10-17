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

#[derive(Default)]
struct ExpectedHistory {
    revision: u64,
    attempts: Vec<EffectAttemptId>,
    status: Option<EffectStatus>,
    guarantee: Option<EffectGuarantee>,
}

// Update from the submitted input before calling any production reducer API.
fn expect_transition(history: &mut ExpectedHistory, action: &Action) {
    let status = match action {
        Action::PlanEffect { guarantee, .. } => {
            history.guarantee = Some(*guarantee);
            EffectStatus::Planned
        }
        Action::MarkEffectDispatched { attempt_id, .. } => {
            history.attempts.push(*attempt_id);
            EffectStatus::Dispatched
        }
        Action::ResolveEffect { observation } => observation.status.clone(),
        _ => unreachable!("only effect transitions are recorded"),
    };
    history.revision += 1;
    history.status = Some(status);
}

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    WidenRetry,
    DuplicateAttempt,
    AcceptStale,
    ReplayAgain,
}

fn assert_history(reducer: &Reducer, history: &ExpectedHistory) {
    assert_eq!(
        reducer.revision(),
        history.revision,
        "revision differs from independent history"
    );
    let effect = reducer.effect(EffectId::from_bytes([9; 16]));
    let Some(status) = &history.status else {
        assert!(effect.is_none());
        return;
    };
    let effect = effect.unwrap();
    assert_eq!(
        effect.attempts.len(),
        history.attempts.len(),
        "dispatch attempt count differs from independent history"
    );
    assert_eq!(
        effect.attempts, history.attempts,
        "dispatch attempt order differs from independent history"
    );
    assert_eq!(
        &effect.status, status,
        "status differs from independent history"
    );
    assert_eq!(
        Some(effect.guarantee),
        history.guarantee,
        "guarantee differs from independent history"
    );
}

fn commit(
    reducer: &mut Reducer,
    replay: &mut Reducer,
    history: &mut ExpectedHistory,
    fault: Fault,
    action: Action,
) -> Command {
    assert_history(reducer, history);
    let cmd = command(
        operation(history.revision as u8 + 1),
        history.revision,
        action,
    );
    expect_transition(history, &cmd.action);
    let before = reducer.copy(&mut None).unwrap();
    let planned = reducer.plan(&cmd).unwrap();
    assert_eq!(*reducer, before, "effect planning changed state");
    let applied = reducer.apply(&cmd).unwrap();
    if fault == Fault::DuplicateAttempt
        && let Action::MarkEffectDispatched {
            effect_id,
            attempt_id,
        } = &cmd.action
    {
        reducer
            .projection
            .effects
            .get_mut(effect_id)
            .unwrap()
            .attempts
            .push(*attempt_id);
    }
    assert_history(reducer, history);
    assert_eq!(applied, planned);
    let ApplyResult::Applied { event } = applied else {
        panic!("fresh effect operation replayed")
    };
    assert_eq!(event.revision, history.revision);
    replay.apply_committed(event.clone(), &mut None).unwrap();
    assert_history(replay, history);
    assert_eq!(reducer, replay, "committed effect replay diverged");
    let restored = restore(reducer, &issuer()).unwrap();
    assert_history(&restored, history);
    assert_eq!(restored, *reducer);
    let settled = reducer.copy(&mut None).unwrap();
    assert!(matches!(
        reducer.apply(&cmd).unwrap(),
        ApplyResult::Replayed { .. }
    ));
    if fault == Fault::ReplayAgain
        && matches!(&cmd.action,
        Action::ResolveEffect { observation } if observation.status == EffectStatus::Indeterminate)
    {
        reducer
            .apply_payload_with_work(&event.payload, event.revision + 1, &mut None)
            .unwrap();
        reducer.revision += 1;
    }
    assert_eq!(
        reducer.revision(),
        history.revision,
        "exact retry advanced independent history"
    );
    assert_history(reducer, history);
    assert_eq!(*reducer, settled, "exact effect retry changed state");
    cmd
}

fn plan_effect(id: EffectId, guarantee: EffectGuarantee, payload: &[u8]) -> Action {
    Action::PlanEffect {
        effect_id: id,
        provider: "example.provider".into(),
        guarantee,
        effect_kind: "example.send".into(),
        request: request_file(payload).unwrap(),
        result_schema: schema_file(&json!({"type":"object"})).unwrap(),
    }
}

fn rejected(reducer: &mut Reducer, history: &ExpectedHistory, id: u8, action: Action) {
    assert_history(reducer, history);
    let before = reducer.copy(&mut None).unwrap();
    let cmd = command(operation(id), history.revision, action);
    assert!(reducer.plan(&cmd).is_err(), "invalid effect was planned");
    assert_eq!(*reducer, before);
    assert!(reducer.apply(&cmd).is_err(), "invalid effect was applied");
    assert_eq!(*reducer, before, "effect rejection changed state");
    assert_history(reducer, history);
    let mut event = reducer.events.back().unwrap().clone();
    event.operation_id = cmd.operation_id;
    event.revision = history.revision + 1;
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
    assert_history(reducer, history);
}

fn attestation_fields(payload: &[u8], key_byte: usize, bit: u8, use_original: bool) {
    let owner = issuer();
    let verifier = owner.verifier(&mut None).unwrap();
    let id = EffectId::from_bytes([9; 16]);
    let attempt = EffectAttemptId::from_bytes([10; 16]);
    let mut reducer = reducer_fixture();
    let mut replay = reducer_fixture();
    let mut history = ExpectedHistory::default();
    commit(
        &mut reducer,
        &mut replay,
        &mut history,
        Fault::None,
        plan_effect(id, EffectGuarantee::IdempotentRetry, payload),
    );
    commit(
        &mut reducer,
        &mut replay,
        &mut history,
        Fault::None,
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
            &history,
            200,
            Action::ResolveEffect {
                observation: candidate,
            },
        );
    }
    commit(
        &mut reducer,
        &mut replay,
        &mut history,
        Fault::None,
        Action::ResolveEffect {
            observation: original,
        },
    );
}

fn effect_history(index: u8, retries: usize, payload: &[u8], success: bool, fault: Fault) {
    let guarantee = guarantee(index);
    let id = EffectId::from_bytes([9; 16]);
    let mut reducer = reducer_fixture();
    let mut replay = reducer_fixture();
    let mut history = ExpectedHistory::default();
    commit(
        &mut reducer,
        &mut replay,
        &mut history,
        fault,
        plan_effect(id, guarantee, payload),
    );
    let mut attempt = EffectAttemptId::from_bytes([10; 16]);
    commit(
        &mut reducer,
        &mut replay,
        &mut history,
        fault,
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
            &mut history,
            fault,
            Action::ResolveEffect { observation },
        );
        if guarantee == EffectGuarantee::IdempotentRetry {
            rejected(
                &mut reducer,
                &history,
                200,
                Action::MarkEffectDispatched {
                    effect_id: id,
                    attempt_id: attempt,
                },
            );
        }
        let before = reducer.copy(&mut None).unwrap();
        if fault == Fault::WidenRetry {
            reducer.projection.effects.get_mut(&id).unwrap().guarantee =
                EffectGuarantee::IdempotentRetry;
        }
        let previous_attempt = attempt;
        let next_attempt = EffectAttemptId::from_bytes([20 + round as u8; 16]);
        let cmd = command(
            operation(history.revision as u8 + 1),
            history.revision,
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
            rejected(
                &mut reducer,
                &history,
                history.revision as u8 + 1,
                cmd.action,
            );
            break;
        }
        commit(&mut reducer, &mut replay, &mut history, fault, cmd.action);
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
        let expected_attempt = history.attempts.last().copied().unwrap();
        assert_eq!(expected_attempt, attempt);
        assert_ne!(
            stale.attempt_id, expected_attempt,
            "stale probe used the latest attempt"
        );
        if fault == Fault::AcceptStale {
            *reducer
                .projection
                .effects
                .get_mut(&id)
                .unwrap()
                .attempts
                .last_mut()
                .unwrap() = previous_attempt;
        }
        let stale_command = command(
            operation(200),
            history.revision,
            Action::ResolveEffect {
                observation: stale.clone(),
            },
        );
        assert!(
            reducer.plan(&stale_command).is_err(),
            "stale attempt was accepted"
        );
        rejected(
            &mut reducer,
            &history,
            200,
            Action::ResolveEffect { observation: stale },
        );
        rejected(
            &mut reducer,
            &history,
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
        &mut history,
        fault,
        Action::ResolveEffect {
            observation: observation.clone(),
        },
    );
    assert_eq!(reducer.effect(id).unwrap().status, status);
    rejected(
        &mut reducer,
        &history,
        200,
        Action::ResolveEffect { observation },
    );
    rejected(
        &mut reducer,
        &history,
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
                effect_history(index, retries, &payload, success, Fault::None);
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
    effect_history(1, 1, b"{}", false, Fault::WidenRetry);
}

#[test]
#[should_panic(expected = "dispatch attempt count differs from independent history")]
fn negative_control_detects_double_appended_dispatch_attempts() {
    effect_history(0, 1, b"{}", false, Fault::DuplicateAttempt);
}

#[test]
#[should_panic(expected = "stale attempt was accepted")]
fn negative_control_detects_accepting_a_stale_attempt() {
    effect_history(0, 1, b"{}", false, Fault::AcceptStale);
}

#[test]
#[should_panic(expected = "exact retry advanced independent history")]
fn negative_control_detects_double_applied_exact_retry() {
    effect_history(0, 1, b"{}", false, Fault::ReplayAgain);
}
