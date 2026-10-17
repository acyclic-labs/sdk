// Authority audiences, effect attestations and effect histories through one
// shared oracle: the fixtures only record observations, the invariants in
// `oracle` judge them, and a targeted fault proves each invariant load bearing.
use super::*;
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};
use proptest::prelude::*;

const KINDS: [AggregateKind; 5] = [
    AggregateKind::Agent,
    AggregateKind::Conversation,
    AggregateKind::Session,
    AggregateKind::Turn,
    AggregateKind::Task,
];
const EFFECT: EffectId = EffectId::from_bytes([9; 16]);

#[derive(Debug)]
enum Case {
    Audience {
        id: String,
        key: [u8; 32],
        agent: [u8; 16],
    },
    Attestation {
        payload: Vec<u8>,
        key_byte: usize,
        bit: u8,
    },
    History {
        index: u8,
        retries: usize,
        payload: Vec<u8>,
        success: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    /// Verifies with the grant's own audience instead of the target's.
    OriginalAudience,
    /// Submits the untouched observation in place of each changed field.
    OriginalObservation,
    /// Widens a nonretryable effect to idempotent retry before dispatch.
    WidenRetry,
    /// Appends every dispatch attempt twice.
    DuplicateAttempt,
    /// Rewinds the latest attempt so the stale one is current again.
    AcceptStale,
    /// Applies an exact indeterminate retry a second time.
    ReplayAgain,
}

/// Effect state as the independent history predicts it or as a reducer shows it.
#[derive(Clone, Debug, Default, PartialEq)]
struct History {
    revision: u64,
    attempts: Vec<EffectAttemptId>,
    status: Option<EffectStatus>,
    guarantee: Option<EffectGuarantee>,
}

#[derive(Debug)]
enum Seen {
    History {
        view: &'static str,
        actual: History,
        expected: History,
    },
    Agrees {
        view: &'static str,
        equal: bool,
    },
    Inert {
        what: &'static str,
        unchanged: bool,
    },
    Boundary {
        allowed: bool,
        retryable: bool,
    },
    Refused {
        step: &'static str,
        accepted: bool,
        unchanged: bool,
    },
    Authenticated {
        field: u8,
        verified: bool,
    },
    Audience {
        check: &'static str,
        outcome: bool,
        expected: bool,
    },
}

type See<'a, 'b> = &'a mut Observer<'b, Seen>;

fn violation(ok: bool, detail: impl FnOnce() -> String) -> std::result::Result<(), String> {
    if ok { Ok(()) } else { Err(detail()) }
}

// A production call the case expects to succeed; failure is a recorded
// divergence, never a fixture panic, so deletion reruns can stop cleanly.
fn ok<T, E>(
    see: See<'_, '_>,
    view: &'static str,
    result: std::result::Result<T, E>,
) -> std::result::Result<T, Stop> {
    match result {
        Ok(value) => Ok(value),
        Err(_) => {
            see.see(Seen::Agrees { view, equal: false })?;
            Err(Stop)
        }
    }
}

fn oracle() -> Oracle<Case, Fault, Seen> {
    let history = |index, retries| Case::History {
        index,
        retries,
        payload: b"{}".to_vec(),
        success: false,
    };
    Oracle::declare(run)
        .invariant("history", |seen| match seen {
            Seen::History {
                view,
                actual,
                expected,
            } => violation(actual == expected, || {
                format!("{view}: {actual:?} differs from independent {expected:?}")
            }),
            Seen::Agrees { view, equal } => violation(*equal, || format!("{view} diverged")),
            _ => Ok(()),
        })
        .invariant("inert", |seen| match seen {
            Seen::Inert { what, unchanged } => {
                violation(*unchanged, || format!("{what} changed state"))
            }
            _ => Ok(()),
        })
        .invariant("boundary", |seen| match seen {
            Seen::Boundary { allowed, retryable } => violation(allowed == retryable, || {
                "dispatch crossed a delivery-guarantee boundary".into()
            }),
            _ => Ok(()),
        })
        .invariant("rejection", |seen| match seen {
            Seen::Refused {
                step,
                accepted,
                unchanged,
            } => violation(!accepted && *unchanged, || {
                format!("invalid effect {step}: accepted {accepted}, unchanged {unchanged}")
            }),
            _ => Ok(()),
        })
        .invariant("authenticated", |seen| match seen {
            Seen::Authenticated { field, verified } => violation(!verified, || {
                format!("effect field {field} was not authenticated")
            }),
            _ => Ok(()),
        })
        .invariant("audience", |seen| match seen {
            Seen::Audience {
                check,
                outcome,
                expected,
            } => violation(outcome == expected, || {
                format!("{check} crossed an aggregate-kind boundary")
            }),
            _ => Ok(()),
        })
        .control("history", history(0, 1), Fault::DuplicateAttempt)
        .control("inert", history(0, 1), Fault::ReplayAgain)
        .control("boundary", history(1, 1), Fault::WidenRetry)
        .control("rejection", history(0, 1), Fault::AcceptStale)
        .control(
            "authenticated",
            Case::Attestation {
                payload: b"{}".to_vec(),
                key_byte: 0,
                bit: 0,
            },
            Fault::OriginalObservation,
        )
        .control(
            "audience",
            Case::Audience {
                id: "same-id".into(),
                key: [7; 32],
                agent: [1; 16],
            },
            Fault::OriginalAudience,
        )
        .build()
}

fn run(case: &Case, fault: Option<&Fault>, see: See<'_, '_>) -> std::result::Result<(), Stop> {
    let fault = fault.copied();
    match case {
        Case::Audience { id, key, agent } => {
            audience_binding(id, *key, AgentId::from_bytes(*agent), fault, see)
        }
        Case::Attestation {
            payload,
            key_byte,
            bit,
        } => attestation_fields(payload, *key_byte, *bit, fault, see),
        Case::History {
            index,
            retries,
            payload,
            success,
        } => effect_history(*index, *retries, payload, *success, fault, see),
    }
}

fn audience_binding(
    id: &str,
    key: [u8; 32],
    agent: AgentId,
    fault: Option<Fault>,
    see: See<'_, '_>,
) -> std::result::Result<(), Stop> {
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
            let same = kind == target;
            let foreign = AuthorityIssuer::new(
                "issuer",
                key,
                Authority {
                    kind: target,
                    id: id.into(),
                },
            );
            let verifier = match fault {
                Some(Fault::OriginalAudience) => owner.verifier(&mut None),
                _ => foreign.verifier(&mut None),
            }
            .unwrap();
            let mut check = |check, outcome, expected| {
                see.see(Seen::Audience {
                    check,
                    outcome,
                    expected,
                })
            };
            let verified = verifier.verify(&scope, &mut None).is_ok();
            check("scope verification", verified, same)?;
            let mut reducer = Reducer::new(foreign.audience.clone(), verifier, schemas());
            let before = reducer.copy(&mut None).unwrap();
            check("planning", reducer.plan(&cmd).is_ok(), same)?;
            check("planning leaves state", reducer == before, true)?;
            check("admission", reducer.apply(&cmd).is_ok(), same)?;
            if same {
                check("admitted revision", reducer.revision() == 1, true)?;
                let active = reducer.lifecycle() == LifecycleState::Active;
                check("admitted lifecycle", active, true)?;
                let restored = restore(&reducer, &foreign).map(|restored| restored == reducer);
                check("restore", restored.unwrap_or(false), true)?;
            } else {
                check("foreign admission leaves state", reducer == before, true)?;
            }
        }
    }
    Ok(())
}

fn guarantee(index: u8) -> EffectGuarantee {
    match index % 3 {
        0 => EffectGuarantee::IdempotentRetry,
        1 => EffectGuarantee::AtMostOnce,
        _ => EffectGuarantee::ExactlyOnce,
    }
}

// Update from the submitted input before calling any production reducer API.
fn expect_transition(history: &mut History, action: &Action) {
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

fn observe(
    see: See<'_, '_>,
    view: &'static str,
    reducer: &Reducer,
    expected: &History,
) -> std::result::Result<(), Stop> {
    let effect = reducer.effect(EFFECT);
    see.see(Seen::History {
        view,
        actual: History {
            revision: reducer.revision(),
            attempts: effect
                .map(|effect| effect.attempts.clone())
                .unwrap_or_default(),
            status: effect.map(|effect| effect.status.clone()),
            guarantee: effect.map(|effect| effect.guarantee),
        },
        expected: expected.clone(),
    })
}

struct Effects {
    reducer: Reducer,
    replay: Reducer,
    history: History,
    fault: Option<Fault>,
}

impl Effects {
    fn new(fault: Option<Fault>) -> Self {
        Self {
            reducer: reducer_fixture(),
            replay: reducer_fixture(),
            history: History::default(),
            fault,
        }
    }

    fn commit(&mut self, see: See<'_, '_>, action: Action) -> std::result::Result<(), Stop> {
        let Self {
            reducer,
            replay,
            history,
            fault,
        } = self;
        observe(see, "live", reducer, history)?;
        let cmd = command(
            operation(history.revision as u8 + 1),
            history.revision,
            action,
        );
        expect_transition(history, &cmd.action);
        let before = reducer.copy(&mut None).unwrap();
        let planned = ok(see, "fresh planning", reducer.plan(&cmd))?;
        see.see(Seen::Inert {
            what: "effect planning",
            unchanged: *reducer == before,
        })?;
        let applied = ok(see, "fresh application", reducer.apply(&cmd))?;
        if *fault == Some(Fault::DuplicateAttempt)
            && let Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            } = &cmd.action
        {
            let effect = reducer.projection.effects.get_mut(effect_id).unwrap();
            effect.attempts.push(*attempt_id);
        }
        observe(see, "applied", reducer, history)?;
        see.see(Seen::Agrees {
            view: "apply result",
            equal: applied == planned,
        })?;
        let ApplyResult::Applied { event } = applied else {
            return ok(see, "fresh operation application", Err(()));
        };
        see.see(Seen::Agrees {
            view: "event revision",
            equal: event.revision == history.revision,
        })?;
        let replayed = replay.apply_committed(event.clone(), &mut None);
        ok(see, "committed replay", replayed)?;
        observe(see, "replayed", replay, history)?;
        see.see(Seen::Agrees {
            view: "committed replay",
            equal: reducer == replay,
        })?;
        let restored = ok(see, "restore", restore(reducer, &issuer()))?;
        observe(see, "restored", &restored, history)?;
        see.see(Seen::Agrees {
            view: "restore",
            equal: restored == *reducer,
        })?;
        let settled = reducer.copy(&mut None).unwrap();
        let retried = ok(see, "exact retry", reducer.apply(&cmd))?;
        if *fault == Some(Fault::ReplayAgain)
            && matches!(&cmd.action,
            Action::ResolveEffect { observation } if observation.status == EffectStatus::Indeterminate)
        {
            reducer
                .apply_payload_with_work(&event.payload, event.revision + 1, &mut None)
                .unwrap();
            reducer.revision += 1;
        }
        see.see(Seen::Inert {
            what: "exact effect retry",
            unchanged: matches!(retried, ApplyResult::Replayed { .. }) && *reducer == settled,
        })?;
        observe(see, "retried", reducer, history)
    }

    // Plans, applies and replays an authenticated committed event for an
    // invalid transition; every route must refuse it without changing state.
    fn rejected(
        &mut self,
        see: See<'_, '_>,
        id: u8,
        action: Action,
    ) -> std::result::Result<(), Stop> {
        let Self {
            reducer, history, ..
        } = self;
        let before = reducer.copy(&mut None).unwrap();
        let cmd = command(operation(id), history.revision, action);
        let mut refused = |step, accepted, reducer: &Reducer| {
            see.see(Seen::Refused {
                step,
                accepted,
                unchanged: *reducer == before,
            })
        };
        let accepted = reducer.plan(&cmd).is_ok();
        refused("planning", accepted, &*reducer)?;
        let accepted = reducer.apply(&cmd).is_ok();
        refused("application", accepted, &*reducer)?;
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
        let accepted = reducer.apply_committed(event, &mut None).is_ok();
        refused("committed event", accepted, &*reducer)?;
        observe(see, "after rejection", reducer, history)
    }

    fn attest(&self, attempt: EffectAttemptId, status: EffectStatus) -> EffectAttestation {
        let effect = self.reducer.effect(EFFECT).unwrap();
        issuer()
            .attest_effect(EFFECT, effect, attempt, status, &mut None)
            .unwrap()
    }
}

fn plan_effect(guarantee: EffectGuarantee, payload: &[u8]) -> Action {
    Action::PlanEffect {
        effect_id: EFFECT,
        provider: "example.provider".into(),
        guarantee,
        effect_kind: "example.send".into(),
        request: request_file(payload).unwrap(),
        result_schema: schema_file(&json!({"type":"object"})).unwrap(),
    }
}

fn dispatch(attempt_id: EffectAttemptId) -> Action {
    Action::MarkEffectDispatched {
        effect_id: EFFECT,
        attempt_id,
    }
}

fn resolve(observation: EffectAttestation) -> Action {
    Action::ResolveEffect { observation }
}

fn attestation_fields(
    payload: &[u8],
    key_byte: usize,
    bit: u8,
    fault: Option<Fault>,
    see: See<'_, '_>,
) -> std::result::Result<(), Stop> {
    let verifier = issuer().verifier(&mut None).unwrap();
    let attempt = EffectAttemptId::from_bytes([10; 16]);
    let mut effects = Effects::new(fault);
    effects.commit(see, plan_effect(EffectGuarantee::IdempotentRetry, payload))?;
    effects.commit(see, dispatch(attempt))?;
    let original = effects.attest(attempt, EffectStatus::Indeterminate);
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
        let candidate = match fault {
            Some(Fault::OriginalObservation) => original.clone(),
            _ => changed,
        };
        see.see(Seen::Authenticated {
            field,
            verified: verifier.verify_effect(&candidate, &mut None).is_ok(),
        })?;
        effects.rejected(see, 200, resolve(candidate))?;
    }
    effects.commit(see, resolve(original))
}

fn effect_history(
    index: u8,
    retries: usize,
    payload: &[u8],
    success: bool,
    fault: Option<Fault>,
    see: See<'_, '_>,
) -> std::result::Result<(), Stop> {
    let guarantee = guarantee(index);
    let retryable = guarantee == EffectGuarantee::IdempotentRetry;
    let mut effects = Effects::new(fault);
    effects.commit(see, plan_effect(guarantee, payload))?;
    let mut attempt = EffectAttemptId::from_bytes([10; 16]);
    effects.commit(see, dispatch(attempt))?;
    for round in 0..retries {
        let observation = effects.attest(attempt, EffectStatus::Indeterminate);
        effects.commit(see, resolve(observation))?;
        if retryable {
            effects.rejected(see, 200, dispatch(attempt))?;
        }
        if fault == Some(Fault::WidenRetry) {
            let effect = effects.reducer.projection.effects.get_mut(&EFFECT).unwrap();
            effect.guarantee = EffectGuarantee::IdempotentRetry;
        }
        let previous = attempt;
        let next = EffectAttemptId::from_bytes([20 + round as u8; 16]);
        let revision = effects.history.revision;
        let cmd = command(operation(revision as u8 + 1), revision, dispatch(next));
        see.see(Seen::Boundary {
            allowed: effects.reducer.plan(&cmd).is_ok(),
            retryable,
        })?;
        if !retryable {
            effects.rejected(see, revision as u8 + 1, cmd.action)?;
            break;
        }
        effects.commit(see, cmd.action)?;
        attempt = next;
        let stale = effects.attest(
            previous,
            EffectStatus::Failed {
                message: "stale attempt".into(),
            },
        );
        let verifier = issuer().verifier(&mut None).unwrap();
        verifier.verify_effect(&stale, &mut None).unwrap();
        // Fixture sanity: the probe is stale against the independent history.
        assert_eq!(effects.history.attempts.last(), Some(&attempt));
        assert_ne!(
            stale.attempt_id, attempt,
            "stale probe used the latest attempt"
        );
        if fault == Some(Fault::AcceptStale) {
            let effect = effects.reducer.projection.effects.get_mut(&EFFECT).unwrap();
            *effect.attempts.last_mut().unwrap() = previous;
        }
        effects.rejected(see, 200, resolve(stale))?;
        effects.rejected(see, 200, dispatch(attempt))?;
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
    let observation = effects.attest(attempt, status);
    effects.commit(see, resolve(observation.clone()))?;
    effects.rejected(see, 200, resolve(observation))?;
    effects.rejected(see, 200, dispatch(EffectAttemptId::from_bytes([99; 16])))
}

#[test]
fn aggregate_kinds_are_distinct_authority_audiences() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        id in "[a-zA-Z0-9_-]{1,32}", key in any::<[u8; 32]>(), agent in any::<[u8; 16]>()
    )| oracle.check(&Case::Audience { id, key, agent }));
}

#[test]
fn every_effect_attestation_field_is_authenticated() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        payload in prop::collection::vec(any::<u8>(), 0..128), key_byte in 0..32_usize, bit in 0..8_u8
    )| oracle.check(&Case::Attestation { payload, key_byte, bit }));
}

#[test]
fn effect_retry_replay_and_restore_preserve_guarantees() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        retries in 0..7_usize, payload in prop::collection::vec(any::<u8>(), 0..128)
    )| {
        for index in 0..3 {
            for success in [false, true] {
                let payload = payload.clone();
                oracle.check(&Case::History { index, retries, payload, success });
            }
        }
    });
}
