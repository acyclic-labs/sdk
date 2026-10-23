use super::*;
use proptest::prelude::*;

fn capabilities(mask: u8) -> Capabilities {
    Capabilities::new(
        [
            "event:append",
            "lifecycle:manage",
            "effect:run",
            "effect:plan",
        ]
        .into_iter()
        .enumerate()
        .filter(|(bit, _)| mask & (1 << bit) != 0)
        .map(|(_, name)| name),
    )
}

fn lifecycle(index: u8) -> LifecycleState {
    match index % 6 {
        0 => LifecycleState::Pending,
        1 => LifecycleState::Active,
        2 => LifecycleState::Waiting,
        3 => LifecycleState::Completed,
        4 => LifecycleState::Failed,
        _ => LifecycleState::Cancelled,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn authority_policy_intersection_and_denials_are_monotone(
        masks in prop::collection::vec((0..16_u8, 0..16_u8), 1..8),
        extra in (0..16_u8, 0..16_u8),
    ) {
        let mut layers: Vec<_> = masks.iter().map(|(grants, denies)| crate::AuthorityPolicy {
            grants: capabilities(*grants), denies: capabilities(*denies),
        }).collect();
        let expected = masks.iter().fold(15, |allowed, (grants, denies)| allowed & grants & !denies);
        let original = crate::resolve_policies(&layers);
        prop_assert_eq!(&original, &capabilities(expected));
        layers.reverse();
        prop_assert_eq!(crate::resolve_policies(&layers), original.clone());
        layers.push(crate::AuthorityPolicy { grants: capabilities(extra.0), denies: capabilities(extra.1) });
        let narrowed = crate::resolve_policies(&layers);
        prop_assert!(narrowed.is_subset_of(&original));
        prop_assert_eq!(narrowed, capabilities(expected & extra.0 & !extra.1));
        prop_assert_eq!(crate::resolve_policies(&[]), Capabilities::default());
    }

    #[test]
    fn authority_attenuation_never_widens(
        root in 0..16_u8,
        requests in prop::collection::vec(0..16_u8, 1..16),
        agent in any::<[u8; 16]>(),
    ) {
        let owner = issuer();
        let agent = Some(AgentId::from_bytes(agent));
        let mut parent = owner.issue("root", &capabilities(root), agent, None, &mut None).unwrap();
        let verifier = owner.verifier(&mut None).unwrap();
        for (index, requested) in requests.into_iter().enumerate() {
            let requested = capabilities(requested);
            let result = owner.attenuate(&parent, format!("child-{index}"), &requested);
            prop_assert_eq!(result.is_ok(), requested.is_subset_of(parent.capabilities()));
            if let Ok(child) = result {
                prop_assert!(verifier.verify(&child, &mut None).is_ok());
                prop_assert_eq!(child.agent(), agent);
                prop_assert_eq!(child.parent_proof, Some(*parent.proof));
                prop_assert!(child.capabilities().is_subset_of(&capabilities(root)));
                parent = child;
            }
        }
    }

    #[test]
    fn authority_authenticates_every_scope_field(
        mask in 0..16_u8, field in 0..6_u8, byte in 0..32_usize, bit in 0..8_u8,
    ) {
        scope_field_property(mask, field, byte, bit, false)?;
    }

    #[test]
    fn reducer_rejection_replay_and_restore_preserve_state(
        steps in prop::collection::vec((0..6_u8, any::<bool>(), any::<bool>()), 1..32),
    ) {
        lifecycle_history(steps, LifecycleFault::None)?;
    }

    #[test]
    fn reducer_rejects_forged_events_and_snapshots(field in 0..8_u8) {
        forged_event_property(field, false)?;
    }
}

#[derive(Clone, Copy, PartialEq)]
enum LifecycleFault {
    None,
    RejectAll,
    ReopenTerminal,
    DuplicateRetry,
}

fn lifecycle_history(
    steps: Vec<(u8, bool, bool)>,
    fault: LifecycleFault,
) -> proptest::test_runner::TestCaseResult {
    let mut reducer = reducer_fixture();
    let mut replay = reducer_fixture();
    // These are updated solely from submitted inputs, before production admission.
    let mut phase = LifecycleState::Pending;
    let mut revision = 0;
    prop_assert_eq!(reducer.lifecycle(), phase, "initial lifecycle differs");
    prop_assert_eq!(reducer.revision(), revision, "initial revision differs");
    for (index, (target, stale, unauthorized)) in steps.into_iter().enumerate() {
        let from = phase;
        let to = lifecycle(target);
        let legal = match from {
            LifecycleState::Pending => {
                matches!(to, LifecycleState::Active | LifecycleState::Cancelled)
            }
            LifecycleState::Active | LifecycleState::Waiting => matches!(
                (from, to),
                (LifecycleState::Active, LifecycleState::Waiting)
                    | (LifecycleState::Waiting, LifecycleState::Active)
                    | (
                        _,
                        LifecycleState::Completed
                            | LifecycleState::Failed
                            | LifecycleState::Cancelled
                    )
            ),
            _ => false,
        };
        let accepted = legal && !stale && !unauthorized;
        let mut cmd = command(
            operation(index as u8 + 1),
            revision + u64::from(stale),
            Action::TransitionLifecycle {
                to,
                reason: Some(format!("step-{index}")),
            },
        );
        if unauthorized || fault == LifecycleFault::RejectAll {
            cmd.scope = issuer()
                .issue("empty", &Capabilities::default(), None, None, &mut None)
                .unwrap();
        }
        let reopening = fault == LifecycleFault::ReopenTerminal
            && matches!(
                from,
                LifecycleState::Completed | LifecycleState::Failed | LifecycleState::Cancelled
            )
            && to == LifecycleState::Active;
        if reopening {
            reducer.projection.lifecycle = LifecycleState::Waiting;
        }
        let before = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
        if accepted {
            phase = to;
            revision += 1;
        }
        let planned = reducer.plan(&cmd);
        prop_assert_eq!(&reducer.snapshot(usize::MAX, &mut None).unwrap().0, &before);
        prop_assert_eq!(
            planned.is_ok(),
            accepted,
            "lifecycle admission differs from independent history"
        );
        // Stop each mutant at its named observation, so deleting that assertion
        // permits completion instead of failing a later redundant check.
        if reopening || (fault == LifecycleFault::RejectAll && accepted) {
            return Ok(());
        }
        let applied = reducer.apply(&cmd);
        prop_assert_eq!(
            applied.is_ok(),
            accepted,
            "lifecycle application differs from independent history"
        );
        match applied {
            Ok(ApplyResult::Applied { event }) => {
                prop_assert_eq!(event.revision, revision);
                prop_assert_eq!(reducer.lifecycle(), phase);
                prop_assert_eq!(reducer.revision(), revision);
                replay.apply_committed(event.clone(), &mut None).unwrap();
                prop_assert_eq!(replay.lifecycle(), phase);
                prop_assert_eq!(replay.revision(), revision);
                let settled = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
                prop_assert!(
                    matches!(reducer.apply(&cmd).unwrap(), ApplyResult::Replayed { .. }),
                    "exact retry must replay"
                );
                if fault == LifecycleFault::DuplicateRetry {
                    // Mutant commits an already recorded event a second time.
                    reducer.revision += 1;
                    reducer.events.push_back(event.clone());
                }
                prop_assert_eq!(
                    reducer.revision(),
                    revision,
                    "exact retry advanced independent history"
                );
                if fault == LifecycleFault::DuplicateRetry {
                    return Ok(());
                }
                let mut collision = cmd.clone();
                collision.action = Action::TransitionLifecycle {
                    to: from,
                    reason: None,
                };
                prop_assert!(reducer.apply(&collision).is_err());
                let mut tampered = event;
                tampered.revision += 1;
                prop_assert!(reducer.apply_committed(tampered, &mut None).is_err());
                prop_assert_eq!(reducer.snapshot(usize::MAX, &mut None).unwrap().0, settled);
            }
            Err(_) => prop_assert_eq!(&reducer.snapshot(usize::MAX, &mut None).unwrap().0, &before),
            Ok(ApplyResult::Replayed { .. }) => prop_assert!(false, "fresh operation replayed"),
        }
        let restored = restore(&reducer, &issuer()).unwrap();
        for observed in [&reducer, &replay, &restored] {
            prop_assert_eq!(
                observed.lifecycle(),
                phase,
                "lifecycle differs from independent history"
            );
            prop_assert_eq!(
                observed.revision(),
                revision,
                "revision differs from independent history"
            );
        }
        prop_assert_eq!(&reducer, &replay);
        prop_assert_eq!(&restored, &reducer);
    }
    Ok(())
}

fn scope_field_property(
    mask: u8,
    field: u8,
    byte: usize,
    bit: u8,
    verify_original: bool,
) -> proptest::test_runner::TestCaseResult {
    let owner = issuer();
    let original = owner
        .issue("root", &capabilities(mask), None, None, &mut None)
        .unwrap();
    let mut tampered = original.clone();
    match field {
        0 => tampered.id.push('x'),
        1 => tampered.issuer.push('x'),
        2 => tampered.agent = Some(AgentId::from_bytes([3; 16])),
        3 => tampered.parent_proof = Some([4; 32]),
        4 => tampered.capabilities = capabilities(mask ^ 1),
        _ => tampered.proof[byte] ^= 1 << bit,
    }
    let candidate = if verify_original {
        &original
    } else {
        &tampered
    };
    prop_assert!(
        owner
            .verifier(&mut None)
            .unwrap()
            .verify(candidate, &mut None)
            .is_err()
    );
    let mut foreign = owner.clone();
    foreign.audience.id.push('x');
    prop_assert!(
        foreign
            .verifier(&mut None)
            .unwrap()
            .verify(&original, &mut None)
            .is_err()
    );
    let mut foreign_key = [7; 32];
    foreign_key[byte] ^= 1 << bit;
    foreign = AuthorityIssuer::new(owner.id.clone(), foreign_key, owner.audience.clone());
    prop_assert!(
        foreign
            .verifier(&mut None)
            .unwrap()
            .verify(&original, &mut None)
            .is_err()
    );
    Ok(())
}

fn forged_event_property(
    field: u8,
    replay_original: bool,
) -> proptest::test_runner::TestCaseResult {
    let mut reducer = reducer_fixture();
    let before = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
    let cmd = command(
        operation(1),
        0,
        Action::BindConversation {
            agent: AgentId::from_bytes([1; 16]),
        },
    );
    let ApplyResult::Applied { event } = reducer.plan(&cmd).unwrap() else {
        prop_assert!(false, "fresh command replayed");
        return Ok(());
    };
    let mut forged = event.clone();
    match field {
        0 => forged.revision += 1,
        1 => forged.operation_id = operation(2),
        2 => forged.intent_digest[0] ^= 1,
        3 => forged.scope.id.push('x'),
        4 => forged.scope.capabilities = Capabilities::default(),
        5 => {
            forged.causal_parent = Some(EventReference {
                authority: issuer().audience,
                revision: 1,
            })
        }
        6 => {
            forged.payload = EventPayload::ConversationBound {
                agent: AgentId::from_bytes([2; 16]),
            }
        }
        _ => forged.attestation[0] ^= 1,
    }
    let candidate = if replay_original {
        event.clone()
    } else {
        forged
    };
    prop_assert!(reducer.apply_committed(candidate, &mut None).is_err());
    prop_assert_eq!(reducer.snapshot(usize::MAX, &mut None).unwrap().0, before);
    reducer.apply_committed(event, &mut None).unwrap();
    snapshot_property(&reducer, false)
}

fn snapshot_property(
    reducer: &Reducer,
    restore_original: bool,
) -> proptest::test_runner::TestCaseResult {
    let mut snapshot = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
    if !restore_original {
        snapshot.revision += 1;
    }
    prop_assert!(
        Reducer::restore(
            snapshot,
            issuer().verifier(&mut None).unwrap(),
            schemas(),
            &mut None
        )
        .is_err(),
        "forged snapshot revision was accepted"
    );
    Ok(())
}

#[test]
fn negative_control_detects_verifying_the_original_scope() {
    let mut runner = negative_control_runner();
    let result = runner.run(
        &(0..16_u8, 0..6_u8, 0..32_usize, 0..8_u8),
        |(mask, field, byte, bit)| scope_field_property(mask, field, byte, bit, true),
    );
    assert!(matches!(
        result,
        Err(proptest::test_runner::TestError::Fail(..))
    ));
}

#[test]
fn negative_control_detects_replaying_the_original_event() {
    let result =
        negative_control_runner().run(&(0..8_u8), |field| forged_event_property(field, true));
    assert!(matches!(
        result,
        Err(proptest::test_runner::TestError::Fail(..))
    ));
}

#[test]
fn lifecycle_histories_cover_every_terminal_and_rejected_retry() {
    for terminal in 3..6 {
        let steps = vec![
            (1, false, false),
            (2, true, false),
            (2, false, true),
            (2, false, false),
            (1, false, false),
            (terminal, false, false),
            (1, false, false),
        ];
        lifecycle_history(steps, LifecycleFault::None).unwrap();
    }
    lifecycle_history(
        vec![(5, false, false), (1, false, false)],
        LifecycleFault::None,
    )
    .unwrap();
}

fn detects_lifecycle_fault(fault: LifecycleFault, steps: Vec<(u8, bool, bool)>, message: &str) {
    let result =
        negative_control_runner().run(&Just(steps), |steps| lifecycle_history(steps, fault));
    let Err(proptest::test_runner::TestError::Fail(reason, _)) = result else {
        panic!("lifecycle mutant survived: {message}");
    };
    assert!(
        reason.to_string().contains(message),
        "wrong invariant failed: {reason}"
    );
}

#[test]
fn negative_control_detects_rejecting_all_lifecycle_transitions() {
    detects_lifecycle_fault(
        LifecycleFault::RejectAll,
        vec![(1, false, false)],
        "lifecycle admission differs from independent history",
    );
}

#[test]
fn negative_control_detects_reopening_each_terminal_lifecycle() {
    for terminal in 3..6 {
        detects_lifecycle_fault(
            LifecycleFault::ReopenTerminal,
            vec![
                (1, false, false),
                (terminal, false, false),
                (1, false, false),
            ],
            "lifecycle admission differs from independent history",
        );
    }
}

#[test]
fn negative_control_detects_double_committing_an_exact_lifecycle_retry() {
    detects_lifecycle_fault(
        LifecycleFault::DuplicateRetry,
        vec![(1, false, false)],
        "exact retry advanced independent history",
    );
}

#[test]
fn negative_control_detects_restoring_the_original_snapshot() {
    let result =
        negative_control_runner().run(&Just(()), |()| snapshot_property(&reducer_fixture(), true));
    let Err(proptest::test_runner::TestError::Fail(reason, _)) = result else {
        panic!("snapshot mutant survived");
    };
    assert!(
        reason
            .to_string()
            .contains("forged snapshot revision was accepted")
    );
}

fn negative_control_runner() -> proptest::test_runner::TestRunner {
    let config = ProptestConfig {
        cases: 128,
        failure_persistence: None,
        ..ProptestConfig::default()
    };
    let rng = proptest::test_runner::TestRng::deterministic_rng(config.rng_algorithm);
    proptest::test_runner::TestRunner::new_with_rng(config, rng)
}
