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
                prop_assert_eq!(child.parent_proof, Some(parent.proof));
                prop_assert!(child.capabilities().is_subset_of(&capabilities(root)));
                parent = child;
            }
        }
    }

    #[test]
    fn authority_authenticates_every_scope_field(
        mask in 0..16_u8, field in 0..6_u8, byte in 0..32_usize, bit in 0..8_u8,
    ) {
        let owner = issuer();
        let original = owner.issue("root", &capabilities(mask), None, None, &mut None).unwrap();
        let mut tampered = original.clone();
        match field {
            0 => tampered.id.push('x'),
            1 => tampered.issuer.push('x'),
            2 => tampered.agent = Some(AgentId::from_bytes([3; 16])),
            3 => tampered.parent_proof = Some([4; 32]),
            4 => tampered.capabilities = capabilities(mask ^ 1),
            _ => tampered.proof[byte] ^= 1 << bit,
        }
        prop_assert!(owner.verifier(&mut None).unwrap().verify(&tampered, &mut None).is_err());
        let mut foreign = owner.clone();
        foreign.audience.id.push('x');
        prop_assert!(foreign.verifier(&mut None).unwrap().verify(&original, &mut None).is_err());
        foreign = owner.clone();
        foreign.key[byte] ^= 1 << bit;
        prop_assert!(foreign.verifier(&mut None).unwrap().verify(&original, &mut None).is_err());
    }

    #[test]
    fn reducer_rejection_replay_and_restore_preserve_state(
        steps in prop::collection::vec((0..6_u8, any::<bool>(), any::<bool>()), 1..32),
    ) {
        let mut reducer = reducer_fixture();
        let mut replay = reducer_fixture();
        for (index, (target, stale, unauthorized)) in steps.into_iter().enumerate() {
            let before = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
            let from = reducer.lifecycle();
            let to = lifecycle(target);
            let mut cmd = command(operation(index as u8 + 1), reducer.revision(),
                Action::TransitionLifecycle { to, reason: Some(format!("step-{index}")) });
            if stale { cmd.expected_revision += 1; }
            if unauthorized {
                cmd.scope = issuer().issue("empty", &Capabilities::default(), None, None, &mut None).unwrap();
            }
            let planned = reducer.plan(&cmd);
            prop_assert_eq!(&reducer.snapshot(usize::MAX, &mut None).unwrap().0, &before);
            // Independent lifecycle oracle, including absorbing terminal states.
            let legal = match from {
                LifecycleState::Pending => matches!(to, LifecycleState::Active | LifecycleState::Cancelled),
                LifecycleState::Active => matches!(to, LifecycleState::Waiting | LifecycleState::Completed | LifecycleState::Failed | LifecycleState::Cancelled),
                LifecycleState::Waiting => matches!(to, LifecycleState::Active | LifecycleState::Completed | LifecycleState::Failed | LifecycleState::Cancelled),
                _ => false,
            };
            prop_assert_eq!(planned.is_ok(), legal && !stale && !unauthorized);
            match reducer.apply(&cmd) {
                Ok(ApplyResult::Applied { event }) => {
                    prop_assert_eq!(reducer.revision(), before.revision + 1);
                    prop_assert_eq!(reducer.lifecycle(), to);
                    replay.apply_committed(event.clone(), &mut None).unwrap();
                    let settled = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
                    prop_assert!(matches!(reducer.apply(&cmd).unwrap(), ApplyResult::Replayed { .. }), "exact retry must replay");
                    let mut collision = cmd.clone();
                    collision.action = Action::TransitionLifecycle { to: from, reason: None };
                    prop_assert!(reducer.apply(&collision).is_err());
                    let mut tampered = event;
                    tampered.revision += 1;
                    prop_assert!(reducer.apply_committed(tampered, &mut None).is_err());
                    prop_assert_eq!(reducer.snapshot(usize::MAX, &mut None).unwrap().0, settled);
                }
                Err(_) => prop_assert_eq!(&reducer.snapshot(usize::MAX, &mut None).unwrap().0, &before),
                Ok(ApplyResult::Replayed { .. }) => prop_assert!(false, "fresh operation replayed"),
            }
            prop_assert_eq!(&reducer, &replay);
            prop_assert_eq!(&restore(&reducer, &issuer()).unwrap(), &reducer);
        }
    }

    #[test]
    fn reducer_rejects_forged_events_and_snapshots(field in 0..8_u8) {
        let mut reducer = reducer_fixture();
        let before = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
        let cmd = command(operation(1), 0,
            Action::BindConversation { agent: AgentId::from_bytes([1; 16]) });
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
            5 => forged.causal_parent = Some(EventReference { authority: issuer().audience, revision: 1 }),
            6 => forged.payload = EventPayload::ConversationBound { agent: AgentId::from_bytes([2; 16]) },
            _ => forged.attestation[0] ^= 1,
        }
        prop_assert!(reducer.apply_committed(forged, &mut None).is_err());
        prop_assert_eq!(reducer.snapshot(usize::MAX, &mut None).unwrap().0, before);
        reducer.apply_committed(event, &mut None).unwrap();
        let mut snapshot = reducer.snapshot(usize::MAX, &mut None).unwrap().0;
        snapshot.revision += 1;
        prop_assert!(Reducer::restore(snapshot, issuer().verifier(&mut None).unwrap(), schemas(), &mut None).is_err());
    }
}
