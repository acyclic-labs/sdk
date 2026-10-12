//! Atomic admission, replay, and checkpoint equivalence.
use super::*;

#[derive(Debug)]
enum Step {
    Lifecycle(usize),
    Custom(u8),
    Bind(u8),
    Resubmit(usize),
    ConflictingIntent(usize),
    StaleRevision,
}

fn step() -> impl proptest::strategy::Strategy<Value = Step> {
    use proptest::prelude::*;
    prop_oneof![
        (0..6_usize).prop_map(Step::Lifecycle),
        any::<u8>().prop_map(Step::Custom),
        any::<u8>().prop_map(Step::Bind),
        any::<usize>().prop_map(Step::Resubmit),
        any::<usize>().prop_map(Step::ConflictingIntent),
        Just(Step::StaleRevision)
    ]
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]
    #[test]
    fn reducer_admission_is_replayable_and_atomic(steps in proptest::collection::vec(step(), 1..24)) {
        use proptest::prelude::*;
        let fail = |error: Error| TestCaseError::fail(error.to_string());
        let lifecycle = [LifecycleState::Pending, LifecycleState::Active, LifecycleState::Waiting,
            LifecycleState::Completed, LifecycleState::Failed, LifecycleState::Cancelled];
        let mut reducer = reducer_fixture();
        let mut admitted: Vec<(Command, Event)> = Vec::new();
        for (index, step) in steps.into_iter().enumerate() {
            let id = OperationId::from_bytes((index as u128 + 1).to_be_bytes());
            let revision = reducer.revision();
            let next = match step {
                Step::Lifecycle(to) => command(id, revision,
                    Action::TransitionLifecycle { to: lifecycle[to], reason: None }),
                Step::Custom(text) => command(id, revision, Action::AppendCustom {
                    schema: "example.message".into(), version: 1,
                    content: request_file(format!(r#"{{"text":"{text}"}}"#).as_bytes()).map_err(fail)?,
                }),
                Step::Bind(agent) => command(id, revision, Action::BindConversation { agent: AgentId::from_bytes([agent; 16]) }),
                Step::Resubmit(_) | Step::ConflictingIntent(_) if admitted.is_empty() => continue,
                Step::Resubmit(pick) => {
                    let (prior, event) = &admitted[pick % admitted.len()];
                    prop_assert_eq!(reducer.apply(prior.clone()).map_err(fail)?, ApplyResult::Replayed { event: event.clone() });
                    continue;
                }
                Step::ConflictingIntent(pick) => {
                    let (prior, _) = &admitted[pick % admitted.len()];
                    let before = reducer.copy(&mut None).map_err(fail)?;
                    prop_assert!(matches!(reducer.apply(command(prior.operation_id, revision,
                        Action::TransitionLifecycle { to: LifecycleState::Active, reason: Some("other intent".into()) })),
                        Err(Error::Conflict(_))), "operation identity rejects a conflicting intent");
                    prop_assert_eq!(&reducer, &before);
                    continue;
                }
                Step::StaleRevision => command(id, revision.wrapping_add(1),
                    Action::TransitionLifecycle { to: LifecycleState::Active, reason: None }),
            };
            let before = reducer.copy(&mut None).map_err(fail)?;
            let planned = reducer.plan(&next);
            prop_assert_eq!(&reducer, &before);
            match planned {
                Ok(ApplyResult::Applied { event }) => {
                    prop_assert_eq!(event.revision, revision + 1);
                    prop_assert_eq!(reducer.apply_committed(event.clone(), &mut None).map_err(fail)?,
                        ApplyResult::Applied { event: event.clone() });
                    prop_assert_eq!(reducer.revision(), revision + 1);
                    admitted.push((next, event));
                }
                Ok(ApplyResult::Replayed { .. }) => prop_assert!(false, "fresh operation replayed"),
                Err(_) => {
                    prop_assert!(reducer.apply(next).is_err());
                    prop_assert_eq!(&reducer, &before);
                }
            }
        }
        let snapshot = reducer.snapshot(usize::MAX, &mut None).map_err(fail)?.0;
        let wire = crate::contract::canonical_json_bytes(&snapshot).map_err(fail)?;
        let decoded = crate::contract::json_from_slice(&wire).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let verifier = issuer().verifier(&mut None).map_err(fail)?;
        let restored = Reducer::restore(decoded, verifier.clone(), schemas(), &mut None).map_err(fail)?;
        let mut replayed = Reducer::new(reducer.authority().clone(), verifier, schemas());
        for (_, event) in &admitted { replayed.apply_committed(event.clone(), &mut None).map_err(fail)?; }
        prop_assert_eq!(&restored, &reducer);
        prop_assert_eq!(&replayed, &reducer);
    }
}

#[test]
fn fixed_lifecycle_rejects_terminal_reopening() -> Result<()> {
    let owner = AuthorityIssuer::new(
        "test",
        [7; 32],
        Authority {
            kind: AggregateKind::Turn,
            id: "turn-1".into(),
        },
    );
    let mut reducer = Reducer::new(
        owner.audience.clone(),
        owner.verifier(&mut None)?,
        schemas(),
    );
    let transition = |id, revision, to| {
        let mut command = command(
            operation(id),
            revision,
            Action::TransitionLifecycle { to, reason: None },
        );
        command.scope = owner
            .issue(
                "turn",
                &Capabilities::new(["lifecycle:manage"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof");
        command
    };
    reducer.apply(transition(40, 0, LifecycleState::Active))?;
    reducer.apply(transition(41, 1, LifecycleState::Completed))?;
    assert!(matches!(
        reducer.apply(transition(42, 2, LifecycleState::Active)),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[test]
fn malformed_admission_metadata_is_rejected() -> Result<()> {
    let mut reducer = reducer_fixture();
    let action = Action::AppendCustom {
        schema: "example.message".into(),
        version: 1,
        content: request_file(br#"{"text":"hello"}"#)?,
    };
    let mut empty_key = command(operation(30), 0, action.clone());
    empty_key.idempotency_key = IdempotencyKey(String::new());
    assert!(matches!(reducer.apply(empty_key), Err(Error::Invalid(_))));
    let mut future_cause = command(operation(31), 0, action);
    future_cause.causal_parent = Some(EventReference {
        authority: reducer.authority().clone(),
        revision: 1,
    });
    assert!(matches!(
        reducer.apply(future_cause),
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_component_model_10000_real_signed_transitions_stays_live_source_bounded() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let budget = acyclic_fs::WorkBudget {
        allocation_operations: 4096,
        peak_allocation_bytes: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        source_bytes_read: 16_777_216,
        items_examined: 16_777_216,
        ..acyclic_fs::WorkBudget::default()
    };
    let mut reference = reducer_fixture();
    apply(
        &mut reference,
        1,
        Action::AppendCustom {
            schema: "example.message".into(),
            version: 1,
            content: request_file(br#"{"text":"original primed entry"}"#)?,
        },
    )?;
    let mut state = reference.copy(&mut None)?;
    state.set_resident_event_limit(4)?;
    reference.set_resident_event_limit(4)?;
    state.set_resident_terminal_effect_limit(2)?;
    reference.set_resident_terminal_effect_limit(2)?;
    let owner = issuer();
    let request = request_file(br#"{"prompt":"actual provider fixture request"}"#)?;
    let schema = schema_file(&json!({"type":"object"}))?;
    let custom = request_file(br#"{"text":"steady active extension state"}"#)?;
    let available =
        budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    let mut recent = VecDeque::new();
    for index in 0_u64..10_000 {
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&(index / 4 + 1).to_le_bytes());
        let effect_id = EffectId::from_bytes(id);
        let attempt_id = EffectAttemptId::from_bytes(id);
        let action = match index % 4 {
            0 => Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: custom.clone(),
            },
            1 => Action::PlanEffect {
                effect_id,
                provider: "example.provider".into(),
                guarantee: EffectGuarantee::AtMostOnce,
                effect_kind: "example.model_output".into(),
                request: request.clone(),
                result_schema: schema.clone(),
            },
            2 => Action::MarkEffectDispatched {
                effect_id,
                attempt_id,
            },
            _ => Action::ResolveEffect {
                observation: owner.attest_effect(
                    effect_id,
                    reference.effect(effect_id).expect("admitted effect"),
                    attempt_id,
                    EffectStatus::Failed {
                        message: "actual attested provider rejection".into(),
                    },
                    &mut None,
                )?,
            },
        };
        let mut operation = [0_u8; 16];
        operation[..8].copy_from_slice(&(reference.revision() + 1).to_le_bytes());
        let ApplyResult::Applied { event } = reference.apply(command(
            OperationId::from_bytes(operation),
            reference.revision(),
            action,
        ))?
        else {
            panic!("fresh model event replayed")
        };
        reference.mark_indexed_terminal_effect_with_work(&event, &mut None)?;
        let query = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(Error::from)?;
        let io = crate::filesystem::PreparationIo::from_native_work(&query)?;
        let ApplyResult::Applied { event } =
            state.apply_committed(event, &mut Some(&mut io.history_io()))?
        else {
            panic!("fresh model application replayed")
        };
        state.mark_indexed_terminal_effect_with_work(&event, &mut Some(&mut io.history_io()))?;
        drop(io);
        let receipt = query.receipt();
        assert!(receipt.allocation_operations < 512 && receipt.bytes_copied < 1_048_576);
        assert_eq!(state, reference, "transition {index}");
        assert!(state.events.len() <= 4);
        assert!(
            state.projection.effects.len() <= 3
                && state.projection.indexed_terminal_effects.len() <= 2
        );
        assert_eq!(state.projection.extension_states.len(), 1);
        recent.push_back((index, query));
        if recent.len() > 16 {
            let (index, retired) = recent.pop_front().expect("observation window");
            if index >= 64 {
                retired.admit_retained(available).map_err(Error::from)?;
            }
        }
    }
    assert_eq!(state.revision(), 10_001);
    let mut oldest = [0_u8; 16];
    oldest[..8].copy_from_slice(&1_u64.to_le_bytes());
    assert!(state.effect(EffectId::from_bytes(oldest)).is_none());
    drop(state);
    for (_, work) in recent {
        work.admit_retained(available).map_err(Error::from)?;
    }
    Ok(())
}
