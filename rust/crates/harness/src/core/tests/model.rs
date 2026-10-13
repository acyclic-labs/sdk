//! Atomic admission, replay, and checkpoint equivalence.
use super::*;

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
    let before = reducer.copy(&mut None)?;
    let mut empty_key = command(operation(30), 0, action.clone());
    empty_key.idempotency_key = IdempotencyKey(String::new());
    assert!(matches!(reducer.apply(empty_key), Err(Error::Invalid(_))));
    let mut future_cause = command(operation(31), 0, action.clone());
    future_cause.causal_parent = Some(EventReference {
        authority: reducer.authority().clone(),
        revision: 1,
    });
    assert!(matches!(
        reducer.apply(future_cause),
        Err(Error::Invalid(_))
    ));
    assert_eq!(reducer, before);
    let accepted = command(operation(32), 0, action);
    reducer.apply(accepted.clone())?;
    assert!(matches!(
        reducer.apply(accepted.clone())?,
        ApplyResult::Replayed { .. }
    ));
    let mut conflict = accepted;
    conflict.idempotency_key = IdempotencyKey("another intent".into());
    assert!(matches!(reducer.apply(conflict), Err(Error::Conflict(_))));
    assert_eq!(restore(&reducer, &issuer())?, reducer);
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
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
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
