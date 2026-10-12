//! Native copy, rollback, and bounded long-lived state.
use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

#[test]
fn native_component_clone_cold_restore_and_held_snapshot_survive_real_replacement_and_prune()
-> Result<()> {
    let prime = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
        .map_err(Error::from)?;
    let (mut state, mut reference) = native_component_fixture(&prime)?;
    let snapshot_work =
        NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
            .map_err(Error::from)?;
    let held = state
        .snapshot(
            1,
            &mut Some(
                &mut crate::store::history::native_work_io(&snapshot_work).map_err(Error::from)?,
            ),
        )?
        .0;
    let revision = held.revision;
    let clone_work =
        NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
            .map_err(Error::from)?;
    let mut io = crate::store::history::native_work_io(&clone_work).map_err(Error::from)?;
    let cloned = state.copy(&mut Some(&mut io))?;
    assert_eq!(cloned, state);
    assert_eq!(io.into_custody().bytes(), 0);
    let wire = crate::contract::canonical_json_bytes(&held)?;
    let token = CancellationToken::new();
    let cold =
        NativeMaterializationWork::new(original_proof_budget(), &token).map_err(Error::from)?;
    let baseline = cold.live_allocation_bytes();
    let mut io = crate::store::history::native_work_io(&cold).map_err(Error::from)?;
    let decoded = Snapshot::decode(&wire, wire.len() as u64, &mut Some(&mut io))?;
    assert_eq!(io.into_custody().bytes(), 0);
    token.cancel();
    let receipt = cold.receipt();
    let metadata =
        NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
            .map_err(Error::from)?;
    let mut io = crate::store::history::native_work_io(&metadata).map_err(Error::from)?;
    let verifier = issuer().verifier(&mut Some(&mut io))?;
    let mut restored = Reducer::restore(decoded, verifier, schemas(), &mut Some(&mut io))?;
    drop(io);
    restored.set_resident_event_limit(1)?;
    assert_eq!(restored, state);
    assert_eq!(cold.receipt(), receipt);
    let available = original_proof_budget().peak_allocation_bytes
        - NativeMaterializationWork::accounting_state_bytes();
    for index in 0..3 {
        let work =
            NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
                .map_err(Error::from)?;
        let event = native_component_event(
            &mut reference,
            Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: request_file(format!(r#"{{"text":"replacement {index}"}}"#).as_bytes())?,
            },
        )?;
        native_component_apply(&mut state, event.clone(), &work)?;
        native_component_apply(&mut restored, event, &work)?;
        assert_eq!(state, reference);
        assert_eq!(restored, state);
        assert_eq!(held.revision, revision);
        assert_eq!(cloned.revision(), revision);
        assert_eq!(held.events[0].revision, revision);
    }
    assert!(snapshot_work.admit_retained(available).is_err());
    assert!(clone_work.admit_retained(available).is_err());
    assert!(cold.live_allocation_bytes() > baseline);
    drop((state, restored));
    assert_eq!(cold.live_allocation_bytes(), baseline);
    metadata.admit_retained(available).map_err(Error::from)?;
    assert!(snapshot_work.admit_retained(available).is_err());
    drop(held);
    snapshot_work
        .admit_retained(available)
        .map_err(Error::from)?;
    assert!(clone_work.admit_retained(available).is_err());
    drop(cloned);
    clone_work.admit_retained(available).map_err(Error::from)?;
    Ok(())
}

#[test]
fn native_midapply_denial_drops_partial_copy_and_preserves_unreplaced_source() -> Result<()> {
    use acyclic_fs::{NativeTreeMaterializationError, WorkError};
    let prime = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
        .map_err(Error::from)?;
    let (mut measured, mut reference) = native_component_fixture(&prime)?;
    let event = native_component_event(
        &mut reference,
        Action::AppendCustom {
            schema: "example.message".into(),
            version: 1,
            content: request_file(br#"{"text":"real partially applied replacement"}"#)?,
        },
    )?;
    let success =
        NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
            .map_err(Error::from)?;
    native_component_apply(&mut measured, event.clone(), &success)?;
    let allowance = acyclic_fs::WorkBudget {
        bytes_copied: success.receipt().bytes_copied - 1,
        ..original_proof_budget()
    };
    let source = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
        .map_err(Error::from)?;
    let (mut state, _) = native_component_fixture(&source)?;
    let head = state.events.front().expect("source event").clone();
    let receipt = source.receipt();
    let denied = NativeMaterializationWork::new(allowance, &CancellationToken::new())
        .map_err(Error::from)?;
    let mut io = crate::store::history::native_work_io(&denied).map_err(Error::from)?;
    let error = state
        .apply_committed(event, &mut Some(&mut io))
        .expect_err("byte denial");
    assert!(matches!(
        &error,
        Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }),
            ..
        }
    ));
    drop(error);
    drop(io);
    assert_eq!(
        state
            .projection
            .extension_states
            .get("example.message")
            .expect("live extension")
            .0
            .revision,
        head.revision
    );
    assert_eq!(state.events.front(), Some(&head));
    assert_eq!(state.revision(), head.revision);
    assert_eq!(source.receipt(), receipt);
    let available =
        allowance.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    denied.admit_retained(available).map_err(Error::from)?;
    assert!(source.admit_retained(available).is_err());
    drop(state);
    denied.admit_retained(available).map_err(Error::from)?;
    source.admit_retained(available).map_err(Error::from)?;
    Ok(())
}

#[test]
fn native_component_model_10000_real_signed_transitions_stays_live_source_bounded() -> Result<()> {
    let prime = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
        .map_err(Error::from)?;
    let (mut state, mut reference) = native_component_fixture(&prime)?;
    state.set_resident_event_limit(4)?;
    reference.set_resident_event_limit(4)?;
    state.set_resident_terminal_effect_limit(2)?;
    reference.set_resident_terminal_effect_limit(2)?;
    let owner = issuer();
    let request = request_file(br#"{"prompt":"actual provider fixture request"}"#)?;
    let schema = schema_file(&json!({"type":"object"}))?;
    let custom = request_file(br#"{"text":"steady active extension state"}"#)?;
    let available = original_proof_budget().peak_allocation_bytes
        - NativeMaterializationWork::accounting_state_bytes();
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
        let event = native_component_event(&mut reference, action)?;
        let query =
            NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
                .map_err(Error::from)?;
        native_component_apply(&mut state, event, &query)?;
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
