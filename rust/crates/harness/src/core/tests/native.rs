//! Native copy, rollback, and bounded long-lived state.
use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

fn original_proof_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        allocation_operations: 4096,
        peak_allocation_bytes: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        source_bytes_read: 16_777_216,
        items_examined: 16_777_216,
        ..acyclic_fs::WorkBudget::default()
    }
}

fn native_component_event(reference: &mut Reducer, action: Action) -> Result<Event> {
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&(reference.revision() + 1).to_le_bytes());
    let ApplyResult::Applied { event } = reference.apply(command(
        OperationId::from_bytes(id),
        reference.revision(),
        action,
    ))?
    else {
        panic!("fresh model event replayed")
    };
    reference.mark_indexed_terminal_effect_with_work(&event, &mut None)?;
    Ok(event)
}

fn native_component_apply(
    state: &mut Reducer,
    event: Event,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let ApplyResult::Applied { event } =
        state.apply_committed(event, &mut Some(&mut io.history_io()))?
    else {
        panic!("fresh model application replayed")
    };
    state.mark_indexed_terminal_effect_with_work(&event, &mut Some(&mut io.history_io()))?;
    Ok(())
}

fn native_component_fixture(
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Reducer, Reducer)> {
    let mut reference = reducer_fixture();
    reference.set_resident_event_limit(1)?;
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    let mut state = reference.copy(&mut Some(&mut io.history_io()))?;
    drop(io);
    let event = native_component_event(
        &mut reference,
        Action::AppendCustom {
            schema: "example.message".into(),
            version: 1,
            content: request_file(br#"{"text":"original primed entry"}"#)?,
        },
    )?;
    native_component_apply(&mut state, event, work)?;
    assert_eq!(state, reference);
    Ok((state, reference))
}

fn native_work() -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new())
        .map_err(Error::from)
}

#[test]
fn native_component_clone_cold_restore_and_held_snapshot_survive_real_replacement_and_prune()
-> Result<()> {
    let prime = native_work()?;
    let (mut state, mut reference) = native_component_fixture(&prime)?;
    let snapshot_work = native_work()?;
    let held = state
        .snapshot(
            1,
            &mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(&snapshot_work)?
                    .history_io(),
            ),
        )?
        .0;
    let revision = held.revision;
    let clone_work = native_work()?;
    let io = crate::filesystem::PreparationIo::from_native_work(&clone_work)?;
    let cloned = state.copy(&mut Some(&mut io.history_io()))?;
    assert_eq!(cloned, state);
    drop(io);
    let wire = crate::contract::canonical_json_bytes(&held)?;
    let token = CancellationToken::new();
    let cold =
        NativeMaterializationWork::new(original_proof_budget(), &token).map_err(Error::from)?;
    let baseline = cold.live_allocation_bytes();
    let io = crate::filesystem::PreparationIo::from_native_work(&cold)?;
    let decoded = Snapshot::decode(&wire, wire.len() as u64, &mut Some(&mut io.history_io()))?;
    drop(io);
    token.cancel();
    let receipt = cold.receipt();
    let metadata = native_work()?;
    let io = crate::filesystem::PreparationIo::from_native_work(&metadata)?;
    let verifier = issuer().verifier(&mut Some(&mut io.history_io()))?;
    let mut restored = Reducer::restore(
        decoded,
        verifier,
        schemas(),
        &mut Some(&mut io.history_io()),
    )?;
    drop(io);
    restored.set_resident_event_limit(1)?;
    assert_eq!(restored, state);
    assert_eq!(cold.receipt(), receipt);
    let available = original_proof_budget().peak_allocation_bytes
        - NativeMaterializationWork::accounting_state_bytes();
    for index in 0..3 {
        let work = native_work()?;
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
    }
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
    let prime = native_work()?;
    let (mut measured, mut reference) = native_component_fixture(&prime)?;
    let event = native_component_event(
        &mut reference,
        Action::AppendCustom {
            schema: "example.message".into(),
            version: 1,
            content: request_file(br#"{"text":"real partially applied replacement"}"#)?,
        },
    )?;
    let success = native_work()?;
    native_component_apply(&mut measured, event.clone(), &success)?;
    let allowance = acyclic_fs::WorkBudget {
        bytes_copied: success.receipt().bytes_copied - 1,
        ..original_proof_budget()
    };
    let source = native_work()?;
    let (mut state, _) = native_component_fixture(&source)?;
    let head = state.events.front().expect("source event").clone();
    let receipt = source.receipt();
    let denied = NativeMaterializationWork::new(allowance, &CancellationToken::new())
        .map_err(Error::from)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&denied)?;
    let error = state
        .apply_committed(event, &mut Some(&mut io.history_io()))
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
        state.extension_state("example.message").unwrap().0.revision,
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
    let prime = native_work()?;
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
        let query = native_work()?;
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
