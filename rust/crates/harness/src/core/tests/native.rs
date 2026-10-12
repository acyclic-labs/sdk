//! tests native semantics.
use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_component_clone_cold_restore_and_held_snapshot_survive_real_replacement_and_prune() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let prime = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("prime ledger");
        let (mut state, mut reference) = native_component_fixture(&prime)?;
        let snapshot_work = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("held snapshot ledger");
        let held = state.snapshot(1, &mut Some(&mut crate::store::history::native_work_io(&snapshot_work).map_err(|cause| Error::NativeWork { cause, custody: None })?))?.0;
        let old_revision = held.revision;
        let clone_work = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("independent reducer clone ledger");
        let mut clone_io = crate::store::history::native_work_io(&clone_work).expect("clone observer");
        let cloned = state.copy(&mut Some(&mut clone_io))?;
        assert_eq!(cloned, state);
        assert_eq!(clone_io.into_custody().bytes(), 0, "live copied components own every produced claim");
        let wire = crate::contract::canonical_json_bytes(&held)?;
        let cold_token = CancellationToken::new();
        let cold_work = NativeMaterializationWork::new(original_proof_budget(), &cold_token).expect("cold decoder ledger");
        let cold_baseline = cold_work.live_allocation_bytes();
        let mut cold_io = crate::store::history::native_work_io(&cold_work).expect("cold observer");
        let decoded = Snapshot::decode(&wire, wire.len() as u64, &mut Some(&mut cold_io))?;
        assert_eq!(cold_io.into_custody().bytes(), 0);
        cold_token.cancel();
        let cold_receipt = cold_work.receipt();
        let restore_work = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("restore metadata ledger");
        let mut io = crate::store::history::native_work_io(&restore_work).map_err(Error::from)?;
        let verifier = issuer().verifier(&mut Some(&mut io))?;
        let mut restored = Reducer::restore(decoded, verifier, schemas(), &mut Some(&mut io))?;
        drop(io);
        restored.set_resident_event_limit(1)?;
        assert_eq!(restored, state);
        assert_eq!(cold_work.receipt(), cold_receipt, "restore never spends on the independent source ledger");
        let available = original_proof_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        for index in 0..3 {
            let work = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("replacement query ledger");
            let event = native_component_event(&mut reference, Action::AppendCustom {
                schema: "example.message".into(), version: 1,
                content: request_file(format!(r#"{{"text":"replacement {index}"}}"#).as_bytes())?,
            })?;
            native_component_apply(&mut state, event.clone(), &work)?;
            native_component_apply(&mut restored, event, &work)?;
            assert_eq!(state, reference);
            assert_eq!(restored, state);
            assert_eq!(held.revision, old_revision);
            assert_eq!(cloned.revision(), old_revision);
            assert_eq!(held.events[0].revision, old_revision);
        }
        assert!(snapshot_work.admit_retained(available).is_err());
        assert!(clone_work.admit_retained(available).is_err());
        assert!(cold_work.live_allocation_bytes() > cold_baseline);
        drop((state, restored));
        assert_eq!(cold_work.live_allocation_bytes(), cold_baseline);
        restore_work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert!(snapshot_work.admit_retained(available).is_err(), "held snapshot survives both reducer drops");
        drop(held);
        snapshot_work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert!(clone_work.admit_retained(available).is_err(), "independent reducer copy remains live");
        drop(cloned);
        clone_work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_midapply_denial_drops_partial_copy_and_preserves_unreplaced_source() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let prime = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("prime ledger");
        let (mut measured, mut reference) = native_component_fixture(&prime)?;
        let event = native_component_event(&mut reference, Action::AppendCustom {
            schema: "example.message".into(), version: 1,
            content: request_file(br#"{"text":"real partially applied replacement"}"#)?,
        })?;
        let success = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("measured original ledger");
        native_component_apply(&mut measured, event.clone(), &success)?;
        let mut allowance = original_proof_budget();
        allowance.bytes_copied = success.receipt().bytes_copied - 1;
        let other_prime = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("identical source fixture ledger");
        let (mut state, _) = native_component_fixture(&other_prime)?;
        let source_head = state.events.front().expect("actual old source head").clone();
        let source_receipt = other_prime.receipt();
        let token = CancellationToken::new();
        let denied = NativeMaterializationWork::new(allowance, &token).expect("real late-phase denial ledger");
        let mut io = crate::store::history::native_work_io(&denied).expect("original denial observer");
        let error = state.apply_committed(event, &mut Some(&mut io)).expect_err("copy exceeds the byte limit");
        assert!(matches!(&error, Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { counter: "bytes_copied", .. }), ..
        }));
        drop(error);
        drop(io);
        assert_eq!(state.extension_states.get("example.message").expect("old live state").0.revision, source_head.revision,
            "failed component copy never installs its partial fields");
        assert_eq!(state.events.front(), Some(&source_head), "failed apply preserves the old physical event");
        assert_eq!(state.revision(), source_head.revision);
        assert_eq!(other_prime.receipt(), source_receipt);
        let available = allowance.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        denied.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        assert!(other_prime.admit_retained(available).is_err(), "unreplaced original cached event and buffers remain owned");
        drop(state);
        denied.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        other_prime.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_component_model_10000_real_signed_transitions_stays_live_source_bounded() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let prime = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("prime ledger");
        let (mut state, mut reference) = native_component_fixture(&prime)?;
        state.set_resident_event_limit(4)?;
        reference.set_resident_event_limit(4)?;
        state.set_resident_terminal_effect_limit(2)?;
        reference.set_resident_terminal_effect_limit(2)?;
        let owner = issuer();
        let request = request_file(br#"{"prompt":"actual provider fixture request"}"#)?;
        let result_schema = schema_file(&json!({"type":"object"}))?;
        let custom = request_file(br#"{"text":"steady active extension state"}"#)?;
        let available = original_proof_budget().peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        let mut recent = VecDeque::new();
        for index in 0_u64..10_000 {
            let mut id = [0_u8; 16];
            id[..8].copy_from_slice(&(index / 4 + 1).to_le_bytes());
            let effect_id = EffectId::from_bytes(id);
            let attempt_id = EffectAttemptId::from_bytes(id);
            let action = match index % 4 {
                0 => Action::AppendCustom {
                    schema: "example.message".into(), version: 1, content: custom.clone(),
                },
                1 => Action::PlanEffect {
                    effect_id, provider: "example.provider".into(),
                    guarantee: EffectGuarantee::AtMostOnce, effect_kind: "example.model_output".into(),
                    request: request.clone(), result_schema: result_schema.clone(),
                },
                2 => Action::MarkEffectDispatched { effect_id, attempt_id },
                _ => Action::ResolveEffect { observation: owner.attest_effect(
                    effect_id, reference.effect(effect_id).expect("real admitted provider effect"),
                    attempt_id, EffectStatus::Failed { message: "actual attested provider rejection".into() },
                    &mut None,
                )? },
            };
            let event = native_component_event(&mut reference, action)?;
            let query = NativeMaterializationWork::new(original_proof_budget(), &CancellationToken::new()).expect("independent original query ledger");
            native_component_apply(&mut state, event, &query)?;
            let receipt = query.receipt();
            assert!(receipt.allocation_operations < 512 && receipt.bytes_copied < 1_048_576,
                "one active transition must not materialize its 10k lifetime prefix");
            assert_eq!(state, reference, "canonical provider/effect state diverged at transition {index}");
            assert!(state.events.len() <= 4);
            assert!(state.effects.len() <= 3 && state.indexed_terminal_effects.len() <= 2);
            assert_eq!(state.extension_states.len(), 1);
            recent.push_back((index, query));
            if recent.len() > 16 {
                let (retired_index, retired) = recent.pop_front().expect("bounded observation window");
                if retired_index >= 64 {
                    retired.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                }
            }
        }
        assert_eq!(state.revision(), 10_001);
        let mut oldest_effect = [0_u8; 16];
        oldest_effect[..8].copy_from_slice(&1_u64.to_le_bytes());
        assert!(state.effect(EffectId::from_bytes(oldest_effect)).is_none(), "the first actual provider effect was physically retired");
        drop(state);
        for (_, work) in recent {
            work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        Ok(())
    }
