use super::*;
use serde_json::json;

fn fixture() -> (Reducer, AuthorityIssuer) {
    let owner = AuthorityIssuer::new("checkpoint-owner", [23; 32],
        Authority { kind: AggregateKind::Agent, id: "checkpoint-agent".into() });
    (Reducer::new(owner.audience.clone(), owner.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()), owner)
}

#[test]
fn recomputing_public_digest_cannot_authenticate_forged_projection() -> Result<()> {
    let (reducer, owner) = fixture();
    let original = reducer.snapshot(usize::MAX, &mut None)?.0;
    let mut forged = original.copy(&mut None)?; forged.projection.lifecycle = LifecycleState::Completed;
    forged.state_digest = forged.digest_with_work(&mut None)?;
    assert!(matches!(Reducer::restore(forged, owner.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()), Err(Error::Unauthorized(_))));
    let mut cache = original; cache.revision = 50; cache.state_digest = cache.digest_with_work(&mut None)?;
    assert!(matches!(Reducer::restore(cache, owner.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()), Err(Error::Unauthorized(_))));
    Ok(())
}

#[test]
fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
    let (reducer, owner) = fixture();
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    for other in [AuthorityIssuer::new("checkpoint-owner", [24; 32], reducer.authority.clone()),
        AuthorityIssuer::new("other-owner", [23; 32], reducer.authority),
        AuthorityIssuer::new("checkpoint-owner", [23; 32], Authority { kind: AggregateKind::Agent, id: "other-agent".into() })] {
        assert!(matches!(Reducer::restore(snapshot.copy(&mut None)?, other.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new()), Err(Error::Unauthorized(_))));
    }
    let mut old = snapshot; old.format_version = 2;
    assert!(matches!(Reducer::restore(old, owner.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new()), Err(Error::Unsupported(_))));
    Ok(())
}

#[test]
fn restore_pins_complete_installed_bindings_and_allows_new_namespaces() -> Result<()> {
    let (mut reducer, owner) = fixture();
    reducer.schemas.register("example.pinned", 1, json!({"type":"object"}), [8; 32], ExtensionForkPolicy::Inherit,
        [], Some(json!({"type":"object"})))?;
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    assert!(matches!(Reducer::restore(snapshot.copy(&mut None)?, owner.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new()), Err(Error::Unsupported(_))));
    for edit in 0..5 {
        let mut changed = reducer.schemas.clone();
        let binding = Arc::make_mut(changed.schemas.as_mut().expect("fixture registry"))
            .get_mut(&("example.pinned".into(), 1)).expect("fixture binding");
        match edit {
            0 => binding.schema = json!({"type":"string"}),
            1 => binding.implementation_digest = [9; 32],
            2 => binding.fork_policy = ExtensionForkPolicy::Reset,
            3 => { binding.dependencies.insert(ExtensionDependency { name: "example.dependency".into(), version: 1 }); }
            _ => binding.configuration_schema = Some(json!({"type":"string"})),
        }
        assert!(matches!(Reducer::restore(snapshot.copy(&mut None)?, owner.verifier(&mut None).expect("verifier copy"), changed), Err(Error::Conflict(_))));
    }
    let mut expanded = reducer.schemas.clone();
    expanded.register("example.additional", 1, json!({"type":"string"}), [9; 32], ExtensionForkPolicy::Reset, [], None)?;
    let restored = Reducer::restore(snapshot, owner.verifier(&mut None).expect("verifier copy"), expanded.clone())?;
    reducer.schemas = expanded;
    assert_eq!(restored, reducer);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_original_cold_decoder_preserves_digest_and_issuer_mac_rejection() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let budget = acyclic_fs::WorkBudget { allocation_operations: 4096, peak_allocation_bytes: 4_194_304,
        bytes_copied: 4_194_304, bytes_encoded: 4_194_304, bytes_hashed: 4_194_304,
        source_bytes_read: 4_194_304, items_examined: 4_194_304, ..Default::default() };
    let (mut reducer, owner) = fixture();
    for (index, to) in [LifecycleState::Active, LifecycleState::Completed].into_iter().enumerate() {
        reducer.apply(Command { operation_id: OperationId::from_bytes([index as u8 + 1; 16]),
            idempotency_key: IdempotencyKey::new(format!("snapshot-source-{index}"))?, expected_revision: index as u64,
            scope: owner.issue("snapshot-source", &Capabilities::new(["lifecycle:manage"]), None, None, &mut None).expect("scope proof"),
            causal_parent: None, action: Action::TransitionLifecycle {
                to, reason: Some(format!("{index}:{}", "owned canonical lifecycle reason".repeat(128))),
            } })?;
    }
    let original = reducer.snapshot(usize::MAX, &mut None)?.0;
    for kind in 0..3 {
        let mut forged = original.copy(&mut None)?;
        if kind == 0 { forged.attestation[0] ^= 1; }
        else {
            forged.projection.lifecycle = LifecycleState::Pending;
            if kind == 1 { forged.state_digest = forged.digest_with_work(&mut None)?; }
        }
        let bytes = crate::contract::canonical_json_bytes(&forged)?;
        let source = NativeMaterializationWork::new(budget, &CancellationToken::new()).expect("cold decoder ledger");
        let mut io = crate::store::history::native_work_io(&source).expect("decoder observer");
        let decoded = Snapshot::decode(&bytes, 4_194_304, &mut Some(&mut io))?;
        drop(io);
        let result = Reducer::restore_native(decoded, owner.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new(), &source);
        if kind == 2 { assert!(matches!(result, Err(Error::Invalid(_)))); }
        else { assert!(matches!(result, Err(Error::Unauthorized(_)))); }
        source.admit_retained(budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    }
    Ok(())
}
