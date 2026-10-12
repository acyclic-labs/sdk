use super::*;
use crate::core::tests::{command, issuer, operation, reducer_fixture, request_file};
use serde_json::json;

fn restore(
    snapshot: Snapshot,
    owner: &AuthorityIssuer,
    registry: SchemaRegistry,
) -> Result<Reducer> {
    Reducer::restore(snapshot, owner.verifier(&mut None)?, registry, &mut None)
}

#[test]
fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
    let mut reducer = reducer_fixture();
    let content = request_file(br#"{"text":"hello"}"#)?;
    let action = Action::AppendCustom {
        schema: "example.message".into(),
        version: 1,
        content,
    };
    let ApplyResult::Applied { event } = reducer.apply(command(operation(70), 0, action))? else {
        panic!("fresh append replayed")
    };
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    for other in [
        AuthorityIssuer::new("test", [8; 32], reducer.authority.clone()),
        AuthorityIssuer::new("other", [7; 32], reducer.authority.clone()),
        AuthorityIssuer::new(
            "test",
            [7; 32],
            Authority {
                kind: AggregateKind::Agent,
                id: "other-agent".into(),
            },
        ),
    ] {
        assert!(restore(snapshot.copy(&mut None)?, &other, reducer.schemas.clone()).is_err());
    }
    let mut old = snapshot.copy(&mut None)?;
    old.format_version = 2;
    assert!(restore(old, &issuer(), reducer.schemas.clone()).is_err());
    assert!(restore(snapshot.copy(&mut None)?, &issuer(), SchemaRegistry::new()).is_err());
    let verifier = issuer().verifier(&mut None)?;
    let mut missing = Reducer::new(issuer().audience, verifier, SchemaRegistry::new());
    assert!(missing.apply_committed(event.clone(), &mut None).is_err());
    for edit in 0..5 {
        let mut changed = reducer.schemas.clone();
        let binding = Arc::make_mut(changed.schemas.as_mut().unwrap())
            .get_mut(&("example.message".into(), 1))
            .unwrap();
        match edit {
            0 => binding.schema = json!({"type":"string"}),
            1 => binding.implementation_digest = [8; 32],
            2 => binding.fork_policy = ExtensionForkPolicy::Reset,
            3 => {
                binding.dependencies.insert(ExtensionDependency {
                    name: "example.dependency".into(),
                    version: 1,
                });
            }
            _ => binding.configuration_schema = Some(json!({"type":"string"})),
        }
        assert!(restore(snapshot.copy(&mut None)?, &issuer(), changed.clone()).is_err());
        if edit == 1 || edit == 2 {
            let mut candidate =
                Reducer::new(issuer().audience, issuer().verifier(&mut None)?, changed);
            assert!(candidate.apply_committed(event.clone(), &mut None).is_err());
        }
    }
    let mut exact = reducer_fixture();
    exact.apply_committed(event, &mut None)?;
    assert!(
        exact
            .schemas
            .register(
                "example.message",
                1,
                json!({"type":"object"}),
                [8; 32],
                ExtensionForkPolicy::Inherit,
                [],
                None
            )
            .is_err()
    );
    let mut expanded = reducer.schemas.clone();
    expanded.register(
        "example.additional",
        1,
        json!({"type":"string"}),
        [9; 32],
        ExtensionForkPolicy::Reset,
        [],
        None,
    )?;
    let restored = restore(snapshot, &issuer(), expanded.clone())?;
    assert_eq!(restored.schemas, expanded);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_original_cold_decoder_preserves_digest_and_issuer_mac_rejection() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork};
    let budget = acyclic_fs::WorkBudget {
        allocation_operations: 4096,
        peak_allocation_bytes: 4_194_304,
        bytes_copied: 4_194_304,
        bytes_encoded: 4_194_304,
        bytes_hashed: 4_194_304,
        source_bytes_read: 4_194_304,
        items_examined: 4_194_304,
        ..Default::default()
    };
    let mut reducer = reducer_fixture();
    for (index, to) in [LifecycleState::Active, LifecycleState::Completed]
        .into_iter()
        .enumerate()
    {
        let reason = Some(format!(
            "{index}:{}",
            "owned canonical lifecycle reason".repeat(128)
        ));
        let action = Action::TransitionLifecycle { to, reason };
        reducer.apply(command(operation(index as u8 + 1), index as u64, action))?;
    }
    let original = reducer.snapshot(usize::MAX, &mut None)?.0;
    let available =
        budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
    for kind in 0..3 {
        let mut forged = original.copy(&mut None)?;
        if kind == 0 {
            forged.attestation[0] ^= 1;
        } else {
            forged.projection.lifecycle = LifecycleState::Pending;
            if kind == 1 {
                forged.state_digest = forged.digest_with_work(&mut None)?;
            }
        }
        let bytes = crate::contract::canonical_json_bytes(&forged)?;
        let source = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(Error::from)?;
        let io = crate::filesystem::PreparationIo::from_native_work(&source)?;
        let decoded = Snapshot::decode(&bytes, 4_194_304, &mut Some(&mut io.history_io()))?;
        drop(io);
        let io = crate::filesystem::PreparationIo::from_native_work(&source)?;
        let result = Reducer::restore(
            decoded,
            issuer().verifier(&mut Some(&mut io.history_io()))?,
            reducer.schemas.clone(),
            &mut Some(&mut io.history_io()),
        );
        if kind == 2 {
            assert!(matches!(result, Err(Error::Invalid(_))));
        } else {
            assert!(matches!(result, Err(Error::Unauthorized(_))));
        }
        drop(io);
        source.admit_retained(available).map_err(Error::from)?;
    }
    Ok(())
}
