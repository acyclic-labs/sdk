use super::*;
use serde_json::json;

fn fixture() -> (Reducer, AuthorityIssuer) {
    let owner = AuthorityIssuer::new(
        "checkpoint-owner",
        [23; 32],
        Authority {
            kind: AggregateKind::Agent,
            id: "checkpoint-agent".into(),
        },
    );
    (
        Reducer::new(
            owner.audience.clone(),
            owner.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
        owner,
    )
}

#[test]
fn recomputing_public_digest_cannot_authenticate_forged_projection() -> Result<()> {
    let (reducer, owner) = fixture();
    let original = reducer.snapshot(usize::MAX, &mut None)?.0;
    let mut forged = original.copy(&mut None)?;
    forged.projection.lifecycle = LifecycleState::Completed;
    forged.state_digest = forged.digest_with_work(&mut None)?;
    assert!(matches!(
        Reducer::restore(
            forged,
            owner.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            &mut None
        ),
        Err(Error::Unauthorized(_))
    ));
    let mut cache = original;
    cache.revision = 50;
    cache.state_digest = cache.digest_with_work(&mut None)?;
    assert!(matches!(
        Reducer::restore(
            cache,
            owner.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            &mut None
        ),
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[test]
fn checkpoint_proof_binds_issuer_key_identity_audience_and_format() -> Result<()> {
    let (reducer, owner) = fixture();
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    for other in [
        AuthorityIssuer::new("checkpoint-owner", [24; 32], reducer.authority.clone()),
        AuthorityIssuer::new("other-owner", [23; 32], reducer.authority),
        AuthorityIssuer::new(
            "checkpoint-owner",
            [23; 32],
            Authority {
                kind: AggregateKind::Agent,
                id: "other-agent".into(),
            },
        ),
    ] {
        assert!(matches!(
            Reducer::restore(
                snapshot.copy(&mut None)?,
                other.verifier(&mut None).expect("verifier copy"),
                SchemaRegistry::new(),
                &mut None
            ),
            Err(Error::Unauthorized(_))
        ));
    }
    let mut old = snapshot;
    old.format_version = 2;
    assert!(matches!(
        Reducer::restore(
            old,
            owner.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            &mut None
        ),
        Err(Error::Unsupported(_))
    ));
    Ok(())
}

#[test]
fn restore_pins_complete_installed_bindings_and_allows_new_namespaces() -> Result<()> {
    let (mut reducer, owner) = fixture();
    reducer.schemas.register(
        "example.pinned",
        1,
        json!({"type":"object"}),
        [8; 32],
        ExtensionForkPolicy::Inherit,
        [],
        Some(json!({"type":"object"})),
    )?;
    let snapshot = reducer.snapshot(usize::MAX, &mut None)?.0;
    assert!(matches!(
        Reducer::restore(
            snapshot.copy(&mut None)?,
            owner.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            &mut None
        ),
        Err(Error::Unsupported(_))
    ));
    for edit in 0..5 {
        let mut changed = reducer.schemas.clone();
        let binding = Arc::make_mut(changed.schemas.as_mut().expect("fixture registry"))
            .get_mut(&("example.pinned".into(), 1))
            .expect("fixture binding");
        match edit {
            0 => binding.schema = json!({"type":"string"}),
            1 => binding.implementation_digest = [9; 32],
            2 => binding.fork_policy = ExtensionForkPolicy::Reset,
            3 => {
                binding.dependencies.insert(ExtensionDependency {
                    name: "example.dependency".into(),
                    version: 1,
                });
            }
            _ => binding.configuration_schema = Some(json!({"type":"string"})),
        }
        assert!(matches!(
            Reducer::restore(
                snapshot.copy(&mut None)?,
                owner.verifier(&mut None).expect("verifier copy"),
                changed,
                &mut None
            ),
            Err(Error::Conflict(_))
        ));
    }
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
    let restored = Reducer::restore(
        snapshot,
        owner.verifier(&mut None).expect("verifier copy"),
        expanded.clone(),
        &mut None,
    )?;
    reducer.schemas = expanded;
    assert_eq!(restored, reducer);
    Ok(())
}
