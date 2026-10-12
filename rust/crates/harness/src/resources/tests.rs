use super::*;

#[test]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_provider_creation_preserves_policy_and_partial_copy_custody() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget {
        items_examined: 256,
        allocation_operations: 3,
        bytes_copied: 64,
        peak_allocation_bytes: 256,
        ..acyclic_fs::WorkBudget::default()
    };
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .expect("original provider ledger");
    let (provider, custody) =
        ProviderRef::new_with_native_work("acyclic", "filesystem", "2", &work)?;
    assert_eq!(
        provider,
        ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?
    );
    drop(provider);
    drop(custody);
    drop(
        work.claim(
            budget.peak_allocation_bytes,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )
        .expect("all original provider custody retired"),
    );
    let invalid = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .expect("invalid provider ledger");
    assert_eq!(
        ProviderRef::new_with_native_work("acyclic", "file\nsystem", "2", &invalid)
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Invalid
    );
    let denied = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            bytes_copied: 7,
            ..budget
        },
        &cancellation,
    )
    .expect("partial provider ledger");
    assert!(ProviderRef::new_with_native_work("acyclic", "filesystem", "2", &denied).is_err());
    drop(
        denied
            .claim(
                budget.peak_allocation_bytes,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .expect("partial field copies physically retired"),
    );
    Ok(())
}

#[test]
fn references_are_typed_bounded_and_provider_owned() -> Result<()> {
    let provider = ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?;
    let workspace = WorkspaceRef::new(provider.clone(), [7; 16].into(), None)?;
    let generation = GenerationRef::new(&(provider), &([8; 32]), Some("immutable"), None)?.0;
    assert_eq!(workspace.as_resource().kind(), ResourceKind::Workspace);
    assert_eq!(generation.as_resource().kind(), ResourceKind::Generation);
    assert!(
        WorkspaceRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            Vec::new(),
            None
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn deserialization_cannot_bypass_reference_validation() {
    let invalid_provider = r#"{"namespace":"","family":"filesystem","version":"2"}"#;
    assert!(serde_json::from_str::<ProviderRef>(invalid_provider).is_err());

    let wrong_kind = r#"{
            "kind":"generation",
            "provider":{"namespace":"acyclic","family":"filesystem","version":"2"},
            "key":[1],
            "version":null
        }"#;
    assert!(serde_json::from_str::<WorkspaceRef>(wrong_kind).is_err());

    let unknown_field = r#"{
            "kind":"workspace",
            "provider":{"namespace":"acyclic","family":"filesystem","version":"2"},
            "key":[1],
            "version":null,
            "endpoint":"https://should-not-enter-a-reference.invalid"
        }"#;
    assert!(serde_json::from_str::<ResourceRef>(unknown_field).is_err());
}
