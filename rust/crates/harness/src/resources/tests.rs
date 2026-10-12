use super::*;

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
