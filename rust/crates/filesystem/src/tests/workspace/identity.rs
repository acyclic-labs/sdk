use super::*;

#[test]
fn identity_is_deterministic_and_namespace_scoped() -> Result<(), Box<dyn Error>> {
    let name = value!(WorkspaceName::new(
        "repo",
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ))?;
    let first = value!(WorkspaceId::derive(
        [1; 16],
        &name,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ))?;
    let view = first.identity(b"account")?;
    assert_eq!(view.id().as_bytes(), first.into_bytes());
    // A name-based projection would erase the existing deployment namespace.
    assert_ne!(view.id().as_bytes(), name.as_str().as_bytes());
    let (account, id) =
        acyclic_auth::addressing::ResourceIdentity::decode(&view.encode())?.into_parts();
    assert_eq!(account.into_bytes(), b"account");
    assert_eq!(WorkspaceId::from_bytes(id.as_bytes().try_into()?), first);
    assert_eq!(
        first,
        value!(WorkspaceId::derive(
            [1; 16],
            &name,
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        ))?
    );
    assert_ne!(
        first,
        value!(WorkspaceId::derive(
            [2; 16],
            &name,
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        ))?
    );
    assert_ne!(
        first,
        value!(WorkspaceId::derive(
            [1; 16],
            &value!(WorkspaceName::new(
                "other",
                &crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            ))?,
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        ))?
    );
    Ok(())
}
