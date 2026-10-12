use super::*;

#[tokio::test]
async fn conditional_remove_preserves_typed_stale_identity() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("conditional-remove", B, &cancel)
        .await?
        .value;
    let mut create = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        create
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>
            )
            .await?
            .value,
        TransactionCommit::Committed(_)
    ));
    let actual = workspace.stat("/file.txt", B, &cancel).await?.value.file_id;
    let mut stale = FileId::new();
    while stale == actual {
        stale = FileId::new();
    }
    let mut remove = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    assert!(matches!(
        remove.remove_if("/file.txt", stale).await,
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(
        workspace
            .read("/file.txt", 64, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"current"
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_restore_is_fenced_and_idempotent() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("restore-exact", B, &cancel)
        .await?
        .value;
    workspace.write_text("/state", "first").await?;
    let first = workspace.head(B, &cancel).await?.value;
    workspace.write_text("/state", "second").await?;
    let second = workspace.head(B, &cancel).await?.value;
    let key = IdempotencyKey::from_bytes([0x51; 16]);

    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key, PublicationPermit::Unrestricted).await?,
        WorkspaceRestore::Restored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key, PublicationPermit::Unrestricted).await?,
        WorkspaceRestore::AlreadyRestored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, first.id(), key, PublicationPermit::Unrestricted)
            .await?,
        WorkspaceRestore::IdempotencyConflict
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, second.id(), IdempotencyKey::new(), PublicationPermit::Unrestricted)
            .await?,
        WorkspaceRestore::Stale(ref generation) if generation.id() == first.id()
    ));
    assert_eq!(workspace.head(B, &cancel).await?.value.id(), first.id());
    Ok(())
}

mod directories;
mod selected;
