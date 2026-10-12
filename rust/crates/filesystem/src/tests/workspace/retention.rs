use super::*;

#[tokio::test]
async fn checkpoint_retains_the_exact_current_generation() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", B, &cancel).await?.value;
    let checkpoint = workspace.checkpoint("baseline").await?;
    assert_eq!(checkpoint.0.as_str(), "baseline");
    assert_eq!(
        checkpoint.1.id(),
        workspace.head(B, &cancel).await?.value.id()
    );
    let retried = workspace.checkpoint("baseline").await?;
    assert_eq!(retried.1.id(), checkpoint.1.id());
    Ok(())
}

#[tokio::test]
async fn explicit_pin_is_exact_and_conflicting_reuse_fails_closed() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", B, &cancel).await?.value;
    let first = workspace.head(B, &cancel).await?.value;
    let pin = first.pin("deployment", B, &cancel).await?.value;
    assert_eq!(pin.0.as_str(), "deployment");
    assert_eq!(pin.1.id(), first.id());
    assert_eq!(
        first.pin("deployment", B, &cancel).await?.value.1.id(),
        first.id()
    );

    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction.write_text("/changed", "yes").await?;
    let TransactionCommit::Committed(second) = transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<WorkspaceError>,
        )
        .await?
        .value
    else {
        return Err("second generation did not commit".into());
    };
    assert!(matches!(
        second
            .pin("deployment", B, &cancel)
            .await
            .map_err(|failure| failure.error),
        Err(WorkspaceError::RetentionConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn deletion_is_terminal_and_does_not_invalidate_a_fork() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", B, &cancel).await?.value;
    let generation = workspace.head(B, &cancel).await?.value;
    let fork = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "survivor",
            ForkOptions::from_generation(generation, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    let key = IdempotencyKey::new();
    assert_eq!(workspace.delete(key).await?, WorkspaceDelete::Deleted);
    assert_eq!(
        workspace.delete(key).await?,
        WorkspaceDelete::AlreadyDeleted
    );
    assert_eq!(
        fs.delete_workspace("repo", key).await?,
        WorkspaceDelete::AlreadyDeleted
    );
    assert!(fs.open_workspace("repo", B, &cancel).await.is_err());
    assert!(fs.create_workspace("repo", B, &cancel).await.is_err());
    assert_eq!(fork.head(B, &cancel).await?.value.workspace_id(), fork.id());
    Ok(())
}
