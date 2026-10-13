use super::*;

#[tokio::test]
async fn exact_generation_restore_is_fenced_and_idempotent() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("restore-exact"))?;
    workspace.write_text("/state", "first").await?;
    let first = unbounded!(workspace, head())?;
    workspace.write_text("/state", "second").await?;
    let second = unbounded!(workspace, head())?;
    let key = IdempotencyKey::from_bytes([0x51; 16]);

    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key).await?,
        WorkspaceRestore::Restored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key).await?,
        WorkspaceRestore::AlreadyRestored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, first.id(), key)
            .await?,
        WorkspaceRestore::IdempotencyConflict
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, second.id(), IdempotencyKey::new())
            .await?,
        WorkspaceRestore::Stale(ref generation) if generation.id() == first.id()
    ));
    assert_eq!(unbounded!(workspace, head())?.id(), first.id());
    Ok(())
}

#[tokio::test]
async fn public_workspace_checkout_opens_pinned_and_live_private_views()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("public-checkout"))?;
    let head = unbounded!(workspace, head())?;
    let pinned = unbounded!(
        workspace,
        checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
    )?;
    assert_eq!(pinned.generation_id(), head.id());
    let live_private = unbounded!(
        workspace,
        checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: crate::model::AccessMode::ReadWrite,
                consistency: crate::model::ConsistencyMode::Live,
                mutations: crate::model::MutationMode::PrivateOverlay,
            }
        )
    )?;
    assert_eq!(live_private.generation_id(), head.id());
    Ok(())
}

#[tokio::test]
async fn workspace_forks_an_unpublished_checkpoint_without_mutating_its_source()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source = unbounded!(fs, create_workspace("checkpoint-source"))?;
    let mut transaction = unbounded!(source, begin_transaction(IdempotencyKey::new()))?;
    transaction
        .write_text("/candidate-only", "candidate")
        .await?;
    let checkpoint = transaction
        .checkout
        .checkpoint(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let unpublished = unbounded!(source, generation(checkpoint))?;
    let child = fork_workspace(
        &source,
        "checkpoint-child",
        ForkOptions::from_generation(unpublished, IdempotencyKey::from_bytes([0x44; 16])),
    )
    .await?;

    assert_eq!(
        unbounded!(child, read("/candidate-only", 64))?,
        OwnedBytes::from_static(b"candidate")
    );
    assert!(unbounded!(source, read("/candidate-only", 64)).is_err());
    Ok(())
}

#[tokio::test]
async fn explicit_pin_is_exact_and_conflicting_reuse_fails_closed() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("repo"))?;
    let first = unbounded!(workspace, head())?;
    let pin = unbounded!(first, pin("deployment"))?;
    assert_eq!(pin.0.as_str(), "deployment");
    assert_eq!(pin.1.id(), first.id());
    assert_eq!(unbounded!(first, pin("deployment"))?.1.id(), first.id());

    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    transaction.write_text("/changed", "yes").await?;
    let TransactionCommit::Committed(second) = commit(&mut transaction).await? else {
        return Err("second generation did not commit".into());
    };
    assert!(matches!(
        unbounded!(second, pin("deployment")),
        Err(WorkspaceError::RetentionConflict)
    ));
    Ok(())
}

#[tokio::test]
async fn deletion_is_terminal_and_does_not_invalidate_a_fork() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("repo"))?;
    let generation = unbounded!(workspace, head())?;
    let fork = fork_workspace(
        &workspace,
        "survivor",
        ForkOptions::from_generation(generation, IdempotencyKey::new()),
    )
    .await?;
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
    assert!(unbounded!(fs, open_workspace("repo")).is_err());
    assert!(unbounded!(fs, create_workspace("repo")).is_err());
    assert_eq!(unbounded!(fork, head())?.workspace_id(), fork.id());
    Ok(())
}
