use super::*;

#[tokio::test]
async fn conditional_remove_preserves_typed_stale_identity() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("conditional-remove"))?;
    let mut create = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        commit(&mut create).await?,
        TransactionCommit::Committed(_)
    ));
    let actual = unbounded!(workspace, stat("/file.txt"))?.file_id;
    let mut stale = FileId::new();
    while stale == actual {
        stale = FileId::new();
    }
    let mut remove = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    assert!(matches!(
        unbounded!(remove, remove_if("/file.txt", stale)),
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file.txt", 64))?.as_ref(),
        b"current"
    );
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("repo"))?;
    let key = IdempotencyKey::new();
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(transaction, create_dir_all("/output/nested"))?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        unbounded!(workspace, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );
    let reopened = unbounded!(workspace, generation(generation.id()))?;
    assert_eq!(
        unbounded!(reopened, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );
    let fork = fork_workspace(
        &workspace,
        "copy",
        ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
    )
    .await?;
    assert_eq!(
        unbounded!(fork, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );

    let mut exact_retry = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(exact_retry, create_dir_all("/output/nested"))?;
    exact_retry
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        value!(exact_retry.commit::<WorkspaceError, _, _>(
            None, crate::PublicationPermit::Unrestricted, selected_transaction_budget(),
            &CancellationToken::new(), |_| std::future::ready(Err(crate::OperationFailure::before_work(WorkspaceError::from(FsError::MutationNotAllowed))))
        ).await)?,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut conflicting_retry = unbounded!(workspace, begin_transaction(key))?;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        commit(&mut conflicting_retry).await?,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("transaction-rebase"))?;

    let mut first = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut disjoint = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        commit(&mut first).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut disjoint).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        disjoint.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        commit(&mut disjoint).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/first", 8))?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        unbounded!(workspace, read("/second", 8))?,
        OwnedBytes::from_static(b"two")
    );

    let mut winner = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut overlap = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        commit(&mut winner).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut overlap).await?,
        TransactionCommit::Conflict { .. }
    ));
    let TransactionRebase::Conflicted {
        conflicts,
        truncated,
    } = overlap.rebase(16).await?
    else {
        return Err("overlapping mutation rebased unsafely".into());
    };
    assert!(!conflicts.is_empty());
    assert!(!truncated);
    assert_eq!(
        unbounded!(workspace, read("/first", 8))?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("exact-transaction"))?;
    let base = unbounded!(workspace, head())?;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction_at(&base, IdempotencyKey::new())
    )?;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        transaction.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/upstream", 8))?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        unbounded!(workspace, read("/local", 8))?,
        OwnedBytes::from_static(b"two")
    );

    let other = unbounded!(fs, create_workspace("other-exact-transaction"))?;
    let foreign = unbounded!(other, head())?;
    assert!(matches!(
        unbounded!(
            workspace,
            begin_transaction_at(&foreign, IdempotencyKey::new())
        ),
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("shapes"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_directory("/tree"))?;
    unbounded!(
        transaction,
        write("/tree/source", OwnedBytes::from_static(b"abcdef"))
    )?;
    unbounded!(
        transaction,
        write("/tree/destination", OwnedBytes::from_static(b"......"))
    )?;
    unbounded!(
        transaction,
        create_symbolic_link("/tree/symlink", OwnedBytes::from_static(b"source"))
    )?;
    unbounded!(transaction, hard_link("/tree/source", "/tree/hard-link"))?;
    unbounded!(
        transaction,
        write_range("/tree/source", 1, OwnedBytes::from_static(b"Z"))
    )?;
    transaction
        .zero_range(
            "/tree/source",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
        )
        .await?;
    transaction
        .preallocate(
            "/tree/source",
            crate::ByteRange {
                offset: 0,
                length: 6,
            },
            true,
        )
        .await?;
    transaction
        .clone_range("/tree/source", 0, "/tree/destination", 1, 5)
        .await?;
    unbounded!(
        transaction,
        set_metadata("/tree/source", FileMetadata::default())
    )?;
    transaction.resize("/tree/destination", 8).await?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    let source = OwnedBytes::from_static(b"aZ\0\0ef");
    assert_eq!(unbounded!(workspace, read("/tree/source", 32))?, source);
    assert_eq!(unbounded!(workspace, read("/tree/hard-link", 32))?, source);
    assert_eq!(
        unbounded!(workspace, read("/tree/destination", 32))?,
        OwnedBytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}
