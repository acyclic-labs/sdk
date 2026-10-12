use super::*;

#[tokio::test]
async fn public_workspace_checkout_opens_pinned_and_live_private_views()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("public-checkout", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let head = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let pinned = workspace.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(pinned.generation_id(), head.id());
    let live_private = workspace.checkout(GenerationSelector::Head, CheckoutMode {
                access: crate::model::AccessMode::ReadWrite,
                consistency: crate::model::ConsistencyMode::Live,
                mutations: crate::model::MutationMode::PrivateOverlay,
            }, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(live_private.generation_id(), head.id());
    Ok(())
}

#[tokio::test]
async fn workspace_forks_an_unpublished_checkpoint_without_mutating_its_source()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source = fs.create_workspace("checkpoint-source", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = source.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
    let unpublished = source.generation(checkpoint, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let child = source
        .fork(
            "checkpoint-child",
            ForkOptions::from_generation(unpublished, IdempotencyKey::from_bytes([0x44; 16])),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    assert_eq!(
        child.read("/candidate-only", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"candidate")
    );
    assert!(source.read("/candidate-only", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_dir_all("/output/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        workspace.read("/output/nested/status", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ready")
    );
    let reopened = workspace.generation(generation.id(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(
        reopened.read("/output/nested/status", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ready")
    );
    let fork = workspace
        .fork(
            "copy",
            ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
        crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED })))
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(
        fork.read("/output/nested/status", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ready")
    );

    let mut exact_retry = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    exact_retry.create_dir_all("/output/nested", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    exact_retry
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        exact_retry.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut conflicting_retry = workspace.begin_transaction(key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        conflicting_retry.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("transaction-rebase", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    let mut first = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut disjoint = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        first.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        disjoint.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        disjoint.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        disjoint.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/first", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/second", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"two")
    );

    let mut winner = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut overlap = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        winner.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        overlap.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
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
        workspace.read("/first", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = fs.create_workspace("exact-transaction", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let base = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = workspace
        .begin_transaction_at(&base, IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        transaction.rebase(16).await?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/upstream", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/local", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"two")
    );

    let other = fs.create_workspace("other-exact-transaction", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let foreign = other.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        workspace
            .begin_transaction_at(&foreign, IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error),
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("shapes", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_directory("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/source", bytes::Bytes::from_static(b"abcdef"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/destination", bytes::Bytes::from_static(b"......"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .create_symbolic_link("/tree/symlink", bytes::Bytes::from_static(b"source"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .hard_link("/tree/source", "/tree/hard-link", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write_range("/tree/source", 1, bytes::Bytes::from_static(b"Z"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
    transaction
        .set_metadata("/tree/source", FileMetadata::default(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.resize("/tree/destination", 8).await?;
    assert!(matches!(
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionCommit::Committed(_)
    ));
    let source = OwnedBytes::from_static(b"aZ\0\0ef");
    assert_eq!(workspace.read("/tree/source", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, source);
    assert_eq!(workspace.read("/tree/hard-link", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, source);
    assert_eq!(
        workspace.read("/tree/destination", 32, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("read-shapes", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.create_directory("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/a", bytes::Bytes::from_static(b"abcdef"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .write("/tree/sparse", bytes::Bytes::from(vec![1; 300]), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.hard_link("/tree/a", "/tree/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .create_symbolic_link("/tree/link", bytes::Bytes::from_static(b"a"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .zero_range(
            "/tree/a",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
        )
        .await?;
    transaction.resize("/tree/a", 8).await?;
    transaction
        .zero_range(
            "/tree/sparse",
            crate::ByteRange {
                offset: 100,
                length: 20,
            },
            true,
            false,
        )
        .await?;
    transaction.resize("/tree/sparse", 400).await?;
    let TransactionCommit::Committed(exact) = transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? else {
        return Err("shape publication did not commit".into());
    };

    let file_id = workspace.stat("/tree/a", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id;
    let directory_id = workspace.stat("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.file_id;
    let missing_id = FileId::from_bytes([0xff; 16]);
    let paths = exact
        .paths_for_file_ids([file_id, directory_id, missing_id], 32)
        .await?;
    assert_eq!(
        paths.get(&file_id),
        Some(&vec!["/tree/a".to_owned(), "/tree/b".to_owned()])
    );
    assert_eq!(paths.get(&directory_id), Some(&vec!["/tree".to_owned()]));
    assert!(!paths.contains_key(&missing_id));

    assert_eq!(
        workspace.read_range("/tree/a", 1, 5, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map(|chunk| chunk.bytes)?,
        OwnedBytes::from_static(b"b\0\0ef")
    );
    let a = workspace.stat("/tree/a", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let b = workspace.stat("/tree/b", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        workspace.read_symbolic_link("/tree/link", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map(|chunk| chunk.bytes)?,
        OwnedBytes::from_static(b"a")
    );

    let listing = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let first = listing.list_directory("/tree", None, 1, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value.0).map_err(|failure| failure.error)?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    workspace.write_text("/tree/late", "new").await?;
    let second = listing.list_directory("/tree", Some(&first.entries[0].name), 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value.0).map_err(|failure| failure.error)?;
    assert_eq!(second.entries.len(), 3);
    assert!(!second.has_more);

    let extents = workspace.plan_extents("/tree/sparse", 0, 400, 16).await?;
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::AllocatedZero)
    );
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::Content)
    );

    workspace.write_text("/tree/a", "changed").await?;
    assert_eq!(
        exact.read("/tree/a", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        workspace.read("/tree/a", 16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        OwnedBytes::from_static(b"changed")
    );
    Ok(())
}
