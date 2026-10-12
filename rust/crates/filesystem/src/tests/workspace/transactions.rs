use super::*;

#[tokio::test]
async fn public_workspace_checkout_opens_pinned_and_live_private_views()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("public-checkout", B, &cancel)
        .await?
        .value;
    let head = workspace.head(B, &cancel).await?.value;
    let pinned = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(pinned.generation_id(), head.id());
    let live_private = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: crate::model::AccessMode::ReadWrite,
                consistency: crate::model::ConsistencyMode::Live,
                mutations: crate::model::MutationMode::PrivateOverlay,
            },
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(live_private.generation_id(), head.id());
    Ok(())
}

#[tokio::test]
async fn workspace_forks_an_unpublished_checkpoint_without_mutating_its_source()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let source = fs
        .create_workspace("checkpoint-source", B, &cancel)
        .await?
        .value;
    let mut transaction = source
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .write_text("/candidate-only", "candidate")
        .await?;
    let checkpoint = transaction.checkout.checkpoint(B, &cancel).await?.value;
    let unpublished = source.generation(checkpoint, B, &cancel).await?.value;
    let child = source
        .fork(
            "checkpoint-child",
            ForkOptions::from_generation(unpublished, IdempotencyKey::from_bytes([0x44; 16])),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;

    assert_eq!(
        child.read("/candidate-only", 64, B, &cancel).await?.value,
        OwnedBytes::from_static(b"candidate")
    );
    assert!(
        source
            .read("/candidate-only", 64, B, &cancel)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("repo", B, &cancel).await?.value;
    let key = IdempotencyKey::new();
    let mut transaction = workspace.begin_transaction(key, B, &cancel).await?.value;
    transaction
        .create_dir_all("/output/nested", B, &cancel)
        .await?
        .value;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = transaction
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
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        workspace
            .read("/output/nested/status", 64, B, &cancel)
            .await?
            .value,
        OwnedBytes::from_static(b"ready")
    );
    let reopened = workspace
        .generation(generation.id(), B, &cancel)
        .await?
        .value;
    assert_eq!(
        reopened
            .read("/output/nested/status", 64, B, &cancel)
            .await?
            .value,
        OwnedBytes::from_static(b"ready")
    );
    let fork = workspace
        .fork(
            "copy",
            ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    assert_eq!(
        fork.read("/output/nested/status", 64, B, &cancel)
            .await?
            .value,
        OwnedBytes::from_static(b"ready")
    );

    let mut exact_retry = workspace.begin_transaction(key, B, &cancel).await?.value;
    exact_retry
        .create_dir_all("/output/nested", B, &cancel)
        .await?
        .value;
    exact_retry
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        exact_retry.commit(None, PublicationPermit::Unrestricted, B, &cancel, allow::<WorkspaceError>).await?.value,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut conflicting_retry = workspace.begin_transaction(key, B, &cancel).await?.value;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        conflicting_retry
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>
            )
            .await?
            .value,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("transaction-rebase", B, &cancel)
        .await?
        .value;

    let mut first = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    let mut disjoint = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        first
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
    assert!(matches!(
        disjoint
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>
            )
            .await?
            .value,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        disjoint.rebase(16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        disjoint
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
    assert_eq!(
        workspace.read("/first", 8, B, &cancel).await?.value,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/second", 8, B, &cancel).await?.value,
        OwnedBytes::from_static(b"two")
    );

    let mut winner = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    let mut overlap = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        winner
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
    assert!(matches!(
        overlap
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>
            )
            .await?
            .value,
        TransactionCommit::Conflict { .. }
    ));
    let TransactionRebase::Conflicted {
        conflicts,
        truncated,
    } = overlap.rebase(16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
    else {
        return Err("overlapping mutation rebased unsafely".into());
    };
    assert!(!conflicts.is_empty());
    assert!(!truncated);
    assert_eq!(
        workspace.read("/first", 8, B, &cancel).await?.value,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("exact-transaction", B, &cancel)
        .await?
        .value;
    let base = workspace.head(B, &cancel).await?.value;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = workspace
        .begin_transaction_at(&base, IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>
            )
            .await?
            .value,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        transaction.rebase(16, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        transaction
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
    assert_eq!(
        workspace.read("/upstream", 8, B, &cancel).await?.value,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        workspace.read("/local", 8, B, &cancel).await?.value,
        OwnedBytes::from_static(b"two")
    );

    let other = fs
        .create_workspace("other-exact-transaction", B, &cancel)
        .await?
        .value;
    let foreign = other.head(B, &cancel).await?.value;
    assert!(matches!(
        workspace
            .begin_transaction_at(&foreign, IdempotencyKey::new(), B, &cancel)
            .await,
        Err(OperationFailure {
            error: WorkspaceError::ForeignGeneration,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("shapes", B, &cancel).await?.value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .create_directory("/tree", B, &cancel)
        .await?
        .value;
    transaction
        .write(
            "/tree/source",
            OwnedBytes::from_static(b"abcdef"),
            B,
            &cancel,
        )
        .await?;
    transaction
        .write(
            "/tree/destination",
            OwnedBytes::from_static(b"......"),
            B,
            &cancel,
        )
        .await?;
    transaction
        .create_symbolic_link(
            "/tree/symlink",
            OwnedBytes::from_static(b"source"),
            B,
            &cancel,
        )
        .await?
        .value;
    transaction
        .hard_link("/tree/source", "/tree/hard-link", B, &cancel)
        .await?
        .value;
    transaction
        .write_range("/tree/source", 1, OwnedBytes::from_static(b"Z"), B, &cancel)
        .await?
        .value;
    transaction
        .zero_range(
            "/tree/source",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
         crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .preallocate(
            "/tree/source",
            crate::ByteRange {
                offset: 0,
                length: 6,
            },
            true,
         crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .clone_range("/tree/source", 0, "/tree/destination", 1, 5, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .set_metadata("/tree/source", FileMetadata::default(), B, &cancel)
        .await?
        .value;
    transaction.resize("/tree/destination", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    assert!(matches!(
        transaction
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
    let source = OwnedBytes::from_static(b"aZ\0\0ef");
    assert_eq!(
        workspace.read("/tree/source", 32, B, &cancel).await?.value,
        source
    );
    assert_eq!(
        workspace
            .read("/tree/hard-link", 32, B, &cancel)
            .await?
            .value,
        source
    );
    assert_eq!(
        workspace
            .read("/tree/destination", 32, B, &cancel)
            .await?
            .value,
        OwnedBytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("read-shapes", B, &cancel).await?.value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction
        .create_directory("/tree", B, &cancel)
        .await?
        .value;
    transaction
        .write("/tree/a", OwnedBytes::from_static(b"abcdef"), B, &cancel)
        .await?;
    transaction
        .write("/tree/sparse", OwnedBytes::from(vec![1; 300]), B, &cancel)
        .await?;
    transaction
        .hard_link("/tree/a", "/tree/b", B, &cancel)
        .await?
        .value;
    transaction
        .create_symbolic_link("/tree/link", OwnedBytes::from_static(b"a"), B, &cancel)
        .await?
        .value;
    transaction
        .zero_range(
            "/tree/a",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
         crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.resize("/tree/a", 8, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction
        .zero_range(
            "/tree/sparse",
            crate::ByteRange {
                offset: 100,
                length: 20,
            },
            true,
            false,
         crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    transaction.resize("/tree/sparse", 400, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let TransactionCommit::Committed(exact) = transaction
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
        return Err("shape publication did not commit".into());
    };

    let file_id = workspace.stat("/tree/a", B, &cancel).await?.value.file_id;
    let directory_id = workspace.stat("/tree", B, &cancel).await?.value.file_id;
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
        workspace
            .read_range("/tree/a", 1, 5, B, &cancel)
            .await?
            .value
            .bytes,
        OwnedBytes::from_static(b"b\0\0ef")
    );
    let a = workspace.stat("/tree/a", B, &cancel).await?.value;
    let b = workspace.stat("/tree/b", B, &cancel).await?.value;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        workspace
            .read_symbolic_link("/tree/link", B, &cancel)
            .await?
            .value
            .bytes,
        OwnedBytes::from_static(b"a")
    );

    let listing = workspace.head(B, &cancel).await?.value;
    let first = listing
        .list_directory("/tree", None, 1, B, &cancel)
        .await?
        .value
        .0;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    workspace.write_text("/tree/late", "new").await?;
    let second = listing
        .list_directory("/tree", Some(&first.entries[0].name), 16, B, &cancel)
        .await?
        .value
        .0;
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
        exact.read("/tree/a", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        workspace.read("/tree/a", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"changed")
    );
    Ok(())
}
