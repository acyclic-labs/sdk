use super::*;

#[tokio::test]
async fn selected_record_restore_prepares_file_to_directory_privately_before_one_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = unbounded!(fs, create_workspace("source-selected-directory-shape"))?;
    let mut source_tx = unbounded!(source_workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(source_tx, create_dir_all("/hot/empty/nested"))?;
    unbounded!(
        source_tx,
        set_metadata(
            "/hot",
            FileMetadata {
                posix_mode: MetadataField::Value(0o750),
                ..FileMetadata::default()
            }
        )
    )?;
    unbounded!(
        source_tx,
        write("/hot/value", bytes::Bytes::from_static(b"selected source"))
    )?;
    unbounded!(source_tx, hard_link("/hot/value", "/hot/alias"))?;
    commit(&mut source_tx).await?;
    let source = unbounded!(source_workspace, head())?;
    let workspace = unbounded!(fs, create_workspace("target-selected-directory-shape"))?;
    let mut target_tx = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        target_tx,
        write("/hot", bytes::Bytes::from_static(b"original file"))
    )?;
    unbounded!(target_tx, hard_link("/hot", "/outside-linked"))?;
    unbounded!(
        target_tx,
        write("/outside", bytes::Bytes::from_static(b"user"))
    )?;
    commit(&mut target_tx).await?;
    let original = unbounded!(workspace, head())?;
    let paths = vec![
        "hot".to_owned(),
        "hot/empty".to_owned(),
        "hot/empty/nested".to_owned(),
        "hot/value".to_owned(),
        "hot/alias".to_owned(),
    ];
    let budget = selected_transaction_budget();
    let cancellation = CancellationToken::new();
    let rejected = workspace
        .restore_selected_paths_from(
            &source,
            &paths,
            original.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| async {
                Err(crate::OperationFailure::before_work(
                    FsError::MutationNotAllowed,
                ))
            },
        )
        .await;
    let Err(rejected) = rejected else {
        panic!("owner rejection must prevent the only publication");
    };
    rejected.work.verify(budget)?;

    assert_eq!(unbounded!(workspace, head())?.id(), original.id());
    assert_eq!(
        unbounded!(workspace, read("/hot", 32))?.as_ref(),
        b"original file"
    );
    assert_eq!(
        unbounded!(workspace, stat("/hot"))?.file_id,
        unbounded!(workspace, stat("/outside-linked"))?.file_id
    );
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let original_id = original.id();
    let restored = workspace
        .restore_selected_paths_from(
            &source,
            &paths,
            original.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            move |allowance| {
                let volume = volume.clone();
                let cancellation = guard_cancellation.clone();
                async move {
                    let checked = volume
                        .checkout(
                            GenerationSelector::Exact(original_id),
                            CheckoutMode::read_only_pinned(),
                            allowance,
                            &cancellation,
                        )
                        .await?;
                    drop(checked.value);
                    Ok(crate::OperationReceipt {
                        value: (),
                        work: checked.work,
                    })
                }
            },
        )
        .await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(
        unbounded!(workspace, stat("/hot"))?.metadata.posix_mode,
        Some(0o750)
    );
    assert_eq!(
        unbounded!(workspace, stat("/hot/empty/nested"))?.kind,
        FileKind::Directory
    );
    assert!(
        unbounded!(workspace, list_directory("/hot/empty/nested", None, 1))?
            .entries
            .is_empty()
    );
    assert_eq!(
        unbounded!(workspace, read("/hot/value", 32))?.as_ref(),
        b"selected source"
    );
    assert_eq!(
        unbounded!(workspace, read("/hot/alias", 32))?.as_ref(),
        b"selected source"
    );
    assert_eq!(
        unbounded!(workspace, stat("/hot/value"))?.file_id,
        unbounded!(workspace, stat("/hot/alias"))?.file_id
    );
    assert_eq!(
        unbounded!(workspace, read("/outside-linked", 32))?.as_ref(),
        b"original file"
    );
    assert_eq!(
        unbounded!(workspace, stat("/outside-linked"))?.link_count,
        1
    );
    assert_eq!(
        unbounded!(workspace, read("/outside", 32))?.as_ref(),
        b"user"
    );
    Ok(())
}

#[tokio::test]
async fn selected_record_restore_removes_directory_descendants_before_file_replacement()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = unbounded!(fs, create_workspace("source-selected-file-shape"))?;
    source_workspace
        .write("/hot", bytes::Bytes::from_static(b"replacement file"))
        .await?;
    let source = unbounded!(source_workspace, head())?;
    let workspace = unbounded!(fs, create_workspace("target-selected-file-shape"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, create_dir_all("/hot/nested/empty"))?;
    unbounded!(
        seed,
        write(
            "/hot/nested/old",
            bytes::Bytes::from_static(b"retained outside alias")
        )
    )?;
    unbounded!(seed, hard_link("/hot/nested/old", "/outside-linked"))?;
    unbounded!(
        seed,
        write("/hot/top", bytes::Bytes::from_static(b"old top"))
    )?;
    unbounded!(seed, write("/outside", bytes::Bytes::from_static(b"user")))?;
    commit(&mut seed).await?;
    let original = unbounded!(workspace, head())?;
    let paths = vec![
        "hot/nested/empty".to_owned(),
        "hot/nested/old".to_owned(),
        "hot/nested".to_owned(),
        "hot/top".to_owned(),
        "hot".to_owned(),
    ];
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let rejected = workspace
        .restore_selected_paths_from(
            &source,
            &paths,
            original.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| async {
                Err(crate::OperationFailure::before_work(
                    FsError::MutationNotAllowed,
                ))
            },
        )
        .await;
    let Err(rejected) = rejected else {
        panic!("owner rejection must prevent subtree replacement");
    };
    rejected.work.verify(budget)?;
    assert_eq!(unbounded!(workspace, head())?.id(), original.id());
    assert_eq!(
        unbounded!(workspace, read("/hot/nested/old", 64))?.as_ref(),
        b"retained outside alias"
    );
    assert_eq!(
        unbounded!(workspace, stat("/outside-linked"))?.link_count,
        2
    );
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let original_id = original.id();
    let restored = workspace
        .restore_selected_paths_from(
            &source,
            &paths,
            original.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            move |allowance| {
                let volume = volume.clone();
                let cancellation = guard_cancellation.clone();
                async move {
                    let checked = volume
                        .checkout(
                            GenerationSelector::Exact(original_id),
                            CheckoutMode::read_only_pinned(),
                            allowance,
                            &cancellation,
                        )
                        .await?;
                    drop(checked.value);
                    Ok(crate::OperationReceipt {
                        value: (),
                        work: checked.work,
                    })
                }
            },
        )
        .await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(
        unbounded!(workspace, read("/hot", 64))?.as_ref(),
        b"replacement file"
    );
    assert_eq!(unbounded!(workspace, stat("/hot"))?.kind, FileKind::Regular);
    assert_eq!(
        unbounded!(workspace, read("/outside-linked", 64))?.as_ref(),
        b"retained outside alias"
    );
    assert_eq!(
        unbounded!(workspace, stat("/outside-linked"))?.link_count,
        1
    );
    assert_eq!(
        unbounded!(workspace, read("/outside", 64))?.as_ref(),
        b"user"
    );
    Ok(())
}
