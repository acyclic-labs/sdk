use super::*;

#[tokio::test]
async fn path_restore_reuses_exact_records_and_preserves_unselected_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("restore-paths"))?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([0x60; 16]))
    )?;
    unbounded!(transaction, create_dir_all("/tree"))?;
    unbounded!(
        transaction,
        create_file(
            "/tree/tracked",
            bytes::Bytes::from_static(b"committed"),
            FileMetadata::default()
        )
    )?;
    unbounded!(
        transaction,
        hard_link("/tree/tracked", "/tree/ignored-link")
    )?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    workspace.write_text("/ignored", "live").await?;
    let committed = unbounded!(workspace, head())?;
    let snapshot = fork_workspace(
        &workspace,
        "restore-paths-snapshot",
        ForkOptions::from_generation(committed, IdempotencyKey::from_bytes([0x61; 16])),
    )
    .await?;
    snapshot.remove("/ignored").await?;
    let source = unbounded!(snapshot, head())?;

    workspace.write_text("/tree/tracked", "dirty").await?;
    workspace.write_text("/ignored", "still-live").await?;
    let dirty = unbounded!(workspace, head())?;
    let outcome = value!(
        workspace
            .restore_paths_from(
                &source,
                &["tree/tracked".to_owned()],
                dirty.id(),
                IdempotencyKey::from_bytes([0x62; 16]),
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
    )?;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    assert_eq!(
        unbounded!(workspace, read("/tree/tracked", 64))?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        unbounded!(workspace, read("/tree/ignored-link", 64))?,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        unbounded!(workspace, read("/ignored", 64))?,
        OwnedBytes::from_static(b"still-live")
    );
    let tracked = unbounded!(workspace, stat("/tree/tracked"))?;
    let linked = unbounded!(workspace, stat("/tree/ignored-link"))?;
    assert_eq!(tracked.file_id, linked.file_id);
    assert_eq!(tracked.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn path_apply_is_three_way_conflict_checked() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("apply-paths"))?;
    workspace.write_text("/file", "base").await?;
    let base = unbounded!(workspace, head())?;
    let source_workspace = fork_workspace(
        &workspace,
        "apply-paths-source",
        ForkOptions::from_generation(base.clone(), IdempotencyKey::from_bytes([0x63; 16])),
    )
    .await?;
    source_workspace.write_text("/file", "source").await?;
    let source = unbounded!(source_workspace, head())?;
    let current = unbounded!(workspace, head())?;
    assert!(matches!(
        workspace
            .apply_paths_from(
                Some(&base),
                Some(&source),
                &["file".to_owned()],
                current.id(),
                IdempotencyKey::from_bytes([0x64; 16]),
                crate::PublicationPermit::Unrestricted,
            )
            .await?,
        WorkspacePathApply::Applied(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file", 64))?,
        OwnedBytes::from_static(b"source")
    );

    let conflicting = unbounded!(fs, create_workspace("apply-paths-conflict"))?;
    conflicting.write_text("/file", "base").await?;
    let conflict_base = unbounded!(conflicting, head())?;
    let conflict_source = fork_workspace(
        &conflicting,
        "apply-paths-conflict-source",
        ForkOptions::from_generation(
            conflict_base.clone(),
            IdempotencyKey::from_bytes([0x65; 16]),
        ),
    )
    .await?;
    conflict_source.write_text("/file", "source").await?;
    let conflict_source = unbounded!(conflict_source, head())?;
    conflicting.write_text("/file", "ours").await?;
    let conflict_current = unbounded!(conflicting, head())?;
    let WorkspacePathApply::Conflicted(conflicts) = conflicting
        .apply_paths_from(
            Some(&conflict_base),
            Some(&conflict_source),
            &["file".to_owned()],
            conflict_current.id(),
            IdempotencyKey::from_bytes([0x66; 16]),
            crate::PublicationPermit::Unrestricted,
        )
        .await?
    else {
        return Err("overlapping path application did not conflict".into());
    };
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].path, "file");
    assert_eq!(conflicts[0].kind, ConflictKind::Binary);
    Ok(())
}

#[tokio::test]
async fn selected_restore_guard_failure_retains_original_head_and_exact_outside_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("guarded-selected-restore"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, create_dir_all("/tree"))?;
    unbounded!(
        seed,
        write("/tree/file", bytes::Bytes::from_static(b"source"))
    )?;
    unbounded!(seed, hard_link("/tree/file", "/tree/alias"))?;
    unbounded!(
        seed,
        set_metadata(
            "/tree/file",
            FileMetadata {
                posix_mode: MetadataField::Value(0o640),
                ..FileMetadata::default()
            }
        )
    )?;
    unbounded!(seed, write("/outside", bytes::Bytes::from_static(b"base")))?;
    commit(&mut seed).await?;
    let source = unbounded!(workspace, head())?;
    let source_metadata = unbounded!(source, stat("/tree/file"))?.metadata;
    let mut live = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        live,
        write("/tree/file", bytes::Bytes::from_static(b"dirty"))
    )?;
    unbounded!(
        live,
        write("/removed", bytes::Bytes::from_static(b"remove me"))
    )?;
    unbounded!(live, write("/outside", bytes::Bytes::from_static(b"user")))?;
    commit(&mut live).await?;
    let dirty = unbounded!(workspace, head())?;
    let selected = vec!["tree/file".to_owned(), "removed".to_owned()];
    let budget = selected_transaction_budget();
    let key = IdempotencyKey::new();
    let cancelled = CancellationToken::new();
    let volume = workspace.volume.clone();
    let source_id = source.id();
    let guard_cancellation = cancelled.clone();
    let failed = workspace
        .restore_paths_from(
            &source,
            &selected,
            dirty.id(),
            key,
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancelled,
            move |allowance| {
                let volume = volume.clone();
                let cancellation = guard_cancellation.clone();
                async move {
                    let checked = volume
                        .checkout(
                            GenerationSelector::Exact(source_id),
                            CheckoutMode::read_only_pinned(),
                            allowance,
                            &cancellation,
                        )
                        .await?;
                    drop(checked.value);
                    cancellation.cancel();
                    Err(crate::OperationFailure::new(
                        FsError::Cancelled(crate::CancellationError),
                        checked.work,
                    ))
                }
            },
        )
        .await;
    let Err(failed) = failed else {
        panic!("original seal guard must reject");
    };
    assert!(matches!(failed.error, WorkspaceError::Cancelled(_)));

    assert!(failed.work.bytes_copied > 0);
    failed.work.verify(budget)?;
    assert_eq!(unbounded!(workspace, head())?.id(), dirty.id());
    assert_eq!(
        unbounded!(workspace, read("/tree/file", 32))?.as_ref(),
        b"dirty"
    );
    assert_eq!(
        unbounded!(workspace, read("/removed", 32))?.as_ref(),
        b"remove me"
    );
    assert_eq!(
        unbounded!(workspace, read("/outside", 32))?.as_ref(),
        b"user"
    );

    let cancellation = CancellationToken::new();
    let conflicted = workspace
        .restore_paths_from(
            &source,
            &selected,
            source.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    conflicted.work.verify(budget)?;
    assert!(
        matches!(&conflicted.value, TransactionCommit::Conflict { actual } if actual.id() == dirty.id())
    );
    assert_eq!(unbounded!(workspace, head())?.id(), dirty.id());
    let volume = workspace.volume.clone();
    let guard_cancellation = cancellation.clone();
    let restored = workspace
        .restore_paths_from(
            &source,
            &selected,
            dirty.id(),
            key,
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            move |allowance| {
                let volume = volume.clone();
                let cancellation = guard_cancellation.clone();
                async move {
                    let checked = volume
                        .checkout(
                            GenerationSelector::Exact(source_id),
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
        unbounded!(workspace, read("/tree/file", 32))?.as_ref(),
        b"source"
    );
    assert_eq!(
        unbounded!(workspace, read("/tree/alias", 32))?.as_ref(),
        b"source"
    );
    assert_eq!(
        unbounded!(workspace, stat("/tree/file"))?.metadata,
        source_metadata
    );
    assert_eq!(
        unbounded!(workspace, stat("/tree/file"))?.file_id,
        unbounded!(workspace, stat("/tree/alias"))?.file_id
    );
    assert!(matches!(
        unbounded!(workspace, read("/removed", 32)),
        Err(WorkspaceError::NotFound)
    ));
    assert_eq!(
        unbounded!(workspace, read("/outside", 32))?.as_ref(),
        b"user"
    );
    assert_eq!(
        unbounded!(source, read("/tree/file", 32))?.as_ref(),
        b"source"
    );
    Ok(())
}

#[tokio::test]
async fn measured_directory_record_restore_preserves_original_ignored_descendants()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("record-not-subtree-restore"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, create_dir_all("/tree"))?;
    unbounded!(
        seed,
        set_metadata(
            "/tree",
            FileMetadata {
                posix_mode: MetadataField::Value(0o700),
                ..FileMetadata::default()
            }
        )
    )?;
    unbounded!(
        seed,
        write("/tree/tracked", bytes::Bytes::from_static(b"base"))
    )?;
    commit(&mut seed).await?;
    let source = unbounded!(workspace, head())?;
    let mut live = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        live,
        set_metadata(
            "/tree",
            FileMetadata {
                posix_mode: MetadataField::Value(0o755),
                ..FileMetadata::default()
            }
        )
    )?;
    unbounded!(
        live,
        write("/tree/tracked", bytes::Bytes::from_static(b"dirty"))
    )?;
    unbounded!(
        live,
        write("/tree/ignored", bytes::Bytes::from_static(b"live ignored"))
    )?;
    commit(&mut live).await?;
    let current = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let budget = selected_transaction_budget();
    let restored = workspace
        .restore_paths_from(
            &source,
            &["tree".to_owned()],
            current.id(),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    restored.work.verify(budget)?;
    assert!(matches!(restored.value, TransactionCommit::Committed(_)));
    assert_eq!(
        unbounded!(workspace, stat("/tree"))?.metadata.posix_mode,
        Some(0o700)
    );
    assert_eq!(
        unbounded!(workspace, read("/tree/tracked", 32))?.as_ref(),
        b"dirty"
    );
    assert_eq!(
        unbounded!(workspace, read("/tree/ignored", 32))?.as_ref(),
        b"live ignored"
    );
    Ok(())
}
