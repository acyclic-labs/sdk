use super::*;

#[tokio::test]
async fn conditional_remove_preserves_typed_stale_identity() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "conditional-remove",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut create = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        create
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::default(),
                        },
                    )
                }
            )
            .await?
            .value,
        TransactionCommit::Committed(_)
    ));
    let actual = workspace
        .stat(
            "/file.txt",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .file_id;
    let mut stale = FileId::new();
    while stale == actual {
        stale = FileId::new();
    }
    let mut remove = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert!(matches!(
        remove.remove_if("/file.txt", stale).await,
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(
        workspace
            .read(
                "/file.txt",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"current"
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_restore_is_fenced_and_idempotent() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "restore-exact",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/state", "first").await?;
    let first = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/state", "second").await?;
    let second = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let key = IdempotencyKey::from_bytes([0x51; 16]);

    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key, crate::PublicationPermit::Unrestricted).await?,
        WorkspaceRestore::Restored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace.restore_generation(&first, second.id(), key, crate::PublicationPermit::Unrestricted).await?,
        WorkspaceRestore::AlreadyRestored(ref generation) if generation.id() == first.id()
    ));
    assert!(matches!(
        workspace
            .restore_generation(
                &second,
                first.id(),
                key,
                crate::PublicationPermit::Unrestricted
            )
            .await?,
        WorkspaceRestore::IdempotencyConflict
    ));
    assert!(matches!(
        workspace
            .restore_generation(&second, second.id(), IdempotencyKey::new(), crate::PublicationPermit::Unrestricted)
            .await?,
        WorkspaceRestore::Stale(ref generation) if generation.id() == first.id()
    ));
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        first.id()
    );
    Ok(())
}

#[tokio::test]
async fn path_restore_reuses_exact_records_and_preserves_unselected_state()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "restore-paths",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([0x60; 16]),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    transaction
        .create_dir_all(
            "/tree",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    transaction
        .create_file(
            "/tree/tracked",
            bytes::Bytes::from_static(b"committed"),
            FileMetadata::default(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    transaction
        .hard_link(
            "/tree/tracked",
            "/tree/ignored-link",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert!(matches!(
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::default(),
                        },
                    )
                }
            )
            .await?
            .value,
        TransactionCommit::Committed(_)
    ));
    workspace.write_text("/ignored", "live").await?;
    let committed = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let snapshot = workspace
        .fork(
            "restore-paths-snapshot",
            ForkOptions::from_generation(committed, IdempotencyKey::from_bytes([0x61; 16])),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    snapshot.remove("/ignored").await?;
    let source = snapshot
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;

    workspace.write_text("/tree/tracked", "dirty").await?;
    workspace.write_text("/ignored", "still-live").await?;
    let dirty = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let outcome = workspace
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
        .await?
        .value;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    assert_eq!(
        workspace
            .read(
                "/tree/tracked",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace
            .read(
                "/tree/ignored-link",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace
            .read(
                "/ignored",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value,
        OwnedBytes::from_static(b"still-live")
    );
    let tracked = workspace
        .stat(
            "/tree/tracked",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let linked = workspace
        .stat(
            "/tree/ignored-link",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert_eq!(tracked.file_id, linked.file_id);
    assert_eq!(tracked.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn path_apply_is_three_way_conflict_checked() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "apply-paths",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/file", "base").await?;
    let base = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let source_workspace = workspace
        .fork(
            "apply-paths-source",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::from_bytes([0x63; 16])),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    source_workspace.write_text("/file", "source").await?;
    let source = source_workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let current = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
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
        workspace
            .read(
                "/file",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value,
        OwnedBytes::from_static(b"source")
    );

    let conflicting = fs
        .create_workspace(
            "apply-paths-conflict",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    conflicting.write_text("/file", "base").await?;
    let conflict_base = conflicting
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let conflict_source = conflicting
        .fork(
            "apply-paths-conflict-source",
            ForkOptions::from_generation(
                conflict_base.clone(),
                IdempotencyKey::from_bytes([0x65; 16]),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    conflict_source.write_text("/file", "source").await?;
    let conflict_source = conflict_source
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    conflicting.write_text("/file", "ours").await?;
    let conflict_current = conflicting
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
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
    let workspace = fs
        .create_workspace(
            "guarded-selected-restore",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut seed = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    seed.create_dir_all(
        "/tree",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.write(
        "/tree/file",
        bytes::Bytes::from_static(b"source"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.hard_link(
        "/tree/file",
        "/tree/alias",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.set_metadata(
        "/tree/file",
        FileMetadata {
            posix_mode: MetadataField::Value(0o640),
            ..FileMetadata::default()
        },
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.write(
        "/outside",
        bytes::Bytes::from_static(b"base"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.commit(
        None,
        crate::PublicationPermit::Unrestricted,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
        |_| async {
            Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        },
    )
    .await?
    .value;
    let source = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let source_metadata = source
        .stat(
            "/tree/file",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .metadata;
    let mut live = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    live.write(
        "/tree/file",
        bytes::Bytes::from_static(b"dirty"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    live.write(
        "/removed",
        bytes::Bytes::from_static(b"remove me"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    live.write(
        "/outside",
        bytes::Bytes::from_static(b"user"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    live.commit(
        None,
        crate::PublicationPermit::Unrestricted,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
        |_| async {
            Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        },
    )
    .await?
    .value;
    let dirty = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
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
    assert!(failed.work.allocation_operations > 0);
    assert!(failed.work.bytes_copied > 0);
    failed.work.verify(budget)?;
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        dirty.id()
    );
    assert_eq!(
        workspace
            .read(
                "/tree/file",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"dirty"
    );
    assert_eq!(
        workspace
            .read(
                "/removed",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"remove me"
    );
    assert_eq!(
        workspace
            .read(
                "/outside",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
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
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        dirty.id()
    );
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
        workspace
            .read(
                "/tree/file",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"source"
    );
    assert_eq!(
        workspace
            .read(
                "/tree/alias",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"source"
    );
    assert_eq!(
        workspace
            .stat(
                "/tree/file",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .metadata,
        source_metadata
    );
    assert_eq!(
        workspace
            .stat(
                "/tree/file",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .file_id,
        workspace
            .stat(
                "/tree/alias",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .file_id
    );
    assert!(matches!(
        workspace
            .read(
                "/removed",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error),
        Err(WorkspaceError::NotFound)
    ));
    assert_eq!(
        workspace
            .read(
                "/outside",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"user"
    );
    assert_eq!(
        source
            .read(
                "/tree/file",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"source"
    );
    Ok(())
}

#[tokio::test]
async fn measured_directory_record_restore_preserves_original_ignored_descendants()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "record-not-subtree-restore",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut seed = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    seed.create_dir_all(
        "/tree",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.set_metadata(
        "/tree",
        FileMetadata {
            posix_mode: MetadataField::Value(0o700),
            ..FileMetadata::default()
        },
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.write(
        "/tree/tracked",
        bytes::Bytes::from_static(b"base"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.commit(
        None,
        crate::PublicationPermit::Unrestricted,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
        |_| async {
            Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        },
    )
    .await?
    .value;
    let source = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut live = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    live.set_metadata(
        "/tree",
        FileMetadata {
            posix_mode: MetadataField::Value(0o755),
            ..FileMetadata::default()
        },
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    live.write(
        "/tree/tracked",
        bytes::Bytes::from_static(b"dirty"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    live.write(
        "/tree/ignored",
        bytes::Bytes::from_static(b"live ignored"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    live.commit(
        None,
        crate::PublicationPermit::Unrestricted,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
        |_| async {
            Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        },
    )
    .await?
    .value;
    let current = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
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
        workspace
            .stat(
                "/tree",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .metadata
            .posix_mode,
        Some(0o700)
    );
    assert_eq!(
        workspace
            .read(
                "/tree/tracked",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"dirty"
    );
    assert_eq!(
        workspace
            .read(
                "/tree/ignored",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"live ignored"
    );
    Ok(())
}

#[tokio::test]
async fn selected_record_restore_prepares_file_to_directory_privately_before_one_seal()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = fs
        .create_workspace(
            "source-selected-directory-shape",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut source_tx = source_workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    source_tx
        .create_dir_all(
            "/hot/empty/nested",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    source_tx
        .set_metadata(
            "/hot",
            FileMetadata {
                posix_mode: MetadataField::Value(0o750),
                ..FileMetadata::default()
            },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    source_tx
        .write(
            "/hot/value",
            bytes::Bytes::from_static(b"selected source"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    source_tx
        .hard_link(
            "/hot/value",
            "/hot/alias",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    source_tx
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await?
        .value;
    let source = source_workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let workspace = fs
        .create_workspace(
            "target-selected-directory-shape",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut target_tx = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    target_tx
        .write(
            "/hot",
            bytes::Bytes::from_static(b"original file"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    target_tx
        .hard_link(
            "/hot",
            "/outside-linked",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    target_tx
        .write(
            "/outside",
            bytes::Bytes::from_static(b"user"),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    target_tx
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                })
            },
        )
        .await?
        .value;
    let original = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
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
    assert!(rejected.work.allocation_operations > 0);
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        original.id()
    );
    assert_eq!(
        workspace
            .read(
                "/hot",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"original file"
    );
    assert_eq!(
        workspace
            .stat(
                "/hot",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .file_id,
        workspace
            .stat(
                "/outside-linked",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .file_id
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
        workspace
            .stat(
                "/hot",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .metadata
            .posix_mode,
        Some(0o750)
    );
    assert_eq!(
        workspace
            .stat(
                "/hot/empty/nested",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .kind,
        FileKind::Directory
    );
    assert!(
        workspace
            .list_directory(
                "/hot/empty/nested",
                None,
                1,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value.0)
            .map_err(|failure| failure.error)?
            .entries
            .is_empty()
    );
    assert_eq!(
        workspace
            .read(
                "/hot/value",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"selected source"
    );
    assert_eq!(
        workspace
            .read(
                "/hot/alias",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"selected source"
    );
    assert_eq!(
        workspace
            .stat(
                "/hot/value",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .file_id,
        workspace
            .stat(
                "/hot/alias",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .file_id
    );
    assert_eq!(
        workspace
            .read(
                "/outside-linked",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"original file"
    );
    assert_eq!(
        workspace
            .stat(
                "/outside-linked",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .link_count,
        1
    );
    assert_eq!(
        workspace
            .read(
                "/outside",
                32,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"user"
    );
    Ok(())
}

#[tokio::test]
async fn selected_record_restore_removes_directory_descendants_before_file_replacement()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source_workspace = fs
        .create_workspace(
            "source-selected-file-shape",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    source_workspace
        .write("/hot", bytes::Bytes::from_static(b"replacement file"))
        .await?;
    let source = source_workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let workspace = fs
        .create_workspace(
            "target-selected-file-shape",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let mut seed = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    seed.create_dir_all(
        "/hot/nested/empty",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.write(
        "/hot/nested/old",
        bytes::Bytes::from_static(b"retained outside alias"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.hard_link(
        "/hot/nested/old",
        "/outside-linked",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.write(
        "/hot/top",
        bytes::Bytes::from_static(b"old top"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.write(
        "/outside",
        bytes::Bytes::from_static(b"user"),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    seed.commit(
        None,
        crate::PublicationPermit::Unrestricted,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
        |_| async {
            Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        },
    )
    .await?
    .value;
    let original = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
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
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        original.id()
    );
    assert_eq!(
        workspace
            .read(
                "/hot/nested/old",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"retained outside alias"
    );
    assert_eq!(
        workspace
            .stat(
                "/outside-linked",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .link_count,
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
        workspace
            .read(
                "/hot",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"replacement file"
    );
    assert_eq!(
        workspace
            .stat(
                "/hot",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .kind,
        FileKind::Regular
    );
    assert_eq!(
        workspace
            .read(
                "/outside-linked",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"retained outside alias"
    );
    assert_eq!(
        workspace
            .stat(
                "/outside-linked",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .link_count,
        1
    );
    assert_eq!(
        workspace
            .read(
                "/outside",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"user"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_selected_restore_honors_the_real_retained_authority_reservation()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "native-selected-reservation",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"frozen"))
        .await?;
    let frozen = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"current"))
        .await?;
    let current = workspace
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
    let reservation = workspace
        .reserve_publication(
            IdempotencyKey::new().operation_id(),
            &original,
            &cancellation,
        )
        .await?;
    let before = original.receipt();
    let key = IdempotencyKey::new();
    let paths = vec!["/selected".to_owned()];
    let mut allow =
        |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
    let denied = workspace
        .restore_selected_paths_from_with_permit_guarded_with_native_work(
            &frozen,
            &paths,
            current.id(),
            key,
            crate::PublicationPermit::Unrestricted,
            &original,
            &cancellation,
            &mut allow,
        )
        .await;
    assert!(!matches!(
        denied,
        Ok(TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_))
    ));
    assert_eq!(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .id(),
        current.id()
    );
    assert_eq!(
        workspace
            .read(
                "/selected",
                7,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value,
        OwnedBytes::from_static(b"current")
    );
    assert_eq!(
        original.receipt().authority_records_appended,
        before.authority_records_appended
    );
    let permit = crate::PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    };
    let restored = workspace
        .restore_selected_paths_from_with_permit_guarded_with_native_work(
            &frozen,
            &paths,
            current.id(),
            key,
            permit,
            &original,
            &cancellation,
            &mut allow,
        )
        .await?;
    assert!(matches!(
        restored,
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_)
    ));
    assert_eq!(
        workspace
            .read(
                "/selected",
                6,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value,
        OwnedBytes::from_static(b"frozen")
    );
    workspace
        .release_publication(reservation, &original, &cancellation)
        .await?;
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}
