use super::*;

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
        .restore_paths_from(
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
        .restore_paths_from(
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
        .restore_paths_from(
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
        .restore_paths_from(
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
