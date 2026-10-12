use super::*;

#[tokio::test]
async fn path_restore_reuses_exact_records_and_preserves_unselected_state()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("restore-paths", B, &cancel)
        .await?
        .value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::from_bytes([0x60; 16]), B, &cancel)
        .await?
        .value;
    transaction.create_dir_all("/tree", B, &cancel).await?.value;
    transaction
        .create_file(
            "/tree/tracked",
            OwnedBytes::from_static(b"committed"),
            FileMetadata::default(),
            B,
            &cancel,
        )
        .await?
        .value;
    transaction
        .hard_link("/tree/tracked", "/tree/ignored-link", B, &cancel)
        .await?
        .value;
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
    workspace.write_text("/ignored", "live").await?;
    let committed = workspace.head(B, &cancel).await?.value;
    let snapshot = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "restore-paths-snapshot",
            ForkOptions::from_generation(committed, IdempotencyKey::from_bytes([0x61; 16])),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    snapshot.remove("/ignored").await?;
    let source = snapshot.head(B, &cancel).await?.value;

    workspace.write_text("/tree/tracked", "dirty").await?;
    workspace.write_text("/ignored", "still-live").await?;
    let dirty = workspace.head(B, &cancel).await?.value;
    let outcome = workspace
        .restore_paths_from(
            &source,
            &["tree/tracked".to_owned()],
            dirty.id(),
            IdempotencyKey::from_bytes([0x62; 16]),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?
        .value;
    assert!(matches!(outcome, TransactionCommit::Committed(_)));
    assert_eq!(
        workspace.read("/tree/tracked", 64, B, &cancel).await?.value,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace
            .read("/tree/ignored-link", 64, B, &cancel)
            .await?
            .value,
        OwnedBytes::from_static(b"committed")
    );
    assert_eq!(
        workspace.read("/ignored", 64, B, &cancel).await?.value,
        OwnedBytes::from_static(b"still-live")
    );
    let tracked = workspace.stat("/tree/tracked", B, &cancel).await?.value;
    let linked = workspace
        .stat("/tree/ignored-link", B, &cancel)
        .await?
        .value;
    assert_eq!(tracked.file_id, linked.file_id);
    assert_eq!(tracked.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn path_apply_is_three_way_conflict_checked() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("apply-paths", B, &cancel).await?.value;
    workspace.write_text("/file", "base").await?;
    let base = workspace.head(B, &cancel).await?.value;
    let source_workspace = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "apply-paths-source",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::from_bytes([0x63; 16])),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    source_workspace.write_text("/file", "source").await?;
    let source = source_workspace.head(B, &cancel).await?.value;
    let current = workspace.head(B, &cancel).await?.value;
    assert!(matches!(
        workspace
            .apply_paths_from(
                Some(&base),
                Some(&source),
                &["file".to_owned()],
                current.id(),
                IdempotencyKey::from_bytes([0x64; 16]),
                PublicationPermit::Unrestricted
            )
            .await?,
        WorkspacePathApply::Applied(_)
    ));
    assert_eq!(
        workspace.read("/file", 64, B, &cancel).await?.value,
        OwnedBytes::from_static(b"source")
    );

    let conflicting = fs
        .create_workspace("apply-paths-conflict", B, &cancel)
        .await?
        .value;
    conflicting.write_text("/file", "base").await?;
    let conflict_base = conflicting.head(B, &cancel).await?.value;
    let conflict_source = conflicting
        .fork::<_, _, crate::WorkspaceError>(
            "apply-paths-conflict-source",
            ForkOptions::from_generation(
                conflict_base.clone(),
                IdempotencyKey::from_bytes([0x65; 16]),
            ),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    conflict_source.write_text("/file", "source").await?;
    let conflict_source = conflict_source.head(B, &cancel).await?.value;
    conflicting.write_text("/file", "ours").await?;
    let conflict_current = conflicting.head(B, &cancel).await?.value;
    let WorkspacePathApply::Conflicted(conflicts) = conflicting
        .apply_paths_from(
            Some(&conflict_base),
            Some(&conflict_source),
            &["file".to_owned()],
            conflict_current.id(),
            IdempotencyKey::from_bytes([0x66; 16]),
            PublicationPermit::Unrestricted,
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
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("guarded-selected-restore", B, &cancel)
        .await?
        .value;
    let mut seed = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    seed.create_dir_all("/tree", B, &cancel).await?.value;
    seed.write(
        "/tree/file",
        bytes::Bytes::from_static(b"source"),
        B,
        &cancel,
    )
    .await?
    .value;
    seed.hard_link("/tree/file", "/tree/alias", B, &cancel)
        .await?
        .value;
    seed.set_metadata(
        "/tree/file",
        FileMetadata {
            posix_mode: MetadataField::Value(0o640),
            ..FileMetadata::default()
        },
        B,
        &cancel,
    )
    .await?
    .value;
    seed.write("/outside", bytes::Bytes::from_static(b"base"), B, &cancel)
        .await?
        .value;
    seed.commit(
        None,
        crate::PublicationPermit::Unrestricted,
        B,
        &cancel,
        |_| async {
            Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        },
    )
    .await?
    .value;
    let source = workspace.head(B, &cancel).await?.value;
    let source_metadata = source.stat("/tree/file", B, &cancel).await?.value.metadata;
    let mut live = workspace
        .begin_transaction(IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    live.write(
        "/tree/file",
        bytes::Bytes::from_static(b"dirty"),
        B,
        &cancel,
    )
    .await?
    .value;
    live.write(
        "/removed",
        bytes::Bytes::from_static(b"remove me"),
        B,
        &cancel,
    )
    .await?
    .value;
    live.write("/outside", bytes::Bytes::from_static(b"user"), B, &cancel)
        .await?
        .value;
    live.commit(
        None,
        crate::PublicationPermit::Unrestricted,
        B,
        &cancel,
        |_| async {
            Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        },
    )
    .await?
    .value;
    let dirty = workspace.head(B, &cancel).await?.value;
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
    assert_eq!(workspace.head(B, &cancel).await?.value.id(), dirty.id());
    assert_eq!(
        workspace
            .read("/tree/file", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"dirty"
    );
    assert_eq!(
        workspace
            .read("/removed", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"remove me"
    );
    assert_eq!(
        workspace
            .read("/outside", 32, B, &cancel)
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
    assert_eq!(workspace.head(B, &cancel).await?.value.id(), dirty.id());
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
            .read("/tree/file", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"source"
    );
    assert_eq!(
        workspace
            .read("/tree/alias", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"source"
    );
    assert_eq!(
        workspace
            .stat("/tree/file", B, &cancel)
            .await?
            .value
            .metadata,
        source_metadata
    );
    assert_eq!(
        workspace
            .stat("/tree/file", B, &cancel)
            .await?
            .value
            .file_id,
        workspace
            .stat("/tree/alias", B, &cancel)
            .await?
            .value
            .file_id
    );
    assert!(matches!(
        workspace
            .read("/removed", 32, B, &cancel)
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error),
        Err(WorkspaceError::NotFound)
    ));
    assert_eq!(
        workspace
            .read("/outside", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"user"
    );
    assert_eq!(
        source
            .read("/tree/file", 32, B, &cancel)
            .await?
            .value
            .as_ref(),
        b"source"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_selected_restore_honors_the_real_retained_authority_reservation()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("native-selected-reservation", B, &cancel)
        .await?
        .value;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"frozen"))
        .await?;
    let frozen = workspace.head(B, &cancel).await?.value;
    workspace
        .write("/selected", bytes::Bytes::from_static(b"current"))
        .await?;
    let current = workspace.head(B, &cancel).await?.value;
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
        .restore_paths_from_with_native_work(
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
    assert_eq!(workspace.head(B, &cancel).await?.value.id(), current.id());
    assert_eq!(
        workspace.read("/selected", 7, B, &cancel).await?.value,
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
        .restore_paths_from_with_native_work(
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
        workspace.read("/selected", 6, B, &cancel).await?.value,
        OwnedBytes::from_static(b"frozen")
    );
    workspace
        .release_publication(reservation, &original, &cancellation)
        .await?;
    original.receipt().verify(selected_transaction_budget())?;
    Ok(())
}
