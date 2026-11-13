use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn original_publication_retains_both_logical_cuts_after_cleanup_and_later_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("removed"), b"source before")?;
    std::fs::write(source_root.path().join("edited"), b"before")?;
    std::fs::write(source_root.path().join("same"), b"same bytes")?;
    let view = mounted_view(source_root.path(), "original-native-cuts").await?;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let publish = async || {
        let mut acknowledged = None;
        let receipt = view
            .sync_async_with_permit_qualified_native_work_guarded_with_ack(
                crate::PublicationPermit::Unrestricted,
                &work,
                &cancellation,
                &mut acknowledged,
                None,
                |_| async { Ok::<(), MountSourceError>(()) },
            )
            .await
            .map_err(|(cause, _)| cause)?;
        assert_eq!(acknowledged, Some(receipt));
        Ok::<_, MountSourceError>(receipt)
    };
    view.write_range(&mounted(&["same"]), 0, Bytes::from_static(b"same bytes"))?;
    let unchanged = publish().await?;
    let (unchanged_before, unchanged_after) = view
        .lazy
        .original_publication_cuts(
            unchanged
                .operation()
                .ok_or("missing source promotion receipt")?,
            &mut accounting,
        )
        .await?
        .ok_or("missing source promotion cuts")?;
    assert!(
        view.lazy
            .matches_optional_content_at_snapshots(
                unchanged_before,
                unchanged_after,
                "/same",
                &mut accounting,
            )
            .await?,
        "source promotion with unchanged bytes counted as a content change"
    );
    assert_eq!(
        view.lazy
            .content_changed_at_snapshots(unchanged_before, unchanged_after, &mut accounting,)
            .await?,
        None,
        "source identity promotion invented complete alias coverage"
    );
    let created = mounted(&["created"]);
    view.create_file(&created, FileMetadata::default(), None, &work)?;
    view.write_range(&created, 0, Bytes::from_static(b"original publication"))?;
    view.remove(&mounted(&["removed"]), None, &work)?;
    view.write_range(&mounted(&["edited"]), 0, Bytes::from_static(b"after!"))?;
    view.write_range(&mounted(&["same"]), 0, Bytes::from_static(b"same bytes"))?;
    let receipt = publish().await?;
    let operation = receipt.operation().ok_or("missing native DATA operation")?;
    assert!(
        view.lazy
            .state(&mut accounting)
            .await?
            .pending_remove
            .is_none()
    );
    let cuts = view
        .lazy
        .original_publication_cuts(operation, &mut accounting)
        .await?
        .ok_or("original native cuts missing")?;
    let (before, after) = cuts;
    assert_eq!(
        view.lazy
            .content_changed_at_snapshots(before, after, &mut accounting)
            .await?,
        Some(true),
        "native logical change was not witnessed"
    );
    assert_eq!(
        view.lazy
            .content_changed_at_snapshots(after, after, &mut accounting)
            .await?,
        Some(false),
        "an identical complete logical cut was not unchanged"
    );
    assert_eq!(after.authored_generation, receipt.generation());
    for path in ["/created", "/removed", "/edited"] {
        assert!(
            !view
                .lazy
                .matches_optional_content_at_snapshots(before, after, path, &mut accounting)
                .await?,
            "original content change was lost at {path}",
        );
    }
    assert!(
        view.lazy
            .matches_optional_content_at_snapshots(before, after, "/same", &mut accounting)
            .await?,
        "source promotion with unchanged bytes counted as a content change",
    );
    assert!(
        view.lazy
            .matches_optional_content_at_snapshots(before, after, "/absent", &mut accounting)
            .await?,
    );
    assert!(
        view.lazy
            .stat_at_snapshot(before, "/created", &mut accounting)
            .await?
            .is_none()
    );
    assert!(
        view.lazy
            .stat_at_snapshot(after, "/removed", &mut accounting)
            .await?
            .is_none()
    );
    assert_eq!(
        view.lazy
            .read_at_snapshot(before, "/removed", None, 64, &mut accounting)
            .await?
            .0
            .bytes
            .as_ref(),
        b"source before"
    );
    assert_eq!(
        view.lazy
            .read_at_snapshot(after, "/created", None, 64, &mut accounting)
            .await?
            .0
            .bytes
            .as_ref(),
        b"original publication"
    );
    std::fs::remove_file(source_root.path().join("edited"))?;
    view.fence_changes()?;
    assert_eq!(
        view.lazy
            .read_at_snapshot(before, "/edited", None, 64, &mut accounting)
            .await?
            .0
            .bytes
            .as_ref(),
        b"before"
    );
    assert_eq!(
        view.lazy
            .read_at_snapshot(after, "/edited", None, 64, &mut accounting)
            .await?
            .0
            .bytes
            .as_ref(),
        b"after!"
    );
    view.write_range(&created, 0, Bytes::from_static(b"later"))?;
    let later = publish().await?;
    assert_ne!(later.generation(), receipt.generation());
    assert_eq!(
        view.lazy
            .original_publication(operation, &mut accounting)
            .await?,
        Some(receipt)
    );
    assert_eq!(
        view.lazy
            .original_publication_cuts(operation, &mut accounting)
            .await?,
        Some(cuts)
    );
    assert!(
        !view
            .lazy
            .matches_optional_content_at_snapshots(before, after, "/edited", &mut accounting)
            .await?,
        "later writes or source removal replaced original content evidence",
    );
    assert!(
        view.lazy
            .matches_optional_content_at_snapshots(after, after, "/edited", &mut accounting)
            .await?,
        "an identical immutable cut differed from itself",
    );
    assert!(
        view.lazy
            .original_publication_cuts(crate::OperationId::new(), &mut accounting)
            .await?
            .is_none()
    );
    assert!(
        publish().await?.operation().is_none(),
        "clean sync invented a DATA operation"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn caller_selected_publication_replays_a_clean_receipt_without_claiming_later_edits()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let view = mounted_view(source_root.path(), "caller-native-capture").await?;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let operation = crate::OperationId::new();
    let mut denied_acknowledgement = None;
    let denied = view
        .sync_async_with_permit_qualified_native_work_guarded_with_ack(
            crate::PublicationPermit::Unrestricted,
            &work,
            &cancellation,
            &mut denied_acknowledgement,
            Some(operation),
            |_| async { Err::<(), _>(MountSourceError::Stale) },
        )
        .await;
    assert!(denied.is_err());
    assert!(denied_acknowledgement.is_none());
    assert!(
        view.lazy
            .original_publication(operation, &mut accounting)
            .await?
            .is_none()
    );
    let capture = async |operation| {
        let mut acknowledged = None;
        let receipt = view
            .sync_async_with_permit_qualified_native_work_guarded_with_ack(
                crate::PublicationPermit::Unrestricted,
                &work,
                &cancellation,
                &mut acknowledged,
                Some(operation),
                |_| async { Ok::<(), MountSourceError>(()) },
            )
            .await
            .map_err(|(cause, _)| cause)?;
        assert_eq!(acknowledged, Some(receipt));
        Ok::<_, MountSourceError>(receipt)
    };
    let clean = capture(operation)
        .await
        .map_err(|error| format!("clean capture: {error}"))?;
    assert_eq!(clean.operation(), Some(operation));
    let (before, after) = view
        .lazy
        .original_publication_cuts(operation, &mut accounting)
        .await
        .map_err(|error| format!("clean original cuts: {error}"))?
        .ok_or("missing clean publication cuts")?;
    assert_eq!(before.overlay, after.overlay);
    assert_eq!(before.shadows, after.shadows);
    let later_path = mounted(&["later"]);
    view.create_file(&later_path, FileMetadata::default(), None, &work)?;
    view.write_range(&later_path, 0, Bytes::from_static(b"later native edit"))?;
    for (published_later, retain_later) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        if published_later {
            let later = capture(crate::OperationId::new()).await?;
            assert_ne!(later.generation(), clean.generation());
        }
        let pending = retain_later.then(crate::OperationId::new);
        if let Some(pending) = pending {
            let mut checkout = view.authored.shared_checkout().lock().await;
            checkout.retain_operation_id(pending)?;
        }
        let appended = work.receipt().authority_records_appended;
        let mut acknowledged = None;
        let replay = view
            .sync_async_with_permit_qualified_native_work_guarded_with_ack(
                crate::PublicationPermit::Unrestricted,
                &work,
                &cancellation,
                &mut acknowledged,
                Some(operation),
                |_: &crate::NativeMaterializationWork|
                    -> std::future::Ready<Result<(), MountSourceError>> {
                    panic!("settled capture replay must not seal later edits");
                },
            )
            .await
            .map_err(|(cause, _)| cause)?;
        assert_eq!(replay, clean);
        assert_eq!(acknowledged, Some(clean));
        assert_eq!(work.receipt().authority_records_appended, appended);
        if let Some(pending) = pending {
            let mut checkout = view.authored.shared_checkout().lock().await;
            assert_eq!(
                checkout.retained_operation_id(),
                pending,
                "completed replay replaced later recovery custody"
            );
            checkout.clear_retained_operation(pending);
        }
        assert!(
            view.lazy
                .stat_at_snapshot(after, "/later", &mut accounting)
                .await?
                .is_none()
        );
    }
    assert_eq!(
        view.lazy
            .read_range("/later", 0, 17, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"later native edit"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn publication_rebases_authored_records_but_rejects_overlapping_callbacks()
-> Result<(), Box<dyn std::error::Error>> {
    for overlap in [false, true] {
        let root = tempfile::tempdir()?;
        let view = mounted_view(root.path(), "publication-callback").await?;
        let work =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        let first = mounted(&["first"]);
        let second = mounted(&["second"]);
        let file = view
            .create_file(&first, FileMetadata::default(), None, &work)?
            .lookup
            .node
            .file_id;
        view.create_file(&second, FileMetadata::default(), None, &work)?;
        let mut carrier = adapter::NativeDemandCarrier::Native(&work);
        let mut candidate = view
            .authored
            .shared_checkout()
            .candidate(
                None,
                &mut carrier,
                work.cancellation(),
                Some(view.authored.cancellation()),
            )
            .await?;
        candidate
            .checkout
            .change_content(
                file,
                crate::ContentChange::Write {
                    offset: 0,
                    bytes: Bytes::from_static(b"published").into(),
                },
                crate::ContentTimes::Preserve,
                &work,
                work.cancellation(),
            )
            .await?;
        // Force the callback between staging and adoption; no scheduler race.
        view.write_range(
            if overlap { &first } else { &second },
            0,
            Bytes::from_static(b"callback"),
        )?;
        assert_ne!(
            candidate.stamp,
            view.authored.shared_checkout().ledger.stamp()
        );
        let result = view
            .install_publication(
                candidate,
                &ViewEffect {
                    nodes: vec![file],
                    ..ViewEffect::default()
                },
                &mut carrier,
                work.cancellation(),
            )
            .await;
        if overlap {
            assert!(matches!(result, Err(MountSourceError::Stale)));
            assert_eq!(view.read_range(&first, 0, 8)?.as_ref(), b"callback");
        } else {
            result?;
            assert_eq!(view.read_range(&first, 0, 9)?.as_ref(), b"published");
            assert_eq!(view.read_range(&second, 0, 8)?.as_ref(), b"callback");
        }
    }
    Ok(())
}

/// Git's `index.lock` -> `index` -> `index.lock` inside a source directory:
/// the name renamed away is created again under the operation its promotion
/// bound, and the new file never takes the renamed file's identity.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_name_renamed_away_in_a_source_directory_is_created_again()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let source_root = tempfile::tempdir()?;
    std::fs::create_dir(source_root.path().join("sub"))?;
    std::fs::write(source_root.path().join("sub").join("keep"), b"keep")?;
    let view = mounted_view(source_root.path(), "mounted-recreated-name").await?;
    let lazy = Arc::clone(&view.lazy);
    let lock = mounted(&["sub", "lock"]);
    let index = mounted(&["sub", "index"]);
    view.create_file(&lock, FileMetadata::default(), None, &work)?;
    view.write_range(&lock, 0, Bytes::from_static(b"first"))?;
    view.rename(&lock, &index, false, &work, None)?;
    view.create_file(&lock, FileMetadata::default(), None, &work)?;
    view.write_range(&lock, 0, Bytes::from_static(b"second"))?;
    let renamed = view.lookup(&index)?.ok_or("index")?.node.file_id;
    let recreated = view.lookup(&lock)?.ok_or("lock")?.node.file_id;
    assert_ne!(renamed, recreated);
    view.sync_async().await?;
    for (path, bytes) in [("/sub/index", &b"first"[..]), ("/sub/lock", &b"second"[..])] {
        let read = lazy
            .read_range(path, 0, bytes.len() as u64, &mut accounting)
            .await?;
        assert_eq!(read.bytes.as_ref(), bytes, "{path}");
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn rebound_does_not_publish_a_stale_removed_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);

    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("a"), b"old-a")?;
    std::fs::write(source_root.path().join("b"), b"old-b")?;
    let view = mounted_view(source_root.path(), "mounted-rebound-identity").await?;
    let lazy = Arc::clone(&view.lazy);
    let a = mounted(&["a"]);
    let b = mounted(&["b"]);
    let _open_a = view.open_file(&a)?;
    let _open_b = view.open_file(&b)?;
    let old_a = view.lookup(&a)?.expect("source a").node.file_id;
    let old_b = view.lookup(&b)?.expect("source b").node.file_id;
    view.remove(&a, Some(old_a), &work)?;
    view.create_file(&a, FileMetadata::default(), None, &work)?;
    view.write_range(&a, 0, Bytes::from_static(b"new-a"))?;
    view.remove(&b, Some(old_b), &work)?;
    view.sync_async().await?;
    assert_eq!(
        lazy.read_range("/a", 0, 5, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"new-a"
    );
    assert!(matches!(
        lazy.lookup("/b", &mut accounting).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn an_advance_records_only_a_moved_head_or_source() -> Result<(), Box<dyn std::error::Error>>
{
    let source_root = tempfile::tempdir()?;
    let (demand, view, recorded) = recorded_view(source_root.path()).await?;
    let changes = || recorded.0.lock().map(|positions| positions.len());
    let binding = view.binding_epoch();

    view.advance_to_head_async().await?;
    assert_eq!(changes().map_err(|_| "poisoned observer")?, 0);
    assert_eq!(
        view.binding_epoch(),
        binding,
        "an advance that moved nothing keeps every binding"
    );

    demand.invalidate();
    view.advance_to_head_async().await?;
    assert!(
        changes().map_err(|_| "poisoned observer")? > 0,
        "a rebind to a new source epoch changes the view"
    );
    assert_ne!(view.binding_epoch(), binding);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn removing_a_source_only_file_supersedes_its_cached_lookups()
-> Result<(), Box<dyn std::error::Error>> {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source_root = tempfile::tempdir()?;
    let (_, view, recorded) = recorded_view(source_root.path()).await?;

    let path = mounted(&["source-only"]);
    let stamp = view.view_stamp().ok_or("a lazy view carries stamps")?;
    let file_id = view.lookup(&path)?.ok_or("source-only file")?.node.file_id;
    assert!(view.unchanged_since(&path, Some(file_id), stamp));

    view.remove(&path, Some(file_id), &work)?;

    assert_eq!(view.lookup(&path)?, None);
    assert!(!view.unchanged_since(&path, Some(file_id), stamp));
    assert!(!view.unchanged_since(&path, None, stamp));
    assert!(!view.unchanged_since(&MountPath::root(), None, stamp));
    let positions = recorded.0.lock().map_err(|_| "poisoned observer")?.clone();
    assert!(
        positions.iter().any(|position| *position > stamp),
        "observers learn of the removal"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn manual_fsync_publishes_nothing_until_sync() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);

    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source_root = tempfile::tempdir()?;
    let view = mounted_view(source_root.path(), "manual-fsync").await?;
    let lazy = Arc::clone(&view.lazy);
    let written = mounted(&["written"]);
    view.create_file(&written, FileMetadata::default(), None, &work)?;
    view.write_range(&written, 0, Bytes::from_static(b"fsynced"))?;
    view.flush(&work)?;
    assert!(
        matches!(
            lazy.lookup("/written", &mut accounting).await,
            Err(LazyWorkspaceError::NotFound)
        ),
        "a native fsync under manual publication publishes nothing"
    );
    view.sync_async().await?;
    assert_eq!(
        lazy.read_range("/written", 0, 7, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"fsynced"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn replaced_source_identity_keeps_open_handle_and_surviving_alias()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);

    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("a"), b"source")?;
    std::fs::write(source_root.path().join("c"), b"target")?;
    std::fs::hard_link(source_root.path().join("c"), source_root.path().join("d"))?;
    let view = mounted_view_of_with_authored_work(
        Arc::new(
            native_source(
                source_root.path(),
                crate::model::FilesystemProfile::Portable,
            )
            .await?,
        ),
        "mounted-identity-replace",
        crate::MemoryLazyWorkspaceStore::default(),
        crate::VolumeConfig {
            // The final unrelated writer advances the head while the mount
            // retains its removal. This case requires disjoint composition.
            concurrency: crate::model::ConcurrencyMode::SerializedAuthority,
            ..crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral)
        },
        None,
    )
    .await?;
    let lazy = Arc::clone(&view.lazy);
    let a = mounted(&["a"]);
    let c = mounted(&["c"]);
    let d = mounted(&["d"]);
    let old = view.open_file(&c)?;
    view.rename(&a, &c, true, &work, None)?;
    old.write_range(0, Bytes::from_static(b"update"))?;
    assert_eq!(view.read_range(&c, 0, 6)?.as_ref(), b"source");
    assert_eq!(view.read_range(&d, 0, 6)?.as_ref(), b"update");
    view.sync_async().await?;
    assert_eq!(
        lazy.read_range("/c", 0, 6, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"source"
    );
    assert_eq!(
        lazy.read_range("/d", 0, 6, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"update"
    );
    old.write_range(0, Bytes::from_static(b"second!"))?;
    view.sync_async().await?;
    assert_eq!(
        lazy.read_range("/c", 0, 6, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"source"
    );
    assert_eq!(
        lazy.read_range("/d", 0, 7, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"second!"
    );
    view.allocate_range(&d, 0, 1, MountRangeAllocation::PunchHole)?;
    view.sync_async().await?;
    assert_eq!(
        lazy.read_range("/d", 0, 7, &mut accounting)
            .await?
            .bytes
            .as_ref(),
        b"\0econd!"
    );
    let current = view.open_file(&c)?;
    let current_id = current.lookup()?.node.file_id;
    view.remove(&c, Some(current_id), &work)?;
    current.write_range(0, Bytes::from_static(b"orphan"))?;
    assert_eq!(current.read_range(0, 6)?.as_ref(), b"orphan");
    let mut external = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), &work, work.cancellation())
        .await?;
    external
        .write(
            "/external",
            Bytes::from_static(b"unrelated"),
            &work,
            work.cancellation(),
        )
        .await?;
    external
        .commit_with_native_work(
            None,
            crate::PublicationPermit::Unrestricted,
            &work,
            work.cancellation(),
            &mut |_| std::future::ready(Ok::<(), crate::FsError>(())),
        )
        .await?;
    view.sync_async().await?;
    assert!(matches!(
        lazy.lookup("/c", &mut accounting).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn conflicting_mount_removal_releases_publication_intent()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);

    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"source")?;
    let view = mounted_view(source_root.path(), "conflicting-mount-removal").await?;
    let lazy = Arc::clone(&view.lazy);
    let path = mounted(&["file"]);
    let file = view.open_file(&path)?;
    view.remove(&path, Some(file.lookup()?.node.file_id), &work)?;
    let mut conflicting = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), &work, work.cancellation())
        .await?;
    conflicting
        .write(
            "/file",
            Bytes::from_static(b"external"),
            &work,
            work.cancellation(),
        )
        .await?;
    conflicting
        .commit_with_native_work(
            None,
            crate::PublicationPermit::Unrestricted,
            &work,
            work.cancellation(),
            &mut |_| std::future::ready(Ok::<(), crate::FsError>(())),
        )
        .await?;
    assert!(matches!(
        view.sync_async().await,
        Err(MountSourceError::Stale)
    ));
    assert!(
        lazy.mount_removal_publication(&mut accounting)
            .await?
            .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn remembered_resolutions_follow_every_change_to_the_view()
-> Result<(), Box<dyn std::error::Error>> {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("a"), b"source")?;
    let view = mounted_view(source_root.path(), "mounted-resolutions").await?;
    let (a, b) = (mounted(&["a"]), mounted(&["b"]));

    // Remembered answers: a source node, and an absence.
    let source = view.lookup(&a)?.ok_or("source file absent")?;
    assert_eq!(view.lookup(&a)?, Some(source));
    assert_eq!(view.lookup(&b)?, None);
    assert_eq!(view.lookup(&b)?, None);

    // A create through the view replaces the remembered absence.
    let created = view
        .create_file(&b, FileMetadata::default(), None, &work)?
        .lookup;
    assert_eq!(
        view.lookup(&b)?.map(|lookup| lookup.node.file_id),
        Some(created.node.file_id)
    );

    // A write through a handle promotes the file; its answer follows.
    view.open_file(&a)?
        .write_range(0, Bytes::from_static(b"authored-longer"))?;
    assert_eq!(
        view.lookup(&a)?.map(|lookup| lookup.node.logical_bytes),
        Some(15)
    );

    // A removal replaces the remembered answer with an absence.
    view.remove(&a, None, &work)?;
    assert_eq!(view.lookup(&a)?, None);
    assert!(matches!(
        view.open_file(&a),
        Err(MountSourceError::NotFound)
    ));

    // Names beneath a directory the source lacks are absent until created.
    let fresh = mounted(&["fresh"]);
    let inner = mounted(&["fresh", "inner"]);
    assert_eq!(view.lookup(&fresh)?, None);
    view.create_directory(&fresh, FileMetadata::default(), &work)?;
    assert_eq!(view.lookup(&inner)?, None);
    let created = view
        .create_file(&inner, FileMetadata::default(), None, &work)?
        .lookup;
    assert_eq!(
        view.lookup(&inner)?.map(|lookup| lookup.node.file_id),
        Some(created.node.file_id)
    );

    // Open the current source version after an external write.
    std::fs::write(source_root.path().join("c"), b"first")?;
    let c = mounted(&["c"]);
    assert!(view.lookup(&c)?.is_some());
    std::fs::write(source_root.path().join("c"), b"second!")?;
    assert_eq!(view.open_file(&c)?.read_range(0, 7)?.as_ref(), b"second!");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn promotion_and_attribute_reads_leave_cached_lookups_current()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    std::fs::create_dir(source_root.path().join("d"))?;
    std::fs::write(source_root.path().join("d").join("f"), b"source")?;
    std::fs::write(source_root.path().join("g"), b"other")?;
    let view = mounted_view(source_root.path(), "mounted-promotion-effect").await?;
    let directory = mounted(&["d"]);
    let file = mounted(&["d", "f"]);
    let other = mounted(&["g"]);
    // Read first so NTFS reports directory write times before sampling.
    for path in [&directory, &file, &other] {
        view.lookup(path)?;
    }
    view.fence_changes()?;
    let before = [&directory, &file, &other].map(|path| view.lookup(path));
    let stamp = view.view_stamp().ok_or("lazy view has no stamp")?;

    // Reading attributes of a source file answers from the source.
    assert!(
        view.read_attribute(
            &file,
            b"user.absent",
            &crate::NativeMaterializationWork::new(
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .map_err(|(error, _)| error)?
        )?
        .is_none()
    );
    assert!(view.list_attributes(&file, None, 8)?.names.is_empty());
    assert!(view.authored.lookup(&file)?.is_none());

    // Conservative watcher invalidations are allowed; a current answer must match.
    view.promote_locked(&file, None)?;
    assert!(view.authored.lookup(&file)?.is_some());
    let after = [&directory, &file, &other].map(|path| view.lookup(path));
    for ((path, before), after) in [&directory, &file, &other].iter().zip(before).zip(after) {
        let (before, after) = (before?, after?);
        let file_id = after.map(|lookup| lookup.node.file_id);
        assert!(
            !view.unchanged_since(path, file_id, stamp) || before == after,
            "a promotion left a current cache answer stale for {path:?}"
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn source_hard_link_open_unlink_write_preserves_surviving_alias()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);

    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    for (write_before_unlink, write_before_first_sync, promote_survivor) in [
        (false, false, false),
        (false, true, false),
        (true, true, false),
        (true, true, true),
    ] {
        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"source")?;
        std::fs::hard_link(source_root.path().join("a"), source_root.path().join("b"))?;
        let view = mounted_view(source_root.path(), "mounted-identity-unlink").await?;
        let lazy = Arc::clone(&view.lazy);
        let a = mounted(&["a"]);
        let b = mounted(&["b"]);
        let open = view.open_file(&a)?;
        let identity = open.lookup()?.node.file_id;
        assert_eq!(
            view.lookup(&b)?.expect("surviving alias").node.file_id,
            identity
        );
        if write_before_unlink {
            open.write_range(0, Bytes::from_static(b"prime!"))
                .expect("pre-unlink write");
        }
        let existing_alias = if promote_survivor {
            {
                let _mutation = view.mutation_lease(None, None)?;
                view.promote_locked(&b, None)?;
            }
            assert!(view.authored.lookup(&b)?.is_some());
            Some(view.open_file(&b)?)
        } else {
            None
        };
        view.remove(&a, Some(identity), &work)
            .expect("unlink source alias");
        if write_before_first_sync {
            open.write_range(0, Bytes::from_static(b"update"))
                .expect("post-unlink write");
        }
        let first = if write_before_first_sync {
            b"update"
        } else {
            b"source"
        };
        assert!(view.lookup(&a)?.is_none(), "removed name must stay absent");
        assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
        if let Some(alias) = &existing_alias {
            assert_eq!(alias.read_range(0, 6)?.as_ref(), first);
        }
        view.sync_async().await?;
        assert!(
            view.lookup(&a)?.is_none(),
            "sync must not restore removed name"
        );
        assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
        if promote_survivor {
            assert_eq!(view.authored.read_range(&b, 0, 6)?.as_ref(), first);
        }
        assert!(matches!(
            lazy.lookup("/a", &mut accounting).await,
            Err(LazyWorkspaceError::NotFound)
        ));
        assert_eq!(
            lazy.read_range("/b", 0, 6, &mut accounting)
                .await?
                .bytes
                .as_ref(),
            first
        );
        open.write_range(0, Bytes::from_static(b"second!"))?;
        assert_eq!(view.lookup(&b)?.expect("alias").node.logical_bytes, 7);
        assert_eq!(view.read_range(&b, 0, 7)?.as_ref(), b"second!");
        assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
        let c = mounted(&["c"]);
        let linked = view.hard_link(&b, &c, Some(identity), true, &work)?;
        let facts = linked.facts.expect("authorized hard-link facts");
        assert_eq!(
            facts.lookup.node.logical_bytes, 7,
            "hard-link reply discarded current detached file facts"
        );
        assert_eq!(facts.lookup.node.file_id, identity);
        assert_eq!(facts.lookup.metadata, open.lookup()?.metadata);
        assert_eq!(facts.lookup.node.link_count, 2);
        assert_eq!(facts.stamp, linked.stamp);
        assert_eq!(view.open_file(&c)?.read_range(0, 7)?.as_ref(), b"second!");
        view.remove(&c, Some(identity), &work)?;
        if write_before_unlink {
            view.remove(&b, Some(identity), &work)?;
            open.write_range(0, Bytes::from_static(b"orphan!"))?;
            assert_eq!(open.read_range(0, 7)?.as_ref(), b"orphan!");
            assert!(view.lookup(&b)?.is_none());
        }
        drop(existing_alias);
        drop(open);
        view.sync_async().await?;
        if write_before_unlink {
            assert!(matches!(
                lazy.lookup("/b", &mut accounting).await,
                Err(LazyWorkspaceError::NotFound)
            ));
        } else {
            assert_eq!(
                lazy.read_range("/b", 0, 7, &mut accounting)
                    .await?
                    .bytes
                    .as_ref(),
                b"second!"
            );
            assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
        }
    }
    Ok(())
}

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
async fn mounted_native_view_of<A, O, D, S>(
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    work: &crate::NativeMaterializationWork,
) -> Result<LazyMountSource<A, O, D, S>, Box<dyn std::error::Error>>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let cancellation = work.cancellation();
    let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
    let checkout = lazy
        .workspace()
        .checkout_accounted(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            &mut accounting,
            cancellation,
        )
        .await?;
    let config = checkout.volume_config();
    let root = crate::workspace::customer_path_with_accounting(
        "/",
        config,
        &mut accounting,
        cancellation,
    )?;
    let root_text = root
        .0
        .unicode_text_with_accounting(&mut accounting, cancellation)
        .map_err(crate::WorkspaceError::from)?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(authored_root) = root.1
    else {
        return Err("authored root lacks native allocation custody".into());
    };
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(lazy_root) = root_text.1
    else {
        return Err("lazy root lacks native allocation custody".into());
    };
    CallbackRuntime::create()?;
    let shared = crate::native_mount::SharedCheckout::with_publication_meter(
        checkout,
        crate::native_mount::MountPublication::Manual,
        work,
    )?;
    let authored = adapter::tests::access::bind_test_access(
        CheckoutMountSource::new_at_meter(shared, config, root.0, authored_root, work, None)?,
        vec![],
        None,
    )?
    .into_arc_meter(work)?;
    // Match production mount composition: both retained roots and the source
    // allocation belong to the original native work, not a borrowed receipt.
    let mut source_allocation = work.claim(
        std::mem::size_of::<LazyMountSource<A, O, D, S>>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    Ok(LazyMountSource::new(
        lazy,
        authored,
        root_text.0,
        adapter::NativeDemandCarrier::Native(work),
        Some((lazy_root, &mut source_allocation)),
    )?)
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread")]
async fn mounted_read_selection_rejects_other_pending_kinds_and_prepared_forks()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("removed"), b"source")?;
    let store = crate::MemoryLazyWorkspaceStore::default();
    let view = mounted_view_of(
        Arc::new(
            native_source(
                source_root.path(),
                crate::model::FilesystemProfile::Portable,
            )
            .await?,
        ),
        "pending-read-negatives",
        store.clone(),
    )
    .await?;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut accounting = crate::kernel::KernelAccounting::native(&work, &cancellation);
    let prior = view.lazy.state(&mut accounting).await?;
    let cut = view.lazy.snapshot(&mut accounting).await?;
    let operation = IdempotencyKey::new();
    view.lazy
        .prepare_mount_removals(
            &["/removed".to_owned()],
            &[],
            &[],
            operation,
            &mut accounting,
        )
        .await?;
    let staged = store
        .load_lazy_workspace(view.lazy.workspace().id(), &mut accounting)
        .await?
        .ok_or("staged removal")?;
    // This fixture supplies operation custody only to exercise rejection paths;
    // the consumer test above obtains it through real candidate installation.
    view.authored
        .shared_checkout()
        .lock()
        .await
        .retain_operation_id(operation.operation_id())?;
    let mut carrier = adapter::NativeDemandCarrier::Native(&work);
    let observation = view
        .authored
        .shared_checkout()
        .observe(
            ViewGate::callback_owner()?,
            &mut carrier,
            &cancellation,
            None,
        )
        .await?;
    let foreign = mounted_view(source_root.path(), "foreign-pending-read").await?;
    let foreign_observation = foreign
        .authored
        .shared_checkout()
        .observe(
            ViewGate::callback_owner()?,
            &mut carrier,
            &cancellation,
            None,
        )
        .await?;
    assert!(matches!(
        view.lazy
            .mounted_read_state(&foreign_observation, &mut accounting)
            .await,
        Err(LazyWorkspaceError::Filesystem(
            crate::FsError::VolumeMismatch
        ))
    ));
    let mut revision = staged.revision;
    for case in [
        "source-only",
        "authored",
        "overlay",
        "fork",
        "source",
        "epoch",
    ] {
        let mut next = staged.clone();
        next.revision = revision + 1;
        match case {
            "source-only" => {
                next.pending_remove.as_mut().ok_or("pending")?.kind =
                    crate::lazy_workspace::PendingLazyRemoveKind::SourceOnly
            }
            "authored" => {
                next.pending_remove.as_mut().ok_or("pending")?.kind =
                    crate::lazy_workspace::PendingLazyRemoveKind::Authored {
                        idempotency_key: operation,
                    }
            }
            "overlay" => {
                next.pending_remove
                    .as_mut()
                    .ok_or("pending")?
                    .prepared_overlay = prior.overlay
            }
            "fork" => {
                next.fork_origin = Some(crate::lazy_workspace::LazyForkOrigin {
                    operation_id: crate::OperationId::new(),
                    snapshot: cut,
                    config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                    phase: crate::lazy_workspace::LazyForkPhase::Prepared,
                })
            }
            "source" => next.source.identity[0] ^= 1,
            _ => next.source.epoch += 1,
        }
        assert!(
            store
                .compare_and_swap_lazy_workspace(
                    view.lazy.workspace().id(),
                    revision,
                    next.clone(),
                    &mut accounting,
                )
                .await?
        );
        revision = next.revision;
        assert!(matches!(
            view.lazy
                .mounted_read_state(&observation, &mut accounting)
                .await,
            Err(LazyWorkspaceError::Concurrent | LazyWorkspaceError::StaleSource)
        ));
        assert_eq!(
            store
                .load_lazy_workspace(view.lazy.workspace().id(), &mut accounting)
                .await?,
            Some(next),
            "rejected read changed durable pending-removal authority"
        );
    }
    Ok(())
}

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_ack_reads_keep_operation_custody_until_real_companion_settlement()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::{KernelAccounting, KernelOwnedAllocation};
    use crate::lazy_workspace::{
        LazyOverlay, LazyOverlayId, LazyShadow, LazyShadowId, LazyWorkspaceState,
        PendingLazyRemoveKind,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone)]
    struct PausedSettlement {
        inner: crate::core_state::LocalCoreStateStore,
        armed: Arc<AtomicBool>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        cancel_after_cas: bool,
    }
    impl LazyWorkspaceStore for PausedSettlement {
        async fn load_lazy_workspace(
            &self,
            id: crate::WorkspaceId,
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
            self.inner.load_lazy_workspace(id, ctx).await
        }
        async fn load_lazy_overlay(
            &self,
            id: LazyOverlayId,
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
            self.inner.load_lazy_overlay(id, ctx).await
        }
        async fn load_lazy_shadow(
            &self,
            id: LazyShadowId,
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
            self.inner.load_lazy_shadow(id, ctx).await
        }
        async fn compare_and_swap_lazy_workspace(
            &self,
            id: crate::WorkspaceId,
            revision: u64,
            state: LazyWorkspaceState,
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<bool, LazyWorkspaceError> {
            let mut settlement = false;
            if state.pending_remove.is_none() && self.armed.load(Ordering::Acquire) {
                let original = self.inner.load_lazy_workspace(id, ctx).await?;
                if original.as_ref().is_some_and(|original| {
                    original.revision == revision
                        && original.pending_remove.as_ref().is_some_and(|pending| {
                            matches!(pending.kind, PendingLazyRemoveKind::Mounted { .. })
                        })
                }) && self.armed.swap(false, Ordering::AcqRel)
                {
                    settlement = true;
                    self.entered.notify_one();
                    self.release.notified().await;
                }
            }
            let applied = self
                .inner
                .compare_and_swap_lazy_workspace(id, revision, state, ctx)
                .await?;
            if settlement && applied && self.cancel_after_cas {
                ctx.native_work()
                    .ok_or(LazyWorkspaceError::NativeLedgerMismatch)?
                    .cancellation()
                    .cancel();
            }
            Ok(applied)
        }
        async fn put_lazy_overlay(
            &self,
            id: LazyOverlayId,
            value: (LazyOverlay, KernelOwnedAllocation),
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<(), LazyWorkspaceError> {
            self.inner.put_lazy_overlay(id, value, ctx).await
        }
        async fn put_lazy_shadow(
            &self,
            id: LazyShadowId,
            value: (LazyShadow, KernelOwnedAllocation),
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<(), LazyWorkspaceError> {
            self.inner.put_lazy_shadow(id, value, ctx).await
        }
    }

    for (unknown_ack, cancel_after_cas) in [(false, false), (true, false), (false, true)] {
        let source_root = tempfile::tempdir()?;
        let state_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("source-only"), b"source")?;
        std::fs::write(source_root.path().join("removed"), b"removed")?;
        let store = PausedSettlement {
            inner: crate::core_state::LocalCoreStateStore::new(state_root.path()),
            armed: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            cancel_after_cas,
        };
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut accounting = KernelAccounting::native(&work, &cancellation);
        let simulation = crate::Simulation::default();
        let (authority, objects) = simulation.stores();
        let fs = crate::Fs::new(authority, objects, crate::EmbeddedCapabilities::MEMORY);
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "post-ack-settlement",
                Arc::new(
                    native_source(
                        source_root.path(),
                        crate::model::FilesystemProfile::Portable,
                    )
                    .await?,
                ),
                store.clone(),
                crate::VolumeInitialization {
                    config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                    access_policy: None,
                },
                crate::OperationId::new(),
                &mut accounting,
            )
            .await?
            .0,
        );
        let view = Arc::new(mounted_native_view_of(lazy, &work).await?);
        let source_path = mounted(&["source-only"]);
        let expected_source = view
            .lookup_pinned_with_meter(&source_path, &work)?
            .ok_or("source-only lookup")?;
        let source_pin = expected_source.1.ok_or("source content pin")?;
        let authored_path = mounted(&["authored"]);
        view.create_file(&authored_path, FileMetadata::default(), None, &work)?;
        view.write_range(&authored_path, 0, Bytes::from_static(b"authored"))?;
        let expected_authored = view
            .lookup_with_meter(&authored_path, &work)?
            .ok_or("authored lookup")?;
        view.remove(&mounted(&["removed"]), None, &work)?;
        let root_before = view
            .authored
            .shared_checkout()
            .lock()
            .await
            .root()
            .file_table;
        store.armed.store(true, Ordering::Release);
        let original = work.clone();
        let allow = move |work: &crate::NativeMaterializationWork| {
            assert!(work.shares_allocation_context(&original));
            std::future::ready(Ok::<(), MountSourceError>(()))
        };
        let mut acknowledged = None;
        if unknown_ack {
            simulation.schedule_next(
                crate::SimulationOperation::AuthorityAppend,
                crate::SimulationFault::AmbiguousAuthorityAppend,
            )?;
            let lost = view
                .sync_async_with_permit_qualified_native_work_guarded_with_ack(
                    crate::PublicationPermit::Unrestricted,
                    &work,
                    &cancellation,
                    &mut acknowledged,
                    None,
                    allow.clone(),
                )
                .await;
            assert!(matches!(
                lost,
                Err((
                    MountSourceError::Filesystem(crate::FsError::Authority(
                        crate::AuthorityStoreError::Indeterminate { .. }
                    )),
                    _
                ))
            ));
            assert!(acknowledged.is_none());
            assert_eq!(
                view.lookup_pinned_with_meter(&source_path, &work)?,
                Some(expected_source)
            );
            let mut carrier = adapter::NativeDemandCarrier::Native(&work);
            assert!(matches!(
                view.authored
                    .shared_checkout()
                    .candidate(
                        None,
                        &mut carrier,
                        &cancellation,
                        Some(view.authored.cancellation()),
                    )
                    .await,
                Err(MountSourceError::Stale)
            ));
        }
        let appended_before_reconciliation = work.receipt().authority_records_appended;
        let guarded = move |work: &crate::NativeMaterializationWork| {
            assert!(!unknown_ack, "a durable original must not be sealed twice");
            allow(work)
        };
        let mut publication = Box::pin(
            view.sync_async_with_permit_qualified_native_work_guarded_with_ack(
                crate::PublicationPermit::Unrestricted,
                &work,
                &cancellation,
                &mut acknowledged,
                None,
                guarded,
            ),
        );
        tokio::select! {
            _ = store.entered.notified() => {}
            result = &mut publication => panic!("publication escaped its real settlement CAS: {result:?}"),
        }
        // DATA is durable and the real companion CAS is still pending. This is
        // after SharedCheckout::publish returned, not inside its pre-DATA guard.
        let pending = store
            .inner
            .load_lazy_workspace(view.lazy.workspace().id(), &mut accounting)
            .await?
            .ok_or("pending companion")?;
        let pending_remove = pending.pending_remove.as_ref().ok_or("mounted marker")?;
        let PendingLazyRemoveKind::Mounted { idempotency_key } = pending_remove.kind else {
            return Err("wrong pending kind".into());
        };
        let generation = view
            .lazy
            .operation_generation(idempotency_key, &mut accounting)
            .await?
            .ok_or("DATA was not committed before settlement")?;
        let mut carrier = adapter::NativeDemandCarrier::Native(&work);
        let observation = view
            .authored
            .shared_checkout()
            .observe(
                ViewGate::callback_owner()?,
                &mut carrier,
                &cancellation,
                Some(view.authored.cancellation()),
            )
            .await?;
        let observed_operation = observation.retained_operation();
        let observed_root = observation.root().file_table;
        let observed_generation = observation.generation_id();
        let source_lookup = view.lookup_pinned_with_meter(&source_path, &work);
        let source_bytes = view.read_range_with_meter(&source_path, 0, 6, &work);
        let mut pinned_delivered = 0;
        let mut pinned_packets_match = true;
        let pinned_delivery = view.read_pinned_with_meter(
            &source_path,
            source_pin,
            crate::ByteRange {
                offset: 0,
                length: 6,
            },
            3,
            &mut |offset, packet| {
                let packet_bytes = packet.bytes.as_ref();
                let end = pinned_delivered + packet_bytes.len();
                pinned_packets_match &= offset == pinned_delivered as u64
                    && b"source".get(pinned_delivered..end) == Some(packet_bytes);
                pinned_delivered = end;
                Ok(())
            },
            &work,
        );
        let root_lookup = view.lookup_with_meter(&MountPath::root(), &work);
        let authored_lookup = view.lookup_with_meter(&authored_path, &work);
        let authored_bytes = view.read_range_with_meter(&authored_path, 0, 8, &work);
        let identity = view
            .authored
            .record_by_id(
                expected_authored.node.file_id,
                ViewGate::callback_owner()?,
                &mut carrier,
            )
            .await;
        let directory = (|| {
            let mut pages = Vec::<MountDirectoryPage>::new();
            for _ in 0..4 {
                let cursor = pages.last().and_then(|page| page.next_cursor.as_ref());
                let page = view.read_directory_with_meter(&MountPath::root(), cursor, 1, &work)?;
                assert!(page.entries.len() <= 1);
                let complete = page.next_cursor.is_none();
                pages.push(page);
                if complete {
                    break;
                }
            }
            assert!(pages.last().is_some_and(|page| page.next_cursor.is_none()));
            let mut actual = pages
                .iter()
                .flat_map(|page| &page.entries)
                .map(|entry| (entry.name.clone(), entry.node.file_id))
                .collect::<Vec<_>>();
            actual.sort();
            Ok::<_, MountSourceError>(actual)
        })();
        drop(observation);
        let strict_denied = matches!(
            view.authored
                .shared_checkout()
                .candidate(
                    None,
                    &mut carrier,
                    &cancellation,
                    Some(view.authored.cancellation()),
                )
                .await,
            Err(MountSourceError::Stale)
        );
        assert_eq!(
            store
                .inner
                .load_lazy_workspace(view.lazy.workspace().id(), &mut accounting)
                .await?,
            Some(pending),
            "reads must not settle the companion marker"
        );
        // Release real storage before assertions, so either outcome returns
        // custody through the production publication/settlement path.
        store.release.notify_one();
        let result = publication.await;
        if cancel_after_cas {
            assert!(cancellation.is_cancelled());
            assert!(
                result.is_err(),
                "post-CAS cancellation must remain a failure"
            );
        } else {
            result.map_err(|(error, _)| error)?;
        }
        let ack = acknowledged.ok_or("settled ACK")?;
        assert_eq!(ack.operation(), Some(idempotency_key.operation_id()));
        assert_eq!(ack.generation(), generation);
        assert_eq!(observed_root, root_before);
        assert_eq!(observed_generation, generation);
        assert_eq!(
            source_lookup?,
            Some(expected_source),
            "post-ACK source lookup"
        );
        assert_eq!(source_bytes?.bytes.as_ref(), b"source");
        pinned_delivery?;
        assert!(
            pinned_packets_match,
            "post-ACK pinned content or offsets changed"
        );
        assert_eq!(
            pinned_delivered, 6,
            "post-ACK pinned delivery was incomplete"
        );
        assert!(root_lookup?.is_some());
        assert_eq!(authored_lookup?, Some(expected_authored));
        assert_eq!(authored_bytes?.bytes.as_ref(), b"authored");
        assert_eq!(identity?.file_id, expected_authored.node.file_id);
        let mut expected = [
            (
                source_path.components()[0].clone(),
                expected_source.0.node.file_id,
            ),
            (
                authored_path.components()[0].clone(),
                expected_authored.node.file_id,
            ),
        ]
        .to_vec();
        expected.sort();
        assert_eq!(directory?, expected, "post-ACK listing");
        assert_eq!(observed_operation, Some(idempotency_key.operation_id()));
        assert!(strict_denied, "mutation must await companion settlement");
        if cancel_after_cas {
            let retry_cancel = CancellationToken::new();
            let retry_work =
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &retry_cancel)
                    .map_err(|(error, _)| error)?;
            let mut retry_accounting = KernelAccounting::native(&retry_work, &retry_cancel);
            let ready = store
                .inner
                .load_lazy_workspace(view.lazy.workspace().id(), &mut retry_accounting)
                .await?
                .ok_or("companion already settled before cancellation")?;
            assert!(ready.pending_remove.is_none());
            let mut retry_carrier = adapter::NativeDemandCarrier::Native(&retry_work);
            let retained = view
                .authored
                .shared_checkout()
                .observe(
                    ViewGate::callback_owner()?,
                    &mut retry_carrier,
                    &retry_cancel,
                    None,
                )
                .await?;
            assert_eq!(
                retained.retained_operation(),
                Some(idempotency_key.operation_id())
            );
            drop(retained);
            assert_eq!(
                view.lookup_pinned_with_meter(&source_path, &retry_work)?,
                Some(expected_source)
            );
            assert!(matches!(
                view.authored
                    .shared_checkout()
                    .candidate(
                        None,
                        &mut retry_carrier,
                        &retry_cancel,
                        Some(view.authored.cancellation()),
                    )
                    .await,
                Err(MountSourceError::Stale)
            ));
            let no_second_seal = |_: &crate::NativeMaterializationWork|
                -> std::future::Ready<Result<(), MountSourceError>> {
                panic!("settled committed DATA must not be sealed again")
            };
            let mut retry_ack = None;
            view.sync_async_with_permit_qualified_native_work_guarded_with_ack(
                crate::PublicationPermit::Unrestricted,
                &retry_work,
                &retry_cancel,
                &mut retry_ack,
                None,
                no_second_seal,
            )
            .await
            .map_err(|(error, _)| error)?;
            let retried = retry_ack.ok_or("reconciled post-CAS ACK")?;
            assert_eq!(retried.operation(), ack.operation());
            assert_eq!(retried.generation(), ack.generation());
            assert_eq!(retry_work.receipt().authority_records_appended, 0);
            let settled = view
                .authored
                .shared_checkout()
                .observe(
                    ViewGate::callback_owner()?,
                    &mut retry_carrier,
                    &retry_cancel,
                    None,
                )
                .await?;
            assert_eq!(settled.retained_operation(), None);
            continue;
        }
        let ready = store
            .inner
            .load_lazy_workspace(view.lazy.workspace().id(), &mut accounting)
            .await?
            .ok_or("settled companion")?;
        assert!(ready.pending_remove.is_none());
        let settled_observation = view
            .authored
            .shared_checkout()
            .observe(
                ViewGate::callback_owner()?,
                &mut carrier,
                &cancellation,
                None,
            )
            .await?;
        assert_eq!(settled_observation.retained_operation(), None);
        if unknown_ack {
            assert_eq!(
                work.receipt().authority_records_appended,
                appended_before_reconciliation
            );
        }
    }
    Ok(())
}
