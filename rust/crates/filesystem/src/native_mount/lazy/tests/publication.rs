use super::*;

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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

    demand.invalidate(&mut crate::demand::DemandWork::new(
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))?;
    view.advance_to_head_async().await?;
    assert!(
        changes().map_err(|_| "poisoned observer")? > 0,
        "a rebind to a new source epoch changes the view"
    );
    assert_ne!(view.binding_epoch(), binding);
    Ok(())
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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
    let view = mounted_view(source_root.path(), "mounted-identity-replace").await?;
    let lazy = Arc::clone(&view.lazy);
    let a = mounted(&["a"]);
    let c = mounted(&["c"]);
    let d = mounted(&["d"]);
    let old = view.open_file(&c)?;
    view.rename(&a, &c, true, &work)?;
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

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread")]
async fn remembered_resolutions_follow_every_change_to_the_view()
-> Result<(), Box<dyn std::error::Error>> {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("a"), b"source")?;
    let view = mounted_view(source_root.path(), "mounted-resolutions").await?;
    let lazy = Arc::clone(&view.lazy);
    let (a, b) = (mounted(&["a"]), mounted(&["b"]));

    // Remembered answers: a source node, and an absence.
    let source = view.lookup(&a)?.ok_or("source file absent")?;
    assert_eq!(view.lookup(&a)?, Some(source));
    assert_eq!(view.lookup(&b)?, None);
    assert_eq!(view.lookup(&b)?, None);

    // A create through the view replaces the remembered absence.
    let created = view.create_file(&b, FileMetadata::default(), None, &work)?;
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
    let created = view.create_file(&inner, FileMetadata::default(), None, &work)?;
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

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test(flavor = "multi_thread")]
async fn promotion_and_attribute_reads_leave_cached_lookups_current()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    std::fs::create_dir(source_root.path().join("d"))?;
    std::fs::write(source_root.path().join("d").join("f"), b"source")?;
    std::fs::write(source_root.path().join("g"), b"other")?;
    let view = mounted_view(source_root.path(), "mounted-promotion-effect").await?;
    let lazy = Arc::clone(&view.lazy);
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
    assert!(view.read_attribute(&file, b"user.absent", 1024)?.is_none());
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

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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

#[test]
fn abandoned_directory_cursors_are_bounded_and_cleared() {
    let cursors = CursorTable::new();
    let tokens = (0..=MAXIMUM_LAZY_DIRECTORY_CURSORS)
        .map(|cursor| cursors.remember((2, 1), cursor).expect("cursor"))
        .collect::<Vec<_>>();
    let retained = tokens
        .iter()
        .filter_map(|token| cursors.take((2, 1), Some(token)).ok().flatten())
        .count();
    assert_eq!(retained, MAXIMUM_LAZY_DIRECTORY_CURSORS);

    let stale = cursors.remember((2, 1), 0).expect("stale cursor");
    assert!(matches!(
        cursors.take((3, 1), Some(&stale)),
        Err(MountSourceError::Stale)
    ));
    let token = cursors.remember((2, 1), 0).expect("retained cursor");
    cursors.clear();
    assert!(matches!(
        cursors.take((2, 1), Some(&token)),
        Err(MountSourceError::Stale)
    ));
}
