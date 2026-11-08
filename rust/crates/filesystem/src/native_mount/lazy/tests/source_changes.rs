use super::*;

#[cfg(all(target_os = "linux", feature = "native-watch"))]
#[test]
fn cleanup_retry_observes_the_retired_source_watch_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir_all(source.path().join("child/grandchild"))?;
    let reported = source.path().join("child/grandchild/reported");
    std::fs::write(&reported, b"before")?;
    let runtime = shared_runtime()?;
    let demand = Arc::new(runtime.block_on(native_source(
        source.path(),
        crate::model::FilesystemProfile::Portable,
    ))?);
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let view = runtime.block_on(mounted_view_of_with_authored_work(
        Arc::clone(&demand),
        "cleanup-retry",
        crate::MemoryLazyWorkspaceStore::default(),
        crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
        Some(&original),
    ))?;
    assert!(
        view.lookup(&mounted(&["child", "grandchild", "reported"]))?
            .is_some()
    );
    let watch = demand
        .physical_watch_for_test()
        .ok_or("production lazy source has no native watch")?;
    let (retired, observed) = std::sync::mpsc::channel();
    watch.observe_retirement(retired);
    let reading = watch.paused();
    let wait = CancellationToken::new();
    let cancel = wait.clone();
    crate::cancellation::custody::BEFORE_WAIT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || cancel.cancel()));
    });
    let first = view.cancel_cleanup_with_join(
        Some(&wait),
        crate::native_mount::retirement::SourceRetirementWait::Caller,
    );
    assert!(
        wait.is_cancelled(),
        "cleanup did not reach native read custody"
    );
    assert!(matches!(first, Err(MountSourceError::Cancelled)));
    assert!(view.authored.capture_retired());
    assert!(!watch.is_stopped());
    drop(reading);
    // This is the original subscription and recorder. Its actual callback
    // refuses because authored cleanup already revoked capture admission.
    std::fs::write(reported, b"changed")?;
    observed.recv()?;
    assert!(view.physically_retired()?);
    let before = original.receipt();
    let retired = view.cancel_cleanup_with_join(
        Some(&wait),
        crate::native_mount::retirement::SourceRetirementWait::Caller,
    )?;
    assert!(!retired.changed);
    assert!(
        matches!(retired.failure, Some(MountSourceError::Cancelled)),
        "cleanup retry erased the retired watch's callback failure"
    );
    assert_eq!(
        original.receipt(),
        before,
        "retry repeated physical cleanup"
    );
    let again = view.cancel_cleanup_with_join(
        Some(&wait),
        crate::native_mount::retirement::SourceRetirementWait::Caller,
    )?;
    assert!(!again.changed);
    assert!(again.failure.is_none());
    Ok(())
}

#[test]
fn lookups_follow_changes_made_to_the_source_outside_the_view()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"short")?;
    std::fs::create_dir(source.path().join("d"))?;
    let runtime = shared_runtime()?;
    let view = runtime.block_on(mounted_view(source.path(), "source-changes"))?;
    assert!(view.view_stamp().is_some(), "a local source is watched");
    let (a, d) = (mounted(&["a"]), mounted(&["d"]));
    let (created, beneath) = (mounted(&["d", "x"]), mounted(&["gone", "x"]));
    let size = |path: &MountPath| -> Result<Option<u64>, MountSourceError> {
        Ok(view.lookup(path)?.map(|lookup| lookup.node.logical_bytes))
    };
    for _ in 0..2 {
        assert_eq!(size(&a)?, Some(5));
        assert_eq!(size(&created)?, None);
        assert_eq!(size(&beneath)?, None);
    }

    std::fs::write(source.path().join("a"), b"much longer")?;
    std::fs::write(source.path().join("d/x"), b"x")?;
    std::fs::create_dir(source.path().join("gone"))?;
    std::fs::write(source.path().join("gone/x"), b"xy")?;
    view.fence_changes()?;
    assert_eq!(size(&a)?, Some(11));
    assert_eq!(size(&created)?, Some(1));
    assert_eq!(size(&beneath)?, Some(2));
    let listed = view.read_directory(&d, None, 64)?;
    assert_eq!(
        listed
            .entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>(),
        vec![created.components()[1].clone()]
    );

    std::fs::remove_file(source.path().join("a"))?;
    view.fence_changes()?;
    assert_eq!(size(&a)?, None);
    Ok(())
}
#[test]
fn a_source_that_lost_changes_is_read_again_in_full() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"short")?;
    let runtime = shared_runtime()?;
    let view = runtime.block_on(mounted_view(source.path(), "lost-changes"))?;
    let a = mounted(&["a"]);
    assert!(view.lookup(&a)?.is_some());
    let stamp = view.view_stamp().ok_or("a watched view carries stamps")?;
    SourceChangeRecorder {
        lazy: Arc::downgrade(&view.lazy),
        authored: Arc::downgrade(&view.authored),
        constructor_inputs: Mutex::new(None),
    }
    .source_changed(
        &[SourceChange::Everything],
        &mut crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ),
    )?;
    assert!(!view.unchanged_since(&a, None, stamp));
    assert!(view.lookup(&a)?.is_some());
    Ok(())
}
#[test]
fn an_empty_page_ends_its_listing() -> Result<(), Box<dyn std::error::Error>> {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("empty"))?;
    std::fs::write(source.path().join("a"), b"a")?;
    let runtime = shared_runtime()?;
    let view = runtime.block_on(mounted_view(source.path(), "empty-pages"))?;
    let empty = view.read_directory(&mounted(&["empty"]), None, 64)?;
    assert!(empty.entries.is_empty());
    assert!(empty.next_cursor.is_none(), "an empty page continues");
    view.create_file(&mounted(&["b"]), FileMetadata::default(), None, &work)?;

    let first = view.read_directory_with_meter(&MountPath::root(), None, 1, &work)?;
    let first_cursor = first.next_cursor().ok_or("first page must continue")?;
    let peer_stop = CancellationToken::new();
    let peer_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &peer_stop)
        .map_err(|(error, _)| error)?;
    let peer_baseline = peer_work.live_allocation_bytes();
    let peers = (0..1_024)
        .map(|_| view.read_directory_with_meter(&MountPath::root(), None, 1, &peer_work))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(peers.iter().all(|page| page.next_cursor().is_some()));
    let other = runtime.block_on(mounted_view(source.path(), "independent-listing-owner"))?;
    assert!(matches!(
        other.read_directory_with_meter(&MountPath::root(), Some(first_cursor), 1, &work),
        Err(MountSourceError::Stale)
    ));
    drop(other);
    let visited = work.receipt().source_entries_visited;
    let resumed =
        view.read_directory_with_meter(&MountPath::root(), Some(first_cursor), 1, &work)?;
    assert_eq!(resumed.entries().len(), 1);
    assert_eq!(
        work.receipt().source_entries_visited,
        visited,
        "continuation replayed its source prefix"
    );
    assert!(matches!(
        view.read_directory_with_meter(&MountPath::root(), Some(first_cursor), 1, &work),
        Err(MountSourceError::Stale)
    ));
    peer_stop.cancel();
    drop(peers);
    assert_eq!(
        peer_work.live_allocation_bytes(),
        peer_baseline,
        "abandoned consumers must release their original credit"
    );
    drop(resumed);
    drop(first);

    let mut cursor = None;
    let mut listed = 0;
    loop {
        let page = view.read_directory(&MountPath::root(), cursor.as_ref(), 1)?;
        assert!(
            !page.entries.is_empty() || page.next_cursor.is_none(),
            "an empty page continues"
        );
        listed += page.entries.len();
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(listed, 3, "a, b, and empty");
    drop(cursor);
    #[cfg(target_os = "linux")]
    {
        let page = view.read_directory_with_meter(&MountPath::root(), None, 1, &work)?;
        assert!(page.next_cursor().is_some());
        let view = Arc::new(view);
        assert!(
            crate::native_mount::fuse::tests::retirement::retire_source_in_physical_loop(
                view.clone(),
                Some(page),
            )?
        );
        assert!(view.physically_retired()?);
    }
    #[cfg(not(target_os = "linux"))]
    {
        view.cancel_cleanup(None)?;
        assert!(view.physically_retired()?);
    }
    Ok(())
}
#[test]
fn an_open_handle_reports_its_file_as_it_is_now() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"short")?;
    let runtime = shared_runtime()?;
    let view = runtime.block_on(mounted_view(source.path(), "handle-facts"))?;
    let a = mounted(&["a"]);
    let handle = view.open_file(&a)?;
    assert_eq!(handle.lookup()?.node.logical_bytes, 5);
    let opened = handle.lookup()?.node.file_id;

    // Rewritten in place: the same file, with new facts.
    std::fs::write(source.path().join("a"), b"much longer")?;
    assert_eq!(handle.lookup()?.node.logical_bytes, 11);
    assert_eq!(handle.lookup()?.node.file_id, opened);

    // Replaced: the path names another file; the handle keeps its own.
    std::fs::write(source.path().join("b"), b"replacement!!")?;
    std::fs::rename(source.path().join("b"), source.path().join("a"))?;
    let facts = handle.lookup()?;
    assert_eq!(facts.node.file_id, opened);
    assert_eq!(facts.node.logical_bytes, 5);
    Ok(())
}

#[test]
fn an_open_handle_keeps_facts_when_its_parent_path_stops_resolving()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("a"))?;
    std::fs::write(source.path().join("a/file"), b"short")?;
    let runtime = shared_runtime()?;
    let view = runtime.block_on(mounted_view(source.path(), "handle-moved-parent"))?;
    let handle = view.open_file(&mounted(&["a", "file"]))?;
    let opened = handle.lookup()?.node;
    std::fs::rename(source.path().join("a"), source.path().join("moved"))?;
    std::fs::write(source.path().join("a"), b"not a directory")?;
    let current = handle.lookup()?.node;
    assert_eq!(current.file_id, opened.file_id);
    assert_eq!(current.logical_bytes, opened.logical_bytes);
    std::fs::remove_file(source.path().join("a"))?;
    assert_eq!(handle.lookup()?.node.file_id, opened.file_id);
    let cancelled = CancellationToken::new();
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            backend_read_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancelled,
    )
    .map_err(|(error, _)| error)?;
    assert!(handle.lookup_with_meter(&denied).is_err());
    cancelled.cancel();
    assert!(handle.lookup_with_meter(&denied).is_err());
    Ok(())
}
#[test]
fn a_dropped_watched_view_releases_its_stores_at_once() -> Result<(), Box<dyn std::error::Error>> {
    use crate::LocalCoreStateStore;

    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"a")?;
    let state = tempfile::tempdir()?;
    let state_root = std::fs::canonicalize(state.path())?;
    let runtime = shared_runtime()?;
    // A store released late fails the very next open; the rounds only
    // vary where the source change lands against the drop.
    for round in 0..24 {
        let store = LocalCoreStateStore::open_owned(state_root.join("core"))
            .map_err(|error| format!("round {round}: {error}"))?;
        let view = runtime.block_on(async {
            mounted_view_of(
                Arc::new(
                    native_source(source.path(), crate::model::FilesystemProfile::Portable).await?,
                ),
                &format!("released-{round}"),
                store,
            )
            .await
        })?;
        assert!(view.view_stamp().is_some(), "a local source is watched");
        assert!(view.lookup(&mounted(&["a"]))?.is_some());
        // A report in delivery while the view drops.
        std::fs::write(source.path().join("a"), format!("round {round}"))?;
        drop(view);
    }
    Ok(())
}
#[cfg(unix)]
#[test]
fn a_file_linked_outside_the_source_is_read_afresh() -> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let (source, outside) = (parent.path().join("source"), parent.path().join("outside"));
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir_all(&outside)?;
    std::fs::write(source.join("linked"), b"short")?;
    std::fs::hard_link(source.join("linked"), outside.join("alias"))?;
    std::fs::write(source.join("single"), b"single")?;
    let runtime = shared_runtime()?;
    let view = runtime.block_on(mounted_view(&source, "linked-outside"))?;
    let (linked_path, single) = (mounted(&["linked"]), mounted(&["single"]));
    let stamp = view.view_stamp().ok_or("a local source is watched")?;
    let linked_file = view.lookup(&linked_path)?.ok_or("linked")?.node;
    let single_file = view.lookup(&single)?.ok_or("single")?.node;
    assert_eq!(linked_file.logical_bytes, 5);
    assert!(!view.unchanged_since(&linked_path, Some(linked_file.file_id), stamp));
    assert!(!view.node_unchanged_since(linked_file.file_id, stamp));
    // Single-link files are observable; a delayed/coalesced watcher report
    // may still invalidate the earlier stamp, especially with FSEvents.
    assert!(view.reports_changes_to(single_file.file_id));

    // Written in place through the name outside the source: unreported.
    std::fs::OpenOptions::new()
        .append(true)
        .open(outside.join("alias"))
        .and_then(|mut file| std::io::Write::write_all(&mut file, b" and longer"))?;
    assert_eq!(
        view.lookup(&linked_path)?
            .map(|lookup| lookup.node.logical_bytes),
        Some(16)
    );
    Ok(())
}
#[test]
fn a_promotion_changes_its_directory_listing() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("d"))?;
    std::fs::write(source.path().join("d/f"), b"source")?;
    let runtime = shared_runtime()?;
    let view = runtime.block_on(mounted_view(source.path(), "listing-promotion"))?;
    let (d, f) = (mounted(&["d"]), mounted(&["d", "f"]));
    view.promote_locked(&d, None)?;
    let stamp = view.view_stamp().ok_or("the view has no stamp")?;
    view.promote_locked(&f, None)?;
    assert!(!view.unchanged_since(&d, None, stamp));
    Ok(())
}
