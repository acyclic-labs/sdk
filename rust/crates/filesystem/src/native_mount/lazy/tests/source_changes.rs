use super::*;

/// The source changes outside the view and reports each change: once
#[test]
fn lookups_follow_changes_made_to_the_source_outside_the_view()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"short")?;
    std::fs::create_dir(source.path().join("d"))?;
    let runtime = tokio::runtime::Runtime::new()?;
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

/// Lost notifications invalidate every remembered answer.
#[test]
fn a_source_that_lost_changes_is_read_again_in_full() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"short")?;
    let runtime = tokio::runtime::Runtime::new()?;
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
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )?;
    assert!(!view.unchanged_since(&a, None, stamp));
    assert!(view.lookup(&a)?.is_some());
    Ok(())
}

/// A listing's pages are empty only where it ends, so a driver never
#[test]
fn an_empty_page_ends_its_listing() -> Result<(), Box<dyn std::error::Error>> {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;

    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("empty"))?;
    std::fs::write(source.path().join("a"), b"a")?;
    let runtime = tokio::runtime::Runtime::new()?;
    let view = runtime.block_on(mounted_view(source.path(), "empty-pages"))?;
    let empty = view.read_directory(&mounted(&["empty"]), None, 64)?;
    assert!(empty.entries.is_empty());
    assert!(empty.next_cursor.is_none(), "an empty page continues");
    view.create_file(&mounted(&["b"]), FileMetadata::default(), None, &work)?;
    let mut cursor = None;
    let mut listed = 0;
    loop {
        let page = view.read_directory(&MountPath::root(), cursor.as_deref(), 1)?;
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
    Ok(())
}

/// An open handle reports its file's facts as they are now, as a native
#[test]
fn an_open_handle_reports_its_file_as_it_is_now() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"short")?;
    let runtime = tokio::runtime::Runtime::new()?;
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

/// Dropping a watched view releases everything it holds before the drop
#[test]
fn a_dropped_watched_view_releases_its_stores_at_once() -> Result<(), Box<dyn std::error::Error>> {
    use crate::LocalCoreStateStore;

    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"a")?;
    let state = tempfile::tempdir()?;
    let runtime = tokio::runtime::Runtime::new()?;
    // A store released late fails the very next open; the rounds only
    // vary where the source change lands against the drop.
    for round in 0..24 {
        let store = LocalCoreStateStore::open_owned(state.path().join("core"))
            .map_err(|error| format!("round {round}: {error}"))?;
        let view = runtime.block_on(async {
            mounted_view_of(
                Arc::new(native_source(source.path()).await?),
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

/// A file with a name outside the watched root changes unreported when
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
    let runtime = tokio::runtime::Runtime::new()?;
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

/// A source that cannot report its changes is read afresh behind every
#[test]
fn an_unwatched_source_is_read_afresh_and_never_cached() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"short")?;
    let runtime = tokio::runtime::Runtime::new()?;
    let demand = Arc::new(runtime.block_on(native_source(source.path()))?.unwatched());
    let view = runtime.block_on(mounted_view_of(
        Arc::clone(&demand),
        "unwatched",
        crate::MemoryLazyWorkspaceStore::default(),
    ))?;
    assert_eq!(view.view_stamp(), None, "an unwatched view is never cached");
    let (a, absent) = (mounted(&["a"]), mounted(&["absent"]));
    let size = |path: &MountPath| -> Result<Option<u64>, MountSourceError> {
        Ok(view.lookup(path)?.map(|lookup| lookup.node.logical_bytes))
    };
    assert_eq!(size(&a)?, Some(5));
    assert_eq!(size(&absent)?, None);
    std::fs::write(source.path().join("a"), b"much longer")?;
    std::fs::write(source.path().join("absent"), b"present")?;
    assert_eq!(size(&a)?, Some(11));
    assert_eq!(size(&absent)?, Some(7));
    Ok(())
}

/// Promoting a name moves its answer from the source into the checkout,
#[test]
fn a_promotion_changes_its_directory_listing() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("d"))?;
    std::fs::write(source.path().join("d/f"), b"source")?;
    let runtime = tokio::runtime::Runtime::new()?;
    let view = runtime.block_on(mounted_view(source.path(), "listing-promotion"))?;
    let (d, f) = (mounted(&["d"]), mounted(&["d", "f"]));
    view.promote_locked(&d, None)?;
    let stamp = view.view_stamp().ok_or("the view has no stamp")?;
    view.promote_locked(&f, None)?;
    assert!(!view.unchanged_since(&d, None, stamp));
    Ok(())
}
