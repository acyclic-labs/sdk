use super::*;

/// The source changes outside the view and reports each change: once
    /// the report is recorded, no remembered answer outlives what the
    /// source names, and lookups agree with listings.
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

/// A watched source is read once per name: a remembered answer, an
    /// absence included, holds without reading the source again until the
    /// source reports a change to the name, and then exactly that name is
    /// read again.
    #[test]
    fn a_watched_source_is_read_again_only_for_what_it_reports_changed()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(
            runtime.block_on(native_source(source.path()))?,
            true,
        ));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "watched-reads"))?;
        let (a, b, absent) = (mounted(&["a"]), mounted(&["b"]), mounted(&["absent"]));
        let size = |path: &MountPath| -> Result<Option<u64>, MountSourceError> {
            Ok(view.lookup(path)?.map(|lookup| lookup.node.logical_bytes))
        };

        // Create the initial entries after the watch starts. Linux and
        // Windows can fence these reports exactly. FSEvents has no delivery
        // barrier, so wait for the names (or a conservative everything
        // report) and then for its report stream to go quiet before reading
        // the first answers. This keeps startup notifications out of the
        // cache-counting assertion below.
        std::fs::write(source.path().join("a"), b"short")?;
        std::fs::write(source.path().join("b"), b"other")?;
        #[cfg(target_os = "macos")]
        {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut reports = demand.report_count();
            let mut quiet = 0;
            while quiet < 10 || !demand.reports_cover(&[b"a", b"b"]) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the startup notifications never settled"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
                let current = demand.report_count();
                if current == reports {
                    quiet += 1;
                } else {
                    reports = current;
                    quiet = 0;
                }
            }
        }
        #[cfg(not(target_os = "macos"))]
        view.fence_changes()?;

        assert_eq!(size(&a)?, Some(5));
        assert_eq!(size(&b)?, Some(5));
        assert_eq!(size(&absent)?, None);
        let read = demand.lookups();
        for _ in 0..3 {
            assert_eq!(size(&a)?, Some(5));
            assert_eq!(size(&b)?, Some(5));
            assert_eq!(size(&absent)?, None);
        }
        assert_eq!(demand.lookups(), read, "remembered answers read the source");

        std::fs::write(source.path().join("a"), b"much longer")?;
        std::fs::write(source.path().join("absent"), b"present")?;
        // Waits for the reports themselves: a macOS fence would report
        // everything and read every name again.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while (size(&a)?, size(&absent)?) != (Some(11), Some(7)) {
            assert!(
                std::time::Instant::now() < deadline,
                "the changes were never reported"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let reread = demand.lookups();
        assert!(reread > read, "a reported change reads the source again");
        assert_eq!(size(&b)?, Some(5));
        // FSEvents may coalesce these changes into one event on the root
        // that names no item, which rightly reports everything; only the
        // other hosts name each change exactly.
        #[cfg(not(target_os = "macos"))]
        assert_eq!(
            demand.lookups(),
            reread,
            "an unchanged name is not read again"
        );
        Ok(())
    }

/// Lost notifications invalidate every remembered answer.
    #[test]
    fn a_source_that_lost_changes_is_read_again_in_full() -> Result<(), Box<dyn std::error::Error>>
    {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"short")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(
            runtime.block_on(native_source(source.path()))?,
            true,
        ));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "lost-changes"))?;
        let a = mounted(&["a"]);
        assert!(view.lookup(&a)?.is_some());
        let stamp = view.view_stamp().ok_or("a watched view carries stamps")?;
        let read = demand.lookups();
        SourceChangeRecorder {
            lazy: Arc::downgrade(&view.lazy),
            authored: Arc::downgrade(&view.authored),
        }
        .source_changed(&[SourceChange::Everything]);
        assert!(!view.unchanged_since(&a, None, stamp));
        assert!(view.lookup(&a)?.is_some());
        assert!(demand.lookups() > read, "everything is read again");
        Ok(())
    }

/// A listing's pages are empty only where it ends, so a driver never
    /// mistakes an empty directory for a listing that stopped making progress.
    #[test]
    fn an_empty_page_ends_its_listing() -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("empty"))?;
        std::fs::write(source.path().join("a"), b"a")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "empty-pages"))?;
        let empty = view.read_directory(&mounted(&["empty"]), None, 64)?;
        assert!(empty.entries.is_empty());
        assert!(empty.next_cursor.is_none(), "an empty page continues");
        view.create_file(&mounted(&["b"]), FileMetadata::default())?;
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
    /// `fstat` does, while the path still names that file, and keeps the
    /// facts it was opened with once the path names another.
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
    /// returns, however its source's notifications are delivered, so the
    /// stores it wrote can be opened again at once.
    #[test]
    fn a_dropped_watched_view_releases_its_stores_at_once() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::model::{CheckoutMode, GenerationSelector};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, LocalCoreStateStore};

        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"a")?;
        let state = tempfile::tempdir()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let fs = Fs::memory();
        // A store released late fails the very next open; the rounds only
        // vary where the source change lands against the drop.
        for round in 0..24 {
            let store = LocalCoreStateStore::open_owned(state.path().join("core"))
                .map_err(|error| format!("round {round}: {error}"))?;
            let view = runtime.block_on(async {
                let lazy = Arc::new(
                    LazyWorkspace::attach(
                        &fs,
                        &format!("released-{round}"),
                        Arc::new(native_source(source.path()).await?),
                        store,
                    )
                    .await?,
                );
                let checkout = lazy
                    .workspace()
                    .engine_checkout(
                        GenerationSelector::Head,
                        CheckoutMode::tracking_transaction(),
                    )
                    .await?;
                let config = checkout.volume_config();
                let authored = Arc::new(CheckoutMountSource::new(
                    Arc::new(SharedCheckout::with_publication(
                        checkout,
                        MountPublication::Manual,
                    )),
                    config,
                )?);
                Ok::<_, Box<dyn std::error::Error>>(LazyMountSource::new(
                    lazy,
                    authored,
                    "/".to_owned(),
                )?)
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
    /// written through that name, so the view reads it afresh, never from
    /// what it remembered, and no driver may cache it.
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
    /// remembered answer, and no driver may cache what the view answers.
    #[test]
    fn an_unwatched_source_is_read_afresh_and_never_cached()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("a"), b"short")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let demand = Arc::new(Counted::new(
            runtime.block_on(native_source(source.path()))?,
            false,
        ));
        let view = runtime.block_on(mounted_view_of(Arc::clone(&demand), "unwatched"))?;
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
    /// which a listing merges separately, so a promotion changes its
    /// directory's listing even where every name still resolves as it did:
    /// a continuation begun before must not list the name a second time.
    #[test]
    fn a_promotion_changes_its_directory_listing() -> Result<(), Box<dyn std::error::Error>> {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("d"))?;
        std::fs::write(source.path().join("d/f"), b"source")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let view = runtime.block_on(mounted_view(source.path(), "listing-promotion"))?;
        let (d, f) = (mounted(&["d"]), mounted(&["d", "f"]));
        view.promote_locked(&d)?;
        let stamp = view.view_stamp().ok_or("the view has no stamp")?;
        view.promote_locked(&f)?;
        assert!(!view.unchanged_since(&d, None, stamp));
        Ok(())
    }
