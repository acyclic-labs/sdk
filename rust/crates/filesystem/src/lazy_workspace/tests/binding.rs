use super::*;
    /// A view bound before its shared source advanced (a refresh of the
    /// physical root) follows the new epoch on its next access instead of
    /// failing: every fork of a root reads through one source.
    #[tokio::test]
    async fn a_view_follows_its_shared_source_to_a_newer_epoch() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "follows",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/observed.txt").await.expect("observe");
        source.invalidate();
        assert_eq!(
            root.stat("/new.txt").await.expect("follows").logical_bytes,
            Some(6)
        );
        assert_eq!(
            root.snapshot().await.expect("snapshot").source,
            source.reference()
        );
    }

    #[tokio::test]
    async fn source_tombstones_list_only_removed_source_paths() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "tombstones",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/kept.txt").await.expect("observe kept");
        root.stat("/removed.txt").await.expect("observe removed");
        root.remove("/removed.txt")
            .await
            .expect("remove source path");
        root.remove("/also-removed.txt")
            .await
            .expect("remove unobserved source path");
        root.stat("/hidden.txt").await.expect("observe hidden");
        root.hide_source_path("/hidden.txt")
            .await
            .expect("hide source path");
        assert!(root.stat("/hidden.txt").await.is_err());
        let tombstones = root.source_tombstones(16).await.expect("tombstones");
        assert_eq!(
            tombstones
                .iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>(),
            ["/also-removed.txt", "/removed.txt"]
        );
        let current = root
            .current_source_version("/removed.txt")
            .await
            .expect("current version");
        assert_eq!(current, tombstones.get(1).map(|(_, version)| *version));
        assert!(matches!(
            root.source_tombstones(1).await,
            Err(LazyWorkspaceError::Work(_))
        ));
    }

    #[tokio::test]
    async fn exactify_threads_cancellation_into_nested_source_work() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &fs,
            "cancelled-exactify",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let cancellation = CancellationToken::new();
        source.cancel_next_page();

        assert!(matches!(
            root.exactify(WorkBudget::UNBOUNDED, &cancellation).await,
            Err(LazyWorkspaceError::Cancelled)
        ));
        assert!(cancellation.is_cancelled());
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exactify_budget_exhaustion_cannot_publish_the_current_path() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"content")));
        let root = LazyWorkspace::attach(
            &fs,
            "budgeted-exactify",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let before = root
            .workspace()
            .head()
            .await
            .expect("head before exactify")
            .id();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.authority_records_appended = 0;

        assert!(matches!(
            root.exactify(budget, &CancellationToken::new()).await,
            Err(LazyWorkspaceError::Workspace(_))
        ));
        assert_eq!(
            root.workspace()
                .head()
                .await
                .expect("head after rejection")
                .id(),
            before
        );
        assert!(matches!(
            root.workspace().stat("/file.txt").await,
            Err(WorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn exactify_snapshot_rejects_before_destination_publication() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "snapshot-source",
            Arc::new(CountingSource::new(Bytes::from_static(b"content"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let snapshot = root.snapshot().await.expect("snapshot");

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            root.exactify_snapshot(
                snapshot,
                "cancelled-destination",
                IdempotencyKey::from_bytes([0x91; 16]),
                WorkBudget::UNBOUNDED,
                &cancelled,
            )
            .await,
            Err(LazyWorkspaceError::Cancelled)
        ));
        assert!(fs.open_workspace("cancelled-destination").await.is_err());

        assert!(matches!(
            root.exactify_snapshot(
                snapshot,
                "budget-destination",
                IdempotencyKey::from_bytes([0x92; 16]),
                WorkBudget::default(),
                &CancellationToken::new(),
            )
            .await,
            Err(LazyWorkspaceError::Work(_))
        ));
        assert!(fs.open_workspace("budget-destination").await.is_err());
    }

    #[tokio::test]
    async fn full_directory_page_peak_is_admitted_before_projection() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "page-memory",
            Arc::new(
                CountingSource::new(Bytes::from_static(b"content")).with_directory_entries(1_024),
            ),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let receipt = root
            .list_directory_measured(
                "/",
                None,
                1_024,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
                None,
            )
            .await
            .expect("measured page");
        assert_eq!(receipt.value.entries.len(), 1_024);
        assert!(receipt.work.allocation_operations >= 1);

        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.peak_allocation_bytes = receipt.work.peak_allocation_bytes - 1;
        assert!(matches!(
            root.list_directory_measured(
                "/",
                None,
                1_024,
                insufficient,
                &CancellationToken::new(),
                None,
                None,
            )
            .await,
            Err(LazyWorkspaceError::Work(_))
        ));
    }

    #[tokio::test]
    async fn attach_open_and_fork_demand_no_source_work() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"abcdef")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("open");
        let child = reopened
            .fork("child", IdempotencyKey::from_bytes([5; 16]))
            .await
            .expect("fork");
        let first_snapshot = child.snapshot().await.expect("snapshot");
        let second_snapshot = child.snapshot().await.expect("repeat snapshot");
        let resumed_child = root
            .open_related(fs.open_workspace("child").await.expect("child workspace"))
            .await
            .expect("resume related child");
        let sibling = root
            .fork("sibling", IdempotencyKey::from_bytes([6; 16]))
            .await
            .expect("sibling fork");
        let resumed_sibling = resumed_child
            .open_related(
                fs.open_workspace("sibling")
                    .await
                    .expect("sibling workspace"),
            )
            .await
            .expect("constant-work branch switch");
        assert_eq!(
            sibling.snapshot().await.expect("sibling snapshot"),
            resumed_sibling
                .snapshot()
                .await
                .expect("resumed sibling snapshot")
        );

        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(
            child
                .state()
                .await
                .expect("child state")
                .parent_workspace_id,
            Some(root.workspace().id())
        );
        assert_eq!(first_snapshot, second_snapshot);
    }