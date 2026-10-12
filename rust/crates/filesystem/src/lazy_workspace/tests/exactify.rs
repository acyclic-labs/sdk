use super::*;


    #[tokio::test]
    async fn authored_state_and_tombstones_mask_the_source() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        assert_eq!(
            root.read("/file.txt", 32).await.expect("authored read"),
            Bytes::from_static(b"authored")
        );
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        root.remove("/file.txt").await.expect("remove");
        assert!(matches!(
            root.stat("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn source_epoch_can_rebind_without_enumeration() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/observed.txt").await.expect("observe");
        let before = root.snapshot().await.expect("snapshot before rebind");
        source.invalidate();
        let rebound = root.rebind_source().await.expect("rebind");
        assert_eq!(rebound.source, source.reference());
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
        assert_eq!(
            root.stat("/observed.txt")
                .await
                .expect("cached")
                .logical_bytes,
            Some(6)
        );
        let after = root.snapshot().await.expect("snapshot after refresh");
        assert_ne!(after, before);
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn snapshot_exactification_uses_live_epoch_for_unresolved_paths() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let snapshot = root.snapshot().await.expect("snapshot before rebind");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source().await.expect("rebind");
        let exact = root
            .exactify_snapshot(
                snapshot,
                "historical-exact",
                IdempotencyKey::from_bytes([23; 16]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("exactify historical snapshot");

        assert_eq!(
            exact
                .value
                .read("/file.txt", 32)
                .await
                .expect("unresolved file from live epoch"),
            Bytes::from_static(b"changed")
        );
    }

    #[tokio::test]
    async fn snapshot_exactification_keeps_observed_paths_on_the_pinned_epoch() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        root.stat("/file.txt").await.expect("observe old epoch");
        let snapshot = root.snapshot().await.expect("snapshot after observation");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source().await.expect("rebind");
        let exact = root
            .exactify_snapshot(
                snapshot,
                "pinned-exact",
                IdempotencyKey::from_bytes([24; 16]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("exactify pinned observation");

        assert_eq!(
            exact
                .value
                .read("/file.txt", 32)
                .await
                .expect("observed file from pinned epoch"),
            Bytes::from_static(b"source")
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_source_rebind_refreshes_an_observed_file() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        let path = directory.path().join("file.txt");
        std::fs::write(&path, b"before").expect("initial file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "native-rebind",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        assert_eq!(
            root.read("/file.txt", 64).await.expect("initial read"),
            Bytes::from_static(b"before")
        );
        let before = root.snapshot().await.expect("snapshot before change");

        std::fs::write(&path, b"after").expect("external write");
        source.invalidate();
        root.rebind_source().await.expect("rebind source");

        assert_eq!(
            root.read("/file.txt", 64).await.expect("refreshed read"),
            Bytes::from_static(b"after")
        );
        assert_ne!(
            root.snapshot().await.expect("snapshot after change"),
            before
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn exactify_is_bounded_and_returns_only_a_complete_generation() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("nested")).expect("nested directory");
        std::fs::write(directory.path().join("a.txt"), b"alpha").expect("first file");
        std::fs::write(directory.path().join("nested").join("b.txt"), b"beta")
            .expect("second file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let fs = Fs::memory();
        let root =
            LazyWorkspace::attach(&fs, "exactify", source, MemoryLazyWorkspaceStore::default())
                .await
                .expect("attach");
        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.source_entries_visited = 1;
        assert!(matches!(
            root.exactify(insufficient, &CancellationToken::new()).await,
            Err(LazyWorkspaceError::Work(_))
        ));

        let exact = root
            .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("exact capture");
        assert_eq!(
            exact
                .value
                .read("/nested/b.txt", 16)
                .await
                .expect("captured file"),
            Bytes::from_static(b"beta")
        );
        assert_eq!(exact.work.source_entries_visited, 3);
        assert_eq!(
            exact.work.source_bytes_read, 9,
            "version-pinned demand is read once and measured by the blob builder"
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn exactify_subtree_keeps_siblings_lazy() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("hot")).expect("hot directory");
        std::fs::create_dir(directory.path().join("cold")).expect("cold directory");
        std::fs::write(directory.path().join("hot/file.txt"), b"hot").expect("hot file");
        std::fs::write(directory.path().join("cold/file.txt"), b"cold").expect("cold file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "subtree-exactify",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");

        let exact = root
            .exactify_subtree_with_permit(
                "/hot",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await
            .expect("capture hot subtree");
        assert_eq!(
            exact
                .value
                .read("/hot/file.txt", 16)
                .await
                .expect("hot content"),
            Bytes::from_static(b"hot")
        );
        #[cfg(feature = "native-mount")]
        {
            let staged = tempfile::tempdir().expect("empty native stage");
            exact
                .value
                .materialize_path(
                    "/hot",
                    &crate::MaterializeOptions::native(staged.path()),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .expect("materialize only pinned hot subtree");
            assert_eq!(
                std::fs::read(staged.path().join("hot/file.txt")).expect("staged hot file"),
                b"hot"
            );
            assert!(!staged.path().join("cold").exists());
        }
        assert!(matches!(
            exact.value.read("/cold/file.txt", 16).await,
            Err(WorkspaceError::NotFound)
        ));
        assert!(matches!(
            root.lookup("/cold/file.txt")
                .await
                .expect("cold remains lazy"),
            LazyLookup::Source(_)
        ));
        assert!(matches!(
            root.exactify_subtree_with_permit(
                "/hot/file.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await,
            Err(LazyWorkspaceError::NotDirectory)
        ));
    }

    #[cfg(all(windows, feature = "native-watch"))]
    #[tokio::test]
    async fn source_page_batch_lookup_transcodes_windows_names_to_workspace_profile() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::write(directory.path().join("café.txt"), b"source").expect("source file");
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Windows,
                VolumeLimits::default(),
            )
            .await
            .expect("Windows source"),
        );
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "windows-source-portable-authored",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach portable authored workspace");
        assert_eq!(root.workspace().profile(), FilesystemProfile::Portable);
        let exact = root
            .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("exactify Windows source into portable authored workspace");
        assert_eq!(
            exact
                .value
                .read("/café.txt", 16)
                .await
                .expect("captured file"),
            Bytes::from_static(b"source")
        );
    }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn exactify_preserves_hard_links_across_batch_boundaries() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        let source_path = directory.path().join("a000");
        std::fs::write(&source_path, b"shared").expect("source file");
        for index in 1..520 {
            std::fs::hard_link(&source_path, directory.path().join(format!("a{index:03}")))
                .expect("source hard link");
        }
        let source = Arc::new(
            NativeDemandSource::open(
                directory.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await
            .expect("source"),
        );
        let root = LazyWorkspace::attach(
            &Fs::memory(),
            "hard-link-batches",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let exact = root
            .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .expect("exactify all batches");
        let first = exact.value.stat("/a000").await.expect("first stat");
        let last = exact.value.stat("/a519").await.expect("last stat");
        assert_eq!(first.file_id, last.file_id);
        assert_eq!(last.link_count, 520);
    }