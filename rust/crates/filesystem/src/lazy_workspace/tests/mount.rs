use super::*;


    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_promotion_cancels_midstream_without_installing_a_partial_candidate() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from(vec![
            0x5a;
            2 * 1024 * 1024
        ])));
        let root = LazyWorkspace::attach(
            &fs,
            "cancelled-mount-promotion",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let head = root.workspace().head().await.expect("head").id();
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("checkout");
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected unresolved source file");
        };
        let cancellation = CancellationToken::new();
        source.cancel_next_range();
        assert!(matches!(
            root.stage_exact_into_checkout(
                &mut checkout,
                "/file.txt",
                root.source_file_id(&node),
                u64::MAX,
                &cancellation,
            )
            .await,
            Err(LazyWorkspaceError::Cancelled)
        ));
        assert!(cancellation.is_cancelled());
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 1);
        assert_eq!(root.workspace().head().await.expect("head").id(), head);
        assert!(
            checkout
                .lookup_no_follow(
                    &root.namespace_path("/file.txt").expect("path"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .expect("candidate lookup")
                .value
                .record
                .is_none()
        );
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_promotion_stays_in_the_dirty_candidate_until_one_publication() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "staged-mount-promotion",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let original_head = root.workspace().head().await.expect("head").id();
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        let other = root.namespace_path("/other.txt").expect("other path");
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: other.clone(),
                    bytes: Bytes::from_static(b"dirty"),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("dirty mounted write");
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected unresolved source file");
        };
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            root.stage_exact_into_checkout(
                &mut checkout,
                "/file.txt",
                root.source_file_id(&node),
                u64::MAX,
                &cancelled,
            )
            .await,
            Err(LazyWorkspaceError::Cancelled)
        ));
        assert!(
            root.stage_exact_into_checkout(
                &mut checkout,
                "/file.txt",
                root.source_file_id(&node),
                u64::MAX,
                &CancellationToken::new(),
            )
            .await
            .expect("stage source in dirty mount")
        );
        assert!(
            !root
                .stage_exact_into_checkout(
                    &mut checkout,
                    "/file.txt",
                    root.source_file_id(&node),
                    u64::MAX,
                    &CancellationToken::new(),
                )
                .await
                .expect("already staged source identity")
        );
        assert_eq!(
            root.workspace()
                .head()
                .await
                .expect("unpublished head")
                .id(),
            original_head
        );
        let file = root.namespace_path("/file.txt").expect("file path");
        for path in [&other, &file] {
            assert!(
                checkout
                    .lookup_no_follow(path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .expect("candidate lookup")
                    .value
                    .record
                    .is_some()
            );
        }
        checkout
            .commit(
                crate::OperationId::new(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("publish both changes once");
        assert_ne!(
            root.workspace().head().await.expect("published head").id(),
            original_head
        );
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_alias_promotions_preserve_source_hard_links_before_publication() {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"shared"));
        source.node.link_count = Some(2);
        let root = LazyWorkspace::attach(
            &fs,
            "staged-hard-link-promotions",
            Arc::new(source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        for path in ["/a", "/b"] {
            let LazyLookup::Source(node) = root.lookup(path).await.expect("source alias") else {
                panic!("expected unresolved source alias");
            };
            assert!(
                root.stage_exact_into_checkout(
                    &mut checkout,
                    path,
                    root.source_file_id(&node),
                    u64::MAX,
                    &CancellationToken::new(),
                )
                .await
                .expect("stage source alias")
            );
        }
        let first = checkout
            .lookup_no_follow(
                &root.namespace_path("/a").expect("first path"),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("first alias")
            .value
            .record
            .expect("first record");
        let second = checkout
            .lookup_no_follow(
                &root.namespace_path("/b").expect("second path"),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("second alias")
            .value
            .record
            .expect("second record");
        assert_eq!(first.file_id, second.file_id);
        assert_eq!(first.link_count, 2);
        assert_eq!(second.link_count, 2);
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_directory_pages_include_unpublished_checkout_bindings() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "mounted-directory-candidate",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: root.namespace_path("/new.txt").expect("new path"),
                    bytes: Bytes::from_static(b"new"),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("unpublished file");
        let source_page = root
            .list_directory_in_checkout(&mut checkout, "/", None, 16, None)
            .await
            .map(|(page, _)| page)
            .expect("source page");
        assert_eq!(source_page.entries.len(), 1);
        assert!(!source_page.entries[0].authored);
        let authored_page = root
            .list_directory_in_checkout(&mut checkout, "/", source_page.next, 16, None)
            .await
            .map(|(page, _)| page)
            .expect("authored page");
        assert_eq!(authored_page.entries.len(), 1);
        assert!(authored_page.entries[0].authored);
        assert_eq!(authored_page.entries[0].name.as_bytes(), b"new.txt");
        assert!(authored_page.next.is_none());
    }

    #[cfg(all(
        feature = "native-mount",
        feature = "native-watch",
        not(target_arch = "wasm32")
    ))]
    #[tokio::test]
    async fn mounted_replacement_directory_does_not_expose_obsolete_source_children() {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{FilesystemProfile, VolumeLimits};

        let directory = tempfile::tempdir().expect("temporary source");
        std::fs::create_dir(directory.path().join("d")).expect("source directory");
        std::fs::write(directory.path().join("d").join("old.txt"), b"old").expect("source child");
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
            "mounted-directory-replacement",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("mount checkout");
        assert_eq!(
            root.list_directory_in_checkout(&mut checkout, "/d", None, 16, None)
                .await
                .expect("source directory")
                .0
                .entries
                .len(),
            1
        );
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateDirectory {
                    path: root.namespace_path("/d").expect("replacement path"),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("replace with empty authored directory");
        let page = root
            .list_directory_in_checkout(&mut checkout, "/d", None, 16, None)
            .await
            .map(|(page, _)| page)
            .expect("replacement directory");
        assert!(page.entries.is_empty());
        assert!(page.next.is_none());
    }

    #[tokio::test]
    async fn source_only_remove_does_not_advance_the_authored_head() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = LazyWorkspace::attach(
            &fs,
            "source-only-remove-effect",
            source,
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let original_head = root.workspace().head().await.expect("head").id();
        let LazyLookup::Source(node) = root.lookup("/file.txt").await.expect("source") else {
            panic!("expected source file");
        };
        root.remove_if("/file.txt", Some(root.source_file_id(&node)))
            .await
            .expect("source-only tombstone");
        assert_eq!(
            root.workspace().head().await.expect("unchanged head").id(),
            original_head
        );
        assert!(matches!(
            root.lookup("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn mounted_whiteout_follows_the_checkout_publication_decision() {
        let fs = Fs::memory();
        let root = LazyWorkspace::attach(
            &fs,
            "mounted-whiteout-publication",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        let paths = vec!["/file.txt".to_owned()];
        let aborted = IdempotencyKey::new();
        root.prepare_mount_removals(&paths, &[], aborted)
            .await
            .expect("prepare uncommitted whiteout");
        assert!(!root.finish_mount_removals().await.expect("rollback"));
        assert!(matches!(
            root.lookup("/file.txt").await.expect("source remains"),
            LazyLookup::Source(_)
        ));

        let committed = IdempotencyKey::new();
        root.prepare_mount_removals(&paths, &[], committed)
            .await
            .expect("prepare committed whiteout");
        let mut checkout = root
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await
            .expect("checkout");
        assert!(matches!(
            checkout
                .commit_with_permit_even_if_clean(
                    committed.operation_id(),
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .expect("publish unchanged authored root")
                .value,
            crate::CheckoutCommitOutcome::Committed { .. }
        ));
        assert!(root.finish_mount_removals().await.expect("finalize"));
        assert!(matches!(
            root.lookup("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
    }

    /// A mounted removal shadows an identity whose content only an
    /// unpublished checkout staged; publication then flushes a generation
    /// without it. The shadow must never outlive that content across a
    /// power loss, which drops everything staged and unflushed.
    #[cfg(all(feature = "native-mount", feature = "local", any(unix, windows)))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shadowed_identity_keeps_its_content_across_power_loss() {
        let directory = tempfile::tempdir().expect("tempdir");
        let fs_root = directory.path().join("fs");
        let state_root = directory.path().join("state");
        let content = Bytes::from(vec![0x5a; 256 * 1024]);
        let cancellation = CancellationToken::new();
        let removal = IdempotencyKey::new();
        let path = NamespacePath::new(
            vec![LogicalName::new(NameEncoding::Utf8, b"removed.bin".to_vec(), 255).expect("name")],
            crate::model::VolumeLimits::default(),
        )
        .expect("path");
        let shadowed;
        {
            let fs = Fs::local(crate::LocalOptions::new(&fs_root))
                .await
                .expect("fs");
            let released = fs.local_root_release_barrier().expect("release barrier");
            let root = LazyWorkspace::attach(
                &fs,
                "power-loss-shadow",
                Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
                crate::core_state::LocalCoreStateStore::new(&state_root),
            )
            .await
            .expect("attach");
            // A mounted write stages content in the checkout, then the file
            // is removed before any publication, as `rm` after a write does.
            let mut checkout = root
                .workspace()
                .checkout(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                )
                .await
                .expect("checkout");
            checkout
                .create_file(
                    path.clone(),
                    content.clone(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .expect("stage content");
            let identity = checkout
                .lookup_no_follow_with_metadata(&path, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .expect("lookup")
                .value
                .expect("staged file");
            checkout
                .remove(path.clone(), None, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .expect("remove");
            root.prepare_mount_removals(&[], &[(identity.record, identity.metadata)], removal)
                .await
                .expect("shadow the removed identity");
            checkout
                .commit_with_permit_even_if_clean(
                    removal.operation_id(),
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .expect("publish without the removed file");
            assert!(root.finish_mount_removals().await.expect("finish"));
            shadowed = identity.record;
            // Power loss: nothing staged survives the engine.
            drop((checkout, root, fs));
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !released.is_released() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("release");
        }
        let fs = Fs::local(crate::LocalOptions::new(&fs_root))
            .await
            .expect("reopen");
        let reopened = LazyWorkspace::open(
            fs.open_workspace("power-loss-shadow")
                .await
                .expect("workspace"),
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            crate::core_state::LocalCoreStateStore::new(&state_root),
        )
        .await
        .expect("reopen lazy");
        let state = reopened.state().await.expect("state");
        let shadow = reopened
            .shadow_record_measured(
                state.shadows,
                shadowed.file_id,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("shadow lookup")
            .value
            .expect("the removed identity stays shadowed");
        let read = reopened
            .workspace()
            .detached_record(shadow.0)
            .read_range(
                crate::ByteRange {
                    offset: 0,
                    length: u64::try_from(content.len()).expect("length"),
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .expect("the shadowed identity's content survived");
        assert_eq!(read.value.bytes, content);
    }