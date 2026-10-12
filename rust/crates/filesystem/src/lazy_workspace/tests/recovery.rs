use super::*;


    #[tokio::test]
    async fn directory_pages_emit_authored_entries_once_after_source_phase() {
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

        let source_page = root
            .list_directory("/", None, 16)
            .await
            .expect("source phase");
        assert!(source_page.entries.is_empty());
        let authored_page = root
            .list_directory("/", source_page.next, 16)
            .await
            .expect("authored phase");
        assert_eq!(authored_page.entries.len(), 1);
        assert!(authored_page.entries[0].authored);
        assert!(authored_page.next.is_none());
    }

    #[tokio::test]
    async fn directory_cursor_rejects_overlay_changes_between_pages() {
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
        let first = root
            .list_directory("/", None, 16)
            .await
            .expect("first page");
        root.stat("/newly-observed.txt").await.expect("observe");

        assert!(matches!(
            root.list_directory("/", first.next, 16).await,
            Err(LazyWorkspaceError::StaleCursor)
        ));
    }

    #[tokio::test]
    async fn failed_authored_remove_rolls_back_source_tombstone() {
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
        root.write("/directory/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        let before = root.state().await.expect("state");

        assert!(matches!(
            root.remove("/directory").await,
            Err(LazyWorkspaceError::Workspace(_))
        ));
        let after = root.state().await.expect("state after failure");
        assert_eq!(after.overlay, before.overlay);
        assert_eq!(after.shadows, before.shadows);
        assert!(after.pending_remove.is_none());
        assert!(root.stat("/directory").await.expect("directory").authored);
    }

    #[tokio::test]
    async fn open_recovers_remove_intent_from_authored_state() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        root.write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("write");
        let prior = root.state().await.expect("state");
        let LazyLookup::Authored { stat, .. } = root.lookup("/file.txt").await.expect("authored")
        else {
            panic!("expected authored file");
        };
        let record = root
            .workspace()
            .record_by_id(stat.file_id)
            .await
            .expect("authored record");
        let staged_shadows = root
            .insert_shadow(prior.shadows, stat.file_id, record, stat.metadata)
            .await
            .expect("staged identity shadow");
        assert_ne!(staged_shadows, prior.shadows);
        let tombstone = root
            .insert_overlay(
                prior.overlay,
                "/file.txt".to_owned(),
                LazyOverlayChange::Tombstone { removed: None },
            )
            .await
            .expect("tombstone");
        let first_key = IdempotencyKey::from_bytes([0x31; 16]);
        let pending = LazyWorkspaceState {
            revision: prior.revision + 1,
            overlay: tombstone,
            shadows: staged_shadows,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: prior.overlay,
                prior_shadows: prior.shadows,
                prepared_overlay: tombstone,
                kind: PendingLazyRemoveKind::Authored {
                    idempotency_key: first_key,
                },
            }),
            ..prior.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(root.workspace().id(), prior.revision, pending)
                .await
                .expect("prepare")
        );

        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store.clone(),
        )
        .await
        .expect("recover before physical remove");
        let recovered = reopened.state().await.expect("recovered state");
        assert_eq!(recovered.overlay, prior.overlay);
        assert_eq!(recovered.shadows, prior.shadows);
        assert!(recovered.pending_remove.is_none());

        let committed_key = IdempotencyKey::from_bytes([0x32; 16]);
        let prepared = LazyWorkspaceState {
            revision: recovered.revision + 1,
            overlay: tombstone,
            shadows: staged_shadows,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: recovered.overlay,
                prior_shadows: recovered.shadows,
                prepared_overlay: tombstone,
                kind: PendingLazyRemoveKind::Authored {
                    idempotency_key: committed_key,
                },
            }),
            ..recovered.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(
                    reopened.workspace().id(),
                    recovered.revision,
                    prepared
                )
                .await
                .expect("prepare second")
        );
        let mut removal = reopened
            .workspace()
            .begin_transaction(committed_key)
            .await
            .expect("removal transaction");
        removal.remove("/file.txt").await.expect("stage remove");
        assert!(matches!(
            removal.commit().await.expect("physical remove"),
            TransactionCommit::Committed(_)
        ));

        let finalized = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover after physical remove");
        let final_state = finalized.state().await.expect("final state");
        assert_eq!(final_state.overlay, tombstone);
        assert_eq!(final_state.shadows, staged_shadows);
        assert!(matches!(
            finalized.stat("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn source_only_remove_recovery_keeps_tombstone_across_a_competing_create() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        root.stat("/file.txt").await.expect("observe source");
        let prior = root.state().await.expect("state");
        let tombstone = root
            .insert_overlay(
                prior.overlay,
                "/file.txt".to_owned(),
                LazyOverlayChange::Tombstone { removed: None },
            )
            .await
            .expect("tombstone");
        let pending = LazyWorkspaceState {
            revision: prior.revision + 1,
            overlay: tombstone,
            pending_remove: Some(PendingLazyRemove {
                prior_overlay: prior.overlay,
                prior_shadows: prior.shadows,
                prepared_overlay: tombstone,
                kind: PendingLazyRemoveKind::SourceOnly,
            }),
            ..prior.clone()
        };
        assert!(
            store
                .compare_and_swap_lazy_workspace(root.workspace().id(), prior.revision, pending)
                .await
                .expect("prepare")
        );
        root.workspace()
            .write("/file.txt", Bytes::from_static(b"authored"))
            .await
            .expect("competing authored create");

        let reopened = LazyWorkspace::open(
            fs.open_workspace("root").await.expect("workspace"),
            Arc::clone(&source),
            store,
        )
        .await
        .expect("recover source removal");
        assert_eq!(reopened.state().await.expect("state").overlay, tombstone);
        assert_eq!(
            reopened
                .read("/file.txt", 64)
                .await
                .expect("authored replacement"),
            Bytes::from_static(b"authored")
        );
        reopened.remove("/file.txt").await.expect("remove authored");
        assert!(matches!(
            reopened.stat("/file.txt").await,
            Err(LazyWorkspaceError::NotFound)
        ));
    }

    #[tokio::test]
    async fn observation_index_stays_balanced_and_nodes_stay_constant_size() {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let store = MemoryLazyWorkspaceStore::default();
        let root = LazyWorkspace::attach(&fs, "root", Arc::clone(&source), store.clone())
            .await
            .expect("attach");
        for index in 0..1_000 {
            root.stat(&format!("/paths/{index:04}.txt"))
                .await
                .expect("observe");
        }
        let root_id = root.state().await.expect("state").overlay;
        let state = store
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        fn depth(overlays: &BTreeMap<LazyOverlayId, LazyOverlay>, id: LazyOverlayId) -> usize {
            match &overlays[&id] {
                LazyOverlay::Empty => 0,
                LazyOverlay::Node { left, right, .. } => {
                    1 + depth(overlays, *left).max(depth(overlays, *right))
                }
            }
        }
        assert!(depth(&state.overlays, root_id) < 64);
        assert!(state.overlays.values().all(|overlay| {
            serde_json::to_vec(overlay)
                .expect("encode overlay node")
                .len()
                < 2_048
        }));
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1_000);
    }

    #[test]
    fn million_path_exactify_capture_memory_is_exactly_admitted() {
        const PATHS: u64 = 1_000_000;

        fn capture(budget: WorkBudget) -> Result<(WorkCounters, u64), LazyWorkspaceError> {
            let mut paths = Vec::new();
            let mut retained = 0_u64;
            let mut work = WorkCounters::default();
            for index in 0..PATHS {
                let path = format!("/wide/{index:06}.txt");
                reserve_exactify_path_slot(&mut paths, &mut retained, &mut work, budget)?;
                retain_exactify_string(&path, &mut retained, &mut work, budget)?;
                paths.push(path);
            }
            let expected = exactify_path_vector_bytes(&paths)?
                + paths
                    .iter()
                    .map(|path| u64::try_from(path.capacity()).unwrap_or(u64::MAX))
                    .sum::<u64>();
            assert_eq!(retained, expected);
            Ok((work, retained))
        }

        let (work, retained) = capture(WorkBudget::UNBOUNDED).expect("unbounded capture");
        assert!(work.peak_allocation_bytes >= retained);
        assert!(work.allocation_operations >= PATHS);
        let mut insufficient = WorkBudget::UNBOUNDED;
        insufficient.peak_allocation_bytes = work.peak_allocation_bytes - 1;
        assert!(matches!(
            capture(insufficient),
            Err(LazyWorkspaceError::Work(_))
        ));
    }
