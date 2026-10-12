use super::*;


#[tokio::test(flavor = "current_thread")]
async fn held_record_lock_is_bounded_without_stalling_unrelated_records() {
    let directory = tempfile::tempdir().expect("local state directory");
    let store = LocalCoreStateStore::new(directory.path());
    let held_id = workspace();
    let other_id = WorkspaceId::from_bytes([0x55; 16]);
    let namespace = store.namespace().expect("admitted namespace");
    let paths = namespace.record(GIT_COMPAT_NAMESPACE, &held_id.into_bytes());
    std::fs::create_dir_all(&paths.directory).expect("lock directory");
    let held = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&paths.lock)
        .expect("held record lock");
    held.lock_exclusive().expect("hold record lock");

    let contender = tokio::spawn(async move { GitCompatStore::load(&store, held_id).await });
    let unrelated = LocalCoreStateStore::new(directory.path());
    tokio::time::timeout(
        Duration::from_secs(2),
        GitCompatStore::load(&unrelated, other_id),
    )
    .await
    .expect("unrelated record must not stall")
    .expect("unrelated record load");
    assert!(matches!(
        contender.await.expect("contender task"),
        Err(LocalCoreStateStoreError::LockTimeout(path)) if path == paths.lock
    ));
    FileExt::unlock(&held).expect("release held lock");
}


#[tokio::test]
async fn state_survives_reopen() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("admitted namespace");
    let state = WorkspaceLineageRecord {
        version: 1,
        revision: 1,
        ready: true,
        workspace_id: workspace(),
        workspace_name: "durable".to_owned(),
        parent_workspace_id: None,
        parent_workspace_name: None,
        fork_generation: crate::GenerationId::new(Digest::from_bytes([3; 32])),
        initial_generation: crate::GenerationId::new(Digest::from_bytes([3; 32])),
    };
    assert!(
        WorkspaceLineageStore::compare_and_swap(&store, workspace(), 0, state.clone())
            .await
            .expect("initial write")
    );
    // A shared commit is written back before its lock is released.
    assert!(
        std::fs::read(core_log_paths(&namespace).current)
            .expect("core log")
            .is_empty()
    );
    WorkspaceLineageStore::load(&store, workspace())
        .await
        .expect("write the record back");
    let current = namespace
        .record("lineage", &workspace().into_bytes())
        .current;
    let reopened = LocalCoreStateStore::new(directory.path());
    assert_eq!(
        WorkspaceLineageStore::load(&reopened, workspace())
            .await
            .expect("reopen"),
        Some(state)
    );
    assert!(
        read_json::<WorkspaceLineageRecord>(&current)
            .expect("written-back record")
            .is_some()
    );
}


#[tokio::test]
async fn lazy_overlay_load_rejects_content_address_mismatch() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let (overlay, node) = overlay_node("/original");
    store
        .put_lazy_overlay(overlay, node)
        .await
        .expect("persist overlay");
    let (_, tampered) = overlay_node("/tampered");
    let paths = store
        .namespace()
        .expect("admitted namespace")
        .record("lazy-overlays", &overlay.into_bytes())
        .current;
    std::fs::write(
        &paths,
        serde_json::to_vec(&tampered).expect("serialize tampered overlay"),
    )
    .expect("tamper overlay");
    assert!(matches!(
        LocalCoreStateStore::new(directory.path())
            .load_lazy_overlay(overlay)
            .await,
        Err(LocalCoreStateStoreError::Integrity)
    ));
}


#[tokio::test]
async fn immutable_nodes_are_served_from_memory_after_first_use() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
    let (overlay, node) = overlay_node("/memoized");
    store
        .put_lazy_overlay(overlay, node.clone())
        .await
        .expect("put overlay");
    std::fs::remove_dir_all(directory.path().join("lazy-overlays")).expect("drop overlays");
    let clone = store.clone();
    assert_eq!(
        clone
            .load_lazy_overlay(overlay)
            .await
            .expect("memoized overlay"),
        Some(node)
    );
    let (_, different) = overlay_node("/different");
    assert!(
        matches!(
            clone.put_lazy_overlay(overlay, different).await,
            Err(LocalCoreStateStoreError::Integrity)
        ),
        "a memoized address still rejects a different value"
    );
}


#[tokio::test]
async fn empty_nodes_are_implied_without_being_stored() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let overlay = LazyOverlayId::from_bytes(
        content_address(&LazyOverlay::default()).expect("empty overlay address"),
    );
    let shadow: crate::LazyShadowId = serde_json::from_value(serde_json::json!(
        content_address(&crate::LazyShadow::default()).expect("empty shadow address")
    ))
    .expect("empty shadow id");
    store
        .put_lazy_overlay(overlay, LazyOverlay::default())
        .await
        .expect("put empty overlay");
    store
        .put_lazy_shadow(shadow, crate::LazyShadow::default())
        .await
        .expect("put empty shadow");
    assert_eq!(
        std::fs::read_dir(directory.path())
            .expect("store root")
            .count(),
        0,
        "empty nodes are never written"
    );
    assert_eq!(
        store
            .load_lazy_overlay(overlay)
            .await
            .expect("empty overlay"),
        Some(LazyOverlay::default())
    );
    assert_eq!(
        store.load_lazy_shadow(shadow).await.expect("empty shadow"),
        Some(crate::LazyShadow::default())
    );
}


#[tokio::test]
async fn git_compatibility_delete_is_revision_fenced_and_durable() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    assert!(
        GitCompatStore::load(&store, workspace())
            .await
            .expect("absent Git state")
            .is_none()
    );
    let mut state = GitCompatState::new("main", workspace());
    state.revision = 1;
    assert!(
        GitCompatStore::compare_and_swap(&store, workspace(), 0, state)
            .await
            .expect("write Git state")
    );
    assert!(
        !GitCompatStore::compare_and_delete(&store, workspace(), 0)
            .await
            .expect("reject stale delete")
    );
    assert!(
        GitCompatStore::compare_and_delete(&store, workspace(), 1)
            .await
            .expect("delete Git state")
    );
    let reopened = LocalCoreStateStore::new(directory.path());
    assert!(
        GitCompatStore::load(&reopened, workspace())
            .await
            .expect("load deleted Git state")
            .is_none()
    );
}


#[tokio::test]
async fn native_lazy_state_uses_original_meter_and_preserves_completed_snapshot_on_restart() {
    let directory = tempfile::tempdir().expect("native core namespace");
    let id = WorkspaceId::from_bytes([0xb1; 16]);
    let mut state = lazy_state(id, 1);
    let snapshot = crate::LazySnapshotRef::new(
        id, crate::GenerationId::new(Digest::from_bytes([0xb2; 32])),
        state.source, state.overlay, state.shadows,
    );
    state.completed_selected_import = Some(crate::lazy_workspace::CompletedSelectedImport {
        operation: OperationId::from_bytes([0xb3; 16]),
        snapshot, destination: snapshot, source: snapshot,
        input: Digest::from_bytes([0xb4; 32]),
        config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
        source_config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
    });
    let cancellation = crate::CancellationToken::new();
    let work = crate::materializer::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED, &cancellation,
    ).expect("original native meter");
    work.charge(crate::WorkCounters { bytes_hashed: 5, ..Default::default() }).expect("original prior producer");
    let store = LocalCoreStateStore::new(directory.path());
    assert!(store.compare_and_swap_lazy_workspace_with_native_work(
        id, 0, state.clone(), &work,
    ).await.expect("native original CAS"));
    let first = store.load_lazy_workspace_with_native_work(id, &work).await
        .expect("native original load").expect("actual state");
    assert_eq!(first.as_state(), &state);
    let (_, allocation) = first.into_parts();
    assert!(work.shares_ledger(allocation.work()));
    drop(allocation);
    assert_eq!(work.receipt().bytes_hashed, 5);
    let before = work.receipt().object_bytes_written;
    let mut stale = state.clone();
    stale.revision += 1;
    assert!(!store.compare_and_swap_lazy_workspace_with_native_work(
        id, 0, stale, &work,
    ).await.expect("original CAS miss"));
    assert_eq!(work.receipt().object_bytes_written, before);
    drop(store);
    let restarted = LocalCoreStateStore::new(directory.path());
    let original = restarted.load_lazy_workspace_with_native_work(id, &work).await
        .expect("real backend restart").expect("original retained state");
    assert_eq!(original.as_state().completed_selected_import, state.completed_selected_import);
    assert_eq!(original.as_state().source, state.source);
    assert_eq!(original.as_state().workspace_id, id);
}


#[tokio::test]
async fn native_lazy_state_exact_read_budget_retains_prior_work_and_typed_cancellation() {
    let directory = tempfile::tempdir().expect("native core namespace");
    let id = WorkspaceId::from_bytes([0xb5; 16]);
    let store = LocalCoreStateStore::new(directory.path());
    assert!(store.compare_and_swap_lazy_workspace(id, 0, lazy_state(id, 1))
        .await.expect("durable fixture"));
    drop(store);
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget { source_bytes_read: 7, ..crate::WorkBudget::UNBOUNDED };
    let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation)
        .expect("original finite meter");
    work.charge(crate::WorkCounters { bytes_hashed: 5, ..Default::default() }).expect("prior actual work");
    let restarted = LocalCoreStateStore::new(directory.path());
    let error = restarted.load_lazy_workspace_with_native_work(id, &work).await
        .expect_err("actual decoder stops at original exact source allowance");
    assert!(matches!(error, crate::LazyWorkspaceError::Demand(crate::DemandError::Work(_))));
    assert_eq!(work.receipt().source_bytes_read, 7);
    assert_eq!(work.receipt().bytes_hashed, 5);
    work.receipt().verify(budget).expect("all actual spent prefix");
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(
        restarted.load_lazy_workspace_with_native_work(id, &work).await,
        Err(crate::LazyWorkspaceError::Cancelled),
    ));
    assert_eq!(work.receipt(), before);
}


#[tokio::test]
async fn measured_lazy_family_preserves_pre_cancelled_failures_without_io() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let workspace = WorkspaceId::from_bytes([41; 16]);
    let overlay = LazyOverlayId::from_bytes([42; 32]);
    let shadow = crate::LazyShadowId::from_bytes([43; 32]);
    let cancellation = crate::CancellationToken::new();
    cancellation.cancel();
    let budget = crate::WorkBudget::UNBOUNDED;
    let errors = [
        store.load_lazy_workspace_measured(workspace, budget, &cancellation).await.unwrap_err(),
        store.compare_and_swap_lazy_workspace_measured(
            workspace, 0, lazy_state(workspace, 1), budget, &cancellation,
        ).await.unwrap_err(),
        store.load_lazy_overlay_measured(overlay, budget, &cancellation).await.unwrap_err(),
        store.put_lazy_overlay_measured(overlay, LazyOverlay::Empty, budget, &cancellation).await.unwrap_err(),
        store.load_lazy_shadow_measured(shadow, budget, &cancellation).await.unwrap_err(),
        store.put_lazy_shadow_measured(shadow, crate::LazyShadow::Empty, budget, &cancellation).await.unwrap_err(),
    ];
    for error in errors {
        assert!(matches!(error.semantic_error(), crate::LazyWorkspaceError::Cancelled));
        assert_eq!(error.spent_work(), crate::WorkCounters::UNCHARGED);
    }
    assert!(std::fs::read_dir(directory.path()).expect("namespace entries").next().is_none());
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_lazy_workspace_cache_is_never_stale_under_concurrency() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let directory = tempfile::tempdir().expect("temporary directory");
    let workspaces = [
        WorkspaceId::from_bytes([0x61; 16]),
        WorkspaceId::from_bytes([0x62; 16]),
    ];
    let seed = LocalCoreStateStore::new(directory.path());
    for workspace_id in workspaces {
        assert!(
            seed.compare_and_swap_lazy_workspace(workspace_id, 0, lazy_state(workspace_id, 1))
                .await
                .expect("seed binding")
        );
    }
    drop(seed);

    // Every binding is durable but uncached, so first loads race their
    // fills against concurrent writes.
    let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
    let committed = Arc::new(workspaces.map(|workspace_id| (workspace_id, AtomicU64::new(1))));
    let tasks = (0..8_usize)
        .map(|task| {
            let owner = owner.clone();
            let committed = Arc::clone(&committed);
            tokio::spawn(async move {
                let mut wins = 0_u64;
                for round in 0..64_usize {
                    let (workspace_id, floor) = committed
                        .get((task + round) % committed.len())
                        .expect("binding slot");
                    let floor_before_load = floor.load(Ordering::SeqCst);
                    let current = owner
                        .load_lazy_workspace(*workspace_id)
                        .await
                        .expect("load binding")
                        .expect("binding is present");
                    assert!(
                        current.revision >= floor_before_load,
                        "load returned revision {} after revision {floor_before_load} committed",
                        current.revision
                    );
                    let expected_revision = current.revision;
                    let replacement = LazyWorkspaceState {
                        revision: expected_revision + 1,
                        ..current
                    };
                    let revision = replacement.revision;
                    if owner
                        .compare_and_swap_lazy_workspace(
                            *workspace_id,
                            expected_revision,
                            replacement,
                        )
                        .await
                        .expect("swap binding")
                    {
                        floor.fetch_max(revision, Ordering::SeqCst);
                        wins += 1;
                    }
                }
                wins
            })
        })
        .collect::<Vec<_>>();
    let mut wins = 0;
    for task in tasks {
        wins += task.await.expect("contender task");
    }

    let mut advanced = 0;
    let mut cached = Vec::new();
    for (workspace_id, floor) in committed.iter() {
        let state = owner
            .load_lazy_workspace(*workspace_id)
            .await
            .expect("load final binding")
            .expect("final binding is present");
        assert_eq!(state.revision, floor.load(Ordering::SeqCst));
        advanced += state.revision - 1;
        cached.push(state);
    }
    assert_eq!(
        advanced, wins,
        "every successful swap advanced exactly once"
    );

    drop(owner);
    let durable = LocalCoreStateStore::new(directory.path());
    for state in cached {
        assert_eq!(
            durable
                .load_lazy_workspace(state.workspace_id)
                .await
                .expect("load durable binding"),
            Some(state)
        );
    }
}


/// Collection removes every node no lazy workspace reaches, keeps every node one does, and a node put again after removal is stored again.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_node_collection_keeps_exactly_the_reachable_nodes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
    let empty = LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty"));
    let (leaf, leaf_node) = linked_overlay("/a", empty, empty);
    let (root, root_node) = linked_overlay("/b", leaf, empty);
    let (orphan, orphan_node) = linked_overlay("/old", empty, empty);
    for (id, node) in [
        (leaf, leaf_node),
        (root, root_node),
        (orphan, orphan_node.clone()),
    ] {
        store.put_lazy_overlay(id, node).await.expect("put overlay");
    }
    let empty_shadows = crate::LazyShadowId::from_bytes(
        content_address(&crate::LazyShadow::Empty).expect("empty shadow"),
    );
    // Collection walks the index, not the records it holds.
    let record = vec![1, 2, 3];
    let metadata =
        crate::WorkspaceMetadata::from_engine(crate::kernel::FileMetadata::default());
    let shadow = crate::LazyShadow::Node {
        file_id: crate::foundation::FileId::from_bytes([7; 16]),
        priority: 0,
        record: record.clone(),
        metadata: Box::new(metadata),
        left: empty_shadows,
        right: empty_shadows,
    };
    let shadow_id =
        crate::LazyShadowId::from_bytes(content_address(&shadow).expect("shadow address"));
    store
        .put_lazy_shadow(shadow_id, shadow)
        .await
        .expect("put shadow");
    let orphan_shadow = crate::LazyShadow::Node {
        file_id: crate::foundation::FileId::from_bytes([8; 16]),
        priority: 0,
        record,
        metadata: Box::new(metadata),
        left: empty_shadows,
        right: empty_shadows,
    };
    let orphan_shadow_id = crate::LazyShadowId::from_bytes(
        content_address(&orphan_shadow).expect("orphan shadow address"),
    );
    store
        .put_lazy_shadow(orphan_shadow_id, orphan_shadow)
        .await
        .expect("put orphan shadow");
    let mut state = lazy_state(workspace(), 1);
    state.overlay = root;
    state.shadows = shadow_id;
    assert!(
        store
            .compare_and_swap_lazy_workspace(workspace(), 0, state)
            .await
            .expect("bind lazy workspace")
    );

    assert_eq!(store.collect_lazy_nodes().await.expect("collect"), 2);
    let reopened = LocalCoreStateStore::new(directory.path());
    drop(store);
    for id in [root, leaf] {
        assert!(
            reopened
                .load_lazy_overlay(id)
                .await
                .expect("load")
                .is_some()
        );
    }
    assert!(
        reopened
            .load_lazy_shadow(shadow_id)
            .await
            .expect("load")
            .is_some()
    );
    assert!(
        reopened
            .load_lazy_overlay(orphan)
            .await
            .expect("load")
            .is_none()
    );
    assert!(
        reopened
            .load_lazy_shadow(orphan_shadow_id)
            .await
            .expect("load")
            .is_none()
    );
    reopened
        .put_lazy_overlay(orphan, orphan_node)
        .await
        .expect("put again");
    assert!(
        reopened
            .load_lazy_overlay(orphan)
            .await
            .expect("load")
            .is_some()
    );
}


#[tokio::test(flavor = "multi_thread")]
async fn first_prepared_freeze_rejects_concurrent_source_update_and_collected_roots() {
    for component in 0..3 {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
        let parent = origin_parent_fixture(&store).await;
        let prepared = prepared_origin_fixture(&parent);
        let advanced = advanced_origin_parent(&parent);
        let mut updated = LazyWorkspaceState { revision: parent.revision + 1, ..parent.clone() };
        match component {
            0 => updated.source = advanced.source,
            1 => updated.overlay = advanced.overlay,
            _ => updated.shadows = advanced.shadows,
        }
        let updater = store.clone();
        let update = updated.clone();
        let expected = parent.revision;
        assert!(tokio::spawn(async move {
            updater.compare_and_swap_lazy_workspace(update.workspace_id, expected, update)
                .await.expect("concurrent source binding update")
        }).await.expect("update task"));
        assert_eq!(store.collect_lazy_nodes().await.expect("collect old source roots"), u64::from(component != 0));
        if component == 1 {
            assert!(store.load_lazy_overlay(parent.overlay).await.expect("old overlay").is_none());
        } else if component == 2 {
            assert!(store.load_lazy_shadow(parent.shadows).await.expect("old shadow").is_none());
        }

        let cancellation = crate::CancellationToken::new();
        let rejected = store.compare_and_swap_lazy_workspace_measured(
            prepared.workspace_id, 0, prepared.clone(), crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.expect("stale freeze is a CAS miss");
        assert!(!rejected.value);
        assert!(rejected.work.items_examined > 0);
        assert_eq!(rejected.work.object_bytes_written, 0);
        assert!(store.load_lazy_workspace(prepared.workspace_id).await.expect("child").is_none());

        let refreshed = prepared_origin_fixture(&updated);
        assert!(store.compare_and_swap_lazy_workspace(
            refreshed.workspace_id, 0, refreshed,
        ).await.expect("current source freeze"));
    }
}


#[tokio::test(flavor = "multi_thread")]
async fn original_freeze_recovers_without_ack_and_retains_roots_after_parent_changes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
    let parent = origin_parent_fixture(&store).await;
    let prepared = prepared_origin_fixture(&parent);
    // The caller loses the first write's acknowledgement, then recovers
    // only the authentic persisted original record after reopening.
    let _ = store.compare_and_swap_lazy_workspace(
        prepared.workspace_id, 0, prepared.clone(),
    ).await.expect("durable original freeze");
    drop(store);
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("reopened store");
    assert_eq!(store.load_lazy_workspace(prepared.workspace_id).await.expect("recover"), Some(prepared.clone()));
    let updated = advanced_origin_parent(&parent);
    assert!(store.compare_and_swap_lazy_workspace(
        parent.workspace_id, parent.revision, updated.clone(),
    ).await.expect("advance mutable parent"));
    assert_eq!(store.collect_lazy_nodes().await.expect("prepared origin roots"), 0);
    assert!(!store.compare_and_swap_lazy_workspace(
        prepared.workspace_id, 0, prepared.clone(),
    ).await.expect("lost acknowledgement retry is revision fenced"));

    let mut committed = prepared.clone();
    committed.revision += 1;
    committed.fork_origin.as_mut().expect("origin").phase = crate::LazyForkPhase::Committed;
    assert!(store.compare_and_swap_lazy_workspace(
        committed.workspace_id, prepared.revision, committed.clone(),
    ).await.expect("commit original without rereading mutable parent"));
    let child = LazyWorkspaceState {
        revision: committed.revision + 1,
        source: updated.source,
        overlay: updated.overlay,
        shadows: updated.shadows,
        ..committed.clone()
    };
    assert!(store.compare_and_swap_lazy_workspace(
        child.workspace_id, committed.revision, child.clone(),
    ).await.expect("later child observation epoch"));
    assert_eq!(store.collect_lazy_nodes().await.expect("committed origin roots"), 0);
    assert!(store.load_lazy_overlay(parent.overlay).await.expect("original overlay retained").is_some());
    assert!(store.load_lazy_shadow(parent.shadows).await.expect("original shadows retained").is_some());
    let origin = prepared.fork_origin.expect("original");
    assert_eq!(store.lazy_retained_generations().await.expect("original generation roots"),
        [(origin.snapshot.authored_generation, origin.config)]);

    let next = LazyWorkspaceState { revision: child.revision + 1, ..child.clone() };
    let mut removed = next.clone();
    removed.fork_origin = None;
    let mut new_operation = next.clone();
    new_operation.fork_origin.as_mut().expect("origin").operation_id = OperationId::from_bytes([0xb0; 16]);
    let mut new_config = next.clone();
    new_config.fork_origin.as_mut().expect("origin").config.limits.maximum_read_bytes += 1;
    let mut new_snapshot = next.clone();
    new_snapshot.fork_origin.as_mut().expect("origin").snapshot.authored_generation =
        crate::GenerationId::new(Digest::from_bytes([0xb1; 32]));
    let mut new_parent = next.clone();
    new_parent.parent_workspace_id = Some(WorkspaceId::from_bytes([0xb2; 16]));
    let mut regressed = next;
    regressed.fork_origin.as_mut().expect("origin").phase = crate::LazyForkPhase::Prepared;
    for replacement in [removed, new_operation, new_config, new_snapshot, new_parent, regressed] {
        assert!(!store.compare_and_swap_lazy_workspace(
            child.workspace_id, child.revision, replacement,
        ).await.expect("immutable original rejects replacement"));
    }
    assert_eq!(store.load_lazy_workspace(child.workspace_id).await.expect("unchanged child"), Some(child));
}
