use super::*;

#[tokio::test(flavor = "current_thread")]
async fn held_record_lock_is_bounded_without_stalling_unrelated_records() {
    let directory = tempfile::tempdir().expect("local state directory");
    let store = LocalCoreStateStore::new(directory.path());
    let held_id = workspace();
    let other_id = WorkspaceId::from_bytes([0x55; 16]);
    let namespace = store.namespace().expect("admitted namespace");
    let paths = namespace
        .record(GIT_COMPAT_NAMESPACE, &held_id.into_bytes())
        .expect("record paths");
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
    let reopened = LocalCoreStateStore::new(directory.path());
    assert_eq!(
        WorkspaceLineageStore::load(&reopened, workspace())
            .await
            .expect("reopen"),
        Some(state)
    );
}

#[tokio::test]
async fn lazy_overlay_load_rejects_content_address_mismatch() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::new(directory.path());
    let empty = LazyOverlayId::from_bytes([0; 32]);
    let (overlay, node) = linked_overlay("/original", empty, empty);
    let allocation = accounting
        .claim_owned(node.owned_bytes(), 0)
        .expect("overlay allocation");
    store
        .put_lazy_overlay(overlay, (node, allocation), &mut accounting)
        .await
        .expect("persist overlay");
    let (_, tampered) = linked_overlay("/tampered", empty, empty);
    let paths = store
        .namespace()
        .expect("admitted namespace")
        .record("lazy-overlays", &overlay.into_bytes())
        .expect("record paths")
        .current;
    std::fs::write(
        &paths,
        serde_json::to_vec(&tampered).expect("serialize tampered overlay"),
    )
    .expect("tamper overlay");
    assert!(matches!(
        LocalCoreStateStore::new(directory.path())
            .load_lazy_overlay(overlay, &mut accounting)
            .await,
        Err(crate::LazyWorkspaceError::NativeRecordIntegrity)
    ));
}

#[tokio::test]
async fn immutable_nodes_are_served_from_memory_after_first_use() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
    let empty = LazyOverlayId::from_bytes([0; 32]);
    let (overlay, node) = linked_overlay("/memoized", empty, empty);
    let value = node.clone();
    let allocation = accounting
        .claim_owned(value.owned_bytes(), 0)
        .expect("overlay allocation");
    store
        .put_lazy_overlay(overlay, (value, allocation), &mut accounting)
        .await
        .expect("put overlay");
    std::fs::remove_dir_all(directory.path().join("lazy-overlays")).expect("drop overlays");
    let clone = store.clone();
    assert_eq!(
        clone
            .load_lazy_overlay(overlay, &mut accounting)
            .await
            .expect("memoized overlay")
            .as_ref()
            .map(|(value, _)| value),
        Some(&node)
    );
    let (_, different) = linked_overlay("/different", empty, empty);
    let allocation = accounting
        .claim_owned(different.owned_bytes(), 0)
        .expect("different overlay allocation");
    assert!(
        matches!(
            clone
                .put_lazy_overlay(overlay, (different, allocation), &mut accounting)
                .await,
            Err(crate::LazyWorkspaceError::NativeRecordIntegrity)
        ),
        "a memoized address still rejects a different value"
    );
}

#[tokio::test]
async fn empty_nodes_are_implied_without_being_stored() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
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
        .put_lazy_overlay(
            overlay,
            (
                LazyOverlay::default(),
                crate::kernel::KernelOwnedAllocation::Standalone,
            ),
            &mut accounting,
        )
        .await
        .expect("put empty overlay");
    store
        .put_lazy_shadow(
            shadow,
            (
                crate::LazyShadow::default(),
                crate::kernel::KernelOwnedAllocation::Standalone,
            ),
            &mut accounting,
        )
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
            .load_lazy_overlay(overlay, &mut accounting)
            .await
            .expect("empty overlay")
            .as_ref()
            .map(|(value, _)| value),
        Some(&LazyOverlay::default())
    );
    assert_eq!(
        store
            .load_lazy_shadow(shadow, &mut accounting)
            .await
            .expect("empty shadow")
            .as_ref()
            .map(|(value, _)| value),
        Some(&crate::LazyShadow::default())
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
        id,
        crate::GenerationId::new(Digest::from_bytes([0xb2; 32])),
        state.source,
        state.overlay,
        state.shadows,
    );
    state.completed_selected_import = Some(crate::lazy_workspace::CompletedSelectedImport {
        operation: OperationId::from_bytes([0xb3; 16]),
        snapshot,
        destination: snapshot,
        source: snapshot,
        input: Digest::from_bytes([0xb4; 32]),
        config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
        source_config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
    });
    let cancellation = crate::CancellationToken::new();
    let work = crate::materializer::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .expect("original native meter");
    work.charge(crate::WorkCounters {
        bytes_hashed: 5,
        ..Default::default()
    })
    .expect("original prior producer");
    let mut accounting = crate::kernel::KernelAccounting::native(&work, work.cancellation());
    let store = LocalCoreStateStore::new(directory.path());
    assert!(
        store
            .compare_and_swap_lazy_workspace(id, 0, state.clone(), &mut accounting)
            .await
            .expect("native original CAS")
    );
    let first = store
        .load_lazy_workspace(id, &mut accounting)
        .await
        .expect("native original load")
        .expect("actual state");
    assert_eq!(&first, &state);
    assert_eq!(work.receipt().bytes_hashed, 5);
    let before = work.receipt().object_bytes_written;
    let mut stale = state.clone();
    stale.revision += 1;
    assert!(
        !store
            .compare_and_swap_lazy_workspace(id, 0, stale, &mut accounting)
            .await
            .expect("original CAS miss")
    );
    assert_eq!(work.receipt().object_bytes_written, before);
    drop(store);
    let restarted = LocalCoreStateStore::new(directory.path());
    let original = restarted
        .load_lazy_workspace(id, &mut accounting)
        .await
        .expect("real backend restart")
        .expect("original retained state");
    assert_eq!(
        original.completed_selected_import,
        state.completed_selected_import
    );
    assert_eq!(original.source, state.source);
    assert_eq!(original.workspace_id, id);
}

#[tokio::test]
async fn native_lazy_state_exact_read_budget_retains_prior_work_and_typed_cancellation() {
    let directory = tempfile::tempdir().expect("native core namespace");
    let id = WorkspaceId::from_bytes([0xb5; 16]);
    let store = LocalCoreStateStore::new(directory.path());
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                id,
                0,
                lazy_state(id, 1),
                &mut crate::kernel::KernelAccounting::standalone(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
            )
            .await
            .expect("durable fixture")
    );
    drop(store);
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        source_bytes_read: 7,
        ..crate::WorkBudget::UNBOUNDED
    };
    let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation)
        .expect("original finite meter");
    work.charge(crate::WorkCounters {
        bytes_hashed: 5,
        ..Default::default()
    })
    .expect("prior actual work");
    let mut accounting = crate::kernel::KernelAccounting::native(&work, work.cancellation());
    let restarted = LocalCoreStateStore::new(directory.path());
    let error = restarted
        .load_lazy_workspace(id, &mut accounting)
        .await
        .expect_err("actual decoder stops at original exact source allowance");
    assert!(matches!(
        error,
        crate::LazyWorkspaceError::Demand(crate::DemandError::Work(_))
    ));
    assert_eq!(work.receipt().source_bytes_read, 7);
    assert_eq!(work.receipt().bytes_hashed, 5);
    work.receipt()
        .verify(budget)
        .expect("all actual spent prefix");
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(
        restarted.load_lazy_workspace(id, &mut accounting).await,
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
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let errors = [
        store
            .load_lazy_workspace(workspace, &mut accounting)
            .await
            .unwrap_err(),
        store
            .compare_and_swap_lazy_workspace(
                workspace,
                0,
                lazy_state(workspace, 1),
                &mut accounting,
            )
            .await
            .unwrap_err(),
        store
            .load_lazy_overlay(overlay, &mut accounting)
            .await
            .err()
            .expect("cancelled overlay read"),
        store
            .put_lazy_overlay(
                overlay,
                (
                    LazyOverlay::Empty,
                    crate::kernel::KernelOwnedAllocation::Standalone,
                ),
                &mut accounting,
            )
            .await
            .unwrap_err(),
        store
            .load_lazy_shadow(shadow, &mut accounting)
            .await
            .err()
            .expect("cancelled shadow read"),
        store
            .put_lazy_shadow(
                shadow,
                (
                    crate::LazyShadow::Empty,
                    crate::kernel::KernelOwnedAllocation::Standalone,
                ),
                &mut accounting,
            )
            .await
            .unwrap_err(),
    ];
    for error in errors {
        assert!(matches!(
            error.semantic_error(),
            crate::LazyWorkspaceError::Cancelled
        ));
        assert_eq!(error.spent_work(), crate::WorkCounters::UNCHARGED);
    }
    assert_eq!(accounting.spent(), crate::WorkCounters::UNCHARGED);
    assert!(
        std::fs::read_dir(directory.path())
            .expect("namespace entries")
            .next()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_lazy_workspace_cache_is_never_stale_under_concurrency() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let cancellation = crate::CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let directory = tempfile::tempdir().expect("temporary directory");
    let workspaces = [
        WorkspaceId::from_bytes([0x61; 16]),
        WorkspaceId::from_bytes([0x62; 16]),
    ];
    let seed = LocalCoreStateStore::new(directory.path());
    for workspace_id in workspaces {
        assert!(
            seed.compare_and_swap_lazy_workspace(
                workspace_id,
                0,
                lazy_state(workspace_id, 1),
                &mut accounting
            )
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
                let cancellation = crate::CancellationToken::new();
                let mut accounting = crate::kernel::KernelAccounting::standalone(
                    crate::WorkBudget::UNBOUNDED,
                    &cancellation,
                );
                let mut wins = 0_u64;
                for round in 0..64_usize {
                    let (workspace_id, floor) = committed
                        .get((task + round) % committed.len())
                        .expect("binding slot");
                    let floor_before_load = floor.load(Ordering::SeqCst);
                    let current = owner
                        .load_lazy_workspace(*workspace_id, &mut accounting)
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
                            &mut accounting,
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
            .load_lazy_workspace(*workspace_id, &mut accounting)
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
                .load_lazy_workspace(state.workspace_id, &mut accounting)
                .await
                .expect("load durable binding"),
            Some(state)
        );
    }
}

/// Collection removes every node no lazy workspace reaches, keeps every node one does, and a node put again after removal is stored again.
#[tokio::test(flavor = "multi_thread")]
async fn lazy_node_collection_keeps_exactly_the_reachable_nodes() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
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
        let allocation = accounting
            .claim_owned(node.owned_bytes(), 0)
            .expect("overlay allocation");
        store
            .put_lazy_overlay(id, (node, allocation), &mut accounting)
            .await
            .expect("put overlay");
    }
    let empty_shadows = crate::LazyShadowId::from_bytes(
        content_address(&crate::LazyShadow::Empty).expect("empty shadow"),
    );
    // Collection walks the index, not the records it holds.
    let record = vec![1, 2, 3];
    let metadata = crate::WorkspaceMetadata::from_engine(crate::kernel::FileMetadata::default());
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
    let allocation = accounting
        .claim_owned(shadow.owned_bytes(), 0)
        .expect("shadow allocation");
    store
        .put_lazy_shadow(shadow_id, (shadow, allocation), &mut accounting)
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
    let allocation = accounting
        .claim_owned(orphan_shadow.owned_bytes(), 0)
        .expect("orphan allocation");
    store
        .put_lazy_shadow(
            orphan_shadow_id,
            (orphan_shadow, allocation),
            &mut accounting,
        )
        .await
        .expect("put orphan shadow");
    let mut state = lazy_state(workspace(), 1);
    state.overlay = root;
    state.shadows = shadow_id;
    assert!(
        store
            .compare_and_swap_lazy_workspace(workspace(), 0, state, &mut accounting)
            .await
            .expect("bind lazy workspace")
    );

    assert_eq!(store.collect_lazy_nodes().await.expect("collect"), 2);
    let reopened = LocalCoreStateStore::new(directory.path());
    drop(store);
    for id in [root, leaf] {
        assert!(
            reopened
                .load_lazy_overlay(id, &mut accounting)
                .await
                .expect("load")
                .is_some()
        );
    }
    assert!(
        reopened
            .load_lazy_shadow(shadow_id, &mut accounting)
            .await
            .expect("load")
            .is_some()
    );
    assert!(
        reopened
            .load_lazy_overlay(orphan, &mut accounting)
            .await
            .expect("load")
            .is_none()
    );
    assert!(
        reopened
            .load_lazy_shadow(orphan_shadow_id, &mut accounting)
            .await
            .expect("load")
            .is_none()
    );
    let allocation = accounting
        .claim_owned(orphan_node.owned_bytes(), 0)
        .expect("reinsert allocation");
    reopened
        .put_lazy_overlay(orphan, (orphan_node, allocation), &mut accounting)
        .await
        .expect("put again");
    assert!(
        reopened
            .load_lazy_overlay(orphan, &mut accounting)
            .await
            .expect("load")
            .is_some()
    );
}
