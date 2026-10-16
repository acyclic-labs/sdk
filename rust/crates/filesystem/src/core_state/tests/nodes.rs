use super::*;

#[tokio::test(flavor = "current_thread")]
async fn held_record_lock_is_bounded_without_stalling_unrelated_records() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let held_id = OperationId::from_bytes([4; 16]);
    let other_id = OperationId::from_bytes([0x55; 16]);
    let namespace = store.namespace().unwrap();
    let paths = namespace
        .record(MATERIALIZATION_FAMILY, &held_id.into_bytes())
        .unwrap();
    std::fs::create_dir_all(paths.directory()).unwrap();
    let held = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&paths.lock)
        .unwrap();
    held.lock_exclusive().unwrap();

    let (waiting, wait_started) = tokio::sync::oneshot::channel();
    let (resume, resumed) = std::sync::mpsc::sync_channel(1);
    let contender = tokio::spawn(async move {
        let mut waiting = Some(waiting);
        let mut attempts = 0;
        let mut original_deadline = None;
        store
            .load_materialization_with_retry(held_id, move |deadline| {
                assert_eq!(*original_deadline.get_or_insert(deadline), deadline);
                attempts += 1;
                assert!(attempts <= 2, "one retry, then timeout at the deadline");
                let now = if attempts == 1 {
                    deadline - RECORD_LOCK_RETRY
                } else {
                    deadline
                };
                retry_record_lock_at(deadline, now, |delay| {
                    assert_eq!(delay, RECORD_LOCK_RETRY);
                    waiting.take().unwrap().send(()).unwrap();
                    resumed.recv().unwrap();
                })
            })
            .await
    });
    wait_started.await.unwrap();
    assert!(!contender.is_finished());
    let unrelated = LocalCoreStateStore::new(directory.path());
    assert!(
        MaterializationJournalStore::load(&unrelated, other_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!contender.is_finished());
    resume.send(()).unwrap();
    assert!(matches!(
        contender.await.unwrap(),
        Err(LocalCoreStateStoreError::LockTimeout(path)) if path == paths.lock
    ));
    FileExt::unlock(&held).unwrap();
    assert!(
        MaterializationJournalStore::load(&unrelated, held_id)
            .await
            .unwrap()
            .is_none()
    );
}

proptest::proptest! {
    #[test]
    fn record_lock_retry_obeys_the_original_deadline(
        deadline_ns in proptest::num::u64::ANY,
        now_ns in proptest::num::u64::ANY,
        at_deadline in proptest::bool::ANY,
    ) {
        let now_ns = if at_deadline { deadline_ns } else { now_ns };
        let origin = Instant::now();
        let mut waited = None;
        let retry = retry_record_lock_at(
            origin + Duration::from_nanos(deadline_ns),
            origin + Duration::from_nanos(now_ns),
            |duration| waited = Some(duration),
        );
        proptest::prop_assert_eq!(retry, now_ns < deadline_ns);
        proptest::prop_assert_eq!(waited, retry.then_some(RECORD_LOCK_RETRY));
    }
}

#[tokio::test]
async fn lazy_overlay_load_rejects_content_address_mismatch() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = unbounded_accounting(&cancellation);
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let empty = LazyOverlayId::from_bytes([0; 32]);
    let (overlay, node) = linked_overlay("/original", empty, empty);
    let allocation = accounting.claim_owned(node.owned_bytes(), 0).unwrap();
    store
        .put_lazy_overlay(overlay, (node, allocation), &mut accounting)
        .await
        .unwrap();
    let (_, tampered) = linked_overlay("/tampered", empty, empty);
    let paths = store
        .namespace()
        .unwrap()
        .record("lazy-overlays", &overlay.into_bytes())
        .unwrap()
        .current;
    std::fs::write(&paths, serde_json::to_vec(&tampered).unwrap()).unwrap();
    assert!(matches!(
        LocalCoreStateStore::new(directory.path())
            .load_lazy_overlay(overlay, &mut accounting)
            .await,
        Err(crate::LazyWorkspaceError::NativeRecordIntegrity)
    ));
}

#[tokio::test]
async fn empty_nodes_are_implied_without_being_stored() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = unbounded_accounting(&cancellation);
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let overlay = LazyOverlayId::from_bytes(content_address(&LazyOverlay::default()).unwrap());
    let shadow: crate::LazyShadowId = serde_json::from_value(serde_json::json!(
        content_address(&crate::LazyShadow::default()).unwrap()
    ))
    .unwrap();
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
        .unwrap();
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
        .unwrap();
    assert_eq!(
        std::fs::read_dir(directory.path()).unwrap().count(),
        0,
        "empty nodes are never written"
    );
    assert_eq!(
        store
            .load_lazy_overlay(overlay, &mut accounting)
            .await
            .unwrap()
            .as_ref()
            .map(|(value, _)| value),
        Some(&LazyOverlay::default())
    );
    assert_eq!(
        store
            .load_lazy_shadow(shadow, &mut accounting)
            .await
            .unwrap()
            .as_ref()
            .map(|(value, _)| value),
        Some(&crate::LazyShadow::default())
    );
}

#[tokio::test]
async fn native_lazy_state_uses_original_meter_and_preserves_completed_snapshot_on_restart() {
    let directory = tempfile::tempdir().unwrap();
    let id = WorkspaceId::from_bytes([0xb1; 16]);
    let mut state = lazy_state(id, 1);
    let snapshot = crate::LazySnapshotRef::new(
        id,
        crate::GenerationId::new(Digest::from_bytes([0xb2; 32])),
        state.source,
        state.overlay,
        state.shadows,
    );
    state.selected_import_roots = Some(
        [(
            snapshot,
            crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
        ); 3],
    );
    let cancellation = crate::CancellationToken::new();
    let work = crate::materializer::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .unwrap();
    work.charge(crate::WorkCounters {
        bytes_hashed: 5,
        ..Default::default()
    })
    .unwrap();
    let mut accounting = crate::kernel::KernelAccounting::native(&work, work.cancellation());
    let store = LocalCoreStateStore::new(directory.path());
    assert!(
        store
            .compare_and_swap_lazy_workspace(id, 0, state.clone(), &mut accounting)
            .await
            .unwrap()
    );
    let first = store
        .load_lazy_workspace(id, &mut accounting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&first, &state);
    assert!(
        work.receipt().bytes_hashed >= 5,
        "completed hash prefix was lost"
    );
    let before = work.receipt().object_bytes_written;
    let mut stale = state.clone();
    stale.revision += 1;
    assert!(
        !store
            .compare_and_swap_lazy_workspace(id, 0, stale, &mut accounting)
            .await
            .unwrap()
    );
    assert_eq!(work.receipt().object_bytes_written, before);
    drop(store);
    let restarted = LocalCoreStateStore::new(directory.path());
    let original = restarted
        .load_lazy_workspace(id, &mut accounting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.selected_import_roots, state.selected_import_roots);
    assert_eq!(original.source, state.source);
    assert_eq!(original.workspace_id, id);
}

#[tokio::test]
async fn native_lazy_state_exact_read_budget_retains_prior_work_and_typed_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let id = WorkspaceId::from_bytes([0xb5; 16]);
    let store = LocalCoreStateStore::new(directory.path());
    assert!(swap_binding(&store, 0, lazy_state(id, 1)).await);
    drop(store);
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        source_bytes_read: 7,
        ..crate::WorkBudget::UNBOUNDED
    };
    let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    work.charge(crate::WorkCounters {
        bytes_hashed: 5,
        ..Default::default()
    })
    .unwrap();
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
    work.receipt().verify(budget).unwrap();
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
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let workspace = WorkspaceId::from_bytes([41; 16]);
    let overlay = LazyOverlayId::from_bytes([42; 32]);
    let shadow = crate::LazyShadowId::from_bytes([43; 32]);
    let cancellation = crate::CancellationToken::new();
    cancellation.cancel();
    let mut accounting = unbounded_accounting(&cancellation);
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
            .unwrap(),
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
            .unwrap(),
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
            crate::LazyWorkspaceError::Filesystem(crate::FsError::Cancelled(_))
        ));
    }
    assert_eq!(accounting.spent(), crate::WorkCounters::UNCHARGED);
    assert!(
        std::fs::read_dir(directory.path())
            .unwrap()
            .next()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_lazy_workspace_cache_is_never_stale_under_concurrency() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let cancellation = crate::CancellationToken::new();
    let mut accounting = unbounded_accounting(&cancellation);
    let directory = tempfile::tempdir().unwrap();
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
            .unwrap()
        );
    }
    drop(seed);
    let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let committed = Arc::new(workspaces.map(|workspace_id| (workspace_id, AtomicU64::new(1))));
    let tasks = (0..8_usize)
        .map(|task| {
            let owner = owner.clone();
            let committed = Arc::clone(&committed);
            tokio::spawn(async move {
                let cancellation = crate::CancellationToken::new();
                let mut accounting = unbounded_accounting(&cancellation);
                let mut wins = 0_u64;
                for round in 0..64_usize {
                    let (workspace_id, floor) =
                        committed.get((task + round) % committed.len()).unwrap();
                    let floor_before_load = floor.load(Ordering::SeqCst);
                    let current = owner
                        .load_lazy_workspace(*workspace_id, &mut accounting)
                        .await
                        .unwrap()
                        .unwrap();
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
                        .unwrap()
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
        wins += task.await.unwrap();
    }

    let mut advanced = 0;
    let mut cached = Vec::new();
    for (workspace_id, floor) in committed.iter() {
        let state = owner
            .load_lazy_workspace(*workspace_id, &mut accounting)
            .await
            .unwrap()
            .unwrap();
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
                .unwrap(),
            Some(state)
        );
    }
}
