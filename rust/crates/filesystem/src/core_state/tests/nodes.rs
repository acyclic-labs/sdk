use super::*;

#[tokio::test]
async fn reopened_overlay_source_alias_keeps_decoded_credit_until_last_drop() {
    let cancellation = crate::CancellationToken::new();
    let mut writer = unbounded_accounting(&cancellation);
    let leaf =
        crate::demand::SourceReference::opaque([0x61; 16], 7, &[0x63; 257], &mut writer).unwrap();
    let pair = crate::demand::SourceReference::pair(
        [0x62; 16],
        &leaf,
        &crate::demand::SourceReference::new([0x64; 16], 9),
        &mut writer,
    )
    .unwrap();
    // Scalar cuts have no component backing; opaque and routed cuts do.
    for source in [
        crate::demand::SourceReference::new([0x60; 16], 3),
        leaf,
        pair,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalCoreStateStore::new(directory.path());
        let empty = LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).unwrap());
        let (_, mut node) = linked_overlay("/observed", empty, empty);
        let LazyOverlay::Node { change, .. } = &mut node else {
            unreachable!()
        };
        *change = crate::lazy_workspace::LazyOverlayChange::Observe {
            source: source.clone(),
            node: crate::demand::SourceNode {
                kind: crate::demand::SourceNodeKind::RegularFile,
                file_identity: [0x65; 32],
                link_count: Some(1),
                device: None,
                logical_bytes: Some(1),
                version: crate::demand::SourceVersion([0x66; 32]),
                metadata: Default::default(),
            },
            complete: false,
        };
        let id = LazyOverlayId::from_bytes(content_address(&node).unwrap());
        let allocation = writer.claim_owned(node.owned_bytes(), 1).unwrap();
        store
            .put_lazy_overlay(id, (node, allocation), &mut writer)
            .await
            .unwrap();
        drop(store);
        // Completion headers retire inline, independently of the returned alias.
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let reopened = LocalCoreStateStore::new(directory.path());
        let work =
            crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation).unwrap();
        let baseline = work.live_allocation_bytes();
        let mut ctx = crate::kernel::KernelAccounting::native(&work, work.cancellation());
        let loaded = reopened
            .load_lazy_overlay(id, &mut ctx)
            .await
            .unwrap()
            .unwrap();
        let LazyOverlay::Node {
            change:
                crate::lazy_workspace::LazyOverlayChange::Observe {
                    source: decoded, ..
                },
            ..
        } = &loaded.0
        else {
            panic!("observation missing")
        };
        assert_eq!(decoded, &source);
        let alias = decoded.clone();
        let bytes = loaded.0.owned_bytes();
        drop(loaded.0);
        crate::kernel::retire_allocation(loaded.1, bytes, &mut ctx).unwrap();
        drop(reopened);
        assert!(
            work.live_allocation_bytes() >= baseline + alias.components.len() as u64,
            "overlay source alias lost its decoded memory credit"
        );
        drop(alias);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "source credit failed to retire"
        );
    }
}

#[tokio::test]
async fn reopened_shadow_source_alias_keeps_decoded_credit_until_last_drop() {
    let cancellation = crate::CancellationToken::new();
    let mut writer = unbounded_accounting(&cancellation);
    let source =
        crate::demand::SourceReference::opaque([0x71; 16], 7, &[0x73; 257], &mut writer).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let empty =
        crate::LazyShadowId::from_bytes(content_address(&crate::LazyShadow::Empty).unwrap());
    let node = crate::LazyShadow::Node {
        file_id: crate::FileId::from_bytes([0x75; 16]),
        priority: 0,
        authored: None,
        observed: Some(crate::LazySourceRecord {
            source: source.clone(),
            version: crate::demand::SourceVersion([0x76; 32]),
            content: crate::LazyStoredRecord {
                record: vec![1, 2, 3],
                metadata: crate::WorkspaceMetadata::from_engine(Default::default()),
            },
        }),
        left: empty,
        right: empty,
    };
    let id = crate::LazyShadowId::from_bytes(content_address(&node).unwrap());
    let allocation = writer.claim_owned(node.owned_bytes(), 1).unwrap();
    store
        .put_lazy_shadow(id, (node, allocation), &mut writer)
        .await
        .unwrap();
    drop(store);
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let reopened = LocalCoreStateStore::new(directory.path());
    let work =
        crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    let mut ctx = crate::kernel::KernelAccounting::native(&work, work.cancellation());
    let loaded = reopened
        .load_lazy_shadow(id, &mut ctx)
        .await
        .unwrap()
        .unwrap();
    let crate::LazyShadow::Node {
        observed: Some(observed),
        ..
    } = &loaded.0
    else {
        panic!("source before-image missing")
    };
    assert_eq!(observed.source, source);
    let alias = observed.source.clone();
    let bytes = loaded.0.owned_bytes();
    drop(loaded.0);
    crate::kernel::retire_allocation(loaded.1, bytes, &mut ctx).unwrap();
    drop(reopened);
    assert!(
        work.live_allocation_bytes() >= baseline + alias.components.len() as u64,
        "shadow source alias lost its decoded memory credit"
    );
    drop(alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
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
        .put_lazy_overlay(overlay, (node.clone(), allocation), &mut accounting)
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
    let allocation = accounting.claim_owned(node.owned_bytes(), 0).unwrap();
    assert!(matches!(
        store
            .put_lazy_overlay(overlay, (node, allocation), &mut accounting)
            .await,
        Err(crate::LazyWorkspaceError::NativeRecordIntegrity)
    ));
    assert_eq!(
        std::fs::read(paths).unwrap(),
        serde_json::to_vec(&tampered).unwrap()
    );
}

#[tokio::test]
async fn concurrent_immutable_nodes_retain_payload_and_publication_custody() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = unbounded_accounting(&cancellation);
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let empty = LazyOverlayId::from_bytes([0; 32]);
    let publications = ["/a", "/b", "/c", "/a", "/b", "/c"].map(|path| {
        let store = &store;
        let cancellation = &cancellation;
        async move {
            let mut accounting = unbounded_accounting(&cancellation);
            let (overlay, node) = linked_overlay(path, empty, empty);
            let allocation = accounting.claim_owned(node.owned_bytes(), 0).unwrap();
            store
                .put_lazy_overlay(overlay, (node.clone(), allocation), &mut accounting)
                .await
                .unwrap();
            (overlay, node)
        }
    });
    let stored = futures::future::join_all(publications).await;
    let (absent, _) = linked_overlay("/absent", empty, empty);
    assert!(
        store
            .load_lazy_overlay(absent, &mut accounting)
            .await
            .unwrap()
            .is_none()
    );
    for (overlay, node) in &stored {
        assert_eq!(
            store
                .load_lazy_overlay(*overlay, &mut accounting)
                .await
                .unwrap()
                .as_ref()
                .map(|(value, _)| value),
            Some(node)
        );
    }
    // Withdraw a real publication while its original native producer is held.
    // The caller cannot reuse admission or retire the producer's input credit.
    let mut caller =
        crate::kernel::KernelAccounting::standalone(native::finite_budget(), &cancellation);
    let anchor =
        crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, &mut caller).unwrap();
    let (id, value) = linked_overlay("/withdrawn", empty, empty);
    let bytes = value.owned_bytes();
    let allocation = caller.claim_owned(bytes, 1).unwrap();
    let funded = anchor.standalone_live_bytes().unwrap();
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
    let mut publish =
        Box::pin(schedule.scope(store.put_lazy_overlay(id, (value, allocation), &mut caller)));
    assert!(futures::poll!(publish.as_mut()).is_pending());
    let boundary = boundaries
        .recv()
        .await
        .expect("accepted publication has a producer");
    drop(publish);
    assert!(
        matches!(
            caller.remaining(),
            Err(crate::FsError::AccountingHandoffActive)
        ),
        "withdrawn publication reactivated its original caller"
    );
    assert!(
        anchor.standalone_live_bytes().unwrap() >= funded,
        "withdrawn publication retired its producer's input"
    );
    let driver = tokio::spawn(async move {
        while let Some(boundary) = boundaries.recv().await {
            boundary.release();
        }
    });
    boundary.release();
    schedule.drained().await;
    assert!(
        caller.spent().object_bytes_written > 0,
        "withdrawn publication lost its completed original work"
    );
    let live = accounting.standalone_parts().unwrap().0.live_bytes();
    let (loaded, mut allocation) = store
        .load_lazy_overlay(id, &mut accounting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.owned_bytes(), bytes);
    assert_eq!(
        accounting.standalone_parts().unwrap().0.live_bytes(),
        live + bytes,
        "loaded node must transfer its existing original backing"
    );
    drop(loaded);
    accounting.release_owned(&mut allocation, bytes).unwrap();
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), live);
    driver.abort();
    let _ = driver.await;
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
    let cancellation = crate::CancellationToken::new();
    state.source = crate::demand::SourceReference::pair(
        [0xb3; 16],
        &state.source,
        &crate::demand::SourceReference::new([0xb4; 16], 2),
        &mut unbounded_accounting(&cancellation),
    )
    .unwrap();
    let snapshot = crate::LazySnapshotRef::new(
        id,
        crate::GenerationId::new(Digest::from_bytes([0xb2; 32])),
        state.source.clone(),
        state.overlay,
        state.shadows,
    );
    state.selected_import_roots = Some(std::array::from_fn(|_| {
        (
            snapshot.clone(),
            crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
        )
    }));
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
    // Run the real native transaction inline so its completion owner retires
    // before the alias assertions; background completion headers have their
    // own physical lifetime independent of the returned record's backing.
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let restarted = LocalCoreStateStore::new(directory.path());
    let decoded_work = crate::materializer::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .unwrap();
    let decoded_baseline = decoded_work.live_allocation_bytes();
    let mut decoded_accounting =
        crate::kernel::KernelAccounting::native(&decoded_work, decoded_work.cancellation());
    let original = restarted
        .load_lazy_workspace(id, &mut decoded_accounting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.selected_import_roots, state.selected_import_roots);
    assert_eq!(original.source, state.source);
    assert_eq!(original.workspace_id, id);
    let held_cut = original.source.clone();
    let held_roots = original.selected_import_roots.clone();
    drop(original);
    drop(restarted);
    assert!(
        decoded_work.live_allocation_bytes() > decoded_baseline,
        "returned source aliases retain their original decoded backing"
    );
    drop(held_cut);
    drop(held_roots);
    assert_eq!(decoded_work.live_allocation_bytes(), decoded_baseline);
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

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_held_immutable_publication_does_not_exclude_another_address() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let cancellation = crate::CancellationToken::new();
    let empty = LazyOverlayId::from_bytes([0; 32]);
    let (first_id, first_node) = linked_overlay("/held", empty, empty);
    let (second_id, second_node) = linked_overlay("/independent", empty, empty);
    let namespace = store.namespace().unwrap();
    let mut held =
        crate::core_state::nodes::publication_pause::hold(&namespace.root, first_id.into_bytes());
    let mut first_accounting = unbounded_accounting(&cancellation);
    let allocation = first_accounting
        .claim_owned(first_node.owned_bytes(), 0)
        .unwrap();
    let mut first = Box::pin(store.put_lazy_overlay(
        first_id,
        (first_node.clone(), allocation),
        &mut first_accounting,
    ));
    assert!(futures::poll!(first.as_mut()).is_pending());
    // Wait for the actual post-rename publication cut, not just worker admission.
    tokio::time::timeout(std::time::Duration::from_secs(2), held.entered())
        .await
        .expect("first publisher never reached its physical publication cut");
    // A dedicated inline producer supplies execution capacity even when the
    // shared blocking pool has only one worker, occupied by the held producer.
    let independent_store = store.clone();
    let independent_node = second_node.clone();
    let (completed, mut completion) = tokio::sync::oneshot::channel();
    let second = std::thread::spawn(move || {
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let cancellation = crate::CancellationToken::new();
        let mut accounting = unbounded_accounting(&cancellation);
        let allocation = accounting
            .claim_owned(independent_node.owned_bytes(), 0)
            .unwrap();
        let result = futures::executor::block_on(independent_store.put_lazy_overlay(
            second_id,
            (independent_node, allocation),
            &mut accounting,
        ));
        let _ = completed.send(result);
    });
    let independent =
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut completion).await;
    // Always release and finish both real producers before reporting a failure.
    drop(held);
    first.await.unwrap();
    second.join().unwrap();
    independent
        .expect("an unrelated immutable address waited for the held publisher")
        .expect("independent producer disappeared")
        .unwrap();
    let mut accounting = unbounded_accounting(&cancellation);
    for (id, expected) in [(first_id, first_node), (second_id, second_node)] {
        assert_eq!(
            store
                .load_lazy_overlay(id, &mut accounting)
                .await
                .unwrap()
                .as_ref()
                .map(|(node, _)| node),
            Some(&expected)
        );
    }
}
