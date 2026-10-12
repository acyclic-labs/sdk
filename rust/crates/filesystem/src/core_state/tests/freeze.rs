use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn first_prepared_freeze_rejects_concurrent_source_update_and_collected_roots() {
    for component in 0..3 {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
        let parent = origin_parent_fixture(&store).await;
        let prepared = prepared_origin_fixture(&parent);
        let advanced = advanced_origin_parent(&parent);
        let mut updated = LazyWorkspaceState {
            revision: parent.revision + 1,
            ..parent.clone()
        };
        match component {
            0 => updated.source = advanced.source,
            1 => updated.overlay = advanced.overlay,
            _ => updated.shadows = advanced.shadows,
        }
        let updater = store.clone();
        let update = updated.clone();
        let expected = parent.revision;
        assert!(
            tokio::spawn(async move {
                updater
                    .compare_and_swap_lazy_workspace(
                        update.workspace_id,
                        expected,
                        update,
                        &mut crate::kernel::KernelAccounting::standalone(
                            crate::WorkBudget::UNBOUNDED,
                            &crate::CancellationToken::new(),
                        ),
                    )
                    .await
                    .expect("concurrent source binding update")
            })
            .await
            .expect("update task")
        );
        assert_eq!(
            store
                .collect_lazy_nodes()
                .await
                .expect("collect old source roots"),
            u64::from(component != 0)
        );
        if component == 1 {
            assert!(
                store
                    .load_lazy_overlay(
                        parent.overlay,
                        &mut crate::kernel::KernelAccounting::standalone(
                            crate::WorkBudget::UNBOUNDED,
                            &crate::CancellationToken::new()
                        )
                    )
                    .await
                    .expect("old overlay")
                    .is_none()
            );
        } else if component == 2 {
            assert!(
                store
                    .load_lazy_shadow(
                        parent.shadows,
                        &mut crate::kernel::KernelAccounting::standalone(
                            crate::WorkBudget::UNBOUNDED,
                            &crate::CancellationToken::new()
                        )
                    )
                    .await
                    .expect("old shadow")
                    .is_none()
            );
        }

        let cancellation = crate::CancellationToken::new();
        let mut accounting = crate::kernel::KernelAccounting::standalone(
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let rejected = store
            .compare_and_swap_lazy_workspace(
                prepared.workspace_id,
                0,
                prepared.clone(),
                &mut accounting,
            )
            .await
            .expect("stale freeze is a CAS miss");
        assert!(!rejected);
        assert!(accounting.spent().items_examined > 0);
        assert_eq!(accounting.spent().object_bytes_written, 0);
        assert!(
            store
                .load_lazy_workspace(
                    prepared.workspace_id,
                    &mut crate::kernel::KernelAccounting::standalone(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                )
                .await
                .expect("child")
                .is_none()
        );

        let refreshed = prepared_origin_fixture(&updated);
        assert!(
            store
                .compare_and_swap_lazy_workspace(
                    refreshed.workspace_id,
                    0,
                    refreshed,
                    &mut crate::kernel::KernelAccounting::standalone(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                )
                .await
                .expect("current source freeze")
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn original_freeze_recovers_without_ack_and_retains_roots_after_parent_changes() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
    let parent = origin_parent_fixture(&store).await;
    let prepared = prepared_origin_fixture(&parent);
    // The caller loses the first write's acknowledgement, then recovers
    // only the authentic persisted original record after reopening.
    let _ = store
        .compare_and_swap_lazy_workspace(
            prepared.workspace_id,
            0,
            prepared.clone(),
            &mut accounting,
        )
        .await
        .expect("durable original freeze");
    drop(store);
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("reopened store");
    assert_eq!(
        store
            .load_lazy_workspace(prepared.workspace_id, &mut accounting)
            .await
            .expect("recover"),
        Some(prepared.clone())
    );
    let updated = advanced_origin_parent(&parent);
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                parent.workspace_id,
                parent.revision,
                updated.clone(),
                &mut accounting
            )
            .await
            .expect("advance mutable parent")
    );
    assert_eq!(
        store
            .collect_lazy_nodes()
            .await
            .expect("prepared origin roots"),
        0
    );
    assert!(
        !store
            .compare_and_swap_lazy_workspace(
                prepared.workspace_id,
                0,
                prepared.clone(),
                &mut accounting
            )
            .await
            .expect("lost acknowledgement retry is revision fenced")
    );

    let mut committed = prepared.clone();
    committed.revision += 1;
    committed.fork_origin.as_mut().expect("origin").phase = crate::LazyForkPhase::Committed;
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                committed.workspace_id,
                prepared.revision,
                committed.clone(),
                &mut accounting
            )
            .await
            .expect("commit original without rereading mutable parent")
    );
    let child = LazyWorkspaceState {
        revision: committed.revision + 1,
        source: updated.source,
        overlay: updated.overlay,
        shadows: updated.shadows,
        ..committed.clone()
    };
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                child.workspace_id,
                committed.revision,
                child.clone(),
                &mut accounting
            )
            .await
            .expect("later child observation epoch")
    );
    assert_eq!(
        store
            .collect_lazy_nodes()
            .await
            .expect("committed origin roots"),
        0
    );
    assert!(
        store
            .load_lazy_overlay(parent.overlay, &mut accounting)
            .await
            .expect("original overlay retained")
            .is_some()
    );
    assert!(
        store
            .load_lazy_shadow(parent.shadows, &mut accounting)
            .await
            .expect("original shadows retained")
            .is_some()
    );
    let origin = prepared.fork_origin.expect("original");
    assert_eq!(
        store
            .lazy_retained_generations()
            .await
            .expect("original generation roots"),
        [(origin.snapshot.authored_generation, origin.config)]
    );

    let next = LazyWorkspaceState {
        revision: child.revision + 1,
        ..child.clone()
    };
    let mut removed = next.clone();
    removed.fork_origin = None;
    let mut new_operation = next.clone();
    new_operation
        .fork_origin
        .as_mut()
        .expect("origin")
        .operation_id = OperationId::from_bytes([0xb0; 16]);
    let mut new_config = next.clone();
    new_config
        .fork_origin
        .as_mut()
        .expect("origin")
        .config
        .limits
        .maximum_read_bytes += 1;
    let mut new_snapshot = next.clone();
    new_snapshot
        .fork_origin
        .as_mut()
        .expect("origin")
        .snapshot
        .authored_generation = crate::GenerationId::new(Digest::from_bytes([0xb1; 32]));
    let mut new_parent = next.clone();
    new_parent.parent_workspace_id = Some(WorkspaceId::from_bytes([0xb2; 16]));
    let mut regressed = next;
    regressed.fork_origin.as_mut().expect("origin").phase = crate::LazyForkPhase::Prepared;
    for replacement in [
        removed,
        new_operation,
        new_config,
        new_snapshot,
        new_parent,
        regressed,
    ] {
        assert!(
            !store
                .compare_and_swap_lazy_workspace(
                    child.workspace_id,
                    child.revision,
                    replacement,
                    &mut accounting
                )
                .await
                .expect("immutable original rejects replacement")
        );
    }
    assert_eq!(
        store
            .load_lazy_workspace(child.workspace_id, &mut accounting)
            .await
            .expect("unchanged child"),
        Some(child)
    );
}
