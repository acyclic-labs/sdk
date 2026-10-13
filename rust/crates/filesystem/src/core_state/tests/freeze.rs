use super::*;
use crate::kernel::KernelAccounting;
use crate::{CancellationToken, WorkBudget};

#[tokio::test(flavor = "multi_thread")]
async fn first_prepared_freeze_rejects_concurrent_source_update_and_collected_roots() {
    for component in 0..3 {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
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
                let cancellation = CancellationToken::new();
                let mut accounting =
                    KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
                updater
                    .compare_and_swap_lazy_workspace(
                        update.workspace_id,
                        expected,
                        update,
                        &mut accounting,
                    )
                    .await
                    .unwrap()
            })
            .await
            .unwrap()
        );
        assert_eq!(
            store.collect_lazy_nodes().await.unwrap(),
            u64::from(component != 0)
        );
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        if component == 1 {
            assert!(
                store
                    .load_lazy_overlay(parent.overlay, &mut accounting)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else if component == 2 {
            assert!(
                store
                    .load_lazy_shadow(parent.shadows, &mut accounting)
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        let rejection_cancellation = CancellationToken::new();
        let mut rejected =
            KernelAccounting::standalone(WorkBudget::UNBOUNDED, &rejection_cancellation);
        assert!(
            !store
                .compare_and_swap_lazy_workspace(
                    prepared.workspace_id,
                    0,
                    prepared.clone(),
                    &mut rejected
                )
                .await
                .unwrap()
        );
        assert!(rejected.spent().items_examined > 0);
        assert_eq!(rejected.spent().object_bytes_written, 0);
        assert!(
            store
                .load_lazy_workspace(prepared.workspace_id, &mut accounting)
                .await
                .unwrap()
                .is_none()
        );
        let refreshed = prepared_origin_fixture(&updated);
        assert!(
            store
                .compare_and_swap_lazy_workspace(
                    refreshed.workspace_id,
                    0,
                    refreshed,
                    &mut accounting
                )
                .await
                .unwrap()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn original_freeze_recovers_without_ack_and_retains_roots_after_parent_changes() {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let parent = origin_parent_fixture(&store).await;
    let prepared = prepared_origin_fixture(&parent);
    // Recover from the durable write without consuming its acknowledgement.
    let _ = store
        .compare_and_swap_lazy_workspace(
            prepared.workspace_id,
            0,
            prepared.clone(),
            &mut accounting,
        )
        .await
        .unwrap();
    drop(store);
    let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    assert_eq!(
        store
            .load_lazy_workspace(prepared.workspace_id, &mut accounting)
            .await
            .unwrap(),
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
            .unwrap()
    );
    assert_eq!(store.collect_lazy_nodes().await.unwrap(), 0);
    assert!(
        !store
            .compare_and_swap_lazy_workspace(
                prepared.workspace_id,
                0,
                prepared.clone(),
                &mut accounting
            )
            .await
            .unwrap()
    );

    let mut committed = prepared.clone();
    committed.revision += 1;
    committed.fork_origin.as_mut().unwrap().phase = crate::LazyForkPhase::Committed;
    assert!(
        store
            .compare_and_swap_lazy_workspace(
                committed.workspace_id,
                prepared.revision,
                committed.clone(),
                &mut accounting
            )
            .await
            .unwrap()
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
            .unwrap()
    );
    assert_eq!(store.collect_lazy_nodes().await.unwrap(), 0);
    assert!(
        store
            .load_lazy_overlay(parent.overlay, &mut accounting)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .load_lazy_shadow(parent.shadows, &mut accounting)
            .await
            .unwrap()
            .is_some()
    );
    let mut replacements: [_; 6] = std::array::from_fn(|_| LazyWorkspaceState {
        revision: child.revision + 1,
        ..child.clone()
    });
    replacements[0].fork_origin = None;
    replacements[1].fork_origin.as_mut().unwrap().operation_id =
        OperationId::from_bytes([0xb0; 16]);
    replacements[2]
        .fork_origin
        .as_mut()
        .unwrap()
        .config
        .limits
        .maximum_read_bytes += 1;
    replacements[3]
        .fork_origin
        .as_mut()
        .unwrap()
        .snapshot
        .authored_generation = crate::GenerationId::new(Digest::from_bytes([0xb1; 32]));
    replacements[4].parent_workspace_id = Some(WorkspaceId::from_bytes([0xb2; 16]));
    replacements[5].fork_origin.as_mut().unwrap().phase = crate::LazyForkPhase::Prepared;
    for replacement in replacements {
        assert!(
            !store
                .compare_and_swap_lazy_workspace(
                    child.workspace_id,
                    child.revision,
                    replacement,
                    &mut accounting
                )
                .await
                .unwrap()
        );
    }
    assert_eq!(
        store
            .load_lazy_workspace(child.workspace_id, &mut accounting)
            .await
            .unwrap(),
        Some(child)
    );
}
