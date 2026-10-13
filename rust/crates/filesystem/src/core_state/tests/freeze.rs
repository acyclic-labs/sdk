use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn prepared_freeze_authenticates_each_source_component() {
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
        assert!(
            tokio::spawn(async move { swap_binding(&updater, parent.revision, update).await })
                .await
                .unwrap()
        );
        let cancellation = crate::CancellationToken::new();
        let mut rejected = unbounded_accounting(&cancellation);
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
        assert!(load_binding(&store, prepared.workspace_id).await.is_none());
        assert!(swap_binding(&store, 0, prepared_origin_fixture(&updated)).await);
    }
}

#[tokio::test]
async fn freeze_survives_lost_acknowledgement_and_keeps_its_origin_immutable() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let parent = origin_parent_fixture(&store).await;
    let prepared = prepared_origin_fixture(&parent);
    let _ = swap_binding(&store, 0, prepared.clone()).await;
    drop(store);
    let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    assert_eq!(
        load_binding(&store, prepared.workspace_id).await,
        Some(prepared.clone())
    );
    let updated = advanced_origin_parent(&parent);
    assert!(swap_binding(&store, parent.revision, updated.clone()).await);
    assert!(!swap_binding(&store, 0, prepared.clone()).await);
    let mut committed = prepared.clone();
    committed.revision += 1;
    committed.fork_origin.as_mut().unwrap().phase = crate::LazyForkPhase::Committed;
    assert!(swap_binding(&store, prepared.revision, committed.clone()).await);
    let child = LazyWorkspaceState {
        revision: committed.revision + 1,
        source: updated.source,
        overlay: updated.overlay,
        shadows: updated.shadows,
        ..committed.clone()
    };
    assert!(swap_binding(&store, committed.revision, child.clone()).await);
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
        assert!(!swap_binding(&store, child.revision, replacement).await);
    }
    assert_eq!(load_binding(&store, child.workspace_id).await, Some(child));
}
