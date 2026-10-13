use super::*;

#[tokio::test]
async fn committed_change_survives_a_failing_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    for revision in 1..OWNED_CHECKPOINT {
        assert!(swap_binding(&owner, revision - 1, lazy_state(workspace(), revision)).await);
    }
    assert!(
        WorkspaceLineageStore::load(&owner, CRASH_WORKSPACE)
            .await
            .unwrap()
            .is_none()
    );
    std::fs::write(directory.path().join(LINEAGE_FAMILY), b"obstacle").unwrap();
    let record = crash_lineage();
    assert!(
        WorkspaceLineageStore::compare_and_swap(&owner, CRASH_WORKSPACE, 0, record.clone())
            .await
            .unwrap()
    );
    assert!(
        WorkspaceLineageStore::load(&owner, CRASH_WORKSPACE)
            .await
            .is_ok(),
        "resident reads need no checkpoint"
    );
    assert!(
        owner
            .compare_and_swap_lazy_workspace(
                workspace(),
                OWNED_CHECKPOINT - 1,
                lazy_state(workspace(), OWNED_CHECKPOINT),
                &mut unbounded_accounting(&crate::CancellationToken::new())
            )
            .await
            .is_err(),
        "the next transaction reports the failed checkpoint"
    );
    std::fs::remove_file(directory.path().join(LINEAGE_FAMILY)).unwrap();
    assert!(
        swap_binding(
            &owner,
            OWNED_CHECKPOINT - 1,
            lazy_state(workspace(), OWNED_CHECKPOINT)
        )
        .await
    );
    drop(owner);
    assert_eq!(
        WorkspaceLineageStore::load(&LocalCoreStateStore::new(directory.path()), CRASH_WORKSPACE)
            .await
            .unwrap(),
        Some(record)
    );
}

#[tokio::test]
async fn intact_undecodable_frame_fails_closed_and_is_kept() {
    type Edit = fn(&mut serde_json::Value);
    let edits: [(&str, Edit); 3] = [
        ("unknown record key", |frame| {
            frame["change"][0][0] = serde_json::json!({ "Future": [0] });
        }),
        ("unknown frame field", |frame| {
            frame["future"] = serde_json::json!(1);
        }),
        ("unknown payload field", |frame| {
            frame["change"][0][1]["LazyWorkspace"]["future"] = serde_json::json!(1);
        }),
    ];
    for (case, edit) in edits {
        let directory = tempfile::tempdir().unwrap();
        let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
        assert!(swap_binding(&owner, 0, lazy_state(workspace(), 1)).await);
        drop(owner);
        let log = rewrite_intact_frame(directory.path(), edit);
        let store = LocalCoreStateStore::new(directory.path());
        assert!(
            matches!(
                store
                    .load_lazy_workspace(
                        workspace(),
                        &mut unbounded_accounting(&crate::CancellationToken::new())
                    )
                    .await,
                Err(crate::LazyWorkspaceError::NativeRecordCorrupt(_, _))
            ),
            "{case}: an intact frame that does not decode is corruption"
        );
        let namespace = store.namespace().unwrap();
        assert_eq!(
            std::fs::read(&namespace.record(CORE_LOG_FAMILY, &[0; 16]).unwrap().current).unwrap(),
            log,
            "{case}: a failed replay must not checkpoint the log away"
        );
    }
}

#[tokio::test]
async fn deferred_commits_are_visible_at_once_and_durable_on_commit() {
    let directory = tempfile::tempdir().unwrap();
    let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let (deferred, durability) = owner.defer_durability();
    assert!(swap_binding(&deferred, 0, lazy_state(workspace(), 1)).await);
    assert_eq!(
        load_binding(&owner, workspace()).await,
        Some(lazy_state(workspace(), 1))
    );
    durability.commit().await.unwrap();
    assert!(swap_binding(&deferred, 1, lazy_state(workspace(), 2)).await);
    drop((owner, deferred));
    assert_eq!(
        LocalCoreStateStore::new(directory.path())
            .load_lazy_workspace(
                workspace(),
                &mut unbounded_accounting(&crate::CancellationToken::new())
            )
            .await
            .unwrap(),
        Some(lazy_state(workspace(), 2))
    );
}

#[test]
fn owned_core_commits_are_crash_atomic() {
    assert_core_commits_are_crash_atomic(true);
}

#[test]
fn shared_core_commits_are_crash_atomic() {
    assert_core_commits_are_crash_atomic(false);
}
