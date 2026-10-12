use super::*;

#[tokio::test]
    async fn committed_change_survives_a_failing_checkpoint() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        for revision in 1..OWNED_CHECKPOINT {
            assert!(
                owner
                    .compare_and_swap_lazy_workspace(workspace(), revision - 1, lazy_state(workspace(), revision), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                    .await
                    .expect("commit below the checkpoint threshold")
            );
        }
        assert!(
            WorkspaceLineageStore::load(&owner, CRASH_WORKSPACE)
                .await
                .expect("absent lineage becomes resident")
                .is_none()
        );
        // The lineage family's directory cannot be created, so the checkpoint
        // that the last commit makes due fails.
        std::fs::write(directory.path().join(LINEAGE_FAMILY), b"obstacle").expect("obstacle");
        let record = crash_lineage();
        assert!(
            WorkspaceLineageStore::compare_and_swap(&owner, CRASH_WORKSPACE, 0, record.clone())
                .await
                .expect("the commit that makes a checkpoint due succeeds")
        );
        assert!(
            WorkspaceLineageStore::load(&owner, CRASH_WORKSPACE)
                .await
                .is_ok(),
            "resident reads need no checkpoint"
        );
        assert!(
            owner
                .compare_and_swap_lazy_workspace(workspace(), OWNED_CHECKPOINT - 1, lazy_state(workspace(), OWNED_CHECKPOINT), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                .await
                .is_err(),
            "the next transaction reports the failed checkpoint"
        );
        std::fs::remove_file(directory.path().join(LINEAGE_FAMILY)).expect("remove obstacle");
        assert!(
            owner
                .compare_and_swap_lazy_workspace(workspace(), OWNED_CHECKPOINT - 1, lazy_state(workspace(), OWNED_CHECKPOINT), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                .await
                .expect("the retried checkpoint succeeds")
        );
        drop(owner);
        assert_eq!(
            WorkspaceLineageStore::load(
                &LocalCoreStateStore::new(directory.path()),
                CRASH_WORKSPACE
            )
            .await
            .expect("durable lineage"),
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
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        assert!(
            owner
                .compare_and_swap_lazy_workspace(workspace(), 0, lazy_state(workspace(), 1), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                .await
                .expect("commit")
        );
        // The owner leaves its committed frame in the log for replay.
        drop(owner);
        let log = rewrite_intact_frame(directory.path(), edit);
        let store = LocalCoreStateStore::new(directory.path());
        assert!(
            matches!(
                store.load_lazy_workspace(workspace(), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())).await,
                Err(crate::LazyWorkspaceError::NativeRecordCorrupt(_, _))
            ),
            "{case}: an intact frame that does not decode is corruption"
        );
        let namespace = store.namespace().expect("admitted namespace");
        assert_eq!(
            std::fs::read(
                &namespace
                    .record(CORE_LOG_FAMILY, &[0; 16])
                    .expect("record paths")
                    .current
            )
            .expect("read core log"),
            log,
            "{case}: a failed replay must not checkpoint the log away"
        );
    }
}

#[tokio::test]
async fn deferred_commits_are_visible_at_once_and_durable_on_commit() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
    let (deferred, durability) = owner.defer_durability();
    assert!(
        deferred
            .compare_and_swap_lazy_workspace(workspace(), 0, lazy_state(workspace(), 1), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
            .await
            .expect("deferred commit")
    );
    assert_eq!(
        owner
            .load_lazy_workspace(workspace(), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
            .await
            .expect("visible deferred commit"),
        Some(lazy_state(workspace(), 1))
    );
    durability.commit().await.expect("flush deferred commits");
    assert!(
        deferred
            .compare_and_swap_lazy_workspace(workspace(), 1, lazy_state(workspace(), 2), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
            .await
            .expect("commit after the scope ended")
    );
    let log = {
        let namespace = owner.namespace().expect("admitted namespace");
        &namespace
            .record(CORE_LOG_FAMILY, &[0; 16])
            .expect("record paths")
            .current
    };
    let bytes = std::fs::read(&log).expect("read core log");
    assert_eq!(
        replayable_changes(&bytes).expect("decodable log").len(),
        2,
        "both frames are in the log"
    );
    drop((owner, deferred));
    assert_eq!(
        LocalCoreStateStore::new(directory.path())
            .load_lazy_workspace(workspace(), &mut crate::kernel::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
            .await
            .expect("durable binding"),
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
