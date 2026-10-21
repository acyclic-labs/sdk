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

#[test]
fn checkpoint_batches_family_barriers_before_discarding_the_wal() {
    for native in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let cancellation = crate::CancellationToken::new();
        let work =
            crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation).unwrap();
        let store = LocalCoreStateStore::new(directory.path());
        let store = if native {
            store.materialization(work.clone())
        } else {
            store
        };
        let namespace = store.namespace().unwrap();
        let keys = [
            RecordKey::Context(WorkspaceContextId::from_bytes([1; 16])),
            RecordKey::Context(WorkspaceContextId::from_bytes([2; 16])),
            RecordKey::Children(WorkspaceContextId::from_bytes([1; 16])),
            RecordKey::Children(WorkspaceContextId::from_bytes([2; 16])),
            RecordKey::ChildCount(WorkspaceContextId::from_bytes([1; 16])),
            RecordKey::ChildCount(WorkspaceContextId::from_bytes([2; 16])),
            RecordKey::ChildIndex,
            RecordKey::Lineage(WorkspaceId::from_bytes([1; 16])),
            RecordKey::Lineage(WorkspaceId::from_bytes([2; 16])),
            RecordKey::LazyWorkspace(WorkspaceId::from_bytes([1; 16])),
            RecordKey::LazyWorkspace(WorkspaceId::from_bytes([2; 16])),
        ];
        let mut log = CoreLog::open(&namespace, Arc::default()).unwrap();
        let mut change = if native {
            Change::new(&work).unwrap()
        } else {
            Change::default()
        };
        for key in keys {
            change.put((key, None).into()).unwrap();
        }
        log.commit(&namespace, change).unwrap();
        let wal = namespace.record(CORE_LOG_FAMILY, &[0; 16]).unwrap().current;
        assert!(std::fs::metadata(&wal).unwrap().len() > 0);
        let before = work.receipt();
        log.checkpoint(&namespace).unwrap();
        if native {
            // Eleven physical record-file barriers plus six distinct family
            // directory barriers. The commit already flushed its WAL frame.
            assert_eq!(
                work.receipt().durability_operations - before.durability_operations,
                keys.len() as u64 + 6,
                "checkpoint flushed the same family repeatedly"
            );
        }
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
        drop(log);
        // Discard every resident cache. Each family must independently survive
        // the empty WAL, including the singleton index and absent records.
        let reopened = CoreLog::open(&namespace, Arc::default()).unwrap();
        assert!(reopened.dirty.is_empty());
        for key in keys {
            let path = key.paths(&namespace).unwrap().current;
            assert_eq!(std::fs::read(path).unwrap(), b"null");
        }
        work.receipt().verify(native::finite_budget()).unwrap();
    }
}

#[test]
fn checkpoint_reuses_each_native_family_parent_for_all_records() {
    fn checkpoint(records: u8) -> crate::WorkCounters {
        let directory = tempfile::tempdir().unwrap();
        let cancellation = crate::CancellationToken::new();
        let work =
            crate::NativeMaterializationWork::new(native::finite_budget(), &cancellation).unwrap();
        let store = LocalCoreStateStore::new(directory.path()).materialization(work.clone());
        let namespace = store.namespace().unwrap();
        let mut log = CoreLog::open(&namespace, Arc::default()).unwrap();
        let mut change = Change::new(&work).unwrap();
        for byte in 1..=records {
            change
                .put(
                    (
                        RecordKey::Context(WorkspaceContextId::from_bytes([byte; 16])),
                        None,
                    )
                        .into(),
                )
                .unwrap();
        }
        log.commit(&namespace, change).unwrap();
        let before = work.receipt();
        log.checkpoint(&namespace).unwrap();
        let after = work.receipt();
        drop(log);
        let reopened = CoreLog::open(&namespace, Arc::default()).unwrap();
        assert!(reopened.dirty.is_empty());
        for byte in 1..=records {
            assert_eq!(
                std::fs::read(
                    RecordKey::Context(WorkspaceContextId::from_bytes([byte; 16]))
                        .paths(&namespace)
                        .unwrap()
                        .current
                )
                .unwrap(),
                b"null"
            );
        }
        work.receipt().verify(native::finite_budget()).unwrap();
        crate::WorkCounters {
            backend_read_operations: after.backend_read_operations - before.backend_read_operations,
            durability_operations: after.durability_operations - before.durability_operations,
            object_bytes_written: after.object_bytes_written - before.object_bytes_written,
            ..Default::default()
        }
    }
    let one = checkpoint(1);
    let two = checkpoint(2);
    // A creating leaf open is a backend write; its regular-file metadata
    // check is one backend read on both Windows and Unix. Parent traversal,
    // creation, validation and flushing are per family.
    assert_eq!(
        two.backend_read_operations - one.backend_read_operations,
        1,
        "checkpoint reopened the same native family parent per record"
    );
    assert_eq!(two.durability_operations - one.durability_operations, 1);
    assert_eq!(two.object_bytes_written - one.object_bytes_written, 4);
}
