use super::*;

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            } else {
                // Windows enforces the live owner's byte-range lock even on
                // reads beyond EOF. Empty lock files have no bytes to read.
                let bytes = if entry.metadata().unwrap().len() == 0 {
                    Vec::new()
                } else {
                    std::fs::read(entry.path()).unwrap()
                };
                files.insert(entry.path(), bytes);
            }
        }
    }
    files
}

#[tokio::test]
async fn absent_inspection_creates_nothing_and_does_not_hide_quota_refusal() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("absent");
    let store = LocalCoreStateStore::new(&root).inspecting();
    let id = WorkspaceId::from_bytes([99; 16]);
    assert!(GitCompatStore::load(&store, id).await.unwrap().is_none());
    assert!(!root.exists());
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        backend_read_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let result = GitCompatStore::load(&store.materialization(work), id).await;
    assert!(
        result.is_err(),
        "quota refusal must not become an absent record"
    );
    assert!(!root.exists());
}

#[tokio::test]
async fn cold_wal_replay_is_private_and_missing_records_create_nothing() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let record = super::super::tests::crash_lineage();
    assert!(
        WorkspaceLineageStore::compare_and_swap(&owner, record.workspace_id, 0, record.clone())
            .await
            .unwrap()
    );
    drop(owner);
    let store = LocalCoreStateStore::new(directory.path()).inspecting();
    let before = files(directory.path());
    assert_eq!(
        WorkspaceLineageStore::load(&store, record.workspace_id)
            .await
            .unwrap(),
        Some(record.clone())
    );
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        durability_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let native = store.materialization(work.clone());
    assert_eq!(
        WorkspaceLineageStore::load(&native, record.workspace_id)
            .await
            .unwrap(),
        Some(record)
    );
    assert_eq!(
        work.receipt().durability_operations,
        0,
        "native inspection must not admit or execute a durability barrier"
    );
    assert!(
        WorkspaceLineageStore::load(&store, WorkspaceId::from_bytes([99; 16]))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        GitCompatStore::load(&store, WorkspaceId::from_bytes([99; 16]))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        files(directory.path()),
        before,
        "inspection must not checkpoint a cold WAL or create missing locks"
    );
}

#[tokio::test]
async fn first_shared_writer_invalidates_even_a_failed_speculative_replay() {
    let directory = tempfile::tempdir().unwrap();
    drop(LocalCoreStateStore::open_owned(directory.path()).unwrap());
    let store = LocalCoreStateStore::new(directory.path()).inspecting();
    let value = store
        .read_transaction(|root| {
            let mut paths = root.record(CORE_LOG_FAMILY, &[0; 16])?;
            let mut calls = 0;
            let result = with_wal(&mut paths, || {
                calls += 1;
                if calls == 1 {
                    let mut writer_paths = root.record(CORE_LOG_FAMILY, &[0; 16])?;
                    let mut lock = records::acquire_lock(&mut writer_paths, LockMode::Exclusive)?;
                    lock.unlock()?;
                    Err(LocalCoreStateStoreError::Integrity)
                } else {
                    Ok(Some(42))
                }
            })?;
            assert_eq!(calls, 2, "a racing writer requires replay under its lock");
            Ok(result)
        })
        .await
        .unwrap();
    assert_eq!(value, Some(42));
}

#[tokio::test]
async fn inspection_neither_flushes_nor_exposes_a_deferred_after_image() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    let first = super::super::tests::crash_lineage();
    assert!(
        WorkspaceLineageStore::compare_and_swap(&owner, first.workspace_id, 0, first.clone())
            .await
            .unwrap()
    );
    let mut second = first.clone();
    second.revision += 1;
    let (deferred, durability) = owner.defer_durability();
    assert!(
        WorkspaceLineageStore::compare_and_swap(
            &deferred,
            first.workspace_id,
            first.revision,
            second.clone()
        )
        .await
        .unwrap()
    );
    let Ownership::Owned(control) = &owner.ownership else {
        unreachable!()
    };
    let flushed = control.log().as_ref().unwrap().flushed;
    let before = files(directory.path());
    let selected = owner.inspecting();
    assert_eq!(
        WorkspaceLineageStore::load(&selected, first.workspace_id)
            .await
            .unwrap(),
        Some(first.clone())
    );
    let cancellation = crate::CancellationToken::new();
    let budget = crate::WorkBudget {
        durability_operations: 0,
        ..crate::WorkBudget::UNBOUNDED
    };
    let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let native = selected.materialization(work.clone());
    assert_eq!(
        WorkspaceLineageStore::load(&native, first.workspace_id)
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(work.receipt().durability_operations, 0);
    assert_eq!(control.log().as_ref().unwrap().flushed, flushed);
    assert!(control.pending.load(Ordering::Acquire));
    assert_eq!(files(directory.path()), before);
    assert!(
        WorkspaceLineageStore::compare_and_swap(
            &selected,
            first.workspace_id,
            first.revision,
            second.clone()
        )
        .await
        .is_err(),
        "inspection handles must reject publication"
    );
    durability.commit().await.unwrap();
    assert_eq!(
        WorkspaceLineageStore::load(&selected, first.workspace_id)
            .await
            .unwrap(),
        Some(second)
    );
}
