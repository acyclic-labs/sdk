use super::*;
use crate::foundation::Digest;
use crate::materializer::{NativeMaterializationWork, NativeTreeMaterializationError};
use crate::workspace_context::{WorkspaceContextRoot, WorkspaceContextState, WorkspaceRootId};

pub(super) fn finite_budget() -> crate::WorkBudget {
    let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("budget encoding");
    for value in budget
        .as_object_mut()
        .expect("all resource fields")
        .values_mut()
    {
        *value = serde_json::Value::from(100_000_000_u64);
    }
    serde_json::from_value(budget).expect("finite native budget")
}

fn lineage(id: WorkspaceId) -> WorkspaceLineageRecord {
    let generation = crate::GenerationId::new(Digest::from_bytes([0xd1; 32]));
    WorkspaceLineageRecord {
        version: 1,
        revision: 1,
        ready: true,
        workspace_id: id,
        workspace_name: "native-original".into(),
        parent_workspace_id: None,
        parent_workspace_name: None,
        fork_generation: generation,
        initial_generation: generation,
    }
}

fn paid_lineage(
    value: &WorkspaceLineageRecord,
    work: &NativeMaterializationWork,
    cxl: &crate::CancellationToken,
) -> (WorkspaceLineageRecord, crate::NativeAllocation) {
    let owned =
        native_records::clone_native_record_value(&RecordValue::Lineage(value.clone()), work, cxl)
            .expect("actual original producer");
    let parts = owned;
    match parts.0 {
        RecordValue::Lineage(value) => (value, parts.1),
        _ => panic!("lineage producer changed its actual type"),
    }
}

fn context(
    id: WorkspaceContextId,
    parent: Option<WorkspaceContextId>,
    directory: &Path,
) -> WorkspaceContext {
    let roots = (1..=2)
        .map(|index| {
            let root_id = WorkspaceRootId::from_bytes([index; 16]);
            (
                root_id,
                WorkspaceContextRoot {
                    root_id,
                    source_path: directory.join(format!("source-{index}")),
                    workspace_id: WorkspaceId::from_bytes([index + 10; 16]),
                    workspace_name: format!("native-root-{index}"),
                    parent_workspace_id: parent.map(|_| WorkspaceId::from_bytes([index + 20; 16])),
                    mount_path: Some(directory.join(format!("mount-{index}"))),
                },
            )
        })
        .collect();
    WorkspaceContext {
        version: 1,
        revision: 1,
        context_id: id,
        parent_context_id: parent,
        roots,
        state: WorkspaceContextState::Active,
    }
}

#[tokio::test]
async fn stock_native_lineage_preserves_original_resource_and_durable_restart() {
    let directory = tempfile::tempdir().unwrap();
    let id = WorkspaceId::from_bytes([0xd2; 16]);
    let cxl = crate::CancellationToken::new();
    let original = NativeMaterializationWork::new(finite_budget(), &cxl).unwrap();
    let work = original.native_volume_work(finite_budget()).unwrap();
    let value = lineage(id);
    let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    for expected in [true, false] {
        assert_eq!(
            WorkspaceLineageStore::compare_and_swap_with_native_work(
                &store,
                id,
                0,
                paid_lineage(&value, &work, &cxl),
                &work,
                &cxl,
            )
            .await
            .unwrap(),
            expected,
        );
        if expected {
            let loaded = WorkspaceLineageStore::load_with_native_work(&store, id, &work, &cxl)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.0, value);
            assert!(loaded.1.work().shares_allocation_context(&work));
        }
    }
    drop(store);
    let reopened = LocalCoreStateStore::new(directory.path());
    let loaded = WorkspaceLineageStore::load_with_native_work(&reopened, id, &work, &cxl)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.0, value);
    assert_eq!(
        WorkspaceLineageStore::load(&reopened, id).await.unwrap(),
        Some(value)
    );
    work.receipt().verify(finite_budget()).unwrap();
}

#[tokio::test]
async fn stock_native_input_forgeries_and_cancellation_fail_before_exchange() {
    let directory = tempfile::tempdir().unwrap();
    let id = WorkspaceId::from_bytes([0xd3; 16]);
    let cxl = crate::CancellationToken::new();
    let original = NativeMaterializationWork::new(finite_budget(), &cxl).unwrap();
    let work = original.native_volume_work(finite_budget()).unwrap();
    let other = original.native_volume_work(finite_budget()).unwrap();
    let store = LocalCoreStateStore::new(directory.path().join("not-created"));
    let foreign = paid_lineage(&lineage(id), &other, &cxl);
    let before = work.receipt();
    let error = WorkspaceLineageStore::compare_and_swap_with_native_work(
        &store, id, 0, foreign, &work, &cxl,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error.0,
        crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::LazyWork(
            crate::LazyWorkspaceError::NativeLedgerMismatch
        ))
    ));
    assert_eq!(work.receipt(), before);
    let forged = (
        lineage(id),
        work.claim(0, 0, crate::WorkCounters::UNCHARGED).unwrap(),
    );
    let error = WorkspaceLineageStore::compare_and_swap_with_native_work(
        &store, id, 0, forged, &work, &cxl,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error.0,
        crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::Integrity)
    ));
    assert!(!store.root().exists());
    cxl.cancel();
    let before = work.receipt();
    let error = WorkspaceLineageStore::load_with_native_work(&store, id, &work, &cxl)
        .await
        .unwrap_err();
    assert!(matches!(
        error.0,
        crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::LazyWork(
            crate::LazyWorkspaceError::Cancelled
        ))
    ));
    assert_eq!(work.receipt(), before);
    assert!(!store.root().exists());
}

#[tokio::test]
async fn stock_native_context_projects_deep_payload_and_stops_at_exact_source_limit() {
    let directory = tempfile::tempdir().unwrap();
    let id = WorkspaceContextId::from_bytes([0xd4; 16]);
    let value = context(id, None, directory.path());
    let store = LocalCoreStateStore::new(directory.path());
    assert!(
        WorkspaceContextStore::compare_and_swap(&store, id, 0, value.clone())
            .await
            .unwrap()
    );
    let cxl = crate::CancellationToken::new();
    let original = NativeMaterializationWork::new(finite_budget(), &cxl).unwrap();
    let tight = original
        .native_volume_work(crate::WorkBudget {
            source_bytes_read: 7,
            ..finite_budget()
        })
        .unwrap();
    let error = WorkspaceContextStore::load_with_native_work(&store, id, &tight, &cxl)
        .await
        .unwrap_err();
    assert!(matches!(
        error.0,
        crate::workspace_context::WorkspaceContextError::Store(LocalCoreStateStoreError::LazyWork(
            crate::LazyWorkspaceError::Demand(crate::DemandError::Work(_))
        ))
    ));
    assert_eq!(tight.receipt().source_bytes_read, 7);
    let original = NativeMaterializationWork::new(finite_budget(), &cxl).unwrap();
    let work = original.native_volume_work(finite_budget()).unwrap();
    let mut loaded = WorkspaceContextStore::load_with_native_work(&store, id, &work, &cxl)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.0, value);
    assert!(loaded.1.work().shares_allocation_context(&work));
    assert_eq!(
        loaded.1.bytes(),
        native_records::native_context_owned_bytes(&loaded.0, &work).unwrap()
    );
    loaded.0.revision += 1;
    loaded.0.state = WorkspaceContextState::Frozen;
    assert!(
        WorkspaceContextStore::compare_and_swap_with_native_work(
            &store, id, 1, loaded, &work, &cxl,
        )
        .await
        .unwrap()
    );
    drop(store);
    let reopened = LocalCoreStateStore::new(directory.path());
    let loaded = WorkspaceContextStore::load_with_native_work(&reopened, id, &work, &cxl)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.0.revision, 2);
    assert_eq!(loaded.0.state, WorkspaceContextState::Frozen);
    assert_eq!(loaded.0.roots, value.roots);
    work.receipt().verify(finite_budget()).unwrap();
}

async fn assert_bucket(
    store: &LocalCoreStateStore,
    parent: WorkspaceContextId,
    expected: BTreeSet<WorkspaceContextId>,
) {
    let children: Option<ContextChildren> = store.load_logged(parent).await.unwrap();
    let count: Option<ContextChildCount> = store.load_logged(parent).await.unwrap();
    if expected.is_empty() {
        assert!(children.is_none());
        assert!(count.is_none());
    } else {
        assert_eq!(children.unwrap().0, expected);
        assert_eq!(count.unwrap().0, expected.len() as u64);
    }
}

#[tokio::test]
async fn intact_corrupt_native_log_retains_exact_error_path_custody() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().unwrap();
    let paths = namespace.record(CORE_LOG_FAMILY, &[0; 16]).unwrap();
    std::fs::create_dir_all(&paths.directory).unwrap();
    let frame = format!("{} {{}}\n", blake3::hash(b"{}").to_hex());
    std::fs::write(&paths.current, frame).unwrap();
    let cxl = crate::CancellationToken::new();
    let original = NativeMaterializationWork::new(finite_budget(), &cxl).unwrap();
    let work = original.native_volume_work(finite_budget()).unwrap();
    let error = WorkspaceLineageStore::load_with_native_work(
        &store,
        WorkspaceId::from_bytes([0xd5; 16]),
        &work,
        &cxl,
    )
    .await
    .unwrap_err();
    let crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::LazyWork(
        crate::LazyWorkspaceError::NativeRecordCorrupt(path, Some(allocation)),
    )) = error.0
    else {
        panic!("corrupt log lost its typed path");
    };
    let owned = (path, allocation);
    assert_eq!(owned.1.bytes(), owned.0.capacity() as u64);
    assert!(owned.1.work().shares_allocation_context(&work));
    assert_eq!(owned.0, paths.current);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
        fn ordinary_and_native_context_parent_transitions_survive_real_wal_restart(moves in proptest::collection::vec(0_u8..4, 1..12)) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("test runtime");
            runtime.block_on(async {
                for native in [false, true] {
                    let directory = tempfile::tempdir().expect("real canonical namespace");
                    let parents = [1_u8, 2, 3].map(|byte| WorkspaceContextId::from_bytes([byte; 16]));
                    let child = WorkspaceContextId::from_bytes([4; 16]);
                    let sibling = WorkspaceContextId::from_bytes([5; 16]);
                    let store = LocalCoreStateStore::open_owned(directory.path()).expect("real owner");
                    for parent in parents {
                        let mut root = context(parent, None, directory.path());
                        root.roots.clear();
                        assert!(WorkspaceContextStore::compare_and_swap(&store, parent, 0, root).await.expect("root fixture"));
                    }
                    let mut selected = context(child, Some(parents[0]), directory.path());
                    selected.roots.clear();
                    let mut other = context(sibling, Some(parents[1]), directory.path());
                    other.roots.clear();
                    assert!(WorkspaceContextStore::compare_and_swap(&store, child, 0, selected.clone()).await.expect("child fixture"));
                    assert!(WorkspaceContextStore::compare_and_swap(&store, sibling, 0, other).await.expect("sibling fixture"));
                    let cancellation = crate::CancellationToken::new();
                    let original = NativeMaterializationWork::new(finite_budget(), &cancellation).expect("finite original");
                    let work = original.native_volume_work(finite_budget()).expect("original resource");
                    // Always exercise last-child removal and an unaffected sibling;
                    // generated suffixes then explore repeated moves and no-ops.
                    for destination in [1_u8, 3, 2, 0].iter().chain(moves.iter()) {
                        let previous = selected.revision;
                        selected.revision += 1;
                        selected.parent_context_id = parents.get(*destination as usize).copied();
                        if native {
                            let packet = (selected.clone(), work.claim(0, 0, crate::WorkCounters::UNCHARGED).expect("actual zero deep payload"));
                            assert!(WorkspaceContextStore::compare_and_swap_with_native_work(
                                &store, child, previous, packet, &work, &cancellation,
                            ).await.expect("original parent transition"));
                        } else {
                            assert!(WorkspaceContextStore::compare_and_swap(&store, child, previous, selected.clone()).await.expect("ordinary parent transition"));
                        }
                        for parent in parents {
                            let mut expected = BTreeSet::new();
                            if parent == parents[1] { expected.insert(sibling); }
                            if Some(parent) == selected.parent_context_id { expected.insert(child); }
                            assert_bucket(&store, parent, expected).await;
                        }
                    }
                    drop(store);
                    let reopened = LocalCoreStateStore::new(directory.path());
                    assert_eq!(WorkspaceContextStore::load(&reopened, child).await.expect("reopened child"), Some(selected.clone()));
                    for parent in parents {
                        let mut expected = BTreeSet::new();
                        if parent == parents[1] { expected.insert(sibling); }
                        if Some(parent) == selected.parent_context_id { expected.insert(child); }
                        assert_bucket(&reopened, parent, expected).await;
                    }
                    work.receipt().verify(finite_budget()).expect("same finite original receipt");
                }
            });
        }
}
