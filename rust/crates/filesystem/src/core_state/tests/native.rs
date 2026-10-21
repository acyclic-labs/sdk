use super::*;
use crate::materializer::NativeMaterializationWork;
use crate::workspace_context::{WorkspaceContextRoot, WorkspaceContextState, WorkspaceRootId};

pub(super) fn finite_budget() -> crate::WorkBudget {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).unwrap();
    for value in budget.as_object_mut().unwrap().values_mut() {
        *value = serde_json::Value::from(100_000_000_u64);
    }
    serde_json::from_value(budget).unwrap()
}

fn lineage(id: WorkspaceId) -> WorkspaceLineageRecord {
    WorkspaceLineageRecord {
        workspace_id: id,
        workspace_name: "native-original".into(),
        parent_workspace_id: None,
        parent_workspace_name: None,
        ..crash_lineage()
    }
}

fn paid_lineage(
    value: &WorkspaceLineageRecord,
    work: &NativeMaterializationWork,
    cxl: &crate::CancellationToken,
) -> (WorkspaceLineageRecord, crate::NativeAllocation) {
    let owned =
        native_records::clone_native_record_value(&RecordValue::Lineage(value.clone()), work, cxl)
            .unwrap();
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
            store
                .compare_lineage(id, 0, paid_lineage(&value, &work, &cxl), &work, &cxl,)
                .await
                .unwrap(),
            expected,
        );
        if expected {
            let loaded = store.load_lineage(id, &work, &cxl).await.unwrap().unwrap();
            assert_eq!(loaded.0, value);
            assert!(loaded.1.work().shares_allocation_context(&work));
        }
    }
    drop(store);
    let reopened = LocalCoreStateStore::new(directory.path());
    let loaded = reopened
        .load_lineage(id, &work, &cxl)
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
    let error = store
        .compare_lineage(id, 0, foreign, &work, &cxl)
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
    let error = store
        .compare_lineage(id, 0, forged, &work, &cxl)
        .await
        .unwrap_err();
    assert!(matches!(
        error.0,
        crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::Integrity)
    ));
    assert!(!store.root().exists());
    cxl.cancel();
    let before = work.receipt();
    let error = store.load_lineage(id, &work, &cxl).await.unwrap_err();
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
    let error = store.load_context(id, &tight, &cxl).await.unwrap_err();
    assert!(matches!(
        error.0,
        crate::workspace_context::WorkspaceContextError::Store(LocalCoreStateStoreError::LazyWork(
            crate::LazyWorkspaceError::Demand(crate::DemandError::Work(_))
        ))
    ));
    assert_eq!(tight.receipt().source_bytes_read, 7);
    let original = NativeMaterializationWork::new(finite_budget(), &cxl).unwrap();
    let work = original.native_volume_work(finite_budget()).unwrap();
    let mut loaded = store.load_context(id, &work, &cxl).await.unwrap().unwrap();
    assert_eq!(loaded.0, value);
    assert!(loaded.1.work().shares_allocation_context(&work));
    assert_eq!(
        loaded.1.bytes(),
        native_records::native_context_owned_bytes(&loaded.0, &work).unwrap()
    );
    loaded.0.revision += 1;
    loaded.0.state = WorkspaceContextState::Frozen;
    assert!(
        store
            .compare_context(id, 1, loaded, &work, &cxl,)
            .await
            .unwrap()
    );
    drop(store);
    let reopened = LocalCoreStateStore::new(directory.path());
    let loaded = reopened
        .load_context(id, &work, &cxl)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.0.revision, 2);
    assert_eq!(loaded.0.state, WorkspaceContextState::Frozen);
    assert_eq!(loaded.0.roots, value.roots);
    work.receipt().verify(finite_budget()).unwrap();
}

fn assert_bucket_values(
    children: Option<ContextChildren>,
    count: Option<ContextChildCount>,
    expected: BTreeSet<WorkspaceContextId>,
) {
    if expected.is_empty() {
        assert!(children.is_none());
        assert!(count.is_none());
    } else {
        assert_eq!(children.unwrap().0, expected);
        assert_eq!(count.unwrap().0, expected.len() as u64);
    }
}

async fn assert_parent_buckets(
    store: &LocalCoreStateStore,
    parents: [WorkspaceContextId; 3],
    child: WorkspaceContextId,
    sibling: WorkspaceContextId,
    selected_parent: Option<WorkspaceContextId>,
) {
    // Check the same six records at one WAL cut, with one native I/O dispatch.
    let buckets = store
        .transaction(move |root| {
            root.with_log(|log| {
                parents
                    .into_iter()
                    .map(|parent| {
                        Ok((
                            log.get::<ContextChildren>(&root, parent, None, None)?
                                .map(|(value, _)| value),
                            log.get::<ContextChildCount>(&root, parent, None, None)?
                                .map(|(value, _)| value),
                        ))
                    })
                    .collect::<Result<Vec<_>, LocalCoreStateStoreError>>()
            })
        })
        .await
        .unwrap();
    assert_eq!(buckets.len(), parents.len());
    for (parent, (children, count)) in parents.into_iter().zip(buckets) {
        let mut expected = BTreeSet::new();
        if parent == parents[1] {
            expected.insert(sibling);
        }
        if Some(parent) == selected_parent {
            expected.insert(child);
        }
        assert_bucket_values(children, count, expected);
    }
}

#[tokio::test]
async fn intact_corrupt_native_log_retains_exact_error_path_custody() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().unwrap();
    let paths = namespace.record(CORE_LOG_FAMILY, &[0; 16]).unwrap();
    std::fs::create_dir_all(paths.directory()).unwrap();
    let frame = format!("{} {{}}\n", blake3::hash(b"{}").to_hex());
    std::fs::write(&paths.current, frame).unwrap();
    let cxl = crate::CancellationToken::new();
    let original = NativeMaterializationWork::new(finite_budget(), &cxl).unwrap();
    let work = original.native_volume_work(finite_budget()).unwrap();
    let error = store
        .load_lineage(WorkspaceId::from_bytes([0xd5; 16]), &work, &cxl)
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

static CONTEXT_PARENT_BOOTSTRAP_WRITES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

fn context_parent_bootstrap() -> &'static tempfile::TempDir {
    static BOOTSTRAP: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    BOOTSTRAP.get_or_init(|| {
        crate::initialize_test_native_io();
        let directory = tempfile::tempdir().unwrap();
        futures::executor::block_on(async {
            let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
            for (byte, parent) in [(1, None), (2, None), (3, None), (4, Some(1)), (5, Some(2))] {
                let id = WorkspaceContextId::from_bytes([byte; 16]);
                let parent = parent.map(|byte| WorkspaceContextId::from_bytes([byte; 16]));
                let mut state = context(id, parent, directory.path());
                // The seed contains no native paths and can be copied to an independent store.
                state.roots.clear();
                assert!(
                    WorkspaceContextStore::compare_and_swap(&store, id, 0, state)
                        .await
                        .unwrap()
                );
                CONTEXT_PARENT_BOOTSTRAP_WRITES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
        directory
    })
}

async fn context_parent_walk(
    directory: &Path,
    native: bool,
    mut selected: WorkspaceContext,
    moves: &[u8],
) {
    let parents = [1_u8, 2, 3].map(|byte| WorkspaceContextId::from_bytes([byte; 16]));
    let child = WorkspaceContextId::from_bytes([4; 16]);
    let sibling = WorkspaceContextId::from_bytes([5; 16]);
    let store = LocalCoreStateStore::open_owned(directory).unwrap();
    let cancellation = crate::CancellationToken::new();
    let original = NativeMaterializationWork::new(finite_budget(), &cancellation).unwrap();
    let work = original.native_volume_work(finite_budget()).unwrap();
    for destination in moves {
        let previous = selected.revision;
        selected.revision += 1;
        selected.parent_context_id = parents.get(*destination as usize).copied();
        if native {
            let packet = (
                selected.clone(),
                work.claim(0, 0, crate::WorkCounters::UNCHARGED).unwrap(),
            );
            assert!(
                store
                    .compare_context(child, previous, packet, &work, &cancellation)
                    .await
                    .unwrap()
            );
        } else {
            assert!(
                WorkspaceContextStore::compare_and_swap(&store, child, previous, selected.clone())
                    .await
                    .unwrap()
            );
        }
        assert_parent_buckets(&store, parents, child, sibling, selected.parent_context_id).await;
    }
    drop(store);
    let reopened = LocalCoreStateStore::new(directory);
    assert_eq!(
        WorkspaceContextStore::load(&reopened, child).await.unwrap(),
        Some(selected.clone())
    );
    assert_parent_buckets(
        &reopened,
        parents,
        child,
        sibling,
        selected.parent_context_id,
    )
    .await;
    work.receipt().verify(finite_budget()).unwrap();
}

static CONTEXT_PARENT_PREFIX_WRITES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

fn context_parent_prefixes() -> &'static [tempfile::TempDir; 2] {
    static PREFIXES: std::sync::OnceLock<[tempfile::TempDir; 2]> = std::sync::OnceLock::new();
    PREFIXES.get_or_init(|| {
        [false, true].map(|native| {
            let directory = tempfile::tempdir().unwrap();
            copy_tree(context_parent_bootstrap().path(), directory.path());
            let child = WorkspaceContextId::from_bytes([4; 16]);
            let parent = WorkspaceContextId::from_bytes([1; 16]);
            let mut selected = context(child, Some(parent), directory.path());
            selected.roots.clear();
            // Qualify the fixed path once through each API, including its cold reopen.
            futures::executor::block_on(context_parent_walk(
                directory.path(),
                native,
                selected,
                &[1, 3, 2, 0],
            ));
            CONTEXT_PARENT_PREFIX_WRITES.fetch_add(4, std::sync::atomic::Ordering::SeqCst);
            directory
        })
    })
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn ordinary_and_native_context_parent_transitions_survive_real_wal_restart(moves in proptest::collection::vec(0_u8..4, 1..12)) {
        let prefixes = context_parent_prefixes();
        assert_eq!(CONTEXT_PARENT_BOOTSTRAP_WRITES.load(std::sync::atomic::Ordering::SeqCst), 5,
            "the immutable context seed must be committed once for the entire property");
        assert_eq!(CONTEXT_PARENT_PREFIX_WRITES.load(std::sync::atomic::Ordering::SeqCst), 8,
            "the fixed context path must be qualified once through each API");
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            for (native, prefix) in [false, true].into_iter().zip(prefixes) {
                let directory = tempfile::tempdir().unwrap();
                copy_tree(prefix.path(), directory.path());
                let child = WorkspaceContextId::from_bytes([4; 16]);
                let parent = WorkspaceContextId::from_bytes([1; 16]);
                let mut selected = context(child, Some(parent), directory.path());
                selected.roots.clear();
                // Every generated walk starts at the same state and revision as before.
                selected.revision = 5;
                context_parent_walk(directory.path(), native, selected, &moves).await;
            }
        });
    }
}
