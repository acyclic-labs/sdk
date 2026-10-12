use super::*;
use crate::foundation::Digest;
use crate::materializer::{NativeMaterializationWork, NativeTreeMaterializationError};
use crate::workspace_context::{WorkspaceContextRoot, WorkspaceContextState, WorkspaceRootId};

fn finite_budget() -> crate::WorkBudget {
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
) -> NativeOwnedCoreState<WorkspaceLineageRecord> {
    let owned =
        native_records::clone_native_record_value(&RecordValue::Lineage(value.clone()), work, cxl)
            .expect("actual original producer");
    let parts = owned.into_parts();
    match parts.0 {
        RecordValue::Lineage(value) => NativeOwnedCoreState::from_parts(value, parts.1),
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
    let directory = tempfile::tempdir().expect("original namespace");
    let id = WorkspaceId::from_bytes([0xd2; 16]);
    let cancellation = crate::CancellationToken::new();
    let original =
        NativeMaterializationWork::new(finite_budget(), &cancellation).expect("finite original");
    let work = original
        .native_volume_work(finite_budget())
        .expect("actual resource view");
    let value = lineage(id);
    let store = LocalCoreStateStore::open_owned(directory.path()).expect("real owner");
    assert!(
        WorkspaceLineageStore::compare_and_swap_with_native_work(
            &store,
            id,
            0,
            paid_lineage(&value, &work, &cancellation),
            &work,
            &cancellation,
        )
        .await
        .expect("stock original CAS")
    );
    let loaded = WorkspaceLineageStore::load_with_native_work(&store, id, &work, &cancellation)
        .await
        .expect("stock original load")
        .expect("exact record");
    assert_eq!(loaded.as_state(), &value);
    let parts = loaded.into_parts();
    assert!(parts.1.matches_work(&work));
    drop(parts);
    assert!(
        !WorkspaceLineageStore::compare_and_swap_with_native_work(
            &store,
            id,
            0,
            paid_lineage(&value, &work, &cancellation),
            &work,
            &cancellation,
        )
        .await
        .expect("actual CAS miss")
    );
    drop(store);
    let restarted = LocalCoreStateStore::new(directory.path());
    let loaded = WorkspaceLineageStore::load_with_native_work(&restarted, id, &work, &cancellation)
        .await
        .expect("cold canonical WAL replay")
        .expect("durable record");
    assert_eq!(loaded.as_state(), &value);
    assert_eq!(
        WorkspaceLineageStore::load(&restarted, id)
            .await
            .expect("ordinary consumer"),
        Some(value)
    );
    work.receipt()
        .verify(finite_budget())
        .expect("same finite original receipt");
}

#[tokio::test]
async fn stock_native_input_forgeries_and_cancellation_fail_before_exchange() {
    let directory = tempfile::tempdir().expect("original namespace");
    let id = WorkspaceId::from_bytes([0xd3; 16]);
    let cancellation = crate::CancellationToken::new();
    let original =
        NativeMaterializationWork::new(finite_budget(), &cancellation).expect("finite original");
    let work = original
        .native_volume_work(finite_budget())
        .expect("first resource");
    let other = original
        .native_volume_work(finite_budget())
        .expect("distinct same-ledger resource");
    let store = LocalCoreStateStore::new(directory.path().join("not-created"));
    let foreign = paid_lineage(&lineage(id), &other, &cancellation);
    let before = work.receipt();
    let error = WorkspaceLineageStore::compare_and_swap_with_native_work(
        &store,
        id,
        0,
        foreign,
        &work,
        &cancellation,
    )
    .await
    .expect_err("resource substitution");
    assert!(matches!(
        error.0,
        crate::lineage::WorkspaceLineageError::Store(
            LocalCoreStateStoreError::NativeLedgerMismatch
        )
    ));
    assert_eq!(work.receipt(), before);
    let forged = NativeOwnedCoreState::from_parts(
        lineage(id),
        work.claim(0, 0, crate::WorkCounters::UNCHARGED)
            .expect("zero-byte caller guard"),
    );
    let error = WorkspaceLineageStore::compare_and_swap_with_native_work(
        &store,
        id,
        0,
        forged,
        &work,
        &cancellation,
    )
    .await
    .expect_err("unfunded caller payload");
    assert!(matches!(
        error.0,
        crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::Integrity)
    ));
    assert!(
        !store.root().exists(),
        "neither denial creates or opens the namespace"
    );
    cancellation.cancel();
    let before = work.receipt();
    let error = WorkspaceLineageStore::load_with_native_work(&store, id, &work, &cancellation)
        .await
        .expect_err("original cancellation");
    assert!(matches!(
        error.0,
        crate::lineage::WorkspaceLineageError::Store(
            LocalCoreStateStoreError::MaterializationWork(
                NativeTreeMaterializationError::Cancelled
            )
        )
    ));
    assert_eq!(work.receipt(), before);
    assert!(!store.root().exists());
}

#[tokio::test]
async fn stock_native_context_projects_deep_payload_and_stops_at_exact_source_limit() {
    let directory = tempfile::tempdir().expect("original namespace");
    let id = WorkspaceContextId::from_bytes([0xd4; 16]);
    let value = context(id, None, directory.path());
    let store = LocalCoreStateStore::new(directory.path());
    assert!(
        WorkspaceContextStore::compare_and_swap(&store, id, 0, value.clone())
            .await
            .expect("legacy canonical fixture")
    );
    let cancellation = crate::CancellationToken::new();
    let mut tight = finite_budget();
    tight.source_bytes_read = 7;
    let tight_original =
        NativeMaterializationWork::new(finite_budget(), &cancellation).expect("original");
    let tight_work = tight_original
        .native_volume_work(tight)
        .expect("finite exact read view");
    let error =
        WorkspaceContextStore::load_with_native_work(&store, id, &tight_work, &cancellation)
            .await
            .expect_err("real original read boundary");
    assert!(matches!(
        error.0,
        crate::workspace_context::WorkspaceContextError::Store(
            LocalCoreStateStoreError::MaterializationWork(NativeTreeMaterializationError::Work(_))
        )
    ));
    assert_eq!(tight_work.receipt().source_bytes_read, 7);
    let original =
        NativeMaterializationWork::new(finite_budget(), &cancellation).expect("original");
    let work = original
        .native_volume_work(finite_budget())
        .expect("resource view");
    let loaded = WorkspaceContextStore::load_with_native_work(&store, id, &work, &cancellation)
        .await
        .expect("funded deep projection")
        .expect("context");
    assert_eq!(loaded.as_state(), &value);
    let mut parts = loaded.into_parts();
    assert!(parts.1.matches_work(&work));
    assert_eq!(
        parts.1.bytes(),
        native_records::native_context_owned_bytes(&parts.0, &work).expect("actual capacities")
    );
    parts.0.revision += 1;
    parts.0.state = WorkspaceContextState::Frozen;
    assert!(
        WorkspaceContextStore::compare_and_swap_with_native_work(
            &store,
            id,
            1,
            NativeOwnedCoreState::from_parts(parts.0, parts.1),
            &work,
            &cancellation,
        )
        .await
        .expect("same canonical context CAS")
    );
    drop(store);
    let reopened = LocalCoreStateStore::new(directory.path());
    let actual = WorkspaceContextStore::load_with_native_work(&reopened, id, &work, &cancellation)
        .await
        .expect("reopened original context")
        .expect("context");
    assert_eq!(actual.as_state().revision, 2);
    assert_eq!(actual.as_state().state, WorkspaceContextState::Frozen);
    assert_eq!(actual.as_state().roots, value.roots);
    work.receipt()
        .verify(finite_budget())
        .expect("finite root resource");
}

async fn assert_bucket(
    store: &LocalCoreStateStore,
    parent: WorkspaceContextId,
    expected: BTreeSet<WorkspaceContextId>,
) {
    let children: Option<ContextChildren> =
        store.load_logged(parent).await.expect("canonical children");
    let count: Option<ContextChildCount> =
        store.load_logged(parent).await.expect("canonical count");
    if expected.is_empty() {
        assert!(children.is_none(), "empty affected bucket must be retired");
        assert!(count.is_none(), "its cached count must retire with it");
    } else {
        assert_eq!(children.expect("bucket").0, expected);
        assert_eq!(count.expect("count").0, expected.len() as u64);
    }
}

#[tokio::test]
async fn intact_corrupt_native_log_retains_exact_error_path_custody() {
    let directory = tempfile::tempdir().expect("canonical namespace");
    let store = LocalCoreStateStore::new(directory.path());
    let namespace = store.namespace().expect("fixture namespace");
    let paths = &namespace
        .record(CORE_LOG_FAMILY, &[0; 16])
        .expect("record paths");
    std::fs::create_dir_all(&paths.directory).expect("fixture log family");
    let payload = b"{}";
    let frame = format!(
        "{} {}\n",
        blake3::hash(payload).to_hex(),
        std::str::from_utf8(payload).expect("fixture")
    );
    std::fs::write(&paths.current, frame).expect("intact malformed canonical frame");
    let cancellation = crate::CancellationToken::new();
    let original =
        NativeMaterializationWork::new(finite_budget(), &cancellation).expect("original");
    let work = original
        .native_volume_work(finite_budget())
        .expect("actual original resource");
    let error = WorkspaceLineageStore::load_with_native_work(
        &store,
        WorkspaceId::from_bytes([0xd5; 16]),
        &work,
        &cancellation,
    )
    .await
    .expect_err("intact frame must not be ignored");
    match error.0 {
        crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::LazyWork(
            crate::LazyWorkspaceError::NativeRecordCorrupt(path, Some(allocation)),
        )) => {
            let owned = (path, allocation);
            assert_eq!(owned.1.bytes(), owned.0.capacity() as u64);
            assert!(owned.1.matches_work(&work));
            assert_eq!(owned.0, paths.current);
            drop(owned);
        }
        error => panic!("original typed path cause was lost: {error:?}"),
    }
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
                        let packet = NativeOwnedCoreState::from_parts(selected.clone(), work.claim(0, 0, crate::WorkCounters::UNCHARGED).expect("actual zero deep payload"));
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
