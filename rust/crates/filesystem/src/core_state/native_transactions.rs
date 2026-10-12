//! Original-funded admission to the existing local CoreLog transaction.

use super::*;
use crate::materializer::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError};

/// The lexical storage is destroyed before its original allocation custody.
struct NativeCorePath {
    path: PathBuf,
    allocation: NativeAllocation,
}

fn overflow() -> LocalCoreStateStoreError {
    NativeTreeMaterializationError::Work(crate::WorkError::Overflow).into()
}

fn check(work: &NativeMaterializationWork, cancellation: &crate::CancellationToken) -> Result<(), LocalCoreStateStoreError> {
    cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
    work.cancellation().check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
    work.admit(crate::WorkCounters::UNCHARGED)?;
    Ok(())
}

pub(super) fn checked_native_packet<T>(
    packet: NativeOwnedCoreState<T>,
    original: &NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    owned_bytes: impl FnOnce(&T, &NativeMaterializationWork) -> Result<u64, LocalCoreStateStoreError>,
) -> Result<NativeOwnedCoreState<T>, LocalCoreStateStoreError> {
    check(original, cancellation)?;
    // Keep the tuple intact on denial: physical DTO first, original guard last.
    let parts = packet.into_parts();
    if !parts.1.matches_work(original) {
        return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
    }
    if parts.1.bytes() != owned_bytes(&parts.0, original)? {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    Ok(NativeOwnedCoreState::from_parts(parts.0, parts.1))
}

fn copy_path(root: &Path, leaf: Option<&Path>, work: &NativeMaterializationWork) -> Result<NativeCorePath, LocalCoreStateStoreError> {
    let copied = root.as_os_str().len().checked_add(leaf.map_or(0, |path| path.as_os_str().len())).ok_or_else(overflow)?;
    let capacity = copied.checked_add(usize::from(leaf.is_some())).ok_or_else(overflow)?;
    let copy = crate::WorkCounters { bytes_copied: u64::try_from(copied).map_err(|_| overflow())?, ..crate::WorkCounters::UNCHARGED };
    work.admit(copy)?;
    let allocation = work.claim_allocation(u64::try_from(capacity).map_err(|_| overflow())?, u64::from(capacity != 0), crate::WorkCounters::UNCHARGED)?;
    let mut owned = NativeCorePath { path: PathBuf::new(), allocation };
    owned.path.try_reserve_exact(capacity).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    // Keep custody of the real allocator capacity before any late refusal.
    owned.allocation.resize_retained(u64::try_from(owned.path.capacity()).map_err(|_| overflow())?)?;
    owned.path.push(root);
    if let Some(leaf) = leaf { owned.path.push(leaf); }
    work.spent(copy)?;
    Ok(owned)
}

struct NativeCoreLogFrame<F> {
    operation: Option<F>,
    root: Option<NativeCorePath>,
    ownership: Ownership,
    deferral: Option<Arc<AtomicBool>>,
    work: NativeMaterializationWork,
    cancellation: crate::CancellationToken,
}

/// Box backing is physically freed before the guard which admitted that Box.
/// The named frame stays with the accepted worker, not its waiting future.
struct NativeCoreLogSource<F> {
    frame: Box<NativeCoreLogFrame<F>>,
    _allocation: NativeAllocation,
}

struct NativeCoreNamespace {
    namespace: Namespace,
    _allocation: NativeAllocation,
}

impl<F> NativeCoreLogSource<F> {
    fn run<T>(mut self) -> Result<T, LocalCoreStateStoreError>
    where F: FnOnce(&Namespace, &mut CoreLog, &NativeMaterializationWork, &crate::CancellationToken) -> Result<T, LocalCoreStateStoreError> {
        check(&self.frame.work, &self.frame.cancellation)?;
        let root = self.frame.root.take().ok_or(LocalCoreStateStoreError::Integrity)?;
        let admission = match &self.frame.ownership {
            Ownership::Shared => {
                crate::native_host::create_directories(&root.path, &mut &self.frame.work)?;
                let lock_path = copy_path(&root.path, Some(Path::new(OWNER_LOCK)), &self.frame.work)?;
                let lock = materialization_open_options(&lock_path.path, &crate::native_host::NativeOpenOptions::ReadWriteCreate, &self.frame.work)?;
                let cleanup = self.frame.work.reserve_cleanup_write()?;
                let read = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
                self.frame.work.admit(read)?;
                check(&self.frame.work, &self.frame.cancellation)?;
                let acquired = FileExt::try_lock_shared(&lock);
                // Acquire the cleanup owner before reporting completed work.
                let held = acquired.as_ref().is_ok();
                let owner_lock = held.then(|| OwnerLock(lock, Some(cleanup)));
                self.frame.work.spent(read)?;
                match acquired {
                    Ok(()) => Admission::Shared { _lock: owner_lock.ok_or(LocalCoreStateStoreError::Integrity)? },
                    Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                        return Err(LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeRecordOwnershipConflict(root.path, Some(root.allocation))));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ownership::Owned(owner) => Admission::Owned(Arc::clone(owner)),
        };
        let namespace = NativeCoreNamespace {
            namespace: Namespace {
                root: root.path,
                durability: if matches!(admission, Admission::Owned(_)) && self.frame.deferral.as_ref().is_some_and(|deferral| deferral.load(Ordering::Acquire)) { Durability::Deferred } else { Durability::Flushed },
                admission,
            },
            _allocation: root.allocation,
        };
        check(&self.frame.work, &self.frame.cancellation)?;
        let operation = self.frame.operation.take().ok_or(LocalCoreStateStoreError::Integrity)?;
        namespace.namespace.with_measured_log(&self.frame.work, |log| {
            operation(&namespace.namespace, log, &self.frame.work, &self.frame.cancellation)
        })
    }
}

impl LocalCoreStateStore {
    pub(super) async fn native_logged_transaction<T: Send + 'static, F>(
        &self,
        original: &NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        operation: F,
    ) -> Result<T, LocalCoreStateStoreError>
    where F: FnOnce(&Namespace, &mut CoreLog, &NativeMaterializationWork, &crate::CancellationToken) -> Result<T, LocalCoreStateStoreError> + Send + 'static {
        check(original, cancellation)?;
        let root = copy_path(&self.root, None, original)?;
        let allocation = original.claim_allocation(u64::try_from(std::mem::size_of::<NativeCoreLogFrame<F>>()).map_err(|_| overflow())?, 1, crate::WorkCounters::UNCHARGED)?;
        let source = NativeCoreLogSource {
            frame: Box::new(NativeCoreLogFrame {
                operation: Some(operation), root: Some(root), ownership: self.ownership.clone(),
                deferral: self.deferral.as_ref().map(Arc::clone),
                work: original.clone(), cancellation: cancellation.clone(),
            }),
            _allocation: allocation,
        };
        // This getter only clones the SAME original/resource/cancellation owner.
        // The runtime funds its actual queue/completion/closure, not a new budget.
        let task = acyclic_native_runtime::run_blocking_io_measured(move || source.run(), original.runtime_meter_owner())
            .map_err(NativeTreeMaterializationError::from_runtime_error)?;
        task.await.map_err(NativeTreeMaterializationError::from_runtime_error)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::Digest;
    use crate::workspace_context::{WorkspaceContextRoot, WorkspaceContextState, WorkspaceRootId};

    fn finite_budget() -> crate::WorkBudget {
        let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("budget encoding");
        for value in budget.as_object_mut().expect("all resource fields").values_mut() {
            *value = serde_json::Value::from(100_000_000_u64);
        }
        serde_json::from_value(budget).expect("finite native budget")
    }

    fn lineage(id: WorkspaceId) -> WorkspaceLineageRecord {
        let generation = crate::GenerationId::new(Digest::from_bytes([0xd1; 32]));
        WorkspaceLineageRecord {
            version: 1, revision: 1, ready: true, workspace_id: id,
            workspace_name: "native-original".into(), parent_workspace_id: None,
            parent_workspace_name: None, fork_generation: generation, initial_generation: generation,
        }
    }

    fn paid_lineage(value: &WorkspaceLineageRecord, work: &NativeMaterializationWork, cxl: &crate::CancellationToken) -> NativeOwnedCoreState<WorkspaceLineageRecord> {
        let owned = native_records::clone_native_record_value(&RecordValue::Lineage(value.clone()), work, cxl).expect("actual original producer");
        let parts = owned.into_parts();
        match parts.0 {
            RecordValue::Lineage(value) => NativeOwnedCoreState::from_parts(value, parts.1),
            _ => panic!("lineage producer changed its actual type"),
        }
    }

    fn context(id: WorkspaceContextId, parent: Option<WorkspaceContextId>, directory: &Path) -> WorkspaceContext {
        let roots = (1..=2).map(|index| {
            let root_id = WorkspaceRootId::from_bytes([index; 16]);
            (root_id, WorkspaceContextRoot {
                root_id, source_path: directory.join(format!("source-{index}")),
                workspace_id: WorkspaceId::from_bytes([index + 10; 16]),
                workspace_name: format!("native-root-{index}"),
                parent_workspace_id: parent.map(|_| WorkspaceId::from_bytes([index + 20; 16])),
                mount_path: Some(directory.join(format!("mount-{index}"))),
            })
        }).collect();
        WorkspaceContext { version: 1, revision: 1, context_id: id,
            parent_context_id: parent, roots, state: WorkspaceContextState::Active }
    }

    #[tokio::test]
    async fn stock_native_lineage_preserves_original_resource_and_durable_restart() {
        let directory = tempfile::tempdir().expect("original namespace");
        let id = WorkspaceId::from_bytes([0xd2; 16]);
        let cancellation = crate::CancellationToken::new();
        let original = NativeMaterializationWork::new(finite_budget(), &cancellation).expect("finite original");
        let work = original.native_volume_work(finite_budget()).expect("actual resource view");
        let value = lineage(id);
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("real owner");
        assert!(WorkspaceLineageStore::compare_and_swap_with_native_work(
            &store, id, 0, paid_lineage(&value, &work, &cancellation), &work, &cancellation,
        ).await.expect("stock original CAS"));
        let loaded = WorkspaceLineageStore::load_with_native_work(&store, id, &work, &cancellation)
            .await.expect("stock original load").expect("exact record");
        assert_eq!(loaded.as_state(), &value);
        let parts = loaded.into_parts();
        assert!(parts.1.matches_work(&work));
        drop(parts);
        assert!(!WorkspaceLineageStore::compare_and_swap_with_native_work(
            &store, id, 0, paid_lineage(&value, &work, &cancellation), &work, &cancellation,
        ).await.expect("actual CAS miss"));
        drop(store);
        let restarted = LocalCoreStateStore::new(directory.path());
        let loaded = WorkspaceLineageStore::load_with_native_work(&restarted, id, &work, &cancellation)
            .await.expect("cold canonical WAL replay").expect("durable record");
        assert_eq!(loaded.as_state(), &value);
        assert_eq!(WorkspaceLineageStore::load(&restarted, id).await.expect("ordinary consumer"), Some(value));
        work.receipt().verify(finite_budget()).expect("same finite original receipt");
    }

    #[tokio::test]
    async fn stock_native_input_forgeries_and_cancellation_fail_before_exchange() {
        let directory = tempfile::tempdir().expect("original namespace");
        let id = WorkspaceId::from_bytes([0xd3; 16]);
        let cancellation = crate::CancellationToken::new();
        let original = NativeMaterializationWork::new(finite_budget(), &cancellation).expect("finite original");
        let work = original.native_volume_work(finite_budget()).expect("first resource");
        let other = original.native_volume_work(finite_budget()).expect("distinct same-ledger resource");
        let store = LocalCoreStateStore::new(directory.path().join("not-created"));
        let foreign = paid_lineage(&lineage(id), &other, &cancellation);
        let before = work.receipt();
        let error = WorkspaceLineageStore::compare_and_swap_with_native_work(
            &store, id, 0, foreign, &work, &cancellation,
        ).await.expect_err("resource substitution");
        assert!(matches!(error.0, crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::NativeLedgerMismatch)));
        assert_eq!(work.receipt(), before);
        let forged = NativeOwnedCoreState::from_parts(lineage(id), work.claim(0).expect("zero-byte caller guard"));
        let error = WorkspaceLineageStore::compare_and_swap_with_native_work(
            &store, id, 0, forged, &work, &cancellation,
        ).await.expect_err("unfunded caller payload");
        assert!(matches!(error.0, crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::Integrity)));
        assert!(!store.root().exists(), "neither denial creates or opens the namespace");
        cancellation.cancel();
        let before = work.receipt();
        let error = WorkspaceLineageStore::load_with_native_work(&store, id, &work, &cancellation)
            .await.expect_err("original cancellation");
        assert!(matches!(error.0, crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::MaterializationWork(NativeTreeMaterializationError::Cancelled))));
        assert_eq!(work.receipt(), before);
        assert!(!store.root().exists());
    }

    #[tokio::test]
    async fn stock_native_context_projects_deep_payload_and_stops_at_exact_source_limit() {
        let directory = tempfile::tempdir().expect("original namespace");
        let id = WorkspaceContextId::from_bytes([0xd4; 16]);
        let value = context(id, None, directory.path());
        let store = LocalCoreStateStore::new(directory.path());
        assert!(WorkspaceContextStore::compare_and_swap(&store, id, 0, value.clone()).await.expect("legacy canonical fixture"));
        let cancellation = crate::CancellationToken::new();
        let mut tight = finite_budget();
        tight.source_bytes_read = 7;
        let tight_original = NativeMaterializationWork::new(finite_budget(), &cancellation).expect("original");
        let tight_work = tight_original.native_volume_work(tight).expect("finite exact read view");
        let error = WorkspaceContextStore::load_with_native_work(&store, id, &tight_work, &cancellation)
            .await.expect_err("real original read boundary");
        assert!(matches!(error.0, crate::workspace_context::WorkspaceContextError::Store(LocalCoreStateStoreError::MaterializationWork(NativeTreeMaterializationError::Work(_)))));
        assert_eq!(tight_work.receipt().source_bytes_read, 7);
        let original = NativeMaterializationWork::new(finite_budget(), &cancellation).expect("original");
        let work = original.native_volume_work(finite_budget()).expect("resource view");
        let loaded = WorkspaceContextStore::load_with_native_work(&store, id, &work, &cancellation)
            .await.expect("funded deep projection").expect("context");
        assert_eq!(loaded.as_state(), &value);
        let mut parts = loaded.into_parts();
        assert!(parts.1.matches_work(&work));
        assert_eq!(parts.1.bytes(), native_records::native_context_owned_bytes(&parts.0, &work).expect("actual capacities"));
        parts.0.revision += 1;
        parts.0.state = WorkspaceContextState::Frozen;
        assert!(WorkspaceContextStore::compare_and_swap_with_native_work(
            &store, id, 1, NativeOwnedCoreState::from_parts(parts.0, parts.1), &work, &cancellation,
        ).await.expect("same canonical context CAS"));
        drop(store);
        let reopened = LocalCoreStateStore::new(directory.path());
        let actual = WorkspaceContextStore::load_with_native_work(&reopened, id, &work, &cancellation)
            .await.expect("reopened original context").expect("context");
        assert_eq!(actual.as_state().revision, 2);
        assert_eq!(actual.as_state().state, WorkspaceContextState::Frozen);
        assert_eq!(actual.as_state().roots, value.roots);
        work.receipt().verify(finite_budget()).expect("finite root resource");
    }

    async fn assert_bucket(store: &LocalCoreStateStore, parent: WorkspaceContextId, expected: BTreeSet<WorkspaceContextId>) {
        let children: Option<ContextChildren> = store.load_logged(parent).await.expect("canonical children");
        let count: Option<ContextChildCount> = store.load_logged(parent).await.expect("canonical count");
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
        let paths = core_log_paths(&namespace);
        std::fs::create_dir_all(&paths.directory).expect("fixture log family");
        let payload = b"{}";
        let frame = format!("{} {}\n", blake3::hash(payload).to_hex(), std::str::from_utf8(payload).expect("fixture"));
        std::fs::write(&paths.current, frame).expect("intact malformed canonical frame");
        let cancellation = crate::CancellationToken::new();
        let original = NativeMaterializationWork::new(finite_budget(), &cancellation).expect("original");
        let work = original.native_volume_work(finite_budget()).expect("actual original resource");
        let error = WorkspaceLineageStore::load_with_native_work(
            &store, WorkspaceId::from_bytes([0xd5; 16]), &work, &cancellation,
        ).await.expect_err("intact frame must not be ignored");
        match error.0 {
            crate::lineage::WorkspaceLineageError::Store(LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeRecordCorrupt(path, Some(allocation)))) => {
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
                            let packet = NativeOwnedCoreState::from_parts(selected.clone(), work.claim(0).expect("actual zero deep payload"));
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
}
