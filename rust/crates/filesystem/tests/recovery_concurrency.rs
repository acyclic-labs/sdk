//! Deterministic recovery and concurrency conformance for durable filesystem protocols.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_let_else,
    clippy::panic,
    reason = "test fixtures use explicit fatal assertions to keep exhaustive state-machine cases readable"
)]

use acyclic_fs::{
    Digest, GenerationId, JournaledMaterializer, MaterializationBackend, MaterializationEdit,
    MaterializationError, MaterializationJournal, MaterializationJournalStore,
    MaterializationPhase, MaterializationPlan, MaterializationPreimage, MaterializationRecovery,
    OperationId, OperationWindowCoordinator, OperationWindowError, OperationWindowFinish,
    OperationWindowPhase, OperationWindowSnapshot, OperationWindowStore, WorkspaceId,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Barrier;

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn workspace(byte: u8) -> WorkspaceId {
    WorkspaceId::from_bytes([byte; 16])
}

#[derive(Clone, Default)]
struct SharedWindowStore(Arc<Mutex<BTreeMap<WorkspaceId, OperationWindowSnapshot>>>);

#[derive(Debug, Error)]
#[error("window test store unavailable")]
struct WindowStoreError;

impl OperationWindowStore for SharedWindowStore {
    type Error = WindowStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        self.0
            .lock()
            .map_err(|_| WindowStoreError)
            .map(|states| states.get(&workspace_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        let mut states = self.0.lock().map_err(|_| WindowStoreError)?;
        if states.get(&workspace_id).map_or(0, |state| state.revision) != expected_revision {
            return Ok(false);
        }
        states.insert(workspace_id, replacement);
        Ok(true)
    }
}

#[derive(Clone)]
struct RacingWindowStore {
    inner: SharedWindowStore,
    initial_loads: Arc<AtomicUsize>,
    barrier: Arc<Barrier>,
}

impl RacingWindowStore {
    fn pair() -> (Self, Self) {
        let store = Self {
            inner: SharedWindowStore::default(),
            initial_loads: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(Barrier::new(2)),
        };
        (store.clone(), store)
    }
}

impl OperationWindowStore for RacingWindowStore {
    type Error = WindowStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        let state = self.inner.load(workspace_id).await?;
        if self.initial_loads.fetch_add(1, Ordering::SeqCst) < 2 {
            self.barrier.wait().await;
        }
        Ok(state)
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        self.inner
            .compare_and_swap(workspace_id, expected_revision, replacement)
            .await
    }
}

fn permutations(values: [usize; 3]) -> [[usize; 3]; 6] {
    [
        [values[0], values[1], values[2]],
        [values[0], values[2], values[1]],
        [values[1], values[0], values[2]],
        [values[1], values[2], values[0]],
        [values[2], values[0], values[1]],
        [values[2], values[1], values[0]],
    ]
}

#[tokio::test]
async fn every_overlapping_lease_close_order_reconciles_exactly_once() {
    for (case, order) in permutations([0, 1, 2]).into_iter().enumerate() {
        let workspace_id = workspace(u8::try_from(case + 1).expect("small case index"));
        let coordinator = OperationWindowCoordinator::new(SharedWindowStore::default());
        let mut leases = Vec::new();
        for owner in ["one", "two", "three"] {
            leases.push(
                coordinator
                    .begin(workspace_id, generation(1), owner, 10, 100)
                    .await
                    .expect("open overlapping lease"),
            );
        }
        for parent in [generation(2), generation(3), generation(4)] {
            assert!(
                coordinator
                    .observe_parent(workspace_id, parent)
                    .await
                    .expect("observe parent")
            );
        }

        for (position, lease_index) in order.into_iter().enumerate() {
            let result = coordinator
                .finish(&leases[lease_index], 20)
                .await
                .expect("close lease");
            if position < 2 {
                assert_eq!(
                    result,
                    OperationWindowFinish::StillActive {
                        remaining: u32::try_from(2 - position).expect("small remaining count")
                    }
                );
            } else {
                let OperationWindowFinish::Reconcile(reconcile) = result else {
                    panic!("the final close did not claim reconciliation");
                };
                assert_eq!(reconcile.pinned_parent, generation(1));
                assert_eq!(reconcile.pending_parent, Some(generation(4)));
                assert_eq!(
                    coordinator
                        .finish(&leases[lease_index], 21)
                        .await
                        .expect("duplicate close"),
                    OperationWindowFinish::AlreadyClosed
                );
                coordinator
                    .complete_reconcile(workspace_id, reconcile.ticket)
                    .await
                    .expect("complete reconciliation");
            }
        }
        assert!(matches!(
            coordinator
                .inspect(workspace_id)
                .await
                .expect("inspect")
                .phase,
            OperationWindowPhase::Idle
        ));
    }
}

#[tokio::test]
async fn concurrent_open_expiry_and_late_writer_fencing_survive_restart() {
    let workspace_id = workspace(0x40);
    let (left_store, right_store) = RacingWindowStore::pair();
    let restart_store = left_store.inner.clone();
    let first = OperationWindowCoordinator::new(left_store);
    let second = OperationWindowCoordinator::new(right_store);
    let (left, right) = tokio::join!(
        first.begin(workspace_id, generation(1), "left", 10, 20),
        second.begin(workspace_id, generation(2), "right", 10, 40)
    );
    let left = left.expect("left lease");
    let right = right.expect("right lease");
    assert_eq!(left.pinned_parent, right.pinned_parent);

    drop(first);
    drop(second);
    let recovered = OperationWindowCoordinator::new(restart_store);
    assert_eq!(
        recovered.finish(&left, 20).await.expect("expire left"),
        OperationWindowFinish::AlreadyClosed
    );
    assert_eq!(
        recovered
            .finish(&right, 40)
            .await
            .expect("expire final lease"),
        OperationWindowFinish::AlreadyClosed
    );
    let ticket = recovered
        .claim_reconcile(workspace_id, 40)
        .await
        .expect("claim expired window")
        .expect("durable recovery ticket");
    assert!(matches!(
        recovered
            .begin(workspace_id, generation(3), "late", 41, 60)
            .await,
        Err(OperationWindowError::Reconciling)
    ));
    recovered
        .observe_parent(workspace_id, generation(4))
        .await
        .expect("observe during reconciliation");
    recovered
        .complete_reconcile(workspace_id, ticket.ticket)
        .await
        .expect("complete first ticket");
    let next = recovered
        .claim_reconcile(workspace_id, 42)
        .await
        .expect("claim subsequent parent")
        .expect("durable subsequent ticket");
    assert_eq!(next.pending_parent, Some(generation(4)));
    assert!(matches!(
        recovered
            .complete_reconcile(workspace_id, ticket.ticket)
            .await,
        Err(OperationWindowError::StaleTicket)
    ));
}

#[derive(Clone, Default)]
struct CrashJournalStore(Arc<Mutex<CrashJournalState>>);

#[derive(Default)]
struct CrashJournalState {
    journals: BTreeMap<OperationId, MaterializationJournal>,
    writes: usize,
    fail_after_write: Option<usize>,
}

impl CrashJournalStore {
    fn fail_on_relative_write(&self, offset: usize) {
        let mut state = self.0.lock().expect("journal state");
        state.fail_after_write = Some(state.writes + offset);
    }

    fn clear_failure(&self) {
        self.0.lock().expect("journal state").fail_after_write = None;
    }
}

#[derive(Debug, Error)]
#[error("simulated process crash after durable journal write")]
struct CrashStoreError;

impl MaterializationJournalStore for CrashJournalStore {
    type Error = CrashStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        Ok(self
            .0
            .lock()
            .map_err(|_| CrashStoreError)?
            .journals
            .get(&operation_id)
            .cloned())
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<bool, Self::Error> {
        let mut state = self.0.lock().map_err(|_| CrashStoreError)?;
        if state
            .journals
            .get(&operation_id)
            .map_or(0, |journal| journal.revision)
            != expected_revision
        {
            return Ok(false);
        }
        state.journals.insert(operation_id, replacement);
        state.writes += 1;
        if state.fail_after_write == Some(state.writes) {
            return Err(CrashStoreError);
        }
        Ok(true)
    }
}

#[derive(Clone)]
struct TreeBackend(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

#[derive(Debug, Error)]
#[error("tree test backend unavailable")]
struct TreeBackendError;

impl TreeBackend {
    fn source() -> Self {
        Self(Arc::new(Mutex::new(source_tree())))
    }

    fn snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        self.0.lock().expect("tree state").clone()
    }
}

fn source_tree() -> BTreeMap<String, Vec<u8>> {
    BTreeMap::from([
        ("a".to_owned(), b"old-a".to_vec()),
        ("b".to_owned(), b"old-b".to_vec()),
        ("from".to_owned(), b"old-from".to_vec()),
        ("to".to_owned(), b"old-to".to_vec()),
    ])
}

fn target_tree() -> BTreeMap<String, Vec<u8>> {
    BTreeMap::from([
        ("a".to_owned(), b"new-a".to_vec()),
        ("to".to_owned(), b"old-from".to_vec()),
    ])
}

impl MaterializationBackend for TreeBackend {
    type Error = TreeBackendError;

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let entries = self.0.lock().map_err(|_| TreeBackendError)?;
        let image = match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                encode_preimages([entries.get(path).cloned(), None])
            }
            MaterializationEdit::Rename { from, to } => {
                encode_preimages([entries.get(from).cloned(), entries.get(to).cloned()])
            }
            _ => return Err(TreeBackendError),
        };
        Ok(MaterializationPreimage { image })
    }

    async fn apply(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
        let mut entries = self.0.lock().map_err(|_| TreeBackendError)?;
        match edit {
            MaterializationEdit::Install { path, image } => {
                entries.insert(path.clone(), image.clone());
            }
            MaterializationEdit::Remove { path } => {
                entries.remove(path);
            }
            MaterializationEdit::Rename { from, to } => {
                if let Some(value) = entries.remove(from) {
                    entries.insert(to.clone(), value);
                } else if !entries.contains_key(to) {
                    return Err(TreeBackendError);
                }
            }
            _ => return Err(TreeBackendError),
        }
        Ok(())
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let [first, second] = decode_preimages(&preimage.image)?;
        let mut entries = self.0.lock().map_err(|_| TreeBackendError)?;
        let restore = |entries: &mut BTreeMap<String, Vec<u8>>, path: &str, value| match value {
            Some(value) => {
                entries.insert(path.to_owned(), value);
            }
            None => {
                entries.remove(path);
            }
        };
        match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                restore(&mut entries, path, first);
            }
            MaterializationEdit::Rename { from, to } => {
                restore(&mut entries, from, first);
                restore(&mut entries, to, second);
            }
            _ => return Err(TreeBackendError),
        }
        Ok(())
    }
}

fn encode_preimages(values: [Option<Vec<u8>>; 2]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for value in values {
        match value {
            Some(bytes) => {
                encoded.push(1);
                encoded.extend_from_slice(
                    &u32::try_from(bytes.len())
                        .expect("small test entry")
                        .to_le_bytes(),
                );
                encoded.extend_from_slice(&bytes);
            }
            None => encoded.push(0),
        }
    }
    encoded
}

fn decode_preimages(bytes: &[u8]) -> Result<[Option<Vec<u8>>; 2], TreeBackendError> {
    let mut cursor = 0;
    let mut values = [None, None];
    for value in &mut values {
        let tag = *bytes.get(cursor).ok_or(TreeBackendError)?;
        cursor += 1;
        if tag == 0 {
            continue;
        }
        if tag != 1 {
            return Err(TreeBackendError);
        }
        let length_bytes: [u8; 4] = bytes
            .get(cursor..cursor + 4)
            .ok_or(TreeBackendError)?
            .try_into()
            .map_err(|_| TreeBackendError)?;
        cursor += 4;
        let length =
            usize::try_from(u32::from_le_bytes(length_bytes)).map_err(|_| TreeBackendError)?;
        let end = cursor.checked_add(length).ok_or(TreeBackendError)?;
        *value = Some(bytes.get(cursor..end).ok_or(TreeBackendError)?.to_vec());
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(TreeBackendError);
    }
    Ok(values)
}

#[derive(Clone)]
struct MutationCrashBackend {
    inner: TreeBackend,
    fail_apply: Arc<AtomicUsize>,
    fail_restore: Arc<AtomicUsize>,
}

impl MutationCrashBackend {
    fn after_apply(inner: TreeBackend, ordinal: usize) -> Self {
        Self {
            inner,
            fail_apply: Arc::new(AtomicUsize::new(ordinal)),
            fail_restore: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn after_restore(inner: TreeBackend, ordinal: usize) -> Self {
        Self {
            inner,
            fail_apply: Arc::new(AtomicUsize::new(0)),
            fail_restore: Arc::new(AtomicUsize::new(ordinal)),
        }
    }

    fn trips(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some(value.saturating_sub(1))
            })
            .is_ok_and(|previous| previous == 1)
    }
}

impl MaterializationBackend for MutationCrashBackend {
    type Error = TreeBackendError;

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        self.inner.capture(edit).await
    }

    async fn apply(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
        self.inner.apply(edit).await?;
        if Self::trips(&self.fail_apply) {
            return Err(TreeBackendError);
        }
        Ok(())
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        self.inner.restore(edit, preimage).await?;
        if Self::trips(&self.fail_restore) {
            return Err(TreeBackendError);
        }
        Ok(())
    }
}

fn plan(case: u8) -> MaterializationPlan {
    MaterializationPlan {
        operation_id: OperationId::from_bytes([case; 16]),
        from: generation(1),
        to: generation(2),
        edits: vec![
            MaterializationEdit::Install {
                path: "a".to_owned(),
                image: b"new-a".to_vec(),
            },
            MaterializationEdit::Remove {
                path: "b".to_owned(),
            },
            MaterializationEdit::Rename {
                from: "from".to_owned(),
                to: "to".to_owned(),
            },
        ],
    }
}

#[tokio::test]
async fn restart_completes_from_every_forward_journal_boundary() {
    let expected = target_tree();
    // Prepared, Applying, one progress record per edit, Applied.
    let boundary_count = plan(0).edits.len() + 3;
    for boundary in 1..=boundary_count {
        let store = CrashJournalStore::default();
        store.fail_on_relative_write(boundary);
        let backend = TreeBackend::source();
        let operation = plan(u8::try_from(boundary).expect("small boundary"));
        let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
        assert!(matches!(
            materializer.apply(operation.clone()).await,
            Err(MaterializationError::Store(_))
        ));

        store.clear_failure();
        let restarted = JournaledMaterializer::new(store, backend.clone());
        let recovered = restarted
            .recover(operation.operation_id, MaterializationRecovery::Complete)
            .await
            .expect("restart recovery")
            .expect("durable journal");
        assert_eq!(recovered.phase, MaterializationPhase::Applied);
        assert_eq!(backend.snapshot(), expected);
        assert_eq!(
            restarted
                .apply(operation)
                .await
                .expect("duplicate replay")
                .phase,
            MaterializationPhase::Applied
        );
    }
}

#[tokio::test]
async fn restart_rolls_back_every_partial_forward_boundary() {
    let expected = source_tree();
    // Prepared, Applying, and one progress record per edit. The terminal Applied
    // boundary is covered by the reverse-boundary test below.
    let boundary_count = plan(0).edits.len() + 2;
    for boundary in 1..=boundary_count {
        let store = CrashJournalStore::default();
        store.fail_on_relative_write(boundary);
        let backend = TreeBackend::source();
        let operation = plan(u8::try_from(boundary + 0x10).expect("small boundary"));
        assert!(matches!(
            JournaledMaterializer::new(store.clone(), backend.clone())
                .apply(operation.clone())
                .await,
            Err(MaterializationError::Store(_))
        ));

        store.clear_failure();
        let recovered = JournaledMaterializer::new(store, backend.clone())
            .recover(operation.operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("partial rollback recovery")
            .expect("durable journal");
        assert_eq!(recovered.phase, MaterializationPhase::RolledBack);
        assert_eq!(backend.snapshot(), expected);
    }
}

#[tokio::test]
async fn restart_rolls_back_from_every_reverse_journal_boundary() {
    let expected = source_tree();
    // RollingBack, one reverse restore-progress record per edit, RolledBack.
    let boundary_count = plan(0).edits.len() + 2;
    for boundary in 1..=boundary_count {
        let store = CrashJournalStore::default();
        let backend = TreeBackend::source();
        let operation = plan(u8::try_from(boundary + 8).expect("small boundary"));
        JournaledMaterializer::new(store.clone(), backend.clone())
            .apply(operation.clone())
            .await
            .expect("initial apply");
        store.fail_on_relative_write(boundary);
        let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
        assert!(matches!(
            materializer
                .recover(operation.operation_id, MaterializationRecovery::RollBack)
                .await,
            Err(MaterializationError::Store(_))
        ));

        store.clear_failure();
        let restarted = JournaledMaterializer::new(store, backend.clone());
        let recovered = restarted
            .recover(operation.operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("restart rollback")
            .expect("durable journal");
        assert_eq!(recovered.phase, MaterializationPhase::RolledBack);
        assert_eq!(backend.snapshot(), expected);
        assert_eq!(
            restarted
                .recover(operation.operation_id, MaterializationRecovery::RollBack)
                .await
                .expect("duplicate rollback")
                .expect("terminal journal")
                .phase,
            MaterializationPhase::RolledBack
        );
    }
}

#[tokio::test]
async fn restart_repairs_progress_that_lags_host_mutation() {
    let target = target_tree();
    let source = source_tree();

    for ordinal in 1..=3 {
        let forward_store = CrashJournalStore::default();
        let forward_tree = TreeBackend::source();
        let forward_plan = plan(u8::try_from(0x40 + ordinal).expect("small ordinal"));
        assert!(matches!(
            JournaledMaterializer::new(
                forward_store.clone(),
                MutationCrashBackend::after_apply(forward_tree.clone(), ordinal),
            )
            .apply(forward_plan.clone())
            .await,
            Err(MaterializationError::Backend(_))
        ));
        let completed = JournaledMaterializer::new(forward_store, forward_tree.clone())
            .recover(forward_plan.operation_id, MaterializationRecovery::Complete)
            .await
            .expect("complete after unrecorded mutation")
            .expect("forward journal");
        assert_eq!(completed.phase, MaterializationPhase::Applied);
        assert_eq!(forward_tree.snapshot(), target);

        let rollback_store = CrashJournalStore::default();
        let rollback_tree = TreeBackend::source();
        let rollback_plan = plan(u8::try_from(0x50 + ordinal).expect("small ordinal"));
        JournaledMaterializer::new(rollback_store.clone(), rollback_tree.clone())
            .apply(rollback_plan.clone())
            .await
            .expect("prepare rollback fixture");
        assert!(matches!(
            JournaledMaterializer::new(
                rollback_store.clone(),
                MutationCrashBackend::after_restore(rollback_tree.clone(), ordinal),
            )
            .recover(
                rollback_plan.operation_id,
                MaterializationRecovery::RollBack,
            )
            .await,
            Err(MaterializationError::Backend(_))
        ));
        let rolled_back = JournaledMaterializer::new(rollback_store, rollback_tree.clone())
            .recover(
                rollback_plan.operation_id,
                MaterializationRecovery::RollBack,
            )
            .await
            .expect("rollback after unrecorded restoration")
            .expect("rollback journal");
        assert_eq!(rolled_back.phase, MaterializationPhase::RolledBack);
        assert_eq!(rollback_tree.snapshot(), source);
    }
}

#[tokio::test]
async fn immutable_materialization_plan_rejects_conflicting_replay() {
    let store = CrashJournalStore::default();
    let backend = TreeBackend::source();
    let materializer = JournaledMaterializer::new(store, backend);
    let original = plan(0x30);
    materializer
        .apply(original.clone())
        .await
        .expect("initial apply");
    let mut altered = original;
    altered.to = generation(9);
    assert!(matches!(
        materializer.apply(altered).await,
        Err(MaterializationError::PlanConflict)
    ));
}
