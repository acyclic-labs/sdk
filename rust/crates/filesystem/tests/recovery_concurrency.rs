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
    MaterializationError, MaterializationIntent, MaterializationJournal, MaterializationJournalStore,
    MaterializationObservation, MaterializationPhase, MaterializationPlan, MaterializationPreimage,
    MaterializationRecovery,
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
    fail_before_write: Option<usize>,
    fail_after_write: Option<usize>,
}

impl CrashJournalStore {
    fn fail_before_relative_write(&self, offset: usize) {
        let mut state = self.0.lock().expect("journal state");
        state.fail_before_write = Some(state.writes + offset);
    }

    fn fail_on_relative_write(&self, offset: usize) {
        let mut state = self.0.lock().expect("journal state");
        state.fail_after_write = Some(state.writes + offset);
    }

    fn fail_at_relative_write(&self, offset: usize, after_write: bool) {
        if after_write {
            self.fail_on_relative_write(offset);
        } else {
            self.fail_before_relative_write(offset);
        }
    }

    fn clear_failure(&self) {
        let mut state = self.0.lock().expect("journal state");
        state.fail_before_write = None;
        state.fail_after_write = None;
    }
}

#[derive(Debug, Error)]
#[error("simulated process crash at durable journal write")]
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
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        let mut state = self.0.lock().map_err(|_| CrashStoreError)?;
        if state
            .journals
            .get(&operation_id)
            .map_or(0, |journal| journal.revision)
            != expected_revision
        {
            return Ok((false, replacement));
        }
        if state.fail_before_write == Some(state.writes + 1) {
            return Err(CrashStoreError);
        }
        state.journals.insert(operation_id, replacement.clone());
        state.writes += 1;
        if state.fail_after_write == Some(state.writes) {
            return Err(CrashStoreError);
        }
        Ok((true, replacement))
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

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        _preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
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

// Receipts record actual raw calls and returned preimages, not metered work.
#[derive(Clone, Default)]
struct MutationReceipts {
    capture_attempts: Vec<MaterializationEdit>,
    captures: Vec<(MaterializationEdit, MaterializationPreimage)>,
    applications: Vec<(MaterializationEdit, MaterializationPreimage)>,
    restorations: Vec<(MaterializationEdit, MaterializationPreimage)>,
}

#[derive(Clone)]
struct MutationCrashBackend {
    inner: TreeBackend,
    fail_capture: Arc<AtomicUsize>,
    receipts: Arc<Mutex<MutationReceipts>>,
    fail_apply: Arc<AtomicUsize>,
    fail_restore: Arc<AtomicUsize>,
}

impl MutationCrashBackend {
    fn new(inner: TreeBackend) -> Self {
        Self {
            inner,
            fail_capture: Arc::new(AtomicUsize::new(0)),
            receipts: Arc::new(Mutex::new(MutationReceipts::default())),
            fail_apply: Arc::new(AtomicUsize::new(0)),
            fail_restore: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn before_capture(inner: TreeBackend, ordinal: usize) -> Self {
        let backend = Self::new(inner);
        backend.fail_capture.store(ordinal, Ordering::SeqCst);
        backend
    }

    fn after_apply(inner: TreeBackend, ordinal: usize) -> Self {
        let backend = Self::new(inner);
        backend.fail_apply.store(ordinal, Ordering::SeqCst);
        backend
    }

    fn after_restore(inner: TreeBackend, ordinal: usize) -> Self {
        let backend = Self::new(inner);
        backend.fail_restore.store(ordinal, Ordering::SeqCst);
        backend
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
        self.receipts
            .lock()
            .map_err(|_| TreeBackendError)?
            .capture_attempts
            .push(edit.clone());
        if Self::trips(&self.fail_capture) {
            return Err(TreeBackendError);
        }
        let preimage = self.inner.capture(edit).await?;
        self.receipts
            .lock()
            .map_err(|_| TreeBackendError)?
            .captures
            .push((edit.clone(), preimage.clone()));
        Ok(preimage)
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        self.inner.apply(edit, preimage).await?;
        self.receipts
            .lock()
            .map_err(|_| TreeBackendError)?
            .applications
            .push((edit.clone(), preimage.clone()));
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
        self.receipts
            .lock()
            .map_err(|_| TreeBackendError)?
            .restorations
            .push((edit.clone(), preimage.clone()));
        if Self::trips(&self.fail_restore) {
            return Err(TreeBackendError);
        }
        Ok(())
    }
}

fn plan(case: u8) -> MaterializationPlan {
    MaterializationPlan {
        operation_id: OperationId::from_bytes([case; 16]),
        intent: MaterializationIntent::GenerationTransition {
            from: generation(1),
            to: generation(2),
        },
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

fn assert_original_forward_journal(
    journal: &MaterializationJournal,
    original: &MaterializationPlan,
    durable_writes: usize,
) {
    let edits = original.edits.len();
    assert_eq!(&journal.plan, original);
    assert_eq!(
        journal.revision,
        u64::try_from(durable_writes).expect("small journal revision")
    );
    assert_eq!(journal.preimages.len(), durable_writes.saturating_sub(1).min(edits));
    assert_eq!(
        journal.applied,
        u32::try_from(durable_writes.saturating_sub(edits + 3).min(edits))
            .expect("small edit count")
    );
    assert_eq!(journal.restored, 0);
    let phase = if durable_writes <= edits + 1 {
        MaterializationPhase::Preparing
    } else if durable_writes == edits + 2 {
        MaterializationPhase::Prepared
    } else if durable_writes < 2 * edits + 4 {
        MaterializationPhase::Applying
    } else {
        MaterializationPhase::Applied
    };
    assert_eq!(journal.phase, phase);
}

#[tokio::test]
async fn restart_completes_from_every_forward_journal_boundary() {
    let expected = target_tree();
    // Preparing, one preimage record per edit, Prepared, Applying, one
    // progress record per edit, Applied: the production persist boundaries.
    let boundary_count = 2 * plan(0).edits.len() + 4;
    for after_write in [false, true] {
        for boundary in 1..=boundary_count {
            let store = CrashJournalStore::default();
            store.fail_at_relative_write(boundary, after_write);
            let backend = TreeBackend::source();
            let operation = plan(u8::try_from(boundary).expect("small boundary"));
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer.apply(operation.clone()).await,
                Err(MaterializationError::Store(_))
            ));
            drop(materializer);

            store.clear_failure();
            let restarted = JournaledMaterializer::new(store, backend.clone());
            let retained = restarted
                .load(operation.operation_id)
                .await
                .expect("query original journal after reopen");
            let durable_writes = boundary - usize::from(!after_write);
            if let Some(journal) = &retained {
                assert_original_forward_journal(journal, &operation, durable_writes);
                if journal.phase == MaterializationPhase::Preparing {
                    assert_eq!(backend.snapshot(), source_tree());
                }
            } else {
                assert_eq!(durable_writes, 0);
                assert_eq!(backend.snapshot(), source_tree());
            }
            let recovered = restarted
                .recover(operation.operation_id, MaterializationRecovery::Complete)
                .await
                .expect("restart recovery");
            let recovered = if retained.is_some() {
                recovered.expect("durable journal")
            } else {
                assert!(recovered.is_none());
                restarted.apply(operation.clone()).await.expect("retry undurable intent")
            };
            assert_eq!(recovered.plan, operation);
            assert_eq!(recovered.phase, MaterializationPhase::Applied);
            assert_eq!(backend.snapshot(), expected);
            assert_eq!(
                restarted.apply(operation).await.expect("duplicate replay").phase,
                MaterializationPhase::Applied
            );
        }
    }
}

#[tokio::test]
async fn restart_rolls_back_every_partial_forward_boundary() {
    let expected = source_tree();
    // All forward boundaries except terminal Applied, covered below.
    let boundary_count = 2 * plan(0).edits.len() + 3;
    for after_write in [false, true] {
        for boundary in 1..=boundary_count {
            let store = CrashJournalStore::default();
            store.fail_at_relative_write(boundary, after_write);
            let backend = TreeBackend::source();
            let operation = plan(u8::try_from(boundary + 0x10).expect("small boundary"));
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer.apply(operation.clone()).await,
                Err(MaterializationError::Store(_))
            ));
            drop(materializer);

            store.clear_failure();
            let restarted = JournaledMaterializer::new(store, backend.clone());
            let retained = restarted
                .load(operation.operation_id)
                .await
                .expect("query partial original journal");
            let durable_writes = boundary - usize::from(!after_write);
            if let Some(journal) = &retained {
                assert_original_forward_journal(journal, &operation, durable_writes);
                if journal.phase == MaterializationPhase::Preparing {
                    assert_eq!(backend.snapshot(), expected);
                }
            } else {
                assert_eq!(durable_writes, 0);
            }
            let recovered = restarted
                .recover(operation.operation_id, MaterializationRecovery::RollBack)
                .await
                .expect("partial rollback recovery");
            if let Some(journal) = recovered {
                assert!(retained.is_some());
                assert_eq!(journal.plan, operation);
                assert_eq!(journal.phase, MaterializationPhase::RolledBack);
            } else {
                assert!(retained.is_none());
            }
            assert_eq!(backend.snapshot(), expected);
        }
    }
}

#[tokio::test]
async fn restart_rolls_back_from_every_reverse_journal_boundary() {
    let expected = source_tree();
    // RollingBack, one reverse restore-progress record per edit, RolledBack.
    let boundary_count = plan(0).edits.len() + 2;
    for after_write in [false, true] {
        for boundary in 1..=boundary_count {
            let store = CrashJournalStore::default();
            let backend = TreeBackend::source();
            let operation = plan(u8::try_from(boundary + 8).expect("small boundary"));
            JournaledMaterializer::new(store.clone(), backend.clone())
                .apply(operation.clone())
                .await
                .expect("initial apply");
            assert_eq!(backend.snapshot(), target_tree());
            store.fail_at_relative_write(boundary, after_write);
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer
                    .recover(operation.operation_id, MaterializationRecovery::RollBack)
                    .await,
                Err(MaterializationError::Store(_))
            ));
            drop(materializer);

            store.clear_failure();
            let restarted = JournaledMaterializer::new(store, backend.clone());
            let retained = restarted
                .load(operation.operation_id)
                .await
                .expect("query original rollback journal")
                .expect("durable journal");
            assert_eq!(retained.plan, operation);
            let recovered = restarted
                .recover(operation.operation_id, MaterializationRecovery::RollBack)
                .await
                .expect("restart rollback")
                .expect("durable journal");
            assert_eq!(recovered.plan, operation);
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
}

#[tokio::test]
async fn capture_failures_reopen_original_partial_intent_without_physical_edits() {
    for recovery in [MaterializationRecovery::Complete, MaterializationRecovery::RollBack] {
        // Fail before the first capture and at each later path in this install,
        // remove, rename plan. Only successfully persisted captures may survive.
        for ordinal in 1..=plan(0).edits.len() {
            let store = CrashJournalStore::default();
            let tree = TreeBackend::source();
            let backend = MutationCrashBackend::before_capture(tree.clone(), ordinal);
            let original = plan(u8::try_from(0x60 + ordinal).expect("small capture ordinal"));
            let materializer = JournaledMaterializer::new(store.clone(), backend.clone());
            assert!(matches!(
                materializer.apply(original.clone()).await,
                Err(MaterializationError::Backend(_))
            ));
            drop(materializer);

            let receipts = backend.receipts.lock().expect("actual backend receipts").clone();
            assert_eq!(receipts.capture_attempts, original.edits[..ordinal]);
            assert_eq!(receipts.captures.len(), ordinal - 1);
            assert!(receipts.applications.is_empty());
            assert!(receipts.restorations.is_empty());
            assert_eq!(tree.snapshot(), source_tree());

            let restarted = JournaledMaterializer::new(store.clone(), backend.clone());
            let retained = restarted
                .load(original.operation_id)
                .await
                .expect("query same store after capture failure")
                .expect("original intent survives before first capture");
            assert_original_forward_journal(&retained, &original, ordinal);
            assert_eq!(retained.phase, MaterializationPhase::Preparing);
            let captured: Vec<_> = original.edits[..ordinal - 1]
                .iter()
                .cloned()
                .zip(retained.preimages.iter().cloned())
                .collect();
            assert_eq!(receipts.captures, captured);
            let inspected = restarted
                .inspect(original.operation_id)
                .await
                .expect("inspect only retained physical endpoints")
                .expect("same original journal");
            assert_eq!(inspected.journal, retained);
            assert_eq!(inspected.root, acyclic_fs::MaterializationRootObservation::Unverified);
            assert_eq!(
                inspected.preparation,
                acyclic_fs::MaterializationPreparationObservation::Unverified
            );
            // The raw fixture cannot independently witness divergence. It must
            // not synthesize either observations or preimages for missing edits.
            assert_eq!(
                inspected.observations,
                vec![MaterializationObservation::Unverified; ordinal - 1]
            );

            let mut changed_from = original.clone();
            changed_from.intent = MaterializationIntent::GenerationTransition {
                from: generation(8),
                to: generation(2),
            };
            let mut changed_to = original.clone();
            changed_to.intent = MaterializationIntent::GenerationTransition {
                from: generation(1),
                to: generation(9),
            };
            let mut changed_image = original.clone();
            changed_image.edits[0] = MaterializationEdit::Install {
                path: "a".to_owned(),
                image: b"conflicting-a".to_vec(),
            };
            let mut changed_order = original.clone();
            changed_order.edits.reverse();
            for changed in [changed_from, changed_to, changed_image, changed_order] {
                assert!(matches!(
                    restarted.apply(changed).await,
                    Err(MaterializationError::PlanConflict)
                ));
                assert_eq!(
                    restarted.load(original.operation_id).await.expect("query after conflict"),
                    Some(retained.clone())
                );
                assert_eq!(tree.snapshot(), source_tree());
            }
            let after_queries = backend.receipts.lock().expect("query receipts").clone();
            assert_eq!(after_queries.capture_attempts, receipts.capture_attempts);
            assert_eq!(after_queries.captures, receipts.captures);
            assert!(after_queries.applications.is_empty());
            assert!(after_queries.restorations.is_empty());

            // Recover the original operation, never allocate a substitute plan.
            let recovered = restarted
                .recover(original.operation_id, recovery)
                .await
                .expect("recover partially captured original plan")
                .expect("original journal");
            assert_eq!(recovered.plan, original);
            let final_receipts = backend.receipts.lock().expect("recovery receipts").clone();
            match recovery {
                MaterializationRecovery::Complete => {
                    assert_eq!(recovered.phase, MaterializationPhase::Applied);
                    assert_eq!(tree.snapshot(), target_tree());
                    assert_eq!(recovered.preimages[..ordinal - 1], retained.preimages);
                    let expected_receipts: Vec<_> = original.edits
                        .iter()
                        .cloned()
                        .zip(recovered.preimages.iter().cloned())
                        .collect();
                    assert_eq!(final_receipts.captures, expected_receipts);
                    assert_eq!(final_receipts.applications, expected_receipts);
                    assert!(final_receipts.restorations.is_empty());
                    let mut expected_attempts = original.edits[..ordinal].to_vec();
                    expected_attempts.extend_from_slice(&original.edits[ordinal - 1..]);
                    assert_eq!(final_receipts.capture_attempts, expected_attempts);
                }
                MaterializationRecovery::RollBack => {
                    assert_eq!(recovered.phase, MaterializationPhase::RolledBack);
                    assert_eq!(recovered.revision, retained.revision + 1);
                    assert_eq!(recovered.preimages, retained.preimages);
                    assert_eq!(recovered.applied, 0);
                    assert_eq!(recovered.restored, 0);
                    assert_eq!(tree.snapshot(), source_tree());
                    assert_eq!(final_receipts.capture_attempts, receipts.capture_attempts);
                    assert_eq!(final_receipts.captures, receipts.captures);
                    assert!(final_receipts.applications.is_empty());
                    assert!(final_receipts.restorations.is_empty());
                    assert!(matches!(
                        restarted.apply(original.clone()).await,
                        Err(MaterializationError::PlanConflict)
                    ));
                }
            }
            assert_eq!(
                restarted.load(original.operation_id).await.expect("query recovered original"),
                Some(recovered)
            );
        }
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
        let forward_backend = MutationCrashBackend::after_apply(forward_tree.clone(), ordinal);
        assert!(matches!(
            JournaledMaterializer::new(
                forward_store.clone(),
                forward_backend.clone(),
            )
            .apply(forward_plan.clone())
            .await,
            Err(MaterializationError::Backend(_))
        ));
        let restarted = JournaledMaterializer::new(forward_store, forward_tree.clone());
        let retained = restarted
            .load(forward_plan.operation_id)
            .await
            .expect("query original operation after unrecorded apply")
            .expect("forward journal");
        assert_eq!(retained.plan, forward_plan);
        assert_eq!(retained.phase, MaterializationPhase::Applying);
        assert_eq!(retained.applied, u32::try_from(ordinal - 1).expect("small ordinal"));
        let expected_receipts: Vec<_> = forward_plan.edits
            .iter()
            .cloned()
            .zip(retained.preimages.iter().cloned())
            .collect();
        let receipts = forward_backend.receipts.lock().expect("forward receipts").clone();
        assert_eq!(receipts.capture_attempts, forward_plan.edits);
        assert_eq!(receipts.captures, expected_receipts);
        assert_eq!(receipts.applications, expected_receipts[..ordinal]);
        assert!(receipts.restorations.is_empty());
        let completed = restarted
            .recover(forward_plan.operation_id, MaterializationRecovery::Complete)
            .await
            .expect("complete after unrecorded mutation")
            .expect("forward journal");
        assert_eq!(completed.phase, MaterializationPhase::Applied);
        assert_eq!(completed.plan, forward_plan);
        assert_eq!(completed.preimages, retained.preimages);
        assert_eq!(forward_tree.snapshot(), target);

        let rollback_store = CrashJournalStore::default();
        let rollback_tree = TreeBackend::source();
        let rollback_plan = plan(u8::try_from(0x50 + ordinal).expect("small ordinal"));
        JournaledMaterializer::new(rollback_store.clone(), rollback_tree.clone())
            .apply(rollback_plan.clone())
            .await
            .expect("prepare rollback fixture");
        let rollback_backend = MutationCrashBackend::after_restore(rollback_tree.clone(), ordinal);
        assert!(matches!(
            JournaledMaterializer::new(
                rollback_store.clone(),
                rollback_backend.clone(),
            )
            .recover(
                rollback_plan.operation_id,
                MaterializationRecovery::RollBack,
            )
            .await,
            Err(MaterializationError::Backend(_))
        ));
        let restarted = JournaledMaterializer::new(rollback_store, rollback_tree.clone());
        let retained = restarted
            .load(rollback_plan.operation_id)
            .await
            .expect("query original operation after unrecorded restore")
            .expect("rollback journal");
        assert_eq!(retained.plan, rollback_plan);
        assert_eq!(retained.phase, MaterializationPhase::RollingBack);
        assert_eq!(retained.restored, u32::try_from(ordinal - 1).expect("small ordinal"));
        let expected_restores: Vec<_> = rollback_plan.edits
            .iter()
            .cloned()
            .zip(retained.preimages.iter().cloned())
            .rev()
            .take(ordinal)
            .collect();
        let receipts = rollback_backend.receipts.lock().expect("rollback receipts").clone();
        assert_eq!(receipts.restorations, expected_restores);
        assert!(receipts.capture_attempts.is_empty());
        assert!(receipts.captures.is_empty());
        assert!(receipts.applications.is_empty());
        let rolled_back = restarted
            .recover(
                rollback_plan.operation_id,
                MaterializationRecovery::RollBack,
            )
            .await
            .expect("rollback after unrecorded restoration")
            .expect("rollback journal");
        assert_eq!(rolled_back.phase, MaterializationPhase::RolledBack);
        assert_eq!(rolled_back.plan, rollback_plan);
        assert_eq!(rolled_back.preimages, retained.preimages);
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
    altered.intent = MaterializationIntent::GenerationTransition {
        from: generation(1),
        to: generation(9),
    };
    assert!(matches!(
        materializer.apply(altered).await,
        Err(MaterializationError::PlanConflict)
    ));
}
