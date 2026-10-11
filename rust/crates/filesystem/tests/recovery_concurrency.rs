//! Deterministic recovery and concurrency conformance for durable filesystem protocols.
//! Crash injection wraps the production Memory journal store and keeps each
//! ordinary or original-ledger-funded ownership packet through the write seam.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_let_else,
    clippy::panic,
    reason = "test fixtures use explicit fatal assertions to keep exhaustive state-machine cases readable"
)]

use acyclic_fs::{
    Digest, GenerationId, JournaledMaterializer, MaterializationBackend, MaterializationEdit,
    MaterializationError, MaterializationIntent, MaterializationJournalStore,
    MaterializationPhase, MaterializationPlan, MaterializationPreimage,
    MaterializationRecovery, MemoryMaterializationJournalStore,
    MemoryMaterializationJournalStoreError,
    OperationId, OperationWindowCoordinator, OperationWindowError, OperationWindowFinish,
    OperationWindowPhase, OperationWindowSnapshot, OperationWindowStore, WorkspaceId,
};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::{
    CancellationToken, MaterializationActionCompletion, NativeMaterializationWork,
    NativeTreeMaterializationBackend, WorkBudget,
};
use acyclic_fs::materializer::{MaterializationObservation, OwnedMaterializationJournal, OwnedMaterializationPreimage};
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

// Keep the production packet store; the fixture controls only its write seam.
#[derive(Clone, Default)]
struct CrashJournalStore {
    inner: Arc<MemoryMaterializationJournalStore>,
    state: Arc<Mutex<CrashJournalState>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Default)]
struct CrashJournalState {
    writes: usize,
    fail_before_write: Option<usize>,
    fail_after_write: Option<usize>,
}

impl CrashJournalStore {
    fn fail_before_relative_write(&self, offset: usize) {
        let mut state = self.state.lock().expect("journal fault state");
        state.fail_before_write = Some(state.writes + offset);
    }

    fn fail_on_relative_write(&self, offset: usize) {
        let mut state = self.state.lock().expect("journal fault state");
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
        let mut state = self.state.lock().expect("journal fault state");
        state.fail_before_write = None;
        state.fail_after_write = None;
    }

    fn fails_before_next_write(&self) -> bool {
        let state = self.state.lock().expect("journal fault state");
        state.fail_before_write == Some(state.writes + 1)
    }

    fn did_write(&self) -> bool {
        let mut state = self.state.lock().expect("journal fault state");
        state.writes += 1;
        state.fail_after_write == Some(state.writes)
    }
}

#[derive(Debug, Error)]
enum CrashStoreError {
    #[error(transparent)]
    Memory(#[from] MemoryMaterializationJournalStoreError),
    #[error("simulated process crash at durable journal write")]
    InjectedCrash(OwnedMaterializationJournal),
}

#[cfg(not(target_arch = "wasm32"))]
struct CrashActionCompletion<'a> {
    actual: <MemoryMaterializationJournalStore as MaterializationJournalStore>::ActionCompletion<'a>,
    before_write: Option<OwnedMaterializationJournal>,
    store: &'a CrashJournalStore,
    // Freeze the relative write ordinal from admission through the real action.
    _write_gate: tokio::sync::MutexGuard<'a, ()>,
}

#[cfg(not(target_arch = "wasm32"))]
impl MaterializationActionCompletion for CrashActionCompletion<'_> {
    type Error = CrashStoreError;

    async fn complete(self) -> Result<OwnedMaterializationJournal, Self::Error> {
        let Self { actual, before_write, store, _write_gate } = self;
        if let Some(journal) = before_write {
            // No CAS took place. Retire the genuine accepted frame, retaining
            // its real-store-loaded packet and original guard in the failure.
            drop(actual);
            return Err(CrashStoreError::InjectedCrash(journal));
        }
        let journal = actual.complete().await?;
        if store.did_write() {
            // The actual CAS happened; only its acknowledgement is lost.
            return Err(CrashStoreError::InjectedCrash(journal));
        }
        Ok(journal)
    }
}

impl MaterializationJournalStore for CrashJournalStore {
    type Error = CrashStoreError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a> = CrashActionCompletion<'a> where Self: 'a;

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action<'a>(
        &'a self,
        journal: OwnedMaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        let write_gate = self.write_gate.lock().await;
        // The fault is armed before admission. Its retained error packet is
        // a real Memory load funded by the original native ledger, never a
        // plain journal clone or an allocation after the host action.
        let before_write = if self.fails_before_next_write() {
            self.inner.load(journal.plan.operation_id).await?
        } else {
            None
        };
        let actual = self.inner.prepare_action(journal).await?;
        Ok(CrashActionCompletion {
            actual, before_write, store: self, _write_gate: write_gate,
        })
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OwnedMaterializationJournal>, Self::Error> {
        self.inner.load(operation_id).await.map_err(CrashStoreError::from)
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: OwnedMaterializationJournal,
    ) -> Result<(bool, OwnedMaterializationJournal), Self::Error> {
        let _write_gate = self.write_gate.lock().await;
        if self.fails_before_next_write() {
            // Rejected CAS attempts neither consume a write ordinal nor crash.
            // Query only at an armed pre-write seam, through the actual store.
            let current = self.inner.load(operation_id).await?;
            if current.as_ref().map_or(0, |journal| journal.revision) == expected_revision {
                return Err(CrashStoreError::InjectedCrash(replacement));
            }
        }
        let (stored, replacement) = self.inner
            .compare_and_swap(operation_id, expected_revision, replacement).await?;
        if stored && self.did_write() {
            return Err(CrashStoreError::InjectedCrash(replacement));
        }
        Ok((stored, replacement))
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
    fn append_preimage(&self, journal: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error> {
        if preimage.custody.is_some() { return Err(TreeBackendError); }
        journal.preimages.push(preimage.preimage);
        Ok(())
    }

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<OwnedMaterializationPreimage, Self::Error> {
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
        Ok(OwnedMaterializationPreimage::ordinary(MaterializationPreimage { image }))
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
    fn append_preimage(&self, journal: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error> {
        self.inner.append_preimage(journal, preimage)
    }

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<OwnedMaterializationPreimage, Self::Error> {
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
            .push((edit.clone(), preimage.preimage.clone()));
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
    packet: &OwnedMaterializationJournal,
    original: &MaterializationPlan,
    durable_writes: usize,
) {
    #[cfg(not(target_arch = "wasm32"))]
    assert!(packet.custody.is_none(), "ordinary tree fixture alone permits receipt copies");
    let journal = &packet.journal;
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
                .zip(retained.journal.preimages.iter().cloned())
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
                    restarted.load(original.operation_id).await.expect("query after conflict")
                        .as_ref().map(|packet| &packet.journal),
                    Some(&retained.journal)
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
                    #[cfg(not(target_arch = "wasm32"))]
                    assert!(recovered.custody.is_none(), "ordinary receipt copy only");
                    let expected_receipts: Vec<_> = original.edits
                        .iter()
                        .cloned()
                        .zip(recovered.journal.preimages.iter().cloned())
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
        #[cfg(not(target_arch = "wasm32"))]
        assert!(retained.custody.is_none(), "ordinary receipt copy only");
        let expected_receipts: Vec<_> = forward_plan.edits
            .iter()
            .cloned()
            .zip(retained.journal.preimages.iter().cloned())
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
        #[cfg(not(target_arch = "wasm32"))]
        assert!(retained.custody.is_none(), "ordinary receipt copy only");
        let expected_restores: Vec<_> = rollback_plan.edits
            .iter()
            .cloned()
            .zip(retained.journal.preimages.iter().cloned())
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

#[tokio::test]
async fn rejected_cas_does_not_consume_or_trip_a_relative_crash_boundary() {
    for after_write in [false, true] {
        let store = CrashJournalStore::default();
        let engine = JournaledMaterializer::new(store.clone(), TreeBackend::source());
        let journal = engine.prepare(plan(0x70)).await.expect("ordinary original intent");
        let operation_id = journal.plan.operation_id;
        assert_eq!(store.state.lock().expect("fault state").writes, 1);
        #[cfg(not(target_arch = "wasm32"))]
        assert!(journal.custody.is_none(), "ordinary fixture has no native authority");
        store.fail_at_relative_write(1, after_write);

        let mut candidate = store.load(operation_id).await.expect("real load").expect("intent");
        candidate.revision += 1;
        let payload = candidate.plan.edits.as_ptr();
        let (stored, candidate) = store.compare_and_swap(operation_id, 0, candidate)
            .await.expect("stale CAS is not a write or an injected crash");
        assert!(!stored);
        assert_eq!(candidate.plan.edits.as_ptr(), payload);
        assert_eq!(store.state.lock().expect("fault state").writes, 1);
        let error = store.compare_and_swap(operation_id, 1, candidate)
            .await.expect_err("the next accepted write reaches the armed boundary");
        let CrashStoreError::InjectedCrash(candidate) = error else {
            panic!("expected the static injected crash with its owned packet");
        };
        assert_eq!(candidate.plan.edits.as_ptr(), payload);
        assert_eq!(
            store.state.lock().expect("fault state").writes,
            1 + usize::from(after_write),
        );
        drop(engine);
        store.clear_failure();
        let reopened = JournaledMaterializer::new(store.clone(), TreeBackend::source());
        let retained = reopened.load(operation_id).await.expect("same real store reopen")
            .expect("original intent");
        assert_eq!(retained.revision, 1 + u64::from(after_write));
        assert_eq!(&retained.plan, &candidate.plan);
        #[cfg(not(target_arch = "wasm32"))]
        assert!(retained.custody.is_none());
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn finite_native_budget() -> WorkBudget {
    let mut value = serde_json::to_value(WorkBudget::UNBOUNDED).expect("budget encoding");
    for field in value.as_object_mut().expect("budget object").values_mut() {
        *field = serde_json::Value::from(100_000_000_u64);
    }
    serde_json::from_value(value).expect("finite native budget")
}

#[cfg(not(target_arch = "wasm32"))]
fn native_backend(
    directory: &std::path::Path,
    work: &NativeMaterializationWork,
) -> NativeTreeMaterializationBackend {
    let root = directory.join("root");
    let operation = directory.join("operation");
    std::fs::create_dir(&root).expect("fixture source root");
    std::fs::create_dir_all(operation.join("target")).expect("fixture target");
    std::fs::write(root.join("file"), b"before").expect("fixture source bytes");
    std::fs::write(operation.join("target/file"), b"target").expect("fixture target bytes");
    let (root, custody) = work.copy_path(&root).expect("funded backend root path");
    let mut custody = custody;
    let root = root;
    let (operation, operation_custody) = work.copy_path(&operation)
        .expect("funded backend operation path");
    let mut operation_custody = operation_custody;
    let operation = operation;
    custody.absorb(&mut operation_custody).expect("same original allocation context");
    NativeTreeMaterializationBackend::new(root, operation, Some(custody), work.clone())
        .expect("real original-resource backend")
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_prepared_packet(
    store: &CrashJournalStore,
    backend: &NativeTreeMaterializationBackend,
    work: &NativeMaterializationWork,
    case: u8,
) -> OwnedMaterializationJournal {
    let (path, path_custody) = work.copy_bytes(b"file").expect("real paid path copy");
    let mut path_custody = path_custody;
    let path = path;
    let mut custody = work.claim(std::mem::size_of::<MaterializationEdit>() as u64)
        .expect("admit plan vector before allocation");
    let path = path; // Retire path data before either possible owning guard.
    let mut edits = Vec::with_capacity(1);
    custody.reconcile_capacity(
        (edits.capacity() * std::mem::size_of::<MaterializationEdit>()) as u64,
    ).expect("retain actual vector capacity before any late refusal");
    custody.absorb(&mut path_custody).expect("same original paid plan");
    edits.push(MaterializationEdit::Remove {
        path: String::from_utf8(path).expect("static valid UTF-8 without copying"),
    });
    let plan = MaterializationPlan {
        operation_id: OperationId::from_bytes([case; 16]),
        intent: MaterializationIntent::GenerationTransition {
            from: generation(1), to: generation(2),
        },
        edits,
    };
    JournaledMaterializer::new(store.clone(), backend.clone())
        .prepare_owned(plan, custody).await.expect("production-owned original journal")
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn accepted_completion_crashes_keep_native_custody_and_truthful_durable_prefixes() {
    acyclic_native_runtime::initialize_native_io_services().expect("actual native I/O services");
    for after_write in [false, true] {
        for cancel_after_action in [false, true] {
            let directory = tempfile::tempdir().expect("physical action fixture");
            let cancellation = CancellationToken::new();
            let budget = finite_native_budget();
            let original = NativeMaterializationWork::new(budget, &cancellation)
                .expect("one original finite work ledger");
            let work = original.native_volume_work(budget).expect("original native resource");
            let backend = native_backend(directory.path(), &work);
            let store = CrashJournalStore::default();
            let journal = native_prepared_packet(&store, &backend, &work, 0x71).await;
            let operation_id = journal.plan.operation_id;
            let last_durable_work = journal.work;
            let payload = journal.plan.edits.as_ptr();
            let packet_bytes = journal.custody.as_ref().expect("production Some custody").bytes();
            assert!(journal.custody.as_ref().expect("original custody").work()
                .shares_allocation_context(&work));
            let file = directory.path().join("root/file");
            let mut output = std::fs::OpenOptions::new().write(true).open(&file)
                .expect("fixture output before admission");
            store.fail_at_relative_write(1, after_write);
            let before_admission = original.receipt();
            let completion = store.prepare_action(journal).await
                .expect("genuine Memory completion accepted before the physical action");
            assert!(original.receipt().bytes_copied > before_admission.bytes_copied);
            assert_eq!(std::fs::read(&file).expect("unchanged before acceptance"), b"before");
            work.write_owned_bytes(&mut output, b"after!").expect("actual original-resource action");
            if cancel_after_action {
                cancellation.cancel();
            }
            let live_prefix = original.receipt();
            let durable_prefix = work.journal_work().expect("pure original durable prefix");
            let error = completion.complete().await.expect_err("real before/after write seam");
            assert_eq!(original.receipt(), live_prefix, "no allocation, copy, or admission after action");
            let CrashStoreError::InjectedCrash(packet) = error else {
                panic!("expected the injected crash to retain its real owned packet");
            };
            let custody = packet.custody.as_ref().expect("native fault never becomes ordinary None");
            assert!(custody.work().shares_allocation_context(&work));
            let retained_packet_bytes = custody.bytes();
            assert_eq!(retained_packet_bytes,
                NativeMaterializationWork::journal_control_bytes(&packet.journal)
                    .expect("actual packet capacities"));
            assert_eq!(packet.revision, 1 + u64::from(after_write));
            assert_eq!(packet.work, if after_write { durable_prefix } else { last_durable_work });
            if after_write {
                assert_eq!(retained_packet_bytes, packet_bytes);
                assert_eq!(packet.plan.edits.as_ptr(), payload, "actual completion returns its input");
            }
            assert_eq!(store.state.lock().expect("fault state").writes, 1 + usize::from(after_write));
            assert!(live_prefix.output_bytes > last_durable_work.output_bytes);
            assert_eq!(std::fs::read(&file).expect("independent physical observation"), b"after!");
            live_prefix.verify(budget).expect("full observed prefix fits the original finite budget");
            store.clear_failure();
            if cancel_after_action {
                assert!(matches!(
                    store.load(operation_id).await,
                    Err(CrashStoreError::Memory(MemoryMaterializationJournalStoreError::NativeWork(
                        acyclic_fs::NativeTreeMaterializationError::Cancelled,
                    )))
                ), "cold reads never replace the cancelled original meter with an unbounded one");
            } else {
                // Reopen the consumer, not a replacement journal engine or ledger.
                let reopened = JournaledMaterializer::new(store.clone(), backend.clone());
                let retained = reopened.load_owned(operation_id).await.expect("real same-store reopen")
                    .expect("original durable journal");
                assert_eq!(&retained.journal, &packet.journal);
                assert!(retained.custody.as_ref().expect("funded reopened packet").work()
                    .shares_allocation_context(&work));
            }
            let live_bytes = original.live_allocation_bytes();
            drop(packet);
            assert_eq!(original.live_allocation_bytes(), live_bytes - retained_packet_bytes);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn accepted_completion_conflict_keeps_the_whole_real_memory_error_and_packet() {
    acyclic_native_runtime::initialize_native_io_services().expect("actual native I/O services");
    let directory = tempfile::tempdir().expect("real native conflict fixture");
    let budget = finite_native_budget();
    let original = NativeMaterializationWork::new(budget, &CancellationToken::new())
        .expect("original finite meter");
    let work = original.native_volume_work(budget).expect("original native resource");
    let backend = native_backend(directory.path(), &work);
    let store = CrashJournalStore::default();
    let journal = native_prepared_packet(&store, &backend, &work, 0x72).await;
    let operation_id = journal.plan.operation_id;
    let payload = journal.plan.edits.as_ptr();
    let completion = store.prepare_action(journal).await.expect("actual accepted Memory completion");
    // An independent actor reaches the same real Memory store, not fixture metadata.
    let mut competitor = store.inner.load(operation_id).await.expect("actual competing load")
        .expect("original record");
    competitor.revision += 1;
    let (stored, competitor) = store.inner.compare_and_swap(operation_id, 1, competitor).await
        .expect("genuine competing CAS");
    assert!(stored);
    let before = original.receipt();
    let error = completion.complete().await.expect_err("actual accepted CAS sees the winner");
    assert_eq!(original.receipt(), before, "conflict performs no late copy or allocation");
    let CrashStoreError::Memory(MemoryMaterializationJournalStoreError::ActionConflict(packet))
        = error else {
        panic!("expected the intact real Memory conflict error");
    };
    assert_eq!(packet.plan.edits.as_ptr(), payload);
    assert!(packet.custody.as_ref().expect("intact original error custody").work()
        .shares_allocation_context(&work));
    let retained = store.load(operation_id).await.expect("real winning record query").expect("winner");
    assert_eq!(&retained.journal, &competitor.journal);
    assert_eq!(store.state.lock().expect("fault state").writes, 1,
        "failed completion consumes no wrapped write ordinal");
    original.receipt().verify(budget).expect("actual whole conflict prefix");
}
