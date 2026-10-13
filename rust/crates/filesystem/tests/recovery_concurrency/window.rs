use super::*;

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
