/// Serialization and publication boundary for a canonical checkout.
pub struct SharedCheckout<A, O> {
    state: tokio::sync::RwLock<SharedCheckoutState<A, O>>,
    revision: Arc<AtomicU64>,
    ledger: Arc<ViewLedger>,
    view_gate: Arc<ViewGate>,
    grouped: StdMutex<GroupedQueue>,
    /// Creations applied together with their first bytes; see
    /// [`PendingCreate`].
    pending: StdMutex<Vec<Arc<PendingCreate>>>,
}

/// Most bytes a created file holds before its creation is applied.
const PENDING_CREATE_BYTES: u64 = 1 << 20;

/// Exclusive access that excludes native callbacks.
pub struct SharedCheckoutGuard<'a, A, O> {
    _view: ViewWriteLease,
    state: tokio::sync::RwLockWriteGuard<'a, SharedCheckoutState<A, O>>,
}

/// One callback's shared admission to the checkout's current view.
///
/// Observations overlap freely. Each excludes every mutation until it drops,
/// because a mutation first needs the view gate's exclusive lease; reads go
/// through [`Checkout::observer`], which cannot outlive this admission.
pub(super) struct SharedCheckoutObservation<'a, A, O> {
    _view: ViewReadLease,
    state: tokio::sync::RwLockReadGuard<'a, SharedCheckoutState<A, O>>,
}

/// Locked checkout state. Dereferencing reaches the canonical checkout while
/// publication helpers retain an indeterminate operation in the same lock.
pub struct SharedCheckoutState<A, O> {
    checkout: Checkout<A, O>,
    publication_operation: Option<OperationId>,
    publication: MountPublication,
    revision: Arc<AtomicU64>,
    ledger: Arc<ViewLedger>,
    /// The file table every change up to now has been recorded against.
    recorded_view: ObjectId,
}

impl<A, O> SharedCheckout<A, O> {
    /// Creates the sole shared adapter boundary for one canonical checkout.
    #[must_use]
    pub fn new(checkout: Checkout<A, O>) -> Self {
        Self::with_publication(checkout, MountPublication::CloseAndSync)
    }

    /// Creates a shared checkout with one explicit native publication policy.
    #[must_use]
    pub fn with_publication(checkout: Checkout<A, O>, publication: MountPublication) -> Self {
        let revision = Arc::new(AtomicU64::new(1));
        let ledger = Arc::new(ViewLedger::new());
        Self {
            state: tokio::sync::RwLock::new(SharedCheckoutState {
                recorded_view: checkout.root().file_table,
                checkout,
                publication_operation: None,
                publication,
                revision: Arc::clone(&revision),
                ledger: Arc::clone(&ledger),
            }),
            revision,
            ledger,
            view_gate: Arc::new(ViewGate::new()),
            pending: StdMutex::new(Vec::new()),
            grouped: StdMutex::new(GroupedQueue::default()),
        }
    }

    /// Applies one change by group commit. Changes queued while the checkout
    /// is busy are applied together, as one mutation per hold and in queue
    /// order, by a drain that runs detached from every caller, so no
    /// caller's deadline can interrupt a group; each caller receives exactly
    /// its own change's result.
    ///
    /// Queued changes are concurrent: none was issued after another's result
    /// returned, so applying them in queue order is linearizable. A caller
    /// that stops waiting before its change is claimed abandons it, and an
    /// abandoned change never applies.
    pub(super) async fn apply_grouped(
        self: &Arc<Self>,
        change: GroupedChange,
        cancellation: &CancellationToken,
    ) -> Result<GroupedOutcome, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        // A change may depend on a pending creation: apply those first.
        self.settle_pending().await;
        let (reply, outcome) = tokio::sync::oneshot::channel();
        let admission = Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_QUEUED)));
        let _abandon = AbandonUnlessClaimed(Arc::clone(&admission));
        let start_drain = {
            let mut queue = self
                .grouped
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queue.requests.push_back(GroupedRequest {
                change,
                admission,
                reply,
                origin: ViewOrigin::current(),
                recorded: false,
            });
            !std::mem::replace(&mut queue.draining, true)
        };
        if start_drain {
            let checkout = Arc::clone(self);
            let cancellation = cancellation.clone();
            // The caller that starts the drain runs it in place, saving a
            // hand-off to another thread; if the caller stops waiting, the
            // drain moves to its own task and still runs to completion.
            RunToCompletion::new(async move { checkout.drain_grouped(&cancellation).await }).await;
        }
        outcome.await.map_err(|_| MountSourceError::Stale)?
    }

    /// Applies queued changes, one group per hold of the checkout, until the
    /// queue is empty. Abandoned changes are discarded unapplied.
    async fn drain_grouped(&self, cancellation: &CancellationToken)
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        in_heap(move || async move {
            loop {
                let mut checkout = self.lock_settled().await;
                let requests = {
                    let mut queue = self
                        .grouped
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut claimed = Vec::new();
                    while claimed.len() < MAXIMUM_GROUPED_CHANGES
                        && let Some(request) = queue.requests.pop_front()
                    {
                        if request.admission.claim() {
                            claimed.push(request);
                        }
                    }
                    if claimed.is_empty() {
                        queue.draining = false;
                        return;
                    }
                    claimed
                };
                checkout
                    .apply_grouped_requests(requests, cancellation)
                    .await;
            }
        })
        .await;
    }

    /// Serializes external checkout access and excludes native callbacks,
    /// once every pending creation is applied.
    pub async fn lock(&self) -> SharedCheckoutGuard<'_, A, O>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.settle_pending().await;
        self.lock_settled().await
    }

    async fn lock_settled(&self) -> SharedCheckoutGuard<'_, A, O> {
        let view = self.view_gate.write().await;
        let state = self.state.write().await;
        SharedCheckoutGuard { _view: view, state }
    }

    /// Records `pending` as created: its name changed now, though its
    /// creation is applied later (see [`PendingCreate`]).
    /// Registers a pending creation, atomically with checking that no other
    /// pending creation holds its path and, given `absent_since`, that
    /// nothing rebound the path since it was found absent. Answers whether
    /// it registered.
    fn hold_pending(&self, pending: Arc<PendingCreate>, absent_since: Option<ViewStamp>) -> bool {
        let mut held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if held.iter().any(|other| other.path == pending.path)
            || absent_since.is_some_and(|stamp| !self.binding_unchanged_since(&pending.path, stamp))
        {
            return false;
        }
        {
            let _origin = pending.origin.enter();
            self.ledger.record(&ViewChange::Bound(&pending.path));
        }
        self.revision.fetch_add(1, Ordering::AcqRel);
        held.push(pending);
        true
    }

    /// The pending creation of `path`, reported as it stands.
    fn pending_lookup(&self, path: &NamespacePath) -> Option<MountLookup> {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|pending| pending.path == *path)
            .find_map(|pending| pending.lookup())
    }

    fn pending_create(&self, file_id: FileId) -> Option<Arc<PendingCreate>> {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|pending| pending.file_id == file_id)
            .cloned()
    }

    /// Applies every pending creation as one group. The exclusive hold is
    /// taken first, so no reader sees the checkout between the claim and
    /// the change; each creation keeps its own outcome.
    async fn settle_pending(&self)
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if self
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
        {
            return;
        }
        let mut checkout = self.lock_settled().await;
        let pending =
            std::mem::take(&mut *self.pending.lock().unwrap_or_else(PoisonError::into_inner));
        let mut requests = Vec::with_capacity(pending.len());
        let mut outcomes = Vec::with_capacity(pending.len());
        for create in pending {
            if let Some(change) = create.begin_applying() {
                let (reply, outcome) = tokio::sync::oneshot::channel();
                requests.push(GroupedRequest {
                    change,
                    admission: Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_CLAIMED))),
                    reply,
                    origin: create.origin,
                    recorded: true,
                });
                outcomes.push((create, outcome));
            }
        }
        checkout
            .apply_grouped_requests(requests, &CancellationToken::new())
            .await;
        drop(checkout);
        for (create, outcome) in outcomes {
            create.finish_applying(outcome.await.unwrap_or(Err(MountSourceError::Stale)));
        }
    }

    /// Admits one callback to the current view alongside every other reader,
    /// once every pending creation is applied. A callback that already reads
    /// the view reads the one it began with.
    pub(super) async fn observe(
        &self,
        owner: CallbackOwner,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if !self.view_gate.reads_for_callback(owner) {
            self.settle_pending().await;
        }
        self.observe_as_is(owner).await
    }

    /// Admits one callback to the view as it stands, pending creations and
    /// all: for facts no creation changes.
    async fn observe_as_is(
        &self,
        owner: CallbackOwner,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        let view = self.view_gate.read_for_callback(owner, None).await?;
        let state = self.state.read().await;
        Ok(SharedCheckoutObservation { _view: view, state })
    }

    /// Whether a native durability request (`fsync`, or a close that
    /// publishes) publishes; [`MountPublication::Manual`] leaves every
    /// publication to an explicit sync or unmount.
    pub(super) async fn publishes_at_native_boundary(&self) -> bool {
        self.state.read().await.publishes_at_native_boundary()
    }

    /// Begins an optimistic transaction on a copy of the current candidate.
    /// No reader is excluded: the copy records nothing into this checkout.
    pub(super) async fn candidate(&self) -> Result<CheckoutCandidate<A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.state.read().await.candidate()
    }

    /// Advances with every recorded change to the checkout's view.
    pub(super) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.view_gate.is_stable() && self.ledger.unchanged_since(path, file_id, stamp)
    }

    fn reported_unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.view_gate.is_stable() && self.ledger.reported_unchanged_since(path, file_id, stamp)
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.node_unchanged_since(file_id, stamp)
    }

    fn listing_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.listing_unchanged_since(path, stamp)
    }

    fn binding_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.binding_unchanged_since(path, stamp)
    }
}

impl<A, O> Deref for SharedCheckoutGuard<'_, A, O> {
    type Target = SharedCheckoutState<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<A, O> DerefMut for SharedCheckoutGuard<'_, A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<A, O> Drop for SharedCheckoutGuard<'_, A, O> {
    fn drop(&mut self) {
        if self.state.checkout.root().file_table != self.state.recorded_view {
            self.state.record(&ViewChange::Everything);
        }
    }
}

impl<A, O> Deref for SharedCheckoutObservation<'_, A, O> {
    type Target = SharedCheckoutState<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<A, O> Deref for SharedCheckoutState<A, O> {
    type Target = Checkout<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.checkout
    }
}

impl<A, O> DerefMut for SharedCheckoutState<A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.checkout
    }
}
