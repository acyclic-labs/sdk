/// Serialization and publication boundary for a canonical checkout.
pub struct SharedCheckout<A, O> {
    state: tokio::sync::RwLock<SharedCheckoutState<A, O>>,
    pub(super) ledger: Arc<ViewLedger>,
    view_gate: Arc<ViewGate>,
    publishing: AtomicBool,
    returned: StdMutex<Option<SharedCheckoutState<A, O>>>,
    publication_changed: tokio::sync::Notify,
    _native_allocation: Option<crate::NativeAllocation>,
}

/// Exclusive access that excludes native callbacks.
pub struct SharedCheckoutGuard<'a, A, O> {
    _view: ViewWriteLease,
    state: tokio::sync::RwLockWriteGuard<'a, SharedCheckoutState<A, O>>,
}

/// One callback's shared admission to the checkout's current view.
pub(super) struct SharedCheckoutObservation<'a, A, O> {
    _view: ViewReadLease,
    state: tokio::sync::RwLockReadGuard<'a, SharedCheckoutState<A, O>>,
}

/// Locked checkout state. Dereferencing reaches the canonical checkout while
pub struct SharedCheckoutState<A, O> {
    checkout: Checkout<A, O>,
    publication_operation: Option<OperationId>,
    publication_evidence: Option<(
        crate::lazy_workspace::NativeSnapshotContext,
        crate::PublicationPermit,
        crate::Digest,
        acyclic_stream::OwnedBytes,
    )>,
    publication: MountPublication,
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
        let ledger = Arc::new(ViewLedger::default());
        Self {
            state: tokio::sync::RwLock::new(SharedCheckoutState {
                recorded_view: checkout.root().file_table,
                checkout,
                publication_operation: None,
                publication_evidence: None,
                publication,
                ledger: Arc::clone(&ledger),
            }),
            ledger,
            view_gate: Arc::new(ViewGate::new()),
            publishing: AtomicBool::new(false),
            returned: StdMutex::new(None),
            publication_changed: tokio::sync::Notify::new(),
            _native_allocation: None,
        }
    }

    pub(crate) fn with_publication_meter(
        checkout: Checkout<A, O>,
        publication: MountPublication,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, MountSourceError> {
        let allocation = work
            .claim(
                std::mem::size_of::<Self>() as u64,
                1,
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(MountSourceError::NativeWork)?;
        let ledger = ViewLedger::new(&mut NativeDemandCarrier::Native(work))?;
        let view_gate = ViewGate::new_with_native_work(work)?;
        Ok(Arc::new(Self {
            state: tokio::sync::RwLock::new(SharedCheckoutState {
                recorded_view: checkout.root().file_table,
                checkout,
                publication_operation: None,
                publication_evidence: None,
                publication,
                ledger: Arc::clone(&ledger),
            }),
            ledger,
            view_gate,
            publishing: AtomicBool::new(false),
            returned: StdMutex::new(None),
            publication_changed: tokio::sync::Notify::new(),
            _native_allocation: Some(allocation),
        }))
    }

    /// Serializes checkout access and excludes native callbacks.
    pub async fn lock(&self) -> SharedCheckoutGuard<'_, A, O> {
        loop {
            let changed = self.publication_changed.notified();
            let view = self.view_gate.write().await;
            let mut state = self.state.write().await;
            if self.publishing.load(Ordering::Acquire) {
                drop(state);
                drop(view);
                changed.await;
                continue;
            }
            if let Some(original) = self
                .returned
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                *state = original;
            }
            return SharedCheckoutGuard { _view: view, state };
        }
    }

    /// Admits one callback alongside other readers of the current view.
    pub(super) async fn observe(
        &self,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        loop {
            let view = self
                .view_gate
                .read_for_callback_with_carrier(owner, None, carrier, cancellation, lifetime)
                .await?;
            let state = await_lock(self.state.read(), carrier, cancellation, lifetime).await?;
            if self
                .returned
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none()
            {
                return Ok(SharedCheckoutObservation { _view: view, state });
            }
            drop(state);
            drop(view);
            drop(await_lock(self.lock(), carrier, cancellation, lifetime).await?);
        }
    }

    /// Whether a native durability request (`fsync`, or a close that
    pub(super) async fn publishes_at_native_boundary(&self) -> bool {
        self.state.read().await.publishes_at_native_boundary()
    }

    /// Begins an optimistic transaction on a copy of the current candidate.
    pub(super) async fn candidate(
        &self,
        operation: Option<OperationId>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let state = await_lock(self.lock(), carrier, cancellation, lifetime).await?;
        let epoch = self.view_gate.generation();
        let stamp = self.ledger.stamp();
        Ok((
            state.candidate(operation, carrier, cancellation)?,
            epoch,
            stamp,
        ))
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
