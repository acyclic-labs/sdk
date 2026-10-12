struct Publication<'a, A, O> {
    shared: &'a SharedCheckout<A, O>,
    original: Option<SharedCheckoutState<A, O>>,
    notification: Option<super::view_ledger::PreparedEverythingNotification>,
}

impl<A, O> Drop for Publication<'_, A, O> {
    fn drop(&mut self) {
        let Some(mut original) = self.original.take() else {
            return;
        };
        let changed = original.checkout.root().file_table != original.recorded_view;
        original.recorded_view = original.checkout.root().file_table;
        *self
            .shared
            .returned
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(original);
        self.shared.publishing.store(false, Ordering::Release);
        self.shared.publication_changed.notify_waiters();
        if let Some(notification) = self.notification.take() {
            if changed {
                notification.record();
            }
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    async fn publish(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut state = await_lock(self.lock(), &mut carrier, cancellation, lifetime).await?;
        let projection = state
            .checkout
            .private_candidate(work, cancellation)
            .map_err(fs_error)?;
        let notification = self
            .ledger
            .prepare_everything_notification(work)?;
        let operation = state.retained_operation_id();
        let projected = SharedCheckoutState {
            recorded_view: projection.root().file_table,
            checkout: projection,
            publication_operation: Some(operation),
            publication: state.publication,
            revision: Arc::clone(&state.revision),
            ledger: Arc::clone(&state.ledger),
        };
        carrier.check(cancellation, lifetime)?;
        self.publishing.store(true, Ordering::Release);
        let mut publication = Publication {
            shared: self,
            original: Some(std::mem::replace(&mut *state.state, projected)),
            notification: Some(notification),
        };
        drop(state);
        let original = publication.original.as_mut().expect("original checkout");
        let producer = original.checkout.private_control_custody();
        let result = super::publication::seal_checkout_with_permit_guarded_with_native_work(
            &mut original.checkout,
            operation,
            permit,
            false,
            work,
            cancellation,
            &mut source_publication_guard(guard, lifetime.cloned()),
            Some(&producer),
            None,
            None,
            None,
        )
        .await;
        let result = result.and_then(|ack| {
            if ack.is_some()
                || original.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay
            {
                original.clear_retained_operation(operation);
            }
            ack.map(|_| ()).ok_or(MountSourceError::Stale)
        });
        drop(publication);
        drop(self.lock().await);
        result.and(carrier.check(cancellation, lifetime))
    }
}
