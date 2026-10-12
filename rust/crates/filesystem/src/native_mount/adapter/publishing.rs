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
    pub(in crate::native_mount) async fn publish<E, F, Fut>(
        &self,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        before_publication: &mut F,
        context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
    ) -> Result<(), (E, Option<MountSourceError>)>
    where
        E: From<FsError> + From<MountSourceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let early = |cause: MountSourceError| (E::from(cause), None);
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut state = await_lock(self.lock(), &mut carrier, cancellation, lifetime).await.map_err(early)?;
        let projection = state
            .checkout
            .private_candidate(work, cancellation)
            .map_err(fs_error).map_err(early)?;
        let notification = self
            .ledger
            .prepare_everything_notification(work).map_err(early)?;
        let force = state.has_retained_operation();
        let operation = state.retained_operation_id();
        let evidence = context.map(|context| {
            crate::lazy_workspace::native_publication_evidence(
                context, operation, permit, &mut crate::kernel::KernelAccounting::native(work, cancellation),
            )
        }).transpose().map_err(fs_error).map_err(early)?;
        let projected = SharedCheckoutState {
            recorded_view: projection.root().file_table,
            checkout: projection,
            publication_operation: Some(operation),
            publication: state.publication,
            revision: Arc::clone(&state.revision),
            ledger: Arc::clone(&state.ledger),
        };
        carrier.check(cancellation, lifetime).map_err(early)?;
        self.publishing.store(true, Ordering::Release);
        let mut publication = Publication {
            shared: self,
            original: Some(std::mem::replace(&mut *state.state, projected)),
            notification: Some(notification),
        };
        drop(state);
        let original = publication.original.as_mut().expect("original checkout");
        let producer = original.checkout.private_control_custody();
        let mut before = before_publication.clone();
        let source_lifetime = lifetime.cloned();
        let mut guarded = move |work: &crate::NativeMaterializationWork| {
            if source_lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                return futures::future::Either::Left(std::future::ready(Err(E::from(MountSourceError::Stale))));
            }
            let verified = before(work);
            let lifetime = source_lifetime.clone();
            futures::future::Either::Right(async move {
                if lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                    return Err(E::from(MountSourceError::Stale));
                }
                verified.await?;
                if lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                    return Err(E::from(MountSourceError::Stale));
                }
                Ok(())
            })
        };
        let result = super::publication::seal_checkout_with_permit_guarded_with_native_work(
            &mut original.checkout,
            operation,
            permit,
            force,
            work,
            cancellation,
            &mut guarded,
            Some(&producer),
            evidence.as_ref().map(|(digest, _)| *digest),
            evidence.map(|(_, bytes)| bytes),
            context.map(|_| permit),
        )
        .await;
        let result = result.and_then(|ack| {
            if ack.is_some()
                || original.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay
            {
                original.clear_retained_operation(operation);
            }
            if let (Some((operation, generation)), Some(context)) = (ack, context) {
                *acknowledged = Some(match operation {
                    Some(operation) => crate::NativePublicationSnapshot::published(context, operation, generation),
                    None => crate::NativePublicationSnapshot::unchanged(context, generation),
                });
            }
            ack.map(|_| ()).ok_or_else(|| E::from(MountSourceError::Stale))
        });
        drop(publication);
        drop(self.lock().await);
        match (result, carrier.check(cancellation, lifetime)) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(cause), Ok(())) => Err((cause, None)),
            (Ok(()), Err(cause)) => Err((E::from(cause), None)),
            (Err(cause), Err(accounting)) => Err((cause, Some(accounting))),
        }
    }
}
