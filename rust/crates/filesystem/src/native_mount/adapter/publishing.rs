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
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<MountSourceError> + Send + 'static,
    {
        let mut carrier = NativeDemandCarrier::Native(work);
        let mut state = await_lock(self.lock(), &mut carrier, cancellation, lifetime)
            .await
            .map_err(|error| (E::from(error), None))?;
        let force = state.has_retained_operation();
        let operation = state.retained_operation_id();
        let early = |error: MountSourceError| (E::from(error), None);
        let projection = state
            .checkout
            .private_candidate(work, cancellation)
            .map_err(fs_error)
            .map_err(early)?;
        let notification = self
            .ledger
            .prepare_everything_notification(work)
            .map_err(early)?;
        let projected = SharedCheckoutState {
            recorded_view: projection.root().file_table,
            checkout: projection,
            publication_operation: Some(operation),
            publication_evidence: None,
            publication: state.publication,
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
        let mut before_publication = before_publication.clone();
        let result: Result<(), E> = async {
            carrier.check(cancellation, lifetime)?;
            let original = publication.original.as_mut().expect("original checkout");
            if original.publication_evidence.is_none()
                && (force || original.checkout.has_pending_mutations())
                && let Some(context) = context
            {
                let (digest, evidence) = crate::lazy_workspace::native_publication_evidence(
                    context,
                    operation,
                    permit,
                    &mut crate::kernel::KernelAccounting::native(work, cancellation),
                )
                .map_err(fs_error)?;
                original.publication_evidence = Some((context, permit, digest, evidence));
            }
            let (context, fingerprint_permit, digest, evidence) =
                match &original.publication_evidence {
                    Some((context, permit, digest, evidence)) => (
                        Some(*context),
                        Some(*permit),
                        Some(*digest),
                        Some(evidence.clone()),
                    ),
                    None => (context, None, None, None),
                };
            let producer = original.checkout.private_control_custody();
            let source_lifetime = lifetime.cloned();
            let mut guarded = move |work: &crate::NativeMaterializationWork| {
                if source_lifetime
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled)
                {
                    return futures::future::Either::Left(std::future::ready(Err(E::from(
                        MountSourceError::Stale,
                    ))));
                }
                let future = before_publication(work);
                let lifetime = source_lifetime.clone();
                futures::future::Either::Right(async move {
                    if lifetime
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        return Err(E::from(MountSourceError::Stale));
                    }
                    future.await?;
                    if lifetime
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        return Err(E::from(MountSourceError::Stale));
                    }
                    Ok(())
                })
            };
            let ack = super::publication::seal_checkout_with_permit_guarded_with_native_work(
                &mut original.checkout,
                operation,
                permit,
                force,
                work,
                cancellation,
                &mut guarded,
                Some(&producer),
                digest,
                evidence,
                fingerprint_permit,
            )
            .await?;
            if ack.is_some()
                || original.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay
            {
                original.clear_retained_operation(operation);
            }
            let (operation, generation) = ack.ok_or(MountSourceError::Stale)?;
            if let Some(context) = context {
                *acknowledged = Some(match operation {
                    Some(operation) => {
                        crate::NativePublicationSnapshot::published(context, operation, generation)
                    }
                    None => crate::NativePublicationSnapshot::unchanged(context, generation),
                });
            }
            Ok(())
        }
        .await;
        drop(publication);
        drop(self.lock().await);
        match (result, carrier.check(cancellation, lifetime)) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), secondary) => Err((primary, secondary.err())),
            (Ok(_), Err(error)) => Err((E::from(error), None)),
        }
    }
}
