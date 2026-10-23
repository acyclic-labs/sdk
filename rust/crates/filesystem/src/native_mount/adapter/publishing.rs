struct Publication<'a, A, O> {
    shared: &'a SharedCheckout<A, O>,
    original: Option<SharedCheckoutState<A, O>>,
    fence: Option<tokio::sync::RwLockWriteGuard<'a, ()>>,
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
        // Restore canonical state before releasing publishers and candidates.
        // Observers can start either, including when this future is cancelled.
        drop(self.fence.take());
        self.shared.publication_changed.notify_waiters();
        if changed {
            self.shared.ledger.notify_everything();
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    pub(in crate::native_mount) async fn publish<E, F, Fut>(
        &self,
        permit: crate::PublicationPermit,
        accounting: crate::kernel::KernelAccounting<'_>,
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
        let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = accounting
        else {
            return Err((E::from(MountSourceError::OriginalSnapshotUnavailable), None));
        };
        let mut carrier = NativeDemandCarrier::Native(work);
        let _publication = await_lock(
            self.publication_fence.write(),
            &mut carrier,
            cancellation,
            lifetime,
        )
        .await
        .map_err(|error| (E::from(error), None))?;
        let mut state = await_lock(self.lock(), &mut carrier, cancellation, lifetime)
            .await
            .map_err(|error| (E::from(error), None))?;
        let force = state.has_retained_operation();
        let early = |error: MountSourceError| (E::from(error), None);
        let submitted = publication_flag(work, force).map_err(early)?;
        let operation = state.retained_operation_id();
        let projected = state
            .publication_candidate(operation, work, cancellation)
            .map_err(early)?;
        carrier.check(cancellation, lifetime).map_err(early)?;
        self.publishing.store(true, Ordering::Release);
        let mut original = std::mem::replace(&mut *state.state, projected);
        // Publish the candidate bound to this invocation; readers keep the prior checkout.
        std::mem::swap(&mut original.checkout, &mut state.state.checkout);
        let mut publication = Publication {
            shared: self,
            original: Some(original),
            fence: Some(_publication),
        };
        drop(state);
        let mut before_publication = before_publication.clone();
        let result: Result<(), E> = async {
            carrier.check(cancellation, lifetime)?;
            let original = publication
                .original
                .as_mut()
                .ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?;
            let (context, mut proof) = original.publication_proof(
                context,
                operation,
                permit,
                work,
                cancellation,
                force,
            )?;
            let producer = original.checkout.private_control_custody();
            proof.producer = Some(&producer);
            let deadline = original.pending_deadline;
            let source_lifetime = lifetime.cloned();
            let guard_submitted = Arc::clone(&submitted);
            let clock = self.clock.clone();
            let mut guarded = move |work: &crate::NativeMaterializationWork| {
                publication_guard(
                    work,
                    &mut before_publication,
                    source_lifetime.as_ref(),
                    &guard_submitted,
                    &clock,
                    deadline,
                )
            };
            let ack = super::publication::seal_checkout_with_permit_guarded_with_native_work(
                &mut original.checkout,
                operation,
                permit,
                force,
                crate::kernel::KernelAccounting::native(work, cancellation),
                &mut guarded,
                proof,
            )
            .await?;
            if ack.is_some()
                || original.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay
            {
                original.clear_retained_operation(operation);
            }
            if ack.is_some() || !original.checkout.has_pending_mutations() {
                original.pending_deadline = None;
            }
            let (operation, generation) = ack.ok_or(MountSourceError::Stale)?;
            if let Some(context) = context {
                *acknowledged = Some(crate::NativePublicationSnapshot {
                    context,
                    operation,
                    generation,
                });
            }
            Ok(())
        }
        .await;
        // Retain cancelled or possibly submitted operations for reconciliation.
        if result.is_err()
            && !submitted.0.load(Ordering::Acquire)
            && carrier.check(cancellation, lifetime).is_ok()
            && let Some(original) = publication.original.as_mut()
        {
            original.clear_retained_operation(operation);
        }
        drop(publication);
        drop(self.lock().await);
        let secondary = carrier.check(cancellation, lifetime);
        match result {
            Ok(()) => secondary.map_err(|error| (E::from(error), None)),
            Err(primary) => Err((primary, secondary.err())),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckoutState<A, O> {
    fn publication_candidate(
        &self,
        operation: OperationId,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Self, MountSourceError> {
        let projection = self
            .checkout
            .private_candidate(
                &mut crate::kernel::allocation::KernelAccounting::native(work, cancellation),
                cancellation,
            )
            .map_err(MountSourceError::from)?;
        Ok(SharedCheckoutState {
            recorded_view: projection.root().file_table,
            checkout: projection,
            publication_operation: Some(operation),
            publication_evidence: None,
            publication: self.publication,
            pending_deadline: None,
            ledger: Arc::clone(&self.ledger),
        })
    }

    fn publication_proof<'p>(
        &mut self,
        context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        operation: OperationId,
        permit: crate::PublicationPermit,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        force: bool,
    ) -> Result<
        (
            Option<crate::lazy_workspace::NativeSnapshotContext>,
            super::publication::PublicationProof<'p>,
        ),
        MountSourceError,
    > {
        if self.publication_evidence.is_none()
            && (force || self.checkout.has_pending_mutations())
            && let Some(context) = context
        {
            let (digest, evidence) = crate::lazy_workspace::publication_evidence(
                context,
                operation,
                permit,
                &mut crate::kernel::KernelAccounting::native(work, cancellation),
            )
            .map_err(MountSourceError::from)?;
            self.publication_evidence = Some((context, permit, digest, evidence));
        }
        Ok(match &self.publication_evidence {
            Some((context, permit, digest, evidence)) => (
                Some(*context),
                super::publication::PublicationProof {
                    producer: None,
                    context: Some(*digest),
                    evidence: Some(evidence.clone()),
                    fingerprint_permit: Some(*permit),
                },
            ),
            None => (context, super::publication::PublicationProof::default()),
        })
    }
}

fn publication_guard<E, F, Fut>(
    work: &crate::NativeMaterializationWork,
    before_publication: &mut F,
    lifetime: Option<&CancellationToken>,
    submitted: &Arc<(AtomicBool, crate::NativeAllocation)>,
    clock: &crate::native_mount::clock::Clock,
    deadline: Option<u64>,
) -> futures::future::Either<
    std::future::Ready<Result<(), E>>,
    impl Future<Output = Result<(), E>> + Send + 'static + use<E, F, Fut>,
>
where
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: From<MountSourceError> + Send + 'static,
{
    if lifetime.is_some_and(CancellationToken::is_cancelled) {
        return futures::future::Either::Left(std::future::ready(Err(E::from(
            MountSourceError::Stale,
        ))));
    }
    if let Err(error) = check_mutation_deadline(deadline, clock) {
        return futures::future::Either::Left(std::future::ready(Err(E::from(error))));
    }
    let clock = clock.clone();
    let future = before_publication(work);
    let lifetime = lifetime.cloned();
    let submitted = Arc::clone(submitted);
    futures::future::Either::Right(async move {
        if lifetime
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(E::from(MountSourceError::Stale));
        }
        future.await?;
        check_mutation_deadline(deadline, &clock).map_err(E::from)?;
        if lifetime
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(E::from(MountSourceError::Stale));
        }
        submitted.0.store(true, Ordering::Release);
        Ok(())
    })
}

fn publication_flag(
    work: &crate::NativeMaterializationWork,
    force: bool,
) -> Result<Arc<(AtomicBool, crate::NativeAllocation)>, MountSourceError> {
    let bytes = (std::mem::size_of::<(AtomicBool, crate::NativeAllocation)>()
        + 2 * std::mem::size_of::<usize>()) as u64;
    let allocation = work
        .claim(bytes, 1, crate::WorkCounters::UNCHARGED)
        .map_err(MountSourceError::NativeWork)?;
    Ok(Arc::new((AtomicBool::new(force), allocation)))
}
