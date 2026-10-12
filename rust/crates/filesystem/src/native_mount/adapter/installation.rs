impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    async fn install_change(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        change: ViewChange<'_>,
        work: &crate::NativeMaterializationWork,
        lifetime: &CancellationToken,
        guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError> {
        let cancellation = work.cancellation();
        let publication = self
            .install_candidate(
                candidate,
                epoch,
                stamp,
                &mut NativeDemandCarrier::Native(work),
                cancellation,
                Some(lifetime),
                |carrier| record_view_change(&self.ledger, &change, carrier),
                |checkout, candidate, prepared| {
                    cancellation
                        .commit_if_both_live(lifetime, || {
                            checkout.adopt_prepared_candidate(
                                &candidate.base,
                                candidate.checkout,
                                prepared,
                            )
                        })
                        .map_err(|_| MountSourceError::Cancelled)
                },
            )
            .await?
            .ok_or(MountSourceError::Stale)?;
        if publication == MountPublication::PerMutation {
            self.publish(
                crate::PublicationPermit::Unrestricted,
                work,
                cancellation,
                Some(lifetime),
                &mut source_publication_guard(guard, Some(lifetime.clone())),
                None,
                &mut None,
            )
            .await
            .map_err(|(error, _)| error)?;
        }
        Ok(())
    }

    pub(super) async fn install_candidate(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: impl FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError>,
        commit: impl FnOnce(
            &mut Checkout<A, O>,
            CheckoutCandidate<A, O>,
            crate::facade::PreparedCandidateAdoption,
        ) -> Result<bool, MountSourceError>,
    ) -> Result<Option<MountPublication>, MountSourceError> {
        carrier.check(cancellation, lifetime)?;
        if self.view_gate.generation() != epoch || self.ledger.stamp() != stamp {
            return Ok(None);
        }
        // Observers may reenter the source, so dispatch before taking its write lease.
        record(carrier)?;
        let mut state = await_lock(self.lock(), carrier, cancellation, lifetime).await?;
        if self.view_gate.generation() != epoch
            || state.publication_operation.is_some()
                && state.publication_operation != candidate.operation
        {
            return Ok(None);
        }
        let prepared = if let Some(original) = carrier.original_native_work() {
            state
                .checkout
                .prepare_candidate_adoption(
                    &candidate.base,
                    &candidate.checkout,
                    &original,
                    cancellation,
                )
                .map_err(MountSourceError::Filesystem)?
        } else {
            let (budget, live) = carrier.remaining()?;
            let result = state.checkout.prepare_candidate_adoption(
                &candidate.base,
                &candidate.checkout,
                budget,
                cancellation,
            );
            carrier.accept_with_prior_live(result, live, fs_error)?
        };
        let Some(prepared) = prepared else {
            return Ok(None);
        };
        if carrier.original_native_work().is_none() {
            carrier
                .with_work(|work| {
                    work.retain_checkout_control(&candidate.checkout.private_control_custody())
                })
                .map_err(super::lazy::mount_demand_error)?;
        }
        carrier.check(cancellation, lifetime)?;
        let operation = candidate.operation;
        if !commit(&mut state.checkout, candidate, prepared)? {
            return Ok(None);
        }
        if let Some(operation) = operation {
            state.retain_operation_id(operation)?;
        }
        state.recorded_view = state.checkout.root().file_table;
        Ok(Some(state.publication))
    }
}
