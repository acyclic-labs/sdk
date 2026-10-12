impl<A: AsyncAuthorityStore, O: AsyncObjectStore> SharedCheckout<A, O> {
    pub(super) async fn install_candidate<T>(
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
        operation: Option<OperationId>,
        complete: impl FnOnce(MountPublication, &mut SharedCheckoutState<A, O>) -> T,
    ) -> Result<Option<T>, MountSourceError> {
        carrier.check(cancellation, lifetime)?;
        if self.view_gate.generation() != epoch || self.ledger.stamp() != stamp {
            return Ok(None);
        }
        // Observers may reenter the source, so dispatch before taking its write lease.
        record(carrier)?;
        let mut state = await_lock(self.lock(), carrier, cancellation, lifetime).await?;
        if self.view_gate.generation() != epoch {
            return Ok(None);
        }
        if operation.is_some_and(|operation| {
            state
                .publication_operation
                .is_some_and(|held| held != operation)
        }) {
            return Err(MountSourceError::Stale);
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
            let (budget, live) = carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(super::lazy::mount_demand_error)?,
                    work.live_allocation_bytes(),
                ))
            })?;
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
        if !commit(&mut state.checkout, candidate, prepared)? {
            return Ok(None);
        }
        state.recorded_view = state.checkout.root().file_table;
        state.revision.fetch_add(1, Ordering::AcqRel);
        if let Some(operation) = operation {
            state.publication_operation = Some(operation);
        }
        Ok(Some(complete(state.publication, &mut state)))
    }
}
