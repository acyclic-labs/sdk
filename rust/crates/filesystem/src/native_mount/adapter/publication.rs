impl<A, O> SharedCheckoutState<A, O> {
    fn publishes_at_native_boundary(&self) -> bool {
        self.publication != MountPublication::Manual
    }

    /// Rejects later mutation while one publication has an unresolved result.
    pub fn ensure_publication_resolved(&self) -> Result<(), MountSourceError> {
        if self.publication_operation.is_some() {
            return Err(MountSourceError::Stale);
        }
        Ok(())
    }

    /// Returns the exact operation retained until publication resolves.
    pub fn retained_operation_id(&mut self) -> OperationId {
        *self
            .publication_operation
            .get_or_insert_with(OperationId::new)
    }

    pub(super) fn has_retained_operation(&self) -> bool {
        self.publication_operation.is_some()
    }

    /// Retains a caller-selected operation identity, or rejects a conflicting
    pub fn retain_operation_id(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), MountSourceError> {
        match self.publication_operation {
            None => {
                self.publication_operation = Some(operation_id);
                Ok(())
            }
            Some(retained) if retained == operation_id => Ok(()),
            Some(_) => Err(MountSourceError::Stale),
        }
    }

    /// Clears only an operation with a known terminal publication outcome.
    pub fn clear_retained_operation(&mut self, operation_id: OperationId) {
        if self.publication_operation == Some(operation_id) {
            self.publication_operation = None;
        }
    }

    /// Begins an optimistic transaction on a copy of this candidate.
    pub(super) fn candidate(
        &self,
        operation: Option<OperationId>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<CheckoutCandidate<A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        match operation {
            Some(operation) if self.publication_operation == Some(operation) => {}
            Some(_) => return Err(MountSourceError::Stale),
            None => self.ensure_publication_resolved()?,
        }
        let clone = |carrier: &mut NativeDemandCarrier<'_>| {
            if let Some(work) = carrier.original_native_work() {
                return self
                    .checkout
                    .private_candidate(&work, cancellation)
                    .map_err(MountSourceError::Filesystem);
            }
            let result = carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.live_allocation_bytes(),
                    self.checkout.private_candidate(
                        work.remaining().map_err(super::lazy::mount_demand_error)?,
                        cancellation,
                    ),
                ))
            })?;
            let checkout = carrier.accept_with_prior_live(result.1, result.0, fs_error)?;
            carrier
                .with_work(|work| work.retain_checkout_control(&checkout.private_control_custody()))
                .map_err(super::lazy::mount_demand_error)?;
            Ok(checkout)
        };
        Ok(CheckoutCandidate {
            base: clone(carrier)?,
            checkout: clone(carrier)?,
        })
    }

    /// Records one change to the checkout's view. The exclusive guard keeps
    pub(super) fn record(&mut self, change: &ViewChange<'_>) {
        record_view_change(
            &self.ledger,
            change,
            &mut NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )),
        )
        .expect("external SDK view recording");
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
    }
}
