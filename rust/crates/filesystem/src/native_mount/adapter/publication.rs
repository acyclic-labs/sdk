impl<A, O> SharedCheckoutState<A, O> {
    async fn seal(&mut self, cancellation: &CancellationToken) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let operation_id = self.retained_operation_id();
        let result = seal_checkout(
            &mut self.checkout,
            operation_id,
            boundary_budget(),
            cancellation,
        )
        .await;
        if result.is_ok()
            || (matches!(result, Err(MountSourceError::Stale))
                && self.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay)
        {
            self.clear_retained_operation(operation_id);
        }
        result
    }

    async fn seal_with_permit(
        &mut self,
        permit: crate::PublicationPermit,
        force: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let operation_id = self.retained_operation_id();
        let result = if force {
            super::publication::seal_checkout_with_permit_force(
                &mut self.checkout,
                operation_id,
                permit,
                budget,
                cancellation,
            )
            .await
        } else {
            super::seal_checkout_with_permit(
                &mut self.checkout,
                operation_id,
                permit,
                budget,
                cancellation,
            )
            .await
        };
        if result.is_ok()
            || (matches!(result, Err(MountSourceError::Stale))
                && self.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay)
        {
            self.clear_retained_operation(operation_id);
        }
        result
    }

    /// Changes content and timestamps, then records and publishes the mutation.
    pub(super) async fn change_content<F: crate::facade::ContentFile>(
        &mut self,
        file: F,
        change: ContentChange<F>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let node = match file.address() {
            crate::facade::ContentAddress::Path(path) => self.node_at(path, cancellation).await?,
            crate::facade::ContentAddress::File(file_id) => file_id,
        };
        self.checkout
            .change_content(
                file,
                change,
                ContentTimes::Stamp(content_change_time()?),
                boundary_budget(),
                cancellation,
            )
            .await
            .map_err(engine_error)?;
        self.publish_after_mutation(ViewChange::Node(node), cancellation)
            .await
    }

    /// Records exactly what one mutation changed, then publishes it when the
    /// policy publishes every mutation.
    pub(super) async fn publish_after_mutation(
        &mut self,
        change: ViewChange<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.record(&change);
        self.publish_mutation(cancellation).await
    }

    /// Publishes one recorded mutation when the policy publishes every
    /// mutation.
    pub(super) async fn publish_mutation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        in_heap(move || async move {
            if self.publication == MountPublication::PerMutation {
                self.seal(cancellation).await?;
            }
            Ok(())
        })
        .await
    }

    async fn publish_at_native_boundary(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if self.publishes_at_native_boundary() {
            self.seal(cancellation).await?;
        }
        Ok(())
    }

    fn publishes_at_native_boundary(&self) -> bool {
        self.publication != MountPublication::Manual
    }

    /// Rejects later mutation while one publication has an unresolved result.
    ///
    /// # Errors
    ///
    /// Returns [`MountSourceError::Stale`] while publication is indeterminate.
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
    /// unresolved publication from another surface sharing this checkout.
    ///
    /// # Errors
    ///
    /// Returns [`MountSourceError::Stale`] for a different retained operation.
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

    /// The node a mutation is about to change through `path`, read without
    /// recording an observation.
    async fn node_at(
        &self,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> Result<FileId, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.node_if_bound(path, cancellation)
            .await?
            .ok_or(MountSourceError::NotFound)
    }

    async fn node_if_bound(
        &self,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileId>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        Ok(self
            .checkout
            .inspector(boundary_budget(), cancellation)
            .map_err(facade_error)?
            .value
            .lookup_no_follow(path, boundary_budget(), cancellation)
            .await
            .map_err(engine_error)?
            .value
            .record
            .map(|record| record.file_id))
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
    /// every lookup from overlapping the change it records.
    pub(super) fn record(&mut self, change: &ViewChange<'_>) {
        record_view_change(
            &self.ledger,
            change,
            &mut NativeDemandCarrier::Borrowed(&mut super::view_ledger::unbounded_view_work()),
        )
        .expect("external SDK view recording");
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
    }
}
