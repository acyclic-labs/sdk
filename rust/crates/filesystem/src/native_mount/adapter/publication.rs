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

    /// Applies one content change by stable identity and stamps its
    /// modification and status-change times in the same mutation, as a
    /// native write, truncation, or allocation does, then publishes it like
    /// any mutation. Times a profile does not represent stay unavailable.
    pub(super) async fn change_content_by_id(
        &mut self,
        file_id: FileId,
        change: ContentChange<FileId>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.checkout
            .change_content_by_id(
                file_id,
                change,
                ContentTimes::Stamp(content_change_time()?),
                boundary_budget(),
                cancellation,
            )
            .await
            .map_err(engine_error)?;
        self.publish_after_mutation(ViewChange::Node(file_id), cancellation)
            .await
    }

    /// Applies one content change at an exact path with its time stamp as
    /// one mutation, exactly as [`Self::change_content_by_id`] does.
    pub(super) async fn change_content(
        &mut self,
        path: NamespacePath,
        change: ContentChange<NamespacePath>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let node = self.node_at(&path, cancellation).await?;
        self.checkout
            .change_content(
                path,
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
            .inspector()
            .lookup_no_follow(path, boundary_budget(), cancellation)
            .await
            .map_err(engine_error)?
            .value
            .record
            .map(|record| record.file_id))
    }

    /// Begins an optimistic transaction on a copy of this candidate.
    pub(super) fn candidate(&self) -> Result<CheckoutCandidate<A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.ensure_publication_resolved()?;
        Ok(CheckoutCandidate {
            base: self.private_candidate(),
            checkout: self.private_candidate(),
        })
    }

    /// Whether a transaction's candidate may still be installed: nothing
    /// but observations has changed this checkout since the transaction
    /// began, so installing undoes no other change, publication included.
    pub(super) fn admits(&self, candidate: &CheckoutCandidate<A, O>) -> bool {
        self.checkout
            .admits_candidate(&candidate.base, &candidate.checkout)
    }

    /// Installs a transaction's candidate with its recorded effect, only
    /// when [`Self::admits`] it. Returns whether it installed; any other
    /// change since leaves the checkout untouched for the caller to retry.
    pub(super) fn install_candidate(
        &mut self,
        candidate: CheckoutCandidate<A, O>,
        installed: &ViewEffect,
    ) -> bool {
        if !self
            .checkout
            .adopt_candidate(&candidate.base, candidate.checkout)
        {
            return false;
        }
        self.record(&ViewChange::Effect(installed));
        true
    }

    /// Applies queued changes as one group, records exactly what each
    /// successful one changed, publishes once when the policy publishes
    /// every mutation, and answers every caller.
    async fn apply_grouped_requests(
        &mut self,
        requests: Vec<GroupedRequest>,
        cancellation: &CancellationToken,
    ) where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        in_heap(move || async move {
            if let Err(error) = self.ensure_publication_resolved() {
                for request in requests {
                    let _ = request.reply.send(Err(error.clone()));
                }
                return;
            }
            let origins = requests
                .iter()
                .map(|request| (request.origin, request.recorded))
                .collect::<Vec<_>>();
            let (changes, replies): (Vec<_>, Vec<_>) = requests
                .into_iter()
                .map(|request| (request.change, request.reply))
                .unzip();
            let views = changes
                .iter()
                .map(|change| match change {
                    GroupedChange::CreateFile { path, .. } => GroupedView::Bound(path.clone()),
                    GroupedChange::Content { file_id, .. } => GroupedView::Node(*file_id),
                })
                .collect::<Vec<_>>();
            let results = match self
                .checkout
                .apply_group(changes, boundary_budget(), cancellation)
                .await
            {
                Ok(receipt) => receipt.value,
                Err(failure) => {
                    let error = engine_error(failure.error);
                    for reply in replies {
                        let _ = reply.send(Err(error.clone()));
                    }
                    return;
                }
            };
            let mut changed = false;
            for ((result, view), (origin, recorded)) in results.iter().zip(&views).zip(origins) {
                if result.is_ok() {
                    changed = true;
                    if recorded {
                        continue;
                    }
                    let _origin = origin.enter();
                    match view {
                        GroupedView::Bound(path) => self.record(&ViewChange::Bound(path)),
                        GroupedView::Node(file_id) => self.record(&ViewChange::Node(*file_id)),
                    }
                }
            }
            if changed {
                // Changes recorded when they were requested are recorded too.
                self.recorded_view = self.checkout.root().file_table;
            }
            let published = if changed {
                self.publish_mutation(cancellation).await
            } else {
                Ok(())
            };
            for ((result, view), reply) in results.into_iter().zip(views).zip(replies) {
                let answer = match result {
                    Ok(outcome) => published.clone().map(|()| outcome),
                    Err(error) => Err(match view {
                        GroupedView::Bound(_) => fs_error(error),
                        GroupedView::Node(_) => engine_error(error),
                    }),
                };
                let _ = reply.send(answer);
            }
        })
        .await;
    }

    /// Records one change to the checkout's view. The exclusive guard keeps
    /// every lookup from overlapping the change it records.
    pub(super) fn record(&mut self, change: &ViewChange<'_>) {
        self.ledger.record(change);
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
    }
}
