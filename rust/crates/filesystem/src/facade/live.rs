use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    /// Explicitly performs the same observation-safe head advancement used
    pub async fn refresh_live(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        if self.mode.consistency != ConsistencyMode::Live {
            return Err(OperationFailure::before_work(FsError::RefreshNotAllowed));
        }
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .synchronize_live_with_accounting(&mut accounting, cancellation)
            .await
            .map(|()| self.generation_id());
        measured(result, &accounting)
    }

    pub(crate) async fn synchronize_live_with_accounting(
        &mut self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        if self.mode.consistency != ConsistencyMode::Live {
            return Ok(());
        }
        if self.live_operation_id.is_some() {
            return Err(FsError::PendingLiveMutation);
        }
        let (decision, allocation) = self
            .rebase_head_with_accounting(
                self.volume.config.limits.maximum_checkout_dependencies,
                accounting,
                cancellation,
            )
            .await?;
        match decision {
            RebaseDecision::Safe { .. } => Ok(()),
            RebaseDecision::Conflicted {
                conflicts,
                truncated,
            } => match allocation {
                crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                    Err(FsError::LiveConflict {
                        conflicts,
                        truncated,
                    })
                }
                #[cfg(not(target_arch = "wasm32"))]
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
                    Err(FsError::NativeLiveConflict {
                        conflicts,
                        truncated,
                        allocation,
                    })
                }
            },
        }
    }

    /// Applies and durably publishes one direct-live mutation operation.
    pub async fn mutate_live(
        &mut self,
        operations: Vec<Mutation>,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation()
            .map_err(OperationFailure::before_work)?;
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        let mutation = self.mutate(operations, budget, cancellation).await?;
        let mut work = mutation.work;
        let publication = self
            .resume_live(operation_id, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, publication.work)?;
        Ok(OperationReceipt {
            value: publication.value,
            work,
        })
    }

    /// Compiles, atomically applies, and durably publishes one authored live transaction.
    pub async fn apply_authored_live(
        &mut self,
        authored: Vec<AuthoredMutation>,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AuthoredLiveMutationResult> {
        self.validate_live_operation()
            .map_err(OperationFailure::before_work)?;
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        let transaction = self
            .apply_authored_transaction(authored, budget, cancellation)
            .await?;
        let mut work = transaction.work;
        let created_file_ids = transaction.value.created_file_ids;
        let publication = self
            .resume_live(operation_id, remaining(work, budget)?, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, publication.work)?;
        Ok(OperationReceipt {
            value: AuthoredLiveMutationResult {
                created_file_ids,
                outcome: publication.value,
            },
            work,
        })
    }

    /// Publishes this candidate once with the original operation identity.
    /// A conditional conflict leaves the candidate unchanged and permits an
    /// explicit `rebase_head`; an indeterminate error retains the identity fence.
    pub async fn resume_live(
        &mut self,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation()
            .map_err(OperationFailure::before_work)?;
        if !self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::NoPendingMutations));
        }
        match self.live_operation_id {
            Some(retained) if retained != operation_id => {
                return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
            }
            Some(_) => {}
            None => self.live_operation_id = Some(operation_id),
        }
        let publication = self
            .publish_live(operation_id, budget, cancellation)
            .await?;
        if matches!(
            publication.value,
            LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. }
                | LiveMutationOutcome::Conflict { .. }
        ) {
            self.live_operation_id = None;
        }
        Ok(publication)
    }

    /// Publishes once or resolves the exact retained durable operation.
    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) async fn resume_live_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        producer_custody: Option<&Arc<crate::kernel::ProofDataCustody>>,
        operation_context: Option<Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        original_fingerprint_permit: Option<PublicationPermit>,
    ) -> Result<(LiveMutationOutcome, crate::NativeAllocation), E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        accounting.check().map_err(E::from)?;
        self.validate_live_operation().map_err(E::from)?;
        self.validate_native_work(original).map_err(E::from)?;
        if !self.has_pending_mutations() {
            return Err(E::from(FsError::NoPendingMutations));
        }
        match self.live_operation_id {
            Some(retained) if retained != operation_id => {
                return Err(E::from(FsError::PendingLiveMutation));
            }
            Some(_) => {}
            None => self.live_operation_id = Some(operation_id),
        }
        let outcome = match self
            .retry_stale_commit_with_native_work(
                operation_id,
                permit,
                original,
                cancellation,
                before_publication,
                producer_custody,
                operation_context,
                operation_evidence.clone(),
                original_fingerprint_permit,
            )
            .await?
        {
            Some(outcome) => outcome,
            None => {
                self.publish_pending_with_native_work(
                    operation_id,
                    operation_context,
                    operation_evidence,
                    original_fingerprint_permit,
                    permit,
                    original,
                    cancellation,
                    before_publication,
                    producer_custody,
                )
                .await?
            }
        };
        let allocation = accounting.claim_owned(0, 0).map_err(E::from)?;
        if matches!(
            outcome,
            LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. }
                | LiveMutationOutcome::Conflict { .. }
        ) {
            self.live_operation_id = None;
        }
        match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
                Ok((outcome, allocation))
            }
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                unreachable!("native live outcomes preserve original allocation custody")
            }
        }
    }

    pub(super) fn validate_live_operation(&self) -> Result<(), FsError> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.consistency != ConsistencyMode::Live
            || self.mode.mutations != MutationMode::DirectLive
        {
            return Err(FsError::MutationNotAllowed);
        }
        Ok(())
    }

    pub(super) async fn publish_live(
        &mut self,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.publish_pending(
            operation_id,
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            budget,
            cancellation,
            &mut |_| async {
                FsResult::Ok(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
    }
}
