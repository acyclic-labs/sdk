use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {


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
        let result = self.synchronize_live_with_accounting(&mut accounting, cancellation).await
            .map(|()| self.generation_id());
        measured(result, &accounting)
    }


    pub(crate) async fn synchronize_live_with_accounting(
        &mut self,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        if self.mode.consistency != ConsistencyMode::Live { return Ok(()); }
        if self.live_operation_id.is_some() { return Err(FsError::PendingLiveMutation); }
        let (decision, allocation) = self.rebase_head_with_accounting(
            self.volume.config.limits.maximum_checkout_dependencies, accounting, cancellation,
        ).await?;
        match decision {
            RebaseDecision::Safe { .. } => { drop(allocation); Ok(()) }
            RebaseDecision::Conflicted { conflicts, truncated } => {
                match allocation {
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                        Err(FsError::LiveConflict { conflicts, truncated }),
                    #[cfg(not(target_arch = "wasm32"))]
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                        Err(FsError::NativeLiveConflict { conflicts, truncated, allocation }),
                }
            }
        }
    }


    /// Applies and durably publishes one direct-live mutation operation.
    pub async fn mutate_live(
        &mut self,
        operations: Vec<Mutation>,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation(maximum_attempts).map_err(OperationFailure::before_work)?;
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        let mutation = self
            .mutate_candidate(operations, budget, cancellation)
            .await?;
        let mut work = mutation.work;
        let publication = self
            .resume_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, publication.work)?;
        Ok(FsReceipt {
            value: publication.value,
            work,
        })
    }


    /// Compiles, atomically applies, and durably publishes one authored live transaction.
    pub async fn apply_authored_live(
        &mut self,
        authored: Vec<AuthoredMutation>,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AuthoredLiveMutationResult> {
        self.validate_live_operation(maximum_attempts).map_err(OperationFailure::before_work)?;
        if self.has_pending_mutations() {
            return Err(OperationFailure::before_work(FsError::PendingLiveMutation));
        }
        let transaction = self
            .apply_authored_transaction(authored, budget, cancellation)
            .await?;
        let mut work = transaction.work;
        let created_file_ids = transaction.value.created_file_ids;
        let publication = self
            .resume_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                remaining(work, budget)?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, publication.work)?;
        Ok(FsReceipt {
            value: AuthoredLiveMutationResult {
                created_file_ids,
                outcome: publication.value,
            },
            work,
        })
    }


    /// Resumes publication of an unresolved direct-live candidate.
    pub async fn resume_live(
        &mut self,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        self.validate_live_operation(maximum_attempts).map_err(OperationFailure::before_work)?;
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
            .publish_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                budget,
                cancellation,
            )
            .await?;
        if matches!(
            publication.value,
            LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. }
        ) {
            self.live_operation_id = None;
        }
        Ok(publication)
    }


    /// Resumes the existing live retry controller on the original operation.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn resume_live_with_native_work<'a, E, F, Fut>(
        &mut self,
        operation_id: OperationId,
        maximum_attempts: u32,
        maximum_conflicts: u32,
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
            + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        accounting.check().map_err(E::from)?;
        self.validate_live_operation(maximum_attempts).map_err(E::from)?;
        self.validate_native_work(original).map_err(E::from)?;
        if !self.has_pending_mutations() {
            return Err(E::from(FsError::NoPendingMutations));
        }
        match self.live_operation_id {
            Some(retained) if retained != operation_id =>
                return Err(E::from(FsError::PendingLiveMutation)),
            Some(_) => {}
            None => self.live_operation_id = Some(operation_id),
        }
        let mut state = LiveRetryState::new(maximum_attempts)
            .map_err(|error| E::from(FsError::LiveRetry(error)))?;
        let mut empty_allocation = accounting.claim_owned(0, 0).map_err(E::from)?;
        let (outcome, allocation) = loop {
            accounting.check().map_err(E::from)?;
            let mut before_publication = (*before_publication).clone();
            let publication = match self.retry_stale_commit_with_native_work(
                operation_id, permit, original, cancellation, &mut before_publication,
                producer_custody, operation_context, operation_evidence.clone(), original_fingerprint_permit,
            ).await? {
                Some(outcome) => outcome,
                None => self.publish_pending_with_native_work(
                    operation_id, operation_context, operation_evidence.clone(), original_fingerprint_permit,
                    permit, original, cancellation, &mut before_publication, producer_custody,
                ).await?,
            };
            if let Some(completed) = self.finish_live_retry(
                &mut state, publication, maximum_conflicts, &mut accounting, cancellation, &mut empty_allocation,
            ).await.map_err(E::from)? {
                break completed;
            }
        };
        if matches!(outcome,
            LiveMutationOutcome::Committed { .. }
                | LiveMutationOutcome::AlreadyCommitted { .. }
                | LiveMutationOutcome::IdempotencyConflict { .. })
        {
            self.live_operation_id = None;
        }
        match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =>
                Ok((outcome, allocation)),
            crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                unreachable!("native live outcomes preserve original allocation custody"),
        }
    }


    pub(super) fn validate_live_operation(&self, maximum_attempts: u32) -> Result<(), FsError> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.consistency != ConsistencyMode::Live
            || self.mode.mutations != MutationMode::DirectLive
        {
            return Err(FsError::MutationNotAllowed);
        }
        if maximum_attempts == 0 {
            return Err(FsError::ZeroLiveRetryLimit);
        }
        Ok(())
    }


    pub(super) async fn publish_live(
        &mut self, operation_id: OperationId, maximum_attempts: u32, maximum_conflicts: u32,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> FsResult<LiveMutationOutcome> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            accounting.check()?;
            let mut state = LiveRetryState::new(maximum_attempts).map_err(FsError::LiveRetry)?;
            let mut empty_allocation = accounting.claim_owned(0, 0)?;
            loop {
                accounting.check()?;
                let publication = self.publish_pending(
                    operation_id, None, None, None, PublicationPermit::Unrestricted,
                    accounting.remaining()?, cancellation,
                    &mut |_| async { FsResult::Ok(FsReceipt { value: (), work: WorkCounters::UNCHARGED }) },
                ).await;
                let incoming = match &publication { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
                let (allocations, work, allowance, _) = accounting.standalone_parts()
                    .ok_or(FsError::PendingMutationAllocationFailed)?;
                *work = merge_simultaneous_work(*work, incoming, allocations.live_bytes(), allowance)
                    .map_err(|failure| failure.error)?;
                let publication = publication.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                if let Some((outcome, allocation)) = self.finish_live_retry(
                    &mut state, publication, maximum_conflicts, &mut accounting, cancellation, &mut empty_allocation,
                ).await? {
                    drop(allocation);
                    return Ok(outcome);
                }
            }
        }.await;
        measured(result, &accounting)
    }


    pub(super) async fn finish_live_retry(
        &mut self, state: &mut LiveRetryState, publication: CheckoutCommitOutcome, maximum_conflicts: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
        empty_allocation: &mut KernelOwnedAllocation,
    ) -> Result<Option<(LiveMutationOutcome, KernelOwnedAllocation)>, FsError> {
        match state.observe_publication(live_publication_observation(publication)).map_err(FsError::LiveRetry)? {
            LiveRetryAction::Complete(outcome) => Ok(Some((
                outcome, std::mem::replace(empty_allocation, KernelOwnedAllocation::Standalone),
            ))),
            LiveRetryAction::Publish => Err(FsError::LiveRetry(LiveRetryError::InvalidTransition)),
            LiveRetryAction::Rebase => {
                let (decision, allocation) = self.rebase_head_with_accounting(
                    maximum_conflicts, accounting, cancellation,
                ).await?;
                match state.observe_rebase(decision).map_err(FsError::LiveRetry)? {
                    LiveRetryAction::Publish => { drop(allocation); Ok(None) }
                    LiveRetryAction::Complete(outcome) => Ok(Some((outcome, allocation))),
                    LiveRetryAction::Rebase => Err(FsError::LiveRetry(LiveRetryError::InvalidTransition)),
                }
            }
        }
    }
}

pub(super) const fn live_publication_observation(
    outcome: CheckoutCommitOutcome,
) -> LivePublicationObservation {
    match outcome {
        CheckoutCommitOutcome::Committed {
            generation_id,
            head,
        } => LivePublicationObservation::Committed {
            generation_id,
            head,
        },
        CheckoutCommitOutcome::AlreadyCommitted {
            generation_id,
            head,
        } => LivePublicationObservation::AlreadyCommitted {
            generation_id,
            head,
        },
        CheckoutCommitOutcome::Conflict { actual } => {
            LivePublicationObservation::Conflict { actual }
        }
        CheckoutCommitOutcome::Fenced { actual_epoch } => {
            LivePublicationObservation::Fenced { actual_epoch }
        }
        CheckoutCommitOutcome::IdempotencyConflict {
            committed_fingerprint,
        } => LivePublicationObservation::IdempotencyConflict {
            committed_fingerprint,
        },
    }
}

