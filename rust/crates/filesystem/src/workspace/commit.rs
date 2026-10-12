use super::*;

/// Terminal result of one atomic workspace transaction publication.
pub enum TransactionCommit<A, O> {
    /// This call published the generation.
    Committed(Generation<A, O>),
    /// The same key and transaction were already durable.
    AlreadyCommitted(Generation<A, O>),
    /// Another writer changed an observed dependency first.
    Conflict {
        /// Actual immutable workspace head.
        actual: Generation<A, O>,
    },
    /// This writer was superseded by a newer authority epoch.
    Fenced,
    /// The key was previously bound to different transaction input.
    IdempotencyConflict,
}

/// Observation-safe advancement of one retained transaction candidate.
pub enum TransactionRebase<A, O> {
    /// The current head is now the candidate's immutable base and its sparse
    Rebased(Generation<A, O>),
    /// One or more exact observed or mutated regions changed upstream.
    Conflicted {
        /// Stable region-specific conflicts bounded by the caller.
        conflicts: Vec<crate::kernel::RebaseConflict>,
        /// Additional conflicts exceeded the retained result bound.
        truncated: bool,
    },
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Publishes this private candidate under the supplied limits and seal.
    pub async fn commit<E, F, Fut>(
        &mut self, context: Option<(crate::Digest, acyclic_stream::OwnedBytes, crate::PublicationPermit)>,
        permit: crate::PublicationPermit, budget: WorkBudget, cancellation: &CancellationToken, mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, E>
    where
        E: From<WorkspaceError> + From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<crate::OperationReceipt<()>, E>> + ProviderTask,
    {
        let (context, evidence, fingerprint_permit) = match context {
            Some((context, evidence, permit)) => (Some(context), Some(evidence), Some(permit)),
            None => (None, None, None),
        };
        crate::heap_future::in_heap_measured_with_error(move |budget| async move {
            let (outcome, mut work) = if self.requires_rebase {
                let receipt = self.checkout.retry_stale_commit(self.idempotency_key.operation_id(), permit, budget, cancellation, &mut before_publication, None, context, evidence, fingerprint_permit).await?;
                (receipt.value, receipt.work)
            } else {
                let receipt = self.checkout.commit(self.idempotency_key.operation_id(), context, evidence, fingerprint_permit, permit, budget, cancellation, &mut before_publication).await?;
                (Some(receipt.value), receipt.work)
            };
            let value = match outcome.and_then(|outcome| self.commit_value(outcome)) {
                Some(value) => value,
                None => {
                    let remaining = work.remaining(budget).map_err(|error| crate::OperationFailure::new(E::from(WorkspaceError::from(error)), work))?;
                    let actual = self.workspace.head(remaining, cancellation).await
                        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity).map_error(E::from))?;
                    work = work.checked_add(actual.work).map_err(|error| crate::OperationFailure::new(E::from(WorkspaceError::from(error)), work))?;
                    TransactionCommit::Conflict { actual: actual.value }
                }
            };
            Ok(crate::OperationReceipt { value, work })
        }, budget, cancellation, E::from(WorkspaceError::Cancelled(crate::CancellationError)), None, |error| E::from(WorkspaceError::from(error))).await
    }

    fn commit_value(&self, outcome: CheckoutCommitOutcome) -> Option<TransactionCommit<A, O>> {
        let generation = |id| Generation { workspace: self.workspace.clone(), id, publication_closure: self.checkout.publication_closure(id) };
        Some(match outcome {
            CheckoutCommitOutcome::Committed { generation_id, .. } => TransactionCommit::Committed(generation(generation_id)),
            CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => TransactionCommit::AlreadyCommitted(generation(generation_id)),
            CheckoutCommitOutcome::Conflict { .. } => return None,
            CheckoutCommitOutcome::Fenced { .. } => TransactionCommit::Fenced,
            CheckoutCommitOutcome::IdempotencyConflict { .. } => TransactionCommit::IdempotencyConflict,
        })
    }

    /// Publishes on the original native scope with the supplied seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn commit_with_native_work<'a, E, F, Fut>(
        &mut self, context: Option<(crate::Digest, acyclic_stream::OwnedBytes, crate::PublicationPermit)>,
        permit: crate::PublicationPermit, original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken, before_publication: &mut F,
    ) -> Result<TransactionCommit<A, O>, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let (context, evidence, fingerprint_permit) = match context {
            Some((context, evidence, permit)) => (Some(context), Some(evidence), Some(permit)),
            None => (None, None, None),
        };
        let outcome = if self.requires_rebase {
            self.checkout.retry_stale_commit_with_native_work(self.idempotency_key.operation_id(), permit, original, cancellation, before_publication, None, context, evidence, fingerprint_permit).await?
        } else {
            let mut before_publication = (*before_publication).clone();
            let mode = self.checkout.mode();
            if mode.access != crate::model::AccessMode::ReadWrite || mode.mutations != crate::model::MutationMode::PrivateOverlay { return Err(FsError::MutationNotAllowed.into()); }
            Some(self.checkout.publish_pending_with_native_work(self.idempotency_key.operation_id(), context, evidence, fingerprint_permit, permit, original, cancellation, &mut before_publication, None).await?)
        };
        if let Some(value) = outcome.and_then(|outcome| self.commit_value(outcome)) { return Ok(value); }
        let actual = self.workspace.volume.checkout_with_native_work(GenerationSelector::Head, CheckoutMode::read_only_pinned(), original, cancellation).await?;
        Ok(TransactionCommit::Conflict { actual: Generation { workspace: self.workspace.clone(), id: actual.generation_id(), publication_closure: None } })
    }

    /// Advances a private candidate only when its observed dependencies remain valid.
    pub async fn rebase(&mut self, maximum_conflicts: u32) -> Result<TransactionRebase<A, O>, WorkspaceError> {
        let decision = self.checkout.rebase_head(maximum_conflicts, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(WorkspaceError::engine)?.value;
        Ok(match decision {
            crate::kernel::RebaseDecision::Safe { generation } => {
                self.requires_rebase = false;
                TransactionRebase::Rebased(Generation { workspace: self.workspace.clone(), id: generation, publication_closure: None })
            }
            crate::kernel::RebaseDecision::Conflicted { conflicts, truncated } => TransactionRebase::Conflicted { conflicts, truncated },
        })
    }
}
