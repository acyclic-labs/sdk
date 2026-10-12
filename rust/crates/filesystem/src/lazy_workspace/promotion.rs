use super::*;


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    /// Promotes one demanded node under an authority-side publication permit.
    pub async fn promote_with_permit(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.promote_with_permit_measured(
            path,
            maximum_bytes,
            idempotency_key,
            permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
    }

    async fn promote_with_permit_measured(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        let requested = self.canonical_path(path)?;
        let lookup = self
            .lookup_measured(&requested, budget, cancellation)
            .await?;
        self.promote_resolved_with_permit_measured(
            &requested,
            lookup.value,
            maximum_bytes,
            idempotency_key,
            permit,
            lookup.work,
            budget,
            cancellation,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "carries the resolved lookup, byte bound, idempotency key, permit, work, budget, and cancellation of one promotion"
    )]
    async fn promote_resolved_with_permit_measured(
        &self,
        requested: &str,
        lookup: ResolvedLazyLookup,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<TransactionCommit<A, O>>, LazyWorkspaceError> {
        work.verify(budget)?;
        let transaction = self
            .workspace
            .begin_transaction_measured(
                idempotency_key,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await
            .map_err(workspace_error)?;
        work = account_work(work, transaction.work, budget)?;
        let mut transaction = transaction.value;
        if let Some(parent) = parent_path(requested) {
            let applied = transaction
                .create_dir_all_measured(parent, remaining_work(work, budget)?, cancellation)
                .await
                .map_err(workspace_error)?;
            work = account_work(work, applied, budget)?;
        }
        let applied = self
            .apply_resolved_promotion_measured(
                &mut transaction,
                requested,
                lookup,
                maximum_bytes,
                remaining_work(work, budget)?,
                cancellation,
            )
            .await?;
        work = account_work(work, applied.work, budget)?;
        if !applied.value {
            return self.existing_promotion(work, budget, cancellation).await;
        }
        self.commit_promotion(transaction, permit, work, budget, cancellation)
            .await
    }

    /// Applies one already-resolved source fact to an unpublished candidate.
    /// Callers choose the publication boundary; the source semantics stay here.
    async fn apply_resolved_promotion_measured(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        lookup: ResolvedLazyLookup,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<bool>, LazyWorkspaceError> {
        in_heap(move || async move {
            let mut work = WorkCounters::default();
            let ResolvedLazyLookup { lookup, source } = lookup;
            let pinned = match lookup {
                LazyLookup::Source(node) => PinnedSourceNode {
                    source: source.ok_or(LazyWorkspaceError::Concurrent)?,
                    node,
                },
                LazyLookup::Authored {
                    path: authored_path,
                    ..
                } => {
                    if authored_path == requested {
                        return Ok(OperationReceipt { value: false, work });
                    }
                    let applied = transaction
                        .hard_link_measured(
                            &authored_path,
                            requested,
                            remaining_work(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(workspace_error)?;
                    work = account_work(work, applied, budget)?;
                    return Ok(OperationReceipt { value: true, work });
                }
                LazyLookup::Shadow { record, .. } => {
                    let applied = transaction
                        .restore_record_measured(
                            requested,
                            record,
                            remaining_work(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(workspace_error)?;
                    work = account_work(work, applied, budget)?;
                    return Ok(OperationReceipt { value: true, work });
                }
            };
            let populated = self
                .populate_source_node_measured(
                    transaction,
                    requested,
                    &pinned,
                    maximum_bytes,
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await?;
            work = account_work(work, populated, budget)?;
            Ok(OperationReceipt { value: true, work })
        })
        .await
    }

    /// Promotes one source node and accepts only a durable successful outcome.
    ///
    /// Mount adapters use this boundary so a typed conflict can never advance
    /// their authored view as though promotion succeeded.
    pub async fn promote_exact(
        &self,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let requested = self.canonical_path(path)?;
        let lookup = self.lookup(&requested).await?;
        if self.direct_file_id_for_lookup(&lookup) != expected_source {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        if let LazyLookup::Authored { path, .. } = lookup
            && path == requested
        {
            return self.workspace.head().await.map_err(workspace_error);
        }
        match self
            .promote(&requested, maximum_bytes, idempotency_key)
            .await?
        {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            TransactionCommit::Conflict { .. } | TransactionCommit::Fenced => {
                Err(LazyWorkspaceError::StaleIdentity)
            }
            TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
        }
    }

    /// Promotes one pinned source fact into an unpublished mount candidate.
    /// The caller owns the only eventual workspace publication; promotion
    /// itself must never advance HEAD ahead of dirty mounted mutations.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn stage_exact_into_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &str,
        expected_source: FileId,
        maximum_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<bool, LazyWorkspaceError> {
        in_heap(move || async move {
            let requested = self.canonical_path(path)?;
            let lookup = self
                .lookup_measured(&requested, WorkBudget::UNBOUNDED, cancellation)
                .await?
                .value;
            let actual = self.direct_file_id_for_lookup(&lookup.lookup);
            if actual != expected_source {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let candidate_path = self.namespace_path(&requested)?;
            if let Some(record) = checkout
                .lookup_no_follow(&candidate_path, WorkBudget::UNBOUNDED, cancellation)
                .await
                .map_err(|failure| workspace_error(failure.error.into()))?
                .value
                .record
            {
                return (record.file_id == expected_source)
                    .then_some(false)
                    .ok_or(LazyWorkspaceError::StaleIdentity);
            }
            if matches!(
                &lookup.lookup,
                LazyLookup::Authored { path, .. } if path == &requested
            ) {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let existing_identity = if matches!(
                &lookup.lookup,
                LazyLookup::Source(node) if node.kind != SourceNodeKind::Directory
            ) {
                match checkout
                    .read_file_record_by_id(expected_source, WorkBudget::UNBOUNDED, cancellation)
                    .await
                {
                    Ok(receipt) => Some(receipt.value),
                    Err(failure) if matches!(failure.error, crate::FsError::NotFound) => None,
                    Err(failure) => {
                        return Err(LazyWorkspaceError::Workspace(failure.error.to_string()));
                    }
                }
            } else {
                None
            };
            let mut transaction = crate::Transaction::for_checkout_candidate(
                &self.workspace,
                checkout.private_candidate(),
            );
            if let Some(parent) = parent_path(&requested) {
                transaction
                    .create_dir_all_measured(parent, WorkBudget::UNBOUNDED, cancellation)
                    .await
                    .map_err(workspace_error)?;
            }
            if let Some(record) = existing_identity {
                transaction
                    .restore_record_measured(
                        &requested,
                        record,
                        WorkBudget::UNBOUNDED,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                *checkout = transaction.into_checkout_candidate();
                return Ok(true);
            }
            let changed = self
                .apply_resolved_promotion_measured(
                    &mut transaction,
                    &requested,
                    lookup,
                    maximum_bytes,
                    WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await?
                .value;
            if changed {
                *checkout = transaction.into_checkout_candidate();
            }
            Ok(changed)
        })
        .await
    }
}
