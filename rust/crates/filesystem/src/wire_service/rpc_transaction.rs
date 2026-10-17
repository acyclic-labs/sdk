impl<A, O> FilesystemWireService<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn rpc_apply_transaction(
        &self,
        request: Request<wire::ApplyTransactionRequest>,
    ) -> Result<Response<wire::MutationResponse>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.mutations.is_empty()
            || u32::try_from(request.mutations.len()).unwrap_or(u32::MAX)
                > self.limits.maximum_transaction_mutations
        {
            return Err(Status::invalid_argument(
                "transaction mutation count is invalid",
            ));
        }
        if request.maximum_conflicts == 0
            || request.maximum_conflicts > self.limits.maximum_page_items
        {
            return Err(Status::invalid_argument(
                "transaction conflict bound is invalid",
            ));
        }
        let base = self.generation(request.base).await?;
        let workspace = self.workspace(generation_ref(&base).workspace).await?;
        let mut transaction = workspace
            .begin_transaction_at(
                &base,
                operation(request.operation)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        for mutation in request.mutations {
            apply_mutation(&mut transaction, mutation).await?;
        }
        let first = transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        if !matches!(first, TransactionCommit::Conflict { .. }) {
            return Ok(Response::new(commit_message(first)));
        }
        match transaction
            .rebase(
                request.maximum_conflicts,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value
        {
            TransactionRebase::Rebased(_) => Ok(Response::new(commit_message(
                transaction
                    .commit(
                        None,
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                        |_| async {
                            Ok::<_, crate::OperationFailure<WorkspaceError>>(
                                crate::OperationReceipt {
                                    value: (),
                                    work: crate::WorkCounters::UNCHARGED,
                                },
                            )
                        },
                    )
                    .await
                    .map_err(|failure| status(&failure.error))?
                    .value,
            ))),
            TransactionRebase::Conflicted {
                conflicts,
                truncated,
            } => Ok(Response::new(mutation_with_conflicts(
                wire::MutationStatus::Conflict,
                conflicts.into_iter().map(transaction_conflict).collect(),
                truncated,
            ))),
        }
    }
}
