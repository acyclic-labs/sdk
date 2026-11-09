use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Workspace<A, O> {
    /// Forks an exact generation with the supplied limits and publication guard.
    pub async fn fork<W, G, E>(
        &self,
        destination: impl AsRef<str>,
        options: ForkOptions<A, O>,
        permit: crate::PublicationPermit,
        work: W,
        cancellation: &CancellationToken,
        guard: G,
    ) -> W::Output<Self, E>
    where
        W: crate::facade::FsWork,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
        E: From<FsError> + From<WorkspaceError> + Send,
    {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .fork_accounted(
                destination.as_ref(),
                options,
                permit,
                &mut accounting,
                guard,
            )
            .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn fork_accounted<G, E>(
        &self,
        destination: &str,
        options: ForkOptions<A, O>,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
        mut guard: G,
    ) -> Result<Self, E>
    where
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
        E: From<FsError> + From<WorkspaceError> + Send,
    {
        let mut frame = None;
        let (mut future, bytes) = crate::heap_future::in_heap_admitted(
            (&mut *accounting, &mut frame),
            |(accounting, frame), bytes| {
                **frame = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
                Ok::<_, E>(())
            },
            |(accounting, _)| async move {
                let bytes = options
                    .paths
                    .as_ref()
                    .map_or(Ok(0), workspace_owned_strings_bytes)
                    .map_err(E::from)?;
                let (allocation, admitted) = accounting.record_actual_owned(bytes)?;
                admitted?;
                if options.generation.workspace.id != self.id {
                    return Err(WorkspaceError::ForeignGeneration.into());
                }
                let name =
                    WorkspaceName::with_accounting(destination, accounting).map_err(E::from)?;
                self.volume
                    .fs
                    .fork_workspace(
                        name,
                        &options.generation,
                        options.idempotency_key,
                        crate::kernel::Owned::new(options.paths, allocation, bytes),
                        permit,
                        accounting,
                        Some(&mut guard),
                    )
                    .await
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        if let Some(mut frame) = frame {
            accounting.release_owned(&mut frame, bytes)?;
        }
        result
    }

    /// Retains the current generation under a permanent label.
    pub async fn checkpoint(
        &self,
        label: impl AsRef<str>,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let cancellation = CancellationToken::new();
        let label = WorkspaceName::new(label, &WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| failure.error)?
            .value;
        let generation = self
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let text = label.as_str().to_owned();
        let bytes = text.capacity() as u64;
        self.volume
            .fs
            .retain_workspace_generation(
                &self.volume,
                generation.id,
                crate::kernel::RetentionKind::Checkpoint,
                crate::kernel::Owned::new(text, KernelOwnedAllocation::Standalone, bytes),
                None,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?;
        Ok((label, generation))
    }

    /// Durably pins the current immutable generation.
    pub async fn pin(
        &self,
        identity: impl AsRef<str>,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let cancellation = CancellationToken::new();
        self.head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
            .pin(identity, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)
            .map(|receipt| receipt.value)
    }

    /// Permanently deletes this named head using the exact retry identity.
    pub async fn delete(&self, key: IdempotencyKey) -> Result<WorkspaceDelete, WorkspaceError> {
        self.volume
            .fs
            .delete_workspace_volume(&self.volume, key.operation_id())
            .await
    }

    /// Performs the operation under the supplied limits.
    pub async fn diff(
        &self,
        from: &Generation<A, O>,
        to: &Generation<A, O>,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<ChangeSet<A, O>>, WorkspaceError> {
        if from.workspace.id != self.id
            || to.workspace.id != self.id
            || !self.volume.fs.same_deployment(&from.workspace.volume.fs)
            || !self.volume.fs.same_deployment(&to.workspace.volume.fs)
        {
            return Err(crate::OperationFailure::before_work(
                WorkspaceError::ForeignGeneration,
            ));
        }
        let receipt = from
            .diff_to(to, maximum_changes, budget, cancellation)
            .await?;
        Ok(crate::OperationReceipt {
            value: receipt.value.0,
            work: receipt.work,
        })
    }

    /// Plans a join into the target workspace.
    pub fn join_into(&self, target: &Workspace<A, O>) -> JoinBuilder<A, O> {
        JoinBuilder {
            source: self.clone(),
            target: target.clone(),
            history: JoinHistory::Merge,
            git_metadata: false,
            maximum_generations: 4_096,
            maximum_changes: self.volume.config().limits.maximum_paths_per_batch,
            maximum_conflicts: 1_024,
        }
    }

    /// Rebases this fork onto its current source using the supplied work source.
    /// Admission, cancellation and conflict backing retain that original owner.
    pub async fn live_rebase<W: crate::facade::FsWork>(
        &self,
        idempotency_key: IdempotencyKey,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceRebase<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .live_rebase_with_accounting(
                crate::facade::WorkspaceRebaseRequest {
                    target: &self.volume,
                    operation_id: idempotency_key.operation_id(),
                    maximum_generations,
                    maximum_changes,
                    maximum_conflicts,
                },
                &mut accounting,
                cancellation,
            )
            .await;
        work.finish(result, &accounting)
    }

    async fn live_rebase_with_accounting(
        &self,
        request: crate::facade::WorkspaceRebaseRequest<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceRebase<A, O>, WorkspaceError> {
        let mut scope = accounting.allocation_scope()?;
        let accounting = &mut *scope;
        let outcome = self
            .volume
            .fs
            .live_rebase_workspace(request, accounting, cancellation)
            .await?;
        let bytes = outcome.bytes;
        let allocation = super::paths::workspace_allocation(outcome.allocation, accounting)?;
        let generation = |id| Generation {
            workspace: self.clone(),
            id,
            publication_closure: None,
        };
        let result = match outcome.value {
            crate::facade::WorkspaceJoinOutcome::Joined(id, _) => {
                WorkspaceRebase::Rebased(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::AlreadyJoined(id, _) => {
                WorkspaceRebase::AlreadyRebased(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::NoChanges(id) => {
                WorkspaceRebase::Current(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Stale(id) => {
                WorkspaceRebase::Stale(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Conflicted(conflicts, truncated) => {
                WorkspaceRebase::Conflicted {
                    conflicts,
                    truncated,
                    allocation,
                }
            }
            crate::facade::WorkspaceJoinOutcome::Fenced => WorkspaceRebase::Fenced,
            crate::facade::WorkspaceJoinOutcome::IdempotencyConflict => {
                WorkspaceRebase::IdempotencyConflict
            }
        };
        scope.keep_bytes(bytes)?;
        Ok(result)
    }
}
