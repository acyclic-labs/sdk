use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> JoinPlan<A, O> {
    /// Applies exact resolutions on the original invocation and publication seal.
    pub async fn apply_with_native_work<'original, F, Fut, E>(
        &self,
        options: ApplyOptions,
        resolutions: &[(MergeConflict, crate::kernel::MergeConflictResolution)],
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        self.apply_resolutions_with_native_work(
            options,
            crate::facade::WorkspaceJoinResolutions::Slice(resolutions),
            permit,
            original,
            cancellation,
            before_publication,
        )
        .await
    }

    /// Validates a declarative candidate and applies it under the original seal.
    pub async fn apply_candidate_with_native_work<F, Fut, E>(
        &self,
        options: ApplyOptions,
        candidate: &UnpublishedMergeCandidate,
        permit: crate::PublicationPermit,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + From<DrivenJoinError> + Send + 'static,
    {
        let resolutions = self.prepare_candidate(candidate).await.map_err(E::from)?;
        self.apply_resolutions_with_native_work(
            options,
            crate::facade::WorkspaceJoinResolutions::Map(&resolutions),
            permit,
            original,
            cancellation,
            before_publication,
        )
        .await
    }

    async fn apply_resolutions_with_native_work<F, Fut, E>(
        &self,
        options: ApplyOptions,
        resolutions: crate::facade::WorkspaceJoinResolutions<'_>,
        permit: crate::PublicationPermit,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        if options.if_target != self.target_head.id {
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(original, cancellation);
            let (id, _) = self
                .target_head
                .workspace
                .volume
                .fs
                .workspace_head_state(
                    &self.target_head.workspace.volume,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            return Ok((
                JoinOutcome::StaleTarget(Generation {
                    workspace: self.target_head.workspace.clone(),
                    id,
                    publication_closure: None,
                }),
                original
                    .claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(FsError::from)?,
            ));
        }
        let (outcome, allocation) = self
            .target_head
            .workspace
            .volume
            .fs
            .apply_workspace_join_guarded_with_native_work(
                crate::facade::WorkspaceJoinRequest {
                    target: &self.target_head.workspace.volume,
                    base: self.base,
                    source: &self.source_head,
                    expected_target: options.if_target,
                    expected_head: self.target_authority_head,
                    history: self.history,
                    operation_id: options.idempotency_key.operation_id(),
                    permit,
                    maximum_generations: self.maximum_generations,
                    maximum_changes: self.maximum_changes,
                    maximum_conflicts: self.maximum_conflicts,
                    resolutions,
                },
                original,
                cancellation,
                before_publication,
            )
            .await?;
        self.map_join_outcome(outcome)
            .map(|outcome| (outcome, allocation))
            .map_err(E::from)
    }
}
