use super::*;

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Retains the exact conflicted paths in the existing pending join record.
    pub async fn record_merge_conflicts(
        &self,
        operation: OperationId,
        paths: &BTreeMap<String, Option<crate::FileId>>,
    ) -> Result<(), GitCompatError<S::Error>> {
        let mut state = self.load().await?;
        let pending = state.pending.as_mut().ok_or(GitCompatError::InvalidState)?;
        if pending.id.operation_id() != operation
            || !matches!(pending.action, GitFilesystemAction::Join { .. })
        {
            return Err(GitCompatError::InvalidState);
        }
        let GitPendingMutation::Join { conflicts, .. } = &mut pending.mutation else {
            return Err(GitCompatError::InvalidState);
        };
        if let Some(existing) = conflicts.as_ref() {
            return if existing == paths {
                Ok(())
            } else {
                Err(GitCompatError::InvalidState)
            };
        }
        *conflicts = Some(paths.clone());
        if self.publish(state).await? {
            return Ok(());
        }
        Err(GitCompatError::Contended)
    }
}

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Returns the durable transition awaiting completion or rollback.
    pub async fn pending_transition(
        &self,
    ) -> Result<Option<GitPendingTransition>, GitCompatError<S::Error>> {
        self.load().await.map(|state| state.pending)
    }

    /// Settles an exact transition from the executor's authenticated result.
    pub async fn complete_transition_result(
        &self,
        transition: GitTransitionId,
        result: &GitFilesystemResult,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.settle(transition, result, false, None).await
    }

    /// Settles `transition` and saves its result with it, under the request
    /// that started it rather than the one resuming it: the
    /// filesystem result itself, for a transition that settles to no state
    /// change, when `filesystem` is set. A request that explicitly
    /// `continues` the transition saves the result under itself too, in the
    /// same update, so its own replay finds it.
    pub(super) async fn settle(
        &self,
        transition: GitTransitionId,
        result: &GitFilesystemResult,
        filesystem: bool,
        continues: Option<OperationId>,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.update(|state| {
            let pending = state
                .pending
                .take()
                .filter(|pending| pending.id == transition)
                .ok_or(GitCompatError::StaleTransition)?;
            validate_completion_result(state, &pending, result)?;
            let request = pending.request;
            let output = match complete_pending(state, pending.mutation, result) {
                Ok(GitCommandOutput::NoOp) if filesystem => {
                    GitCommandOutput::Filesystem(result.clone())
                }
                Ok(output) => output,
                Err(GitCompatError::NothingToCommit) => return Ok(None),
                Err(error) => return Err(error),
            };
            state.record_request(request, self.unanswered(), &output);
            if continues != request {
                state.record_request(continues, self.unanswered(), &output);
            }
            Ok(Some(output))
        })
        .await?
        .ok_or(GitCompatError::NothingToCommit)
    }

    /// Clears an exact transition after its filesystem rollback.
    pub async fn abort_transition(
        &self,
        transition: GitTransitionId,
    ) -> Result<(), GitCompatError<S::Error>> {
        self.update(|state| {
            if state.pending.as_ref().map(|pending| pending.id) != Some(transition) {
                return Err(GitCompatError::StaleTransition);
            }
            state.pending = None;
            Ok(())
        })
        .await
    }

    /// Registers the workspace produced by a compatibility branch fork.
    pub async fn register_branch_workspace(
        &self,
        branch: impl Into<String>,
        workspace_id: WorkspaceId,
        head: Option<GitCommitId>,
        switch: bool,
    ) -> Result<(), GitCompatError<S::Error>> {
        let branch = branch.into();
        self.update(|state| {
            register_branch_workspace_state(state, branch, workspace_id, head, switch)
        })
        .await
    }

    /// Atomically replaces an observed branch workspace and clears the old
    /// fork's history. A concurrent replacement cannot be overwritten.
    pub async fn rebind_branch_workspace(
        &self,
        branch: impl Into<String>,
        expected: WorkspaceId,
        workspace_id: WorkspaceId,
    ) -> Result<(), GitCompatError<S::Error>> {
        let branch = branch.into();
        self.update(|state| {
            let existing = state
                .branches
                .get(&branch)
                .ok_or_else(|| GitCompatError::UnknownObject(branch.clone()))?;
            if existing.workspace_id == workspace_id {
                return Ok(());
            }
            if existing.workspace_id != expected {
                return Err(GitCompatError::Contended);
            }
            if let Some(pending) = &state.pending {
                return Err(GitCompatError::TransitionPending(pending.id));
            }
            if state.current_branch == branch {
                return Err(GitCompatError::WorkspaceMismatch);
            }
            state.branches.remove(&branch);
            register_branch_workspace_state(state, branch, workspace_id, None, false)?;
            Ok(())
        })
        .await
    }
}
