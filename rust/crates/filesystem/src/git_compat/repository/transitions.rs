use super::*;

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
        self.update(|state| {
            let pending = state
                .pending
                .take()
                .filter(|pending| pending.id == transition)
                .ok_or(GitCompatError::StaleTransition)?;
            validate_completion_result(state, &pending, result)?;
            match complete_pending(state, pending.mutation, result) {
                Ok(output) => Ok(Some(output)),
                Err(GitCompatError::NothingToCommit) => Ok(None),
                Err(error) => Err(error),
            }
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
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let branch = branch.into();
        self.update(|state| {
            register_branch_workspace_state(state, branch.clone(), workspace_id, head, switch)
        })
        .await
    }
}
