use super::*;

fn inspected_join(pending: &GitPendingTransition) -> bool {
    matches!(
        &pending.mutation,
        GitPendingMutation::Join {
            conflicts: Some(_),
            ..
        }
    )
}

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Executes a typed command and completes its filesystem transition.
    pub async fn run<E: GitFilesystemExecutor>(
        &self,
        command: GitCommand,
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        self.validate_workspace_tree(workspace_tree).await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        match &command {
            GitCommand::MergeContinue => {
                return self.continue_join(workspace_tree, executor).await;
            }
            GitCommand::MergeAbort => return self.abort_join(executor).await,
            GitCommand::Add { .. } => {
                if let Some(pending) = self.pending_transition().await? {
                    if matches!(pending.mutation, GitPendingMutation::Join { .. }) {
                        return Ok(GitCommandOutput::NoOp);
                    }
                    return Err(GitCompatError::TransitionPending(pending.id).into());
                }
            }
            _ => {}
        }
        let output = self.execute_validated(command, workspace_tree).await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        match output {
            GitCommandOutput::Prepared { transition, action } => {
                self.finish(transition, &action, executor).await
            }
            output => Ok(output),
        }
    }

    /// Parses argv and completes the requested filesystem transition.
    pub async fn run_argv<E: GitFilesystemExecutor>(
        &self,
        argv: &[String],
        workspace_tree: GitTreeRef,
        default_author: &str,
        now_seconds: i64,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let command = parse_git_argv(argv, default_author, now_seconds)?;
        self.run(command, workspace_tree, executor).await
    }

    /// Replays the durable pending transition with its original operation identity.
    /// Inspected joins require explicit merge continuation or abort.
    pub async fn resume<E: GitFilesystemExecutor>(
        &self,
        executor: &E,
    ) -> Result<Option<GitCommandOutput>, GitCompatRunError<S::Error, E::Error>> {
        match self.pending_transition().await? {
            Some(pending) if inspected_join(&pending) => Ok(None),
            Some(pending) => self
                .finish(pending.id, &pending.action, executor)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn finish<E: GitFilesystemExecutor>(
        &self,
        transition: GitTransitionId,
        action: &GitFilesystemAction,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let result = self
            .perform(transition.operation_id(), action, executor)
            .await?;
        match self.complete_transition_result(transition, &result).await? {
            GitCommandOutput::NoOp => Ok(GitCommandOutput::Filesystem(result)),
            output => Ok(output),
        }
    }

    async fn perform<E: GitFilesystemExecutor>(
        &self,
        operation: OperationId,
        action: &GitFilesystemAction,
        executor: &E,
    ) -> Result<GitFilesystemResult, GitCompatRunError<S::Error, E::Error>> {
        let result = executor
            .execute(operation, action)
            .await
            .map_err(GitCompatRunError::Executor)?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        Ok(result)
    }

    async fn continue_join<E: GitFilesystemExecutor>(
        &self,
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let pending = self.pending_join("continue").await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        let result = executor
            .validate_merge_resolution(pending.id.operation_id(), &pending.action, workspace_tree)
            .await
            .map_err(GitCompatRunError::Executor)?;
        self.complete_transition_result(pending.id, &result)
            .await
            .map_err(Into::into)
    }

    async fn abort_join<E: GitFilesystemExecutor>(
        &self,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let pending = self.pending_join("abort").await?;
        let GitFilesystemAction::Join { target_tree, .. } = pending.action else {
            return Err(GitCompatError::InvalidState.into());
        };
        let mut hash = blake3::Hasher::new();
        hash.update(&pending.id.into_bytes());
        hash.update(b"abort");
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        let operation = OperationId::from_bytes(identity);
        let result = self
            .perform(
                operation,
                &GitFilesystemAction::RestoreGeneration {
                    tree: target_tree,
                    paths: None,
                },
                executor,
            )
            .await?;
        self.abort_transition(pending.id).await?;
        Ok(GitCommandOutput::Filesystem(result))
    }

    async fn pending_join(
        &self,
        operation: &str,
    ) -> Result<GitPendingTransition, GitCompatError<S::Error>> {
        let pending = self.pending_transition().await?.ok_or_else(|| {
            GitCompatError::InvalidCommand(format!("merge --{operation} requires a pending merge"))
        })?;
        if !matches!(pending.mutation, GitPendingMutation::Join { .. }) {
            return Err(GitCompatError::TransitionPending(pending.id));
        }
        Ok(pending)
    }

    /// Prepares one typed command against the selected workspace tree.
    pub async fn execute(
        &self,
        command: GitCommand,
        workspace_tree: GitTreeRef,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.validate_workspace_tree(workspace_tree).await?;
        self.execute_validated(command, workspace_tree).await
    }

    /// Parses argv and prepares the resulting typed command.
    pub async fn execute_argv(
        &self,
        argv: &[String],
        workspace_tree: GitTreeRef,
        default_author: &str,
        now_seconds: i64,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let command = parse_git_argv(argv, default_author, now_seconds)?;
        self.execute(command, workspace_tree).await
    }

    async fn execute_validated(
        &self,
        command: GitCommand,
        workspace_tree: GitTreeRef,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            if let Some(pending) = state
                .pending
                .as_ref()
                .filter(|pending| !inspected_join(pending))
            {
                return Err(GitCompatError::TransitionPending(pending.id));
            }
            let before = state.clone();
            let output = execute_command::<S::Error>(&mut state, command.clone(), workspace_tree)?;
            if let Some(pending) = &before.pending {
                if state != before
                    || matches!(
                        &output,
                        GitCommandOutput::Action(_) | GitCommandOutput::Prepared { .. }
                    )
                {
                    return Err(GitCompatError::TransitionPending(pending.id));
                }
                return Ok(output);
            }
            let output = match output {
                GitCommandOutput::Action(action) => {
                    prepare_transition(&mut state, action, GitPendingMutation::NoOp)
                }
                output => output,
            };
            if state == before || self.publish(state).await? {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    async fn validate_workspace_tree(
        &self,
        workspace_tree: GitTreeRef,
    ) -> Result<(), GitCompatError<S::Error>> {
        let state = self.load().await?;
        if state
            .current()
            .map_err(|_| GitCompatError::InvalidState)?
            .workspace_id
            != workspace_tree.workspace_id()
        {
            return Err(GitCompatError::WorkspaceMismatch);
        }
        Ok(())
    }
}
