use super::*;

/// Repository façade over one SDK workspace and private compatibility state.
pub struct GitCompatRepository<S> {
    pub(super) workspace_id: WorkspaceId,
    pub(super) store: S,
}

pub(super) struct GitCommitRecord {
    pub(super) expected_head: Option<GitCommitId>,
    pub(super) tree: GitTreeRef,
    pub(super) workspace_tree: GitTreeRef,
    pub(super) tracked_paths: BTreeSet<String>,
    pub(super) capture_proof: Option<GitCaptureProof>,
    pub(super) message: String,
    pub(super) author: String,
    pub(super) authored_at_seconds: i64,
}

/// Compatibility history record produced by one distributed publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitPublicationRecord {
    /// Filtered Git-visible tree.
    pub tree: GitTreeRef,
    /// Complete live workspace tree after publication.
    pub workspace_tree: GitTreeRef,
    /// Optional compatibility head from the published child.
    pub source_head: Option<GitCommitId>,
    /// Paths represented by the filtered compatibility tree.
    pub tracked_paths: BTreeSet<String>,
    /// SDK-verified origin for a filtered publication tree.
    pub capture_proof: Option<GitCaptureProof>,
    /// Merge commit message.
    pub message: String,
    /// Author identity.
    pub author: String,
    /// Unix epoch timestamp in seconds.
    pub authored_at_seconds: i64,
}

impl<S> GitCompatRepository<S> {
    /// Creates a façade. State is initialized lazily on the first command.
    #[must_use]
    pub const fn new(workspace_id: WorkspaceId, store: S) -> Self {
        Self { workspace_id, store }
    }

    /// Borrows the state adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Returns the paths tracked by the current compatibility branch.
    pub async fn tracked_paths(&self) -> Result<BTreeSet<String>, GitCompatError<S::Error>> {
        self.load()
            .await?
            .current()
            .map(|branch| branch.tracked_paths.clone())
            .map_err(|_| GitCompatError::InvalidState)
    }

    /// Returns the latest explicit compatibility commit for the current branch.
    pub async fn head(&self) -> Result<Option<GitCommitId>, GitCompatError<S::Error>> {
        self.load().await?.current().map(|branch| branch.head).map_err(|_| GitCompatError::InvalidState)
    }

    /// Executes a command and all required filesystem work through one composed
    pub async fn run<E: GitFilesystemExecutor, T: IntoGitTreeRef>(
        &self,
        command: GitCommand,
        workspace_tree: T,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let workspace_tree = workspace_tree.into_git_tree_ref(self.workspace_id);
        self.validate_workspace_tree(workspace_tree).await?;
        executor.validate().await.map_err(GitCompatRunError::Executor)?;
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
        executor.validate().await.map_err(GitCompatRunError::Executor)?;
        self.finish_output(output, workspace_tree, executor).await
    }

    pub(super) async fn continue_join<E: GitFilesystemExecutor>(
        &self,
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let pending = self
            .pending_transition()
            .await?
            .ok_or_else(|| GitCompatError::InvalidCommand("merge --continue requires a pending merge".to_owned()))?;
        if !matches!(pending.mutation, GitPendingMutation::Join { .. }) {
            return Err(GitCompatError::TransitionPending(pending.id).into());
        }
        let GitFilesystemAction::Join { tracked_paths, .. } = &pending.action else {
            return Err(GitCompatError::InvalidState.into());
        };
        executor.validate().await.map_err(GitCompatRunError::Executor)?;
        self.complete_transition_result(
            pending.id,
            &GitFilesystemResult::Applied { tree: Some(workspace_tree), tracked_paths: Some(tracked_paths.clone()) },
        )
        .await
        .map_err(Into::into)
    }

    pub(super) async fn abort_join<E: GitFilesystemExecutor>(
        &self,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let pending = self
            .pending_transition()
            .await?
            .ok_or_else(|| GitCompatError::InvalidCommand("merge --abort requires a pending merge".to_owned()))?;
        if !matches!(pending.mutation, GitPendingMutation::Join { .. }) {
            return Err(GitCompatError::TransitionPending(pending.id).into());
        }
        let GitFilesystemAction::Join { target_tree, .. } = pending.action else {
            return Err(GitCompatError::InvalidState.into());
        };
        let mut operation_bytes = Vec::with_capacity(21);
        operation_bytes.extend_from_slice(&pending.id.into_bytes());
        operation_bytes.extend_from_slice(b"abort");
        let digest = blake3::hash(&operation_bytes);
        let mut operation_id = [0_u8; 16];
        operation_id.copy_from_slice(&digest.as_bytes()[..16]);
        let result = executor
            .execute(
                OperationId::from_bytes(operation_id),
                &GitFilesystemAction::RestoreGeneration { tree: target_tree, paths: None },
            )
            .await
            .map_err(GitCompatRunError::Executor)?;
        executor.validate().await.map_err(GitCompatRunError::Executor)?;
        self.abort_transition(pending.id).await?;
        Ok(GitCommandOutput::Filesystem(result))
    }

    /// Parses argv following the `acyclic git` umbrella subcommand and executes
    pub async fn run_argv<E: GitFilesystemExecutor, T: IntoGitTreeRef>(
        &self,
        argv: &[String],
        workspace_tree: T,
        default_author: &str,
        now_seconds: i64,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let command = parse_argv(argv, default_author, now_seconds)?;
        self.run(command, workspace_tree, executor).await
    }

    /// Recovers the durable pending transition, if any, through the same
    pub async fn resume<E: GitFilesystemExecutor>(
        &self,
        executor: &E,
    ) -> Result<Option<GitCommandOutput>, GitCompatRunError<S::Error, E::Error>> {
        let Some(pending) = self.pending_transition().await? else {
            return Ok(None);
        };
        let result =
            executor.execute(pending.id.operation_id(), &pending.action).await.map_err(GitCompatRunError::Executor)?;
        executor.validate().await.map_err(GitCompatRunError::Executor)?;
        let output = self.complete_transition_result(pending.id, &result).await?;
        Ok(Some(if matches!(output, GitCommandOutput::NoOp) { GitCommandOutput::Filesystem(result) } else { output }))
    }

    pub(super) async fn finish_output<E: GitFilesystemExecutor>(
        &self,
        output: GitCommandOutput,
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let (action, operation_id, transition) = match output {
            GitCommandOutput::Action(action) => {
                let operation_id = action_operation_id(self.workspace_id, workspace_tree, &action)?;
                (action, operation_id, None)
            }
            GitCommandOutput::Prepared { transition, action } => (action, transition.operation_id(), Some(transition)),
            other => return Ok(other),
        };
        let result = executor.execute(operation_id, &action).await.map_err(GitCompatRunError::Executor)?;
        executor.validate().await.map_err(GitCompatRunError::Executor)?;
        if let Some(transition) = transition {
            let completed = self.complete_transition_result(transition, &result).await?;
            return Ok(if matches!(completed, GitCommandOutput::NoOp) {
                GitCommandOutput::Filesystem(result)
            } else {
                completed
            });
        }
        match (action, result) {
            (
                GitFilesystemAction::CaptureCommit {
                    workspace_tree,
                    message,
                    author,
                    authored_at_seconds,
                    expected_head,
                    ..
                },
                GitFilesystemResult::Captured { tree, tracked_paths, proof },
            ) => self
                .record_captured_commit(GitCommitRecord {
                    expected_head,
                    tree,
                    workspace_tree,
                    tracked_paths,
                    capture_proof: proof,
                    message,
                    author,
                    authored_at_seconds,
                })
                .await
                .map_err(Into::into),
            (
                GitFilesystemAction::ForkBranch { branch, head, switch, .. },
                GitFilesystemResult::Forked { workspace_id },
            ) => self.register_branch_workspace(branch, workspace_id, head, switch).await.map_err(Into::into),
            (GitFilesystemAction::CaptureCommit { .. }, _) | (GitFilesystemAction::ForkBranch { .. }, _) => {
                Err(GitCompatRunError::InvalidResult)
            }
            (_, result) => Ok(GitCommandOutput::Filesystem(result)),
        }
    }

    /// Executes one typed command against an exact live workspace generation.
    pub async fn execute<T: IntoGitTreeRef>(
        &self,
        command: GitCommand,
        workspace_tree: T,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let workspace_tree = workspace_tree.into_git_tree_ref(self.workspace_id);
        self.validate_workspace_tree(workspace_tree).await?;
        self.execute_validated(command, workspace_tree).await
    }

    pub(super) async fn execute_validated(
        &self,
        command: GitCommand,
        workspace_tree: GitTreeRef,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            if let Some(pending) = &state.pending {
                return Err(GitCompatError::TransitionPending(pending.id));
            }
            let before = state.clone();
            let output = execute_command(&mut state, command.clone(), workspace_tree).map_err(map_state_error)?;
            let output = match output {
                GitCommandOutput::Action(action) => prepare_transition(&mut state, action, GitPendingMutation::NoOp),
                output => output,
            };
            if state == before {
                return Ok(output);
            }
            let expected = next_revision(&mut state.revision);
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    pub(super) async fn validate_workspace_tree(
        &self,
        workspace_tree: GitTreeRef,
    ) -> Result<(), GitCompatError<S::Error>> {
        let state = self.load().await?;
        if state.current().map_err(|_| GitCompatError::InvalidState)?.workspace_id != workspace_tree.workspace_id() {
            return Err(GitCompatError::WorkspaceMismatch);
        }
        Ok(())
    }

    /// Returns the durable sequencer transition that requires recovery.
    pub async fn pending_transition(&self) -> Result<Option<GitPendingTransition>, GitCompatError<S::Error>> {
        self.load().await.map(|state| state.pending)
    }

    /// Commits a prepared transition after its filesystem action succeeds.
    pub async fn complete_transition(
        &self,
        transition: GitTransitionId,
        resulting_tree: Option<GitTreeRef>,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.complete_transition_result(
            transition,
            &GitFilesystemResult::Applied { tree: resulting_tree, tracked_paths: None },
        )
        .await
    }

    /// Commits a prepared transition using the executor's complete typed result.
    pub async fn complete_transition_result(
        &self,
        transition: GitTransitionId,
        result: &GitFilesystemResult,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let pending = state
                .pending
                .clone()
                .filter(|pending| pending.id == transition)
                .ok_or(GitCompatError::StaleTransition)?;
            validate_completion_result(&state, &pending, result)?;
            let output = match complete_pending(&mut state, pending.mutation, result) {
                Ok(output) => Some(output),
                Err(GitCompatError::NothingToCommit) => None,
                Err(error) => return Err(error),
            };
            state.pending = None;
            let expected = next_revision(&mut state.revision);
            if self.compare_and_swap_state(expected, state).await? {
                return output.ok_or(GitCompatError::NothingToCommit);
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Clears an exact prepared transition after the executor proves that no
    pub async fn abort_transition(&self, transition: GitTransitionId) -> Result<(), GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            if state.pending.as_ref().map(|pending| pending.id) != Some(transition) {
                return Err(GitCompatError::StaleTransition);
            }
            state.pending = None;
            let expected = next_revision(&mut state.revision);
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(());
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Parses and executes argv following the `acyclic git` umbrella
    pub async fn execute_argv<T: IntoGitTreeRef>(
        &self,
        argv: &[String],
        workspace_tree: T,
        default_author: &str,
        now_seconds: i64,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let command = parse_argv(argv, default_author, now_seconds)?;
        self.execute(command, workspace_tree).await
    }

    /// Completes a previously emitted [`GitFilesystemAction::ForkBranch`].
    pub async fn register_branch_workspace(
        &self,
        branch: impl Into<String>,
        workspace_id: WorkspaceId,
        head: Option<GitCommitId>,
        switch: bool,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let branch = branch.into();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let output = register_branch_workspace_state(&mut state, branch.clone(), workspace_id, head, switch)?;
            let expected = next_revision(&mut state.revision);
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Records a compatibility commit after the core executor filters newly
    pub async fn record_commit(
        &self,
        expected_head: Option<GitCommitId>,
        generation: GenerationId,
        tracked_paths: BTreeSet<String>,
        message: impl Into<String>,
        author: impl Into<String>,
        authored_at_seconds: i64,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.record_captured_commit(GitCommitRecord {
            expected_head,
            tree: GitGenerationRef { workspace_id: self.workspace_id, generation }.into(),
            workspace_tree: GitGenerationRef { workspace_id: self.workspace_id, generation }.into(),
            tracked_paths,
            capture_proof: None,
            message: message.into(),
            author: author.into(),
            authored_at_seconds,
        })
        .await
    }

    /// Records a merge/publication performed by the distributed workspace
    pub async fn record_publication(
        &self,
        publication: GitPublicationRecord,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let GitPublicationRecord {
            tree,
            workspace_tree,
            source_head,
            tracked_paths,
            capture_proof,
            message,
            author,
            authored_at_seconds,
        } = publication;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let current = state.current().map_err(|_| GitCompatError::InvalidState)?.clone();
            if workspace_tree.workspace_id() != current.workspace_id {
                return Err(GitCompatError::WorkspaceMismatch);
            }
            if current.head.and_then(|head| state.commits.get(&head)).is_some_and(|commit| commit.tree == tree) {
                return Ok(GitCommandOutput::NoOp);
            }
            let mut parents = current.head.into_iter().collect::<Vec<_>>();
            if let Some(source_head) = source_head
                && !parents.contains(&source_head)
            {
                if !state.commits.contains_key(&source_head) {
                    state.external_parents.insert(source_head);
                }
                parents.push(source_head);
            }
            let previous_tree = current.head.and_then(|id| state.commits.get(&id).map(|commit| commit.tree));
            if !capture_proof_valid(tree, workspace_tree, capture_proof.as_ref(), previous_tree) {
                return Err(GitCompatError::WorkspaceMismatch);
            }
            let commit = GitCommit::new(
                tree,
                workspace_tree,
                capture_proof.clone(),
                tracked_paths.clone(),
                parents,
                author.clone(),
                authored_at_seconds,
                message.clone(),
            );
            state.reflog.insert(0, current.head);
            state.commits.insert(commit.id, commit.clone());
            let branch = state.current_mut().map_err(|_| GitCompatError::InvalidState)?;
            branch.head = Some(commit.id);
            branch.tracked_paths = tracked_paths.clone();
            let expected = next_revision(&mut state.revision);
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(GitCommandOutput::Committed(commit));
            }
        }
        Err(GitCompatError::Contended)
    }

    pub(super) async fn record_captured_commit(
        &self,
        record: GitCommitRecord,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let GitCommitRecord {
            expected_head,
            tree,
            workspace_tree,
            tracked_paths,
            capture_proof,
            message,
            author,
            authored_at_seconds,
        } = record;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let output = record_captured_commit_state(
                &mut state,
                GitCommitRecord {
                    expected_head,
                    tree,
                    workspace_tree,
                    tracked_paths: tracked_paths.clone(),
                    capture_proof: capture_proof.clone(),
                    message: message.clone(),
                    author: author.clone(),
                    authored_at_seconds,
                },
            )?;
            let expected = next_revision(&mut state.revision);
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    pub(super) async fn load(&self) -> Result<GitCompatState, GitCompatError<S::Error>> {
        let state = self
            .store
            .load(self.workspace_id)
            .await
            .map_err(GitCompatError::Store)?
            .unwrap_or_else(|| GitCompatState::new("main", self.workspace_id));
        validate_git_state(&state).map_err(|()| GitCompatError::InvalidState)?;
        Ok(state)
    }

    pub(super) async fn compare_and_swap_state(
        &self,
        expected: u64,
        state: GitCompatState,
    ) -> Result<bool, GitCompatError<S::Error>> {
        validate_git_state(&state).map_err(|()| GitCompatError::InvalidState)?;
        self.store.compare_and_swap(self.workspace_id, expected, state).await.map_err(GitCompatError::Store)
    }
}
