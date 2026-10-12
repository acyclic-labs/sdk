use super::*;

pub(super) fn prepare_transition(
    state: &mut GitCompatState,
    action: GitFilesystemAction,
    mutation: GitPendingMutation,
) -> GitCommandOutput {
    let transition = GitTransitionId::new();
    state.pending = Some(GitPendingTransition {
        id: transition,
        action: action.clone(),
        mutation,
    });
    GitCommandOutput::Prepared { transition, action }
}

pub(super) fn register_branch_workspace_state<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    branch: String,
    workspace_id: WorkspaceId,
    head: Option<GitCommitId>,
    switch: bool,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    if let Some(existing) = state.branches.get(&branch) {
        if existing.workspace_id != workspace_id || existing.head != head {
            return Err(GitCompatError::BranchExists(branch));
        }
        if switch {
            state.current_branch = branch;
        }
        return Ok(GitCommandOutput::NoOp);
    }
    let tracked_paths = head
        .and_then(|head| state.commits.get(&head))
        .map(|commit| commit.tracked_paths.clone())
        .unwrap_or_default();
    state.branches.insert(
        branch.clone(),
        GitBranch {
            name: branch.clone(),
            workspace_id,
            head,
            tracked_paths,
        },
    );
    if switch {
        state.current_branch = branch;
    }
    Ok(GitCommandOutput::NoOp)
}

pub(super) fn record_captured_commit_state<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    commit: GitCommit,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let current = state
        .current()
        .map_err(|_| GitCompatError::InvalidState)?
        .clone();
    if commit.parents.as_slice() != current.head.as_slice() {
        return Err(GitCompatError::InvalidState);
    }
    let head_tree = current
        .head
        .and_then(|id| state.commits.get(&id).map(|commit| commit.tree));
    if head_tree == Some(commit.tree) {
        return Err(GitCompatError::NothingToCommit);
    }
    if !capture_proof_valid(
        commit.tree,
        commit.workspace_tree,
        commit.capture_proof.as_ref(),
        head_tree,
    ) {
        return Err(GitCompatError::WorkspaceMismatch);
    }
    state.reflog.insert(0, current.head);
    state.commits.insert(commit.id, commit.clone());
    let current = state
        .current_mut()
        .map_err(|_| GitCompatError::InvalidState)?;
    current.head = Some(commit.id);
    current.tracked_paths = commit.tracked_paths.clone();
    Ok(GitCommandOutput::Committed(commit))
}

#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive match settles every pending mutation kind against its filesystem result"
)]
pub(super) fn complete_pending<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    mutation: GitPendingMutation,
    result: &GitFilesystemResult,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let resulting_tree = result.resulting_tree();
    match mutation {
        GitPendingMutation::NoOp => Ok(GitCommandOutput::NoOp),
        mutation @ (GitPendingMutation::CaptureCommit { .. }
        | GitPendingMutation::ForkBranch { .. }) => {
            complete_creation_pending(state, mutation, result)
        }
        GitPendingMutation::Switch { branch } => {
            if !state.branches.contains_key(&branch) {
                return Err(GitCompatError::InvalidState);
            }
            state.current_branch = branch;
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::Reset { head } => {
            let previous = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .head;
            state.reflog.insert(0, previous);
            let tracked_paths = state
                .commits
                .get(&head)
                .ok_or(GitCompatError::InvalidState)?
                .tracked_paths
                .clone();
            let branch = state
                .current_mut()
                .map_err(|_| GitCompatError::InvalidState)?;
            branch.head = Some(head);
            branch.tracked_paths = tracked_paths;
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::StashPush { tree } => {
            state.stash.push(tree);
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::StashPop { tree } => {
            if state.stash.last() != Some(&tree) {
                return Err(GitCompatError::InvalidState);
            }
            state.stash.pop();
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::Join {
            source_head,
            source_branch,
            rebase,
        } => {
            let tree = resulting_tree.ok_or(GitCompatError::MissingResultGeneration)?;
            let tracked_paths = match result {
                GitFilesystemResult::Applied {
                    tracked_paths: Some(paths),
                    ..
                } => paths.clone(),
                _ => return Err(GitCompatError::InvalidState),
            };
            let previous = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .head;
            let parents = if rebase {
                source_head.into_iter().collect()
            } else {
                previous.into_iter().chain(source_head).collect()
            };
            let verb = if rebase { "rebase" } else { "merge" };
            record_generated_commit(
                state,
                tree,
                parents,
                tracked_paths,
                format!("{verb} {source_branch}"),
                previous,
            )
        }
        GitPendingMutation::ApplyCommit { commit, reverse } => {
            let tree = resulting_tree.ok_or(GitCompatError::MissingResultGeneration)?;
            let tracked_paths = match result {
                GitFilesystemResult::Applied {
                    tracked_paths: Some(paths),
                    ..
                } => paths.clone(),
                _ => return Err(GitCompatError::InvalidState),
            };
            let previous = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .head;
            let verb = if reverse { "revert" } else { "cherry-pick" };
            record_generated_commit(
                state,
                tree,
                previous.into_iter().collect(),
                tracked_paths,
                format!("{verb} {}", commit.to_hex()),
                previous,
            )
        }
        GitPendingMutation::Bisect {
            state: replacement,
            result,
        } => {
            state.bisect = *replacement;
            Ok(GitCommandOutput::Bisect(result))
        }
    }
}

pub(super) fn complete_creation_pending<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    mutation: GitPendingMutation,
    result: &GitFilesystemResult,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    match (mutation, result) {
        (
            GitPendingMutation::CaptureCommit {
                workspace_tree,
                message,
                author,
                authored_at_seconds,
                expected_head,
            },
            GitFilesystemResult::Captured {
                tree,
                tracked_paths,
                proof,
            },
        ) => record_captured_commit_state(
            state,
            GitCommit::new(
                *tree,
                workspace_tree,
                proof.clone(),
                tracked_paths.clone(),
                expected_head.into_iter().collect(),
                author,
                authored_at_seconds,
                message,
            ),
        ),
        (
            GitPendingMutation::ForkBranch {
                branch,
                head,
                switch,
            },
            GitFilesystemResult::Forked { workspace_id },
        ) => register_branch_workspace_state(state, branch, *workspace_id, head, switch),
        _ => Err(GitCompatError::InvalidState),
    }
}

pub(super) fn record_generated_commit<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    tree: GitTreeRef,
    parents: Vec<GitCommitId>,
    tracked_paths: BTreeSet<String>,
    message: String,
    previous: Option<GitCommitId>,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let commit = GitCommit::new(
        tree,
        tree,
        None,
        tracked_paths.clone(),
        parents,
        "git-compat",
        0,
        message,
    );
    state.reflog.insert(0, previous);
    state.commits.insert(commit.id, commit.clone());
    let branch = state
        .current_mut()
        .map_err(|_| GitCompatError::InvalidState)?;
    branch.head = Some(commit.id);
    branch.tracked_paths = tracked_paths;
    Ok(GitCommandOutput::Committed(commit))
}
