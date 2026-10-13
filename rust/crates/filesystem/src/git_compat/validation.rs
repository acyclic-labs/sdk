use super::*;

pub(super) fn validate_completion_result<E: std::error::Error + 'static>(
    state: &GitCompatState,
    pending: &GitPendingTransition,
    result: &GitFilesystemResult,
) -> Result<(), GitCompatError<E>> {
    let workspace_id = state
        .current()
        .map_err(|_| GitCompatError::InvalidState)?
        .workspace_id;
    match result {
        GitFilesystemResult::Captured { tree, proof, .. } => {
            let GitPendingMutation::CaptureCommit {
                workspace_tree,
                expected_head,
                ..
            } = &pending.mutation
            else {
                return Err(GitCompatError::WorkspaceMismatch);
            };
            let previous_tree =
                expected_head.and_then(|id| state.commits.get(&id).map(|commit| commit.tree));
            let reused_head = proof.is_none() && previous_tree == Some(*tree);
            let valid_capture =
                capture_proof_valid(*tree, *workspace_tree, proof.as_ref(), previous_tree)
                    && proof
                        .as_ref()
                        .is_none_or(|proof| proof.operation_id == pending.id.operation_id());
            if !reused_head && !valid_capture {
                return Err(GitCompatError::WorkspaceMismatch);
            }
        }
        _ if result
            .resulting_tree()
            .is_some_and(|tree| tree.workspace_id() != workspace_id) =>
        {
            return Err(GitCompatError::WorkspaceMismatch);
        }
        _ => {}
    }
    if let GitPendingMutation::CaptureCommit { workspace_tree, .. } = pending.mutation
        && workspace_tree.workspace_id() != workspace_id
    {
        return Err(GitCompatError::WorkspaceMismatch);
    }
    Ok(())
}

pub(super) fn capture_proof_valid(
    tree: GitTreeRef,
    workspace_tree: GitTreeRef,
    proof: Option<&GitCaptureProof>,
    previous_tree: Option<GitTreeRef>,
) -> bool {
    if tree.workspace_id() == workspace_tree.workspace_id() {
        return proof.is_none();
    }
    proof.is_some_and(|proof| proof.structurally_valid(tree, workspace_tree, previous_tree))
}

pub(super) fn validate_git_state(state: &GitCompatState) -> Result<(), ()> {
    if state.version != STATE_VERSION
        || state.branches.is_empty()
        || !state.branches.contains_key(&state.current_branch)
    {
        return Err(());
    }
    validate_git_branches(state)?;
    let known_workspaces: BTreeSet<_> = state
        .branches
        .values()
        .map(|branch| branch.workspace_id)
        .collect();
    validate_git_commits(state, &known_workspaces)?;
    validate_git_commit_graph(state)?;
    validate_git_references(state, &known_workspaces)
}

pub(super) fn validate_git_branches(state: &GitCompatState) -> Result<(), ()> {
    for (name, branch) in &state.branches {
        let head_paths = match branch.head {
            Some(head) => Some(&state.commits.get(&head).ok_or(())?.tracked_paths),
            None => None,
        };
        if name.is_empty()
            || branch.name != *name
            || head_paths.map_or(!branch.tracked_paths.is_empty(), |tracked_paths| {
                tracked_paths != &branch.tracked_paths
            })
        {
            return Err(());
        }
    }
    Ok(())
}

pub(super) fn validate_git_commits(
    state: &GitCompatState,
    known_workspaces: &BTreeSet<WorkspaceId>,
) -> Result<(), ()> {
    for (id, commit) in &state.commits {
        if *id != commit.id
            || commit.hash_id() != commit.id
            || !known_workspaces.contains(&commit.workspace_tree.workspace_id())
            || !capture_proof_valid(
                commit.tree,
                commit.workspace_tree,
                commit.capture_proof.as_ref(),
                commit
                    .parents
                    .first()
                    .and_then(|id| state.commits.get(id).map(|parent| parent.tree)),
            )
            || commit.parents.iter().any(|parent| {
                !state.commits.contains_key(parent) && !state.external_parents.contains(parent)
            })
        {
            return Err(());
        }
    }
    Ok(())
}

pub(super) fn validate_git_commit_graph(state: &GitCompatState) -> Result<(), ()> {
    let mut permanent = BTreeSet::new();
    for root in state.commits.keys().copied() {
        if permanent.contains(&root) {
            continue;
        }
        let mut active = BTreeSet::new();
        let mut stack = vec![(root, false)];
        while let Some((id, expanded)) = stack.pop() {
            if expanded {
                active.remove(&id);
                permanent.insert(id);
                continue;
            }
            if permanent.contains(&id) {
                continue;
            }
            if !active.insert(id) {
                return Err(());
            }
            stack.push((id, true));
            let commit = state.commits.get(&id).ok_or(())?;
            stack.extend(
                commit
                    .parents
                    .iter()
                    .rev()
                    .filter(|parent| state.commits.contains_key(parent))
                    .map(|parent| (*parent, false)),
            );
        }
    }
    Ok(())
}

pub(super) fn validate_git_references(
    state: &GitCompatState,
    known_workspaces: &BTreeSet<WorkspaceId>,
) -> Result<(), ()> {
    let known_commit = |id: &GitCommitId| state.commits.contains_key(id);
    let known_reference =
        |id: &GitCommitId| state.commits.contains_key(id) || state.external_parents.contains(id);
    if state.external_parents.iter().any(|external| {
        state.commits.contains_key(external)
            || !state
                .commits
                .values()
                .any(|commit| commit.parents.contains(external))
    }) {
        return Err(());
    }
    if state.tags.values().any(|id| !known_commit(id))
        || state.reflog.iter().flatten().any(|id| !known_commit(id))
        || state
            .stash
            .iter()
            .any(|tree| !known_workspaces.contains(&tree.workspace_id()))
        || state.bisect.as_ref().is_some_and(|bisect| {
            !known_commit(&bisect.original_head)
                || !known_commit(&bisect.bad)
                || bisect.good.as_ref().is_some_and(|id| !known_commit(id))
                || bisect.current.as_ref().is_some_and(|id| !known_commit(id))
                || bisect.skipped.iter().any(|id| !known_commit(id))
        })
    {
        return Err(());
    }
    if let Some(pending) = &state.pending {
        let valid = match &pending.mutation {
            GitPendingMutation::CaptureCommit {
                workspace_tree,
                expected_head,
                ..
            } => {
                workspace_tree.workspace_id() == state.current().map_err(|_| ())?.workspace_id
                    && expected_head.as_ref().is_none_or(known_commit)
            }
            GitPendingMutation::ForkBranch { branch, head, .. } => {
                !branch.is_empty()
                    && !state.branches.contains_key(branch)
                    && head.as_ref().is_none_or(known_commit)
            }
            GitPendingMutation::Switch { branch } => state.branches.contains_key(branch),
            GitPendingMutation::Reset { head }
            | GitPendingMutation::ApplyCommit { commit: head, .. } => known_commit(head),
            GitPendingMutation::Join { source_head, .. } => {
                source_head.as_ref().is_none_or(known_reference)
            }
            GitPendingMutation::NoOp
            | GitPendingMutation::StashPush { .. }
            | GitPendingMutation::StashPop { .. }
            | GitPendingMutation::Bisect { .. } => true,
        };
        if !valid {
            return Err(());
        }
    }
    Ok(())
}
