use super::*;

pub(super) fn action_operation_id(
    workspace_id: WorkspaceId,
    workspace_tree: GitTreeRef,
    action: &GitFilesystemAction,
) -> Result<OperationId, serde_json::Error> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ACTION_DOMAIN);
    hasher.update(&workspace_id.into_bytes());
    hash_tree_ref(&mut hasher, workspace_tree);
    let encoded = serde_json::to_vec(action)?;
    hash_bytes(&mut hasher, &encoded);
    let mut operation = [0; 16];
    operation.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(operation))
}

#[allow(clippy::too_many_lines, reason = "one exhaustive dispatch over every supported Git command")]
pub(super) fn execute_command(
    state: &mut GitCompatState,
    command: GitCommand,
    workspace: GitTreeRef,
) -> Result<GitCommandOutput, GitCompatStateError> {
    let current = state.current()?.clone();
    if state.bisect.is_some()
        && !matches!(
            &command,
            GitCommand::Status
                | GitCommand::Diff { .. }
                | GitCommand::Log { .. }
                | GitCommand::Show { .. }
                | GitCommand::RevParse { .. }
                | GitCommand::SymbolicRef { .. }
                | GitCommand::MergeBase { .. }
                | GitCommand::LsFiles
                | GitCommand::CheckIgnore { .. }
                | GitCommand::Bisect { .. }
        )
    {
        return Err(GitCompatStateError::Bisect("finish with 'git bisect reset' before changing history".to_owned()));
    }
    let visible_head = state.bisect.as_ref().and_then(|bisect| bisect.current).or(current.head);
    let head_workspace_tree = visible_head.and_then(|id| state.commits.get(&id).map(|commit| commit.workspace_tree));
    let head_tree = visible_head.and_then(|id| state.commits.get(&id).map(|commit| commit.tree));
    Ok(match command {
        GitCommand::MergeContinue | GitCommand::MergeAbort => {
            return Err(GitCompatStateError::Invalid);
        }
        GitCommand::Status => GitCommandOutput::Status(GitStatus {
            branch: current.name,
            head: visible_head,
            workspace,
            dirty: match head_workspace_tree {
                Some(head) if head == workspace => GitDirtyState::Clean,
                Some(GitTreeRef::Lazy(_)) | None if matches!(workspace, GitTreeRef::Lazy(_)) => GitDirtyState::Unknown,
                _ => GitDirtyState::Dirty,
            },
            all_changes_staged: true,
        }),
        GitCommand::Diff { .. } => GitCommandOutput::Action(GitFilesystemAction::Diff {
            from: head_tree,
            to: workspace,
            tracked_paths: current.tracked_paths,
        }),
        GitCommand::Log { maximum } => GitCommandOutput::Commits(walk_commits(state, current.head, maximum)),
        GitCommand::Show { object } => {
            let id = resolve_object(state, object.as_ref())?;
            GitCommandOutput::Commits(vec![state.commits.get(&id).cloned().ok_or(GitCompatStateError::Invalid)?])
        }
        GitCommand::Add { .. } => GitCommandOutput::NoOp,
        GitCommand::Commit { message, author, authored_at_seconds } => {
            if head_workspace_tree == Some(workspace) {
                return Err(GitCompatStateError::NothingToCommit);
            }
            let action = GitFilesystemAction::CaptureCommit {
                workspace_tree: workspace,
                head_tree,
                head_workspace_tree: Box::new(head_workspace_tree),
                tracked_paths: current.tracked_paths.clone(),
                message: message.clone(),
                author: author.clone(),
                authored_at_seconds,
                expected_head: current.head,
            };
            prepare_transition(
                state,
                action,
                GitPendingMutation::CaptureCommit {
                    workspace_tree: workspace,
                    message,
                    author,
                    authored_at_seconds,
                    expected_head: current.head,
                },
            )
        }
        GitCommand::Branch { create } => {
            if let Some(name) = create {
                if state.branches.contains_key(&name) {
                    return Err(GitCompatStateError::BranchExists(name));
                }
                let action = GitFilesystemAction::ForkBranch {
                    branch: name.clone(),
                    source_tree: workspace,
                    head: current.head,
                    switch: false,
                };
                return Ok(prepare_transition(
                    state,
                    action,
                    GitPendingMutation::ForkBranch { branch: name, head: current.head, switch: false },
                ));
            }
            GitCommandOutput::Branches {
                current: state.current_branch.clone(),
                branches: state.branches.values().cloned().collect(),
            }
        }
        GitCommand::Switch { branch, create } => {
            if create && !state.branches.contains_key(&branch) {
                let action = GitFilesystemAction::ForkBranch {
                    branch: branch.clone(),
                    source_tree: workspace,
                    head: current.head,
                    switch: true,
                };
                return Ok(prepare_transition(
                    state,
                    action,
                    GitPendingMutation::ForkBranch { branch, head: current.head, switch: true },
                ));
            }
            let target = state.branches.get(&branch).ok_or_else(|| GitCompatStateError::Unknown(branch.clone()))?;
            prepare_transition(
                state,
                GitFilesystemAction::SwitchWorkspace { workspace_id: target.workspace_id },
                GitPendingMutation::Switch { branch },
            )
        }
        GitCommand::Restore { source, paths } => {
            let id = resolve_object(state, source.as_ref())?;
            let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
            GitCommandOutput::Action(GitFilesystemAction::RestorePaths {
                tree: commit.tree,
                paths: expand_pathspecs(&paths, &commit.tracked_paths),
            })
        }
        GitCommand::Reset { target, mode } => {
            let id = resolve_object(state, Some(&target))?;
            let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
            let tree = commit.tree;
            let target_tracked_paths = commit.tracked_paths.clone();
            match mode {
                GitResetMode::Soft | GitResetMode::Mixed => {
                    state.reflog.insert(0, current.head);
                    let branch = state.current_mut()?;
                    branch.head = Some(id);
                    branch.tracked_paths = target_tracked_paths;
                    GitCommandOutput::NoOp
                }
                GitResetMode::Hard => prepare_transition(
                    state,
                    GitFilesystemAction::RestoreGeneration {
                        tree,
                        paths: Some(current.tracked_paths.union(&target_tracked_paths).cloned().collect()),
                    },
                    GitPendingMutation::Reset { head: id },
                ),
            }
        }
        GitCommand::Merge { branch } => {
            let source = state.branches.get(&branch).ok_or_else(|| GitCompatStateError::Unknown(branch.clone()))?;
            let tracked_paths = current.tracked_paths.union(&source.tracked_paths).cloned().collect();
            prepare_transition(
                state,
                GitFilesystemAction::Join {
                    target_tree: workspace,
                    source_workspace: source.workspace_id,
                    rebase: false,
                    tracked_paths,
                },
                GitPendingMutation::Join { source_head: source.head, source_branch: branch, rebase: false },
            )
        }
        GitCommand::Rebase { branch } => {
            let source = state.branches.get(&branch).ok_or_else(|| GitCompatStateError::Unknown(branch.clone()))?;
            let tracked_paths = current.tracked_paths.union(&source.tracked_paths).cloned().collect();
            prepare_transition(
                state,
                GitFilesystemAction::Join {
                    target_tree: workspace,
                    source_workspace: source.workspace_id,
                    rebase: true,
                    tracked_paths,
                },
                GitPendingMutation::Join { source_head: source.head, source_branch: branch, rebase: true },
            )
        }
        GitCommand::StashPush => {
            let tree = head_workspace_tree.ok_or(GitCompatStateError::UnbornHead)?;
            prepare_transition(
                state,
                GitFilesystemAction::RestoreGeneration { tree, paths: None },
                GitPendingMutation::StashPush { tree: workspace },
            )
        }
        GitCommand::StashPop => {
            let tree = state.stash.last().copied().ok_or(GitCompatStateError::EmptyStash)?;
            prepare_transition(
                state,
                GitFilesystemAction::RestoreGeneration { tree, paths: None },
                GitPendingMutation::StashPop { tree },
            )
        }
        GitCommand::CherryPick { object } => {
            let commit = resolve_object(state, Some(&object))?;
            let record = state.commits.get(&commit).ok_or(GitCompatStateError::Invalid)?;
            let parent = record.parents.first().and_then(|parent| state.commits.get(parent));
            let base = parent.map(|commit| commit_generation_ref(commit, current.workspace_id));
            let source = Some(commit_generation_ref(record, current.workspace_id));
            let paths = parent.map_or_else(
                || record.tracked_paths.clone(),
                |parent| parent.tracked_paths.union(&record.tracked_paths).cloned().collect(),
            );
            let tracked_paths = current.tracked_paths.union(&paths).cloned().collect();
            prepare_transition(
                state,
                GitFilesystemAction::ApplyCommit { commit, reverse: false, base, source, paths, tracked_paths },
                GitPendingMutation::ApplyCommit { commit, reverse: false },
            )
        }
        GitCommand::Revert { object } => {
            let commit = resolve_object(state, Some(&object))?;
            let record = state.commits.get(&commit).ok_or(GitCompatStateError::Invalid)?;
            let parent = record.parents.first().and_then(|parent| state.commits.get(parent));
            let base = Some(commit_generation_ref(record, current.workspace_id));
            let source = parent.map(|commit| commit_generation_ref(commit, current.workspace_id));
            let paths = parent.map_or_else(
                || record.tracked_paths.clone(),
                |parent| parent.tracked_paths.union(&record.tracked_paths).cloned().collect(),
            );
            let tracked_paths = current.tracked_paths.union(&paths).cloned().collect();
            prepare_transition(
                state,
                GitFilesystemAction::ApplyCommit { commit, reverse: true, base, source, paths, tracked_paths },
                GitPendingMutation::ApplyCommit { commit, reverse: true },
            )
        }
        GitCommand::Tag { name, target, delete } => {
            if let Some(name) = name {
                if delete {
                    state.tags.remove(&name);
                } else {
                    let id = resolve_object(state, target.as_ref())?;
                    state.tags.insert(name, id);
                }
            }
            GitCommandOutput::Tags(state.tags.clone())
        }
        GitCommand::Blame { path } => GitCommandOutput::Action(GitFilesystemAction::Blame {
            path,
            commits: walk_commits(state, current.head, u32::MAX),
        }),
        GitCommand::Grep { pattern, path } => {
            GitCommandOutput::Action(GitFilesystemAction::Grep { pattern, path, tree: workspace })
        }
        GitCommand::Clean { dry_run } => GitCommandOutput::Action(GitFilesystemAction::Clean {
            dry_run,
            tree: workspace,
            tracked_paths: current.tracked_paths,
        }),
        GitCommand::Archive { object } => {
            let snapshot = match object {
                Some(object) => {
                    let id = resolve_object(state, Some(&object))?;
                    let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
                    commit.tree
                }
                None => workspace,
            };
            GitCommandOutput::Action(GitFilesystemAction::Archive { tree: snapshot })
        }
        GitCommand::Apply { patch } => GitCommandOutput::Action(GitFilesystemAction::ApplyPatch { patch }),
        GitCommand::Bisect { arguments } => execute_bisect(state, &current, workspace, &arguments)?,
        GitCommand::RevParse { argument } => {
            let value = match argument.as_str() {
                "--is-inside-work-tree" => "true".to_owned(),
                "--abbrev-ref HEAD" => current.name,
                "--symbolic-full-name HEAD" => format!("refs/heads/{}", current.name),
                _ => resolve_object(state, Some(&GitObjectName(argument)))?.to_hex(),
            };
            GitCommandOutput::Text(value)
        }
        GitCommand::SymbolicRef { short } => {
            GitCommandOutput::Text(if short { current.name } else { format!("refs/heads/{}", current.name) })
        }
        GitCommand::MergeBase { left, right } => {
            let left = resolve_object(state, Some(&left))?;
            let right = resolve_object(state, Some(&right))?;
            let common = nearest_common_ancestor(state, left, right)
                .ok_or_else(|| GitCompatStateError::Unknown("merge base".to_owned()))?;
            GitCommandOutput::Text(common.to_hex())
        }
        GitCommand::LsFiles => GitCommandOutput::Paths(current.tracked_paths.into_iter().collect()),
        GitCommand::CheckIgnore { paths } => {
            GitCommandOutput::Action(GitFilesystemAction::CheckIgnore { paths, tree: workspace })
        }
    })
}

pub(super) fn expand_pathspecs(paths: &[String], tracked_paths: &BTreeSet<String>) -> Vec<String> {
    let mut expanded = BTreeSet::new();
    for path in paths {
        let path = path.trim_matches('/');
        let mut matched = false;
        for tracked in tracked_paths {
            if tracked == path || is_path_below(tracked, path) {
                expanded.insert(tracked.clone());
                matched = true;
            }
        }
        if !matched && !path.is_empty() {
            expanded.insert(path.to_owned());
        }
    }
    expanded.into_iter().collect()
}
