use super::*;

pub(super) fn execute_bisect(
    state: &mut GitCompatState,
    branch: &GitBranch,
    workspace: GitTreeRef,
    arguments: &[String],
) -> Result<GitCommandOutput, GitCompatStateError> {
    let subcommand = arguments.first().map_or("start", String::as_str);
    match subcommand {
        "start" => {
            if state.bisect.is_some() {
                return Err(GitCompatStateError::Bisect(
                    "a bisect session is already active".to_owned(),
                ));
            }
            let bad = arguments
                .get(1)
                .map(|value| resolve_object(state, Some(&GitObjectName(value.clone()))))
                .transpose()?
                .or(branch.head)
                .ok_or(GitCompatStateError::UnbornHead)?;
            let good = arguments
                .get(2)
                .map(|value| resolve_object(state, Some(&GitObjectName(value.clone()))))
                .transpose()?;
            let bad_record = state
                .commits
                .get(&bad)
                .ok_or(GitCompatStateError::Invalid)?;
            if bad_record.workspace_tree != workspace {
                return Err(GitCompatStateError::Bisect(
                    "the working copy must match the bad commit before bisect starts".to_owned(),
                ));
            }
            let session = GitBisectState {
                original_branch: state.current_branch.clone(),
                original_head: branch.head.ok_or(GitCompatStateError::UnbornHead)?,
                good,
                bad,
                current: Some(bad),
                skipped: BTreeSet::new(),
            };
            advance_bisect(state, branch, session)
        }
        "good" | "bad" | "skip" => {
            let mut session = state.bisect.clone().ok_or_else(|| {
                GitCompatStateError::Bisect("start a bisect session first".to_owned())
            })?;
            let marked = arguments
                .get(1)
                .map(|value| resolve_object(state, Some(&GitObjectName(value.clone()))))
                .transpose()?
                .or(session.current)
                .ok_or_else(|| {
                    GitCompatStateError::Bisect("no current commit is selected".to_owned())
                })?;
            match subcommand {
                "good" => session.good = Some(marked),
                "bad" => session.bad = marked,
                "skip" => {
                    session.skipped.insert(marked);
                }
                _ => unreachable!(),
            }
            advance_bisect(state, branch, session)
        }
        "reset" => {
            let session = state.bisect.clone().ok_or_else(|| {
                GitCompatStateError::Bisect("no bisect session is active".to_owned())
            })?;
            let result = GitBisectResult {
                active: false,
                good: session.good,
                bad: Some(session.bad),
                current: None,
                remaining: 0,
                first_bad: None,
            };
            prepare_bisect_checkout(state, branch, session.original_head, None, result)
        }
        "log" | "visualize" => Ok(GitCommandOutput::Bisect(bisect_result(
            state,
            state.bisect.as_ref(),
        )?)),
        other => Err(GitCompatStateError::Bisect(format!(
            "unsupported bisect operation '{other}'"
        ))),
    }
}

pub(super) fn advance_bisect(
    state: &mut GitCompatState,
    branch: &GitBranch,
    mut session: GitBisectState,
) -> Result<GitCommandOutput, GitCompatStateError> {
    let Some(good) = session.good else {
        session.current = Some(session.bad);
        let result = bisect_result(state, Some(&session))?;
        state.bisect = Some(session);
        return Ok(GitCommandOutput::Bisect(result));
    };
    let chain = first_parent_range(state, session.bad, good)?;
    let candidates: Vec<_> = chain
        .iter()
        .copied()
        .skip(1)
        .take(chain.len().saturating_sub(2))
        .filter(|commit| !session.skipped.contains(commit))
        .collect();
    if candidates.is_empty() {
        session.current = None;
        let result = GitBisectResult {
            active: true,
            good: Some(good),
            bad: Some(session.bad),
            current: None,
            remaining: 0,
            first_bad: Some(session.bad),
        };
        state.bisect = Some(session);
        return Ok(GitCommandOutput::Bisect(result));
    }
    let candidate = *candidates
        .get(candidates.len() / 2)
        .ok_or_else(|| GitCompatStateError::Bisect("no bisection candidate".to_owned()))?;
    session.current = Some(candidate);
    let result = GitBisectResult {
        active: true,
        good: Some(good),
        bad: Some(session.bad),
        current: Some(candidate),
        remaining: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
        first_bad: None,
    };
    prepare_bisect_checkout(state, branch, candidate, Some(session), result)
}

pub(super) fn prepare_bisect_checkout(
    state: &mut GitCompatState,
    branch: &GitBranch,
    commit: GitCommitId,
    replacement: Option<GitBisectState>,
    result: GitBisectResult,
) -> Result<GitCommandOutput, GitCompatStateError> {
    let record = state
        .commits
        .get(&commit)
        .ok_or(GitCompatStateError::Invalid)?;
    let paths = branch
        .tracked_paths
        .union(&record.tracked_paths)
        .cloned()
        .collect();
    Ok(prepare_transition(
        state,
        GitFilesystemAction::RestoreGeneration {
            tree: record.tree,
            paths: Some(paths),
        },
        GitPendingMutation::Bisect {
            state: Box::new(replacement),
            result,
        },
    ))
}

pub(super) fn bisect_result(
    state: &GitCompatState,
    session: Option<&GitBisectState>,
) -> Result<GitBisectResult, GitCompatStateError> {
    let Some(session) = session else {
        return Ok(GitBisectResult {
            active: false,
            good: None,
            bad: None,
            current: None,
            remaining: 0,
            first_bad: None,
        });
    };
    let remaining = match session.good {
        Some(good) => first_parent_range(state, session.bad, good)?
            .into_iter()
            .skip(1)
            .filter(|commit| *commit != good && !session.skipped.contains(commit))
            .count(),
        None => 0,
    };
    Ok(GitBisectResult {
        active: true,
        good: session.good,
        bad: Some(session.bad),
        current: session.current,
        remaining: u32::try_from(remaining).unwrap_or(u32::MAX),
        first_bad: session.good.filter(|_| remaining == 0).map(|_| session.bad),
    })
}
