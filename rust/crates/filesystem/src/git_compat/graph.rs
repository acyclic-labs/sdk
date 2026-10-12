use super::*;

pub(super) fn nearest_common_ancestor(
    state: &GitCompatState,
    left: GitCommitId,
    right: GitCommitId,
) -> Option<GitCommitId> {
    fn distances(state: &GitCompatState, start: GitCommitId) -> BTreeMap<GitCommitId, u32> {
        let mut result = BTreeMap::new();
        let mut pending = std::collections::VecDeque::from([(start, 0_u32)]);
        while let Some((commit, distance)) = pending.pop_front() {
            if result.get(&commit).is_some_and(|known| *known <= distance) {
                continue;
            }
            result.insert(commit, distance);
            if let Some(record) = state.commits.get(&commit) {
                pending.extend(
                    record
                        .parents
                        .iter()
                        .copied()
                        .map(|parent| (parent, distance.saturating_add(1))),
                );
            }
        }
        result
    }

    let left_distances = distances(state, left);
    let right_distances = distances(state, right);
    left_distances
        .iter()
        .filter_map(|(commit, left_distance)| {
            right_distances
                .get(commit)
                .map(|right_distance| (*commit, left_distance + right_distance, *left_distance))
        })
        .min_by_key(|(commit, total, left_distance)| (*total, *left_distance, *commit))
        .map(|(commit, _, _)| commit)
}

pub(super) fn resolve_object<E: std::error::Error + 'static>(
    state: &GitCompatState,
    object: Option<&str>,
) -> Result<GitCommitId, GitCompatError<E>> {
    let object = object.unwrap_or("HEAD");
    if object == "HEAD" {
        return visible_head(state).ok_or(GitCompatError::UnbornHead);
    }
    if let Some(branch) = state.branches.get(object) {
        return branch.head.ok_or(GitCompatError::UnbornHead);
    }
    if let Some(id) = state.tags.get(object) {
        return Ok(*id);
    }
    let mut matching = state
        .commits
        .keys()
        .filter(|id| id.to_hex().starts_with(object));
    match (matching.next(), matching.next()) {
        (Some(id), None) => Ok(*id),
        _ => Err(GitCompatError::UnknownObject(object.to_owned())),
    }
}

pub(super) fn visible_head(state: &GitCompatState) -> Option<GitCommitId> {
    state
        .bisect
        .as_ref()
        .and_then(|bisect| bisect.current)
        .or_else(|| state.current().ok().and_then(|branch| branch.head))
}

pub(super) fn first_parent_range<E: std::error::Error + 'static>(
    state: &GitCompatState,
    bad: GitCommitId,
    good: GitCommitId,
) -> Result<Vec<GitCommitId>, GitCompatError<E>> {
    let mut range = Vec::new();
    let mut next = Some(bad);
    let mut seen = BTreeSet::new();
    while let Some(commit) = next {
        if !seen.insert(commit) {
            return Err(GitCompatError::InvalidState);
        }
        range.push(commit);
        if commit == good {
            return Ok(range);
        }
        next = state
            .commits
            .get(&commit)
            .ok_or(GitCompatError::InvalidState)?
            .parents
            .first()
            .copied();
    }
    Err(GitCompatError::InvalidCommand(
        "the good commit is not a first-parent ancestor of the bad commit".to_owned(),
    ))
}

pub(super) fn walk_commits(
    state: &GitCompatState,
    mut next: Option<GitCommitId>,
    maximum: u32,
) -> Vec<GitCommit> {
    let mut commits = Vec::new();
    while let Some(id) = next {
        if commits.len() >= usize::try_from(maximum).unwrap_or(usize::MAX) {
            break;
        }
        let Some(commit) = state.commits.get(&id) else {
            break;
        };
        commits.push(commit.clone());
        next = commit.parents.first().copied();
    }
    commits
}
