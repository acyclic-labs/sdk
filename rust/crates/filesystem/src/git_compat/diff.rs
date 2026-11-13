use super::*;

/// Exact Git-visible change counts without creating a compatibility snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitDiffCounts {
    /// Changed eligible file records, counted once across hard-link aliases.
    pub file_changes: usize,
    /// Changed eligible namespace bindings.
    pub binding_changes: usize,
}

/// A Git-visible path delta between two exact generations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitPathChange {
    /// Portable repository-relative path.
    pub path: String,
    /// Whether the path was added, deleted or modified.
    pub status: GitPathStatus,
}

/// The binding and record change at a path.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitPathStatus {
    /// The path is absent from the source.
    Added,
    /// The path is absent from the destination.
    Deleted,
    /// Both sides bind the path and their Git-visible values differ.
    Modified,
}

/// Exact counts and the paths they describe, under one ignore-policy decision.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitDiff {
    /// Identity-aware file and binding counts.
    pub counts: GitDiffCounts,
    /// Changed paths in portable path order.
    pub paths: Vec<GitPathChange>,
}

/// Compares exact generations using the ignore policy belonging to each side.
///
/// `repository` is an optional lower-precedence ignore policy applied beneath
/// each side's own `.gitignore` files.
pub async fn git_compatible_diff_counts<A, O>(
    from: &Generation<A, O>,
    to: &Generation<A, O>,
    repository: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    maximum_changes: impl Into<Option<u32>>,
    cancellation: &CancellationToken,
) -> Result<GitDiffCounts, GitCaptureError>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    git_compatible_diff(
        from,
        to,
        repository,
        already_tracked,
        maximum_changes,
        cancellation,
    )
    .await
    .map(|diff| diff.counts)
}

/// Compares exact generations once, returning eligible paths and counts.
/// Directory records and hard-link count changes alone are not file changes.
pub async fn git_compatible_diff<A, O>(
    from: &Generation<A, O>,
    to: &Generation<A, O>,
    repository: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    maximum_changes: impl Into<Option<u32>>,
    cancellation: &CancellationToken,
) -> Result<GitDiff, GitCaptureError>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let maximum_changes = maximum_changes.into();
    let diff = from
        .diff_to(to, maximum_changes, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let paths = diff
        .changed_paths(maximum_changes, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let mut before = (BTreeMap::new(), None);
    let mut after = (BTreeMap::new(), None);
    let mut files = BTreeSet::new();
    let mut bindings = 0_usize;
    let mut changed_paths = Vec::new();
    for change in paths {
        cancellation.check().map_err(WorkspaceError::from)?;
        let path = portable_changed_path(&change.path)?;
        // Git tracks file endpoints, not directory identities. A kind change
        // therefore adds or deletes a file rather than modifying a directory.
        let before_file = change
            .before
            .filter(|record| record.kind != FileKind::Directory);
        let after_file = change
            .after
            .filter(|record| record.kind != FileKind::Directory);
        let record = after_file.or(before_file);
        let Some(record) = record else { continue };
        let (generation, (policies, root)) = if after_file.is_some() {
            (to, &mut after)
        } else {
            (from, &mut before)
        };
        let root = match root {
            Some(root) => root,
            None => root.insert(repository_root(generation, policies, repository).await?),
        };
        if !git_path_eligible_with_root_at(
            generation,
            policies,
            root,
            &path,
            false,
            already_tracked,
        )
        .await?
        {
            continue;
        }
        let comparable = |record: Option<crate::kernel::FileRecord>| {
            record.map(|mut record| {
                record.link_count = 0;
                record
            })
        };
        let file_changed = comparable(before_file) != comparable(after_file);
        let binding_changed =
            before_file.map(|record| record.file_id) != after_file.map(|record| record.file_id);
        if file_changed {
            files.insert(record.file_id);
        }
        if binding_changed {
            bindings = bindings.saturating_add(1);
        }
        if file_changed || binding_changed {
            changed_paths.push(GitPathChange {
                path,
                status: match (before_file, after_file) {
                    (None, Some(_)) => GitPathStatus::Added,
                    (Some(_), None) => GitPathStatus::Deleted,
                    _ => GitPathStatus::Modified,
                },
            });
        }
    }
    changed_paths.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(GitDiff {
        counts: GitDiffCounts {
            file_changes: files.len(),
            binding_changes: bindings,
        },
        paths: changed_paths,
    })
}

/// Lists every path whose binding or record differs between two generations.
/// Paths are ordered ancestor-first; removal applies the reverse order.
pub async fn git_restore_paths<A, O>(
    from: &Generation<A, O>,
    to: &Generation<A, O>,
    maximum_changes: impl Into<Option<u32>>,
    cancellation: &CancellationToken,
) -> Result<Vec<String>, GitCaptureError>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let maximum_changes = maximum_changes.into();
    let changes = from
        .diff_to(to, maximum_changes, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?
        .changed_paths(maximum_changes, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let mut paths = Vec::new();
    for change in changes {
        cancellation.check().map_err(WorkspaceError::from)?;
        paths.push(portable_changed_path(&change.path)?);
    }
    Ok(paths
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

/// Returns the Git-visible tracked paths after a workspace transition that
/// publishes another tree's authored changes, such as a join.
///
/// `published` is the generation the transition itself wrote over `before`,
/// and `current` is the tree being committed: the same generation for a clean
/// join, or the resolved tree when a conflicted join is continued. Every
/// non-directory path the transition added or changed becomes a candidate
/// exactly when `current`'s ignore policy admits it, the same rule a capture
/// applies to a newly authored file. Previously tracked paths are candidates
/// too. A candidate is tracked while `current` holds it as a non-directory
/// entry. Paths the target writes after the transition, such as an untracked
/// file edited while resolving a conflict, never become tracked here.
/// `repository` is the repository-level policy, as in
/// [`git_compatible_diff_counts`].
pub async fn git_tracked_paths_after<A, O>(
    before: &Generation<A, O>,
    published: &Generation<A, O>,
    current: &Generation<A, O>,
    repository: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    maximum_changes: impl Into<Option<u32>>,
    cancellation: &CancellationToken,
) -> Result<BTreeSet<String>, GitCaptureError>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let maximum_changes = maximum_changes.into();
    let changes = before
        .diff_to(
            published,
            maximum_changes,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?
        .changed_paths(maximum_changes, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let mut published_paths = BTreeSet::new();
    for change in changes {
        if change
            .after
            .is_some_and(|record| record.kind != FileKind::Directory)
        {
            published_paths.insert(portable_changed_path(&change.path)?);
        }
    }
    let mut policies = BTreeMap::new();
    let root = repository_root(current, &mut policies, repository).await?;
    let mut tracked = BTreeSet::new();
    for path in already_tracked.union(&published_paths) {
        cancellation.check().map_err(WorkspaceError::from)?;
        match current
            .stat(&format!("/{path}"), WorkBudget::UNBOUNDED, cancellation)
            .await
        {
            Ok(receipt) if receipt.value.kind != FileKind::Directory => {}
            Ok(_) => continue,
            Err(failure) if failure.error.is_absent() => continue,
            Err(failure) => return Err(failure.error.into()),
        }
        if already_tracked.contains(path)
            || git_path_eligible_with_root_at(
                current,
                &mut policies,
                &root,
                path,
                false,
                already_tracked,
            )
            .await?
        {
            tracked.insert(path.clone());
        }
    }
    Ok(tracked)
}

/// One generation's root `.gitignore` layered over the repository policy.
async fn repository_root<A, O>(
    generation: &Generation<A, O>,
    policies: &mut BTreeMap<String, GitIgnorePolicy>,
    repository: &GitIgnorePolicy,
) -> Result<GitIgnorePolicy, GitCaptureError>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    load_git_ignore_policy(generation, policies, "").await?;
    Ok(policies
        .get("")
        .map_or_else(|| repository.clone(), |root| repository.followed_by(root)))
}

pub(super) async fn git_path_eligible_with_root_at<A, O>(
    generation: &Generation<A, O>,
    policies: &mut BTreeMap<String, GitIgnorePolicy>,
    root: &GitIgnorePolicy,
    path: &str,
    is_directory: bool,
    already_tracked: &BTreeSet<String>,
) -> Result<bool, GitCaptureError>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    if already_tracked.contains(path) {
        return Ok(true);
    }
    let mut directory = String::new();
    let parent = path.rsplit_once('/').map(|(parent, _)| parent);
    for component in parent.into_iter().flat_map(|parent| parent.split('/')) {
        load_git_ignore_policy(generation, policies, &directory).await?;
        if !directory.is_empty() {
            directory.push('/');
        }
        directory.push_str(component);
        let prefix = format!("{directory}/");
        let tracked_descendant = already_tracked
            .range(prefix.clone()..)
            .next()
            .is_some_and(|candidate| candidate.starts_with(&prefix));
        if !tracked_descendant && !git_path_eligible(root, policies, &directory, true, false) {
            return Ok(false);
        }
    }
    load_git_ignore_policy(generation, policies, &directory).await?;
    Ok(git_path_eligible(root, policies, path, is_directory, false))
}
