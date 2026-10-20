use super::*;

/// Exact Git-visible change counts without creating a compatibility snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitDiffCounts {
    /// Changed eligible file records, counted once across hard-link aliases.
    pub file_changes: usize,
    /// Changed eligible namespace bindings.
    pub binding_changes: usize,
}

/// Compares exact generations using the ignore policy belonging to each side.
///
/// `repository` is the repository-level policy, Git's `info/exclude`: it
/// applies beneath every side's own `.gitignore` files, and the host paths it
/// reserves are never counted.
pub async fn git_compatible_diff_counts<A, O>(
    from: &Generation<A, O>,
    to: &Generation<A, O>,
    repository: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    maximum_changes: u32,
    cancellation: &CancellationToken,
) -> Result<GitDiffCounts, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
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
    for change in paths {
        cancellation.check().map_err(WorkspaceError::from)?;
        let path = portable_changed_path(&change.path)?;
        let record = change.after.or(change.before);
        let Some(record) = record else { continue };
        if record.kind == FileKind::Directory {
            continue;
        }
        let (generation, (policies, root)) = if change.after.is_some() {
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
        if comparable(change.before) != comparable(change.after) {
            files.insert(record.file_id);
        }
        if change.before.map(|record| record.file_id) != change.after.map(|record| record.file_id) {
            bindings = bindings.saturating_add(1);
        }
    }
    Ok(GitDiffCounts {
        file_changes: files.len(),
        binding_changes: bindings,
    })
}

/// Returns the Git-visible tracked paths after a workspace transition that
/// publishes another tree's authored changes, such as a join.
///
/// Previously tracked paths stay tracked while they remain non-directory
/// entries. Every other non-directory path the transition added or changed is
/// tracked exactly when the resulting generation's ignore policy admits it, the
/// same rule a capture applies to a newly authored file. `repository` is the
/// repository-level policy, as in [`git_compatible_diff_counts`]; a path it
/// reserves is never tracked. When `source` is given, a path becomes newly
/// tracked only if that joined tree holds it too, so files the target authors
/// afterwards, such as an untracked file written while resolving a conflict,
/// stay untracked.
pub async fn git_tracked_paths_after<A, O>(
    before: &Generation<A, O>,
    after: &Generation<A, O>,
    source: Option<&Generation<A, O>>,
    repository: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    maximum_changes: u32,
    cancellation: &CancellationToken,
) -> Result<BTreeSet<String>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let mut tracked = BTreeSet::new();
    for path in already_tracked {
        cancellation.check().map_err(WorkspaceError::from)?;
        if repository.reserves(path) {
            continue;
        }
        match after
            .stat(&format!("/{path}"), WorkBudget::UNBOUNDED, cancellation)
            .await
        {
            Ok(receipt) if receipt.value.kind != FileKind::Directory => {
                tracked.insert(path.clone());
            }
            Ok(_) => {}
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {}
            Err(failure) => return Err(failure.error.into()),
        }
    }
    let changes = before
        .diff_to(after, maximum_changes, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?
        .changed_paths(maximum_changes, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let mut policies = BTreeMap::new();
    let root = repository_root(after, &mut policies, repository).await?;
    for change in changes {
        cancellation.check().map_err(WorkspaceError::from)?;
        if change
            .after
            .is_none_or(|record| record.kind == FileKind::Directory)
        {
            continue;
        }
        let path = portable_changed_path(&change.path)?;
        if let Some(source) = source {
            match source
                .stat(&format!("/{path}"), WorkBudget::UNBOUNDED, cancellation)
                .await
            {
                Ok(_) => {}
                Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => continue,
                Err(failure) => return Err(failure.error.into()),
            }
        }
        if !tracked.contains(&path)
            && git_path_eligible_with_root_at(
                after,
                &mut policies,
                &root,
                &path,
                false,
                already_tracked,
            )
            .await?
        {
            tracked.insert(path);
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
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
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
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    if root.reserves(path) {
        return Ok(false);
    }
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
