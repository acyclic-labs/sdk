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
pub async fn git_compatible_diff_counts<A, O>(
    from: &Generation<A, O>,
    to: &Generation<A, O>,
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
    let mut before_policies = BTreeMap::new();
    let mut after_policies = BTreeMap::new();
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
        let (generation, policies) =
            if change.after.is_some() { (to, &mut after_policies) } else { (from, &mut before_policies) };
        if !git_path_eligible_at(generation, policies, &path, already_tracked).await? {
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
    Ok(GitDiffCounts { file_changes: files.len(), binding_changes: bindings })
}

pub(super) async fn git_path_eligible_at<A, O>(
    generation: &Generation<A, O>,
    policies: &mut BTreeMap<String, GitIgnorePolicy>,
    path: &str,
    already_tracked: &BTreeSet<String>,
) -> Result<bool, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    load_git_ignore_policy(generation, policies, "").await?;
    let root = policies.get("").cloned().unwrap_or_default();
    git_path_eligible_with_root_at(generation, policies, &root, path, false, already_tracked).await
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
    if already_tracked.contains(path) {
        return Ok(true);
    }
    let mut directory = String::new();
    let components = path.split('/').collect::<Vec<_>>();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        load_git_ignore_policy(generation, policies, &directory).await?;
        if !directory.is_empty() {
            directory.push('/');
        }
        directory.push_str(component);
        let prefix = format!("{directory}/");
        let tracked_descendant =
            already_tracked.range(prefix.clone()..).next().is_some_and(|candidate| candidate.starts_with(&prefix));
        if !tracked_descendant && !git_path_eligible(root, policies, &directory, true, false) {
            return Ok(false);
        }
    }
    load_git_ignore_policy(generation, policies, &directory).await?;
    Ok(git_path_eligible(root, policies, path, is_directory, false))
}
