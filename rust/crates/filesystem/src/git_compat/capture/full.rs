use super::*;

pub(crate) async fn capture_git_compatible_generation_from<A, O>(
    live: crate::Generation<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let (mut entries, nested_policies) = scan_git_capture_entries(&live).await?;
    entries.sort_by(|left, right| {
        path_depth(&left.0)
            .cmp(&path_depth(&right.0))
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut excluded = Vec::<String>::new();
    let mut tracked_paths = BTreeSet::new();
    for (path, is_directory) in &entries {
        let parent_excluded = excluded.iter().any(|parent| is_path_below(path, parent));
        // A host-owned path is outside the repository even if an earlier
        // capture recorded it as tracked.
        let reserved = policy.reserves(path);
        let tracked = !reserved && already_tracked.contains(path);
        let tracked_descendant = !reserved
            && *is_directory
            && already_tracked
                .iter()
                .any(|candidate| is_path_below(candidate, path));
        if parent_excluded
            || reserved
            || (!git_path_eligible(policy, &nested_policies, path, *is_directory, tracked)
                && !tracked_descendant)
        {
            excluded.push(path.clone());
        } else if !is_directory {
            tracked_paths.insert(path.clone());
        }
    }
    if excluded.is_empty() {
        return Ok(GitCapturedGeneration {
            generation: live,
            tracked_paths,
            origin: None,
        });
    }
    apply_git_capture(
        &live,
        Some(&live),
        None,
        &excluded,
        tracked_paths,
        operation_id,
    )
    .await
}
