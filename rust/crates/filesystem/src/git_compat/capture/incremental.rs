use super::*;

/// Captures eligible paths changed since the preceding live generation.
pub async fn capture_git_compatible_generation_incremental<A, O>(
    workspace: &Workspace<A, O>,
    previous_live: &Generation<A, O>,
    previous_capture: &Generation<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let changes = previous_live
        .diff_to(
            &live,
            u32::MAX,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let changed = changes
        .changed_paths(u32::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let mut portable = Vec::with_capacity(changed.len());
    for change in changed {
        let path = portable_changed_path(&change.path)?;
        if path.rsplit('/').next() == Some(".gitignore") {
            return capture_git_compatible_generation(
                workspace,
                policy,
                already_tracked,
                operation_id,
            )
            .await;
        }
        portable.push((path, change.before, change.after));
    }

    let mut eligible = Vec::new();
    let mut tracked_paths = already_tracked.clone();
    let mut before_policies = BTreeMap::new();
    let mut after_policies = BTreeMap::new();
    for (path, before, after) in portable {
        let kind = after.or(before).map(|record| record.kind);
        let is_directory = kind == Some(FileKind::Directory);
        let tracked = already_tracked.contains(&path);
        let tracked_descendant = is_directory
            && already_tracked
                .iter()
                .any(|candidate| is_path_below(candidate, &path));
        let (generation, policies) = if after.is_some() {
            (&live, &mut after_policies)
        } else {
            (previous_live, &mut before_policies)
        };
        let allowed = git_path_eligible_with_root_at(
            generation,
            policies,
            policy,
            &path,
            is_directory,
            already_tracked,
        )
        .await?;
        if tracked || tracked_descendant || allowed {
            eligible.push(path.clone());
        }
        if !is_directory {
            if after.is_some() && (tracked || allowed) {
                tracked_paths.insert(path);
            } else {
                tracked_paths.remove(&path);
            }
        }
    }
    if eligible.is_empty() {
        return Ok(GitCapturedGeneration {
            generation: previous_capture.clone(),
            tracked_paths,
            origin: None,
        });
    }

    apply_git_capture(
        previous_capture,
        Some(previous_live),
        Some(&live),
        &eligible,
        tracked_paths,
        operation_id,
    )
    .await
}
