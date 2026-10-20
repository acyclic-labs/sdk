use super::*;

/// Captures an ignore-aware tree without changing the live workspace.
pub async fn capture_git_compatible_generation<A, O>(
    workspace: &Workspace<A, O>,
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
    capture_git_compatible_generation_from(live, policy, already_tracked, operation_id).await
}

/// Captures an ignore-aware tree from one exact immutable generation.
pub async fn capture_git_compatible_generation_at<A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace
        .generation(generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    capture_git_compatible_generation_from(live, policy, already_tracked, operation_id).await
}

/// Forks `source` with each of `paths` restored to its state in `target`.
///
/// A join from the result publishes none of the source's changes at those
/// paths, so the host-reserved entries of a child projection, such as its
/// volume aliases, never enter the target's tree. The fork is keyed by
/// `operation_id`, so a retried operation reuses it.
pub async fn git_generation_with_paths_from<A, O>(
    source: &Generation<A, O>,
    target: &Generation<A, O>,
    paths: &[String],
    operation_id: OperationId,
) -> Result<Generation<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    if paths.is_empty() {
        return Ok(source.clone());
    }
    apply_git_capture(
        source,
        Some(source),
        Some(target),
        paths,
        BTreeSet::new(),
        operation_id,
    )
    .await
    .map(|captured| captured.generation)
}

mod proof;
pub use proof::*;
mod traversal;
pub(crate) use traversal::*;
mod full;
use full::*;
mod incremental;
pub use incremental::*;
mod apply;
use apply::*;
