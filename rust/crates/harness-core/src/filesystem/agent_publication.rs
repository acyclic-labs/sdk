//! SDK merge planning and compatibility history for agent publication.
use super::workspace_policy::{LocalLazyWorkspace, git_view_policy, ignore_for};
use crate::{Error, Result};
use acyclic_fs::{
    CancellationToken, GitCompatRepository, GitPublicationRecord, GitTreeRef,
    LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend, MultiRootMergeRoot,
    MultiRootPublication, OperationId, WorkBudget, Workspace, WorkspaceError, WorkspaceGraph,
    WorkspaceId, WorkspaceRootId, capture_git_compatible_generation_at,
};
use std::collections::BTreeMap;

/// Plans one child root using its ignores and the destination's tracked paths.
pub async fn plan_agent_merge(
    filesystem: &LocalFs,
    state: &LocalCoreStateStore,
    source: &LocalLazyWorkspace,
    target: &Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    repository: WorkspaceId,
) -> Result<MultiRootMergeRoot> {
    let workspace = source.workspace();
    let plan = workspace
        .join_into(target)
        .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value;
    let tracked = GitCompatRepository::new(repository, state.clone())
        .tracked_paths()
        .await?;
    let ancestor_id = plan.common_ancestor_workspace_id();
    let resolved;
    let ancestor = if ancestor_id == target.id() {
        target
    } else if ancestor_id == workspace.id() {
        workspace
    } else {
        resolved = acyclic_fs::WorkspaceResolver::resolve(
            &(filesystem.clone(), WorkspaceGraph::new(state.clone())),
            ancestor_id,
        )
        .await?;
        &resolved
    };
    let base = ancestor
        .generation(
            plan.common_ancestor(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value;
    let head = workspace
        .generation(
            plan.source_head(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value;
    let changes = base
        .diff_to(
            &head,
            u32::MAX,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value
        .0
        .changed_paths(u32::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value
        .0;
    let child_wins_bindings = ignore_for(source)
        .await?
        .newly_ignored_paths_at(&head, &changes, &tracked)
        .await?;
    Ok(MultiRootMergeRoot {
        source_workspace_id: workspace.id(),
        merge_workspace_id: None,
        source_generation: plan.source_head(),
        target_workspace_id: target.id(),
        target_generation: plan.target_head(),
        base_generation: plan.common_ancestor(),
        child_wins_bindings,
    })
}

/// Records each applied root once. Repositories map root IDs to parent/child IDs.
pub async fn record_agent_publication(
    filesystem: &LocalFs,
    state: &LocalCoreStateStore,
    publication: &MultiRootPublication,
    repositories: &BTreeMap<WorkspaceRootId, (WorkspaceId, WorkspaceId)>,
    parent_agent: &str,
    child_agent: &str,
    authored_at_seconds: i64,
) -> Result<()> {
    for (root_id, root) in &publication.candidate.plan.roots {
        let &(parent_repository_id, child_repository_id) =
            repositories.get(root_id).ok_or_else(|| {
                Error::Unauthorized("publication repository is not configured".into())
            })?;
        let published_generation = publication
            .published_generations
            .get(root_id)
            .copied()
            .ok_or_else(|| Error::Invalid("published root generation is unavailable".into()))?;
        let workspace = acyclic_fs::WorkspaceResolver::resolve(
            &(filesystem.clone(), WorkspaceGraph::new(state.clone())),
            root.target_workspace_id,
        )
        .await
        .map_err(Error::from)?;
        let published = workspace
            .generation(
                published_generation,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        let ignore_text = match published
            .read(
                "/.gitignore",
                1024 * 1024,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
        {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(WorkspaceError::NotFound) => String::new(),
            Err(error) => return Err(Error::from(error)),
        };
        let ignore = git_view_policy(&ignore_text);
        let repository = GitCompatRepository::new(parent_repository_id, state.clone());
        let tracked = repository.tracked_paths().await.map_err(Error::from)?;
        let source_head = GitCompatRepository::new(child_repository_id, state.clone())
            .head()
            .await
            .map_err(Error::from)?;
        let mut capture_hasher = blake3::Hasher::new();
        capture_hasher.update(b"acyclic-publication-capture-v1\0");
        capture_hasher.update(&publication.candidate.plan.operation_id.into_bytes());
        capture_hasher.update(&root_id.into_bytes());
        let mut capture_id = [0_u8; 16];
        capture_id.copy_from_slice(&capture_hasher.finalize().as_bytes()[..16]);
        let captured = capture_git_compatible_generation_at(
            &workspace,
            published_generation,
            &ignore,
            &tracked,
            OperationId::from_bytes(capture_id),
        )
        .await
        .map_err(Error::from)?;
        let capture_proof = captured
            .authenticated_proof(
                OperationId::from_bytes(capture_id),
                &WorkspaceGraph::new(state.clone()),
            )
            .await
            .map_err(Error::from)?;
        repository
            .record_publication(GitPublicationRecord {
                tree: GitTreeRef::exact(
                    captured.generation.workspace_id(),
                    captured.generation.id(),
                ),
                workspace_tree: GitTreeRef::exact(workspace.id(), published_generation),
                source_head,
                tracked_paths: captured.tracked_paths,
                capture_proof,
                message: format!("Merge agents/{child_agent}"),
                author: parent_agent.to_owned(),
                authored_at_seconds,
            })
            .await
            .map_err(Error::from)?;
    }
    Ok(())
}
