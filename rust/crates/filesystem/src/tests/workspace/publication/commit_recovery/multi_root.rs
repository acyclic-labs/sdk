//! The persisted coordinator and lineage, sharing the original physical-cut driver.
use super::*;
use crate::{
    LineageMultiRootPublicationAuthorizer, LocalCoreStateStore, LocalFs, MultiRootMergeCandidate,
    MultiRootMergePlan, MultiRootMergeRoot, MultiRootPublicationCoordinator, Publication,
    WorkspaceContextId, WorkspaceContextRegistry, WorkspaceGraph, WorkspaceMultiRootPublisher,
    WorkspaceRootId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

type Coordinator = MultiRootPublicationCoordinator<
    LocalCoreStateStore,
    WorkspaceMultiRootPublisher<
        crate::LocalAuthorityBackend,
        crate::LocalObjectBackend,
        (LocalFs, WorkspaceGraph<LocalCoreStateStore>),
    >,
    LineageMultiRootPublicationAuthorizer<LocalCoreStateStore>,
>;

fn store(root: &Path) -> TestResult<LocalCoreStateStore> {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    Ok(LocalCoreStateStore::open_owned(root.join("control"))?.materialization(work))
}

pub(super) fn coordinator(root: &Path, fs: LocalFs) -> TestResult<Coordinator> {
    let state = store(root)?;
    Ok(MultiRootPublicationCoordinator::new(
        state.clone(),
        WorkspaceMultiRootPublisher::new((fs, WorkspaceGraph::new(state.clone()))),
        LineageMultiRootPublicationAuthorizer::new(state),
    ))
}

pub(super) fn candidate(root: &Path) -> TestResult<MultiRootMergeCandidate> {
    // These bytes are the actual pinned plan produced by the seeded workspaces.
    Ok(serde_json::from_slice(&std::fs::read(
        root.join("candidate"),
    )?)?)
}

pub(super) async fn seed(root: &Path) -> TestResult {
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let state = store(root)?;
    let graph = WorkspaceGraph::new(state.clone());
    let contexts = WorkspaceContextRegistry::new(state.clone());
    let parent_context = WorkspaceContextId::from_bytes([0x75; 16]);
    let child_context = WorkspaceContextId::from_bytes([0x76; 16]);
    let mut parents = Vec::new();
    let mut children = Vec::new();
    let mut roots = BTreeMap::new();
    for (index, name) in ["repo", "other"].into_iter().enumerate() {
        let parent = unbounded!(fs, create_workspace(name))?;
        let mut transaction = unbounded!(parent, begin_transaction(IdempotencyKey::new()))?;
        transaction.write_text("/file", "old").await?;
        assert!(matches!(
            commit(&mut transaction).await?,
            TransactionCommit::Committed(_)
        ));
        let base = unbounded!(parent, head())?;
        graph.register_root(&parent).await?;
        let child = graph
            .fork(&parent, format!("{name}-child"), IdempotencyKey::new())
            .await?;
        let mut transaction = unbounded!(child, begin_transaction(IdempotencyKey::new()))?;
        transaction.write_text("/file", "new").await?;
        assert!(matches!(
            commit(&mut transaction).await?,
            TransactionCommit::Committed(_)
        ));
        let root_id = WorkspaceRootId::from_bytes([index as u8 + 1; 16]);
        let binding = WorkspaceContextRoot {
            root_id,
            source_path: root.join(format!("physical-{index}")),
            workspace_id: parent.id(),
            workspace_name: name.to_owned(),
            parent_workspace_id: None,
            mount_path: None,
        };
        parents.push(binding.clone());
        children.push(WorkspaceContextRoot {
            workspace_id: child.id(),
            workspace_name: child.name().as_str().to_owned(),
            parent_workspace_id: Some(parent.id()),
            ..binding
        });
        roots.insert(
            root_id,
            MultiRootMergeRoot {
                source_workspace_id: child.id(),
                merge_workspace_id: None,
                source_generation: unbounded!(child, head())?.id(),
                target_workspace_id: parent.id(),
                target_generation: base.id(),
                base_generation: base.id(),
                child_wins_bindings: BTreeSet::new(),
            },
        );
    }
    contexts.register_root(parent_context, parents).await?;
    contexts
        .register_child(child_context, parent_context, children)
        .await?;
    let candidate = MultiRootMergeCandidate {
        resolutions: roots.keys().map(|root| (*root, BTreeMap::new())).collect(),
        plan: MultiRootMergePlan {
            operation_id: crate::OperationId::from_bytes([0x77; 16]),
            parent_context_id: parent_context,
            child_context_id: child_context,
            roots,
        },
    };
    std::fs::write(root.join("candidate"), serde_json::to_vec(&candidate)?)?;
    drop((contexts, graph, state));
    close_local(fs).await?;
    Ok(())
}

pub(super) async fn assert_recovery(root: &Path, acknowledged: bool) -> TestResult {
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let coordinator = coordinator(root, fs.clone())?;
    let mut candidate = candidate(root)?;
    for operation in coordinator.pending_operations().await? {
        assert_eq!(operation, candidate.plan.operation_id);
        assert!(matches!(
            coordinator.resume(operation).await?,
            Some(Publication::Applied(_))
        ));
    }
    let mut contents = Vec::new();
    for name in ["repo", "other"] {
        let workspace = unbounded!(fs, open_workspace(name))?;
        contents.push(unbounded!(workspace, read("/file", 16))?);
        let pinned = candidate
            .plan
            .roots
            .values_mut()
            .find(|root| root.target_workspace_id == workspace.id())
            .ok_or("missing target root")?;
        pinned.target_generation = unbounded!(workspace, head())?.id();
    }
    assert_eq!(
        contents[0], contents[1],
        "recovery left a partial all-roots publication"
    );
    assert!(matches!(contents[0].as_ref(), b"old" | b"new"));
    if acknowledged {
        assert_eq!(
            contents[0].as_ref(),
            b"new",
            "lost an acknowledged all-roots publication"
        );
    }
    // A fresh operation must acquire the same parent and both root reservations.
    candidate.plan.operation_id = crate::OperationId::from_bytes([0x78; 16]);
    assert!(matches!(
        coordinator.publish(candidate).await?,
        Publication::Applied(_)
    ));
    assert!(coordinator.pending_operations().await?.is_empty());
    for name in ["repo", "other"] {
        let workspace = unbounded!(fs, open_workspace(name))?;
        assert_eq!(unbounded!(workspace, read("/file", 16))?.as_ref(), b"new");
        let mut successor = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        successor.write_text("/successor", "live").await?;
        assert!(matches!(
            commit(&mut successor).await?,
            TransactionCommit::Committed(_)
        ));
    }
    drop(coordinator);
    close_local(fs).await?;
    Ok(())
}
