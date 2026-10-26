use super::*;
use crate::{Digest, Fs, WorkspaceName};

fn workspace(name: &str) -> WorkspaceId {
    WorkspaceId::derive(
        [9; 16],
        &WorkspaceName::new(
            name,
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid name")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value
}

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

#[tokio::test]
async fn malformed_lineage_shape_never_enters_the_graph() {
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let child = workspace("child");
    let malformed = WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision: 1,
        ready: true,
        workspace_id: child,
        workspace_name: "child".to_owned(),
        parent_workspace_id: Some(child),
        parent_workspace_name: None,
        fork_generation: generation(1),
        initial_generation: generation(2),
    };

    assert!(matches!(
        graph.register(malformed).await,
        Err(WorkspaceLineageError::IncompatibleState)
    ));
}

#[tokio::test]
async fn direct_parent_authorization_and_recursive_walk_are_durable() {
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let root = workspace("root");
    let child = workspace("child");
    let grandchild = workspace("grandchild");
    graph
        .register(WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: root,
            workspace_name: "root".to_owned(),
            parent_workspace_id: None,
            parent_workspace_name: None,
            fork_generation: generation(1),
            initial_generation: generation(1),
        })
        .await
        .expect("root registration");
    for (workspace_id, parent_workspace_id, byte) in [(child, root, 2), (grandchild, child, 3)] {
        graph
            .register(WorkspaceLineageRecord {
                version: LINEAGE_VERSION,
                revision: 1,
                ready: true,
                workspace_id,
                workspace_name: if workspace_id == child {
                    "child".to_owned()
                } else {
                    "grandchild".to_owned()
                },
                parent_workspace_id: Some(parent_workspace_id),
                parent_workspace_name: Some(if parent_workspace_id == root {
                    "root".to_owned()
                } else {
                    "child".to_owned()
                }),
                fork_generation: generation(byte),
                initial_generation: generation(byte),
            })
            .await
            .expect("child registration");
    }
    graph
        .authorize_join(grandchild, child)
        .await
        .expect("direct parent");
    assert!(matches!(
        graph.authorize_join(grandchild, root).await,
        Err(WorkspaceLineageError::UnauthorizedJoin)
    ));
    let ancestors = graph
        .ancestors(grandchild, 3)
        .await
        .expect("bounded ancestry");
    assert_eq!(
        ancestors
            .iter()
            .map(|record| record.workspace_id)
            .collect::<Vec<_>>(),
        vec![child, root]
    );
    graph.authorize_descendant(grandchild, root).await.unwrap();
    assert!(matches!(
        graph.authorize_descendant(root, child).await,
        Err(WorkspaceLineageError::UnauthorizedJoin)
    ));
}

#[tokio::test]
async fn descendant_authorization_stops_at_the_authenticated_anchor() {
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let root = workspace("unregistered-anchor");
    let child = workspace("registered-child");
    graph
        .register(WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: child,
            workspace_name: "registered-child".into(),
            parent_workspace_id: Some(root),
            parent_workspace_name: Some("unregistered-anchor".into()),
            fork_generation: generation(1),
            initial_generation: generation(2),
        })
        .await
        .unwrap();
    graph.authorize_descendant(child, root).await.unwrap();
    // The former complete-history walk reads beyond the authenticated anchor.
    assert!(matches!(
        graph.ancestors(child, u32::MAX).await,
        Err(WorkspaceLineageError::IncompatibleState)
    ));
    assert!(
        graph
            .authorize_descendant(child, workspace("foreign"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_durable_planned_child_is_completed_by_retry_after_a_crash() {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "root",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("root");
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    graph.register_root(&parent).await.expect("register root");
    let parent_head = parent
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("parent head");
    let child_id = parent
        .volume
        .fs
        .workspace_id("child")
        .expect("derived child identity");
    let planned = WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision: 1,
        ready: false,
        workspace_id: child_id,
        workspace_name: "child".to_owned(),
        parent_workspace_id: Some(parent.id()),
        parent_workspace_name: Some(parent.name().as_str().to_owned()),
        fork_generation: parent_head.id(),
        initial_generation: parent_head.id(),
    };
    graph.register(planned).await.expect("persist fork intent");
    assert!(matches!(
        graph.resolve(child_id).await,
        Err(WorkspaceLineageError::IncompatibleState)
    ));

    let child = graph
        .fork(&parent, "child", IdempotencyKey::from_bytes([31; 16]))
        .await
        .expect("retry completes child");
    assert_eq!(child.id(), child_id);
    let recovered = graph.resolve(child_id).await.expect("ready lineage");
    assert!(recovered.ready);
    assert_eq!(recovered.parent_workspace_id, Some(parent.id()));
    assert_eq!(
        recovered.initial_generation,
        child
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .expect("head")
            .id()
    );
}
