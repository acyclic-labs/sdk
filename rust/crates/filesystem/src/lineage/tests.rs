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

    // Persisted names are identities, so successful normalization alone cannot
    // validate their spelling. Exercise both names through registration and
    // recovery from the real adapter, including a canonical Unicode record.
    for (name, parent_name, valid) in [
        ("caf\u{e9}", "parent", true),
        ("cafe\u{301}", "parent", false),
        ("child", "cafe\u{301}", false),
    ] {
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        let record = WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: workspace(name),
            workspace_name: name.to_owned(),
            parent_workspace_id: Some(workspace(parent_name)),
            parent_workspace_name: Some(parent_name.to_owned()),
            fork_generation: generation(1),
            initial_generation: generation(2),
        };
        if valid {
            graph
                .register(record.clone())
                .await
                .expect("canonical registration");
        } else {
            assert!(
                matches!(
                    graph.register(record.clone()).await,
                    Err(WorkspaceLineageError::IncompatibleState)
                ),
                "noncanonical lineage registration was accepted: {name:?}, {parent_name:?}"
            );
            assert!(
                graph
                    .store
                    .compare_and_swap(record.workspace_id, 0, record.clone())
                    .await
                    .unwrap()
            );
        }
        let recovered = graph.resolve(record.workspace_id).await;
        if valid {
            assert_eq!(recovered.unwrap(), record);
        } else {
            assert!(matches!(
                recovered,
                Err(WorkspaceLineageError::IncompatibleState)
            ));
        }
    }
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

#[derive(Default)]
struct ObservedStore {
    inner: MemoryWorkspaceLineageStore,
    reads: std::sync::atomic::AtomicUsize,
    contend: std::sync::atomic::AtomicBool,
}
impl ObservedStore {
    fn take_reads(&self) -> usize {
        self.reads.swap(0, std::sync::atomic::Ordering::Relaxed)
    }
}
impl WorkspaceLineageStore for ObservedStore {
    type Error = MemoryWorkspaceLineageStoreError;

    async fn load(&self, id: WorkspaceId) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.load(id).await
    }

    async fn compare_and_swap(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        if self
            .contend
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            // A real competing conditional publication wins before ours.
            assert!(
                self.inner
                    .compare_and_swap(id, revision, replacement.clone())
                    .await?
            );
        }
        self.inner.compare_and_swap(id, revision, replacement).await
    }
}

#[tokio::test]
async fn descendant_authorization_stops_at_the_authenticated_anchor() {
    let graph = WorkspaceGraph::new(ObservedStore::default());
    let root = workspace("unregistered-anchor");
    let child = workspace("registered-child");
    let grandchild = workspace("registered-grandchild");
    for (id, name, parent, parent_name) in [
        (child, "registered-child", root, "unregistered-anchor"),
        (
            grandchild,
            "registered-grandchild",
            child,
            "registered-child",
        ),
    ] {
        graph
            .register(WorkspaceLineageRecord {
                version: LINEAGE_VERSION,
                revision: 1,
                ready: true,
                workspace_id: id,
                workspace_name: name.into(),
                parent_workspace_id: Some(parent),
                parent_workspace_name: Some(parent_name.into()),
                fork_generation: generation(1),
                initial_generation: generation(2),
            })
            .await
            .unwrap();
    }
    graph.store().take_reads();
    graph.authorize_descendant(root, root).await.unwrap();
    assert_eq!(graph.store().take_reads(), 0, "the anchor needs no lookup");
    graph.authorize_descendant(child, root).await.unwrap();
    assert_eq!(graph.store().take_reads(), 1, "one observed child edge");
    graph.authorize_descendant(grandchild, root).await.unwrap();
    assert_eq!(
        graph.store().take_reads(),
        2,
        "one observation per parent edge"
    );
    // The former complete-history walk reads beyond the authenticated anchor.
    assert!(matches!(
        graph.ancestors(child, u32::MAX).await,
        Err(WorkspaceLineageError::IncompatibleState)
    ));
    assert_eq!(
        graph.store().take_reads(),
        2,
        "the absent parent is observed once"
    );
    assert!(
        graph
            .authorize_descendant(child, workspace("foreign"))
            .await
            .is_err()
    );
    assert_eq!(graph.store().take_reads(), 2);
    // A workspace this lineage never registered is refused, not corrupt.
    assert!(matches!(
        graph
            .authorize_descendant(workspace("unregistered"), root)
            .await,
        Err(WorkspaceLineageError::UnauthorizedJoin)
    ));
    assert_eq!(
        graph.store().take_reads(),
        1,
        "an absent workspace is classified from its original observation"
    );
    // A stored but incompatible or unfinished record is never treated as absent.
    for (name, version, ready, contend) in [
        ("invalid", 2, true, false),
        ("planned", 1, false, false),
        ("contended", 1, false, true),
    ] {
        let id = workspace(name);
        let record = WorkspaceLineageRecord {
            version,
            revision: 1,
            ready,
            workspace_id: id,
            workspace_name: name.into(),
            parent_workspace_id: Some(root),
            parent_workspace_name: Some("unregistered-anchor".into()),
            fork_generation: generation(1),
            initial_generation: generation(2),
        };
        assert!(
            graph
                .store()
                .compare_and_swap(id, 0, record.clone())
                .await
                .unwrap()
        );
        assert!(matches!(
            graph.authorize_descendant(id, root).await,
            Err(WorkspaceLineageError::IncompatibleState)
        ));
        assert_eq!(
            graph.store().take_reads(),
            1,
            "one original refusal for {name}"
        );
        if !ready {
            let mut promotion = record;
            promotion.ready = true;
            graph
                .store()
                .contend
                .store(contend, std::sync::atomic::Ordering::Relaxed);
            let result = graph.register(promotion).await;
            if contend {
                assert!(matches!(result, Err(WorkspaceLineageError::Contended)));
            } else {
                assert_eq!(result.unwrap().revision, 2);
            }
            assert_eq!(
                graph.store().take_reads(),
                1,
                "promotion retains its observed revision"
            );
            let winner = graph.resolve(id).await.unwrap();
            assert!(winner.ready);
            assert_eq!(winner.revision, 2);
            assert_eq!(graph.store().take_reads(), 1);
        }
    }
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
    let graph = WorkspaceGraph::new(ObservedStore::default());
    graph.register_root(&parent).await.expect("register root");
    assert_eq!(
        graph.store().take_reads(),
        1,
        "root creation uses its original absence"
    );
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
