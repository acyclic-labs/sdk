use super::*;
use crate::{Digest, Fs, MemoryWorkspaceContextStore, TransactionCommit, WorkspaceContextRoot};
use std::path::PathBuf;

#[derive(Debug, Error)]
#[error("test publisher lock failed")]
struct PublisherError;

#[derive(Default)]
struct Publisher {
    published: Mutex<BTreeSet<WorkspaceRootId>>,
    pause_once: Mutex<Option<WorkspaceRootId>>,
    finalize_fail_once: Mutex<Option<WorkspaceRootId>>,
    finalized: Mutex<Vec<WorkspaceRootId>>,
}

#[derive(Debug, Error)]
#[error("test authorizer failed")]
struct AuthorizerError;

struct Allow;

#[derive(Debug, Error)]
#[error("test resolver failed")]
struct ResolverError;

struct Resolver<A, O> {
    workspaces: Mutex<BTreeMap<WorkspaceId, Workspace<A, O>>>,
}

impl<A: AsyncAuthorityStore + Send + Sync, O: AsyncObjectStore + Send + Sync>
    WorkspaceResolver<A, O> for Resolver<A, O>
{
    type Error = ResolverError;

    async fn resolve(&self, workspace_id: WorkspaceId) -> Result<Workspace<A, O>, Self::Error> {
        self.workspaces
            .lock()
            .map_err(|_| ResolverError)?
            .get(&workspace_id)
            .cloned()
            .ok_or(ResolverError)
    }
}

impl MultiRootPublicationAuthorizer for Allow {
    type Error = AuthorizerError;

    async fn authorize(&self, _plan: &MultiRootMergePlan) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl MultiRootPublisher for Publisher {
    type Error = PublisherError;

    async fn prepare(
        &self,
        operation_id: OperationId,
        _root_id: WorkspaceRootId,
        _root: &MultiRootMergeRoot,
        _resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<MultiRootPrepare, Self::Error> {
        Ok(MultiRootPrepare::Ready(MultiRootFence {
            token: operation_id.into_bytes().to_vec(),
        }))
    }

    async fn release(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        _fence: &MultiRootFence,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn project_conflict(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        _conflict: Option<&crate::MergePlan>,
    ) -> Result<GenerationId, Self::Error> {
        Ok(root.target_generation)
    }

    async fn abort_conflict(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        _projected_generation: GenerationId,
    ) -> Result<GenerationId, Self::Error> {
        Ok(root.target_generation)
    }

    async fn declare_conflicts(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        _root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        _paths: &[String],
    ) -> Result<BTreeSet<ConflictKey>, Self::Error> {
        Ok(conflict
            .conflicts
            .iter()
            .map(|view| view.key.clone())
            .collect())
    }

    async fn finish_conflict(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        _projected_generation: GenerationId,
    ) -> Result<MultiRootConflictFinish, Self::Error> {
        Ok(MultiRootConflictFinish {
            generation: root.target_generation,
            fence: MultiRootFence { token: Vec::new() },
        })
    }

    async fn publish(
        &self,
        _operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        _resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        _fence: &MultiRootFence,
    ) -> Result<MultiRootPublishRoot, Self::Error> {
        let mut pause = self.pause_once.lock().map_err(|_| PublisherError)?;
        if *pause == Some(root_id) {
            *pause = None;
            return Ok(MultiRootPublishRoot::UnexpectedTarget);
        }
        let inserted = self
            .published
            .lock()
            .map_err(|_| PublisherError)?
            .insert(root_id);
        Ok(if inserted {
            MultiRootPublishRoot::Published(root.source_generation)
        } else {
            MultiRootPublishRoot::AlreadyPublished(root.source_generation)
        })
    }

    async fn finalize(
        &self,
        _operation_id: OperationId,
        root_id: WorkspaceRootId,
        _root: &MultiRootMergeRoot,
    ) -> Result<(), Self::Error> {
        self.finalized
            .lock()
            .map_err(|_| PublisherError)?
            .push(root_id);
        let mut fail = self.finalize_fail_once.lock().map_err(|_| PublisherError)?;
        if *fail == Some(root_id) {
            *fail = None;
            return Err(PublisherError);
        }
        Ok(())
    }
}

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn candidate() -> MultiRootMergeCandidate {
    let roots = [1_u8, 2]
        .into_iter()
        .map(|byte| {
            (
                WorkspaceRootId::from_bytes([byte; 16]),
                MultiRootMergeRoot {
                    source_workspace_id: WorkspaceId::from_bytes([byte; 16]),
                    merge_workspace_id: None,
                    source_generation: generation(byte),
                    target_workspace_id: WorkspaceId::from_bytes([byte + 10; 16]),
                    target_generation: generation(byte + 10),
                    base_generation: generation(0),
                    child_wins_bindings: BTreeSet::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    MultiRootMergeCandidate {
        plan: MultiRootMergePlan {
            operation_id: OperationId::from_bytes([7; 16]),
            parent_context_id: WorkspaceContextId::from_bytes([8; 16]),
            child_context_id: WorkspaceContextId::from_bytes([9; 16]),
            roots,
        },
        resolutions: [1_u8, 2]
            .into_iter()
            .map(|byte| (WorkspaceRootId::from_bytes([byte; 16]), BTreeMap::new()))
            .collect(),
    }
}

#[tokio::test]
async fn parent_claim_allows_only_one_unacknowledged_publication() {
    let coordinator = MultiRootPublicationCoordinator::new(
        MemoryMultiRootPublicationStore::default(),
        Publisher::default(),
        Allow,
    );
    let first = candidate();
    let Publication::Applied(retained) = coordinator
        .publish_retained(first.clone())
        .await
        .expect("first retained publication")
    else {
        panic!("expected applied publication");
    };
    for version in [0, 2, u32::MAX] {
        let mut obsolete = retained.clone();
        obsolete.version = version;
        assert!(matches!(
            coordinator.acknowledge_applied(&obsolete).await,
            Err(MultiRootPublicationError::InvalidCandidate)
        ));
    }
    let mut second = first;
    second.plan.operation_id = OperationId::from_bytes([0x55; 16]);
    assert!(matches!(
        coordinator.publish_retained(second.clone()).await,
        Err(MultiRootPublicationError::ParentBusy)
    ));
    assert_eq!(
        coordinator
            .pending_operations()
            .await
            .expect("list retained publication"),
        [retained.candidate.plan.operation_id]
    );
    coordinator
        .acknowledge_applied(&retained)
        .await
        .expect("acknowledge first publication");
    assert!(matches!(
        coordinator
            .publish_retained(second)
            .await
            .expect("second publication after acknowledgement"),
        Publication::Applied(_)
    ));
}

#[tokio::test]
async fn add_all_declares_pathless_typed_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let target = fs
        .create_workspace(
            "pathless-conflict-target",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let head = target
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let publisher = WorkspaceMultiRootPublisher::new(Resolver {
        workspaces: Mutex::new(BTreeMap::from([(target.id(), target.clone())])),
    });
    let directory_id = crate::FileId::new();
    let key = ConflictKey::Binding {
        directory_id,
        name: b"entry".to_vec(),
    };
    let plan = crate::MergePlan {
        base: head.id(),
        ours: head.id(),
        theirs: head.id(),
        conflicts: vec![crate::ConflictView {
            key: key.clone(),
            path: None,
            kind: crate::ConflictKind::Binding,
            base: crate::ConflictValue::Binding(None),
            ours: crate::ConflictValue::Binding(None),
            theirs: crate::ConflictValue::Binding(None),
        }],
        truncated: false,
    };
    let root = MultiRootMergeRoot {
        source_workspace_id: target.id(),
        merge_workspace_id: None,
        source_generation: head.id(),
        target_workspace_id: target.id(),
        target_generation: head.id(),
        base_generation: head.id(),
        child_wins_bindings: BTreeSet::new(),
    };

    assert!(
        publisher
            .declare_conflicts(
                OperationId::from_bytes([0x71; 16]),
                WorkspaceRootId::from_bytes([0x72; 16]),
                &root,
                &plan,
                &["specific/path".to_owned()],
            )
            .await?
            .is_empty()
    );
    assert_eq!(
        publisher
            .declare_conflicts(
                OperationId::from_bytes([0x71; 16]),
                WorkspaceRootId::from_bytes([0x72; 16]),
                &root,
                &plan,
                &[".".to_owned()],
            )
            .await?,
        BTreeSet::from([key])
    );
    Ok(())
}

#[test]
fn unchanged_child_generation_is_a_valid_noop_publication() {
    let mut candidate = candidate();
    for root in candidate.plan.roots.values_mut() {
        root.source_generation = root.target_generation;
    }
    assert!(validate_candidate(&candidate).is_ok());
}

#[tokio::test]
async fn committed_publication_pauses_and_rolls_forward_on_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let publisher = Publisher::default();
    *publisher.pause_once.lock().expect("pause lock") = Some(WorkspaceRootId::from_bytes([2; 16]));
    let coordinator = MultiRootPublicationCoordinator::new(
        MemoryMultiRootPublicationStore::default(),
        publisher,
        Allow,
    );
    let candidate = candidate();
    let first = coordinator
        .publish(candidate.clone())
        .await
        .expect("first publication");
    let first = match first {
        Publication::Paused(publication) => publication,
        Publication::Applied(_)
        | Publication::Conflicted(_)
        | Publication::StaleBeforeCommit(_) => {
            return Err("publication should pause".into());
        }
    };
    assert_eq!(first.phase, MultiRootPublicationPhase::Paused);
    assert_eq!(first.published_roots.len(), 1);

    let second = coordinator
        .publish(candidate)
        .await
        .expect("recovered publication");
    let second = match second {
        Publication::Applied(publication) => publication,
        Publication::Paused(_) | Publication::Conflicted(_) | Publication::StaleBeforeCommit(_) => {
            return Err("publication should complete".into());
        }
    };
    assert_eq!(second.phase, MultiRootPublicationPhase::Applied);
    assert_eq!(second.published_roots.len(), 2);
    assert!(
        coordinator
            .pending_operations()
            .await
            .expect("terminal records are reclaimed")
            .is_empty()
    );
    Ok::<(), Box<dyn std::error::Error>>(())
}

#[tokio::test]
async fn journal_created_before_parent_claim_survives_claim_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryMultiRootPublicationStore::default();
    store
        .fail_claim_once
        .store(true, std::sync::atomic::Ordering::Release);
    let coordinator = MultiRootPublicationCoordinator::new(store, Publisher::default(), Allow);
    let operation_id = candidate().plan.operation_id;

    assert!(matches!(
        coordinator.publish(candidate()).await,
        Err(MultiRootPublicationError::Store(_))
    ));
    assert_eq!(coordinator.pending_operations().await?, [operation_id]);

    assert!(matches!(
        coordinator.publish(candidate()).await?,
        Publication::Applied(_)
    ));
    assert!(coordinator.pending_operations().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn finalize_failure_retries_after_the_applied_decision()
-> Result<(), Box<dyn std::error::Error>> {
    let publisher = Publisher::default();
    *publisher.finalize_fail_once.lock().expect("finalize lock") =
        Some(WorkspaceRootId::from_bytes([2; 16]));
    let coordinator = MultiRootPublicationCoordinator::new(
        MemoryMultiRootPublicationStore::default(),
        publisher,
        Allow,
    );
    let operation_id = candidate().plan.operation_id;
    assert!(matches!(
        coordinator.publish(candidate()).await,
        Err(MultiRootPublicationError::Publisher(_))
    ));
    let pending = coordinator.pending_operations().await?;
    assert_eq!(pending, vec![operation_id]);

    let resumed = coordinator.resume(operation_id).await?;
    assert!(matches!(resumed, Some(Publication::Applied(_))));
    assert!(coordinator.pending_operations().await?.is_empty());
    let finalized = coordinator
        .publisher
        .finalized
        .lock()
        .expect("finalized lock");
    assert_eq!(
        finalized.as_slice(),
        &[
            WorkspaceRootId::from_bytes([1; 16]),
            WorkspaceRootId::from_bytes([2; 16]),
            WorkspaceRootId::from_bytes([1; 16]),
            WorkspaceRootId::from_bytes([2; 16]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn context_authorizer_rejects_forged_root_bindings() -> Result<(), Box<dyn std::error::Error>>
{
    let registry = WorkspaceContextRegistry::new(MemoryWorkspaceContextStore::new());
    let plan = candidate().plan;
    let parent_roots = plan
        .roots
        .iter()
        .map(|(root_id, root)| WorkspaceContextRoot {
            root_id: *root_id,
            source_path: PathBuf::from(if cfg!(windows) {
                format!("C:\\roots\\{}", root_id.into_bytes()[0])
            } else {
                format!("/roots/{}", root_id.into_bytes()[0])
            }),
            workspace_id: root.target_workspace_id,
            workspace_name: format!("parent-{}", root_id.into_bytes()[0]),
            parent_workspace_id: None,
            mount_path: None,
        })
        .collect::<Vec<_>>();
    registry
        .register_root(plan.parent_context_id, parent_roots.clone())
        .await?;
    let child_roots = plan
        .roots
        .iter()
        .zip(parent_roots)
        .map(|((root_id, root), parent)| WorkspaceContextRoot {
            root_id: *root_id,
            source_path: parent.source_path,
            workspace_id: root.source_workspace_id,
            workspace_name: format!("child-{}", root_id.into_bytes()[0]),
            parent_workspace_id: Some(root.target_workspace_id),
            mount_path: None,
        })
        .collect::<Vec<_>>();
    registry
        .register_child(plan.child_context_id, plan.parent_context_id, child_roots)
        .await?;
    assert!(registry.authorize(&plan).await?);

    let mut touched_only = plan.clone();
    touched_only
        .roots
        .remove(&WorkspaceRootId::from_bytes([2; 16]));
    assert!(registry.authorize(&touched_only).await?);

    let mut forged = plan;
    forged
        .roots
        .get_mut(&WorkspaceRootId::from_bytes([1; 16]))
        .expect("root")
        .target_workspace_id = WorkspaceId::from_bytes([99; 16]);
    assert!(!registry.authorize(&forged).await?);
    Ok(())
}

mod abort;
mod conflicts;
mod publication;
