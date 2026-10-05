//! Production Git-facade merge publication recovery through a cold reopen.
#![cfg(feature = "filesystem")]

use acyclic_fs::GitCommand;
use acyclic_fs::{
    GitCompatState, GitCompatStore, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitTreeRef, OperationId as FsOperationId, WorkspaceId,
};
use acyclic_harness::{
    AgentId, Capabilities, Error, Result,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::FilesystemGitFacade,
    resources::ProviderRef,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, thiserror::Error)]
#[error("shared Git compatibility store is unavailable")]
struct SharedStoreError;

/// A process boundary for the facade's durable compatibility state. The
/// reopened facade receives a fresh repository object over the same store,
/// matching how a host reconstructs a typed Filesystem facade after restart.
#[derive(Clone, Default)]
struct SharedStore {
    states: Arc<Mutex<BTreeMap<WorkspaceId, GitCompatState>>>,
}

impl GitCompatStore for SharedStore {
    type Error = SharedStoreError;

    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = std::result::Result<Option<GitCompatState>, Self::Error>> + Send {
        let states = self.states.clone();
        async move {
            states
                .lock()
                .map_err(|_| SharedStoreError)
                .map(|states| states.get(&workspace_id).cloned())
        }
    }

    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> impl Future<Output = std::result::Result<bool, Self::Error>> + Send {
        let states = self.states.clone();
        async move {
            let mut states = states.lock().map_err(|_| SharedStoreError)?;
            let revision = states.get(&workspace_id).map_or(0, |state| state.revision);
            if revision != expected_revision {
                return Ok(false);
            }
            states.insert(workspace_id, replacement);
            Ok(true)
        }
    }

    fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> impl Future<Output = std::result::Result<bool, Self::Error>> + Send {
        let states = self.states.clone();
        async move {
            let mut states = states.lock().map_err(|_| SharedStoreError)?;
            let revision = states.get(&workspace_id).map_or(0, |state| state.revision);
            if revision != expected_revision {
                return Ok(false);
            }
            states.remove(&workspace_id);
            Ok(true)
        }
    }
}

struct PublishThenFailExecutor {
    workspace_id: WorkspaceId,
    fail_after_publish: AtomicBool,
    published: AtomicBool,
    operations: Mutex<Vec<FsOperationId>>,
}

impl PublishThenFailExecutor {
    fn new(workspace_id: WorkspaceId) -> Self {
        Self {
            workspace_id,
            fail_after_publish: AtomicBool::new(false),
            published: AtomicBool::new(false),
            operations: Mutex::new(Vec::new()),
        }
    }

    fn fail_after_publish_once(&self) {
        self.fail_after_publish.store(true, Ordering::SeqCst);
    }

    fn operations(&self) -> Vec<FsOperationId> {
        self.operations.lock().expect("operation lock").clone()
    }
}

impl GitFilesystemExecutor for PublishThenFailExecutor {
    type Error = SharedStoreError;

    async fn validate_workspace_tree(
        &self,
        _workspace_tree: GitTreeRef,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn execute(
        &self,
        operation_id: FsOperationId,
        action: &GitFilesystemAction,
    ) -> std::result::Result<GitFilesystemResult, Self::Error> {
        self.operations
            .lock()
            .expect("operation lock")
            .push(operation_id);
        let result = match action {
            GitFilesystemAction::ForkBranch { .. } => GitFilesystemResult::Forked {
                workspace_id: self.workspace_id,
            },
            GitFilesystemAction::Join { .. } | GitFilesystemAction::ApplyCommit { .. } => {
                GitFilesystemResult::Applied {
                    tree: Some(live_tree(self.workspace_id)),
                    tracked_paths: Some(Default::default()),
                }
            }
            _ => GitFilesystemResult::Applied {
                tree: Some(live_tree(self.workspace_id)),
                tracked_paths: None,
            },
        };
        if self.fail_after_publish.swap(false, Ordering::SeqCst) {
            self.published.store(true, Ordering::SeqCst);
            return Err(SharedStoreError);
        }
        Ok(result)
    }
}

fn live_tree(workspace_id: WorkspaceId) -> GitTreeRef {
    GitTreeRef::exact(
        workspace_id,
        acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes([1; 32])),
    )
}

fn transition_facade(
    store: SharedStore,
) -> Result<(FilesystemGitFacade<SharedStore>, WorkspaceId)> {
    let provider = ProviderRef::new("git-facade-production", "filesystem", "2")?;
    let volume = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("root".into()),
    )?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("git-facade-production", [41; 32], authority);
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([42; 16]),
        "root",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
            "fork:publish".to_owned(),
            "project:merge".to_owned(),
        ]),
    );
    let workspace_id = WorkspaceId::from_bytes([43; 16]);
    Ok((
        FilesystemGitFacade::new(workspace_id, store, volume, issuer.verifier(), scope)?,
        workspace_id,
    ))
}

#[tokio::test]
async fn merge_publication_failure_reopens_and_replays_exact_operation() -> Result<()> {
    let store = SharedStore::default();
    let (facade, workspace_id) = transition_facade(store.clone())?;
    let executor = Arc::new(PublishThenFailExecutor::new(workspace_id));
    facade
        .run(
            GitCommand::Branch {
                create: Some("feature".into()),
            },
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await?;
    executor.fail_after_publish_once();
    let merge_error = facade
        .run(
            GitCommand::Merge {
                branch: "feature".into(),
            },
            live_tree(workspace_id),
            executor.as_ref(),
        )
        .await
        .err()
        .ok_or_else(|| Error::Invalid("published merge failure was reported as complete".into()))?;
    assert!(matches!(merge_error, Error::Storage(_)));
    assert!(executor.published.load(Ordering::SeqCst));
    let before_restart = executor.operations();
    assert_eq!(before_restart.len(), 2);

    drop(facade);
    let (reopened, _) = transition_facade(store)?;
    let resumed = reopened
        .resume(executor.as_ref())
        .await?
        .ok_or_else(|| Error::Invalid("published merge was not recoverable after reopen".into()))?;
    assert!(matches!(
        resumed,
        acyclic_fs::GitCommandOutput::Committed(_)
    ));
    assert!(reopened.resume(executor.as_ref()).await?.is_none());

    let operations = executor.operations();
    assert_eq!(operations.len(), 3);
    assert_eq!(operations[1], operations[2]);
    Ok(())
}
