//! Production Git-facade merge publication recovery through a cold reopen.
#![cfg(feature = "filesystem-local")]

use acyclic_fs::GitCommand;
use acyclic_fs::{
    GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult, GitTreeRef,
    LocalCoreStateStore, LocalFs, LocalOptions, OperationId as FsOperationId, WorkspaceId,
};
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, Result,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemGitFacade, FilesystemHost, WorkspaceMutation, workspace_ref},
    resources::{ProviderRef, WorkspaceRef},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tempfile::tempdir;

struct PublishThenFailExecutor {
    host: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    workspace: WorkspaceRef,
    workspace_id: WorkspaceId,
    fail_after_publish: AtomicBool,
    published: AtomicBool,
    operations: Mutex<Vec<FsOperationId>>,
}

impl PublishThenFailExecutor {
    fn new(
        host: Arc<
            FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
        >,
        workspace: WorkspaceRef,
        workspace_id: WorkspaceId,
    ) -> Self {
        Self {
            host,
            workspace,
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
    type Error = acyclic_fs::LocalCoreStateStoreError;

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
        if matches!(action, GitFilesystemAction::Join { .. }) {
            self.host
                .apply(
                    &self.workspace,
                    None,
                    &[WorkspaceMutation::PutFile {
                        path: "/merge-publication.txt".into(),
                        bytes: b"merge publication applied before result persistence".to_vec(),
                    }],
                    &IdempotencyKey::new("merge-publication-real-fs").map_err(|error| {
                        acyclic_fs::LocalCoreStateStoreError::Io(std::io::Error::other(
                            error.to_string(),
                        ))
                    })?,
                )
                .await
                .map_err(|error| {
                    acyclic_fs::LocalCoreStateStoreError::Io(std::io::Error::other(
                        error.to_string(),
                    ))
                })?;
        }
        if self.fail_after_publish.swap(false, Ordering::SeqCst) {
            self.published.store(true, Ordering::SeqCst);
            return Err(acyclic_fs::LocalCoreStateStoreError::Integrity);
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
    store: LocalCoreStateStore,
) -> Result<(FilesystemGitFacade<LocalCoreStateStore>, WorkspaceId)> {
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
    let directory = tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("git-facade-production", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(
        LocalFs::local(LocalOptions::new(directory.path().join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let project = VolumeRef::new(
        provider.clone(),
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("merge-publication-recovery".into()),
    )?;
    host.create_volume(&project).await?;
    let workspace = workspace_ref(project.provider().clone(), &project.storage_name()?)?;
    let store_root = directory.path().join("control-plane");
    let store = LocalCoreStateStore::new(&store_root);
    let (facade, workspace_id) = transition_facade(store)?;
    let executor = Arc::new(PublishThenFailExecutor::new(
        host.clone(),
        workspace.clone(),
        workspace_id,
    ));
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
    assert_eq!(
        host.read(&workspace, None, "/merge-publication.txt", 256)
            .await?
            .as_ref(),
        b"merge publication applied before result persistence"
    );
    let before_restart = executor.operations();
    assert_eq!(before_restart.len(), 2);

    drop(facade);
    let (reopened, _) = transition_facade(LocalCoreStateStore::new(&store_root))?;
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
