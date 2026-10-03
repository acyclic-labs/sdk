//! Black-box authority and delegation coverage for the Harness Git facade.
#![cfg(feature = "filesystem")]

use acyclic_fs::{
    Digest, GenerationId, GitCommand, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitTreeRef, MemoryGitCompatStore, OperationId as FsOperationId,
    WorkspaceId,
};
use acyclic_harness::{
    AgentId, Capabilities, Error, Result,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::FilesystemGitFacade,
    resources::ProviderRef,
};

#[derive(Debug, thiserror::Error)]
#[error("test executor failed")]
struct TestExecutorError;

struct NoopExecutor;

impl GitFilesystemExecutor for NoopExecutor {
    type Error = TestExecutorError;

    async fn execute(
        &self,
        _operation_id: FsOperationId,
        _action: &GitFilesystemAction,
    ) -> std::result::Result<GitFilesystemResult, Self::Error> {
        Ok(GitFilesystemResult::Applied {
            tree: None,
            tracked_paths: None,
        })
    }
}

fn fixture(
    writable: bool,
) -> Result<(
    FilesystemGitFacade<MemoryGitCompatStore>,
    WorkspaceId,
    AuthorityIssuer,
)> {
    let provider = ProviderRef::new("acyclic", "filesystem", "2")?;
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
    let issuer = AuthorityIssuer::new("git-facade", [7; 32], authority);
    let mut grants = vec![volume.capability(VolumeOperation::Read)?];
    if writable {
        grants.push(volume.capability(VolumeOperation::Write)?);
        grants.push("project:merge".into());
    }
    let scope = issuer.root_for_agent(
        AgentId::from_bytes([8; 16]),
        "root",
        Capabilities::new(grants),
    );
    let workspace_id = WorkspaceId::from_bytes([9; 16]);
    let facade = FilesystemGitFacade::new(
        workspace_id,
        MemoryGitCompatStore::new(),
        volume,
        issuer.verifier(),
        scope,
    )?;
    Ok((facade, workspace_id, issuer))
}

fn live_tree(workspace_id: WorkspaceId) -> GitTreeRef {
    GitTreeRef::exact(workspace_id, GenerationId::new(Digest::from_bytes([1; 32])))
}

#[tokio::test]
async fn read_commands_delegate_to_durable_compat_repository() -> Result<()> {
    let (facade, workspace_id, _) = fixture(false)?;
    let output = facade
        .run_argv(
            &["status".into()],
            live_tree(workspace_id),
            "root",
            100,
            &NoopExecutor,
        )
        .await?;
    assert!(matches!(output, acyclic_fs::GitCommandOutput::Status(_)));
    Ok(())
}

#[tokio::test]
async fn mutating_commands_require_the_exact_volume_write_capability() -> Result<()> {
    let (facade, workspace_id, _) = fixture(false)?;
    let error = facade
        .run(
            GitCommand::Commit {
                message: "should be denied".into(),
                author: "root".into(),
                authored_at_seconds: 100,
            },
            live_tree(workspace_id),
            &NoopExecutor,
        )
        .await
        .expect_err("read-only scope must not execute a commit");
    assert!(matches!(error, Error::Unauthorized(_)));
    Ok(())
}

#[tokio::test]
async fn branch_workspace_transitions_require_parent_fork_authority() -> Result<()> {
    let (facade, workspace_id, _) = fixture(true)?;
    let error = facade
        .run(
            GitCommand::Switch {
                branch: "child".into(),
                create: false,
            },
            live_tree(workspace_id),
            &NoopExecutor,
        )
        .await
        .expect_err("a project writer without fork authority cannot switch workspaces");
    assert!(matches!(error, Error::Unsupported(value) if value == "fork:publish"));
    Ok(())
}
