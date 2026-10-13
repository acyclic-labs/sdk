//! The `acyclic git` executor over managed workspaces.

use super::*;
use acyclic_fs::facade::FsWork as _;

#[derive(Debug)]
pub(crate) struct PluginGitError(pub(crate) String);

impl std::fmt::Display for PluginGitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PluginGitError {}

impl From<String> for PluginGitError {
    fn from(message: String) -> Self {
        Self(message)
    }
}

pub(crate) struct PluginGitExecutor<'a> {
    pub(crate) fs: &'a LocalFs,
    pub(crate) store: &'a LocalCoreStateStore,
    pub(crate) current: LocalWorkspace,
    pub(crate) lazy_current: &'a LocalLazyWorkspace,
    pub(crate) repository_id: acyclic_fs::WorkspaceId,
    pub(crate) ignore: GitIgnorePolicy,
    pub(crate) permit: PublicationPermit,
    pub(crate) lease: Option<OperationWindowLease>,
    pub(crate) merge_drivers: Arc<MergeDriverRegistry>,
    pub(crate) merge_cache: AsyncMutex<MemoryMergeResolutionCache>,
    pub(crate) switched: Mutex<Option<LocalLazyWorkspace>>,
}

fn committed(
    outcome: TransactionCommit<LocalAuthorityBackend, LocalObjectBackend>,
) -> Result<GitFilesystemResult, PluginGitError> {
    let generation = match outcome {
        TransactionCommit::Committed(generation)
        | TransactionCommit::AlreadyCommitted(generation) => generation,
        TransactionCommit::Conflict { .. } => {
            return Err(PluginGitExecutor::error(
                "Git transaction raced with another workspace writer",
            ));
        }
        TransactionCommit::Fenced => {
            return Err(PluginGitExecutor::error("Git transaction was fenced"));
        }
        TransactionCommit::IdempotencyConflict => {
            return Err(PluginGitExecutor::error(
                "Git transaction retry identity was reused",
            ));
        }
    };
    Ok(GitFilesystemResult::Applied {
        tree: Some(GitTreeRef::exact(
            generation.workspace_id(),
            generation.id(),
        )),
        tracked_paths: None,
    })
}

mod commands;
mod executor;
mod materialize;
mod resolve;
mod restore;
pub(crate) use commands::{
    git_apply_patch, git_ignore_policy, git_policy, git_requires_exact_workspace,
    git_transition_command, git_workspace_tree, run_git_command,
};
pub(crate) use materialize::RootMaterializingGitExecutor;
