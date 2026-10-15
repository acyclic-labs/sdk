//! The `acyclic git` executor over managed workspaces.

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::kernel::FileKind;
use acyclic_fs::{
    ApplyOptions, CancellationToken, DefaultTextMergeDriver, Generation, GitCommand,
    GitCommandOutput, GitCompatRepository, GitFilesystemAction, GitFilesystemExecutor,
    GitFilesystemResult, GitIgnorePolicy, GitTreeRef, IdempotencyKey, JoinOutcome, LazyWorkspace,
    LocalAuthorityBackend, LocalCoreStateStore, LocalFs, LocalObjectBackend, MaterializeOptions,
    MemoryMergeResolutionCache, MergeDriverRegistry, OperationId, OperationWindowCoordinator,
    OperationWindowLease, PublicationPermit, TransactionCommit, WorkBudget, Workspace,
    WorkspaceError, WorkspaceGraph, WorkspacePathApply, WorkspaceRestore, apply_git_patch,
    blame_git_generations, capture_git_compatible_generation,
    capture_git_compatible_generation_incremental, git_compatible_diff_counts, grep_git_generation,
    resolve_merge_plan, walk_git_tree,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path},
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;
type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
type LocalLazyWorkspace = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;
fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
use crate::filesystem::native_paths::{
    remove_tree_checked, root_materialization_directory, root_materialization_root,
};
mod helpers;
use acyclic_fs::facade::FsWork as _;
use helpers::existing_tracked_paths;
pub use helpers::{merge_drivers_for, reserved_ignore_rules, workspace_path_argument};

/// A git facade execution failure.
#[derive(Debug)]
pub struct GitError(
    /// Diagnostic text.
    pub String,
);

impl std::fmt::Display for GitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for GitError {}

impl From<String> for GitError {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Executes the full git facade against a managed lazy workspace.
pub struct WorkspaceGitExecutor<'a> {
    /// Shared local filesystem.
    pub fs: &'a LocalFs,
    /// Durable compatibility and lineage store.
    pub store: &'a LocalCoreStateStore,
    /// Current workspace.
    pub current: LocalWorkspace,
    /// Demand-backed workspace view.
    pub lazy_current: &'a LocalLazyWorkspace,
    /// Compatibility repository identity.
    pub repository_id: acyclic_fs::WorkspaceId,
    /// Current ignore policy.
    pub ignore: GitIgnorePolicy,
    /// Publication permit for this command.
    pub permit: PublicationPermit,
    /// Optional admitted operation lease.
    pub lease: Option<OperationWindowLease>,
    /// Configured merge drivers.
    pub merge_drivers: Arc<MergeDriverRegistry>,
    /// Merge resolution cache.
    pub merge_cache: AsyncMutex<MemoryMergeResolutionCache>,
    /// Newly selected workspace after checkout.
    pub switched: Mutex<Option<LocalLazyWorkspace>>,
}

fn committed(
    outcome: TransactionCommit<LocalAuthorityBackend, LocalObjectBackend>,
) -> Result<GitFilesystemResult, GitError> {
    let generation = match outcome {
        TransactionCommit::Committed(generation)
        | TransactionCommit::AlreadyCommitted(generation) => generation,
        TransactionCommit::Conflict { .. } => {
            return Err(WorkspaceGitExecutor::error(
                "Git transaction raced with another workspace writer",
            ));
        }
        TransactionCommit::Fenced => {
            return Err(WorkspaceGitExecutor::error("Git transaction was fenced"));
        }
        TransactionCommit::IdempotencyConflict => {
            return Err(WorkspaceGitExecutor::error(
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
pub use commands::{
    git_apply_patch, git_ignore_policy, git_policy, git_workspace_tree, run_git_command,
};
pub use materialize::RootMaterializingGitExecutor;
