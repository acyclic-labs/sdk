//! The `acyclic git` executor over managed workspaces.

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::kernel::FileKind;
use acyclic_fs::{
    ApplyOptions, CancellationToken, Generation, GitCommand, GitCommandOutput, GitCompatRepository,
    GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult, GitIgnorePolicy, GitTreeRef,
    IdempotencyKey, JoinOutcome, LazyWorkspace, LocalAuthorityBackend, LocalCoreStateStore,
    LocalFs, LocalObjectBackend, MemoryMergeResolutionCache, MergeDriverRegistry, OperationId,
    OperationWindowCoordinator, OperationWindowLease, PublicationPermit, TransactionCommit,
    WorkBudget, Workspace as FsWorkspace, WorkspaceError, WorkspaceGraph, WorkspacePathApply,
    WorkspaceRestore, apply_git_patch, blame_git_generations, capture_git_compatible_generation,
    capture_git_compatible_generation_incremental, git_compatible_diff_counts, grep_git_generation,
    walk_git_tree,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;
type LocalWorkspace = FsWorkspace<LocalAuthorityBackend, LocalObjectBackend>;
type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
type LocalLazyWorkspace = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;
fn json_error(source: serde_json::Error) -> Error {
    Error::NativeJson {
        code: crate::wire::ErrorCode::Storage,
        source,
        custody: None,
    }
}
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
mod helpers;
mod join;
use acyclic_fs::facade::FsWork as _;
use helpers::existing_tracked_paths;
mod path;
use path::workspace_path_argument;

use crate::Error;
/// Executes the full git facade against a managed lazy workspace.
pub(crate) struct WorkspaceGitExecutor<'a, F> {
    /// Revalidates task ownership before every effect.
    pub before_effect: &'a F,
    /// Shared local filesystem.
    pub fs: &'a LocalFs,
    /// Durable compatibility and lineage store.
    pub store: &'a LocalCoreStateStore,
    /// Current workspace.
    pub current: LocalWorkspace,
    /// Demand-backed workspace view.
    pub lazy_current: View<'a>,
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
    /// Lock order: merge_cache is never held with switched; switched is a leaf
    /// lock released before filesystem work or an await.
    /// Merge resolution cache.
    pub merge_cache: AsyncMutex<MemoryMergeResolutionCache>,
    /// Newly selected workspace after checkout.
    pub switched: Mutex<Option<OwnedView>>,
}

fn committed(
    outcome: TransactionCommit<LocalAuthorityBackend, LocalObjectBackend>,
) -> Result<GitFilesystemResult, Error> {
    let generation = match outcome {
        TransactionCommit::Committed(generation)
        | TransactionCommit::AlreadyCommitted(generation) => generation,
        TransactionCommit::Conflict { .. } => {
            return Err(Error::Conflict(
                "Git transaction raced with another workspace writer".into(),
            ));
        }
        TransactionCommit::Fenced => {
            return Err(Error::Conflict("Git transaction was fenced".into()));
        }
        TransactionCommit::IdempotencyConflict => {
            return Err(Error::Conflict(
                "Git transaction retry identity was reused".into(),
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

mod agents;
mod commands;
mod errors;
mod failure;
pub use failure::Failure;
mod executor;
mod materialize;
mod resolve;
mod restore;
pub(crate) use commands::{git_apply_patch, git_policy, git_workspace_tree, run_git_command};
pub(crate) use materialize::RootMaterializingGitExecutor;
#[doc(hidden)]
pub use materialize::publish_checkout_transition;

mod run;
#[doc(hidden)]
pub use run::run_with_guard;
pub use run::{Output, Workspace, run};
mod view;
pub use view::{OwnedView, View};
