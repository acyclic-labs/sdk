//! The `acyclic git` executor over managed workspaces.

#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::kernel::FileKind;
use acyclic_fs::{
    ApplyOptions, CancellationToken, Generation, GitCommand, GitCommandOutput, GitCompatRepository,
    GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult, GitIgnorePolicy, GitTreeRef,
    IdempotencyKey, JoinOutcome, MemoryMergeResolutionCache, MergeDriverRegistry, OperationId,
    OperationWindowCoordinator, OperationWindowLease, PublicationPermit, TransactionCommit,
    WorkBudget, Workspace as FsWorkspace, WorkspaceError, WorkspaceGraph, WorkspacePathApply,
    apply_git_patch, blame_git_generations, capture_git_compatible_generation,
    capture_git_compatible_generation_incremental, git_compatible_diff_counts,
    git_paths_preserving_reserved, git_restore_paths, grep_git_generation, walk_git_tree,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
use acyclic_fs::{LazyWorkspace, LocalAuthorityBackend, LocalCoreStateStore, LocalObjectBackend};

/// Default command provider for a native checkout composition.
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
pub type DefaultProvider = acyclic_stream::LocalStream;
/// Default command provider for a portable volume composition.
#[cfg(not(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32"))))]
pub type DefaultProvider = acyclic_stream::MemoryStream;
/// Default object backend for a native checkout composition.
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
pub type DefaultObjects = LocalObjectBackend;
/// Default object backend for a portable volume composition.
#[cfg(not(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32"))))]
pub type DefaultObjects = acyclic_fs::MemoryObjectBackend;
/// Default durable command store for a native checkout composition.
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
pub type DefaultStore = LocalCoreStateStore;
/// Default command store for a portable volume composition.
#[cfg(not(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32"))))]
pub type DefaultStore = acyclic_fs::StreamGitCompatStore<DefaultProvider>;
type ManagedWorkspace<P, O> = FsWorkspace<acyclic_fs::StreamAuthorityStore<P>, O>;
type ManagedGeneration<P, O> = Generation<acyclic_fs::StreamAuthorityStore<P>, O>;
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
type ManagedLazyWorkspace<P, O> = LazyWorkspace<
    acyclic_fs::StreamAuthorityStore<P>,
    O,
    NativeDemandSource,
    LocalCoreStateStore,
>;

mod command_store;
pub use command_store::CommandStore;

fn json_error(source: serde_json::Error) -> Error {
    Error::NativeJson {
        code: crate::wire::ErrorCode::Storage,
        source,
        custody: None,
    }
}
fn now_millis() -> u64 {
    // The shared wall-clock seam; a seeded simulation supplies its time.
    acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock)
}
mod access;
mod helpers;
mod join;
use acyclic_fs::facade::FsWork as _;
use helpers::existing_tracked_paths;
mod path;
use path::workspace_path_argument;

use crate::Error;
/// Executes the full git facade against a managed lazy workspace.
pub(crate) struct WorkspaceGitExecutor<'a, F, P: ?Sized + 'static = DefaultProvider, O: 'static = DefaultObjects, S = DefaultStore> {
    pub access: &'a crate::filesystem::WorkspaceAccess,
    /// Revalidates task ownership before every effect.
    pub before_effect: &'a F,
    /// Shared local filesystem.
    pub fs: &'a acyclic_fs::Fs<acyclic_fs::StreamAuthorityStore<P>, O>,
    /// Durable compatibility and lineage store.
    pub store: &'a S,
    /// Current workspace.
    pub current: ManagedWorkspace<P, O>,
    /// Demand-backed workspace view.
    pub lazy_current: View<'a, P, O>,
    /// Compatibility repository identity.
    pub repository_id: acyclic_fs::WorkspaceId,
    /// Current ignore policy.
    pub ignore: GitIgnorePolicy,
    /// Repository-level policy, Git's `info/exclude`: SDK metadata rules and
    /// host-reserved paths, beneath whichever tree's own `.gitignore` applies.
    pub repository: GitIgnorePolicy,
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
    pub switched: Mutex<Option<OwnedView<P, O>>>,
}

fn committed<P: ?Sized + 'static, O: 'static>(
    outcome: TransactionCommit<acyclic_fs::StreamAuthorityStore<P>, O>,
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

mod actions;
mod agents;
mod commands;
mod errors;
pub(crate) use errors::compat_error;
mod failure;
pub use failure::Failure;
mod executor;
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
mod materialize;
mod resolve;
mod restore;
pub(crate) use commands::{git_apply_patch, git_policy, git_workspace_tree, run_git_command};
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use materialize::RootMaterializingGitExecutor;
#[doc(hidden)]
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
pub use materialize::publish_checkout_transition;

mod run;
#[doc(hidden)]
#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
pub use run::{run_with_guard, run};
pub use run::{Output, Workspace, run_volume_with_guard};
mod view;
pub use view::{OwnedView, View};
