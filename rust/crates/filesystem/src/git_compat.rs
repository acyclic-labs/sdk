//! Git-shaped compatibility state over distributed filesystem generations.
//!
//! This module deliberately implements Git's familiar control vocabulary, not
//! its object database or wire protocols. Compatibility commits point at exact
//! authenticated filesystem generations. Commands that need to mutate or
//! inspect the filesystem return a typed action so the same state machine can
//! drive embedded, hosted, mounted, and language-bound executors.

use crate::kernel::{FileKind, NameEncoding, NamespacePath};
use crate::model::{CheckoutMode, GenerationSelector, VolumeLimits};
use crate::storage::ByteRange;
use crate::workspace::customer_path;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Generation, GenerationId,
    IdempotencyKey, OperationId, ResolvedFile, ResolvedFileRangeReadRequest, TransactionCommit,
    WorkBudget, Workspace, WorkspaceError, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Mutex;
use thiserror::Error;

const STATE_VERSION: u32 = 7;
const COMMIT_DOMAIN: &[u8] = b"acyclic-fs-git-compat-commit-v1\0";
const ACTION_DOMAIN: &[u8] = b"acyclic-fs-git-compat-action-v1\0";
const MAXIMUM_CAS_ATTEMPTS: u8 = 32;
const GREP_READ_CONCURRENCY: usize = 32;
const MAXIMUM_PATCH_FILE_BYTES: u64 = 64 * 1024 * 1024;

async fn resolved_git_regular_files<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &crate::PinnedReader<A, O>,
    root_display: String,
    root: NamespacePath,
    limits: VolumeLimits,
    maximum_entries: usize,
    cancellation: &CancellationToken,
) -> Result<(Vec<(String, ResolvedFile<A, O>)>, bool), WorkspaceError> {
    let mut pending = vec![(root_display, root)];
    let mut regular = Vec::new();
    let mut visited = 0_usize;
    let mut truncated = false;
    'walk: while let Some((directory_display, directory)) = pending.pop() {
        let mut after = None;
        loop {
            let page = reader
                .resolve_directory_page(
                    &directory,
                    after.as_ref(),
                    1_024,
                    WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::engine)?
                .value;
            let has_more = page.has_more;
            after = page.entries.last().map(|entry| entry.name.clone());
            for entry in page.entries {
                if visited >= maximum_entries {
                    truncated = true;
                    break 'walk;
                }
                visited = visited.saturating_add(1);
                let name = match entry.name.encoding() {
                    NameEncoding::Utf8 => std::str::from_utf8(entry.name.as_bytes())
                        .map_err(|_| WorkspaceError::path("non-UTF-8 Git path"))?,
                    NameEncoding::PosixBytes | NameEncoding::WindowsUtf16Le => {
                        return Err(WorkspaceError::path("non-portable Git path"));
                    }
                };
                let path = if directory_display.is_empty() {
                    name.to_owned()
                } else {
                    format!("{directory_display}/{name}")
                };
                if entry.file.description().kind == FileKind::Directory {
                    let mut components = directory.components().to_vec();
                    components.push(entry.name);
                    let child = NamespacePath::new(components, limits)
                        .map_err(|error| WorkspaceError::path(error.to_string()))?;
                    pending.push((path, child));
                } else if entry.file.description().kind == FileKind::Regular {
                    regular.push((path, entry.file));
                }
            }
            if !has_more {
                break;
            }
        }
    }
    regular.sort_by(|left, right| left.0.cmp(&right.0));
    Ok((regular, truncated))
}

async fn resolved_git_grep_files<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &crate::PinnedReader<A, O>,
    root_display: String,
    root: NamespacePath,
    limits: VolumeLimits,
    maximum_entries: usize,
    cancellation: &CancellationToken,
) -> Result<(Vec<(String, ResolvedFile<A, O>)>, bool), WorkspaceError> {
    if root_display.is_empty() {
        return resolved_git_regular_files(
            reader,
            root_display,
            root,
            limits,
            maximum_entries,
            cancellation,
        )
        .await;
    }
    let mut roots = reader
        .resolve_files(
            std::slice::from_ref(&root),
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?
        .value;
    let root_file = roots.pop().flatten().ok_or(WorkspaceError::NotFound)?;
    match root_file.description().kind {
        FileKind::Directory => {
            resolved_git_regular_files(
                reader,
                root_display,
                root,
                limits,
                maximum_entries,
                cancellation,
            )
            .await
        }
        FileKind::Regular => Ok((vec![(root_display, root_file)], false)),
        _ => Ok((Vec::new(), false)),
    }
}

/// Stable BLAKE3 compatibility commit identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GitCommitId([u8; 32]);

impl GitCommitId {
    /// Restores an exact compatibility identity.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns canonical bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Lower-case hexadecimal spelling accepted by the argv façade.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl Serialize for GitCommitId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for GitCommitId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let bytes = hex::decode(&value).map_err(serde::de::Error::custom)?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            serde::de::Error::custom(format!(
                "Git compatibility commit ID has {} bytes instead of 32",
                bytes.len()
            ))
        })?;
        Ok(Self(bytes))
    }
}

/// One explicit Git-facing commit mapped to an SDK generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitCommit {
    /// Content-addressed compatibility identity.
    pub id: GitCommitId,
    /// Exact authenticated filesystem generation.
    pub generation: GenerationId,
    /// Workspace that owns the authenticated history snapshot. Older state
    /// without this field falls back to the compatibility branch workspace.
    #[serde(default)]
    pub generation_workspace_id: Option<WorkspaceId>,
    /// Live working-copy generation captured at commit time. This may differ
    /// from `generation` when newly ignored paths were projected out.
    #[serde(default)]
    pub workspace_generation: Option<GenerationId>,
    /// Exact portable paths represented by this compatibility snapshot.
    #[serde(default)]
    pub tracked_paths: BTreeSet<String>,
    /// Ordered compatibility parents.
    pub parents: Vec<GitCommitId>,
    /// Human or agent identity.
    pub author: String,
    /// Unix epoch timestamp in seconds.
    pub authored_at_seconds: i64,
    /// Commit message.
    pub message: String,
}

impl GitCommit {
    /// Constructs and hashes an explicit compatibility commit.
    #[must_use]
    pub fn new(
        generation: GenerationId,
        parents: Vec<GitCommitId>,
        author: impl Into<String>,
        authored_at_seconds: i64,
        message: impl Into<String>,
    ) -> Self {
        Self::new_with_workspace_generation(
            generation,
            generation,
            parents,
            author,
            authored_at_seconds,
            message,
        )
    }

    fn new_with_workspace_generation(
        generation: GenerationId,
        workspace_generation: GenerationId,
        parents: Vec<GitCommitId>,
        author: impl Into<String>,
        authored_at_seconds: i64,
        message: impl Into<String>,
    ) -> Self {
        let author = author.into();
        let message = message.into();
        let mut hasher = blake3::Hasher::new();
        hasher.update(COMMIT_DOMAIN);
        hasher.update(generation.digest().as_bytes());
        hasher.update(
            &u64::try_from(parents.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for parent in &parents {
            hasher.update(&parent.0);
        }
        hash_bytes(&mut hasher, author.as_bytes());
        hasher.update(&authored_at_seconds.to_le_bytes());
        hash_bytes(&mut hasher, message.as_bytes());
        Self {
            id: GitCommitId(*hasher.finalize().as_bytes()),
            generation,
            generation_workspace_id: None,
            workspace_generation: Some(workspace_generation),
            tracked_paths: BTreeSet::new(),
            parents,
            author,
            authored_at_seconds,
            message,
        }
    }
}

fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
}

/// Compatibility branch backed by one SDK workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitBranch {
    /// Canonical branch name.
    pub name: String,
    /// SDK workspace owning the live working copy.
    pub workspace_id: WorkspaceId,
    /// Last explicit compatibility commit, or unborn.
    pub head: Option<GitCommitId>,
    /// Paths represented by this branch's latest explicit commit.
    #[serde(default)]
    pub tracked_paths: BTreeSet<String>,
}

/// Exact compatibility snapshot identity across SDK workspaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitGenerationRef {
    /// Workspace that authenticates the generation.
    pub workspace_id: WorkspaceId,
    /// Immutable generation identity.
    pub generation: GenerationId,
}

/// Versioned private compatibility state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitCompatState {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Currently selected compatibility branch.
    pub current_branch: String,
    /// Branch catalog.
    pub branches: BTreeMap<String, GitBranch>,
    /// Explicit commit records.
    pub commits: BTreeMap<GitCommitId, GitCommit>,
    /// Lightweight tags.
    pub tags: BTreeMap<String, GitCommitId>,
    /// Most-recent-first prior branch heads.
    pub reflog: Vec<Option<GitCommitId>>,
    /// Stashed exact generations, newest last.
    pub stash: Vec<GenerationId>,
    /// Version-five migration source. New state keeps tracking information on
    /// each branch and never serializes this empty compatibility field.
    #[serde(
        default,
        rename = "tracked_paths",
        skip_serializing_if = "BTreeSet::is_empty"
    )]
    pub legacy_tracked_paths: BTreeSet<String>,
    /// Durable sequencer transition prepared before filesystem side effects.
    #[serde(default)]
    pub pending: Option<GitPendingTransition>,
    /// Active first-parent bisection, if any.
    #[serde(default)]
    pub bisect: Option<GitBisectState>,
}

impl GitCompatState {
    /// Creates an unborn branch for one SDK workspace.
    #[must_use]
    pub fn new(branch: impl Into<String>, workspace_id: WorkspaceId) -> Self {
        let branch = branch.into();
        let mut branches = BTreeMap::new();
        branches.insert(
            branch.clone(),
            GitBranch {
                name: branch.clone(),
                workspace_id,
                head: None,
                tracked_paths: BTreeSet::new(),
            },
        );
        Self {
            version: STATE_VERSION,
            revision: 0,
            current_branch: branch,
            branches,
            commits: BTreeMap::new(),
            tags: BTreeMap::new(),
            reflog: Vec::new(),
            stash: Vec::new(),
            legacy_tracked_paths: BTreeSet::new(),
            pending: None,
            bisect: None,
        }
    }

    fn current(&self) -> Result<&GitBranch, GitCompatStateError> {
        self.branches
            .get(&self.current_branch)
            .ok_or(GitCompatStateError::Invalid)
    }

    fn current_mut(&mut self) -> Result<&mut GitBranch, GitCompatStateError> {
        self.branches
            .get_mut(&self.current_branch)
            .ok_or(GitCompatStateError::Invalid)
    }
}

/// Stable identity for one prepared compatibility filesystem transition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GitTransitionId(OperationId);

impl GitTransitionId {
    /// Creates a fresh transition identity.
    #[must_use]
    pub fn new() -> Self {
        Self(OperationId::new())
    }

    /// Restores a transition identity from canonical bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(OperationId::from_bytes(bytes))
    }

    /// Returns canonical bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }

    /// Returns the underlying operation identity.
    #[must_use]
    pub const fn operation_id(self) -> OperationId {
        self.0
    }
}

impl Default for GitTransitionId {
    fn default() -> Self {
        Self::new()
    }
}

/// State mutation committed only after its filesystem action succeeds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitPendingMutation {
    /// Clear the durable action intent without changing compatibility state.
    NoOp,
    /// Record a commit only after its filtered filesystem snapshot is durable.
    CaptureCommit {
        /// Complete live generation that was captured.
        workspace_generation: GenerationId,
        /// Commit message retained across executor recovery.
        message: String,
        /// Commit author retained across executor recovery.
        author: String,
        /// Signed Unix epoch timestamp retained across executor recovery.
        authored_at_seconds: i64,
        /// Exact compatibility head that must still be current.
        expected_head: Option<GitCommitId>,
    },
    /// Register a compatibility branch only after its SDK workspace exists.
    ForkBranch {
        /// New branch name.
        branch: String,
        /// Compatibility head inherited by the branch.
        head: Option<GitCommitId>,
        /// Whether the new branch becomes current.
        switch: bool,
    },
    /// Select a branch after its generation is installed.
    Switch {
        /// Branch selected after restoration.
        branch: String,
    },
    /// Move the current branch head after a hard restore.
    Reset {
        /// Commit installed after restoration.
        head: GitCommitId,
    },
    /// Record the dirty generation after restoring compatibility HEAD.
    StashPush {
        /// Dirty generation retained by the stash.
        generation: GenerationId,
    },
    /// Remove the exact top stash after it is restored.
    StashPop {
        /// Exact top stash restored before removal.
        generation: GenerationId,
    },
    /// Record a merge or rebase result generation.
    Join {
        /// Source compatibility head captured while preparing.
        source_head: Option<GitCommitId>,
        /// Source branch used in the generated commit message.
        source_branch: String,
        /// Whether resulting ancestry is rebased rather than merged.
        rebase: bool,
    },
    /// Record a cherry-pick or revert result generation.
    ApplyCommit {
        /// Commit whose delta is applied.
        commit: GitCommitId,
        /// Whether the delta is inverted.
        reverse: bool,
    },
    /// Install one bisection checkout and then publish its durable session state.
    Bisect {
        /// Replacement session, or `None` when resetting.
        state: Box<Option<GitBisectState>>,
        /// Machine-readable result returned after the checkout succeeds.
        result: GitBisectResult,
    },
}

/// Durable first-parent bisection state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitBisectState {
    /// Branch that was active before bisection began.
    pub original_branch: String,
    /// Commit restored by `bisect reset`.
    pub original_head: GitCommitId,
    /// Known good ancestor, when supplied.
    pub good: Option<GitCommitId>,
    /// Known bad descendant.
    pub bad: GitCommitId,
    /// Commit currently installed for testing.
    pub current: Option<GitCommitId>,
    /// Commits excluded by `bisect skip`.
    pub skipped: BTreeSet<GitCommitId>,
}

/// Stable machine-readable bisection result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitBisectResult {
    /// Whether a session remains active.
    pub active: bool,
    /// Known good ancestor.
    pub good: Option<GitCommitId>,
    /// Known bad descendant.
    pub bad: Option<GitCommitId>,
    /// Commit installed for the caller to test.
    pub current: Option<GitCommitId>,
    /// Number of unclassified candidates remaining.
    pub remaining: u32,
    /// First bad commit once no candidates remain.
    pub first_bad: Option<GitCommitId>,
}

/// Durable prepared action and its deferred compatibility-state mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitPendingTransition {
    /// Stable retry identity.
    pub id: GitTransitionId,
    /// Filesystem action to execute idempotently.
    pub action: GitFilesystemAction,
    /// Mutation finalized only after successful action execution.
    pub mutation: GitPendingMutation,
}

/// Revision or branch-like object name.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitObjectName(pub String);

/// Reset behavior supported by the compatibility layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GitResetMode {
    /// Move compatibility HEAD without changing the workspace.
    Soft,
    /// Same as soft because there is no separate index.
    Mixed,
    /// Move HEAD and restore the workspace generation.
    Hard,
}

/// Parsed Git-shaped command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitCommand {
    /// Compare compatibility HEAD with the live workspace.
    Status,
    /// Diff HEAD-to-workspace; cached has identical semantics.
    Diff {
        /// Whether the caller used `--cached`; behavior is identical without an index.
        cached: bool,
    },
    /// Walk explicit compatibility history.
    Log {
        /// Maximum first-parent commits to return.
        maximum: u32,
    },
    /// Inspect one compatibility commit.
    Show {
        /// Object to show, defaulting to `HEAD`.
        object: Option<GitObjectName>,
    },
    /// Accepted no-op because changes are always staged.
    Add {
        /// Accepted pathspecs; staging remains automatic.
        paths: Vec<String>,
    },
    /// Capture the complete eligible workspace state.
    Commit {
        /// Commit message.
        message: String,
        /// Author identity.
        author: String,
        /// Signed Unix epoch time.
        authored_at_seconds: i64,
    },
    /// List or create a branch.
    Branch {
        /// Optional branch to create before listing.
        create: Option<String>,
    },
    /// Select a branch, optionally creating it first.
    Switch {
        /// Destination branch.
        branch: String,
        /// Whether to create a missing destination.
        create: bool,
    },
    /// Restore paths from an object.
    Restore {
        /// Source object, defaulting to `HEAD`.
        source: Option<GitObjectName>,
        /// Portable paths to restore.
        paths: Vec<String>,
    },
    /// Move HEAD and optionally the workspace.
    Reset {
        /// Commit-like target.
        target: GitObjectName,
        /// Workspace restoration policy.
        mode: GitResetMode,
    },
    /// Join another compatibility branch.
    Merge {
        /// Source branch.
        branch: String,
    },
    /// Rebase onto another compatibility branch.
    Rebase {
        /// New base branch.
        branch: String,
    },
    /// Stash the complete working generation.
    StashPush,
    /// Restore and remove the newest stash.
    StashPop,
    /// Apply one commit as a cherry-pick.
    CherryPick {
        /// Commit to apply.
        object: GitObjectName,
    },
    /// Invert one explicit commit.
    Revert {
        /// Commit to invert.
        object: GitObjectName,
    },
    /// List, create, or delete a lightweight tag.
    Tag {
        /// Optional tag name; omission lists tags.
        name: Option<String>,
        /// Target object, defaulting to `HEAD`.
        target: Option<GitObjectName>,
        /// Whether to delete instead of create.
        delete: bool,
    },
    /// Attribute lines to compatibility commits.
    Blame {
        /// Portable path to attribute.
        path: String,
    },
    /// Search one live or historical tree.
    Grep {
        /// Search pattern interpreted by the executor.
        pattern: String,
        /// Optional subtree path.
        path: Option<String>,
    },
    /// Remove untracked, non-ignored paths.
    Clean {
        /// Whether to report candidates without mutation.
        dry_run: bool,
    },
    /// Export one compatibility tree.
    Archive {
        /// Object to archive, defaulting to the workspace.
        object: Option<GitObjectName>,
    },
    /// Apply a patch to the working workspace.
    Apply {
        /// Opaque patch bytes.
        patch: Vec<u8>,
    },
    /// Advance a caller-directed bisect session.
    Bisect {
        /// Bisect subcommand and arguments.
        arguments: Vec<String>,
    },
}

/// Filesystem work emitted by the compatibility state machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitFilesystemAction {
    /// Capture a compatibility-eligible generation before recording a commit.
    CaptureCommit {
        /// Complete live workspace generation.
        workspace_generation: GenerationId,
        /// Previous compatibility generation, if HEAD exists.
        head_generation: Option<GenerationId>,
        /// Paths already tracked and therefore eligible even if newly ignored.
        tracked_paths: BTreeSet<String>,
        /// Commit message retained until capture completes.
        message: String,
        /// Author retained until capture completes.
        author: String,
        /// Signed Unix epoch timestamp retained until capture completes.
        authored_at_seconds: i64,
        /// Exact HEAD that must still be current at completion.
        expected_head: Option<GitCommitId>,
    },
    /// Fork a dedicated SDK workspace for a compatibility branch.
    ForkBranch {
        /// New branch name.
        branch: String,
        /// Workspace whose exact generation is the fork source.
        source_workspace: WorkspaceId,
        /// Exact source generation.
        source_generation: GenerationId,
        /// Compatibility head inherited by the new branch.
        head: Option<GitCommitId>,
        /// Whether the new branch should become current after registration.
        switch: bool,
    },
    /// Select an existing branch's live SDK workspace.
    SwitchWorkspace {
        /// Workspace that owns the destination branch's working copy.
        workspace_id: WorkspaceId,
    },
    /// Compute a semantic diff between exact generations.
    Diff {
        /// Explicit compatibility HEAD, or no baseline for an unborn branch.
        from: Option<GitGenerationRef>,
        /// Live workspace generation.
        to: GitGenerationRef,
    },
    /// Move the live workspace to an exact generation.
    RestoreGeneration {
        /// Workspace that authenticates the exact generation.
        workspace_id: WorkspaceId,
        /// Exact generation to install.
        generation: GenerationId,
        /// Paths participating in a compatibility-history restore. `None`
        /// requests an exact same-workspace head restoration (for stash).
        paths: Option<BTreeSet<String>>,
    },
    /// Restore selected paths from an exact generation.
    RestorePaths {
        /// Workspace that authenticates the exact generation.
        workspace_id: WorkspaceId,
        /// Exact source generation.
        generation: GenerationId,
        /// Portable paths to replace.
        paths: Vec<String>,
    },
    /// Join a source workspace into the current workspace.
    Join {
        /// Workspace owning the source branch.
        source_workspace: WorkspaceId,
        /// Whether to record rebase rather than merge ancestry.
        rebase: bool,
    },
    /// Apply one commit relative to its first parent.
    ApplyCommit {
        /// Explicit compatibility commit.
        commit: GitCommitId,
        /// Whether to apply its inverse.
        reverse: bool,
        /// Exact earlier tree, or the empty compatibility tree.
        base: Option<GitGenerationRef>,
        /// Exact later tree, or the empty compatibility tree.
        source: Option<GitGenerationRef>,
        /// Complete portable path set participating in the delta.
        paths: BTreeSet<String>,
    },
    /// Attribute a path across explicit commit history.
    Blame {
        /// Portable path to attribute.
        path: String,
        /// Newest-first explicit history to inspect.
        commits: Vec<GitCommit>,
    },
    /// Search the live tree.
    Grep {
        /// Search pattern.
        pattern: String,
        /// Optional subtree path.
        path: Option<String>,
        /// Exact live tree to search.
        generation: GitGenerationRef,
    },
    /// Discover and optionally remove untracked paths.
    Clean {
        /// Whether mutation is disabled.
        dry_run: bool,
        /// Exact live tree to inspect before any deletion.
        generation: GitGenerationRef,
        /// Paths protected as tracked by the current branch.
        tracked_paths: BTreeSet<String>,
    },
    /// Export one exact generation.
    Archive {
        /// Workspace that authenticates the exact generation.
        workspace_id: WorkspaceId,
        /// Exact generation to export.
        generation: GenerationId,
    },
    /// Parse and apply a patch.
    ApplyPatch {
        /// Opaque patch bytes.
        patch: Vec<u8>,
    },
}

/// Typed result returned by a compatibility filesystem executor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitFilesystemResult {
    /// An eligible compatibility snapshot was captured.
    Captured {
        /// Exact filtered generation.
        generation: GenerationId,
        /// Workspace that owns the filtered generation.
        workspace_id: WorkspaceId,
        /// Paths represented by the captured compatibility history.
        tracked_paths: BTreeSet<String>,
    },
    /// A dedicated compatibility branch workspace was created.
    Forked {
        /// Stable SDK workspace identity.
        workspace_id: WorkspaceId,
    },
    /// A mutating action completed, optionally producing a new generation.
    Applied {
        /// Resulting generation when the action changes workspace state.
        generation: Option<GenerationId>,
    },
    /// Stable executor-defined inspection data.
    Data {
        /// Result category such as `diff`, `grep`, or `archive`.
        kind: String,
        /// Versioned machine-readable result body.
        value: serde_json::Value,
    },
}

impl GitFilesystemResult {
    fn resulting_generation(&self) -> Option<GenerationId> {
        match self {
            Self::Captured { generation, .. } => Some(*generation),
            Self::Applied { generation } => *generation,
            Self::Forked { .. } | Self::Data { .. } => None,
        }
    }
}

/// Backend-neutral execution boundary for Git-shaped filesystem work.
///
/// Implementations receive a stable operation identity for every action. They
/// must apply mutating actions idempotently and return only typed results; the
/// compatibility repository retains all history and sequencer authority.
pub trait GitFilesystemExecutor: Send + Sync {
    /// Executor failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Executes or recovers one exact action.
    fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> impl Future<Output = Result<GitFilesystemResult, Self::Error>> + Send;
}

/// Git-shaped status without an index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitStatus {
    /// Current compatibility branch.
    pub branch: String,
    /// Explicit compatibility HEAD.
    pub head: Option<GitCommitId>,
    /// Exact live workspace generation.
    pub workspace: GenerationId,
    /// Whether HEAD and the workspace differ.
    pub dirty: bool,
    /// Always true: the compatibility layer stages all eligible changes.
    pub all_changes_staged: bool,
}

/// Stable machine-readable command result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitCommandOutput {
    /// No state or filesystem change was required.
    NoOp,
    /// Status result.
    Status(GitStatus),
    /// Ordered commit records.
    Commits(Vec<GitCommit>),
    /// Branch catalog and active branch.
    Branches {
        /// Active branch name.
        current: String,
        /// Complete branch catalog.
        branches: Vec<GitBranch>,
    },
    /// Tag catalog.
    Tags(BTreeMap<String, GitCommitId>),
    /// New explicit commit.
    Committed(GitCommit),
    /// Durable first-parent bisection state.
    Bisect(GitBisectResult),
    /// Compatibility state advanced and filesystem work is required.
    Action(GitFilesystemAction),
    /// Filesystem work durably prepared before a state-changing transition.
    Prepared {
        /// Stable completion/abort identity.
        transition: GitTransitionId,
        /// Idempotent filesystem action to execute.
        action: GitFilesystemAction,
    },
    /// Completed filesystem inspection or mutation result.
    Filesystem(GitFilesystemResult),
}

/// Durable optimistic-concurrency adapter for private compatibility state.
pub trait GitCompatStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads state for one repository/workspace identity.
    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<GitCompatState>, Self::Error>> + Send;

    /// Atomically replaces one expected revision. Revision zero creates state.
    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Atomically removes one exact compatibility-state revision.
    fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

#[derive(Debug, Error)]
enum GitCompatStateError {
    #[error("invalid compatibility state")]
    Invalid,
    #[error("unknown object")]
    Unknown(String),
    #[error("branch already exists")]
    BranchExists(String),
    #[error("HEAD is unborn")]
    UnbornHead,
    #[error("nothing to commit")]
    NothingToCommit,
    #[error("stash is empty")]
    EmptyStash,
    #[error("invalid bisect operation: {0}")]
    Bisect(String),
}

/// Git compatibility failure.
#[derive(Debug, Error)]
pub enum GitCompatError<E: std::error::Error + 'static> {
    /// Durable adapter failed.
    #[error("Git compatibility store failed: {0}")]
    Store(E),
    /// Persisted state is incompatible or internally inconsistent.
    #[error("Git compatibility state is invalid")]
    InvalidState,
    /// Command arguments are invalid.
    #[error("invalid Git-compatible command: {0}")]
    InvalidCommand(String),
    /// Named branch, tag, or commit does not exist.
    #[error("unknown Git-compatible object '{0}'")]
    UnknownObject(String),
    /// Named branch already exists.
    #[error("Git-compatible branch '{0}' already exists")]
    BranchExists(String),
    /// There is no explicit commit to name.
    #[error("Git-compatible HEAD is unborn")]
    UnbornHead,
    /// Nothing differs from explicit compatibility HEAD.
    #[error("nothing to commit")]
    NothingToCommit,
    /// Transport or object-database command is intentionally unavailable.
    #[error("unsupported Git command '{command}': {reason}")]
    Unsupported {
        /// Requested command name.
        command: String,
        /// Stable explanation of the unsupported capability.
        reason: String,
    },
    /// Optimistic state contention exceeded the bounded retry policy.
    #[error("Git compatibility state remained contended")]
    Contended,
    /// Another sequencer transition must be completed or aborted first.
    #[error("Git compatibility transition {0:?} is still pending")]
    TransitionPending(GitTransitionId),
    /// Completion or abort named a stale transition.
    #[error("Git compatibility transition is stale")]
    StaleTransition,
    /// The completed filesystem action did not return its resulting generation.
    #[error("Git compatibility transition requires a resulting generation")]
    MissingResultGeneration,
}

/// Failure from the composed compatibility state machine and action executor.
#[derive(Debug, Error)]
pub enum GitCompatRunError<S: std::error::Error + 'static, E: std::error::Error + 'static> {
    /// Compatibility state, parsing, or optimistic publication failed.
    #[error(transparent)]
    Compat(#[from] GitCompatError<S>),
    /// The filesystem executor failed; prepared state remains recoverable.
    #[error("Git compatibility filesystem action failed: {0}")]
    Executor(E),
    /// The executor returned a result that does not match the requested action.
    #[error("Git compatibility executor returned an invalid result")]
    InvalidResult,
    /// A typed action could not be encoded for its deterministic identity.
    #[error("Git compatibility action encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
}

/// Repository façade over one SDK workspace and private compatibility state.
pub struct GitCompatRepository<S> {
    workspace_id: WorkspaceId,
    store: S,
}

struct GitCommitRecord {
    expected_head: Option<GitCommitId>,
    generation: GenerationId,
    generation_workspace_id: Option<WorkspaceId>,
    workspace_generation: GenerationId,
    tracked_paths: BTreeSet<String>,
    message: String,
    author: String,
    authored_at_seconds: i64,
}

impl<S> GitCompatRepository<S> {
    /// Creates a façade. State is initialized lazily on the first command.
    #[must_use]
    pub const fn new(workspace_id: WorkspaceId, store: S) -> Self {
        Self {
            workspace_id,
            store,
        }
    }

    /// Borrows the state adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Executes a command and all required filesystem work through one composed
    /// recovery-safe boundary.
    ///
    /// Adapters normally need only this method. State-only commands return
    /// immediately, action commands receive a deterministic operation identity,
    /// and prepared sequencer mutations remain durable if the executor fails.
    pub async fn run<E: GitFilesystemExecutor>(
        &self,
        command: GitCommand,
        workspace_generation: GenerationId,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let output = self.execute(command, workspace_generation).await?;
        self.finish_output(output, workspace_generation, executor)
            .await
    }

    /// Parses Git-like argv and executes it through [`Self::run`].
    pub async fn run_argv<E: GitFilesystemExecutor>(
        &self,
        argv: &[String],
        workspace_generation: GenerationId,
        default_author: &str,
        now_seconds: i64,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let command = parse_argv(argv, default_author, now_seconds)?;
        self.run(command, workspace_generation, executor).await
    }

    /// Recovers the durable pending transition, if any, through the same
    /// executor contract used by [`Self::run`].
    pub async fn resume<E: GitFilesystemExecutor>(
        &self,
        executor: &E,
    ) -> Result<Option<GitCommandOutput>, GitCompatRunError<S::Error, E::Error>> {
        let Some(pending) = self.pending_transition().await? else {
            return Ok(None);
        };
        let result = executor
            .execute(pending.id.operation_id(), &pending.action)
            .await
            .map_err(GitCompatRunError::Executor)?;
        let output = self.complete_transition_result(pending.id, &result).await?;
        Ok(Some(if matches!(output, GitCommandOutput::NoOp) {
            GitCommandOutput::Filesystem(result)
        } else {
            output
        }))
    }

    async fn finish_output<E: GitFilesystemExecutor>(
        &self,
        output: GitCommandOutput,
        workspace_generation: GenerationId,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let (action, operation_id, transition) = match output {
            GitCommandOutput::Action(action) => {
                let operation_id =
                    action_operation_id(self.workspace_id, workspace_generation, &action)?;
                (action, operation_id, None)
            }
            GitCommandOutput::Prepared { transition, action } => {
                (action, transition.operation_id(), Some(transition))
            }
            other => return Ok(other),
        };
        let result = executor
            .execute(operation_id, &action)
            .await
            .map_err(GitCompatRunError::Executor)?;
        if let Some(transition) = transition {
            let completed = self.complete_transition_result(transition, &result).await?;
            return Ok(if matches!(completed, GitCommandOutput::NoOp) {
                GitCommandOutput::Filesystem(result)
            } else {
                completed
            });
        }
        match (action, result) {
            (
                GitFilesystemAction::CaptureCommit {
                    workspace_generation,
                    message,
                    author,
                    authored_at_seconds,
                    expected_head,
                    ..
                },
                GitFilesystemResult::Captured {
                    generation,
                    workspace_id,
                    tracked_paths,
                },
            ) => self
                .record_captured_commit(GitCommitRecord {
                    expected_head,
                    generation,
                    generation_workspace_id: Some(workspace_id),
                    workspace_generation,
                    tracked_paths,
                    message,
                    author,
                    authored_at_seconds,
                })
                .await
                .map_err(Into::into),
            (
                GitFilesystemAction::ForkBranch {
                    branch,
                    head,
                    switch,
                    ..
                },
                GitFilesystemResult::Forked { workspace_id },
            ) => self
                .register_branch_workspace(branch, workspace_id, head, switch)
                .await
                .map_err(Into::into),
            (GitFilesystemAction::CaptureCommit { .. }, _)
            | (GitFilesystemAction::ForkBranch { .. }, _) => Err(GitCompatRunError::InvalidResult),
            (_, result) => Ok(GitCommandOutput::Filesystem(result)),
        }
    }

    /// Executes one typed command against an exact live workspace generation.
    pub async fn execute(
        &self,
        command: GitCommand,
        workspace_generation: GenerationId,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            if let Some(pending) = &state.pending {
                return Err(GitCompatError::TransitionPending(pending.id));
            }
            let before = state.clone();
            let output = execute_command(&mut state, command.clone(), workspace_generation)
                .map_err(map_state_error)?;
            let output = match output {
                GitCommandOutput::Action(action) => {
                    prepare_transition(&mut state, action, GitPendingMutation::NoOp)
                }
                output => output,
            };
            if state == before {
                return Ok(output);
            }
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self
                .store
                .compare_and_swap(self.workspace_id, expected, state)
                .await
                .map_err(GitCompatError::Store)?
            {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Returns the durable sequencer transition that requires recovery.
    pub async fn pending_transition(
        &self,
    ) -> Result<Option<GitPendingTransition>, GitCompatError<S::Error>> {
        self.load().await.map(|state| state.pending)
    }

    /// Commits a prepared transition after its filesystem action succeeds.
    pub async fn complete_transition(
        &self,
        transition: GitTransitionId,
        resulting_generation: Option<GenerationId>,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.complete_transition_result(
            transition,
            &GitFilesystemResult::Applied {
                generation: resulting_generation,
            },
        )
        .await
    }

    /// Commits a prepared transition using the executor's complete typed result.
    ///
    /// Capture and branch transitions require their full result so recovery can
    /// publish compatibility state without reconstructing filesystem facts.
    pub async fn complete_transition_result(
        &self,
        transition: GitTransitionId,
        result: &GitFilesystemResult,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let pending = state
                .pending
                .clone()
                .filter(|pending| pending.id == transition)
                .ok_or(GitCompatError::StaleTransition)?;
            let output = complete_pending(&mut state, pending.mutation, result)?;
            state.pending = None;
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self
                .store
                .compare_and_swap(self.workspace_id, expected, state)
                .await
                .map_err(GitCompatError::Store)?
            {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Clears an exact prepared transition after the executor proves that no
    /// filesystem effects remain (or after the materializer rolls them back).
    pub async fn abort_transition(
        &self,
        transition: GitTransitionId,
    ) -> Result<(), GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            if state.pending.as_ref().map(|pending| pending.id) != Some(transition) {
                return Err(GitCompatError::StaleTransition);
            }
            state.pending = None;
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self
                .store
                .compare_and_swap(self.workspace_id, expected, state)
                .await
                .map_err(GitCompatError::Store)?
            {
                return Ok(());
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Parses and executes a Git-like argv vector.
    pub async fn execute_argv(
        &self,
        argv: &[String],
        workspace_generation: GenerationId,
        default_author: &str,
        now_seconds: i64,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let command = parse_argv(argv, default_author, now_seconds)?;
        self.execute(command, workspace_generation).await
    }

    /// Completes a previously emitted [`GitFilesystemAction::ForkBranch`].
    ///
    /// The caller must create a real SDK child workspace first. Registration is
    /// idempotent for the same branch/workspace/head tuple and rejects any
    /// competing result.
    pub async fn register_branch_workspace(
        &self,
        branch: impl Into<String>,
        workspace_id: WorkspaceId,
        head: Option<GitCommitId>,
        switch: bool,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let branch = branch.into();
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let output = register_branch_workspace_state(
                &mut state,
                branch.clone(),
                workspace_id,
                head,
                switch,
            )?;
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self
                .store
                .compare_and_swap(self.workspace_id, expected, state)
                .await
                .map_err(GitCompatError::Store)?
            {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Records a compatibility commit after the core executor filters newly
    /// ignored paths and returns the exact eligible generation.
    pub async fn record_commit(
        &self,
        expected_head: Option<GitCommitId>,
        generation: GenerationId,
        tracked_paths: BTreeSet<String>,
        message: impl Into<String>,
        author: impl Into<String>,
        authored_at_seconds: i64,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.record_captured_commit(GitCommitRecord {
            expected_head,
            generation,
            generation_workspace_id: None,
            workspace_generation: generation,
            tracked_paths,
            message: message.into(),
            author: author.into(),
            authored_at_seconds,
        })
        .await
    }

    async fn record_captured_commit(
        &self,
        record: GitCommitRecord,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let GitCommitRecord {
            expected_head,
            generation,
            generation_workspace_id,
            workspace_generation,
            tracked_paths,
            message,
            author,
            authored_at_seconds,
        } = record;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let output = record_captured_commit_state(
                &mut state,
                GitCommitRecord {
                    expected_head,
                    generation,
                    generation_workspace_id,
                    workspace_generation,
                    tracked_paths: tracked_paths.clone(),
                    message: message.clone(),
                    author: author.clone(),
                    authored_at_seconds,
                },
            )?;
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self
                .store
                .compare_and_swap(self.workspace_id, expected, state)
                .await
                .map_err(GitCompatError::Store)?
            {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    async fn load(&self) -> Result<GitCompatState, GitCompatError<S::Error>> {
        let mut state = self
            .store
            .load(self.workspace_id)
            .await
            .map_err(GitCompatError::Store)?
            .unwrap_or_else(|| GitCompatState::new("main", self.workspace_id));
        if !(1..=STATE_VERSION).contains(&state.version) || state.current().is_err() {
            return Err(GitCompatError::InvalidState);
        }
        if state.version < 6 && !state.legacy_tracked_paths.is_empty() {
            let migrated = std::mem::take(&mut state.legacy_tracked_paths);
            state
                .current_mut()
                .map_err(|_| GitCompatError::InvalidState)?
                .tracked_paths = migrated;
        }
        state.version = STATE_VERSION;
        Ok(state)
    }
}

fn action_operation_id(
    workspace_id: WorkspaceId,
    workspace_generation: GenerationId,
    action: &GitFilesystemAction,
) -> Result<OperationId, serde_json::Error> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ACTION_DOMAIN);
    hasher.update(&workspace_id.into_bytes());
    hasher.update(workspace_generation.digest().as_bytes());
    let encoded = serde_json::to_vec(action)?;
    hash_bytes(&mut hasher, &encoded);
    let mut operation = [0; 16];
    operation.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(operation))
}

#[allow(clippy::too_many_lines)]
fn execute_command(
    state: &mut GitCompatState,
    command: GitCommand,
    workspace: GenerationId,
) -> Result<GitCommandOutput, GitCompatStateError> {
    let current = state.current()?.clone();
    if state.bisect.is_some()
        && !matches!(
            &command,
            GitCommand::Status
                | GitCommand::Diff { .. }
                | GitCommand::Log { .. }
                | GitCommand::Show { .. }
                | GitCommand::Bisect { .. }
        )
    {
        return Err(GitCompatStateError::Bisect(
            "finish with 'git bisect reset' before changing history".to_owned(),
        ));
    }
    let visible_head = state
        .bisect
        .as_ref()
        .and_then(|bisect| bisect.current)
        .or(current.head);
    let head_generation =
        visible_head.and_then(|id| state.commits.get(&id).map(|commit| commit.generation));
    let head_workspace_generation = visible_head.and_then(|id| {
        state
            .commits
            .get(&id)
            .map(|commit| commit.workspace_generation.unwrap_or(commit.generation))
    });
    let head_snapshot = visible_head.and_then(|id| {
        state.commits.get(&id).map(|commit| GitGenerationRef {
            workspace_id: commit
                .generation_workspace_id
                .unwrap_or(current.workspace_id),
            generation: commit.generation,
        })
    });
    Ok(match command {
        GitCommand::Status => GitCommandOutput::Status(GitStatus {
            branch: current.name,
            head: visible_head,
            workspace,
            dirty: head_workspace_generation != Some(workspace),
            all_changes_staged: true,
        }),
        GitCommand::Diff { .. } => GitCommandOutput::Action(GitFilesystemAction::Diff {
            from: head_snapshot,
            to: GitGenerationRef {
                workspace_id: current.workspace_id,
                generation: workspace,
            },
        }),
        GitCommand::Log { maximum } => {
            GitCommandOutput::Commits(walk_commits(state, current.head, maximum))
        }
        GitCommand::Show { object } => {
            let id = resolve_object(state, object.as_ref())?;
            GitCommandOutput::Commits(vec![
                state
                    .commits
                    .get(&id)
                    .cloned()
                    .ok_or(GitCompatStateError::Invalid)?,
            ])
        }
        GitCommand::Add { .. } => GitCommandOutput::NoOp,
        GitCommand::Commit {
            message,
            author,
            authored_at_seconds,
        } => {
            if head_workspace_generation == Some(workspace) {
                return Err(GitCompatStateError::NothingToCommit);
            }
            let action = GitFilesystemAction::CaptureCommit {
                workspace_generation: workspace,
                head_generation,
                tracked_paths: current.tracked_paths.clone(),
                message: message.clone(),
                author: author.clone(),
                authored_at_seconds,
                expected_head: current.head,
            };
            prepare_transition(
                state,
                action,
                GitPendingMutation::CaptureCommit {
                    workspace_generation: workspace,
                    message,
                    author,
                    authored_at_seconds,
                    expected_head: current.head,
                },
            )
        }
        GitCommand::Branch { create } => {
            if let Some(name) = create {
                if state.branches.contains_key(&name) {
                    return Err(GitCompatStateError::BranchExists(name));
                }
                let action = GitFilesystemAction::ForkBranch {
                    branch: name.clone(),
                    source_workspace: current.workspace_id,
                    source_generation: workspace,
                    head: current.head,
                    switch: false,
                };
                return Ok(prepare_transition(
                    state,
                    action,
                    GitPendingMutation::ForkBranch {
                        branch: name,
                        head: current.head,
                        switch: false,
                    },
                ));
            }
            GitCommandOutput::Branches {
                current: state.current_branch.clone(),
                branches: state.branches.values().cloned().collect(),
            }
        }
        GitCommand::Switch { branch, create } => {
            if create && !state.branches.contains_key(&branch) {
                let action = GitFilesystemAction::ForkBranch {
                    branch: branch.clone(),
                    source_workspace: current.workspace_id,
                    source_generation: workspace,
                    head: current.head,
                    switch: true,
                };
                return Ok(prepare_transition(
                    state,
                    action,
                    GitPendingMutation::ForkBranch {
                        branch,
                        head: current.head,
                        switch: true,
                    },
                ));
            }
            let target = state
                .branches
                .get(&branch)
                .ok_or_else(|| GitCompatStateError::Unknown(branch.clone()))?;
            prepare_transition(
                state,
                GitFilesystemAction::SwitchWorkspace {
                    workspace_id: target.workspace_id,
                },
                GitPendingMutation::Switch { branch },
            )
        }
        GitCommand::Restore { source, paths } => {
            let id = resolve_object(state, source.as_ref())?;
            let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
            GitCommandOutput::Action(GitFilesystemAction::RestorePaths {
                workspace_id: commit
                    .generation_workspace_id
                    .unwrap_or(current.workspace_id),
                generation: commit.generation,
                paths: expand_pathspecs(&paths, &commit.tracked_paths),
            })
        }
        GitCommand::Reset { target, mode } => {
            let id = resolve_object(state, Some(&target))?;
            let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
            let generation = commit.generation;
            let target_tracked_paths = commit.tracked_paths.clone();
            let workspace_id = commit
                .generation_workspace_id
                .unwrap_or(current.workspace_id);
            match mode {
                GitResetMode::Soft | GitResetMode::Mixed => {
                    state.reflog.insert(0, current.head);
                    let branch = state.current_mut()?;
                    branch.head = Some(id);
                    branch.tracked_paths = target_tracked_paths;
                    GitCommandOutput::NoOp
                }
                GitResetMode::Hard => prepare_transition(
                    state,
                    GitFilesystemAction::RestoreGeneration {
                        workspace_id,
                        generation,
                        paths: Some(
                            current
                                .tracked_paths
                                .union(&target_tracked_paths)
                                .cloned()
                                .collect(),
                        ),
                    },
                    GitPendingMutation::Reset { head: id },
                ),
            }
        }
        GitCommand::Merge { branch } => {
            let source = state
                .branches
                .get(&branch)
                .ok_or_else(|| GitCompatStateError::Unknown(branch.clone()))?;
            prepare_transition(
                state,
                GitFilesystemAction::Join {
                    source_workspace: source.workspace_id,
                    rebase: false,
                },
                GitPendingMutation::Join {
                    source_head: source.head,
                    source_branch: branch,
                    rebase: false,
                },
            )
        }
        GitCommand::Rebase { branch } => {
            let source = state
                .branches
                .get(&branch)
                .ok_or_else(|| GitCompatStateError::Unknown(branch.clone()))?;
            prepare_transition(
                state,
                GitFilesystemAction::Join {
                    source_workspace: source.workspace_id,
                    rebase: true,
                },
                GitPendingMutation::Join {
                    source_head: source.head,
                    source_branch: branch,
                    rebase: true,
                },
            )
        }
        GitCommand::StashPush => {
            let generation = head_workspace_generation.ok_or(GitCompatStateError::UnbornHead)?;
            prepare_transition(
                state,
                GitFilesystemAction::RestoreGeneration {
                    workspace_id: current.workspace_id,
                    generation,
                    paths: None,
                },
                GitPendingMutation::StashPush {
                    generation: workspace,
                },
            )
        }
        GitCommand::StashPop => {
            let generation = state
                .stash
                .last()
                .copied()
                .ok_or(GitCompatStateError::EmptyStash)?;
            prepare_transition(
                state,
                GitFilesystemAction::RestoreGeneration {
                    workspace_id: current.workspace_id,
                    generation,
                    paths: None,
                },
                GitPendingMutation::StashPop { generation },
            )
        }
        GitCommand::CherryPick { object } => {
            let commit = resolve_object(state, Some(&object))?;
            let record = state
                .commits
                .get(&commit)
                .ok_or(GitCompatStateError::Invalid)?;
            let parent = record
                .parents
                .first()
                .and_then(|parent| state.commits.get(parent));
            let base = parent.map(|commit| commit_generation_ref(commit, current.workspace_id));
            let source = Some(commit_generation_ref(record, current.workspace_id));
            let paths = parent.map_or_else(
                || record.tracked_paths.clone(),
                |parent| {
                    parent
                        .tracked_paths
                        .union(&record.tracked_paths)
                        .cloned()
                        .collect()
                },
            );
            prepare_transition(
                state,
                GitFilesystemAction::ApplyCommit {
                    commit,
                    reverse: false,
                    base,
                    source,
                    paths,
                },
                GitPendingMutation::ApplyCommit {
                    commit,
                    reverse: false,
                },
            )
        }
        GitCommand::Revert { object } => {
            let commit = resolve_object(state, Some(&object))?;
            let record = state
                .commits
                .get(&commit)
                .ok_or(GitCompatStateError::Invalid)?;
            let parent = record
                .parents
                .first()
                .and_then(|parent| state.commits.get(parent));
            let base = Some(commit_generation_ref(record, current.workspace_id));
            let source = parent.map(|commit| commit_generation_ref(commit, current.workspace_id));
            let paths = parent.map_or_else(
                || record.tracked_paths.clone(),
                |parent| {
                    parent
                        .tracked_paths
                        .union(&record.tracked_paths)
                        .cloned()
                        .collect()
                },
            );
            prepare_transition(
                state,
                GitFilesystemAction::ApplyCommit {
                    commit,
                    reverse: true,
                    base,
                    source,
                    paths,
                },
                GitPendingMutation::ApplyCommit {
                    commit,
                    reverse: true,
                },
            )
        }
        GitCommand::Tag {
            name,
            target,
            delete,
        } => {
            if let Some(name) = name {
                if delete {
                    state.tags.remove(&name);
                } else {
                    let id = resolve_object(state, target.as_ref())?;
                    state.tags.insert(name, id);
                }
            }
            GitCommandOutput::Tags(state.tags.clone())
        }
        GitCommand::Blame { path } => GitCommandOutput::Action(GitFilesystemAction::Blame {
            path,
            commits: walk_commits(state, current.head, u32::MAX),
        }),
        GitCommand::Grep { pattern, path } => GitCommandOutput::Action(GitFilesystemAction::Grep {
            pattern,
            path,
            generation: GitGenerationRef {
                workspace_id: current.workspace_id,
                generation: workspace,
            },
        }),
        GitCommand::Clean { dry_run } => GitCommandOutput::Action(GitFilesystemAction::Clean {
            dry_run,
            generation: GitGenerationRef {
                workspace_id: current.workspace_id,
                generation: workspace,
            },
            tracked_paths: current.tracked_paths,
        }),
        GitCommand::Archive { object } => {
            let snapshot = match object {
                Some(object) => {
                    let id = resolve_object(state, Some(&object))?;
                    let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
                    GitGenerationRef {
                        workspace_id: commit
                            .generation_workspace_id
                            .unwrap_or(current.workspace_id),
                        generation: commit.generation,
                    }
                }
                None => GitGenerationRef {
                    workspace_id: current.workspace_id,
                    generation: workspace,
                },
            };
            GitCommandOutput::Action(GitFilesystemAction::Archive {
                workspace_id: snapshot.workspace_id,
                generation: snapshot.generation,
            })
        }
        GitCommand::Apply { patch } => {
            GitCommandOutput::Action(GitFilesystemAction::ApplyPatch { patch })
        }
        GitCommand::Bisect { arguments } => execute_bisect(state, &current, workspace, &arguments)?,
    })
}

fn prepare_transition(
    state: &mut GitCompatState,
    action: GitFilesystemAction,
    mutation: GitPendingMutation,
) -> GitCommandOutput {
    let transition = GitTransitionId::new();
    state.pending = Some(GitPendingTransition {
        id: transition,
        action: action.clone(),
        mutation,
    });
    GitCommandOutput::Prepared { transition, action }
}

fn register_branch_workspace_state<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    branch: String,
    workspace_id: WorkspaceId,
    head: Option<GitCommitId>,
    switch: bool,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    if let Some(existing) = state.branches.get(&branch) {
        if existing.workspace_id != workspace_id || existing.head != head {
            return Err(GitCompatError::BranchExists(branch));
        }
        if switch {
            state.current_branch = branch;
        }
        return Ok(GitCommandOutput::NoOp);
    }
    let tracked_paths = head
        .and_then(|head| state.commits.get(&head))
        .map(|commit| commit.tracked_paths.clone())
        .unwrap_or_default();
    state.branches.insert(
        branch.clone(),
        GitBranch {
            name: branch.clone(),
            workspace_id,
            head,
            tracked_paths,
        },
    );
    if switch {
        state.current_branch = branch;
    }
    Ok(GitCommandOutput::NoOp)
}

fn record_captured_commit_state<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    record: GitCommitRecord,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let current = state
        .current()
        .map_err(|_| GitCompatError::InvalidState)?
        .clone();
    if current.head != record.expected_head {
        return Err(GitCompatError::InvalidState);
    }
    let head_generation = current
        .head
        .and_then(|id| state.commits.get(&id).map(|commit| commit.generation));
    if head_generation == Some(record.generation) {
        return Err(GitCompatError::NothingToCommit);
    }
    let commit = GitCommit::new_with_workspace_generation(
        record.generation,
        record.workspace_generation,
        current.head.into_iter().collect(),
        record.author,
        record.authored_at_seconds,
        record.message,
    );
    let commit = GitCommit {
        generation_workspace_id: record
            .generation_workspace_id
            .or(Some(current.workspace_id)),
        tracked_paths: record.tracked_paths.clone(),
        ..commit
    };
    state.reflog.insert(0, current.head);
    state.commits.insert(commit.id, commit.clone());
    let current = state
        .current_mut()
        .map_err(|_| GitCompatError::InvalidState)?;
    current.head = Some(commit.id);
    current.tracked_paths = record.tracked_paths;
    Ok(GitCommandOutput::Committed(commit))
}

fn complete_pending<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    mutation: GitPendingMutation,
    result: &GitFilesystemResult,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let resulting_generation = result.resulting_generation();
    match mutation {
        GitPendingMutation::NoOp => Ok(GitCommandOutput::NoOp),
        mutation @ (GitPendingMutation::CaptureCommit { .. }
        | GitPendingMutation::ForkBranch { .. }) => {
            complete_creation_pending(state, mutation, result)
        }
        GitPendingMutation::Switch { branch } => {
            if !state.branches.contains_key(&branch) {
                return Err(GitCompatError::InvalidState);
            }
            state.current_branch = branch;
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::Reset { head } => {
            let previous = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .head;
            state.reflog.insert(0, previous);
            let tracked_paths = state
                .commits
                .get(&head)
                .ok_or(GitCompatError::InvalidState)?
                .tracked_paths
                .clone();
            let branch = state
                .current_mut()
                .map_err(|_| GitCompatError::InvalidState)?;
            branch.head = Some(head);
            branch.tracked_paths = tracked_paths;
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::StashPush { generation } => {
            state.stash.push(generation);
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::StashPop { generation } => {
            if state.stash.last() != Some(&generation) {
                return Err(GitCompatError::InvalidState);
            }
            state.stash.pop();
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::Join {
            source_head,
            source_branch,
            rebase,
        } => {
            let generation = resulting_generation.ok_or(GitCompatError::MissingResultGeneration)?;
            let previous = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .head;
            let parents = if rebase {
                source_head.into_iter().collect()
            } else {
                previous.into_iter().chain(source_head).collect()
            };
            let verb = if rebase { "rebase" } else { "merge" };
            record_generated_commit(
                state,
                generation,
                parents,
                format!("{verb} {source_branch}"),
                previous,
            )
        }
        GitPendingMutation::ApplyCommit { commit, reverse } => {
            let generation = resulting_generation.ok_or(GitCompatError::MissingResultGeneration)?;
            let previous = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .head;
            let verb = if reverse { "revert" } else { "cherry-pick" };
            record_generated_commit(
                state,
                generation,
                previous.into_iter().collect(),
                format!("{verb} {}", commit.to_hex()),
                previous,
            )
        }
        GitPendingMutation::Bisect {
            state: replacement,
            result,
        } => {
            state.bisect = *replacement;
            Ok(GitCommandOutput::Bisect(result))
        }
    }
}

fn complete_creation_pending<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    mutation: GitPendingMutation,
    result: &GitFilesystemResult,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    match (mutation, result) {
        (
            GitPendingMutation::CaptureCommit {
                workspace_generation,
                message,
                author,
                authored_at_seconds,
                expected_head,
            },
            GitFilesystemResult::Captured {
                generation,
                workspace_id,
                tracked_paths,
            },
        ) => record_captured_commit_state(
            state,
            GitCommitRecord {
                expected_head,
                generation: *generation,
                generation_workspace_id: Some(*workspace_id),
                workspace_generation,
                tracked_paths: tracked_paths.clone(),
                message,
                author,
                authored_at_seconds,
            },
        ),
        (
            GitPendingMutation::ForkBranch {
                branch,
                head,
                switch,
            },
            GitFilesystemResult::Forked { workspace_id },
        ) => register_branch_workspace_state(state, branch, *workspace_id, head, switch),
        _ => Err(GitCompatError::InvalidState),
    }
}

fn record_generated_commit<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    generation: GenerationId,
    parents: Vec<GitCommitId>,
    message: String,
    previous: Option<GitCommitId>,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let generation_workspace_id = state
        .current()
        .map_err(|_| GitCompatError::InvalidState)?
        .workspace_id;
    let mut tracked_paths = state
        .current()
        .map_err(|_| GitCompatError::InvalidState)?
        .tracked_paths
        .clone();
    for parent in &parents {
        if let Some(commit) = state.commits.get(parent) {
            tracked_paths.extend(commit.tracked_paths.iter().cloned());
        }
    }
    let commit = GitCommit {
        generation_workspace_id: Some(generation_workspace_id),
        tracked_paths: tracked_paths.clone(),
        ..GitCommit::new(generation, parents, "git-compat", 0, message)
    };
    state.reflog.insert(0, previous);
    state.commits.insert(commit.id, commit.clone());
    let branch = state
        .current_mut()
        .map_err(|_| GitCompatError::InvalidState)?;
    branch.head = Some(commit.id);
    branch.tracked_paths = tracked_paths;
    Ok(GitCommandOutput::Committed(commit))
}

fn map_state_error<E: std::error::Error + 'static>(
    error: GitCompatStateError,
) -> GitCompatError<E> {
    match error {
        GitCompatStateError::Invalid => GitCompatError::InvalidState,
        GitCompatStateError::Unknown(name) => GitCompatError::UnknownObject(name),
        GitCompatStateError::BranchExists(name) => GitCompatError::BranchExists(name),
        GitCompatStateError::UnbornHead => GitCompatError::UnbornHead,
        GitCompatStateError::NothingToCommit => GitCompatError::NothingToCommit,
        GitCompatStateError::EmptyStash => GitCompatError::UnknownObject("stash@{0}".to_owned()),
        GitCompatStateError::Bisect(message) => GitCompatError::InvalidCommand(message),
    }
}

fn resolve_object(
    state: &GitCompatState,
    object: Option<&GitObjectName>,
) -> Result<GitCommitId, GitCompatStateError> {
    let Some(object) = object else {
        return visible_head(state).ok_or(GitCompatStateError::UnbornHead);
    };
    if object.0 == "HEAD" {
        return visible_head(state).ok_or(GitCompatStateError::UnbornHead);
    }
    if let Some(branch) = state.branches.get(&object.0) {
        return branch.head.ok_or(GitCompatStateError::UnbornHead);
    }
    if let Some(id) = state.tags.get(&object.0) {
        return Ok(*id);
    }
    let matching: Vec<_> = state
        .commits
        .keys()
        .filter(|id| id.to_hex().starts_with(&object.0))
        .copied()
        .collect();
    match matching.as_slice() {
        [id] => Ok(*id),
        _ => Err(GitCompatStateError::Unknown(object.0.clone())),
    }
}

fn visible_head(state: &GitCompatState) -> Option<GitCommitId> {
    state
        .bisect
        .as_ref()
        .and_then(|bisect| bisect.current)
        .or_else(|| state.current().ok().and_then(|branch| branch.head))
}

fn execute_bisect(
    state: &mut GitCompatState,
    branch: &GitBranch,
    workspace: GenerationId,
    arguments: &[String],
) -> Result<GitCommandOutput, GitCompatStateError> {
    let subcommand = arguments.first().map_or("start", String::as_str);
    match subcommand {
        "start" => {
            if state.bisect.is_some() {
                return Err(GitCompatStateError::Bisect(
                    "a bisect session is already active".to_owned(),
                ));
            }
            let bad = arguments
                .get(1)
                .map(|value| resolve_object(state, Some(&GitObjectName(value.clone()))))
                .transpose()?
                .or(branch.head)
                .ok_or(GitCompatStateError::UnbornHead)?;
            let good = arguments
                .get(2)
                .map(|value| resolve_object(state, Some(&GitObjectName(value.clone()))))
                .transpose()?;
            let bad_record = state
                .commits
                .get(&bad)
                .ok_or(GitCompatStateError::Invalid)?;
            if bad_record
                .workspace_generation
                .unwrap_or(bad_record.generation)
                != workspace
            {
                return Err(GitCompatStateError::Bisect(
                    "the working copy must match the bad commit before bisect starts".to_owned(),
                ));
            }
            let session = GitBisectState {
                original_branch: state.current_branch.clone(),
                original_head: branch.head.ok_or(GitCompatStateError::UnbornHead)?,
                good,
                bad,
                current: Some(bad),
                skipped: BTreeSet::new(),
            };
            advance_bisect(state, branch, session)
        }
        "good" | "bad" | "skip" => {
            let mut session = state.bisect.clone().ok_or_else(|| {
                GitCompatStateError::Bisect("start a bisect session first".to_owned())
            })?;
            let marked = arguments
                .get(1)
                .map(|value| resolve_object(state, Some(&GitObjectName(value.clone()))))
                .transpose()?
                .or(session.current)
                .ok_or_else(|| {
                    GitCompatStateError::Bisect("no current commit is selected".to_owned())
                })?;
            match subcommand {
                "good" => session.good = Some(marked),
                "bad" => session.bad = marked,
                "skip" => {
                    session.skipped.insert(marked);
                }
                _ => unreachable!(),
            }
            advance_bisect(state, branch, session)
        }
        "reset" => {
            let session = state.bisect.clone().ok_or_else(|| {
                GitCompatStateError::Bisect("no bisect session is active".to_owned())
            })?;
            let result = GitBisectResult {
                active: false,
                good: session.good,
                bad: Some(session.bad),
                current: None,
                remaining: 0,
                first_bad: None,
            };
            prepare_bisect_checkout(state, branch, session.original_head, None, result)
        }
        "log" | "visualize" => Ok(GitCommandOutput::Bisect(bisect_result(
            state,
            state.bisect.as_ref(),
        )?)),
        other => Err(GitCompatStateError::Bisect(format!(
            "unsupported bisect operation '{other}'"
        ))),
    }
}

fn advance_bisect(
    state: &mut GitCompatState,
    branch: &GitBranch,
    mut session: GitBisectState,
) -> Result<GitCommandOutput, GitCompatStateError> {
    let Some(good) = session.good else {
        session.current = Some(session.bad);
        let result = bisect_result(state, Some(&session))?;
        state.bisect = Some(session);
        return Ok(GitCommandOutput::Bisect(result));
    };
    let chain = first_parent_range(state, session.bad, good)?;
    let candidates: Vec<_> = chain
        .iter()
        .copied()
        .skip(1)
        .take(chain.len().saturating_sub(2))
        .filter(|commit| !session.skipped.contains(commit))
        .collect();
    if candidates.is_empty() {
        session.current = None;
        let result = GitBisectResult {
            active: true,
            good: Some(good),
            bad: Some(session.bad),
            current: None,
            remaining: 0,
            first_bad: Some(session.bad),
        };
        state.bisect = Some(session);
        return Ok(GitCommandOutput::Bisect(result));
    }
    let candidate = *candidates
        .get(candidates.len() / 2)
        .ok_or_else(|| GitCompatStateError::Bisect("no bisection candidate".to_owned()))?;
    session.current = Some(candidate);
    let result = GitBisectResult {
        active: true,
        good: Some(good),
        bad: Some(session.bad),
        current: Some(candidate),
        remaining: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
        first_bad: None,
    };
    prepare_bisect_checkout(state, branch, candidate, Some(session), result)
}

fn prepare_bisect_checkout(
    state: &mut GitCompatState,
    branch: &GitBranch,
    commit: GitCommitId,
    replacement: Option<GitBisectState>,
    result: GitBisectResult,
) -> Result<GitCommandOutput, GitCompatStateError> {
    let record = state
        .commits
        .get(&commit)
        .ok_or(GitCompatStateError::Invalid)?;
    let paths = branch
        .tracked_paths
        .union(&record.tracked_paths)
        .cloned()
        .collect();
    Ok(prepare_transition(
        state,
        GitFilesystemAction::RestoreGeneration {
            workspace_id: record
                .generation_workspace_id
                .unwrap_or(branch.workspace_id),
            generation: record.generation,
            paths: Some(paths),
        },
        GitPendingMutation::Bisect {
            state: Box::new(replacement),
            result,
        },
    ))
}

fn first_parent_range(
    state: &GitCompatState,
    bad: GitCommitId,
    good: GitCommitId,
) -> Result<Vec<GitCommitId>, GitCompatStateError> {
    let mut range = Vec::new();
    let mut next = Some(bad);
    let mut seen = BTreeSet::new();
    while let Some(commit) = next {
        if !seen.insert(commit) {
            return Err(GitCompatStateError::Invalid);
        }
        range.push(commit);
        if commit == good {
            return Ok(range);
        }
        next = state
            .commits
            .get(&commit)
            .ok_or(GitCompatStateError::Invalid)?
            .parents
            .first()
            .copied();
    }
    Err(GitCompatStateError::Bisect(
        "the good commit is not a first-parent ancestor of the bad commit".to_owned(),
    ))
}

fn bisect_result(
    state: &GitCompatState,
    session: Option<&GitBisectState>,
) -> Result<GitBisectResult, GitCompatStateError> {
    let Some(session) = session else {
        return Ok(GitBisectResult {
            active: false,
            good: None,
            bad: None,
            current: None,
            remaining: 0,
            first_bad: None,
        });
    };
    let remaining = match session.good {
        Some(good) => first_parent_range(state, session.bad, good)?
            .into_iter()
            .skip(1)
            .filter(|commit| *commit != good && !session.skipped.contains(commit))
            .count(),
        None => 0,
    };
    Ok(GitBisectResult {
        active: true,
        good: session.good,
        bad: Some(session.bad),
        current: session.current,
        remaining: u32::try_from(remaining).unwrap_or(u32::MAX),
        first_bad: session.good.filter(|_| remaining == 0).map(|_| session.bad),
    })
}

fn commit_generation_ref(commit: &GitCommit, fallback: WorkspaceId) -> GitGenerationRef {
    GitGenerationRef {
        workspace_id: commit.generation_workspace_id.unwrap_or(fallback),
        generation: commit.generation,
    }
}

fn walk_commits(
    state: &GitCompatState,
    mut next: Option<GitCommitId>,
    maximum: u32,
) -> Vec<GitCommit> {
    let mut commits = Vec::new();
    while let Some(id) = next {
        if commits.len() >= usize::try_from(maximum).unwrap_or(usize::MAX) {
            break;
        }
        let Some(commit) = state.commits.get(&id) else {
            break;
        };
        commits.push(commit.clone());
        next = commit.parents.first().copied();
    }
    commits
}

fn expand_pathspecs(paths: &[String], tracked_paths: &BTreeSet<String>) -> Vec<String> {
    let mut expanded = BTreeSet::new();
    for path in paths {
        let path = path.trim_matches('/');
        let mut matched = false;
        for tracked in tracked_paths {
            if tracked == path || is_path_below(tracked, path) {
                expanded.insert(tracked.clone());
                matched = true;
            }
        }
        if !matched && !path.is_empty() {
            expanded.insert(path.to_owned());
        }
    }
    expanded.into_iter().collect()
}

#[allow(clippy::too_many_lines)]
fn parse_argv<E: std::error::Error + 'static>(
    argv: &[String],
    default_author: &str,
    now_seconds: i64,
) -> Result<GitCommand, GitCompatError<E>> {
    let Some(command) = argv.first().map(String::as_str) else {
        return Err(GitCompatError::InvalidCommand("missing command".to_owned()));
    };
    let args = argv.get(1..).unwrap_or_default();
    match command {
        "status" => Ok(GitCommand::Status),
        "diff" => Ok(GitCommand::Diff {
            cached: args
                .iter()
                .any(|arg| arg == "--cached" || arg == "--staged"),
        }),
        "log" => Ok(GitCommand::Log { maximum: 100 }),
        "show" => Ok(GitCommand::Show {
            object: args.first().cloned().map(GitObjectName),
        }),
        "add" => Ok(GitCommand::Add {
            paths: args.to_vec(),
        }),
        "commit" => {
            let message = option_value(args, "-m")
                .or_else(|| option_value(args, "--message"))
                .ok_or_else(|| GitCompatError::InvalidCommand("commit requires -m".to_owned()))?;
            Ok(GitCommand::Commit {
                message,
                author: default_author.to_owned(),
                authored_at_seconds: now_seconds,
            })
        }
        "branch" => Ok(GitCommand::Branch {
            create: args.first().cloned(),
        }),
        "switch" | "checkout" => {
            let create = args.iter().any(|arg| arg == "-c" || arg == "-b");
            let branch = args
                .iter()
                .find(|arg| !arg.starts_with('-'))
                .cloned()
                .ok_or_else(|| {
                    GitCompatError::InvalidCommand("switch requires a branch".to_owned())
                })?;
            Ok(GitCommand::Switch { branch, create })
        }
        "restore" => {
            let source = option_value(args, "--source").map(GitObjectName);
            let paths = args
                .iter()
                .filter(|arg| {
                    !arg.starts_with('-')
                        && Some(arg.as_str()) != source.as_ref().map(|value| value.0.as_str())
                })
                .cloned()
                .collect();
            Ok(GitCommand::Restore { source, paths })
        }
        "reset" => {
            let mode = if args.iter().any(|arg| arg == "--hard") {
                GitResetMode::Hard
            } else if args.iter().any(|arg| arg == "--soft") {
                GitResetMode::Soft
            } else {
                GitResetMode::Mixed
            };
            let target = args
                .iter()
                .find(|arg| !arg.starts_with('-'))
                .cloned()
                .unwrap_or_else(|| "HEAD".to_owned());
            Ok(GitCommand::Reset {
                target: GitObjectName(target),
                mode,
            })
        }
        "merge" | "rebase" => {
            let branch = args.first().cloned().ok_or_else(|| {
                GitCompatError::InvalidCommand(format!("{command} requires a branch"))
            })?;
            if command == "merge" {
                Ok(GitCommand::Merge { branch })
            } else {
                Ok(GitCommand::Rebase { branch })
            }
        }
        "cherry-pick" | "revert" => {
            let object = GitObjectName(args.first().cloned().ok_or_else(|| {
                GitCompatError::InvalidCommand(format!("{command} requires a commit"))
            })?);
            if command == "cherry-pick" {
                Ok(GitCommand::CherryPick { object })
            } else {
                Ok(GitCommand::Revert { object })
            }
        }
        "stash" => match args.first().map(String::as_str) {
            None | Some("push") => Ok(GitCommand::StashPush),
            Some("pop") => Ok(GitCommand::StashPop),
            Some(other) => Err(GitCompatError::InvalidCommand(format!(
                "unsupported stash operation '{other}'"
            ))),
        },
        "tag" => {
            let delete = args.iter().any(|arg| arg == "-d" || arg == "--delete");
            let values: Vec<_> = args.iter().filter(|arg| !arg.starts_with('-')).collect();
            Ok(GitCommand::Tag {
                name: values.first().map(|value| (*value).clone()),
                target: values.get(1).map(|value| GitObjectName((*value).clone())),
                delete,
            })
        }
        "blame" => Ok(GitCommand::Blame {
            path: args.first().cloned().ok_or_else(|| {
                GitCompatError::InvalidCommand("blame requires a path".to_owned())
            })?,
        }),
        "grep" => Ok(GitCommand::Grep {
            pattern: args.first().cloned().ok_or_else(|| {
                GitCompatError::InvalidCommand("grep requires a pattern".to_owned())
            })?,
            path: args.get(1).cloned(),
        }),
        "clean" => {
            let dry_run = args.iter().any(|arg| arg == "-n" || arg == "--dry-run");
            let force = args.iter().any(|arg| arg == "-f" || arg == "--force");
            if !dry_run && !force {
                return Err(GitCompatError::InvalidCommand(
                    "clean requires -f or --force unless --dry-run is used".to_owned(),
                ));
            }
            Ok(GitCommand::Clean { dry_run })
        }
        "archive" => Ok(GitCommand::Archive {
            object: args.first().cloned().map(GitObjectName),
        }),
        "apply" => Ok(GitCommand::Apply {
            patch: args.join("\n").into_bytes(),
        }),
        "bisect" => Ok(GitCommand::Bisect {
            arguments: args.to_vec(),
        }),
        "clone" | "fetch" | "pull" | "push" | "remote" | "gc" | "repack" | "cat-file"
        | "hash-object" => Err(GitCompatError::Unsupported {
            command: command.to_owned(),
            reason: "the compatibility layer has no Git object database or transport".to_owned(),
        }),
        other => Err(GitCompatError::InvalidCommand(format!(
            "unknown command '{other}'"
        ))),
    }
}

fn option_value(args: &[String], option: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window.first().is_some_and(|value| value == option))
        .and_then(|window| window.get(1).cloned())
}

/// Git-compatible ignore policy used by commit and parent-join capture.
///
/// Already tracked paths always remain eligible. Rules are evaluated in order;
/// later matches win and `!` negates an ignore rule. The matcher intentionally
/// covers the portable Git subset (`*`, `?`, directory suffixes, and anchored
/// paths) without consulting system Git.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitIgnorePolicy {
    rules: Vec<GitIgnoreRule>,
}

/// Exact ignore-aware snapshot produced for one compatibility commit.
pub struct GitCapturedGeneration<A, O> {
    /// Immutable generation containing only compatibility-eligible paths.
    pub generation: Generation<A, O>,
    /// Eligible non-directory paths represented by the snapshot.
    pub tracked_paths: BTreeSet<String>,
}

/// Portable namespace fact used by Git-shaped inspection commands.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitTreeEntry {
    /// Slash-separated path relative to the compatibility root.
    pub path: String,
    /// Stable textual filesystem kind.
    pub kind: String,
}

/// One literal grep match in an authenticated generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitGrepMatch {
    /// Slash-separated path relative to the compatibility root.
    pub path: String,
    /// One-based logical line number.
    pub line: u64,
    /// Complete UTF-8 line without its terminator.
    pub text: String,
}

/// Bounded machine-readable grep result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitGrepResult {
    /// Literal matching lines in stable path/line order.
    pub matches: Vec<GitGrepMatch>,
    /// More paths, bytes, or matches existed beyond the supplied bounds.
    pub truncated: bool,
}

/// Fail-closed unified-patch application error.
#[derive(Debug, Error)]
pub enum GitPatchError {
    /// Workspace access or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Patch bytes are not UTF-8 unified text.
    #[error("Git patch is not UTF-8 unified text")]
    NonText,
    /// Patch syntax is incomplete or unsupported.
    #[error("invalid Git unified patch: {0}")]
    Invalid(String),
    /// A hunk does not match the authenticated working generation.
    #[error("Git patch hunk does not apply to '{path}' at source line {line}")]
    Conflict {
        /// Portable path whose preimage did not match.
        path: String,
        /// One-based source line from the hunk header.
        line: usize,
    },
}

/// One line attributed across explicit compatibility history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitBlameLine {
    /// Compatibility commit that last changed this line position.
    pub commit: GitCommitId,
    /// One-based line number in the selected version.
    pub line: u64,
    /// Complete UTF-8 line without its terminator.
    pub text: String,
}

/// Walks an authenticated tree in stable portable path order.
pub async fn walk_git_tree<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
    root: Option<&str>,
    maximum_entries: u32,
) -> Result<Vec<GitTreeEntry>, WorkspaceError> {
    if maximum_entries == 0 {
        return Ok(Vec::new());
    }
    let root = root.unwrap_or("").trim_matches('/');
    let start = if root.is_empty() {
        "/".to_owned()
    } else {
        format!("/{root}")
    };
    if !root.is_empty() {
        let stat = generation.stat(&start).await?;
        if stat.kind != FileKind::Directory {
            return Ok(vec![GitTreeEntry {
                path: root.to_owned(),
                kind: git_kind(stat.kind).to_owned(),
            }]);
        }
    }
    let maximum = usize::try_from(maximum_entries).unwrap_or(usize::MAX);
    let mut entries = Vec::new();
    let mut pending = vec![start];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page = generation
                .list_directory(&directory, after.as_ref(), 1_024)
                .await?;
            for entry in &page.entries {
                let name = match entry.name.encoding() {
                    NameEncoding::Utf8 => std::str::from_utf8(entry.name.as_bytes())
                        .map_err(|_| WorkspaceError::path("non-UTF-8 Git path"))?,
                    NameEncoding::PosixBytes | NameEncoding::WindowsUtf16Le => {
                        return Err(WorkspaceError::path("non-portable Git path"));
                    }
                };
                let parent = directory.trim_start_matches('/');
                let path = if parent.is_empty() {
                    name.to_owned()
                } else {
                    format!("{parent}/{name}")
                };
                entries.push(GitTreeEntry {
                    path: path.clone(),
                    kind: git_kind(entry.kind).to_owned(),
                });
                if entries.len() >= maximum {
                    entries.sort_by(|left, right| left.path.cmp(&right.path));
                    return Ok(entries);
                }
                if entry.kind == FileKind::Directory {
                    pending.push(format!("/{path}"));
                }
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

/// Searches UTF-8 regular files without consulting the host filesystem.
pub async fn grep_git_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
    pattern: &str,
    root: Option<&str>,
    maximum_entries: u32,
    maximum_file_bytes: u64,
    maximum_matches: u32,
) -> Result<GitGrepResult, WorkspaceError> {
    if maximum_entries == 0 {
        return Ok(GitGrepResult {
            matches: Vec::new(),
            truncated: true,
        });
    }
    let cancellation = CancellationToken::new();
    let checkout = generation
        .workspace
        .engine_checkout(
            GenerationSelector::Exact(generation.id()),
            CheckoutMode::read_only_pinned(),
        )
        .await?;
    let reader = checkout.pinned_reader().map_err(WorkspaceError::engine)?;
    let limits = checkout.volume_config().limits;
    let root = root.unwrap_or("/");
    let root_path = customer_path(&format!("/{}", root.trim_start_matches('/')), limits)?;
    let root_display = root.trim_matches('/').to_owned();
    let maximum_entries = usize::try_from(maximum_entries).unwrap_or(usize::MAX);
    let (regular, mut truncated) = resolved_git_grep_files(
        &reader,
        root_display,
        root_path,
        limits,
        maximum_entries,
        &cancellation,
    )
    .await?;
    let mut matches = Vec::new();
    let maximum_matches = usize::try_from(maximum_matches).unwrap_or(usize::MAX);
    for batch in regular.chunks(GREP_READ_CONCURRENCY) {
        let mut requests = Vec::with_capacity(batch.len());
        let mut selected = Vec::with_capacity(batch.len());
        for (path, file) in batch {
            if file.description().logical_bytes > maximum_file_bytes {
                truncated = true;
                continue;
            }
            requests.push(ResolvedFileRangeReadRequest {
                file,
                range: ByteRange {
                    offset: 0,
                    length: file.description().logical_bytes,
                },
            });
            selected.push(path);
        }
        if requests.is_empty() {
            continue;
        }
        let reads = reader
            .read_resolved_ranges(
                &requests,
                GREP_READ_CONCURRENCY,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value;
        for (path, read) in selected.into_iter().zip(reads) {
            let bytes = read.bytes;
            let Ok(text) = std::str::from_utf8(&bytes) else {
                continue;
            };
            for (index, line) in text.lines().enumerate() {
                if line.contains(pattern) {
                    if matches.len() >= maximum_matches {
                        return Ok(GitGrepResult {
                            matches,
                            truncated: true,
                        });
                    }
                    matches.push(GitGrepMatch {
                        path: path.clone(),
                        line: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                        text: line.to_owned(),
                    });
                }
            }
        }
    }
    Ok(GitGrepResult { matches, truncated })
}

/// Attributes UTF-8 lines using explicit oldest-to-newest compatibility
/// snapshots. This deliberately follows exact line positions; rename and
/// heuristic similarity detection remain presentation concerns.
pub async fn blame_git_generations<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    history_newest_first: &[(GitCommit, Generation<A, O>)],
    path: &str,
    maximum_file_bytes: u64,
) -> Result<Vec<GitBlameLine>, WorkspaceError> {
    let relative = path.trim_start_matches('/');
    let absolute = format!("/{relative}");
    let mut current = Vec::<String>::new();
    let mut owners = Vec::<GitCommitId>::new();
    for (commit, generation) in history_newest_first.iter().rev() {
        let next = if commit.tracked_paths.contains(relative) {
            let bytes = generation.read(&absolute, maximum_file_bytes).await?;
            std::str::from_utf8(&bytes)
                .map_err(|_| WorkspaceError::path("Git blame requires UTF-8 text"))?
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mut next_owners = Vec::with_capacity(next.len());
        for (index, line) in next.iter().enumerate() {
            next_owners.push(if current.get(index) == Some(line) {
                owners.get(index).copied().unwrap_or(commit.id)
            } else {
                commit.id
            });
        }
        current = next;
        owners = next_owners;
    }
    Ok(current
        .into_iter()
        .zip(owners)
        .enumerate()
        .map(|(index, (text, commit))| GitBlameLine {
            commit,
            line: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
            text,
        })
        .collect())
}

#[derive(Clone, Debug)]
struct UnifiedPatch {
    old_path: Option<String>,
    new_path: Option<String>,
    hunks: Vec<UnifiedHunk>,
}

#[derive(Clone, Debug)]
struct UnifiedHunk {
    old_start: usize,
    lines: Vec<String>,
}

/// Applies a bounded UTF-8 unified patch as one atomic workspace transaction.
/// System Git, a Git object database, and host-path writes are never involved.
pub async fn apply_git_patch<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    patch: &[u8],
    idempotency_key: IdempotencyKey,
) -> Result<TransactionCommit<A, O>, GitPatchError> {
    if let Some(generation) = workspace.operation_generation(idempotency_key).await? {
        return Ok(TransactionCommit::AlreadyCommitted(generation));
    }
    let patches = parse_unified_patch(patch)?;
    let head = workspace.head().await?;
    let mut transaction = workspace.begin_transaction(idempotency_key).await?;
    for patch in patches {
        let path = patch
            .new_path
            .as_ref()
            .or(patch.old_path.as_ref())
            .ok_or_else(|| GitPatchError::Invalid("patch has no path".to_owned()))?;
        let original = if patch.old_path.is_none() {
            String::new()
        } else {
            let bytes = head
                .read(&format!("/{path}"), MAXIMUM_PATCH_FILE_BYTES)
                .await?;
            String::from_utf8(bytes.to_vec()).map_err(|_| GitPatchError::NonText)?
        };
        let replacement = apply_unified_hunks(path, &original, &patch.hunks)?;
        if patch.new_path.is_none() {
            if !replacement.is_empty() {
                return Err(GitPatchError::Invalid(
                    "deleted-file patch leaves content".to_owned(),
                ));
            }
            transaction.remove(&format!("/{path}")).await?;
            continue;
        }
        if let Some((parent, _)) = path.rsplit_once('/') {
            transaction.create_dir_all(&format!("/{parent}")).await?;
        }
        transaction
            .write_text(&format!("/{path}"), &replacement)
            .await?;
    }
    transaction.commit().await.map_err(Into::into)
}

#[allow(
    clippy::indexing_slicing,
    reason = "every patch line index is bounded by the surrounding length checks"
)]
fn parse_unified_patch(bytes: &[u8]) -> Result<Vec<UnifiedPatch>, GitPatchError> {
    let text = std::str::from_utf8(bytes).map_err(|_| GitPatchError::NonText)?;
    if text.contains("GIT binary patch") {
        return Err(GitPatchError::Invalid(
            "binary patches require a typed binary mutation".to_owned(),
        ));
    }
    let lines: Vec<_> = text.split_inclusive('\n').collect();
    let mut patches = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        if !lines[index].starts_with("--- ") {
            index += 1;
            continue;
        }
        let old_path = parse_patch_path(lines[index].trim_end_matches(['\r', '\n']), "--- ")?;
        index += 1;
        let Some(next) = lines.get(index) else {
            return Err(GitPatchError::Invalid("missing +++ header".to_owned()));
        };
        let new_path = parse_patch_path(next.trim_end_matches(['\r', '\n']), "+++ ")?;
        index += 1;
        let mut hunks = Vec::new();
        while index < lines.len() && !lines[index].starts_with("--- ") {
            if !lines[index].starts_with("@@ ") {
                index += 1;
                continue;
            }
            let old_start = parse_hunk_old_start(lines[index])?;
            index += 1;
            let mut hunk_lines: Vec<String> = Vec::new();
            while index < lines.len()
                && !lines[index].starts_with("@@ ")
                && !lines[index].starts_with("--- ")
            {
                let line = lines[index];
                if line.starts_with('\\') {
                    let previous = hunk_lines.last_mut().ok_or_else(|| {
                        GitPatchError::Invalid(
                            "no-newline marker does not follow a hunk line".to_owned(),
                        )
                    })?;
                    if previous.ends_with('\n') {
                        previous.pop();
                        if previous.ends_with('\r') {
                            previous.pop();
                        }
                    }
                } else if line.starts_with([' ', '+', '-']) {
                    hunk_lines.push(line.to_owned());
                } else {
                    return Err(GitPatchError::Invalid(
                        "hunk line lacks a unified-diff prefix".to_owned(),
                    ));
                }
                index += 1;
            }
            hunks.push(UnifiedHunk {
                old_start,
                lines: hunk_lines,
            });
        }
        if hunks.is_empty() {
            return Err(GitPatchError::Invalid("patch contains no hunks".to_owned()));
        }
        patches.push(UnifiedPatch {
            old_path,
            new_path,
            hunks,
        });
    }
    if patches.is_empty() {
        return Err(GitPatchError::Invalid("patch contains no files".to_owned()));
    }
    Ok(patches)
}

fn parse_patch_path(line: &str, prefix: &str) -> Result<Option<String>, GitPatchError> {
    let value = line
        .strip_prefix(prefix)
        .ok_or_else(|| GitPatchError::Invalid(format!("missing {prefix}header")))?
        .split('\t')
        .next()
        .unwrap_or_default();
    if value == "/dev/null" {
        return Ok(None);
    }
    let value = value
        .strip_prefix("a/")
        .or_else(|| value.strip_prefix("b/"))
        .unwrap_or(value)
        .trim_matches('/');
    if value.is_empty()
        || value.contains('\\')
        || value.contains(':')
        || value
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(GitPatchError::Invalid(
            "patch path escapes its root".to_owned(),
        ));
    }
    Ok(Some(value.to_owned()))
}

fn parse_hunk_old_start(line: &str) -> Result<usize, GitPatchError> {
    let range = line
        .strip_prefix("@@ -")
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| GitPatchError::Invalid("malformed hunk header".to_owned()))?;
    range
        .split(',')
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| GitPatchError::Invalid("malformed hunk source range".to_owned()))
}

fn apply_unified_hunks(
    path: &str,
    original: &str,
    hunks: &[UnifiedHunk],
) -> Result<String, GitPatchError> {
    let source: Vec<_> = original.split_inclusive('\n').collect();
    let mut output = String::new();
    let mut cursor = 0_usize;
    for hunk in hunks {
        let start = hunk.old_start.saturating_sub(1);
        if start < cursor || start > source.len() {
            return Err(GitPatchError::Conflict {
                path: path.to_owned(),
                line: hunk.old_start,
            });
        }
        for line in source.get(cursor..start).unwrap_or_default() {
            output.push_str(line);
        }
        cursor = start;
        for line in &hunk.lines {
            let Some(prefix) = line.chars().next() else {
                continue;
            };
            let payload = line.get(prefix.len_utf8()..).unwrap_or_default();
            match prefix {
                ' ' | '-' => {
                    if source.get(cursor).copied() != Some(payload) {
                        return Err(GitPatchError::Conflict {
                            path: path.to_owned(),
                            line: cursor.saturating_add(1),
                        });
                    }
                    if prefix == ' ' {
                        output.push_str(payload);
                    }
                    cursor = cursor.saturating_add(1);
                }
                '+' => output.push_str(payload),
                _ => {
                    return Err(GitPatchError::Invalid(
                        "unsupported hunk line prefix".to_owned(),
                    ));
                }
            }
        }
    }
    for line in source.get(cursor..).unwrap_or_default() {
        output.push_str(line);
    }
    Ok(output)
}

const fn git_kind(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Regular => "regular",
        FileKind::Directory => "directory",
        FileKind::SymbolicLink => "symbolic-link",
        FileKind::Fifo => "fifo",
        FileKind::Socket => "socket",
        FileKind::CharacterDevice => "character-device",
        FileKind::BlockDevice => "block-device",
        FileKind::ReparsePoint => "windows-reparse-point",
        FileKind::MountBoundary => "mount-boundary",
    }
}

/// Failure while producing an ignore-aware compatibility snapshot.
#[derive(Debug, Error)]
pub enum GitCaptureError {
    /// Workspace traversal, mutation, or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// A host-native name cannot be represented by Git's portable UTF-8 paths.
    #[error("Git compatibility cannot represent a non-UTF-8 path name")]
    NonPortableName,
    /// A deterministic capture workspace was concurrently changed.
    #[error("Git compatibility capture workspace changed concurrently")]
    Conflict,
}

/// Captures the exact eligible compatibility tree without changing the live
/// workspace. Already tracked paths remain eligible even when newly ignored.
///
/// The filtered snapshot is a cheap SDK fork. Its publication and retention
/// identities derive from `operation_id`, so interrupted captures resume
/// without rescanning or duplicating history.
pub async fn capture_git_compatible_generation<A, O>(
    workspace: &Workspace<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace.head().await?;
    let mut entries = Vec::new();
    let mut pending = vec!["/".to_owned()];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page = live
                .list_directory(&directory, after.as_ref(), 1_024)
                .await?;
            for entry in &page.entries {
                let name = match entry.name.encoding() {
                    NameEncoding::Utf8 => std::str::from_utf8(entry.name.as_bytes())
                        .map_err(|_| GitCaptureError::NonPortableName)?,
                    NameEncoding::PosixBytes | NameEncoding::WindowsUtf16Le => {
                        return Err(GitCaptureError::NonPortableName);
                    }
                };
                let path = if directory == "/" {
                    name.to_owned()
                } else {
                    format!("{}/{}", directory.trim_start_matches('/'), name)
                };
                let is_directory = entry.kind == FileKind::Directory;
                entries.push((path.clone(), is_directory));
                if is_directory {
                    pending.push(format!("/{path}"));
                }
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    entries.sort_by(|left, right| {
        path_depth(&left.0)
            .cmp(&path_depth(&right.0))
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut excluded = Vec::<String>::new();
    let mut tracked_paths = BTreeSet::new();
    for (path, is_directory) in &entries {
        if excluded.iter().any(|parent| is_path_below(path, parent)) {
            continue;
        }
        let tracked = already_tracked.contains(path);
        let tracked_descendant = *is_directory
            && already_tracked
                .iter()
                .any(|candidate| is_path_below(candidate, path));
        if !policy.eligible(path, *is_directory, tracked) && !tracked_descendant {
            excluded.push(path.clone());
        } else if !is_directory {
            tracked_paths.insert(path.clone());
        }
    }
    if excluded.is_empty() {
        return Ok(GitCapturedGeneration {
            generation: live,
            tracked_paths,
        });
    }
    let bytes = operation_id.into_bytes();
    let workspace_hash = hex::encode(&workspace.id().into_bytes()[..6]);
    let capture_name = format!("git-capture-{workspace_hash}-{}", hex::encode(bytes));
    let capture = workspace
        .fork(
            capture_name,
            crate::ForkOptions::from_generation(live, IdempotencyKey::from_bytes(bytes)),
        )
        .await?;
    let mut commit_bytes = bytes;
    commit_bytes[0] ^= 0xa5;
    let commit_key = IdempotencyKey::from_bytes(commit_bytes);
    let generation = if let Some(generation) = capture.operation_generation(commit_key).await? {
        generation
    } else {
        let mut transaction = capture.begin_transaction(commit_key).await?;
        for path in &excluded {
            transaction.remove(&format!("/{path}")).await?;
        }
        match transaction.commit().await? {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => generation,
            TransactionCommit::Conflict { .. }
            | TransactionCommit::Fenced
            | TransactionCommit::IdempotencyConflict => return Err(GitCaptureError::Conflict),
        }
    };
    let pin = format!("capture-{}", hex::encode(bytes));
    generation.pin(pin).await?;
    Ok(GitCapturedGeneration {
        generation,
        tracked_paths,
    })
}

fn path_depth(path: &str) -> usize {
    path.split('/').count()
}

fn is_path_below(candidate: &str, parent: &str) -> bool {
    candidate
        .strip_prefix(parent)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GitIgnoreRule {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
}

impl GitIgnorePolicy {
    /// Parses Gitignore-shaped UTF-8 lines.
    #[must_use]
    pub fn parse(contents: &str) -> Self {
        let rules = contents
            .lines()
            .filter_map(|line| {
                let mut line = line.trim_end_matches('\r');
                if line.is_empty() || line.starts_with('#') {
                    return None;
                }
                let negated = line.starts_with('!');
                if negated {
                    line = line.strip_prefix('!').unwrap_or(line);
                }
                let anchored = line.starts_with('/');
                if anchored {
                    line = line.strip_prefix('/').unwrap_or(line);
                }
                let directory_only = line.ends_with('/');
                if directory_only {
                    line = line.trim_end_matches('/');
                }
                if line.is_empty() {
                    return None;
                }
                Some(GitIgnoreRule {
                    pattern: line.to_owned(),
                    negated,
                    directory_only,
                    anchored,
                })
            })
            .collect();
        Self { rules }
    }

    /// Returns whether a path participates in compatibility commit/join state.
    #[must_use]
    pub fn eligible(&self, path: &str, is_directory: bool, already_tracked: bool) -> bool {
        if already_tracked {
            return true;
        }
        let path = path.trim_start_matches('/');
        let mut ignored = false;
        for rule in &self.rules {
            if ignore_matches(rule, path, is_directory) {
                ignored = !rule.negated;
            }
        }
        !ignored
    }
}

fn ignore_matches(rule: &GitIgnoreRule, path: &str, is_directory: bool) -> bool {
    if rule.directory_only {
        let components: Vec<_> = path.split('/').collect();
        let directory_count = if is_directory {
            components.len()
        } else {
            components.len().saturating_sub(1)
        };
        return (1..=directory_count).any(|end| {
            let prefix = components.get(..end).unwrap_or_default();
            let directory = prefix.join("/");
            if rule.anchored || rule.pattern.contains('/') {
                wildcard_matches(&rule.pattern, &directory)
            } else {
                prefix
                    .iter()
                    .any(|component| wildcard_matches(&rule.pattern, component))
            }
        });
    }
    if rule.anchored || rule.pattern.contains('/') {
        wildcard_matches(&rule.pattern, path)
    } else {
        path.split('/')
            .any(|component| wildcard_matches(&rule.pattern, component))
    }
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index, mut star, mut checkpoint) = (0, 0, None, 0);
    while value_index < value.len() {
        if pattern.get(pattern_index).is_some_and(|byte| {
            *byte == b'?' || value.get(value_index).is_some_and(|value| value == byte)
        }) {
            pattern_index += 1;
            value_index += 1;
        } else if pattern.get(pattern_index) == Some(&b'*') {
            star = Some(pattern_index);
            pattern_index += 1;
            checkpoint = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            checkpoint += 1;
            value_index = checkpoint;
        } else {
            return false;
        }
    }
    while pattern.get(pattern_index) == Some(&b'*') {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

/// Process-local compatibility state adapter for tests and embedded callers.
#[derive(Default)]
pub struct MemoryGitCompatStore {
    states: Mutex<BTreeMap<WorkspaceId, GitCompatState>>,
}

impl MemoryGitCompatStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Process-local compatibility store synchronization failure.
#[derive(Debug, Error)]
#[error("Git compatibility memory store is unavailable")]
pub struct MemoryGitCompatStoreError;

impl GitCompatStore for MemoryGitCompatStore {
    type Error = MemoryGitCompatStoreError;

    async fn load(&self, workspace_id: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.states
            .lock()
            .map_err(|_| MemoryGitCompatStoreError)
            .map(|states| states.get(&workspace_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        let mut states = self.states.lock().map_err(|_| MemoryGitCompatStoreError)?;
        let revision = states.get(&workspace_id).map_or(0, |state| state.revision);
        if revision != expected_revision {
            return Ok(false);
        }
        states.insert(workspace_id, replacement);
        Ok(true)
    }

    async fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        let mut states = self.states.lock().map_err(|_| MemoryGitCompatStoreError)?;
        let revision = states.get(&workspace_id).map_or(0, |state| state.revision);
        if revision != expected_revision {
            return Ok(false);
        }
        states.remove(&workspace_id);
        Ok(true)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::{Digest, Fs, WorkspaceName};

    #[derive(Debug, Error)]
    #[error("test executor failure")]
    struct TestExecutorError;

    struct TestExecutor {
        result: Result<GitFilesystemResult, TestExecutorError>,
        operations: Mutex<Vec<OperationId>>,
    }

    impl TestExecutor {
        fn returning(result: GitFilesystemResult) -> Self {
            Self {
                result: Ok(result),
                operations: Mutex::new(Vec::new()),
            }
        }

        fn failing() -> Self {
            Self {
                result: Err(TestExecutorError),
                operations: Mutex::new(Vec::new()),
            }
        }
    }

    impl GitFilesystemExecutor for TestExecutor {
        type Error = TestExecutorError;

        async fn execute(
            &self,
            operation_id: OperationId,
            _action: &GitFilesystemAction,
        ) -> Result<GitFilesystemResult, Self::Error> {
            self.operations
                .lock()
                .expect("operation lock")
                .push(operation_id);
            self.result
                .as_ref()
                .map(Clone::clone)
                .map_err(|_| TestExecutorError)
        }
    }

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(Digest::from_bytes([byte; 32]))
    }

    fn workspace() -> WorkspaceId {
        WorkspaceId::derive(
            [9; 16],
            &WorkspaceName::new("repo").expect("valid test workspace"),
        )
    }

    #[tokio::test]
    async fn auto_staging_commit_and_machine_readable_history() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let GitCommandOutput::Status(status) = repository
            .execute(GitCommand::Status, generation(1))
            .await
            .expect("status")
        else {
            panic!("expected status");
        };
        assert!(status.dirty);
        assert!(status.all_changes_staged);
        let GitCommandOutput::Prepared {
            transition,
            action: GitFilesystemAction::CaptureCommit { .. },
        } = repository
            .execute(
                GitCommand::Commit {
                    message: "initial".to_owned(),
                    author: "agent".to_owned(),
                    authored_at_seconds: 10,
                },
                generation(1),
            )
            .await
            .expect("capture commit")
        else {
            panic!("expected commit capture");
        };
        let GitCommandOutput::Committed(commit) = repository
            .complete_transition_result(
                transition,
                &GitFilesystemResult::Captured {
                    generation: generation(1),
                    workspace_id: workspace(),
                    tracked_paths: BTreeSet::from(["tracked.txt".to_owned()]),
                },
            )
            .await
            .expect("record commit")
        else {
            panic!("expected commit");
        };
        let GitCommandOutput::Commits(log) = repository
            .execute(GitCommand::Log { maximum: 10 }, generation(1))
            .await
            .expect("log")
        else {
            panic!("expected log");
        };
        assert_eq!(log, vec![commit]);
    }

    #[tokio::test]
    async fn composed_run_finalizes_commit_and_branch_actions() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let commit_executor = TestExecutor::returning(GitFilesystemResult::Captured {
            generation: generation(9),
            workspace_id: workspace(),
            tracked_paths: BTreeSet::from(["tracked.txt".to_owned()]),
        });
        let GitCommandOutput::Committed(commit) = repository
            .run(
                GitCommand::Commit {
                    message: "initial".to_owned(),
                    author: "agent".to_owned(),
                    authored_at_seconds: 10,
                },
                generation(1),
                &commit_executor,
            )
            .await
            .expect("composed commit")
        else {
            panic!("expected composed commit");
        };
        assert_eq!(commit.generation, generation(9));
        assert_eq!(commit.workspace_generation, Some(generation(1)));
        assert!(matches!(
            repository
                .execute(GitCommand::Status, generation(1))
                .await
                .expect("status"),
            GitCommandOutput::Status(GitStatus { dirty: false, .. })
        ));
        let child = WorkspaceId::derive(
            [9; 16],
            &WorkspaceName::new("feature-run").expect("valid workspace name"),
        );
        let branch_executor = TestExecutor::returning(GitFilesystemResult::Forked {
            workspace_id: child,
        });
        repository
            .run(
                GitCommand::Branch {
                    create: Some("feature-run".to_owned()),
                },
                generation(1),
                &branch_executor,
            )
            .await
            .expect("composed branch");
        assert!(matches!(
            repository
                .execute(GitCommand::Branch { create: None }, generation(1))
                .await
                .expect("branches"),
            GitCommandOutput::Branches { branches, .. }
                if branches.iter().any(|branch| branch.workspace_id == child)
        ));
    }

    #[tokio::test]
    async fn composed_run_leaves_failed_transition_for_exact_resume() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let commit_executor = TestExecutor::returning(GitFilesystemResult::Captured {
            generation: generation(1),
            workspace_id: workspace(),
            tracked_paths: BTreeSet::new(),
        });
        repository
            .run(
                GitCommand::Commit {
                    message: "initial".to_owned(),
                    author: "agent".to_owned(),
                    authored_at_seconds: 10,
                },
                generation(1),
                &commit_executor,
            )
            .await
            .expect("initial commit");
        let child = WorkspaceId::derive(
            [9; 16],
            &WorkspaceName::new("recover-run").expect("valid workspace name"),
        );
        repository
            .run(
                GitCommand::Branch {
                    create: Some("recover-run".to_owned()),
                },
                generation(1),
                &TestExecutor::returning(GitFilesystemResult::Forked {
                    workspace_id: child,
                }),
            )
            .await
            .expect("branch");
        let failed = repository
            .run(
                GitCommand::Switch {
                    branch: "recover-run".to_owned(),
                    create: false,
                },
                generation(2),
                &TestExecutor::failing(),
            )
            .await;
        assert!(matches!(failed, Err(GitCompatRunError::Executor(_))));
        let pending = repository
            .pending_transition()
            .await
            .expect("pending transition")
            .expect("durable pending transition");
        let executor = TestExecutor::returning(GitFilesystemResult::Applied {
            generation: Some(generation(1)),
        });
        assert!(
            repository
                .resume(&executor)
                .await
                .expect("resume")
                .is_some()
        );
        assert!(
            repository
                .pending_transition()
                .await
                .expect("pending cleared")
                .is_none()
        );
        assert_eq!(
            executor
                .operations
                .lock()
                .expect("operation lock")
                .as_slice(),
            &[pending.id.operation_id()]
        );
    }

    #[tokio::test]
    async fn capture_omits_newly_ignored_paths_but_keeps_tracked_descendants() {
        let fs = Fs::memory();
        let workspace = fs
            .create_workspace("git-capture-policy")
            .await
            .expect("workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("transaction");
        transaction
            .create_dir_all("/target/nested")
            .await
            .expect("directories");
        transaction
            .write_text("/target/drop.bin", "drop")
            .await
            .expect("ignored file");
        transaction
            .write_text("/target/nested/keep.txt", "keep")
            .await
            .expect("tracked file");
        transaction.commit().await.expect("commit fixture");
        let captured = capture_git_compatible_generation(
            &workspace,
            &GitIgnorePolicy::parse("target/\n"),
            &BTreeSet::from(["target/nested/keep.txt".to_owned()]),
            OperationId::from_bytes([0x31; 16]),
        )
        .await
        .expect("capture");
        assert!(captured.generation.stat("/target/drop.bin").await.is_err());
        assert!(
            captured
                .generation
                .stat("/target/nested/keep.txt")
                .await
                .is_ok()
        );
        assert_eq!(
            captured.tracked_paths,
            BTreeSet::from(["target/nested/keep.txt".to_owned()])
        );
        let recovered = capture_git_compatible_generation(
            &workspace,
            &GitIgnorePolicy::parse("target/\n"),
            &BTreeSet::from(["target/nested/keep.txt".to_owned()]),
            OperationId::from_bytes([0x31; 16]),
        )
        .await
        .expect("recover capture");
        assert_eq!(recovered.generation.id(), captured.generation.id());
    }

    #[tokio::test]
    async fn tree_walk_and_grep_are_generation_bound_and_portable() {
        let fs = crate::Fs::memory();
        let workspace = fs
            .create_workspace("git-inspection")
            .await
            .expect("workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x32; 16]))
            .await
            .expect("transaction");
        transaction.create_dir_all("/src").await.expect("directory");
        transaction
            .write_text("/src/a.txt", "one\nneedle\n")
            .await
            .expect("first file");
        transaction
            .write_text("/src/b.txt", "needle two\n")
            .await
            .expect("second file");
        transaction
            .write_text("/src/large.txt", "needle is too large")
            .await
            .expect("large file");
        transaction
            .write(
                "/src/binary.bin",
                bytes::Bytes::from_static(b"needle\xffbinary"),
            )
            .await
            .expect("binary file");
        let TransactionCommit::Committed(generation) = transaction.commit().await.expect("commit")
        else {
            panic!("fixture did not commit");
        };
        let entries = walk_git_tree(&generation, Some("src"), 10)
            .await
            .expect("walk");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["src/a.txt", "src/b.txt", "src/binary.bin", "src/large.txt"]
        );
        let result = grep_git_generation(&generation, "needle", Some("src"), 10, 16, 10)
            .await
            .expect("grep");
        assert!(result.truncated);
        assert_eq!(result.matches.len(), 2);
        assert_eq!(result.matches[0].line, 2);
        assert_eq!(result.matches[1].line, 1);

        let single = grep_git_generation(&generation, "needle", Some("src/a.txt"), 1, 16, 10)
            .await
            .expect("grep one file");
        assert!(!single.truncated);
        assert_eq!(single.matches.len(), 1);
        assert_eq!(single.matches[0].path, "src/a.txt");
        assert_eq!(single.matches[0].line, 2);
    }

    #[tokio::test]
    async fn patch_application_is_atomic_conflict_checked_and_idempotent() {
        let fs = crate::Fs::memory();
        let workspace = fs.create_workspace("git-patch").await.expect("workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x33; 16]))
            .await
            .expect("transaction");
        transaction
            .write_text("/a.txt", "one\nold\n")
            .await
            .expect("fixture");
        transaction
            .write_text("/gone.txt", "gone")
            .await
            .expect("no-newline fixture");
        transaction.commit().await.expect("commit fixture");

        let patch = b"--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n one\n-old\n+new\n";
        let operation = IdempotencyKey::from_bytes([0x34; 16]);
        let TransactionCommit::Committed(generation) =
            apply_git_patch(&workspace, patch, operation)
                .await
                .expect("apply")
        else {
            panic!("patch did not commit");
        };
        assert_eq!(
            generation
                .read("/a.txt", 1_024)
                .await
                .expect("patched file")
                .as_ref(),
            b"one\nnew\n"
        );
        assert!(matches!(
            apply_git_patch(&workspace, patch, operation)
                .await
                .expect("retry"),
            TransactionCommit::AlreadyCommitted(_)
        ));

        let conflicting = b"--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,2 @@\n one\n-old\n+other\n";
        assert!(matches!(
            apply_git_patch(
                &workspace,
                conflicting,
                IdempotencyKey::from_bytes([0x35; 16])
            )
            .await,
            Err(GitPatchError::Conflict { .. })
        ));

        let add_and_delete = b"--- /dev/null\n+++ b/nested/new.txt\n@@ -0,0 +1 @@\n+made\n--- a/gone.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-gone\n\\ No newline at end of file\n";
        let TransactionCommit::Committed(generation) = apply_git_patch(
            &workspace,
            add_and_delete,
            IdempotencyKey::from_bytes([0x36; 16]),
        )
        .await
        .expect("add and delete") else {
            panic!("add/delete patch did not commit");
        };
        assert_eq!(
            generation
                .read("/nested/new.txt", 1_024)
                .await
                .expect("created file")
                .as_ref(),
            b"made\n"
        );
        assert!(generation.stat("/gone.txt").await.is_err());
        assert!(matches!(
            parse_unified_patch(b"--- C:/outside\n+++ C:/outside\n@@ -0,0 +1 @@\n+x\n"),
            Err(GitPatchError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn bisect_uses_durable_first_parent_generation_checkouts() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let mut commits = Vec::new();
        let mut expected = None;
        for byte in 1..=5 {
            let GitCommandOutput::Committed(commit) = repository
                .record_commit(
                    expected,
                    generation(byte),
                    BTreeSet::from(["tracked.txt".to_owned()]),
                    format!("commit {byte}"),
                    "agent".to_owned(),
                    i64::from(byte),
                )
                .await
                .expect("record commit")
            else {
                panic!("expected recorded commit");
            };
            expected = Some(commit.id);
            commits.push(commit.id);
        }

        let GitCommandOutput::Bisect(started) = repository
            .run(
                GitCommand::Bisect {
                    arguments: vec!["start".to_owned(), commits[4].to_hex(), commits[0].to_hex()],
                },
                generation(5),
                &TestExecutor::returning(GitFilesystemResult::Applied {
                    generation: Some(generation(3)),
                }),
            )
            .await
            .expect("start bisect")
        else {
            panic!("expected bisect state");
        };
        assert_eq!(started.current, Some(commits[2]));
        assert_eq!(started.remaining, 3);

        let GitCommandOutput::Bisect(narrowed) = repository
            .run(
                GitCommand::Bisect {
                    arguments: vec!["good".to_owned()],
                },
                generation(3),
                &TestExecutor::returning(GitFilesystemResult::Applied {
                    generation: Some(generation(4)),
                }),
            )
            .await
            .expect("mark good")
        else {
            panic!("expected narrowed bisect state");
        };
        assert_eq!(narrowed.current, Some(commits[3]));

        let GitCommandOutput::Bisect(done) = repository
            .run(
                GitCommand::Bisect {
                    arguments: vec!["bad".to_owned()],
                },
                generation(4),
                &TestExecutor::returning(GitFilesystemResult::Applied {
                    generation: Some(generation(4)),
                }),
            )
            .await
            .expect("mark bad")
        else {
            panic!("expected completed bisect state");
        };
        assert_eq!(done.first_bad, Some(commits[3]));
        assert_eq!(done.remaining, 0);

        let GitCommandOutput::Bisect(reset) = repository
            .run(
                GitCommand::Bisect {
                    arguments: vec!["reset".to_owned()],
                },
                generation(4),
                &TestExecutor::returning(GitFilesystemResult::Applied {
                    generation: Some(generation(5)),
                }),
            )
            .await
            .expect("reset bisect")
        else {
            panic!("expected reset bisect state");
        };
        assert!(!reset.active);
    }

    #[tokio::test]
    async fn cached_diff_is_head_to_workspace_and_add_is_noop() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let GitCommandOutput::Prepared {
            transition,
            action: GitFilesystemAction::CaptureCommit { .. },
        } = repository
            .execute(
                GitCommand::Commit {
                    message: "initial".to_owned(),
                    author: "agent".to_owned(),
                    authored_at_seconds: 10,
                },
                generation(1),
            )
            .await
            .expect("capture commit")
        else {
            panic!("expected commit capture");
        };
        repository
            .complete_transition_result(
                transition,
                &GitFilesystemResult::Captured {
                    generation: generation(1),
                    workspace_id: workspace(),
                    tracked_paths: BTreeSet::new(),
                },
            )
            .await
            .expect("record commit");
        assert_eq!(
            repository
                .execute(
                    GitCommand::Add {
                        paths: vec![".".to_owned()],
                    },
                    generation(2),
                )
                .await
                .expect("add"),
            GitCommandOutput::NoOp
        );
        assert!(matches!(
            repository
                .execute(GitCommand::Diff { cached: true }, generation(2))
                .await
                .expect("diff"),
            GitCommandOutput::Prepared {
                action: GitFilesystemAction::Diff {
                    from: Some(from),
                    to
                },
                ..
            } if from.generation == generation(1)
                && from.workspace_id == workspace()
                && to.generation == generation(2)
                && to.workspace_id == workspace()
        ));
    }

    #[tokio::test]
    async fn every_branch_requests_and_registers_a_distinct_workspace() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let GitCommandOutput::Prepared {
            transition,
            action:
                GitFilesystemAction::ForkBranch {
                    source_workspace,
                    source_generation,
                    head,
                    switch,
                    ..
                },
        } = repository
            .execute(
                GitCommand::Switch {
                    branch: "feature".to_owned(),
                    create: true,
                },
                generation(4),
            )
            .await
            .expect("fork request")
        else {
            panic!("expected a core workspace fork request");
        };
        assert_eq!(source_workspace, workspace());
        assert_eq!(source_generation, generation(4));
        assert!(head.is_none());
        assert!(switch);
        let child = WorkspaceId::derive(
            [9; 16],
            &WorkspaceName::new("feature").expect("valid workspace name"),
        );
        repository
            .complete_transition_result(
                transition,
                &GitFilesystemResult::Forked {
                    workspace_id: child,
                },
            )
            .await
            .expect("register branch workspace");
        let GitCommandOutput::Status(status) = repository
            .execute(GitCommand::Status, generation(4))
            .await
            .expect("feature status")
        else {
            panic!("expected status");
        };
        assert_eq!(status.branch, "feature");
    }

    #[tokio::test]
    async fn argv_covers_local_history_and_rejects_transport() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        assert!(matches!(
            repository
                .execute_argv(
                    &["reset".to_owned(), "--hard".to_owned(), "HEAD".to_owned()],
                    generation(1),
                    "agent",
                    10,
                )
                .await,
            Err(GitCompatError::UnbornHead)
        ));
        assert!(matches!(
            repository
                .execute_argv(&["clean".to_owned()], generation(1), "agent", 10,)
                .await,
            Err(GitCompatError::InvalidCommand(message)) if message.contains("requires -f")
        ));
        assert!(matches!(
            repository
                .execute_argv(
                    &["clean".to_owned(), "--dry-run".to_owned()],
                    generation(1),
                    "agent",
                    10,
                )
                .await,
            Ok(GitCommandOutput::Prepared {
                action: GitFilesystemAction::Clean { dry_run: true, .. },
                ..
            })
        ));
        assert!(matches!(
            repository
                .execute_argv(&["push".to_owned()], generation(1), "agent", 10,)
                .await,
            Err(GitCompatError::Unsupported { .. })
        ));
    }

    #[tokio::test]
    async fn state_changing_filesystem_actions_are_prepared_then_completed() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let GitCommandOutput::Prepared {
            transition,
            action: GitFilesystemAction::CaptureCommit { .. },
        } = repository
            .execute(
                GitCommand::Commit {
                    message: "initial".to_owned(),
                    author: "agent".to_owned(),
                    authored_at_seconds: 10,
                },
                generation(1),
            )
            .await
            .expect("prepare commit")
        else {
            panic!("expected commit capture");
        };
        let GitCommandOutput::Committed(commit) = repository
            .complete_transition_result(
                transition,
                &GitFilesystemResult::Captured {
                    generation: generation(1),
                    workspace_id: workspace(),
                    tracked_paths: BTreeSet::new(),
                },
            )
            .await
            .expect("record commit")
        else {
            panic!("expected commit");
        };
        repository
            .register_branch_workspace("feature", workspace(), Some(commit.id), false)
            .await
            .expect("register branch");
        let GitCommandOutput::Prepared { transition, .. } = repository
            .execute(
                GitCommand::Switch {
                    branch: "feature".to_owned(),
                    create: false,
                },
                generation(2),
            )
            .await
            .expect("prepare switch")
        else {
            panic!("expected prepared switch");
        };
        assert!(matches!(
            repository
                .execute(GitCommand::Status, generation(2))
                .await,
            Err(GitCompatError::TransitionPending(id)) if id == transition
        ));
        repository
            .complete_transition(transition, None)
            .await
            .expect("complete switch");
        let GitCommandOutput::Status(status) = repository
            .execute(GitCommand::Status, generation(1))
            .await
            .expect("status")
        else {
            panic!("expected status");
        };
        assert_eq!(status.branch, "feature");
    }

    #[test]
    fn ignore_policy_keeps_tracked_paths_and_honors_last_match() {
        let policy = GitIgnorePolicy::parse("target/\n*.log\n!important.log\n");
        assert!(!policy.eligible("target", true, false));
        assert!(!policy.eligible("target/cache.bin", false, false));
        assert!(!policy.eligible("nested/target/cache.bin", false, false));
        assert!(!policy.eligible("nested/debug.log", false, false));
        assert!(policy.eligible("nested/important.log", false, false));
        assert!(policy.eligible("target/cache", false, true));
    }
}
