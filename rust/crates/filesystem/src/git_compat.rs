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

const STATE_VERSION: u32 = 8;
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
    /// Git-visible committed tree. Lazy trees retain unresolved source state.
    pub tree: GitTreeRef,
    /// Complete live working tree captured by the commit. This may differ from
    /// `tree` when newly ignored paths are omitted from compatibility history.
    pub workspace_tree: GitTreeRef,
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
        tree: GitTreeRef,
        parents: Vec<GitCommitId>,
        author: impl Into<String>,
        authored_at_seconds: i64,
        message: impl Into<String>,
    ) -> Self {
        Self::new_with_workspace_tree(tree, tree, parents, author, authored_at_seconds, message)
    }

    fn new_with_workspace_tree(
        tree: GitTreeRef,
        workspace_tree: GitTreeRef,
        parents: Vec<GitCommitId>,
        author: impl Into<String>,
        authored_at_seconds: i64,
        message: impl Into<String>,
    ) -> Self {
        Self::new_with_metadata(
            tree,
            workspace_tree,
            BTreeSet::new(),
            parents,
            author,
            authored_at_seconds,
            message,
        )
    }

    fn new_with_metadata(
        tree: GitTreeRef,
        workspace_tree: GitTreeRef,
        tracked_paths: BTreeSet<String>,
        parents: Vec<GitCommitId>,
        author: impl Into<String>,
        authored_at_seconds: i64,
        message: impl Into<String>,
    ) -> Self {
        let author = author.into();
        let message = message.into();
        let mut hasher = blake3::Hasher::new();
        hasher.update(COMMIT_DOMAIN);
        hash_tree_ref(&mut hasher, tree);
        hash_tree_ref(&mut hasher, workspace_tree);
        hasher.update(
            &u64::try_from(tracked_paths.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for path in &tracked_paths {
            hash_bytes(&mut hasher, path.as_bytes());
        }
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
            tree,
            workspace_tree,
            tracked_paths,
            parents,
            author,
            authored_at_seconds,
            message,
        }
    }
}

fn hash_tree_ref(hasher: &mut blake3::Hasher, tree: GitTreeRef) {
    match tree {
        GitTreeRef::Exact(reference) => {
            hasher.update(&[0]);
            hasher.update(&reference.workspace_id.into_bytes());
            hasher.update(reference.generation.digest().as_bytes());
        }
        GitTreeRef::Lazy(reference) => {
            hasher.update(&[1]);
            hasher.update(&reference.id.into_bytes());
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

impl From<GitGenerationRef> for GitTreeRef {
    fn from(value: GitGenerationRef) -> Self {
        Self::Exact(value)
    }
}

/// One Git-visible tree, either fully authored or lazily source-backed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GitTreeRef {
    /// Exact authenticated authored generation.
    Exact(GitGenerationRef),
    /// Constant-size logical snapshot retaining unresolved source semantics.
    Lazy(crate::LazySnapshotRef),
}

impl GitTreeRef {
    /// Constructs an exact authored tree reference.
    #[must_use]
    pub const fn exact(workspace_id: WorkspaceId, generation: GenerationId) -> Self {
        Self::Exact(GitGenerationRef {
            workspace_id,
            generation,
        })
    }
    /// Workspace owning the authored component.
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        match self {
            Self::Exact(reference) => reference.workspace_id,
            Self::Lazy(reference) => reference.workspace_id,
        }
    }

    /// Authenticated authored generation component.
    #[must_use]
    pub const fn authored_generation(self) -> GenerationId {
        match self {
            Self::Exact(reference) => reference.generation,
            Self::Lazy(reference) => reference.authored_generation,
        }
    }
}

/// Ergonomic input accepted by the Git façade for a live workspace tree.
pub trait IntoGitTreeRef {
    /// Resolves the input using the repository workspace for exact generations.
    fn into_git_tree_ref(self, repository_workspace: WorkspaceId) -> GitTreeRef;
}

impl IntoGitTreeRef for GitTreeRef {
    fn into_git_tree_ref(self, _repository_workspace: WorkspaceId) -> GitTreeRef {
        self
    }
}

impl IntoGitTreeRef for GenerationId {
    fn into_git_tree_ref(self, repository_workspace: WorkspaceId) -> GitTreeRef {
        GitTreeRef::exact(repository_workspace, self)
    }
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
    /// Cross-repository parents introduced by direct child publication.
    /// These identities are explicit graph boundaries, never silently missing
    /// local records.
    #[serde(default)]
    pub external_parents: BTreeSet<GitCommitId>,
    /// Lightweight tags.
    pub tags: BTreeMap<String, GitCommitId>,
    /// Most-recent-first prior branch heads.
    pub reflog: Vec<Option<GitCommitId>>,
    /// Stashed exact generations, newest last.
    pub stash: Vec<GitTreeRef>,
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
            external_parents: BTreeSet::new(),
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
        /// Complete live tree that was captured.
        workspace_tree: GitTreeRef,
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
        /// Dirty tree retained by the stash.
        tree: GitTreeRef,
    },
    /// Remove the exact top stash after it is restored.
    StashPop {
        /// Exact top stash restored before removal.
        tree: GitTreeRef,
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
    /// Continue the one durable conflicted merge after ordinary workspace edits.
    MergeContinue,
    /// Restore the pre-merge working tree and clear the durable merge intent.
    MergeAbort,
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
    /// Resolve one compatibility revision or a small read-only probe.
    RevParse {
        /// Probe or object name accepted by the compatibility subset.
        argument: String,
    },
    /// Print the symbolic compatibility HEAD.
    SymbolicRef {
        /// Whether to omit the `refs/heads/` prefix.
        short: bool,
    },
    /// Find the nearest common explicit compatibility commit.
    MergeBase {
        /// First commit-like object.
        left: GitObjectName,
        /// Second commit-like object.
        right: GitObjectName,
    },
    /// List paths tracked by explicit compatibility history.
    LsFiles,
    /// Test paths against the live compatibility ignore policy.
    CheckIgnore {
        /// Portable paths to test.
        paths: Vec<String>,
    },
}

/// Filesystem work emitted by the compatibility state machine.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitFilesystemAction {
    /// Capture a compatibility-eligible generation before recording a commit.
    CaptureCommit {
        /// Complete live workspace tree.
        workspace_tree: GitTreeRef,
        /// Previous filtered compatibility tree, if HEAD exists.
        head_tree: Option<GitTreeRef>,
        /// Previous complete live tree, if recorded by HEAD.
        head_workspace_tree: Box<Option<GitTreeRef>>,
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
        /// Tree from which to fork the branch workspace.
        source_tree: GitTreeRef,
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
        from: Option<GitTreeRef>,
        /// Live workspace generation.
        to: GitTreeRef,
    },
    /// Move the live workspace to an exact generation.
    RestoreGeneration {
        /// Tree to install.
        tree: GitTreeRef,
        /// Paths participating in a compatibility-history restore. `None`
        /// requests an exact same-workspace head restoration (for stash).
        paths: Option<BTreeSet<String>>,
    },
    /// Restore selected paths from an exact generation.
    RestorePaths {
        /// Source tree.
        tree: GitTreeRef,
        /// Portable paths to replace.
        paths: Vec<String>,
    },
    /// Join a source workspace into the current workspace.
    Join {
        /// Exact target working tree captured before the join began.
        target_tree: GitTreeRef,
        /// Workspace owning the source branch.
        source_workspace: WorkspaceId,
        /// Whether to record rebase rather than merge ancestry.
        rebase: bool,
        /// Complete set of paths that may remain tracked after the join.
        tracked_paths: BTreeSet<String>,
    },
    /// Apply one commit relative to its first parent.
    ApplyCommit {
        /// Explicit compatibility commit.
        commit: GitCommitId,
        /// Whether to apply its inverse.
        reverse: bool,
        /// Exact earlier tree, or the empty compatibility tree.
        base: Option<GitTreeRef>,
        /// Exact later tree, or the empty compatibility tree.
        source: Option<GitTreeRef>,
        /// Complete portable path set participating in the delta.
        paths: BTreeSet<String>,
        /// Complete set of paths that may remain tracked after application.
        tracked_paths: BTreeSet<String>,
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
        tree: GitTreeRef,
    },
    /// Discover and optionally remove untracked paths.
    Clean {
        /// Whether mutation is disabled.
        dry_run: bool,
        /// Exact live tree to inspect before any deletion.
        tree: GitTreeRef,
        /// Paths protected as tracked by the current branch.
        tracked_paths: BTreeSet<String>,
    },
    /// Export one exact generation.
    Archive {
        /// Tree to export.
        tree: GitTreeRef,
    },
    /// Parse and apply a patch.
    ApplyPatch {
        /// Opaque patch bytes.
        patch: Vec<u8>,
    },
    /// Test paths against the ignore policy in one exact live tree.
    CheckIgnore {
        /// Paths to test without shell expansion.
        paths: Vec<String>,
        /// Exact live tree containing `.gitignore` files.
        tree: GitTreeRef,
    },
}

/// Typed result returned by a compatibility filesystem executor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitFilesystemResult {
    /// An eligible compatibility snapshot was captured.
    Captured {
        /// Filtered compatibility tree.
        tree: GitTreeRef,
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
        /// Resulting tree when the action changes workspace state.
        tree: Option<GitTreeRef>,
        /// Exact resulting tracked paths for generated compatibility commits.
        tracked_paths: Option<BTreeSet<String>>,
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
    fn resulting_tree(&self) -> Option<GitTreeRef> {
        match self {
            Self::Captured { tree, .. } => Some(*tree),
            Self::Applied { tree, .. } => *tree,
            Self::Forked { .. } | Self::Data { .. } => None,
        }
    }
}

/// Whether a lazy Git working tree is known to differ from compatibility HEAD.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GitDirtyState {
    /// Every represented path is known equal.
    Clean,
    /// At least one represented path is known different.
    Dirty,
    /// Unresolved source state prevents an exact answer without a scan.
    Unknown,
}

/// Backend-neutral execution boundary for Git-shaped filesystem work.
///
/// Implementations receive a stable operation identity for every action. They
/// must apply mutating actions idempotently and return only typed results; the
/// compatibility repository retains all history and sequencer authority.
pub trait GitFilesystemExecutor: Send + Sync {
    /// Executor failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Confirms that the caller still owns any writer lease governing this command.
    fn validate(&self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

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
    /// Live workspace tree.
    pub workspace: GitTreeRef,
    /// Whether HEAD and the workspace differ.
    pub dirty: GitDirtyState,
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
    /// Stable line-oriented value used by read-only Git probes.
    Text(String),
    /// Stable ordered portable-path result.
    Paths(Vec<String>),
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
    /// The supplied live tree belongs to a workspace other than the selected branch.
    #[error("Git compatibility live tree does not belong to the selected branch workspace")]
    WorkspaceMismatch,
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
    tree: GitTreeRef,
    workspace_tree: GitTreeRef,
    tracked_paths: BTreeSet<String>,
    message: String,
    author: String,
    authored_at_seconds: i64,
}

/// Compatibility history record produced by one distributed publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitPublicationRecord {
    /// Filtered Git-visible tree.
    pub tree: GitTreeRef,
    /// Complete live workspace tree after publication.
    pub workspace_tree: GitTreeRef,
    /// Optional compatibility head from the published child.
    pub source_head: Option<GitCommitId>,
    /// Paths represented by the filtered compatibility tree.
    pub tracked_paths: BTreeSet<String>,
    /// Merge commit message.
    pub message: String,
    /// Author identity.
    pub author: String,
    /// Unix epoch timestamp in seconds.
    pub authored_at_seconds: i64,
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
    /// Returns the paths tracked by the current compatibility branch.
    pub async fn tracked_paths(&self) -> Result<BTreeSet<String>, GitCompatError<S::Error>> {
        self.load()
            .await?
            .current()
            .map(|branch| branch.tracked_paths.clone())
            .map_err(|_| GitCompatError::InvalidState)
    }

    /// Returns the latest explicit compatibility commit for the current branch.
    pub async fn head(&self) -> Result<Option<GitCommitId>, GitCompatError<S::Error>> {
        self.load()
            .await?
            .current()
            .map(|branch| branch.head)
            .map_err(|_| GitCompatError::InvalidState)
    }

    /// Executes a command and all required filesystem work through one composed
    /// recovery-safe boundary.
    ///
    /// Adapters normally need only this method. State-only commands return
    /// immediately, action commands receive a deterministic operation identity,
    /// and prepared sequencer mutations remain durable if the executor fails.
    pub async fn run<E: GitFilesystemExecutor, T: IntoGitTreeRef>(
        &self,
        command: GitCommand,
        workspace_tree: T,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let workspace_tree = workspace_tree.into_git_tree_ref(self.workspace_id);
        self.validate_workspace_tree(workspace_tree).await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        match &command {
            GitCommand::MergeContinue => {
                return self.continue_join(workspace_tree, executor).await;
            }
            GitCommand::MergeAbort => return self.abort_join(executor).await,
            GitCommand::Add { .. } => {
                if let Some(pending) = self.pending_transition().await? {
                    if matches!(pending.mutation, GitPendingMutation::Join { .. }) {
                        // The compatibility working copy is always staged. During a
                        // conflicted join, `add` therefore records the user's intent
                        // without copying content; `merge --continue` captures the
                        // exact current workspace generation.
                        return Ok(GitCommandOutput::NoOp);
                    }
                    return Err(GitCompatError::TransitionPending(pending.id).into());
                }
            }
            _ => {}
        }
        let output = self.execute_validated(command, workspace_tree).await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        self.finish_output(output, workspace_tree, executor).await
    }

    async fn continue_join<E: GitFilesystemExecutor>(
        &self,
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let pending = self.pending_transition().await?.ok_or_else(|| {
            GitCompatError::InvalidCommand("merge --continue requires a pending merge".to_owned())
        })?;
        if !matches!(pending.mutation, GitPendingMutation::Join { .. }) {
            return Err(GitCompatError::TransitionPending(pending.id).into());
        }
        let GitFilesystemAction::Join { tracked_paths, .. } = &pending.action else {
            return Err(GitCompatError::InvalidState.into());
        };
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        self.complete_transition_result(
            pending.id,
            &GitFilesystemResult::Applied {
                tree: Some(workspace_tree),
                tracked_paths: Some(tracked_paths.clone()),
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn abort_join<E: GitFilesystemExecutor>(
        &self,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let pending = self.pending_transition().await?.ok_or_else(|| {
            GitCompatError::InvalidCommand("merge --abort requires a pending merge".to_owned())
        })?;
        if !matches!(pending.mutation, GitPendingMutation::Join { .. }) {
            return Err(GitCompatError::TransitionPending(pending.id).into());
        }
        let GitFilesystemAction::Join { target_tree, .. } = pending.action else {
            return Err(GitCompatError::InvalidState.into());
        };
        let mut operation_bytes = Vec::with_capacity(21);
        operation_bytes.extend_from_slice(&pending.id.into_bytes());
        operation_bytes.extend_from_slice(b"abort");
        let digest = blake3::hash(&operation_bytes);
        let mut operation_id = [0_u8; 16];
        operation_id.copy_from_slice(&digest.as_bytes()[..16]);
        let result = executor
            .execute(
                OperationId::from_bytes(operation_id),
                &GitFilesystemAction::RestoreGeneration {
                    tree: target_tree,
                    paths: None,
                },
            )
            .await
            .map_err(GitCompatRunError::Executor)?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        self.abort_transition(pending.id).await?;
        Ok(GitCommandOutput::Filesystem(result))
    }

    /// Parses argv following the `acyclic git` umbrella subcommand and executes
    /// it through [`Self::run`]. This API never intercepts or invokes system
    /// `git`.
    pub async fn run_argv<E: GitFilesystemExecutor, T: IntoGitTreeRef>(
        &self,
        argv: &[String],
        workspace_tree: T,
        default_author: &str,
        now_seconds: i64,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let command = parse_argv(argv, default_author, now_seconds)?;
        self.run(command, workspace_tree, executor).await
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
        executor
            .validate()
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
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        let (action, operation_id, transition) = match output {
            GitCommandOutput::Action(action) => {
                let operation_id = action_operation_id(self.workspace_id, workspace_tree, &action)?;
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
        executor
            .validate()
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
                    workspace_tree,
                    message,
                    author,
                    authored_at_seconds,
                    expected_head,
                    ..
                },
                GitFilesystemResult::Captured {
                    tree,
                    tracked_paths,
                },
            ) => self
                .record_captured_commit(GitCommitRecord {
                    expected_head,
                    tree,
                    workspace_tree,
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
    pub async fn execute<T: IntoGitTreeRef>(
        &self,
        command: GitCommand,
        workspace_tree: T,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let workspace_tree = workspace_tree.into_git_tree_ref(self.workspace_id);
        self.validate_workspace_tree(workspace_tree).await?;
        self.execute_validated(command, workspace_tree).await
    }

    async fn execute_validated(
        &self,
        command: GitCommand,
        workspace_tree: GitTreeRef,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            if let Some(pending) = &state.pending {
                return Err(GitCompatError::TransitionPending(pending.id));
            }
            let before = state.clone();
            let output = execute_command(&mut state, command.clone(), workspace_tree)
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
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }

    async fn validate_workspace_tree(
        &self,
        workspace_tree: GitTreeRef,
    ) -> Result<(), GitCompatError<S::Error>> {
        let state = self.load().await?;
        if state
            .current()
            .map_err(|_| GitCompatError::InvalidState)?
            .workspace_id
            != workspace_tree.workspace_id()
        {
            return Err(GitCompatError::WorkspaceMismatch);
        }
        Ok(())
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
        resulting_tree: Option<GitTreeRef>,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        self.complete_transition_result(
            transition,
            &GitFilesystemResult::Applied {
                tree: resulting_tree,
                tracked_paths: None,
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
            validate_completion_result(&state, &pending, result)?;
            let output = complete_pending(&mut state, pending.mutation, result)?;
            state.pending = None;
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self.compare_and_swap_state(expected, state).await? {
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
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(());
            }
        }
        Err(GitCompatError::Contended)
    }

    /// Parses and executes argv following the `acyclic git` umbrella
    /// subcommand. Callers strip the two-token prefix before invoking this
    /// method; bare system `git` is outside this façade.
    pub async fn execute_argv<T: IntoGitTreeRef>(
        &self,
        argv: &[String],
        workspace_tree: T,
        default_author: &str,
        now_seconds: i64,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let command = parse_argv(argv, default_author, now_seconds)?;
        self.execute(command, workspace_tree).await
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
            if self.compare_and_swap_state(expected, state).await? {
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
            tree: GitGenerationRef {
                workspace_id: self.workspace_id,
                generation,
            }
            .into(),
            workspace_tree: GitGenerationRef {
                workspace_id: self.workspace_id,
                generation,
            }
            .into(),
            tracked_paths,
            message: message.into(),
            author: author.into(),
            authored_at_seconds,
        })
        .await
    }

    /// Records a merge/publication performed by the distributed workspace
    /// layer without reimplementing compatibility history in an adapter.
    pub async fn record_publication(
        &self,
        publication: GitPublicationRecord,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let GitPublicationRecord {
            tree,
            workspace_tree,
            source_head,
            tracked_paths,
            message,
            author,
            authored_at_seconds,
        } = publication;
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let current = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .clone();
            if current
                .head
                .and_then(|head| state.commits.get(&head))
                .is_some_and(|commit| commit.tree == tree)
            {
                return Ok(GitCommandOutput::NoOp);
            }
            let mut parents = current.head.into_iter().collect::<Vec<_>>();
            if let Some(source_head) = source_head
                && !parents.contains(&source_head)
            {
                if !state.commits.contains_key(&source_head) {
                    state.external_parents.insert(source_head);
                }
                parents.push(source_head);
            }
            let commit = GitCommit::new_with_metadata(
                tree,
                workspace_tree,
                tracked_paths.clone(),
                parents,
                author.clone(),
                authored_at_seconds,
                message.clone(),
            );
            state.reflog.insert(0, current.head);
            state.commits.insert(commit.id, commit.clone());
            let branch = state
                .current_mut()
                .map_err(|_| GitCompatError::InvalidState)?;
            branch.head = Some(commit.id);
            branch.tracked_paths = tracked_paths.clone();
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self.compare_and_swap_state(expected, state).await? {
                return Ok(GitCommandOutput::Committed(commit));
            }
        }
        Err(GitCompatError::Contended)
    }

    async fn record_captured_commit(
        &self,
        record: GitCommitRecord,
    ) -> Result<GitCommandOutput, GitCompatError<S::Error>> {
        let GitCommitRecord {
            expected_head,
            tree,
            workspace_tree,
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
                    tree,
                    workspace_tree,
                    tracked_paths: tracked_paths.clone(),
                    message: message.clone(),
                    author: author.clone(),
                    authored_at_seconds,
                },
            )?;
            let expected = state.revision;
            state.revision = expected.saturating_add(1);
            if self.compare_and_swap_state(expected, state).await? {
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
        validate_git_state(&state).map_err(|()| GitCompatError::InvalidState)?;
        Ok(state)
    }

    async fn compare_and_swap_state(
        &self,
        expected: u64,
        state: GitCompatState,
    ) -> Result<bool, GitCompatError<S::Error>> {
        validate_git_state(&state).map_err(|()| GitCompatError::InvalidState)?;
        self.store
            .compare_and_swap(self.workspace_id, expected, state)
            .await
            .map_err(GitCompatError::Store)
    }
}

fn validate_completion_result<E: std::error::Error + 'static>(
    state: &GitCompatState,
    pending: &GitPendingTransition,
    result: &GitFilesystemResult,
) -> Result<(), GitCompatError<E>> {
    let workspace_id = state
        .current()
        .map_err(|_| GitCompatError::InvalidState)?
        .workspace_id;
    if result
        .resulting_tree()
        .is_some_and(|tree| tree.workspace_id() != workspace_id)
    {
        return Err(GitCompatError::WorkspaceMismatch);
    }
    if let GitPendingMutation::CaptureCommit { workspace_tree, .. } = pending.mutation
        && workspace_tree.workspace_id() != workspace_id
    {
        return Err(GitCompatError::WorkspaceMismatch);
    }
    Ok(())
}

fn validate_git_state(state: &GitCompatState) -> Result<(), ()> {
    if state.version != STATE_VERSION
        || state.branches.is_empty()
        || !state.branches.contains_key(&state.current_branch)
        || !state.legacy_tracked_paths.is_empty()
    {
        return Err(());
    }
    validate_git_branches(state)?;
    let known_workspaces: BTreeSet<_> = state
        .branches
        .values()
        .map(|branch| branch.workspace_id)
        .collect();
    validate_git_commits(state, &known_workspaces)?;
    validate_git_commit_graph(state)?;
    validate_git_references(state, &known_workspaces)
}

fn validate_git_branches(state: &GitCompatState) -> Result<(), ()> {
    for (name, branch) in &state.branches {
        let head_paths = match branch.head {
            Some(head) => Some(&state.commits.get(&head).ok_or(())?.tracked_paths),
            None => None,
        };
        if name.is_empty()
            || branch.name != *name
            || head_paths.map_or(!branch.tracked_paths.is_empty(), |tracked_paths| {
                tracked_paths != &branch.tracked_paths
            })
        {
            return Err(());
        }
    }
    Ok(())
}

fn validate_git_commits(
    state: &GitCompatState,
    known_workspaces: &BTreeSet<WorkspaceId>,
) -> Result<(), ()> {
    for (id, commit) in &state.commits {
        let rebuilt = GitCommit::new_with_metadata(
            commit.tree,
            commit.workspace_tree,
            commit.tracked_paths.clone(),
            commit.parents.clone(),
            commit.author.clone(),
            commit.authored_at_seconds,
            commit.message.clone(),
        );
        if *id != commit.id
            || rebuilt.id != commit.id
            || commit.tree.workspace_id() != commit.workspace_tree.workspace_id()
            || !known_workspaces.contains(&commit.tree.workspace_id())
            || commit.parents.iter().any(|parent| {
                !state.commits.contains_key(parent) && !state.external_parents.contains(parent)
            })
        {
            return Err(());
        }
    }
    Ok(())
}

fn validate_git_commit_graph(state: &GitCompatState) -> Result<(), ()> {
    let mut permanent = BTreeSet::new();
    for root in state.commits.keys().copied() {
        if permanent.contains(&root) {
            continue;
        }
        let mut active = BTreeSet::new();
        let mut stack = vec![(root, false)];
        while let Some((id, expanded)) = stack.pop() {
            if expanded {
                active.remove(&id);
                permanent.insert(id);
                continue;
            }
            if permanent.contains(&id) {
                continue;
            }
            if !active.insert(id) {
                return Err(());
            }
            stack.push((id, true));
            let commit = state.commits.get(&id).ok_or(())?;
            stack.extend(
                commit
                    .parents
                    .iter()
                    .rev()
                    .filter(|parent| state.commits.contains_key(parent))
                    .map(|parent| (*parent, false)),
            );
        }
    }
    Ok(())
}

fn validate_git_references(
    state: &GitCompatState,
    known_workspaces: &BTreeSet<WorkspaceId>,
) -> Result<(), ()> {
    let known_commit = |id: &GitCommitId| state.commits.contains_key(id);
    let known_reference =
        |id: &GitCommitId| state.commits.contains_key(id) || state.external_parents.contains(id);
    if state.external_parents.iter().any(|external| {
        state.commits.contains_key(external)
            || !state
                .commits
                .values()
                .any(|commit| commit.parents.contains(external))
    }) {
        return Err(());
    }
    if state.tags.values().any(|id| !known_commit(id))
        || state.reflog.iter().flatten().any(|id| !known_commit(id))
        || state
            .stash
            .iter()
            .any(|tree| !known_workspaces.contains(&tree.workspace_id()))
        || state.bisect.as_ref().is_some_and(|bisect| {
            !known_commit(&bisect.original_head)
                || !known_commit(&bisect.bad)
                || bisect.good.as_ref().is_some_and(|id| !known_commit(id))
                || bisect.current.as_ref().is_some_and(|id| !known_commit(id))
                || bisect.skipped.iter().any(|id| !known_commit(id))
        })
    {
        return Err(());
    }
    if let Some(pending) = &state.pending {
        let valid = match &pending.mutation {
            GitPendingMutation::CaptureCommit {
                workspace_tree,
                expected_head,
                ..
            } => {
                workspace_tree.workspace_id() == state.current().map_err(|_| ())?.workspace_id
                    && expected_head.as_ref().is_none_or(known_commit)
            }
            GitPendingMutation::ForkBranch { branch, head, .. } => {
                !branch.is_empty()
                    && !state.branches.contains_key(branch)
                    && head.as_ref().is_none_or(known_commit)
            }
            GitPendingMutation::Switch { branch } => state.branches.contains_key(branch),
            GitPendingMutation::Reset { head }
            | GitPendingMutation::ApplyCommit { commit: head, .. } => known_commit(head),
            GitPendingMutation::Join { source_head, .. } => {
                source_head.as_ref().is_none_or(known_reference)
            }
            GitPendingMutation::NoOp
            | GitPendingMutation::StashPush { .. }
            | GitPendingMutation::StashPop { .. }
            | GitPendingMutation::Bisect { .. } => true,
        };
        if !valid {
            return Err(());
        }
    }
    Ok(())
}

fn action_operation_id(
    workspace_id: WorkspaceId,
    workspace_tree: GitTreeRef,
    action: &GitFilesystemAction,
) -> Result<OperationId, serde_json::Error> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ACTION_DOMAIN);
    hasher.update(&workspace_id.into_bytes());
    hash_tree_ref(&mut hasher, workspace_tree);
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
    workspace: GitTreeRef,
) -> Result<GitCommandOutput, GitCompatStateError> {
    let current = state.current()?.clone();
    if state.bisect.is_some()
        && !matches!(
            &command,
            GitCommand::Status
                | GitCommand::Diff { .. }
                | GitCommand::Log { .. }
                | GitCommand::Show { .. }
                | GitCommand::RevParse { .. }
                | GitCommand::SymbolicRef { .. }
                | GitCommand::MergeBase { .. }
                | GitCommand::LsFiles
                | GitCommand::CheckIgnore { .. }
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
    let head_workspace_tree =
        visible_head.and_then(|id| state.commits.get(&id).map(|commit| commit.workspace_tree));
    let head_tree = visible_head.and_then(|id| state.commits.get(&id).map(|commit| commit.tree));
    Ok(match command {
        GitCommand::MergeContinue | GitCommand::MergeAbort => {
            return Err(GitCompatStateError::Invalid);
        }
        GitCommand::Status => GitCommandOutput::Status(GitStatus {
            branch: current.name,
            head: visible_head,
            workspace,
            dirty: match head_workspace_tree {
                Some(head) if head == workspace => GitDirtyState::Clean,
                Some(GitTreeRef::Lazy(_)) | None if matches!(workspace, GitTreeRef::Lazy(_)) => {
                    GitDirtyState::Unknown
                }
                _ => GitDirtyState::Dirty,
            },
            all_changes_staged: true,
        }),
        GitCommand::Diff { .. } => GitCommandOutput::Action(GitFilesystemAction::Diff {
            from: head_tree,
            to: workspace,
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
            if head_workspace_tree == Some(workspace) {
                return Err(GitCompatStateError::NothingToCommit);
            }
            let action = GitFilesystemAction::CaptureCommit {
                workspace_tree: workspace,
                head_tree,
                head_workspace_tree: Box::new(head_workspace_tree),
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
                    workspace_tree: workspace,
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
                    source_tree: workspace,
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
                    source_tree: workspace,
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
                tree: commit.tree,
                paths: expand_pathspecs(&paths, &commit.tracked_paths),
            })
        }
        GitCommand::Reset { target, mode } => {
            let id = resolve_object(state, Some(&target))?;
            let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
            let tree = commit.tree;
            let target_tracked_paths = commit.tracked_paths.clone();
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
                        tree,
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
            let tracked_paths = current
                .tracked_paths
                .union(&source.tracked_paths)
                .cloned()
                .collect();
            prepare_transition(
                state,
                GitFilesystemAction::Join {
                    target_tree: workspace,
                    source_workspace: source.workspace_id,
                    rebase: false,
                    tracked_paths,
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
            let tracked_paths = current
                .tracked_paths
                .union(&source.tracked_paths)
                .cloned()
                .collect();
            prepare_transition(
                state,
                GitFilesystemAction::Join {
                    target_tree: workspace,
                    source_workspace: source.workspace_id,
                    rebase: true,
                    tracked_paths,
                },
                GitPendingMutation::Join {
                    source_head: source.head,
                    source_branch: branch,
                    rebase: true,
                },
            )
        }
        GitCommand::StashPush => {
            let tree = head_workspace_tree.ok_or(GitCompatStateError::UnbornHead)?;
            prepare_transition(
                state,
                GitFilesystemAction::RestoreGeneration { tree, paths: None },
                GitPendingMutation::StashPush { tree: workspace },
            )
        }
        GitCommand::StashPop => {
            let tree = state
                .stash
                .last()
                .copied()
                .ok_or(GitCompatStateError::EmptyStash)?;
            prepare_transition(
                state,
                GitFilesystemAction::RestoreGeneration { tree, paths: None },
                GitPendingMutation::StashPop { tree },
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
            let tracked_paths = current.tracked_paths.union(&paths).cloned().collect();
            prepare_transition(
                state,
                GitFilesystemAction::ApplyCommit {
                    commit,
                    reverse: false,
                    base,
                    source,
                    paths,
                    tracked_paths,
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
            let tracked_paths = current.tracked_paths.union(&paths).cloned().collect();
            prepare_transition(
                state,
                GitFilesystemAction::ApplyCommit {
                    commit,
                    reverse: true,
                    base,
                    source,
                    paths,
                    tracked_paths,
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
            tree: workspace,
        }),
        GitCommand::Clean { dry_run } => GitCommandOutput::Action(GitFilesystemAction::Clean {
            dry_run,
            tree: workspace,
            tracked_paths: current.tracked_paths,
        }),
        GitCommand::Archive { object } => {
            let snapshot = match object {
                Some(object) => {
                    let id = resolve_object(state, Some(&object))?;
                    let commit = state.commits.get(&id).ok_or(GitCompatStateError::Invalid)?;
                    commit.tree
                }
                None => workspace,
            };
            GitCommandOutput::Action(GitFilesystemAction::Archive { tree: snapshot })
        }
        GitCommand::Apply { patch } => {
            GitCommandOutput::Action(GitFilesystemAction::ApplyPatch { patch })
        }
        GitCommand::Bisect { arguments } => execute_bisect(state, &current, workspace, &arguments)?,
        GitCommand::RevParse { argument } => {
            let value = match argument.as_str() {
                "--is-inside-work-tree" => "true".to_owned(),
                "--abbrev-ref HEAD" => current.name,
                "--symbolic-full-name HEAD" => format!("refs/heads/{}", current.name),
                _ => resolve_object(state, Some(&GitObjectName(argument)))?.to_hex(),
            };
            GitCommandOutput::Text(value)
        }
        GitCommand::SymbolicRef { short } => GitCommandOutput::Text(if short {
            current.name
        } else {
            format!("refs/heads/{}", current.name)
        }),
        GitCommand::MergeBase { left, right } => {
            let left = resolve_object(state, Some(&left))?;
            let right = resolve_object(state, Some(&right))?;
            let common = nearest_common_ancestor(state, left, right)
                .ok_or_else(|| GitCompatStateError::Unknown("merge base".to_owned()))?;
            GitCommandOutput::Text(common.to_hex())
        }
        GitCommand::LsFiles => GitCommandOutput::Paths(current.tracked_paths.into_iter().collect()),
        GitCommand::CheckIgnore { paths } => {
            GitCommandOutput::Action(GitFilesystemAction::CheckIgnore {
                paths,
                tree: workspace,
            })
        }
    })
}

fn nearest_common_ancestor(
    state: &GitCompatState,
    left: GitCommitId,
    right: GitCommitId,
) -> Option<GitCommitId> {
    fn distances(state: &GitCompatState, start: GitCommitId) -> BTreeMap<GitCommitId, u32> {
        let mut result = BTreeMap::new();
        let mut pending = std::collections::VecDeque::from([(start, 0_u32)]);
        while let Some((commit, distance)) = pending.pop_front() {
            if result.get(&commit).is_some_and(|known| *known <= distance) {
                continue;
            }
            result.insert(commit, distance);
            if let Some(record) = state.commits.get(&commit) {
                pending.extend(
                    record
                        .parents
                        .iter()
                        .copied()
                        .map(|parent| (parent, distance.saturating_add(1))),
                );
            }
        }
        result
    }

    let left_distances = distances(state, left);
    let right_distances = distances(state, right);
    left_distances
        .iter()
        .filter_map(|(commit, left_distance)| {
            right_distances
                .get(commit)
                .map(|right_distance| (*commit, left_distance + right_distance, *left_distance))
        })
        .min_by_key(|(commit, total, left_distance)| (*total, *left_distance, *commit))
        .map(|(commit, _, _)| commit)
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
    let head_tree = current
        .head
        .and_then(|id| state.commits.get(&id).map(|commit| commit.tree));
    if head_tree == Some(record.tree) {
        return Err(GitCompatError::NothingToCommit);
    }
    let commit = GitCommit::new_with_metadata(
        record.tree,
        record.workspace_tree,
        record.tracked_paths.clone(),
        current.head.into_iter().collect(),
        record.author,
        record.authored_at_seconds,
        record.message,
    );
    state.reflog.insert(0, current.head);
    state.commits.insert(commit.id, commit.clone());
    let current = state
        .current_mut()
        .map_err(|_| GitCompatError::InvalidState)?;
    current.head = Some(commit.id);
    current.tracked_paths = record.tracked_paths;
    Ok(GitCommandOutput::Committed(commit))
}

#[allow(clippy::too_many_lines)]
fn complete_pending<E: std::error::Error + 'static>(
    state: &mut GitCompatState,
    mutation: GitPendingMutation,
    result: &GitFilesystemResult,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let resulting_tree = result.resulting_tree();
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
        GitPendingMutation::StashPush { tree } => {
            state.stash.push(tree);
            Ok(GitCommandOutput::NoOp)
        }
        GitPendingMutation::StashPop { tree } => {
            if state.stash.last() != Some(&tree) {
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
            let tree = resulting_tree.ok_or(GitCompatError::MissingResultGeneration)?;
            let tracked_paths = match result {
                GitFilesystemResult::Applied {
                    tracked_paths: Some(paths),
                    ..
                } => paths.clone(),
                _ => return Err(GitCompatError::InvalidState),
            };
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
                tree,
                parents,
                tracked_paths,
                format!("{verb} {source_branch}"),
                previous,
            )
        }
        GitPendingMutation::ApplyCommit { commit, reverse } => {
            let tree = resulting_tree.ok_or(GitCompatError::MissingResultGeneration)?;
            let tracked_paths = match result {
                GitFilesystemResult::Applied {
                    tracked_paths: Some(paths),
                    ..
                } => paths.clone(),
                _ => return Err(GitCompatError::InvalidState),
            };
            let previous = state
                .current()
                .map_err(|_| GitCompatError::InvalidState)?
                .head;
            let verb = if reverse { "revert" } else { "cherry-pick" };
            record_generated_commit(
                state,
                tree,
                previous.into_iter().collect(),
                tracked_paths,
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
                workspace_tree,
                message,
                author,
                authored_at_seconds,
                expected_head,
            },
            GitFilesystemResult::Captured {
                tree,
                tracked_paths,
            },
        ) => record_captured_commit_state(
            state,
            GitCommitRecord {
                expected_head,
                tree: *tree,
                workspace_tree,
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
    tree: GitTreeRef,
    parents: Vec<GitCommitId>,
    tracked_paths: BTreeSet<String>,
    message: String,
    previous: Option<GitCommitId>,
) -> Result<GitCommandOutput, GitCompatError<E>> {
    let commit = GitCommit::new_with_metadata(
        tree,
        tree,
        tracked_paths.clone(),
        parents,
        "git-compat",
        0,
        message,
    );
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
    workspace: GitTreeRef,
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
            if bad_record.workspace_tree != workspace {
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
            tree: record.tree,
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

fn commit_generation_ref(commit: &GitCommit, _fallback: WorkspaceId) -> GitTreeRef {
    commit.tree
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
#[allow(
    clippy::cognitive_complexity,
    reason = "the argv compatibility matrix is intentionally exhaustive and centralized"
)]
fn parse_argv<E: std::error::Error + 'static>(
    argv: &[String],
    default_author: &str,
    now_seconds: i64,
) -> Result<GitCommand, GitCompatError<E>> {
    reject_shell_composition(argv)?;
    let Some(command) = argv.first().map(String::as_str) else {
        return Err(GitCompatError::InvalidCommand("missing command".to_owned()));
    };
    let args = argv.get(1..).unwrap_or_default();
    match command {
        "status" => {
            require_only_options(command, args, &["--short", "--porcelain", "--porcelain=v1"])?;
            Ok(GitCommand::Status)
        }
        "diff" => {
            require_only_options(command, args, &["--cached", "--staged"])?;
            Ok(GitCommand::Diff {
                cached: args
                    .iter()
                    .any(|arg| arg == "--cached" || arg == "--staged"),
            })
        }
        "log" => Ok(GitCommand::Log {
            maximum: parse_log_maximum(args)?,
        }),
        "show" => Ok(GitCommand::Show {
            object: optional_single_value(command, args)?.map(GitObjectName),
        }),
        "add" => {
            if args.is_empty() {
                return Err(GitCompatError::InvalidCommand(
                    "add requires a path or -A/--all".to_owned(),
                ));
            }
            let mut options_ended = false;
            let mut stages_without_paths = false;
            let mut paths = Vec::new();
            for arg in args {
                if !options_ended && arg == "--" {
                    options_ended = true;
                } else if !options_ended && arg.starts_with('-') {
                    if matches!(arg.as_str(), "-A" | "--all" | "-u" | "--update") {
                        stages_without_paths = true;
                    } else {
                        return unsupported_option(command, arg);
                    }
                } else {
                    paths.push(arg.clone());
                }
            }
            if paths.is_empty() && !stages_without_paths {
                return Err(GitCompatError::InvalidCommand(
                    "add requires a path or -A/--all".to_owned(),
                ));
            }
            Ok(GitCommand::Add { paths })
        }
        "commit" => {
            let mut message = None;
            let mut author = default_author.to_owned();
            let mut index = 0;
            while index < args.len() {
                let Some(argument) = args.get(index) else {
                    break;
                };
                let (name, inline) = split_long_option(argument);
                if name == "-m" || name == "--message" {
                    let value = inline
                        .map(str::to_owned)
                        .or_else(|| args.get(index + 1).cloned())
                        .ok_or_else(|| {
                            GitCompatError::InvalidCommand("commit requires a message after -m".to_owned())
                        })?;
                    if message.replace(value).is_some() {
                        return Err(GitCompatError::InvalidCommand(
                            "multiple commit messages are not supported".to_owned(),
                        ));
                    }
                    index += usize::from(inline.is_none());
                } else if name == "--author" {
                    author = inline
                        .map(str::to_owned)
                        .or_else(|| args.get(index + 1).cloned())
                        .ok_or_else(|| {
                            GitCompatError::InvalidCommand(
                                "commit requires an author after --author".to_owned(),
                            )
                        })?;
                    index += usize::from(inline.is_none());
                } else {
                    return unsupported_option(command, argument);
                }
                index += 1;
            }
            let message = message.ok_or_else(|| {
                GitCompatError::InvalidCommand("commit requires -m".to_owned())
            })?;
            Ok(GitCommand::Commit {
                message,
                author,
                authored_at_seconds: now_seconds,
            })
        }
        "branch" => Ok(GitCommand::Branch {
            create: optional_single_value(command, args)?,
        }),
        "switch" => {
            let (create, values) = positional_with_options(command, args, &["-c"])?;
            let branch = exactly_one(command, &values)?;
            Ok(GitCommand::Switch { branch, create })
        }
        "checkout" => {
            if let Some(separator) = args.iter().position(|argument| argument == "--") {
                if separator > 1 {
                    return Err(GitCompatError::InvalidCommand(
                        "checkout path restoration accepts at most one source object".to_owned(),
                    ));
                }
                let paths = args.get(separator + 1..).unwrap_or_default().to_vec();
                if paths.is_empty() {
                    return Err(GitCompatError::InvalidCommand(
                        "checkout -- requires at least one path".to_owned(),
                    ));
                }
                return Ok(GitCommand::Restore {
                    source: args.first().map(|source| GitObjectName(source.clone())),
                    paths,
                });
            }
            let (create, values) = positional_with_options(command, args, &["-b"])?;
            let branch = exactly_one(command, &values)?;
            Ok(GitCommand::Switch { branch, create })
        }
        "restore" => {
            let mut source = None;
            let mut paths = Vec::new();
            let mut options_ended = false;
            let mut index = 0;
            while index < args.len() {
                let Some(argument) = args.get(index) else {
                    break;
                };
                if !options_ended && argument == "--" {
                    options_ended = true;
                } else if !options_ended {
                    let (name, inline) = split_long_option(argument);
                    if name == "--source" {
                        let value = inline
                            .map(str::to_owned)
                            .or_else(|| args.get(index + 1).cloned())
                            .ok_or_else(|| {
                                GitCompatError::InvalidCommand(
                                    "restore requires an object after --source".to_owned(),
                                )
                            })?;
                        if source.replace(GitObjectName(value)).is_some() {
                            return Err(GitCompatError::InvalidCommand(
                                "restore accepts only one --source".to_owned(),
                            ));
                        }
                        index += usize::from(inline.is_none());
                    } else if argument.starts_with('-') {
                        return unsupported_option(command, argument);
                    } else {
                        paths.push(argument.clone());
                    }
                } else {
                    paths.push(argument.clone());
                }
                index += 1;
            }
            if paths.is_empty() {
                return Err(GitCompatError::InvalidCommand(
                    "restore requires at least one path".to_owned(),
                ));
            }
            Ok(GitCommand::Restore { source, paths })
        }
        "reset" => {
            require_options(command, args, &["--hard", "--soft", "--mixed"])?;
            let modes = args
                .iter()
                .filter(|arg| matches!(arg.as_str(), "--hard" | "--soft" | "--mixed"))
                .count();
            if modes > 1 {
                return Err(GitCompatError::InvalidCommand(
                    "reset accepts only one mode".to_owned(),
                ));
            }
            let mode = if args.iter().any(|arg| arg == "--hard") {
                GitResetMode::Hard
            } else if args.iter().any(|arg| arg == "--soft") {
                GitResetMode::Soft
            } else {
                GitResetMode::Mixed
            };
            let targets = args
                .iter()
                .filter(|arg| !arg.starts_with('-'))
                .cloned()
                .collect::<Vec<_>>();
            let target = match targets.as_slice() {
                [] => "HEAD".to_owned(),
                [target] => target.clone(),
                _ => return Err(GitCompatError::InvalidCommand(
                    "reset accepts only one target; path reset is not supported".to_owned(),
                )),
            };
            Ok(GitCommand::Reset {
                target: GitObjectName(target),
                mode,
            })
        }
        "merge" if matches!(args, [operation] if operation == "--continue") => {
            Ok(GitCommand::MergeContinue)
        }
        "merge" if matches!(args, [operation] if operation == "--abort") => {
            Ok(GitCommand::MergeAbort)
        }
        "merge" | "rebase" => {
            let branch = exactly_one(command, args)?;
            if command == "merge" {
                Ok(GitCommand::Merge { branch })
            } else {
                Ok(GitCommand::Rebase { branch })
            }
        }
        "cherry-pick" | "revert" => {
            let object = GitObjectName(exactly_one(command, args)?);
            if command == "cherry-pick" {
                Ok(GitCommand::CherryPick { object })
            } else {
                Ok(GitCommand::Revert { object })
            }
        }
        "stash" => match args {
            [] => Ok(GitCommand::StashPush),
            [operation] if operation == "push" => Ok(GitCommand::StashPush),
            [operation] if operation == "pop" => Ok(GitCommand::StashPop),
            [other] => Err(GitCompatError::InvalidCommand(format!(
                "unsupported stash operation '{other}'"
            ))),
            _ => Err(GitCompatError::InvalidCommand(
                "stash accepts only push or pop".to_owned(),
            )),
        },
        "tag" => {
            let delete = args.iter().any(|arg| arg == "-d" || arg == "--delete");
            require_options(command, args, &["-d", "--delete"])?;
            let values: Vec<_> = args.iter().filter(|arg| !arg.starts_with('-')).cloned().collect();
            if values.len() > 2 || (delete && values.len() != 1) {
                return Err(GitCompatError::InvalidCommand(
                    "tag accepts [name [target]] or --delete name".to_owned(),
                ));
            }
            Ok(GitCommand::Tag {
                name: values.first().cloned(),
                target: values.get(1).cloned().map(GitObjectName),
                delete,
            })
        }
        "blame" => Ok(GitCommand::Blame {
            path: exactly_one(command, args)?,
        }),
        "grep" => match args {
            [pattern] => Ok(GitCommand::Grep {
                pattern: pattern.clone(),
                path: None,
            }),
            [pattern, path] => Ok(GitCommand::Grep {
                pattern: pattern.clone(),
                path: Some(path.clone()),
            }),
            _ => Err(GitCompatError::InvalidCommand(
                "grep requires a pattern and at most one path".to_owned(),
            )),
        },
        "clean" => {
            require_only_options(
                command,
                args,
                &["-n", "--dry-run", "-f", "--force"],
            )?;
            let dry_run = args
                .iter()
                .any(|arg| matches!(arg.as_str(), "-n" | "--dry-run"));
            let force = args
                .iter()
                .any(|arg| matches!(arg.as_str(), "-f" | "--force"));
            if !dry_run && !force {
                return Err(GitCompatError::InvalidCommand(
                    "clean requires -f or --force unless --dry-run is used".to_owned(),
                ));
            }
            Ok(GitCommand::Clean { dry_run })
        }
        "archive" => Ok(GitCommand::Archive {
            object: optional_single_value(command, args)?.map(GitObjectName),
        }),
        "apply" => Err(GitCompatError::Unsupported {
            command: command.to_owned(),
            reason: "argv cannot safely turn a patch filename into patch bytes; load the file and use the typed Apply command".to_owned(),
        }),
        "bisect" => Ok(GitCommand::Bisect {
            arguments: args.to_vec(),
        }),
        "rev-parse" => match args {
            [argument] if matches!(argument.as_str(), "--is-inside-work-tree" | "HEAD") => {
                Ok(GitCommand::RevParse {
                    argument: argument.clone(),
                })
            }
            [option, head]
                if head == "HEAD"
                    && matches!(
                        option.as_str(),
                        "--abbrev-ref" | "--symbolic-full-name"
                    ) => Ok(GitCommand::RevParse {
                argument: format!("{option} HEAD"),
            }),
            [object] if !object.starts_with('-') => Ok(GitCommand::RevParse {
                argument: object.clone(),
            }),
            _ => Err(GitCompatError::InvalidCommand(
                "rev-parse supports HEAD, one object, --is-inside-work-tree, --abbrev-ref HEAD, or --symbolic-full-name HEAD".to_owned(),
            )),
        },
        "symbolic-ref" => {
            let short = args.iter().any(|argument| argument == "--short");
            require_options(command, args, &["--short"])?;
            let values = args
                .iter()
                .filter(|argument| !argument.starts_with('-'))
                .collect::<Vec<_>>();
            if values.as_slice() != [&"HEAD"] {
                return Err(GitCompatError::InvalidCommand(
                    "symbolic-ref supports only HEAD".to_owned(),
                ));
            }
            Ok(GitCommand::SymbolicRef { short })
        }
        "merge-base" => match args {
            [left, right] => Ok(GitCommand::MergeBase {
                left: GitObjectName(left.clone()),
                right: GitObjectName(right.clone()),
            }),
            _ => Err(GitCompatError::InvalidCommand(
                "merge-base requires exactly two objects".to_owned(),
            )),
        },
        "ls-files" => {
            require_only_options(command, args, &["--cached"])?;
            Ok(GitCommand::LsFiles)
        }
        "check-ignore" => {
            let mut paths = Vec::new();
            for argument in args {
                if argument == "--" {
                    continue;
                }
                if argument.starts_with('-') {
                    return unsupported_option(command, argument);
                }
                paths.push(argument.clone());
            }
            if paths.is_empty() {
                return Err(GitCompatError::InvalidCommand(
                    "check-ignore requires at least one path".to_owned(),
                ));
            }
            Ok(GitCommand::CheckIgnore { paths })
        }
        "clone" | "fetch" | "pull" | "push" | "remote" | "gc" | "repack" | "cat-file"
        | "hash-object" | "init" | "fsck" | "prune" | "pack-objects" | "index-pack"
        | "receive-pack" | "upload-pack" | "read-tree" | "write-tree" | "commit-tree"
        | "update-index" | "worktree" | "submodule" => Err(GitCompatError::Unsupported {
            command: command.to_owned(),
            reason: "the compatibility layer has no Git object database or transport".to_owned(),
        }),
        other => Err(GitCompatError::InvalidCommand(format!(
            "unknown command '{other}'"
        ))),
    }
}

fn reject_shell_composition<E: std::error::Error + 'static>(
    argv: &[String],
) -> Result<(), GitCompatError<E>> {
    if let Some(token) = argv.iter().find(|token| {
        matches!(token.as_str(), "&&" | "||" | ";" | "|" | "&")
            || token.starts_with('>')
            || token.starts_with('<')
            || token.contains('`')
            || token.contains("$(")
    }) {
        return Err(GitCompatError::Unsupported {
            command: token.clone(),
            reason: "shell composition is outside the command-level compatibility façade"
                .to_owned(),
        });
    }
    Ok(())
}

fn require_options<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
    allowed: &[&str],
) -> Result<(), GitCompatError<E>> {
    if let Some(argument) = args
        .iter()
        .find(|argument| argument.starts_with('-') && !allowed.contains(&argument.as_str()))
    {
        return unsupported_option(command, argument);
    }
    Ok(())
}

fn require_only_options<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
    allowed: &[&str],
) -> Result<(), GitCompatError<E>> {
    require_options(command, args, allowed)?;
    if args.iter().any(|argument| !argument.starts_with('-')) {
        return Err(GitCompatError::InvalidCommand(format!(
            "{command} does not accept positional arguments"
        )));
    }
    Ok(())
}

fn unsupported_option<T, E: std::error::Error + 'static>(
    command: &str,
    argument: &str,
) -> Result<T, GitCompatError<E>> {
    Err(GitCompatError::Unsupported {
        command: command.to_owned(),
        reason: format!("option '{argument}' cannot be represented by the compatibility façade"),
    })
}

fn optional_single_value<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
) -> Result<Option<String>, GitCompatError<E>> {
    match args {
        [] => Ok(None),
        [value] if !value.starts_with('-') => Ok(Some(value.clone())),
        [option] => unsupported_option(command, option),
        _ => Err(GitCompatError::InvalidCommand(format!(
            "{command} accepts at most one argument"
        ))),
    }
}

fn exactly_one<E: std::error::Error + 'static>(
    command: &str,
    values: &[String],
) -> Result<String, GitCompatError<E>> {
    match values {
        [value] if !value.starts_with('-') => Ok(value.clone()),
        [option] => unsupported_option(command, option),
        _ => Err(GitCompatError::InvalidCommand(format!(
            "{command} requires exactly one argument"
        ))),
    }
}

fn positional_with_options<E: std::error::Error + 'static>(
    command: &str,
    args: &[String],
    allowed: &[&str],
) -> Result<(bool, Vec<String>), GitCompatError<E>> {
    require_options(command, args, allowed)?;
    Ok((
        args.iter()
            .any(|argument| allowed.contains(&argument.as_str())),
        args.iter()
            .filter(|argument| !allowed.contains(&argument.as_str()))
            .cloned()
            .collect(),
    ))
}

fn split_long_option(argument: &str) -> (&str, Option<&str>) {
    argument
        .split_once('=')
        .map_or((argument, None), |(name, value)| (name, Some(value)))
}

fn parse_log_maximum<E: std::error::Error + 'static>(
    args: &[String],
) -> Result<u32, GitCompatError<E>> {
    if args.is_empty() {
        return Ok(100);
    }
    let value = match args {
        [option, value] if option == "-n" || option == "--max-count" => value.as_str(),
        [option] if option.starts_with("--max-count=") => option
            .split_once('=')
            .map(|(_, value)| value)
            .unwrap_or_default(),
        [option] if option.len() > 1 => option
            .strip_prefix('-')
            .ok_or_else(|| GitCompatError::InvalidCommand("invalid log count".to_owned()))?,
        [option] => return unsupported_option("log", option),
        _ => {
            return Err(GitCompatError::InvalidCommand(
                "log accepts only -n <count>, -<count>, or --max-count=<count>".to_owned(),
            ));
        }
    };
    value.parse::<u32>().map_err(|_| {
        GitCompatError::InvalidCommand("log count must be an unsigned 32-bit integer".to_owned())
    })
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
    /// Initial generation of the capture workspace, when capture forked.
    pub initial_generation: Option<crate::GenerationId>,
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
    apply_git_patch_with_permit(
        workspace,
        patch,
        idempotency_key,
        crate::PublicationPermit::Unrestricted,
    )
    .await
}

/// Applies a bounded UTF-8 patch under one writer permit.
pub async fn apply_git_patch_with_permit<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    patch: &[u8],
    idempotency_key: IdempotencyKey,
    permit: crate::PublicationPermit,
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
    transaction
        .commit_with_permit(permit)
        .await
        .map_err(Into::into)
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

async fn scan_git_capture_entries<A, O>(
    live: &crate::Generation<A, O>,
) -> Result<(Vec<(String, bool)>, BTreeMap<String, GitIgnorePolicy>), GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let mut entries = Vec::new();
    let mut nested_policies = BTreeMap::new();
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
                } else if name == ".gitignore" && directory != "/" {
                    let contents = live.read(&format!("/{path}"), 1024 * 1024).await?;
                    let contents = std::str::from_utf8(&contents)
                        .map_err(|_| GitCaptureError::NonPortableName)?;
                    nested_policies.insert(
                        directory.trim_start_matches('/').to_owned(),
                        GitIgnorePolicy::parse(contents),
                    );
                }
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    Ok((entries, nested_policies))
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
    capture_git_compatible_generation_from(workspace, live, policy, already_tracked, operation_id)
        .await
}

/// Captures an ignore-aware compatibility tree from one exact immutable
/// workspace generation. Later workspace updates cannot leak into the result.
pub async fn capture_git_compatible_generation_at<A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace.generation(generation).await?;
    capture_git_compatible_generation_from(workspace, live, policy, already_tracked, operation_id)
        .await
}

async fn capture_git_compatible_generation_from<A, O>(
    workspace: &Workspace<A, O>,
    live: crate::Generation<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let (mut entries, nested_policies) = scan_git_capture_entries(&live).await?;
    entries.sort_by(|left, right| {
        path_depth(&left.0)
            .cmp(&path_depth(&right.0))
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut excluded = Vec::<String>::new();
    let mut tracked_paths = BTreeSet::new();
    for (path, is_directory) in &entries {
        let parent_excluded = excluded.iter().any(|parent| is_path_below(path, parent));
        let tracked = already_tracked.contains(path);
        let tracked_descendant = *is_directory
            && already_tracked
                .iter()
                .any(|candidate| is_path_below(candidate, path));
        if parent_excluded
            || (!git_path_eligible(policy, &nested_policies, path, *is_directory, tracked)
                && !tracked_descendant)
        {
            excluded.push(path.clone());
        } else if !is_directory {
            tracked_paths.insert(path.clone());
        }
    }
    if excluded.is_empty() {
        return Ok(GitCapturedGeneration {
            generation: live,
            initial_generation: None,
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
    let initial_generation = capture.head().await?.id();
    let mut commit_bytes = bytes;
    commit_bytes[0] ^= 0xa5;
    let commit_key = IdempotencyKey::from_bytes(commit_bytes);
    let generation = if let Some(generation) = capture.operation_generation(commit_key).await? {
        generation
    } else {
        let mut transaction = capture.begin_transaction(commit_key).await?;
        for path in excluded.iter().rev() {
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
        initial_generation: Some(initial_generation),
        tracked_paths,
    })
}

/// Incrementally captures only paths changed since the preceding live
/// generation, starting from the preceding filtered compatibility snapshot.
///
/// Equal Merkle subtrees are skipped. A `.gitignore` change deliberately
/// falls back to an exact capture because eligibility may have changed for an
/// otherwise unchanged path.
pub async fn capture_git_compatible_generation_incremental<A, O>(
    workspace: &Workspace<A, O>,
    previous_live: &Generation<A, O>,
    previous_capture: &Generation<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace.head().await?;
    let changes = previous_live.diff_to(&live, u32::MAX).await?;
    let changed = changes.changed_paths(u32::MAX).await?;
    let mut portable = Vec::with_capacity(changed.len());
    for change in changed {
        let path = portable_changed_path(&change.path)?;
        if path.rsplit('/').next() == Some(".gitignore") {
            return capture_git_compatible_generation(
                workspace,
                policy,
                already_tracked,
                operation_id,
            )
            .await;
        }
        portable.push((path, change.before, change.after));
    }

    let mut eligible = Vec::new();
    let mut tracked_paths = already_tracked.clone();
    for (path, before, after) in portable {
        let kind = after.or(before).map(|record| record.kind);
        let is_directory = kind == Some(FileKind::Directory);
        let tracked = already_tracked.contains(&path);
        let tracked_descendant = is_directory
            && already_tracked
                .iter()
                .any(|candidate| is_path_below(candidate, &path));
        if tracked || tracked_descendant || policy.eligible(&path, is_directory, tracked) {
            eligible.push(path.clone());
        }
        if !is_directory {
            if after.is_some() && (tracked || policy.eligible(&path, false, tracked)) {
                tracked_paths.insert(path);
            } else {
                tracked_paths.remove(&path);
            }
        }
    }
    if eligible.is_empty() {
        return Ok(GitCapturedGeneration {
            generation: previous_capture.clone(),
            initial_generation: None,
            tracked_paths,
        });
    }

    let bytes = operation_id.into_bytes();
    let workspace_hash = hex::encode(&workspace.id().into_bytes()[..6]);
    let capture_name = format!("git-capture-{workspace_hash}-{}", hex::encode(bytes));
    let capture = previous_capture
        .workspace()
        .fork(
            capture_name,
            crate::ForkOptions::from_generation(
                previous_capture.clone(),
                IdempotencyKey::from_bytes(bytes),
            ),
        )
        .await?;
    let mut apply_bytes = bytes;
    apply_bytes[0] ^= 0xa5;
    let capture_head = capture.head().await?;
    let generation = match capture
        .apply_paths_from(
            Some(previous_live),
            Some(&live),
            &eligible,
            capture_head.id(),
            IdempotencyKey::from_bytes(apply_bytes),
        )
        .await?
    {
        crate::WorkspacePathApply::Applied(generation)
        | crate::WorkspacePathApply::AlreadyApplied(generation)
        | crate::WorkspacePathApply::NoChanges(generation) => generation,
        crate::WorkspacePathApply::Conflicted(_)
        | crate::WorkspacePathApply::Stale(_)
        | crate::WorkspacePathApply::Fenced
        | crate::WorkspacePathApply::IdempotencyConflict => return Err(GitCaptureError::Conflict),
    };
    generation
        .pin(format!("capture-{}", hex::encode(bytes)))
        .await?;
    Ok(GitCapturedGeneration {
        generation,
        initial_generation: Some(capture_head.id()),
        tracked_paths,
    })
}

fn portable_changed_path(path: &crate::kernel::NamespacePath) -> Result<String, GitCaptureError> {
    let mut portable = String::new();
    for component in path.components() {
        if !portable.is_empty() {
            portable.push('/');
        }
        match component.encoding() {
            NameEncoding::Utf8 => portable.push_str(
                std::str::from_utf8(component.as_bytes())
                    .map_err(|_| GitCaptureError::NonPortableName)?,
            ),
            NameEncoding::PosixBytes | NameEncoding::WindowsUtf16Le => {
                return Err(GitCaptureError::NonPortableName);
            }
        }
    }
    Ok(portable)
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

    fn apply(&self, path: &str, is_directory: bool, ignored: &mut bool) {
        for rule in &self.rules {
            if ignore_matches(rule, path, is_directory) {
                *ignored = !rule.negated;
            }
        }
    }
}

fn git_path_eligible(
    root: &GitIgnorePolicy,
    nested: &BTreeMap<String, GitIgnorePolicy>,
    path: &str,
    is_directory: bool,
    already_tracked: bool,
) -> bool {
    if already_tracked {
        return true;
    }
    let path = path.trim_start_matches('/');
    let mut ignored = false;
    root.apply(path, is_directory, &mut ignored);
    let mut applicable = nested
        .iter()
        .filter_map(|(directory, policy)| {
            path.strip_prefix(directory)
                .and_then(|suffix| suffix.strip_prefix('/'))
                .map(|relative| (directory.split('/').count(), relative, policy))
        })
        .collect::<Vec<_>>();
    applicable.sort_by_key(|(depth, _, _)| *depth);
    for (_, relative, policy) in applicable {
        policy.apply(relative, is_directory, &mut ignored);
    }
    !ignored
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
    use crate::kernel::{FileMetadata, MetadataField};
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

    fn tree(byte: u8) -> GitTreeRef {
        GitTreeRef::exact(workspace(), generation(byte))
    }

    #[tokio::test]
    async fn foreign_workspace_tree_is_rejected_before_executor_side_effects() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let foreign_workspace = WorkspaceId::derive(
            [8; 16],
            &WorkspaceName::new("foreign").expect("valid foreign workspace"),
        );
        let executor = TestExecutor::returning(GitFilesystemResult::Applied {
            tree: None,
            tracked_paths: None,
        });

        let error = repository
            .run(
                GitCommand::Status,
                GitTreeRef::exact(foreign_workspace, generation(1)),
                &executor,
            )
            .await
            .expect_err("foreign tree must fail closed");

        assert!(matches!(
            error,
            GitCompatRunError::Compat(GitCompatError::WorkspaceMismatch)
        ));
        assert!(
            executor
                .operations
                .lock()
                .expect("operation lock")
                .is_empty(),
            "foreign trees must never reach the filesystem executor"
        );
    }

    #[tokio::test]
    async fn foreign_completion_tree_is_rejected_without_consuming_the_transition() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let GitCommandOutput::Prepared { transition, .. } = repository
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
            panic!("expected prepared commit");
        };
        let foreign = WorkspaceId::derive(
            [8; 16],
            &WorkspaceName::new("foreign-result").expect("valid workspace"),
        );
        assert!(matches!(
            repository
                .complete_transition_result(
                    transition,
                    &GitFilesystemResult::Captured {
                        tree: GitTreeRef::exact(foreign, generation(1)),
                        tracked_paths: BTreeSet::new(),
                    },
                )
                .await,
            Err(GitCompatError::WorkspaceMismatch)
        ));
        assert_eq!(
            repository
                .pending_transition()
                .await
                .expect("pending transition")
                .map(|pending| pending.id),
            Some(transition)
        );
    }

    #[tokio::test]
    async fn persisted_commit_hashes_are_validated_before_use() {
        let store = MemoryGitCompatStore::new();
        let mut state = GitCompatState::new("main", workspace());
        let mut commit = GitCommit::new(tree(1), Vec::new(), "agent", 10, "initial");
        commit.message = "forged".to_owned();
        state.revision = 1;
        state.commits.insert(commit.id, commit.clone());
        state.branches.get_mut("main").expect("main branch").head = Some(commit.id);
        {
            let mut states = store.states.lock().expect("state lock");
            states.insert(workspace(), state);
        }
        let repository = GitCompatRepository::new(workspace(), store);
        assert!(matches!(
            repository.execute(GitCommand::Status, generation(1)).await,
            Err(GitCompatError::InvalidState)
        ));
    }

    #[test]
    fn generated_commit_uses_exact_resulting_tracked_paths() {
        let mut state = GitCompatState::new("main", workspace());
        state.current_mut().expect("current branch").tracked_paths =
            BTreeSet::from(["deleted.txt".to_owned(), "kept.txt".to_owned()]);
        let output = complete_pending::<MemoryGitCompatStoreError>(
            &mut state,
            GitPendingMutation::ApplyCommit {
                commit: GitCommitId::from_bytes([7; 32]),
                reverse: false,
            },
            &GitFilesystemResult::Applied {
                tree: Some(tree(2)),
                tracked_paths: Some(BTreeSet::from(["kept.txt".to_owned()])),
            },
        )
        .expect("generated commit");
        let GitCommandOutput::Committed(commit) = output else {
            panic!("expected generated commit");
        };
        assert_eq!(
            commit.tracked_paths,
            BTreeSet::from(["kept.txt".to_owned()])
        );
        assert_eq!(
            state.current().expect("current branch").tracked_paths,
            commit.tracked_paths
        );
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
        assert_eq!(status.dirty, GitDirtyState::Dirty);
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
                    tree: tree(1),
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
            tree: tree(9),
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
        assert_eq!(commit.tree, tree(9));
        assert_eq!(commit.workspace_tree, tree(1));
        assert!(matches!(
            repository
                .execute(GitCommand::Status, generation(1))
                .await
                .expect("status"),
            GitCommandOutput::Status(GitStatus {
                dirty: GitDirtyState::Clean,
                ..
            })
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
            tree: tree(1),
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
            tree: Some(tree(1)),
            tracked_paths: None,
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
    async fn conflicted_merge_can_continue_or_abort_without_a_second_sequencer() {
        async fn pending_merge(
            repository: &GitCompatRepository<MemoryGitCompatStore>,
            child: WorkspaceId,
        ) {
            repository
                .run(
                    GitCommand::Branch {
                        create: Some("feature".to_owned()),
                    },
                    generation(1),
                    &TestExecutor::returning(GitFilesystemResult::Forked {
                        workspace_id: child,
                    }),
                )
                .await
                .expect("feature branch");
            assert!(matches!(
                repository
                    .run(
                        GitCommand::Merge {
                            branch: "feature".to_owned(),
                        },
                        generation(2),
                        &TestExecutor::failing(),
                    )
                    .await,
                Err(GitCompatRunError::Executor(_))
            ));
        }

        let child = WorkspaceId::derive(
            [9; 16],
            &WorkspaceName::new("merge-control").expect("valid workspace name"),
        );
        let continued = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        pending_merge(&continued, child).await;
        assert_eq!(
            continued
                .run(
                    GitCommand::Add {
                        paths: vec!["conflicted.txt".to_owned()],
                    },
                    generation(3),
                    &TestExecutor::returning(GitFilesystemResult::Applied {
                        tree: None,
                        tracked_paths: None,
                    }),
                )
                .await
                .expect("stage conflict resolution"),
            GitCommandOutput::NoOp
        );
        assert!(
            continued
                .pending_transition()
                .await
                .expect("merge remains pending after add")
                .is_some()
        );
        assert!(matches!(
            continued
                .run(
                    GitCommand::MergeContinue,
                    generation(3),
                    &TestExecutor::returning(GitFilesystemResult::Applied {
                        tree: None,
                        tracked_paths: None,
                    }),
                )
                .await
                .expect("continue merge"),
            GitCommandOutput::Committed(GitCommit { tree: GitTreeRef::Exact(tree), .. })
                if tree.generation == generation(3)
        ));
        assert!(
            continued
                .pending_transition()
                .await
                .expect("continued state")
                .is_none()
        );

        let aborted = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        pending_merge(&aborted, child).await;
        let executor = TestExecutor::returning(GitFilesystemResult::Applied {
            tree: Some(tree(2)),
            tracked_paths: None,
        });
        assert!(matches!(
            aborted
                .run(GitCommand::MergeAbort, generation(4), &executor)
                .await
                .expect("abort merge"),
            GitCommandOutput::Filesystem(GitFilesystemResult::Applied { .. })
        ));
        assert!(
            aborted
                .pending_transition()
                .await
                .expect("aborted state")
                .is_none()
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
    async fn capture_at_generation_excludes_later_workspace_updates() {
        let fs = Fs::memory();
        let workspace = fs
            .create_workspace("git-capture-pinned-generation")
            .await
            .expect("workspace");
        let mut initial = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x35; 16]))
            .await
            .expect("initial transaction");
        initial
            .write_text("/published.txt", "published")
            .await
            .expect("published file");
        let TransactionCommit::Committed(published) =
            initial.commit().await.expect("initial commit")
        else {
            panic!("initial transaction did not commit");
        };
        let mut later = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x36; 16]))
            .await
            .expect("later transaction");
        later
            .write_text("/later.txt", "later")
            .await
            .expect("later file");
        later.commit().await.expect("later commit");

        let captured = capture_git_compatible_generation_at(
            &workspace,
            published.id(),
            &GitIgnorePolicy::default(),
            &BTreeSet::new(),
            OperationId::from_bytes([0x37; 16]),
        )
        .await
        .expect("pinned capture");
        assert!(captured.generation.stat("/published.txt").await.is_ok());
        assert!(captured.generation.stat("/later.txt").await.is_err());
    }

    #[tokio::test]
    async fn capture_preserves_metadata_and_hard_links_through_public_fork_primitives() {
        let fs = Fs::memory();
        let workspace = fs
            .create_workspace("git-capture-records")
            .await
            .expect("workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x41; 16]))
            .await
            .expect("transaction");
        transaction
            .write_text("/source.txt", "linked")
            .await
            .expect("source");
        transaction
            .set_metadata(
                "/source.txt",
                FileMetadata {
                    posix_mode: MetadataField::Value(0o100_640),
                    ..FileMetadata::default()
                },
            )
            .await
            .expect("metadata");
        transaction
            .hard_link("/source.txt", "/linked.txt")
            .await
            .expect("hard link");
        transaction
            .create_dir_all("/ignored")
            .await
            .expect("ignored directory");
        transaction
            .write_text("/ignored/drop.txt", "drop")
            .await
            .expect("ignored file");
        transaction.commit().await.expect("commit fixture");

        let captured = capture_git_compatible_generation(
            &workspace,
            &GitIgnorePolicy::parse("ignored/\n"),
            &BTreeSet::new(),
            OperationId::from_bytes([0x42; 16]),
        )
        .await
        .expect("capture");
        let source = captured
            .generation
            .stat("/source.txt")
            .await
            .expect("source stat");
        let linked = captured
            .generation
            .stat("/linked.txt")
            .await
            .expect("link stat");
        assert_eq!(source.file_id, linked.file_id);
        assert_eq!(source.link_count, 2);
        assert_eq!(source.metadata.posix_mode, Some(0o100_640));
        assert!(captured.generation.stat("/ignored").await.is_err());
    }

    #[tokio::test]
    async fn exact_capture_inherits_nested_ignore_rules_with_local_negation() {
        let fs = Fs::memory();
        let workspace = fs
            .create_workspace("git-capture-nested-policy")
            .await
            .expect("workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x49; 16]))
            .await
            .expect("transaction");
        transaction.create_dir_all("/src").await.expect("directory");
        transaction
            .write_text("/src/.gitignore", "*.tmp\n!keep.tmp\n")
            .await
            .expect("nested policy");
        transaction
            .write_text("/src/drop.tmp", "drop")
            .await
            .expect("ignored file");
        transaction
            .write_text("/src/keep.tmp", "keep")
            .await
            .expect("negated file");
        transaction
            .write_text("/outside.tmp", "outside")
            .await
            .expect("outside file");
        transaction.commit().await.expect("fixture");

        let captured = capture_git_compatible_generation(
            &workspace,
            &GitIgnorePolicy::default(),
            &BTreeSet::new(),
            OperationId::from_bytes([0x4a; 16]),
        )
        .await
        .expect("capture");
        assert!(captured.generation.stat("/src/drop.tmp").await.is_err());
        assert!(captured.generation.stat("/src/keep.tmp").await.is_ok());
        assert!(captured.generation.stat("/outside.tmp").await.is_ok());
    }

    #[tokio::test]
    async fn incremental_capture_applies_only_changed_eligible_paths() {
        let fs = Fs::memory();
        let workspace = fs
            .create_workspace("git-capture-incremental")
            .await
            .expect("workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x51; 16]))
            .await
            .expect("transaction");
        transaction
            .write_text("/kept.txt", "before")
            .await
            .expect("tracked file");
        transaction
            .write_text("/unchanged.txt", "same")
            .await
            .expect("unchanged file");
        let TransactionCommit::Committed(previous_live) =
            transaction.commit().await.expect("base commit")
        else {
            panic!("base fixture did not commit");
        };
        let policy = GitIgnorePolicy::parse("*.tmp\n");
        let previous_capture = capture_git_compatible_generation(
            &workspace,
            &policy,
            &BTreeSet::new(),
            OperationId::from_bytes([0x52; 16]),
        )
        .await
        .expect("base capture");

        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x53; 16]))
            .await
            .expect("transaction");
        transaction
            .write_text("/kept.txt", "after")
            .await
            .expect("edit");
        transaction
            .write_text("/new.tmp", "ignored")
            .await
            .expect("ignored addition");
        transaction.commit().await.expect("live edit");

        let captured = capture_git_compatible_generation_incremental(
            &workspace,
            &previous_live,
            &previous_capture.generation,
            &policy,
            &previous_capture.tracked_paths,
            OperationId::from_bytes([0x54; 16]),
        )
        .await
        .expect("incremental capture");
        assert_eq!(
            captured
                .generation
                .read("/kept.txt", 64)
                .await
                .expect("edited file")
                .as_ref(),
            b"after"
        );
        assert_eq!(
            captured
                .generation
                .read("/unchanged.txt", 64)
                .await
                .expect("unchanged file")
                .as_ref(),
            b"same"
        );
        assert!(captured.generation.stat("/new.tmp").await.is_err());
        assert_eq!(
            captured.tracked_paths,
            BTreeSet::from(["kept.txt".to_owned(), "unchanged.txt".to_owned()])
        );
    }

    #[tokio::test]
    async fn nested_gitignore_change_forces_exact_capture() {
        let fs = Fs::memory();
        let workspace = fs
            .create_workspace("git-capture-nested-ignore")
            .await
            .expect("workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x61; 16]))
            .await
            .expect("transaction");
        transaction.create_dir_all("/src").await.expect("directory");
        transaction
            .write_text("/src/ignored.txt", "present before ignore")
            .await
            .expect("file");
        let TransactionCommit::Committed(previous_live) =
            transaction.commit().await.expect("base commit")
        else {
            panic!("base fixture did not commit");
        };
        let previous_capture = capture_git_compatible_generation(
            &workspace,
            &GitIgnorePolicy::default(),
            &BTreeSet::new(),
            OperationId::from_bytes([0x62; 16]),
        )
        .await
        .expect("base capture");

        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::from_bytes([0x63; 16]))
            .await
            .expect("transaction");
        transaction
            .write_text("/src/.gitignore", "ignored.txt\n")
            .await
            .expect("nested ignore");
        transaction.commit().await.expect("live edit");

        let captured = capture_git_compatible_generation_incremental(
            &workspace,
            &previous_live,
            &previous_capture.generation,
            &GitIgnorePolicy::parse("src/ignored.txt\n"),
            &BTreeSet::new(),
            OperationId::from_bytes([0x64; 16]),
        )
        .await
        .expect("capture");
        assert!(captured.generation.stat("/src/ignored.txt").await.is_err());
        assert!(captured.generation.stat("/src/.gitignore").await.is_ok());
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
                    tree: Some(tree(3)),
                    tracked_paths: None,
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
                    tree: Some(tree(4)),
                    tracked_paths: None,
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
                    tree: Some(tree(4)),
                    tracked_paths: None,
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
                    tree: Some(tree(5)),
                    tracked_paths: None,
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
    async fn distributed_publication_records_one_idempotent_merge_commit() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let GitCommandOutput::Committed(initial) = repository
            .record_commit(
                None,
                generation(1),
                BTreeSet::from(["tracked.txt".to_owned()]),
                "initial",
                "parent",
                1,
            )
            .await
            .expect("initial commit")
        else {
            panic!("expected initial commit");
        };
        let source = WorkspaceId::derive(
            [0x51; 16],
            &WorkspaceName::new("publication-source").expect("workspace name"),
        );
        repository
            .register_branch_workspace("agents/child", source, Some(initial.id), false)
            .await
            .expect("source branch");
        let tracked = BTreeSet::from(["tracked.txt".to_owned(), "new.txt".to_owned()]);
        let GitCommandOutput::Committed(merged) = repository
            .record_publication(GitPublicationRecord {
                tree: tree(2),
                workspace_tree: tree(3),
                source_head: Some(initial.id),
                tracked_paths: tracked.clone(),
                message: "Merge agents/child".to_owned(),
                author: "parent".to_owned(),
                authored_at_seconds: 2,
            })
            .await
            .expect("publication commit")
        else {
            panic!("expected publication commit");
        };
        assert_eq!(merged.parents, vec![initial.id]);
        assert_eq!(merged.tracked_paths, tracked);
        assert_eq!(
            repository.tracked_paths().await.expect("tracked paths"),
            tracked
        );
        assert_eq!(
            repository
                .record_publication(GitPublicationRecord {
                    tree: tree(2),
                    workspace_tree: tree(3),
                    source_head: Some(initial.id),
                    tracked_paths: BTreeSet::new(),
                    message: "retry".to_owned(),
                    author: "parent".to_owned(),
                    authored_at_seconds: 99,
                })
                .await
                .expect("exact retry"),
            GitCommandOutput::NoOp
        );
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
                    tree: tree(1),
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
            } if from == tree(1) && to == tree(2)
        ));
    }

    #[tokio::test]
    async fn every_branch_requests_and_registers_a_distinct_workspace() {
        let repository = GitCompatRepository::new(workspace(), MemoryGitCompatStore::new());
        let GitCommandOutput::Prepared {
            transition,
            action:
                GitFilesystemAction::ForkBranch {
                    source_tree,
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
        assert_eq!(source_tree, tree(4));
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
            .execute(GitCommand::Status, GitTreeRef::exact(child, generation(4)))
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

    #[test]
    fn argv_parser_has_an_explicit_command_conformance_matrix() {
        let cases = [
            ((&["status", "--short"] as &[&str]), GitCommand::Status),
            (
                (&["diff", "--staged"] as &[&str]),
                GitCommand::Diff { cached: true },
            ),
            (
                (&["log", "--max-count=7"] as &[&str]),
                GitCommand::Log { maximum: 7 },
            ),
            (
                (&["show", "HEAD"] as &[&str]),
                GitCommand::Show {
                    object: Some(GitObjectName("HEAD".to_owned())),
                },
            ),
            (
                (&["add", "--all"] as &[&str]),
                GitCommand::Add { paths: Vec::new() },
            ),
            (
                (&["commit", "--message=ready", "--author=agent"] as &[&str]),
                GitCommand::Commit {
                    message: "ready".to_owned(),
                    author: "agent".to_owned(),
                    authored_at_seconds: 10,
                },
            ),
            (
                (&["branch", "topic"] as &[&str]),
                GitCommand::Branch {
                    create: Some("topic".to_owned()),
                },
            ),
            (
                (&["switch", "-c", "topic"] as &[&str]),
                GitCommand::Switch {
                    branch: "topic".to_owned(),
                    create: true,
                },
            ),
            (
                (&["restore", "--source=HEAD", "--", "file"] as &[&str]),
                GitCommand::Restore {
                    source: Some(GitObjectName("HEAD".to_owned())),
                    paths: vec!["file".to_owned()],
                },
            ),
            (
                (&["reset", "--hard", "HEAD"] as &[&str]),
                GitCommand::Reset {
                    target: GitObjectName("HEAD".to_owned()),
                    mode: GitResetMode::Hard,
                },
            ),
            (
                (&["merge", "topic"] as &[&str]),
                GitCommand::Merge {
                    branch: "topic".to_owned(),
                },
            ),
            (
                (&["rebase", "main"] as &[&str]),
                GitCommand::Rebase {
                    branch: "main".to_owned(),
                },
            ),
            ((&["stash", "push"] as &[&str]), GitCommand::StashPush),
            ((&["stash", "pop"] as &[&str]), GitCommand::StashPop),
            (
                (&["cherry-pick", "HEAD"] as &[&str]),
                GitCommand::CherryPick {
                    object: GitObjectName("HEAD".to_owned()),
                },
            ),
            (
                (&["revert", "HEAD"] as &[&str]),
                GitCommand::Revert {
                    object: GitObjectName("HEAD".to_owned()),
                },
            ),
            (
                (&["tag", "v1", "HEAD"] as &[&str]),
                GitCommand::Tag {
                    name: Some("v1".to_owned()),
                    target: Some(GitObjectName("HEAD".to_owned())),
                    delete: false,
                },
            ),
            (
                (&["blame", "file"] as &[&str]),
                GitCommand::Blame {
                    path: "file".to_owned(),
                },
            ),
            (
                (&["grep", "needle", "src"] as &[&str]),
                GitCommand::Grep {
                    pattern: "needle".to_owned(),
                    path: Some("src".to_owned()),
                },
            ),
            (
                (&["clean", "-n"] as &[&str]),
                GitCommand::Clean { dry_run: true },
            ),
            (
                (&["archive", "HEAD"] as &[&str]),
                GitCommand::Archive {
                    object: Some(GitObjectName("HEAD".to_owned())),
                },
            ),
            (
                (&["bisect", "start", "bad", "good"] as &[&str]),
                GitCommand::Bisect {
                    arguments: vec!["start".to_owned(), "bad".to_owned(), "good".to_owned()],
                },
            ),
            (
                (&["checkout", "topic"] as &[&str]),
                GitCommand::Switch {
                    branch: "topic".to_owned(),
                    create: false,
                },
            ),
            (
                (&["checkout", "-b", "topic"] as &[&str]),
                GitCommand::Switch {
                    branch: "topic".to_owned(),
                    create: true,
                },
            ),
            (
                (&["checkout", "HEAD", "--", "src/lib.rs"] as &[&str]),
                GitCommand::Restore {
                    source: Some(GitObjectName("HEAD".to_owned())),
                    paths: vec!["src/lib.rs".to_owned()],
                },
            ),
            (
                (&["rev-parse", "--abbrev-ref", "HEAD"] as &[&str]),
                GitCommand::RevParse {
                    argument: "--abbrev-ref HEAD".to_owned(),
                },
            ),
            (
                (&["symbolic-ref", "--short", "HEAD"] as &[&str]),
                GitCommand::SymbolicRef { short: true },
            ),
            (
                (&["merge-base", "HEAD", "topic"] as &[&str]),
                GitCommand::MergeBase {
                    left: GitObjectName("HEAD".to_owned()),
                    right: GitObjectName("topic".to_owned()),
                },
            ),
            ((&["ls-files"] as &[&str]), GitCommand::LsFiles),
            (
                (&["check-ignore", "--", "target/file"] as &[&str]),
                GitCommand::CheckIgnore {
                    paths: vec!["target/file".to_owned()],
                },
            ),
        ];
        for (argv, expected) in cases {
            let argv = argv
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            assert_eq!(
                parse_argv::<MemoryGitCompatStoreError>(&argv, "default", 10)
                    .expect("supported argv"),
                expected,
                "argv: {argv:?}"
            );
        }
        assert_eq!(
            parse_argv::<MemoryGitCompatStoreError>(
                &["add".to_owned(), "--".to_owned(), "--all".to_owned()],
                "default",
                10,
            )
            .expect("literal option-looking path"),
            GitCommand::Add {
                paths: vec!["--all".to_owned()],
            }
        );
    }

    #[test]
    fn argv_rejects_semantically_unrepresentable_and_shell_composed_commands() {
        for command in [
            "clone",
            "fetch",
            "pull",
            "push",
            "remote",
            "gc",
            "repack",
            "cat-file",
            "hash-object",
            "init",
            "fsck",
            "prune",
            "pack-objects",
            "index-pack",
            "receive-pack",
            "upload-pack",
            "read-tree",
            "write-tree",
            "commit-tree",
            "update-index",
            "worktree",
            "submodule",
            "apply",
        ] {
            let argv = vec![command.to_owned()];
            assert!(
                matches!(
                    parse_argv::<MemoryGitCompatStoreError>(&argv, "agent", 10),
                    Err(GitCompatError::Unsupported { .. })
                ),
                "command should be explicitly unsupported: {command}"
            );
        }
        for argv in [
            vec!["status", "&&", "push"],
            vec!["status", "|", "cat"],
            vec!["status", ">result"],
            vec!["status", "$(push)"],
        ] {
            let argv = argv.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(matches!(
                parse_argv::<MemoryGitCompatStoreError>(&argv, "agent", 10),
                Err(GitCompatError::Unsupported { reason, .. }) if reason.contains("shell composition")
            ));
        }
        for argv in [
            vec!["status", "unexpected"],
            vec!["diff", "--stat"],
            vec!["reset", "HEAD", "path"],
            vec!["merge", "topic", "other"],
            vec!["restore", "--source", "HEAD"],
            vec!["clean", "-d"],
            vec!["clean", "-fd"],
            vec!["clean", "-nd"],
        ] {
            let argv = argv.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(parse_argv::<MemoryGitCompatStoreError>(&argv, "agent", 10).is_err());
        }
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
                    tree: tree(1),
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
