use super::*;

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
        /// Already tracked paths remain visible even if later ignored.
        tracked_paths: BTreeSet<String>,
    },
    /// Move the live workspace to an exact generation.
    RestoreGeneration {
        /// Tree to install.
        tree: GitTreeRef,
        /// Selected restore paths; absent means the complete tree.
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
#[allow(
    clippy::large_enum_variant,
    reason = "keeps the public result shape direct; this is not a hot path"
)]
pub enum GitFilesystemResult {
    /// An eligible compatibility snapshot was captured.
    Captured {
        /// Filtered compatibility tree.
        tree: GitTreeRef,
        /// Paths represented by the captured compatibility history.
        tracked_paths: BTreeSet<String>,
        /// Authenticated origin when `tree` belongs to a filtered SDK fork.
        proof: Option<GitCaptureProof>,
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
    pub(super) fn resulting_tree(&self) -> Option<GitTreeRef> {
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
#[allow(
    clippy::large_enum_variant,
    reason = "CLI responses favor a direct typed shape over allocation"
)]
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
