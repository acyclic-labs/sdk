use super::*;

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
        /// Paths participating in a compatibility-history restore. `None`
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
