use super::*;

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
    pub tracked_paths: BTreeSet<String>,
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
    pub external_parents: BTreeSet<GitCommitId>,
    /// Lightweight tags.
    pub tags: BTreeMap<String, GitCommitId>,
    /// Most-recent-first prior branch heads.
    pub reflog: Vec<Option<GitCommitId>>,
    /// Stashed exact generations, newest last.
    pub stash: Vec<GitTreeRef>,
    /// Durable sequencer transition prepared before filesystem side effects.
    pub pending: Option<GitPendingTransition>,
    /// Active first-parent bisection, if any.
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
            pending: None,
            bisect: None,
        }
    }

    pub(super) fn current(&self) -> Result<&GitBranch, GitCompatStateError> {
        self.branches
            .get(&self.current_branch)
            .ok_or(GitCompatStateError::Invalid)
    }

    pub(super) fn current_mut(&mut self) -> Result<&mut GitBranch, GitCompatStateError> {
        self.branches
            .get_mut(&self.current_branch)
            .ok_or(GitCompatStateError::Invalid)
    }
}

uuid_identity!(
    GitTransitionId,
    "Stable identity for one prepared compatibility filesystem transition."
);

impl GitTransitionId {
    /// Returns the underlying operation identity.
    #[must_use]
    pub const fn operation_id(self) -> OperationId {
        OperationId::from_bytes(self.into_bytes())
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
