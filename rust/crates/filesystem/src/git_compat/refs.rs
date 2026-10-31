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

impl GitCompatState {
    /// Read-only status of an already observed working tree; unresolved lazy
    /// source state stays unknown rather than forcing a filesystem scan.
    /// Returns `None` if the recorded current branch is absent.
    pub fn status(&self, workspace: GitTreeRef) -> Option<GitStatus> {
        let current = self.current().ok()?;
        let head = self
            .bisect
            .as_ref()
            .and_then(|bisect| bisect.current)
            .or(current.head);
        let tree = head.and_then(|id| self.commits.get(&id).map(|commit| commit.workspace_tree));
        Some(GitStatus {
            branch: current.name.clone(),
            head,
            workspace,
            dirty: match tree {
                Some(head) if head == workspace => GitDirtyState::Clean,
                Some(GitTreeRef::Lazy(_)) | None if matches!(workspace, GitTreeRef::Lazy(_)) => {
                    GitDirtyState::Unknown
                }
                _ => GitDirtyState::Dirty,
            },
            all_changes_staged: true,
        })
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
    /// Workspaces of discarded branches that recorded commits or stashes
    /// still held here. Workspace ownership outlives the branch name, so
    /// discarding a branch never orphans the history it already produced.
    /// Records written before discard existed hold no retired workspace, so
    /// an absent field reads as the empty set; no other value is possible.
    #[serde(default)]
    pub retired_workspaces: BTreeSet<WorkspaceId>,
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
    /// Native publication owed after reconciling this exact source generation.
    /// Cleared only once the publication journal acknowledges the checkout.
    pub checkout_reconciliation: Option<(OperationId, GitGenerationRef)>,
    /// The capture owed to a file publication's physical write: the
    /// capture's operation and the staged generation it commits on. Recorded
    /// before the write reaches disk and cleared once that capture is done,
    /// so no other operation's capture can record the write as its own.
    /// Records written before it existed owe nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_capture: Option<(OperationId, GitGenerationRef)>,
    /// Active first-parent bisection, if any.
    pub bisect: Option<GitBisectState>,
    /// The results of state-changing requests, each keyed by the request
    /// that started the command and committed with the command's own effect,
    /// so a replayed request returns its saved result instead of running
    /// again. A result is kept only until its request is answered: once the
    /// reply is durable it is the request's record, so this holds only the
    /// results of requests still awaiting their reply (at most the requests
    /// in flight when an owner stopped), never a history.
    /// Records written before it existed hold none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<OperationId, GitCommandOutput>,
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
            retired_workspaces: BTreeSet::new(),
            commits: BTreeMap::new(),
            external_parents: BTreeSet::new(),
            tags: BTreeMap::new(),
            reflog: Vec::new(),
            stash: Vec::new(),
            pending: None,
            checkout_reconciliation: None,
            write_capture: None,
            bisect: None,
            requests: BTreeMap::new(),
        }
    }

    /// The saved result of request `operation`.
    #[must_use]
    pub fn request(&self, operation: OperationId) -> Option<&GitCommandOutput> {
        self.requests.get(&operation)
    }

    /// Saves `output` as the result of `operation`, the request that started
    /// the command, in the commit that carries its effect, dropping the
    /// results of requests `unanswered` reports answered. Only unanswered
    /// results are ever kept, so the sweep visits only those.
    pub(super) fn record_request(
        &mut self,
        operation: Option<OperationId>,
        unanswered: Option<&GitRequestUnanswered>,
        output: &GitCommandOutput,
    ) {
        if let Some(unanswered) = unanswered {
            self.requests.retain(|request, _| unanswered(*request));
        }
        if let Some(operation) = operation
            && unanswered.is_none_or(|unanswered| unanswered(operation))
        {
            self.requests.insert(operation, output.clone());
        }
    }

    pub(super) fn current(&self) -> Result<&GitBranch, ()> {
        self.branches.get(&self.current_branch).ok_or(())
    }

    pub(super) fn current_mut(&mut self) -> Result<&mut GitBranch, ()> {
        self.branches.get_mut(&self.current_branch).ok_or(())
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
        /// Actual paths requiring text or manual resolution before continuation.
        conflicts: Option<BTreeMap<String, Option<crate::FileId>>>,
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
    /// The request that started the command, whose result its settlement
    /// saves, whichever request resumes it. Records written before it
    /// existed name none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<OperationId>,
    /// Filesystem action to execute idempotently.
    pub action: GitFilesystemAction,
    /// Mutation finalized only after successful action execution.
    pub mutation: GitPendingMutation,
}
