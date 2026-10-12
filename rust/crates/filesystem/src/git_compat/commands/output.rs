use super::*;

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
