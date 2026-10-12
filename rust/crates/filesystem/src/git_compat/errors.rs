use super::*;

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
}
