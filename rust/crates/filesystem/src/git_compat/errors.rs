use super::*;

#[derive(Debug, Error)]
pub(super) enum GitCompatStateError {
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

pub(super) fn map_state_error<E: std::error::Error + 'static>(
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
