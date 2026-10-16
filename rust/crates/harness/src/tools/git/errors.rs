use crate::Error;
use acyclic_fs::{GitCompatError, GitCompatRunError};

pub(super) fn run_error<S: std::error::Error + 'static>(
    error: GitCompatRunError<S, Error>,
) -> Error {
    let error = match error {
        GitCompatRunError::Executor(error) => return error,
        GitCompatRunError::Compat(error) => error,
    };
    match error {
        GitCompatError::InvalidCommand(message) => Error::Invalid(message),
        GitCompatError::UnknownObject(name) => Error::NotFound(name),
        GitCompatError::BranchExists(name) => Error::Conflict(name),
        GitCompatError::Unsupported { command, reason } => {
            Error::Unsupported(format!("{command}: {reason}"))
        }
        GitCompatError::Store(error) => Error::Storage(error.to_string()),
        GitCompatError::InvalidState | GitCompatError::MissingResultGeneration => {
            Error::Storage(error.to_string())
        }
        GitCompatError::WorkspaceMismatch
        | GitCompatError::UnbornHead
        | GitCompatError::NothingToCommit
        | GitCompatError::Contended
        | GitCompatError::TransitionPending(_)
        | GitCompatError::StaleTransition => Error::Conflict(error.to_string()),
    }
}
