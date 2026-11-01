use crate::Error;
use acyclic_fs::{GitCompatError, GitCompatRunError};

pub(super) fn run_error<S: super::CommandStore>(
    error: GitCompatRunError<S::StoreError, Error>,
) -> Error {
    let error = match error {
        GitCompatRunError::Executor(error) => return error,
        GitCompatRunError::Compat(error) => error,
    };
    compat_error::<S>(error)
}

pub(crate) fn compat_error<S: super::CommandStore>(error: GitCompatError<S::StoreError>) -> Error {
    match error {
        GitCompatError::InvalidCommand(message) => Error::Invalid(message),
        GitCompatError::UnknownObject(name) => Error::NotFound(name),
        GitCompatError::BranchExists(name) => Error::Conflict(name),
        GitCompatError::Unsupported { command, reason } => {
            Error::Unsupported(format!("{command}: {reason}"))
        }
        GitCompatError::Store(error) => S::error(error),
        GitCompatError::RequestObservation(error) => {
            acyclic_fs::LazyWorkspaceError::NativeRecordIo(error, None).into()
        }
