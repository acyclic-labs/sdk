use crate::Error;
use acyclic_fs::{GitCompatError, GitCompatRunError};

pub(super) fn run_error(
    error: GitCompatRunError<acyclic_fs::LocalCoreStateStoreError, Error>,
) -> Error {
    let error = match error {
        GitCompatRunError::Executor(error) => return error,
        GitCompatRunError::Compat(error) => error,
    };
    compat_error(error)
}

pub(super) fn compat_error(error: GitCompatError<acyclic_fs::LocalCoreStateStoreError>) -> Error {
    match error {
        GitCompatError::InvalidCommand(message) => Error::Invalid(message),
        GitCompatError::UnknownObject(name) => Error::NotFound(name),
        GitCompatError::BranchExists(name) => Error::Conflict(name),
        GitCompatError::Unsupported { command, reason } => {
            Error::Unsupported(format!("{command}: {reason}"))
        }
        GitCompatError::Store(error) => Error::from(error),
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_error_keeps_its_cause_and_original_allocation() -> crate::Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
        let cancellation = CancellationToken::new();
        let work =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let custody = work.claim(123, 1, WorkCounters::UNCHARGED)?;
        let error = run_error(GitCompatRunError::Executor(
            acyclic_fs::LazyWorkspaceError::NativeRecordIo(
                std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                Some(custody),
            )
            .into(),
        ));
        assert_eq!(work.live_allocation_bytes(), baseline + 123);
        let Error::NativeSource { source, .. } = &error else {
            return Err(Error::Invalid("native source cause was lost".into()));
        };
        let acyclic_fs::LazyWorkspaceError::NativeRecordIo(cause, Some(allocation)) =
            source.as_ref()
        else {
            return Err(Error::Invalid("native IO cause or custody was lost".into()));
        };
        assert_eq!(cause.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(allocation.bytes(), 123);
        drop(error);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
