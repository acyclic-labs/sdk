use crate::Error;
use acyclic_fs::{
    GitCaptureAuthenticationError, GitCompatStore, WorkspaceLineageError, WorkspaceLineageStore,
};

/// Durable refs and lineage used by the shared command executor.
/// Storage adapters retain their original errors at the harness boundary.
pub trait CommandStore:
    GitCompatStore<Error = Self::StoreError> + WorkspaceLineageStore<Error = Self::StoreError> + Clone
{
    /// Original storage failure.
    type StoreError: std::error::Error + Send + Sync + 'static;

    /// Carries an adapter failure into the tool result.
    fn error(error: Self::StoreError) -> Error;

    /// Carries an ancestry failure without discarding its storage cause.
    fn lineage_error(error: WorkspaceLineageError<Self::StoreError>) -> Error;

    /// Carries a capture failure without discarding its original ownership.
    fn capture_error(error: GitCaptureAuthenticationError<Self::StoreError>) -> Error;
}

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
use acyclic_fs::{LocalCoreStateStore, LocalCoreStateStoreError};

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
impl CommandStore for LocalCoreStateStore {
    type StoreError = LocalCoreStateStoreError;

    fn error(error: Self::StoreError) -> Error {
        error.into()
    }

    fn lineage_error(error: WorkspaceLineageError<Self::StoreError>) -> Error {
        error.into()
    }

    fn capture_error(error: GitCaptureAuthenticationError<Self::StoreError>) -> Error {
        error.into()
    }
}

impl<P: acyclic_fs::distributed::AuthorityProvider + ?Sized> CommandStore
    for acyclic_fs::StreamGitCompatStore<P>
{
    type StoreError = acyclic_fs::StreamGitCompatStoreError;

    fn error(error: Self::StoreError) -> Error {
        Error::NativeGit {
            source: Box::new(super::Failure::StreamState(error)),
        }
    }

    fn lineage_error(error: WorkspaceLineageError<Self::StoreError>) -> Error {
        Error::NativeGit {
            source: Box::new(super::Failure::StreamLineage(error)),
        }
    }

    fn capture_error(error: GitCaptureAuthenticationError<Self::StoreError>) -> Error {
        Error::NativeGit {
            source: Box::new(super::Failure::StreamCaptureAuthentication(error)),
        }
    }
}
