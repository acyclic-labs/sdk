use crate::Error;
use acyclic_fs::{
    GitCaptureAuthenticationError, GitCaptureError, GitPatchError,
    OperationWindowError, StreamOperationWindowStoreError, WorkspaceLineageError,
};

#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
use acyclic_fs::{LocalCoreStateStore, LocalCoreStateStoreError};

/// Original SDK causes from the managed git facade.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// Durable provider-backed refs and outcomes.
    #[error(transparent)]
    StreamState(acyclic_fs::StreamGitCompatStoreError),
    /// Provider-backed ancestry admission.
    #[error(transparent)]
    StreamLineage(WorkspaceLineageError<acyclic_fs::StreamGitCompatStoreError>),
    /// Provider-backed tree authentication.
    #[error(transparent)]
    StreamCaptureAuthentication(GitCaptureAuthenticationError<acyclic_fs::StreamGitCompatStoreError>),
    /// Workspace merge-driver configuration failure.
    #[error(transparent)]
    Policy(#[from] acyclic_fs::DriverRegistrationError),
    /// Operation-window coordination failure.
    #[error(transparent)]
    OperationWindow(#[from] OperationWindowError<StreamOperationWindowStoreError>),
    /// Managed tree capture failure.
    #[error(transparent)]
    Capture(#[from] GitCaptureError),
    /// Lazy tree authentication failure.
    #[error(transparent)]
    #[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
    CaptureAuthentication(#[from] GitCaptureAuthenticationError<LocalCoreStateStoreError>),
    /// Workspace ancestry failure.
    #[error(transparent)]
    #[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
    Lineage(#[from] WorkspaceLineageError<LocalCoreStateStoreError>),
    /// Patch application failure.
    #[error(transparent)]
    Patch(#[from] GitPatchError),
    /// Durable git state failure.
    #[error(transparent)]
    #[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
    State(#[from] LocalCoreStateStoreError),
}

impl From<OperationWindowError<StreamOperationWindowStoreError>> for Error {
    fn from(source: OperationWindowError<StreamOperationWindowStoreError>) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::OperationWindow(source)),
        }
    }
}

impl From<GitCaptureError> for Error {
    fn from(source: GitCaptureError) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::Capture(source)),
        }
    }
}

#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
impl From<GitCaptureAuthenticationError<LocalCoreStateStoreError>> for Error {
    fn from(source: GitCaptureAuthenticationError<LocalCoreStateStoreError>) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::CaptureAuthentication(source)),
        }
    }
}

#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
impl From<WorkspaceLineageError<LocalCoreStateStoreError>> for Error {
    fn from(source: WorkspaceLineageError<LocalCoreStateStoreError>) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::Lineage(source)),
        }
    }
}

impl From<GitPatchError> for Error {
    fn from(source: GitPatchError) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::Patch(source)),
        }
    }
}

#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
impl From<LocalCoreStateStoreError> for Error {
    fn from(source: LocalCoreStateStoreError) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::State(source)),
        }
    }
}

#[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::GitCompatError<LocalCoreStateStoreError>> for Error {
    fn from(source: acyclic_fs::GitCompatError<LocalCoreStateStoreError>) -> Self {
        super::errors::compat_error::<LocalCoreStateStore>(source)
    }
}

impl From<acyclic_fs::DriverRegistrationError> for Error {
    fn from(source: acyclic_fs::DriverRegistrationError) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::Policy(source)),
        }
    }
}
