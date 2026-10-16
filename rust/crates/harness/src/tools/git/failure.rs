use crate::Error;
use acyclic_fs::{
    GitCaptureAuthenticationError, GitCaptureError, GitPatchError, LocalCoreStateStoreError,
    OperationWindowError, StreamOperationWindowStoreError, WorkspaceLineageError,
};

/// Original SDK causes from the managed git facade.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
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
    CaptureAuthentication(#[from] GitCaptureAuthenticationError<LocalCoreStateStoreError>),
    /// Workspace ancestry failure.
    #[error(transparent)]
    Lineage(#[from] WorkspaceLineageError<LocalCoreStateStoreError>),
    /// Patch application failure.
    #[error(transparent)]
    Patch(#[from] GitPatchError),
    /// Durable git state failure.
    #[error(transparent)]
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

impl From<GitCaptureAuthenticationError<LocalCoreStateStoreError>> for Error {
    fn from(source: GitCaptureAuthenticationError<LocalCoreStateStoreError>) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::CaptureAuthentication(source)),
        }
    }
}

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

impl From<LocalCoreStateStoreError> for Error {
    fn from(source: LocalCoreStateStoreError) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::State(source)),
        }
    }
}

impl From<acyclic_fs::GitCompatError<LocalCoreStateStoreError>> for Error {
    fn from(source: acyclic_fs::GitCompatError<LocalCoreStateStoreError>) -> Self {
        super::errors::compat_error(source)
    }
}

impl From<acyclic_fs::DriverRegistrationError> for Error {
    fn from(source: acyclic_fs::DriverRegistrationError) -> Self {
        Self::NativeGit {
            source: Box::new(Failure::Policy(source)),
        }
    }
}
