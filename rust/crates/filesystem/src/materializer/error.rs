use super::*;

/// Native filesystem, recovery or finite-work failure.
#[derive(Debug, Error)]
pub enum NativeTreeMaterializationError {
    /// Invalid operation layout or persisted payload.
    #[error("invalid native materialization layout")]
    InvalidLayout,
    /// Required entry is absent.
    #[error("required native entry is missing")]
    MissingEntry,
    /// A witnessed endpoint changed.
    #[error("native endpoint changed")]
    ExternalMutation,
    /// Backup exists before capture.
    #[error("unexpected native backup")]
    UnexpectedBackup,
    /// Native rename edits are unsupported.
    #[error("native rename edit is unsupported")]
    UnsupportedRename,
    /// Authored metadata cannot be verified on this host.
    #[error("source metadata is unverified")]
    UnverifiedSourceMetadata,
    /// Invocation stopped.
    #[error("native materialization cancelled")]
    Cancelled,
    /// Original ledger is poisoned.
    #[error("original native work is poisoned")]
    OriginalWorkPoisoned,
    /// Directory creation has an unacknowledged effect.
    #[error("native preparation is indeterminate")]
    IndeterminatePreparation,
    /// Payload lacks its admitted allocation.
    #[error("native payload lacks allocation")]
    MissingCustody,
    /// Builtin preimage changed.
    #[error("native preimage conflicts")]
    PreimageConflict,
    /// Host I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Finite allowance failed.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Allocator failed.
    #[error("native allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
    /// Named allocation differs from its witness.
    #[error("native allocation changed")]
    AllocationChanged {
        /// Original allocation.
        expected: crate::NativeRootIdentity,
        /// Current allocation.
        actual: crate::NativeRootIdentity,
    },
    /// Complete payload exceeds its byte cap.
    #[error("native payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Admitted cap.
        maximum: u64,
        /// Observed size.
        actual: u64,
    },
}
pub(crate) fn native_materialization_work_error(
    error: crate::DemandError,
) -> NativeTreeMaterializationError {
    use crate::DemandError as D;
    use NativeTreeMaterializationError as N;
    match error {
        D::Work(error) => N::Work(error),
        D::Cancelled => N::Cancelled,
        D::OriginalWorkPoisoned => N::OriginalWorkPoisoned,
        D::Allocation(error) => N::AllocationFailed(error),
        D::Io(error) => N::Io(error),
        error => N::Io(std::io::Error::other(error)),
    }
}
impl NativeTreeMaterializationError {
    /// Recovers the original typed runtime failure.
    pub fn from_runtime_error(error: std::io::Error) -> Self {
        native_materialization_work_error(crate::demand::native_demand_error(error))
    }
}
