use super::*;

/// Native materialization failure.
#[derive(Debug, Error)]
pub enum NativeTreeMaterializationError {
    /// The operation layout or persisted image is invalid.
    #[error("invalid native materialization layout")]
    InvalidLayout,
    /// A required target or preimage is missing.
    #[error("required native entry is missing")]
    MissingEntry,
    /// An endpoint changed outside this operation.
    #[error("native materialization observed an external mutation")]
    ExternalMutation,
    /// A backup exists before capture.
    #[error("unexpected native materialization backup")]
    UnexpectedBackup,
    /// Rename edits are unsupported.
    #[error("native rename edits are unsupported")]
    UnsupportedRename,
    /// Source metadata cannot be fully verified.
    #[error("source metadata is unverified")]
    UnverifiedSourceMetadata,
    /// The invocation was cancelled.
    #[error("native materialization cancelled")]
    Cancelled,
    /// The original accounting context is poisoned.
    #[error("original native work is poisoned")]
    OriginalWorkPoisoned,
    /// Directory creation has an unacknowledged effect.
    #[error("native preparation is indeterminate")]
    IndeterminatePreparation,
    /// A payload lacks its admitted allocation.
    #[error("native payload has no admitted allocation")]
    MissingCustody,
    /// A builtin edit has a different preimage.
    #[error("native edit preimage conflicts")]
    PreimageConflict,
    /// Host I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A finite allowance was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Fallible allocation failed.
    #[error("native allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
    /// The named root no longer has its original allocation.
    #[error("native allocation changed")]
    AllocationChanged {
        /// Original allocation.
        expected: crate::NativeRootIdentity,
        /// Observed allocation.
        actual: crate::NativeRootIdentity,
    },
    /// A complete builtin payload exceeds its admitted cap.
    #[error("native payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Admitted cap.
        maximum: u64,
        /// Observed length.
        actual: u64,
    },
}

pub(crate) fn native_materialization_work_error(
    error: crate::DemandError,
) -> NativeTreeMaterializationError {
    match error {
        crate::DemandError::Work(error) => NativeTreeMaterializationError::Work(error),
        crate::DemandError::Cancelled => NativeTreeMaterializationError::Cancelled,
        crate::DemandError::OriginalWorkPoisoned => {
            NativeTreeMaterializationError::OriginalWorkPoisoned
        }
        crate::DemandError::Allocation(error) => {
            NativeTreeMaterializationError::AllocationFailed(error)
        }
        crate::DemandError::Io(error) => NativeTreeMaterializationError::Io(error),
        error => NativeTreeMaterializationError::Io(std::io::Error::other(error)),
    }
}

impl NativeTreeMaterializationError {
    /// Recovers the typed failure retained by a runtime meter.
    pub fn from_runtime_error(error: std::io::Error) -> Self {
        native_materialization_work_error(crate::demand::native_demand_error(error))
    }
}
