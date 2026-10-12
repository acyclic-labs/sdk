use super::*;

/// Native materialization failure.
#[derive(Debug, Error)]
pub enum NativeTreeMaterializationError {
    /// Native materialization layout is invalid.
    #[error("native materialization layout is invalid")]
    InvalidLayout,
    /// A required native entry is missing.
    #[error("required native entry is missing")]
    MissingEntry,
    /// Native materialization observed an external mutation.
    #[error("native materialization observed an external mutation")]
    ExternalMutation,
    /// Native materialization found an unexpected backup.
    #[error("native materialization found an unexpected backup")]
    UnexpectedBackup,
    /// Native materialization rename edits are unsupported.
    #[error("native materialization rename edits are unsupported")]
    UnsupportedRename,
    /// Native materialization cannot verify complete source metadata.
    #[error("native materialization cannot verify complete source metadata")]
    UnverifiedSourceMetadata,
    /// Native materialization cancelled.
    #[error("native materialization cancelled")]
    Cancelled,
    /// Original native work state is poisoned.
    #[error("original native work state is poisoned")]
    OriginalWorkPoisoned,
    /// Native preparation has an unacknowledged allocation.
    #[error("native preparation has an unacknowledged allocation")]
    IndeterminatePreparation,
    /// Native journal has no admitted allocation.
    #[error("native journal has no admitted allocation")]
    MissingCustody,
    /// Native builtin edit conflicts with its read preimage.
    #[error("native builtin edit conflicts with its read preimage")]
    PreimageConflict,
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Finite work allowance was exhausted.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Fallible allocation failed.
    #[error("native allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
    /// Current held root differs from its original allocation.
    #[error("native allocation changed: expected {expected:?}, observed {actual:?}")]
    AllocationChanged {
        /// Original allocation.
        expected: crate::NativeRootIdentity,
        /// Current allocation.
        actual: crate::NativeRootIdentity,
    },
    /// Complete builtin payload exceeds its admitted cap.
    #[error("native payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Admitted byte cap.
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
    /// Recovers a typed failure carried by the original runtime meter.
    pub fn from_runtime_error(error: std::io::Error) -> Self {
        if error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<crate::DemandError>())
            .is_some_and(|cause| {
                matches!(
                    cause,
                    crate::DemandError::Work(_)
                        | crate::DemandError::Cancelled
                        | crate::DemandError::Allocation(_)
                        | crate::DemandError::OriginalWorkPoisoned
                )
            })
        {
            native_materialization_work_error(crate::demand::native_demand_error(error))
        } else {
            Self::Io(error)
        }
    }
}
