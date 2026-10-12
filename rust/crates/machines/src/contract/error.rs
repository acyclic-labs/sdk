use super::*;

/// Provider-boundary failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("Machines service is unavailable")]
    Unavailable,
    #[error("operation is indeterminate: {0}")]
    Indeterminate(IdempotencyKey),
    #[error("operation observation is indeterminate; inspect operation {0}")]
    OperationIndeterminate(OperationId),
    #[error("operation failed")]
    Failed,
    #[error("operation cancelled")]
    Cancelled,
}

impl ProviderError {
    /// Stable, message-free name of this failure class.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::Unsupported(_) => "unsupported",
            Self::Invalid(_) => "invalid",
            Self::Rejected(_) => "rejected",
            Self::Unavailable => "unavailable",
            Self::Indeterminate(_) => "indeterminate",
            Self::OperationIndeterminate(_) => "operation_indeterminate",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}
