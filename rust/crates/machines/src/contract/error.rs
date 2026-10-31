use super::*;

/// A typed precondition a provider can refuse a mutation on.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Precondition {
    /// Destroying a fork source waits for its live forks: destroy them first.
    #[error("the machine has live forks; recovery: destroy its forks first")]
    LiveForks,
}

impl Precondition {
    /// Stable wire name, carried as the gRPC `FAILED_PRECONDITION` details.
    pub const fn wire(self) -> &'static str {
        match self {
            Self::LiveForks => "acyclic.machines.precondition.live_forks",
        }
    }

    /// The precondition a wire name names.
    pub fn from_wire(value: &[u8]) -> Option<Self> {
        [Self::LiveForks]
            .into_iter()
            .find(|precondition| precondition.wire().as_bytes() == value)
    }
}

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
    /// The request is valid but the target's state refuses it until the
    /// named precondition holds; nothing changed and the key is unspent.
    #[error("failed precondition: {0}")]
    FailedPrecondition(Precondition),
    #[error("Machines service is unavailable")]
    Unavailable,
    #[error("operation is indeterminate: {0}")]
    Indeterminate(IdempotencyKey),
    #[error("operation observation is indeterminate; inspect operation {0}")]
    OperationIndeterminate(OperationId),
    #[error("guest command outcome is indeterminate on machine {0}")]
    CommandIndeterminate(MachineId),
    #[error(
        "guest file outcome is indeterminate on machine {0}; recovery: reconcile the original file operation without redispatch"
    )]
    FileIndeterminate(MachineId),
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
            Self::FailedPrecondition(_) => "failed_precondition",
            Self::Unavailable => "unavailable",
            Self::Indeterminate(_) => "indeterminate",
            Self::OperationIndeterminate(_) => "operation_indeterminate",
            Self::CommandIndeterminate(_) => "command_indeterminate",
            Self::FileIndeterminate(_) => "file_indeterminate",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}
