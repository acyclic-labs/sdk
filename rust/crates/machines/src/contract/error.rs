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
    /// The original failure class and its source, retained together. Attaching
    /// another source keeps the first one; it never grows a wrapper chain.
    #[error(transparent)]
    Caused(ProviderFailure),
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
    /// Before dispatch, `machine` is absent and no work was submitted. After
    /// possible dispatch, it retains the original Machine's uncertain outcome;
    /// this failure never authorizes redispatch.
    #[error("guest deadline source lost (possibly dispatched machine: {machine:?}): {cause}")]
    DeadlineSourceLost {
        machine: Option<MachineId>,
        #[source]
        cause: DeadlineSourceLost,
    },
    #[error("operation failed")]
    Failed,
    #[error("operation cancelled")]
    Cancelled,
}

/// A source-bearing provider failure. Private fields keep its classification
/// free of nested source wrappers.
#[derive(Clone, Debug)]
pub struct ProviderFailure {
    classification: Box<ProviderError>,
    cause: Arc<dyn std::error::Error + Send + Sync>,
}

impl ProviderFailure {
    /// The original bare class; its source is available through `Error::source`.
    pub const fn classification(&self) -> &ProviderError {
        &self.classification
    }
}

impl fmt::Display for ProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.classification, self.cause)
    }
}

impl std::error::Error for ProviderFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

impl PartialEq for ProviderFailure {
    fn eq(&self, other: &Self) -> bool {
        self.classification == other.classification && Arc::ptr_eq(&self.cause, &other.cause)
    }
}
impl Eq for ProviderFailure {}

impl ProviderError {
    /// Attach the original source without changing retry or outcome semantics.
    /// If a source is already retained, it remains authoritative.
    pub fn with_cause(self, cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        match self {
            Self::Caused(_) => self,
            classification => Self::Caused(ProviderFailure {
                classification: Box::new(classification),
                cause: Arc::new(cause),
            }),
        }
    }

    /// The original outcome class, with the same Machine/operation identity.
    /// This borrows at most one frame regardless of observation history.
    pub const fn classification(&self) -> &Self {
        match self {
            Self::Caused(failure) => &failure.classification,
            classification => classification,
        }
    }

    /// Stable, message-free name of this failure class.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Caused(failure) => failure.classification.kind(),
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
            Self::DeadlineSourceLost { .. } => "deadline_source_lost",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}
