use super::*;

/// Loss of the original deadline producer. Clones retain the same native cause;
/// equality identifies that cause, rather than comparing formatted messages.
#[derive(Clone, Debug, thiserror::Error)]
#[error(
    "original deadline producer lost: {0}; recovery: restore the original clock owner before new admission"
)]
pub struct DeadlineSourceLost(#[source] pub Arc<std::io::Error>);

impl PartialEq for DeadlineSourceLost {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for DeadlineSourceLost {}

/// Why observation of the original operation stopped. Neither variant attests
/// physical cancellation of work that may already have been dispatched.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DeadlineStop {
    /// The caller's original deadline elapsed.
    #[error("original guest deadline expired")]
    Expired,
    /// The original operation or its retained authority was cancelled.
    #[error("original guest operation cancelled")]
    Cancelled,
    /// The original clock/alarm producer failed.
    #[error(transparent)]
    SourceLost(#[from] DeadlineSourceLost),
}

impl DeadlineStop {
    /// Refuse before dispatch, preserving producer loss separately from expiry.
    pub fn before_dispatch(self) -> ProviderError {
        match self {
            Self::Expired => ProviderError::Rejected(
                "guest deadline expired before dispatch; recovery: obtain a fresh admitted deadline".into(),
            ),
            Self::Cancelled => ProviderError::Cancelled,
            Self::SourceLost(cause) => ProviderError::DeadlineSourceLost { machine: None, cause },
        }
    }

    /// Retain uncertainty about the original command after possible dispatch.
    pub fn command(self, machine: MachineId) -> ProviderError {
        match self {
            Self::Expired | Self::Cancelled => ProviderError::CommandIndeterminate(machine),
            Self::SourceLost(cause) => ProviderError::DeadlineSourceLost {
                machine: Some(machine),
                cause,
            },
        }
    }

    /// Retain uncertainty about the original file operation after dispatch.
    pub fn file(self, machine: MachineId) -> ProviderError {
        match self {
            Self::Expired | Self::Cancelled => ProviderError::FileIndeterminate(machine),
            Self::SourceLost(cause) => ProviderError::DeadlineSourceLost {
                machine: Some(machine),
                cause,
            },
        }
    }
}
