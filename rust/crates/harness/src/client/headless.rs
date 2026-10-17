//! Filesystem-driven headless policy, shared by native clients.
use crate::{Error, wire};
mod runner;
pub use runner::{HeadlessClient, run_headless, run_jsonl};

/// Settled task outcome with stable process exit codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum HeadlessOutcome {
    /// The task completed, or is idle awaiting another prompt.
    Completed,
    /// Execution failed.
    Failed,
    /// Cancellation settled.
    Cancelled,
    /// Execution needs external input or approval.
    Blocked,
    /// The service cannot establish whether an operation completed.
    Indeterminate,
}

impl HeadlessOutcome {
    pub(crate) fn from_error(error: &Error) -> Self {
        #[cfg(feature = "filesystem")]
        if let Error::TaskFiles(error) = error {
            if let crate::task_volume::Error::FileRead(cause) = error.as_ref() {
                return Self::from_error(cause);
            }
        }
        if matches!(error, Error::GoalBlocked(_)) {
            return Self::Blocked;
        }
        match error.code() {
            wire::ErrorCode::Indeterminate => Self::Indeterminate,
            wire::ErrorCode::InteractionCancelled => Self::Cancelled,
            wire::ErrorCode::InteractionDeclined
            | wire::ErrorCode::InteractionExpired
            | wire::ErrorCode::InteractionDenied => Self::Blocked,
            _ => Self::Failed,
        }
    }

    /// Stable CLI contract: completed=0, failed=1, cancelled=2, blocked=3, indeterminate=4.
    #[must_use]
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Completed => 0,
            Self::Failed => 1,
            Self::Cancelled => 2,
            Self::Blocked => 3,
            Self::Indeterminate => 4,
        }
    }
}
