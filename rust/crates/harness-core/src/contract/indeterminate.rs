use super::{Error, OperationId};
use serde::{Deserialize, Serialize};

/// Why the completion of admitted work is uncertain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum IndeterminateCause {
    /// Completion was not observed; only reconciliation can settle it.
    Unobserved,
    /// A task command left the work uncertain.
    Command(CommandIndeterminacy),
}

/// A task command that left its task's completion uncertain.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error, crate::HeapLayout,
)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[error("task command {command} {reason}")]
pub struct CommandIndeterminacy {
    /// Kind of the command, such as `acyclic.model.v1`.
    pub command: String,
    /// What the command did.
    pub reason: CommandIndeterminacyReason,
}

/// What a task command did that left its task's completion uncertain.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error, crate::HeapLayout,
)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum CommandIndeterminacyReason {
    /// The command failed after it may have taken effect.
    #[error("failed ({code}): {error}")]
    Failed {
        /// Stable code of the source error.
        code: String,
        /// The source error.
        error: String,
    },
    /// The command itself reported indeterminate progress.
    #[error("reported indeterminate progress")]
    Unsettled,
    /// The command's result exceeds the task input limit.
    #[error("result exceeds the task input limit")]
    InputLimit,
}

impl IndeterminateCause {
    /// The error a caller sees when work ends with this cause, keeping the cause.
    #[must_use]
    pub fn into_error(self, operation_id: OperationId) -> Error {
        match self {
            Self::Unobserved => Error::Indeterminate(operation_id),
            Self::Command(cause) => Error::CommandIndeterminate {
                operation_id,
                cause,
            },
        }
    }
}
