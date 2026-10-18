use super::{Error, OperationId, Result};
use serde::{Deserialize, Serialize};

/// Why the completion of admitted work is uncertain.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum IndeterminateCause {
    /// Completion was not observed; only reconciliation can settle it.
    #[default]
    Unobserved,
    /// A task command left the work uncertain.
    Command(CommandIndeterminacy),
}

/// A task command that left its task's completion uncertain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[error("task command {command} {reason}")]
pub struct CommandIndeterminacy {
    /// Kind of the command, such as `acyclic.model.v1`.
    pub command: String,
    /// What the command did.
    pub reason: CommandIndeterminacyReason,
}

/// What a task command did that left its task's completion uncertain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
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
    pub(crate) fn is_unobserved(&self) -> bool {
        matches!(self, Self::Unobserved)
    }

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

    /// Copies the cause, routing every owned string through `text`.
    pub(crate) fn try_copy(&self, mut text: impl FnMut(&str) -> Result<String>) -> Result<Self> {
        let Self::Command(cause) = self else {
            return Ok(Self::Unobserved);
        };
        Ok(Self::Command(CommandIndeterminacy {
            command: text(&cause.command)?,
            reason: match &cause.reason {
                CommandIndeterminacyReason::Failed { code, error } => {
                    CommandIndeterminacyReason::Failed {
                        code: text(code)?,
                        error: text(error)?,
                    }
                }
                CommandIndeterminacyReason::Unsettled => CommandIndeterminacyReason::Unsettled,
                CommandIndeterminacyReason::InputLimit => CommandIndeterminacyReason::InputLimit,
            },
        }))
    }

    /// Bytes the cause's strings own.
    pub(crate) fn owned_bytes(&self) -> u64 {
        let Self::Command(cause) = self else {
            return 0;
        };
        let reason = match &cause.reason {
            CommandIndeterminacyReason::Failed { code, error } => {
                code.capacity() + error.capacity()
            }
            CommandIndeterminacyReason::Unsettled | CommandIndeterminacyReason::InputLimit => 0,
        };
        (cause.command.capacity() + reason) as u64
    }
}
