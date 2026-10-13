use super::*;

/// Durable terminal task outcome.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "keep the public outcome descriptor by value"
)]
pub enum TaskOutcomeRecord {
    /// Schema-checked successful result artifact.
    Succeeded {
        /// Pinned owner-controlled result artifact.
        result: FileRef,
    },
    /// Stable non-secret failure description.
    Failed {
        /// Bounded, non-secret failure description.
        message: String,
    },
    /// Cancellation was acknowledged.
    Cancelled,
    /// Provider must reconcile this exact operation.
    Indeterminate {
        /// Operation requiring owner-mediated reconciliation.
        operation_id: crate::OperationId,
    },
}

impl TaskOutcomeRecord {
    /// Validates bounded metadata and all carried references.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Succeeded { result } => result.validate(&mut None),
            Self::Failed { message } if message.is_empty() => {
                Err(Error::Invalid("task failure description is invalid".into()))
            }
            _ => Ok(()),
        }
    }
}
