use super::*;

/// Immutable evidence of one admitted interaction resolution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionReceipt {
    /// Resolved request identity.
    pub id: Uuid,
    /// Exact resolution version admitted by the conversation owner.
    pub version: u64,
    /// Stable caller operation used for retry and reconciliation.
    pub operation_id: OperationId,
    /// Gapless revision of the authoritative conversation event.
    pub conversation_revision: u64,
    /// Whether this call observed the already committed exact operation.
    pub replayed: bool,
    /// Ref-only terminal or explicitly indeterminate outcome.
    pub outcome: InteractionOutcome,
}

impl ResolutionReceipt {
    /// Checks the independent public receipt shape before trusting its owner.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_nil() || self.version == 0 || self.conversation_revision == 0 {
            return Err(Error::Invalid(
                "interaction resolution receipt is invalid".into(),
            ));
        }
        Ok(())
    }

    /// Converts only a committed interaction-resolution event into its receipt.
    pub fn from_apply_result(result: crate::core::ApplyResult) -> Result<Self> {
        let (event, replayed) = match result {
            crate::core::ApplyResult::Applied { event } => (event, false),
            crate::core::ApplyResult::Replayed { event } => (event, true),
        };
        let crate::core::EventPayload::InteractionResolved { resolution } = event.payload else {
            return Err(Error::Conflict(
                "operation did not resolve an interaction".into(),
            ));
        };
        let receipt = Self {
            id: resolution.id,
            version: resolution.expected_version,
            operation_id: event.operation_id,
            conversation_revision: event.revision,
            replayed,
            outcome: resolution.outcome,
        };
        receipt.validate()?;
        Ok(receipt)
    }
}
