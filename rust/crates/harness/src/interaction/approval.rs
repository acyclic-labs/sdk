use super::*;

/// Exact effect intent that an approval authorizes, independent of display text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalBinding {
    /// Operation to which the decision applies.
    pub operation_id: OperationId,
    /// Digest of the exact approved arguments and action.
    pub action_digest: [u8; 32],
}

impl ApprovalBinding {
    /// Rejects the all-zero sentinel so every approval names an exact intent.
    pub fn validate(&self) -> Result<()> {
        if self.action_digest == [0; 32] {
            return Err(Error::Invalid(
                "approval action digest cannot be zero".into(),
            ));
        }
        Ok(())
    }
}
