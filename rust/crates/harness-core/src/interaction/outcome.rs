use super::*;

/// Closed interaction outcome. Unknown provider outcomes remain explicitly unresolved.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InteractionOutcome {
    /// Schema-valid response content, retained only as a ref.
    Answered {
        /// Immutable response JSON.
        answer: Box<FileRef>,
    },
    /// Exact approval binding was accepted.
    Approved {},
    /// Participant explicitly refused.
    Declined {},
    /// Requester or responder cancelled.
    Cancelled {},
    /// Deadline elapsed before an answer.
    Expired {},
    /// Policy denied the interaction.
    Denied {},
    /// Provider outcome needs reconciliation by stable operation identity.
    Indeterminate {
        /// Operation to reconcile.
        operation_id: OperationId,
    },
}

impl InteractionOutcome {
    /// Unknown outcomes remain open for later reconciliation.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        !matches!(self, Self::Indeterminate { .. })
    }
}
