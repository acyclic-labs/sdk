use super::*;

/// Typed response to one open interaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InteractionResponse {
    /// Free-form question answer.
    Question {
        /// Schema-validated answer.
        value: Value,
    },
    /// Stable selected option identities.
    Choice {
        /// Selected option identities.
        option_ids: Vec<String>,
    },
    /// Structured form submission.
    Form {
        /// Schema-validated object.
        value: Value,
    },
    /// Approval decision.
    Approval {
        /// Whether the exact bound action is approved.
        approved: bool,
        /// Optional participant explanation.
        reason: Option<String>,
    },
}
