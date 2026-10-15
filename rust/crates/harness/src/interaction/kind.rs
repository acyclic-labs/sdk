use super::*;

/// Discriminator retained in the event without copying a participant-facing prompt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    /// Free-form schema-constrained answer.
    Question,
    /// Stable option selection.
    Choice,
    /// Structured schema-constrained form.
    Form,
    /// Approval of one exact operation intent.
    Approval,
}
