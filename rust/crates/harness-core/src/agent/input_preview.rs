//! A bounded presentation of an original accepted message, never a second log.

/// One selected message prefix from the original accepted input.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputPreview {
    /// UTF-8 prefix, ending at a complete scalar boundary.
    pub text: String,
    /// Complete original message length, before display truncation.
    pub bytes: u64,
}

/// One credited source step, including work that disclosed no message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputPreviewStep {
    /// One source record was consumed without selecting a message.
    Progress,
    /// The selected prefix, or proof that the selected cut has no message.
    Selected(Option<InputPreview>),
}
