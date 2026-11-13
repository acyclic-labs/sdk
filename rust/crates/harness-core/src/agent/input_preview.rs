//! A bounded presentation of an original accepted message, never a second log.

/// One selected message prefix from the original accepted input.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputPreview {
    /// UTF-8 prefix, ending at a complete scalar boundary.
    pub text: String,
    /// Complete original human text length, before display truncation.
    pub bytes: u64,
}

/// Which accepted input the original query selects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputSelection {
    /// A bounded prefix of the latest accepted message, including inherited input.
    Latest,
    /// Every own unsettled message, newest first, in credited UTF-8 ranges.
    Pending,
    /// Every original human prompt in this session, newest first, excluding inherited records.
    History,
}

/// One credited source step, including work that disclosed no message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputPreviewStep {
    /// One source record was consumed without selecting a message.
    Progress,
    /// One contiguous range of an original selected message. The complete
    /// original length is in `preview.bytes`; no file identity consumes credit.
    MessageRange {
        /// Original inbox sequence, retained through settlement.
        inbox: u64,
        /// Byte offset in the original message.
        offset: u64,
        /// Credited UTF-8 range and complete original length.
        preview: InputPreview,
    },
    /// The latest prefix, or completion (also after all pending ranges).
    Selected(Option<InputPreview>),
}
