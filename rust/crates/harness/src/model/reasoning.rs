use serde::{Deserialize, Serialize};

/// Provider-neutral reasoning retained in the canonical transcript.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ReasoningBlock {
    /// Meaning of the retained content, independent of its wire representation.
    pub kind: ReasoningKind,
    /// Readable content verified by the origin adapter, when available.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub text: Option<String>,
    /// Provider family, model and credential ownership of the native payload.
    pub origin: ReasoningOrigin,
    /// Exact native continuation bytes; projection never edits this payload.
    pub continuity: Vec<u8>,
    /// What the origin adapter could preserve.
    pub outcome: ReasoningOutcome,
}

/// Origin identity used when choosing native replay or foreign context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ReasoningOrigin {
    /// Protocol family, rather than a display name or registry alias.
    pub provider: String,
    /// Exact selected model that produced this block.
    pub model: String,
    /// Nonsecret scope digest; None means ownership is unknown.
    #[cfg_attr(feature = "wasm", tsify(type = "number[] | null"))]
    pub credential_scope: Option<[u8; 32]>,
}

/// Reasoning shapes normalized by the adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ReasoningKind {
    /// Plain reasoning text.
    Readable,
    /// A provider's readable summary.
    Summary,
    /// Encrypted continuation state.
    Encrypted,
    /// Redacted continuation state.
    Redacted,
    /// An opaque signature, including one attached to a tool call.
    Signature,
    /// Another native continuation shape.
    Opaque,
}

/// Preservation result recorded without changing the original transcript block.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ReasoningOutcome {
    /// Readable content was preserved.
    PreservedReadable,
    /// Native opaque continuity was preserved.
    PreservedOpaque,
    /// Neither readable nor native content was available.
    MissingContent,
    /// Display was intentionally suppressed while continuity was retained.
    SuppressedDisplay,
    /// The target cannot accept this native shape.
    UnsupportedShape,
}

impl ReasoningBlock {
    pub(crate) fn validate_limits(
        &self,
        limits: crate::conversation::Limits,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> crate::Result<()> {
        super::preparation::history_step(work, self.continuity.len() as u64)?;
        crate::executor::json::json_byte_length(self, limits.render_bytes, work).map(|_| ())
    }

    pub(crate) fn payload_bytes(&self) -> crate::Result<u64> {
        [
            self.origin.provider.capacity(),
            self.origin.model.capacity(),
            self.continuity.capacity(),
            self.text.as_ref().map_or(0, String::capacity),
        ]
        .into_iter()
        .try_fold(0u64, |bytes, capacity| {
            bytes
                .checked_add(capacity as u64)
                .ok_or_else(super::inventory::overflow)
        })
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for super::ModelEvent {
    fn diagnostic_label_bytes() -> u64 {
        super::preparation::MODEL_DIAGNOSTIC_LABEL_BYTES
    }
    fn logical_owned_bytes(&self) -> crate::Result<u64> {
        match self {
            Self::ReasoningBlock { block } => block.payload_bytes(),
            _ => Err(crate::Error::Invalid(
                "expected canonical reasoning artifact".into(),
            )),
        }
    }
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::ModelEvent
    }
}
