//! Canonical provider reasoning, matching the current SDK reasoning contract.

use serde::{Deserialize, Serialize};

/// Provider-neutral reasoning retained in the canonical transcript.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
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
    pub(crate) fn validate_limits(&self, limits: crate::conversation::Limits) -> crate::Result<()> {
        limits.validate()?;
        if self.continuity.len() as u64 > limits.render_bytes
            || crate::contract::canonical_json_bytes(self)?.len() as u64 > limits.render_bytes
        {
            return Err(crate::Error::Invalid(
                "model reasoning exceeds render limit".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelContent, ModelContentPart, ModelEvent};

    #[test]
    fn opaque_reasoning_round_trips_and_obeys_render_limits() -> crate::Result<()> {
        let block = ReasoningBlock {
            kind: ReasoningKind::Encrypted,
            text: None,
            origin: ReasoningOrigin {
                provider: "openai-responses".into(),
                model: "gpt-6.1-sol".into(),
                credential_scope: Some([11; 32]),
            },
            continuity: vec![0, 1, 127, 128, 255],
            outcome: ReasoningOutcome::PreservedOpaque,
        };
        let event = ModelEvent::ReasoningBlock {
            block: block.clone(),
        };
        let bytes = crate::contract::canonical_json_bytes(&event)?;
        assert_eq!(
            serde_json::from_slice::<ModelEvent>(&bytes)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
            event
        );
        let content = ModelContent::Part(ModelContentPart::Reasoning { block });
        content.validate_limits(crate::conversation::Limits::default())?;
        assert!(
            content
                .validate_limits(crate::conversation::Limits {
                    render_bytes: 4,
                    ..crate::conversation::Limits::default()
                })
                .is_err()
        );
        assert!(content.validate_user_input().is_err());
        Ok(())
    }
}
