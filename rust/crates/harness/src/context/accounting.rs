//! Provider-owned capacities and request-bound token accounting.

use crate::{Error, Result, model::PreparedModelRequest};
use serde::{Deserialize, Serialize};

/// Actual context and output capacities of one immutable selected model.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelContextCapacity {
    /// Maximum tokens in the provider's complete input plus generated output.
    pub context_tokens: u32,
    /// Maximum generated tokens supported by the selected model.
    pub output_tokens: u32,
}

impl ModelContextCapacity {
    /// Rejects impossible provider advertisements.
    pub fn validate(self) -> Result<()> {
        if self.context_tokens == 0
            || self.output_tokens == 0
            || self.output_tokens > self.context_tokens
        {
            return Err(Error::Invalid("selected model capacity is invalid".into()));
        }
        Ok(())
    }
}

/// Provider-owned additive upper bounds for an exact prepared request.
/// Counters must include structured content, native media and provider framing.
/// The SDK supplies no tokenizer or model-name capacity catalog.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelTokenCount {
    /// Binds every count to the exact canonical model request.
    pub request_digest: [u8; 32],
    /// Upper bound for tools, options and request framing independent of messages.
    pub fixed_tokens: u32,
    /// Ordered per-message upper bounds, including message-specific framing.
    /// The provider must make these safe for ordered subsequences of this request.
    pub message_tokens: Vec<u32>,
}

impl ModelTokenCount {
    /// Verifies request identity and dimensions, returning the full input bound.
    pub fn validate(&self, request: &PreparedModelRequest) -> Result<u64> {
        if self.request_digest != request.manifest().request_digest
            || self.message_tokens.len() != request.request().messages.len()
        {
            return Err(Error::Invalid(
                "token count does not bind the prepared request".into(),
            ));
        }
        self.message_tokens
            .iter()
            .try_fold(u64::from(self.fixed_tokens), |total, count| {
                total
                    .checked_add(u64::from(*count))
                    .ok_or_else(|| Error::Invalid("token count exceeds portable arithmetic".into()))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Model, ModelContent, ModelContentPart, ModelMessage, ModelRequest, ModelRole,
    };

    #[test]
    fn capacity_rejects_zero_and_inverted_bounds() {
        for (context_tokens, output_tokens) in [(0, 1), (1, 0), (1, 2)] {
            assert!(
                ModelContextCapacity {
                    context_tokens,
                    output_tokens
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            ModelContextCapacity {
                context_tokens: 8,
                output_tokens: 8
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn accounting_binds_content_model_and_message_dimensions() -> Result<()> {
        let request = ModelRequest {
            model: Model::new("provider", "model", "revision", serde_json::json!({}))?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("hello".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(1),
        };
        let prepared =
            PreparedModelRequest::prepare(request.clone(), crate::conversation::Limits::default())?;
        let mut count = ModelTokenCount {
            request_digest: prepared.manifest().request_digest,
            fixed_tokens: u32::MAX,
            message_tokens: vec![u32::MAX],
        };
        assert_eq!(count.validate(&prepared)?, 2 * u64::from(u32::MAX));
        let mut changed = request.clone();
        changed.model.revision = "different".into();
        assert!(
            count
                .validate(&PreparedModelRequest::prepare(
                    changed,
                    crate::conversation::Limits::default()
                )?)
                .is_err()
        );
        let mut changed = request;
        changed.messages[0].content = ModelContent::Text("different".into());
        assert!(
            count
                .validate(&PreparedModelRequest::prepare(
                    changed,
                    crate::conversation::Limits::default()
                )?)
                .is_err()
        );
        count.message_tokens.clear();
        assert!(count.validate(&prepared).is_err());
        Ok(())
    }

    #[test]
    fn structured_parts_borrow_original_storage() {
        let part = ModelContentPart::Text {
            text: "content".into(),
        };
        let single = ModelContent::Part(part.clone());
        let ModelContent::Part(original) = &single else {
            unreachable!()
        };
        assert!(std::ptr::eq(&single.parts()[0], original));
        let multiple = ModelContent::Parts(vec![part]);
        let ModelContent::Parts(original) = &multiple else {
            unreachable!()
        };
        assert_eq!(multiple.parts().as_ptr(), original.as_ptr());
        assert!(ModelContent::Text("plain".into()).parts().is_empty());
    }
}
