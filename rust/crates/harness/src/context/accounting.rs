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

/// Verified input admission count and separate planning weights for one request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelTokenCount {
    /// Binds every count to the exact canonical model request.
    pub request_digest: [u8; 32],
    /// Verified total input count, including all provider framing and media.
    #[cfg_attr(feature = "wasm", tsify(type = "bigint"))]
    pub input_tokens: u64,
    /// Planning weight for tools, options and framing; never an admission proof.
    pub fixed_tokens: u32,
    /// Ordered per-message planning weights for compaction selection.
    pub message_tokens: Vec<u32>,
}

impl ModelTokenCount {
    /// Constructs a count from independently proven additive upper bounds.
    /// Remote tokenizers supply `input_tokens` directly instead; planning weights
    /// need not add up to that verified count.
    #[must_use]
    pub fn from_upper_bounds(
        request_digest: [u8; 32],
        fixed_tokens: u32,
        message_tokens: Vec<u32>,
    ) -> Self {
        let input_tokens = message_tokens
            .iter()
            .fold(u64::from(fixed_tokens), |total, n| {
                total.saturating_add(u64::from(*n))
            });
        Self {
            request_digest,
            input_tokens,
            fixed_tokens,
            message_tokens,
        }
    }

    /// Verifies request identity and dimensions, returning the full input bound.
    pub(crate) fn validate(
        &self,
        request: &PreparedModelRequest,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        if self.request_digest != request.request_digest()
            || self.message_tokens.len() != request.request().messages.len()
        {
            return Err(token_count_invalid(
                work,
                "token count does not bind the prepared request",
            ));
        }
        if self.input_tokens > crate::conversation::MAX_EXACT_JS_INTEGER {
            return Err(token_count_invalid(
                work,
                "token count exceeds portable arithmetic",
            ));
        }
        Ok(self.input_tokens)
    }
}

fn token_count_invalid(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::model::{Model, ModelContent, ModelMessage, ModelRequest, ModelRole};

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

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        #[test]
        fn admission_is_independent_of_every_planning_weight(
            text in ".{0,256}", actual in 0u64..1_000_000,
            fixed in proptest::num::u32::ANY, message in proptest::num::u32::ANY,
        ) {
            let request = PreparedModelRequest::prepare(ModelRequest {
                model: Model::new("fixture".into(), "dense".into(), "1".into(), serde_json::json!({})).unwrap(),
                messages: vec![ModelMessage { role: ModelRole::User, content: ModelContent::Text(text) }],
                tools: Vec::new(), max_output_tokens: Some(1),
            }, crate::conversation::Limits::default(), None).unwrap();
            let count = ModelTokenCount { request_digest: request.request_digest(), input_tokens: actual,
                fixed_tokens: fixed, message_tokens: vec![message] };
            proptest::prop_assert_eq!(count.validate(&request, &mut None).unwrap(), actual);
            let bytes = serde_json::to_vec(&count).unwrap();
            let replay: ModelTokenCount = serde_json::from_slice(&bytes).unwrap();
            proptest::prop_assert_eq!(replay.validate(&request, &mut None).unwrap(), actual);
        }
    }

    #[test]
    fn accounting_binds_content_model_and_message_dimensions() -> Result<()> {
        let request = ModelRequest {
            model: Model::new(
                "provider".into(),
                "model".into(),
                "revision".into(),
                serde_json::json!({}),
            )?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("hello".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(1),
        };
        let prepared = PreparedModelRequest::prepare(
            request.clone(),
            crate::conversation::Limits::default(),
            None,
        )?;
        let mut count =
            ModelTokenCount::from_upper_bounds(prepared.request_digest(), u32::MAX, vec![u32::MAX]);
        assert_eq!(
            count.validate(&prepared, &mut None)?,
            2 * u64::from(u32::MAX)
        );
        let mut changed_model = request.clone();
        changed_model.model.revision = "different".into();
        let mut changed_content = request;
        changed_content.messages[0].content = ModelContent::Text("different".into());
        for changed in [changed_model, changed_content] {
            let changed = PreparedModelRequest::prepare(
                changed,
                crate::conversation::Limits::default(),
                None,
            )?;
            assert!(count.validate(&changed, &mut None).is_err());
        }
        count.message_tokens.clear();
        assert!(count.validate(&prepared, &mut None).is_err());
        Ok(())
    }
}
