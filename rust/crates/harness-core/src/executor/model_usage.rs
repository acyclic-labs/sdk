//! Per-call facts borrowed from the original dispatch and completion observation.
use super::*;

/// Provider-reported token counters. Missing, malformed or contradictory values
/// remain unknown. Input and output retain the provider's inclusive/exclusive
/// conventions; cached and reasoning tokens are never added a second time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelTokenUsage {
    /// Original reported input tokens, before any billing interpretation.
    pub input: Option<u64>,
    /// Original reported cached input tokens.
    pub cached: Option<u64>,
    /// Original reported cache-creation tokens.
    pub cache_write: Option<u64>,
    /// Original reported output tokens.
    pub output: Option<u64>,
    /// Original reported reasoning tokens, without adding them to output.
    pub reasoning: Option<u64>,
}

impl ModelTokenUsage {
    pub(crate) fn reported(metadata: &Value) -> Self {
        let usage = &metadata["usage"];
        // Every lookup names a fixed provider field. No response/history scan,
        // aggregation, estimate, or default-zero conversion is performed.
        let counter = |values: &[&Value]| {
            let mut result = None;
            for value in values.iter().filter(|value| !value.is_null()) {
                let observed = value.as_u64()?;
                if result.is_some_and(|prior| prior != observed) {
                    return None;
                }
                result = Some(observed);
            }
            result
        };
        Self {
            input: counter(&[
                &usage["input_tokens"],
                &usage["prompt_tokens"],
                &usage["promptTokenCount"],
            ]),
            cached: counter(&[
                &usage["cache_read_input_tokens"],
                &usage["input_tokens_details"]["cached_tokens"],
                &usage["prompt_tokens_details"]["cached_tokens"],
                &usage["cachedContentTokenCount"],
            ]),
            cache_write: counter(&[&usage["cache_creation_input_tokens"]]),
            output: counter(&[
                &usage["output_tokens"],
                &usage["completion_tokens"],
                &usage["candidatesTokenCount"],
            ]),
            reasoning: counter(&[
                &usage["output_tokens_details"]["reasoning_tokens"],
                &usage["completion_tokens_details"]["reasoning_tokens"],
                &usage["thoughtsTokenCount"],
            ]),
        }
    }
}

/// One JSON value in a private per-call JSONL artifact. Strings are borrowed
/// until the original journal stages its bytes; no provider payload is copied.
#[derive(Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelCallUsage<'a> {
    pub version: u32,
    pub dispatch: crate::model::ModelDispatch,
    pub purpose: ModelPurpose,
    pub provider: &'a str,
    pub model: &'a str,
    pub agent_path: Option<&'a str>,
    pub tokens: ModelTokenUsage,
    /// Dispatch through receipt of the terminal event, including original
    /// journal backpressure between chunks. Excludes the terminal commit.
    /// Unknown for recovered calls and platforms without this clock.
    pub latency_ns: Option<u64>,
}

/// Durable per-call usage view. The journal's terminal observation is the
/// commit authority; an unreferenced staged file is never a completed call.
#[derive(Debug, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelUsageRecord {
    /// Record format, currently one; older layouts are not migrated.
    pub version: u32,
    /// Original call identity, separate from provider-controlled metadata.
    pub dispatch: crate::model::ModelDispatch,
    /// Response or context-summary call.
    pub purpose: ModelPurpose,
    /// Admitted provider binding, rather than an untrusted response label.
    pub provider: String,
    /// Admitted model binding.
    pub model: String,
    /// Original agent path; unknown for an executor without an agent binding.
    pub agent_path: Option<String>,
    /// Provider-reported counters with missing values preserved.
    pub tokens: ModelTokenUsage,
    /// Original local call interval, as defined by the completion producer.
    pub latency_ns: Option<u64>,
}

#[cfg(feature = "filesystem")]
pub(crate) fn validate(
    record: &ModelUsageRecord,
    operation: OperationId,
    step: u32,
    purpose: ModelPurpose,
    event: &ModelEvent,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let ModelEvent::Completed { metadata } = event else {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "usage record has no original terminal model event",
        ));
    };
    if record.version != 1
        || record.dispatch.step != step
        || record.purpose != purpose
        || record.dispatch.operation_id != purpose.attempt_operation(operation, io)?
        || record.dispatch.request_digest == [0; 32]
        || record.provider.is_empty()
        || record.model.is_empty()
        || record.tokens != ModelTokenUsage::reported(metadata)
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "usage record differs from its original model observation",
        ));
    }
    Ok(())
}

pub(super) struct CallClock {
    #[cfg(not(target_arch = "wasm32"))]
    started: Option<std::time::Instant>,
}

impl CallClock {
    pub(super) fn unknown() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            started: None,
        }
    }

    pub(super) fn start(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started = Some(std::time::Instant::now());
        }
    }

    pub(super) fn elapsed_ns(&self) -> Option<u64> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started
                .and_then(|start| u64::try_from(start.elapsed().as_nanos()).ok())
        }
        #[cfg(target_arch = "wasm32")]
        {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn usage_dialects_preserve_units_absence_and_malformed_controls(
            input in any::<u64>(), output in any::<u64>(),
            cached in proptest::option::of(any::<u64>()),
            reasoning in proptest::option::of(any::<u64>()),
        ) {
            // Generated protocol-field mutations, never captured transcripts.
            let dialects = [
                serde_json::json!({"usage":{"input_tokens":input,"output_tokens":output,
                    "cache_read_input_tokens":cached}}),
                serde_json::json!({"usage":{"prompt_tokens":input,"completion_tokens":output,
                    "prompt_tokens_details":{"cached_tokens":cached},
                    "completion_tokens_details":{"reasoning_tokens":reasoning}}}),
                serde_json::json!({"usage":{"input_tokens":input,"output_tokens":output,
                    "input_tokens_details":{"cached_tokens":cached},
                    "output_tokens_details":{"reasoning_tokens":reasoning}}}),
                serde_json::json!({"usage":{"promptTokenCount":input,"candidatesTokenCount":output,
                    "cachedContentTokenCount":cached,"thoughtsTokenCount":reasoning}}),
            ];
            for (index, metadata) in dialects.iter().enumerate() {
                prop_assert_eq!(ModelTokenUsage::reported(metadata), ModelTokenUsage {
                    input: Some(input), output: Some(output), cached,
                    cache_write: None, reasoning: if index == 0 { None } else { reasoning },
                });
            }
            let mut contradictory = dialects[2].clone();
            contradictory["usage"]["prompt_tokens"] = serde_json::json!(input ^ 1);
            prop_assert_eq!(ModelTokenUsage::reported(&contradictory).input, None);
            for invalid in [serde_json::json!(-1), serde_json::json!(1.5), serde_json::json!("0")] {
                contradictory["usage"]["input_tokens"] = invalid;
                prop_assert_eq!(ModelTokenUsage::reported(&contradictory).input, None);
            }
            prop_assert_eq!(ModelTokenUsage::reported(&Value::Null), ModelTokenUsage::default());
            prop_assert_eq!(ModelTokenUsage::reported(&serde_json::json!({"usage":{"input_tokens":0}})).input,
                Some(0));
        }
    }
}
