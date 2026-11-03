//! Per-call facts borrowed from the original dispatch and completion observation.
use super::*;

#[cfg(any(test, feature = "test-support"))]
pub mod codec_fuzz;

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
    pub completed: bool,
    pub dispatch: crate::model::ModelDispatch,
    pub purpose: ModelPurpose,
    pub provider: &'a str,
    pub model: &'a str,
    pub agent_path: Option<&'a str>,
    /// Sampling from the original pinned request.
    pub sampling: crate::model::ModelSampling,
    /// Original requested output ceiling; absent remains unbounded.
    pub max_output_tokens: Option<u32>,
    pub tokens: ModelTokenUsage,
    /// Dispatch through receipt of the terminal event, including original
    /// journal backpressure between chunks. Excludes the terminal commit.
    /// Unknown for recovered calls and platforms without this clock.
    pub latency_ns: Option<u64>,
    /// Original HTTP request identifier, never inferred from the generation id.
    #[cfg(feature = "research")]
    pub provider_request_id: Option<&'a str>,
    /// Provider-reported serving fingerprint; absent if unreported.
    #[cfg(feature = "research")]
    pub system_fingerprint: Option<&'a str>,
    /// Dispatch to first nonempty visible or reasoning delta; absent without one.
    #[cfg(feature = "research")]
    pub ttft_ns: Option<u64>,
    /// Original adapter cache policy and prefix identity, distinct from token hits.
    #[cfg(feature = "research")]
    pub cache: Option<ModelCacheObservation>,
}

/// Durable per-call usage view. The journal's terminal observation is the
/// commit authority; an unreferenced staged file is never a completed call.
#[derive(Clone, Debug, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelUsageRecord {
    /// Record format, currently two; older layouts are not migrated.
    pub version: u32,
    /// A terminal provider event was committed. False preserves an admitted
    /// call whose completion was not observed; its counters stay unknown.
    pub completed: bool,
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
    /// Sampling from the original pinned request.
    #[serde(default)]
    pub sampling: Option<crate::model::ModelSampling>,
    /// Original requested output ceiling; absent remains unbounded.
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    /// Provider-reported counters with missing values preserved.
    pub tokens: ModelTokenUsage,
    /// Original local call interval, as defined by the completion producer.
    pub latency_ns: Option<u64>,
    /// Original HTTP request identifier, never inferred from the generation id.
    #[serde(default)]
    pub provider_request_id: Option<String>,
    /// Provider-reported serving fingerprint; absent if unreported.
    #[serde(default)]
    pub system_fingerprint: Option<String>,
    /// Dispatch to first nonempty visible or reasoning delta; absent without one.
    #[serde(default)]
    pub ttft_ns: Option<u64>,
    /// Original research cache observation; absence does not mean a cache miss.
    #[serde(default)]
    pub cache: Option<ModelCacheObservation>,
}

/// Borrowed identity shared by original-request and original-usage readers.
/// Unknown fields retain their absence; completion measurements are excluded.
#[derive(PartialEq)]
pub(crate) struct ModelUsageAdmission<'a> {
    pub dispatch: crate::model::ModelDispatch,
    pub purpose: ModelPurpose,
    pub provider: &'a str,
    pub model: &'a str,
    pub agent_path: Option<&'a str>,
    pub sampling: Option<crate::model::ModelSampling>,
    pub max_output_tokens: Option<u32>,
}

impl ModelUsageRecord {
    /// The one per-call format accepted by producers, readers and aggregates.
    pub const VERSION: u32 = 2;

    /// Whether both observations retain the same original admitted call.
    /// Completion counters, timing and provider response metadata may change;
    /// the dispatch, agent and pinned request binding may not. Unknown fields
    /// remain distinct from supplied values. Format validation is separate.
    /// Compares a fixed set of fields, borrowing their strings without IO,
    /// allocation, copying, waiting or traversal of other observations.
    #[must_use]
    pub fn same_admission_as(&self, other: &Self) -> bool {
        self.admission() == other.admission()
    }

    pub(crate) fn admission(&self) -> ModelUsageAdmission<'_> {
        ModelUsageAdmission {
            dispatch: self.dispatch,
            purpose: self.purpose,
            provider: &self.provider,
            model: &self.model,
            agent_path: self.agent_path.as_deref(),
            sampling: self.sampling,
            max_output_tokens: self.max_output_tokens,
        }
    }
}

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
            "usage admission failed: no original terminal model event; recovery: select the original committed Completed observation",
        ));
    };
    if record.version != ModelUsageRecord::VERSION
        || !record.completed
        || record.dispatch.step != step
        || record.purpose != purpose
        || record.dispatch.operation_id != purpose.attempt_operation(operation, io)?
        || record.dispatch.request_digest == [0; 32]
        || record.provider.is_empty()
        || record.model.is_empty()
        || record.tokens != ModelTokenUsage::reported(metadata)
        || record
            .cache
            .is_some_and(|cache| Some(cache) != ModelCacheObservation::reported(metadata))
        || record
            .provider_request_id
            .as_deref()
            .is_some_and(|id| Some(id) != metadata["provider_request_id"].as_str())
        || record
            .system_fingerprint
            .as_deref()
            .is_some_and(|id| Some(id) != metadata["system_fingerprint"].as_str())
        || matches!((record.ttft_ns, record.latency_ns), (Some(first), Some(total)) if first > total)
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "usage admission failed: record differs from its original model observation; recovery: select the original completion and its immutable usage artifact",
        ));
    }
    Ok(())
}

/// One bounded page of the original journal's model-call observations.
/// A nonterminal row is replaced only by its exact completed dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelUsagePage {
    /// Admissions and completions in their original commit order.
    pub items: Vec<ModelUsageRecord>,
    /// Exclusive original journal position after every visited observation,
    /// including observations without usage. Resume here after a source event;
    /// an empty or short page does not establish that this turn has settled.
    pub end: u64,
    /// Exclusive journal continuation, including pages with no model calls.
    /// None means this page reached the currently observed tail, not finality.
    pub next: Option<u64>,
}

/// Projects one fixed-size journal page, without scanning prior calls or
/// uncommitted staged artifacts. A caller exports a settled turn by consuming
/// its pages once, retaining only the one currently admitted call. Completed
/// accounting is checked against its original terminal observation; a saved
/// `completed` flag alone cannot certify counters, metadata or finality.
pub async fn usage_page(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    agent_path: &str,
    after: u64,
) -> Result<ModelUsagePage> {
    const PAGE: u32 = 32;
    let records = journal.replay(operation, after, PAGE, None)?.await?;
    let (end, next) = super::event_page::page_continuation(after, records.len(), PAGE)?;
    let mut items = Vec::new();
    for record in records {
        if let Some(usage) = project_record(journal, operation, agent_path, &record).await? {
            items.push(usage);
        }
    }
    Ok(ModelUsagePage { items, end, next })
}

pub(super) async fn project_record(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    agent_path: &str,
    record: &ExecutionRecord,
) -> Result<Option<ModelUsageRecord>> {
    match &record.event {
        ExecutionEvent::ModelStarted {
            step,
            purpose,
            request_digest,
            request,
        } => {
            if record.operation_id != operation || record.sequence == 0 {
                return Err(Error::Storage("usage source admission failed: foreign operation or zero journal sequence; recovery: select the original operation's committed journal record".into()));
            }
            let request = super::transcript::usage_request(journal, request).await?;
            let model = request.model;
            Ok(Some(ModelUsageRecord {
                version: ModelUsageRecord::VERSION,
                completed: false,
                dispatch: crate::model::ModelDispatch {
                    operation_id: purpose.attempt_operation(operation, None)?,
                    step: *step,
                    request_digest: *request_digest,
                },
                purpose: *purpose,
                provider: model.provider,
                model: model.name,
                agent_path: Some(agent_path.to_owned()),
                sampling: Some(request.sampling),
                max_output_tokens: request.max_output_tokens,
                tokens: ModelTokenUsage::default(),
                latency_ns: None,
                provider_request_id: None,
                system_fingerprint: None,
                ttft_ns: None,
                cache: None,
            }))
        }
        ExecutionEvent::Model { usage: Some(_), .. }
        | ExecutionEvent::ModelInline { usage: Some(_), .. } => {
            project_completion(journal, operation, agent_path, record)
                .await
                .map(|(_, usage)| usage)
        }
        _ => Ok(None),
    }
}

/// One original terminal load and one optional immutable usage load. All
/// exporters use this admission before treating accounting as completed.
/// Missing accounting stays unknown; a nonterminal source cannot certify it.
pub(super) async fn project_completion(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    agent_path: &str,
    record: &ExecutionRecord,
) -> Result<(Value, Option<ModelUsageRecord>)> {
    let refuse = |reason: &str| {
        Error::Storage(format!(
            "usage completion admission failed: {reason}; recovery: select the original committed model completion and its immutable usage artifact"
        ))
    };
    let (step, purpose, reference) = match &record.event {
        ExecutionEvent::Model {
            step,
            purpose,
            usage,
            ..
        }
        | ExecutionEvent::ModelInline {
            step,
            purpose,
            usage,
            ..
        } if record.operation_id == operation && record.sequence != 0 => (*step, *purpose, usage),
        _ => return Err(refuse("foreign or absent source identity")),
    };
    let event = super::model_observation::load(journal, &record.event, None)
        .await?
        .ok_or_else(|| refuse("missing original model observation"))?;
    if !matches!(event, ModelEvent::Completed { .. }) {
        return Err(refuse("source is not terminal"));
    }
    let usage = if let Some(reference) = reference {
        // The original journal load already verifies the immutable descriptor.
        let bytes = journal.load(reference, None)?.await?;
        let usage: ModelUsageRecord = crate::contract::json_from_slice(&bytes)
            .map_err(|_| refuse("malformed usage artifact"))?;
        validate(&usage, operation, step, purpose, &event, None)?;
        if usage
            .agent_path
            .as_deref()
            .is_some_and(|agent| agent != agent_path)
        {
            return Err(refuse("foreign agent binding"));
        }
        Some(usage)
    } else {
        None
    };
    match event {
        ModelEvent::Completed { metadata } => Ok((metadata, usage)),
        _ => Err(refuse("source is not terminal")),
    }
}

pub(super) struct CallClock {
    #[cfg(not(target_arch = "wasm32"))]
    started: Option<std::time::Instant>,
    #[cfg(feature = "research")]
    first_token_ns: Option<u64>,
}

impl CallClock {
    pub(super) fn unknown() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            started: None,
            #[cfg(feature = "research")]
            first_token_ns: None,
        }
    }

    pub(super) fn start(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started = Some(std::time::Instant::now());
        }
    }

    #[cfg(feature = "research")]
    pub(super) fn observe(&mut self, event: &ModelEvent) {
        if self.first_token_ns.is_none()
            && matches!(event,
            ModelEvent::Content { delta } | ModelEvent::Reasoning { delta } if !delta.is_empty())
        {
            self.first_token_ns = self.elapsed_ns();
        }
    }

    #[cfg(feature = "research")]
    pub(super) fn ttft_ns(&self) -> Option<u64> {
        self.first_token_ns
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
            cache_write in proptest::option::of(any::<u64>()),
            reasoning in proptest::option::of(any::<u64>()),
        ) {
            // Generated protocol-field mutations, never captured transcripts.
            let dialects = [
                serde_json::json!({"usage":{"input_tokens":input,"output_tokens":output,
                    "cache_read_input_tokens":cached,"cache_creation_input_tokens":cache_write}}),
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
                codec_fuzz::check_metadata(metadata);
                prop_assert_eq!(ModelTokenUsage::reported(metadata), ModelTokenUsage {
                    input: Some(input), output: Some(output), cached,
                    cache_write: if index == 0 { cache_write } else { None },
                    reasoning: if index == 0 { None } else { reasoning },
                });
            }
            let mut contradictory = dialects[2].clone();
            contradictory["usage"]["prompt_tokens"] = serde_json::json!(input ^ 1);
            codec_fuzz::check_metadata(&contradictory);
            prop_assert_eq!(ModelTokenUsage::reported(&contradictory).input, None);
            for invalid in [serde_json::json!(-1), serde_json::json!(1.5), serde_json::json!("0")] {
                contradictory["usage"]["input_tokens"] = invalid;
                codec_fuzz::check_metadata(&contradictory);
                prop_assert_eq!(ModelTokenUsage::reported(&contradictory).input, None);
            }
            prop_assert_eq!(ModelTokenUsage::reported(&Value::Null), ModelTokenUsage::default());
            prop_assert_eq!(ModelTokenUsage::reported(&serde_json::json!({"usage":{"input_tokens":0}})).input,
                Some(0));
        }
    }
}
