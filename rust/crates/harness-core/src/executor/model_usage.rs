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
                &usage["cached_input_tokens"],
                &usage["input_tokens_details"]["cached_tokens"],
                &usage["prompt_tokens_details"]["cached_tokens"],
                &usage["cachedContentTokenCount"],
            ]),
            cache_write: counter(&[
                &usage["cache_creation_input_tokens"],
                &usage["input_tokens_details"]["cache_write_tokens"],
                &usage["prompt_tokens_details"]["cache_write_tokens"],
                &usage["cache_write_input_tokens"],
            ]),
            output: counter(&[
                &usage["output_tokens"],
                &usage["completion_tokens"],
                &usage["candidatesTokenCount"],
            ]),
            reasoning: counter(&[
                &usage["reasoning_output_tokens"],
                &usage["output_tokens_details"]["reasoning_tokens"],
                &usage["output_tokens_details"]["thinking_tokens"],
                &usage["completion_tokens_details"]["reasoning_tokens"],
                &usage["thoughtsTokenCount"],
            ]),
        }
    }
}

/// Output of the transport attempts that one model step abandoned. Provider
/// counters describe only the attempt that completed, so an abandoned attempt
/// is known by exactly what it streamed before its restart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelAbandonedOutput {
    /// Attempts restarted before the completed one.
    pub attempts: u64,
    /// Visible text delta bytes those attempts streamed.
    pub content_bytes: u64,
    /// Reasoning delta bytes those attempts streamed.
    pub reasoning_bytes: u64,
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
    pub tokens: ModelTokenUsage,
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
    /// Dispatch through receipt of the terminal event, including original
    /// journal backpressure between chunks. Excludes the terminal commit.
    /// Unknown for recovered calls and platforms without this clock.
    pub latency_ns: Option<u64>,
    /// Original HTTP request identifier, never inferred from the generation id.
    pub provider_request_id: Option<&'a str>,
    /// Provider-reported serving fingerprint; absent if unreported.
    #[cfg(feature = "research")]
    pub system_fingerprint: Option<&'a str>,
    /// Dispatch to first nonempty visible or reasoning delta; absent without one.
    pub ttft_ns: Option<u64>,
    /// Original adapter cache policy and prefix identity, distinct from token hits.
    #[cfg(feature = "research")]
    pub cache: Option<ModelCacheObservation>,
    /// Output of every attempt this step abandoned before completing.
    pub abandoned: ModelAbandonedOutput,
}

/// Durable per-call usage view. The journal's terminal observation is the
/// commit authority; an unreferenced staged file is never a completed call.
#[derive(Clone, Debug, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelUsageRecord {
    /// Record format, currently five; older layouts are not migrated.
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
    /// Canonical AgentPath: a child appends `/children/<id>` to its parent.
    /// Unknown for an executor without an agent binding.
    pub agent_path: Option<String>,
    /// Sampling from the original pinned request.
    #[serde(default)]
    pub sampling: Option<crate::model::ModelSampling>,
    /// Provider-reported counters with missing values preserved.
    pub tokens: ModelTokenUsage,
    /// Wall time at live provider dispatch, in Unix nanoseconds, with the
    /// shared wall clock's millisecond resolution. Unknown after recovery or
    /// when that observation cannot be represented as u64 nanoseconds.
    pub started_at: Option<u64>,
    /// Wall time when the terminal provider event arrived, before its commit.
    /// Same units and resolution as started_at. Clock corrections can put this
    /// before started_at; latency_ns uses an independent monotonic clock.
    pub completed_at: Option<u64>,
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
    /// Output of every attempt abandoned before the completed one, beside the
    /// completed attempt's provider counters. Unknown until completion.
    pub abandoned: Option<ModelAbandonedOutput>,
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
}

impl crate::filesystem::PreparationRecord for ModelUsageRecord {}

impl ModelUsageRecord {
    /// The one per-call format accepted by producers, readers and aggregates.
    pub const VERSION: u32 = 5;

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
    validate_metadata(record, operation, step, purpose, metadata, io)
}

/// The stock collector already owns the original Completed metadata. Keep its
/// admission on the same validation path without cloning that JSON value.
pub(super) fn validate_metadata(
    record: &ModelUsageRecord,
    operation: OperationId,
    step: u32,
    purpose: ModelPurpose,
    metadata: &Value,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if record.version != ModelUsageRecord::VERSION
        || !record.completed
        || record.abandoned.is_none()
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

/// Proven input occupancy bound of the original prepared context, independent of
/// provider-reported billing counters. Unknown accounting is never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ContextUsage {
    /// The original projection carries no verified accounting.
    Unavailable,
    /// Original request-bound count or upper bound and the verified capacity.
    Counted {
        /// Complete input count or proven upper bound, including framing and media.
        input_tokens: u64,
        /// Complete input plus output capacity.
        capacity_tokens: u32,
    },
}

mod context_usage;

/// One bounded page of the original journal's model-call observations.
/// A nonterminal row is replaced only by its exact completed dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ModelUsagePage {
    /// Admissions and completions in their original commit order.
    pub items: Vec<ModelUsageRecord>,
    /// Compact tool observations from exactly this same source page.
    pub tools: ToolUsage,
    /// Original wait facts from exactly this same bounded source page.
    pub waits: super::WaitUsage,
    /// Last context preparation in this page; absence preserves the prior cut.
    pub context: Option<ContextUsage>,
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
    let mut tools = ToolUsage::default();
    let mut waits = super::WaitUsage::default();
    let mut context = None;
    for record in records {
        tools.observe(journal, operation, &record).await?;
        waits.observe(operation, &record)?;
        if let ExecutionEvent::ContextPrepared { projection, .. }
        | ExecutionEvent::ContextCompacted { projection, .. } = &record.event
        {
            context = Some(context_usage::read(journal, projection).await?);
        }
        if let ExecutionEvent::ModelStarted {
            purpose: ModelPurpose::Response,
            context: window,
            ..
        } = &record.event
        {
            context = Some(match window {
                Some(window) => {
                    window.validate()?;
                    ContextUsage::Counted {
                        input_tokens: window.input_tokens,
                        capacity_tokens: window.capacity_tokens,
                    }
                }
                None => ContextUsage::Unavailable,
            });
        }
        if let Some(usage) = project_record(journal, operation, agent_path, &record).await? {
            items.push(usage);
        }
    }
    Ok(ModelUsagePage {
        items,
        tools,
        waits,
        context,
        end,
        next,
    })
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
            ..
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
                tokens: ModelTokenUsage::default(),
                started_at: None,
                completed_at: None,
                latency_ns: None,
                provider_request_id: None,
                system_fingerprint: None,
                ttft_ns: None,
                cache: None,
                abandoned: None,
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
    started_millis: Option<u64>,
    #[cfg(not(target_arch = "wasm32"))]
    started: Option<tokio::time::Instant>,
    #[cfg(target_arch = "wasm32")]
    started: Option<std::time::Duration>,
    first_token_seen: bool,
    first_token_ns: Option<u64>,
}

impl CallClock {
    pub(super) fn unknown() -> Self {
        Self {
            started_millis: None,
            started: None,
            first_token_seen: false,
            first_token_ns: None,
        }
    }

    pub(super) fn start(&mut self) {
        self.started_millis = Some(Self::wall_millis());
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started = Some(tokio::time::Instant::now());
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.started = crate::http::browser::monotonic_now();
        }
    }

    pub(super) fn started_at(&self) -> Option<u64> {
        self.started_millis
            .and_then(|millis| millis.checked_mul(1_000_000))
    }

    pub(super) fn completed_at(&self) -> Option<u64> {
        // A reconciled call must not acquire the recovery process's time.
        self.started_millis
            .and_then(|_| Self::wall_millis().checked_mul(1_000_000))
    }

    fn wall_millis() -> u64 {
        acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock)
    }

    pub(super) fn observe(&mut self, event: &ModelEvent) {
        if !self.first_token_seen
            && matches!(event,
            ModelEvent::Content { delta } | ModelEvent::Reasoning { delta } if !delta.is_empty())
        {
            self.first_token_seen = true;
            self.first_token_ns = self.elapsed_ns();
        }
    }

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
            self.started
                .zip(crate::http::browser::monotonic_now())
                .and_then(|(started, now)| now.checked_sub(started))
                .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
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
            written in proptest::option::of(any::<u64>()),
            reasoning in proptest::option::of(any::<u64>()),
        ) {
            codec_fuzz::check(include_bytes!(
                "../../../../codec-fuzz/corpus/harness_codecs/usage-llamacpp-text-literal-extension"
            ));
            // Generated protocol-field mutations, never captured transcripts.
            let dialects = [
                serde_json::json!({"usage":{"input_tokens":input,"output_tokens":output,
                    "cache_read_input_tokens":cached,"cache_creation_input_tokens":written}}),
                serde_json::json!({"usage":{"prompt_tokens":input,"completion_tokens":output,
                    "prompt_tokens_details":{"cached_tokens":cached,"cache_write_tokens":written},
                    "completion_tokens_details":{"reasoning_tokens":reasoning}}}),
                serde_json::json!({"usage":{"input_tokens":input,"output_tokens":output,
                    "input_tokens_details":{"cached_tokens":cached},
                    "output_tokens_details":{"reasoning_tokens":reasoning}}}),
                serde_json::json!({"usage":{"promptTokenCount":input,"candidatesTokenCount":output,
                    "cachedContentTokenCount":cached,"thoughtsTokenCount":reasoning}}),
                serde_json::json!({"usage":{"input_tokens":input,"output_tokens":output,
                    "cached_input_tokens":cached,"cache_write_input_tokens":written,
                    "reasoning_output_tokens":reasoning}}),
                serde_json::json!({"usage":{"input_tokens":input,"output_tokens":output,
                    "cache_read_input_tokens":cached,"cache_creation_input_tokens":written,
                    "output_tokens_details":{"thinking_tokens":reasoning}}}),
            ];
            for (index, metadata) in dialects.iter().enumerate() {
                codec_fuzz::check_metadata(metadata);
                for marker in ["$serde_json::private::RawValue", "$serde_json::private::Number"] {
                    let mut extended = metadata.clone();
                    extended["extra"] = serde_json::json!({(marker): "null", "nested": []});
                    let mut bytes = serde_json::to_vec(&extended).unwrap();
                    let event = serde_json::json!({"kind": "completed", "metadata": extended});
                    let restored: ModelEvent =
                        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap();
                    prop_assert_eq!(restored, ModelEvent::Completed { metadata: extended });
                    codec_fuzz::check(&bytes);
                    bytes.extend_from_slice(b" null");
                    prop_assert!(codec_fuzz::check_metadata_bytes(&bytes).is_err());
                    codec_fuzz::check(&bytes);
                }
                prop_assert_eq!(ModelTokenUsage::reported(metadata), ModelTokenUsage {
                    input: Some(input), output: Some(output), cached,
                    cache_write: if matches!(index, 0 | 1 | 4 | 5) { written } else { None },
                    reasoning: if index == 0 { None } else { reasoning },
                });
            }
            // Native aliases participate in the same malformed/conflicting
            // consensus as the original HTTP names, without alias precedence.
            for (pointer, canonical) in [
                ("/usage/cached_input_tokens", "cache_read_input_tokens"),
                ("/usage/cache_write_input_tokens", "cache_creation_input_tokens"),
                ("/usage/reasoning_output_tokens", "thoughtsTokenCount"),
                ("/usage/output_tokens_details/thinking_tokens", "thoughtsTokenCount"),
            ] {
                let mut metadata = serde_json::json!({"usage": {"output_tokens_details": {}}});
                let (parent, leaf) = pointer.rsplit_once('/').unwrap();
                metadata.pointer_mut(parent).unwrap()[leaf] = serde_json::json!(input);
                metadata["usage"][canonical] = serde_json::json!(input ^ 1);
                prop_assert_eq!(ModelTokenUsage::reported(&metadata), ModelTokenUsage::default());
                for invalid in [serde_json::json!(-1), serde_json::json!(1.5), serde_json::json!("0")] {
                    *metadata.pointer_mut(pointer).unwrap() = invalid;
                    prop_assert_eq!(ModelTokenUsage::reported(&metadata), ModelTokenUsage::default());
                }
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
