//! Fully replaceable turn execution and the stock streaming model/tool loop.

use crate::contract::capability;
use crate::obs::{obs_span, traced};
use crate::{
    Error, InteractionId, OperationId, Result, TaskId,
    context::{ContextInput, ContextPipeline},
    conversation::{Attachment, FileRef, Limits, VolumeClass},
    interaction::{Interaction, InteractionOutcome},
    model::{
        Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelProvider,
        ModelRequest, ModelRole,
    },
    projection::SelectedModelContext,
    registry::ComponentIdentity,
    runtime::{
        RuntimeScope, ToolPolicy, ToolPolicyDecision, check_tool_approval, tool_approval_request,
        validate_policy_identity,
    },
    tool::{ToolInvocation, ToolRegistry, ToolResult},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

mod native_stock;
use native_stock::*;
pub(crate) use native_stock::{
    stage_json, stock_boundary, stock_into_iter, stock_push_model_event, stock_retire_text,
    stock_string_copy, stock_string_push, stock_vec_prepare_push, stock_vec_push, stock_vec_reserve,
};

pub(crate) mod execution_claim;
mod journal;
pub use execution_claim::{ExecutionAttempt, ExecutionClaim, ExecutionClaims};
#[cfg(feature = "research")]
pub use journal::SequencedExecutionJournal;
use journal::validate_journal;
pub use journal::{
    ExecutionJournal, ExecutionJournalCacheId, ExecutionRecordQuery, IndexedExecutionFuture,
    IndexedExecutionRecords, ProgressPublication,
};
mod replay;
pub(crate) use replay::replay_execution;
mod accounting;
use accounting::*;
mod binding;
mod compaction;
mod composition;
mod configuration;
mod content;
mod context_files;
mod dispatch;
mod manager;
pub(crate) use tool_changes::decode_tool_table;
mod event_page;
mod tool_changes;
pub use event_page::{
    EXECUTION_EVENT_PAGE_RECORDS, ExecutionActivity, ExecutionEventPage, ExecutionPayloadField,
    event_page,
};
mod model_cache;
mod model_cost;
pub use model_cache::{
    ModelCacheAccess, ModelCacheBreakpoint, ModelCacheError, ModelCacheKey, ModelCacheObservation,
    ModelCacheResidency, ModelCacheTtl,
};
mod model_observation;
pub use model_cost::{
    ModelCostEstimate, ModelPriceError, ModelPriceRecord, ModelPrices, ModelUsageProjection,
    ModelUsageTotals, TokenPrice, UsageTotal,
};
#[cfg(feature = "metrics-export")]
pub mod metrics;
pub(crate) mod model_usage;
#[cfg(feature = "test-support")]
pub use model_usage::codec_fuzz as model_usage_codec_fuzz;
/// Original research read receipts and bounded artifact source.
#[cfg(feature = "research")]
pub mod read_set;
pub(crate) use model_usage::validate as validate_model_usage;
pub use model_usage::{
    ContextUsage, ModelAbandonedOutput, ModelTokenUsage, ModelUsagePage, ModelUsageRecord,
    usage_page,
};
mod tool_usage;
pub use tool_usage::ToolUsage;
mod wait_usage;
pub use wait_usage::WaitUsage;
pub(crate) mod phase;
#[cfg(test)]
pub(crate) use model_observation::append as append_model_event;
pub use model_observation::decode_payload as decode_model_payload;
mod prefix;
mod record_index;
pub(crate) use record_index::call_bucket_key;
mod records;
mod request;
mod response;
mod retained_request;
pub(crate) mod transcript;
mod working_tail;
pub(crate) use transcript::load_projection as load_response_projection;
#[cfg(not(target_arch = "wasm32"))]
mod code_mode;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use code_mode::MEMORY_BYTES as ENGINE_MEMORY;
/// Exercises the original JavaScript interpreter and native allocation owner.
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub use code_mode::model::check as check_code_mode;
mod summary;
pub(crate) mod summary_identity;
mod tool_cards;
pub use tool_cards::ToolCard;
mod tool_execution;
pub use tool_execution::ToolCallSettlement;
mod tool_failure;
pub(crate) use tool_execution::tool_failure_content;
pub(crate) use tool_failure::ToolFailureText;
pub use tool_failure::{ToolErrorClass, ToolFailureCause};
#[cfg(not(target_arch = "wasm32"))]
mod agent_hooks;
#[cfg(not(target_arch = "wasm32"))]
mod tool_hooks;

/// Restores the defect where a resumed driver settled nothing it could not
/// reconcile: an interrupted effect-free model call and an unreconcilable
/// tool effect both left the turn unsettled.
#[doc(hidden)]
pub mod unsettled_resume {
    #[cfg(any(test, feature = "test-support"))]
    thread_local! {
        static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn set(enabled: bool) {
        ENABLED.set(enabled);
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn enabled() -> bool {
        ENABLED.get()
    }
    #[cfg(not(any(test, feature = "test-support")))]
    pub(crate) const fn enabled() -> bool {
        false
    }
}
mod tool_resolution;
mod tool_settlement;
pub(crate) use content::verify_model_contents_scoped;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub use content::verify_model_contents_with_summary_authorization;
pub(crate) use tool_settlement::tool_settlement_key;
pub use tool_settlement::{ToolSettlement, ToolSettlementQuery, ToolTerminalOutcome};
mod inference;
mod retained;
mod turn;
pub use inference::InferenceResult;
pub(crate) use phase::DispatchPhase;
pub use phase::PreDispatchFailure;
pub(crate) use retained::{completed_tool_projection, retained_model_step};
mod checkpoint_selection;
pub use checkpoint_selection::canonical_checkpoint_for_operation;
use checkpoint_selection::*;
mod checkpoint_projection;
pub use checkpoint_projection::load_canonical_checkpoint;
mod payload;
#[doc(hidden)]
pub use payload::decode_json;
pub use payload::load_model_request;
#[doc(hidden)]
pub use payload::preparation_digest;
pub(crate) use payload::{load_json, retain_preparation};
mod admission;

/// Internal replay batch size; callers may request any positive page allowance.
pub(crate) const EXECUTION_REPLAY_PAGE_RECORDS: u32 = 64;

pub(crate) fn validate_execution_page(maximum: u32) -> Result<()> {
    if maximum == 0 {
        return Err(Error::Invalid(
            "execution replay page bound is invalid".into(),
        ));
    }
    Ok(())
}

/// Durable input to any custom executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct TurnInput {
    /// Stable durable turn execution identity.
    pub operation_id: OperationId,
    /// Typed provider-neutral input; canonical conversation storage still uses file refs.
    pub input: ModelContent,
    /// Exact, separately recorded canonical history selection for this turn.
    #[serde(default)]
    pub selected_context: Option<SelectedModelContext>,
    /// Explicit caller budget; `None` continues until the original turn settles.
    pub max_steps: Option<u32>,
}

impl TurnInput {
    /// Constructs a turn from an exact, already recorded conversation selection.
    pub fn from_selected_context(
        operation_id: OperationId,
        selected_context: SelectedModelContext,
    ) -> Result<Self> {
        let input = selected_context
            .messages
            .last()
            .filter(|message| message.role == ModelRole::User)
            .map(|message| message.content.clone())
            .ok_or_else(|| {
                Error::Invalid("selected context must end with a user message".into())
            })?;
        input.validate_turn_input(&mut None)?;
        selected_context.validate_for_input(&input, None)?;
        Ok(Self {
            max_steps: None,
            operation_id,
            input,
            selected_context: Some(selected_context),
        })
    }
}

/// Gapless replay record returned by a durable execution journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ExecutionRecord {
    /// Owning turn execution.
    pub operation_id: OperationId,
    /// Gapless one-based journal sequence.
    pub sequence: u64,
    /// Original Stream commit time in Unix microseconds, carried through replay
    /// and indexed selection without another clock sample or stored payload.
    /// Unknown for journals that supply no original publication timestamp.
    pub committed_at_micros: Option<u64>,
    /// Stable per-step retry identity.
    pub idempotency_key: String,
    /// Canonical observation.
    pub event: ExecutionEvent,
}

/// Canonical executor observation suitable for a durable journal.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, crate::HeapLayout,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ModelPurpose {
    /// The caller's ordinary response/tool loop.
    Response,
    /// A retained context summary inside that same execution.
    Summary,
    /// One capacity-bounded part of a multi-call summary at the same worker step.
    SummaryPart(u64),
    /// Token admission during request preparation, separate from generation.
    Count,
}

impl ModelPurpose {
    /// Stable shared-budget attempt identity on the optional preparation ledger.
    pub fn attempt_operation(
        self,
        execution: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<OperationId> {
        let charge = acyclic_fs::WorkCounters {
            bytes_hashed: if matches!(self, Self::Summary | Self::SummaryPart(_)) {
                b"harness:summary-attempt:v1".len() as u64
                    + 16
                    + if matches!(self, Self::SummaryPart(_)) {
                        8
                    } else {
                        0
                    }
            } else {
                0
            },
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(charge)?;
        }
        let operation = match self {
            Self::Response | Self::Count => execution,
            Self::Summary | Self::SummaryPart(_) => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"harness:summary-attempt:v1");
                hasher.update(&execution.into_bytes());
                if let Self::SummaryPart(part) = self {
                    hasher.update(&part.to_le_bytes());
                }
                let mut bytes = [0; 16];
                bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
                OperationId::from_bytes(bytes)
            }
        };
        if let Some(io) = io {
            io.spent(charge)?;
        }
        Ok(operation)
    }
}

/// Trusted classification attached by the admitted tool executor, never by
/// arbitrary result JSON. The original result reference remains the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ToolRecordKind {
    /// An authored productivity report in its original tool completion.
    Feedback,
}

/// Canonical executor observation suitable for a durable journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[allow(
    clippy::large_enum_variant,
    reason = "journal observations preserve direct typed ref fields"
)]
pub enum ExecutionEvent {
    /// Binds an operation identity to one immutable request and composition.
    Started {
        /// Digest of the input, stock executor version, model, stages, and tools.
        request_digest: [u8; 32],
    },
    /// A trusted producer's accepted progress receipt, recorded before its
    /// terminal result. Derived output and model claims never produce this.
    Progress {
        /// Exact immutable publication: a generation, commit/plan artifact or
        /// a stream event reference pinned to its original version.
        #[cfg_attr(feature = "wasm", tsify(type = "unknown"))]
        publication: crate::resources::ResourceRef,
    },
    /// An original native capture's measured content outcome, including an
    /// explicit unavailable observation. The receipt is host-owned output,
    /// never a counter copied from a model or a tool's stdout.
    NativeCapture {
        /// Original capture evidence and call identity.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        receipt: FileRef,
    },
    /// Appends an immutable conversation envelope at this original journal position.
    ConversationItem {
        /// Executor step admitting the item; this does not close an exchange.
        step: u32,
        /// Original private payload, including source cursor, intent and profile.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        envelope: FileRef,
        /// Identity of records and original fences committed with this item.
        publication: Option<[u8; 32]>,
    },
    /// Pins a response projection before any summary or response model admission.
    ContextPrepared {
        /// Zero-based response step.
        step: u32,
        /// Exact context; model/tools/options are pinned by the Started composition.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        projection: FileRef,
        /// Inbox range included in this exact response request, if bound to an agent.
        steering: Option<crate::agent::BoundaryCut>,
        /// Host-admitted effort delta, restored into the original turn transcript on replay.
        #[serde(skip_serializing_if = "Option::is_none")]
        configuration: Option<crate::model::ConfigurationUpdate>,
    },
    /// Pins a verified compacted projection before response admission.
    ContextCompacted {
        /// Zero-based response step.
        step: u32,
        /// Exact retained context, including its current input marker.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        projection: FileRef,
        /// Proof binding this projection to the prepared source and admitted summary.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        compaction: FileRef,
    },
    /// A model request identity committed before provider dispatch.
    ModelStarted {
        /// Zero-based executor step.
        step: u32,
        /// Distinguishes response and summary admissions in the same journal.
        purpose: ModelPurpose,
        /// Digest of the exact model request.
        request_digest: [u8; 32],
        /// Pinned private artifact containing the exact provider-neutral request bytes.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        request: FileRef,
        /// Verified occupancy of this exact admitted request, when available.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<crate::context::ContextWindow>,
    },
    /// One model stream item was observed.
    Model {
        /// Zero-based executor step.
        step: u32,
        /// Must match the corresponding admitted request.
        purpose: ModelPurpose,
        /// Pinned private event payload. Text uses its typed version-one UTF-8
        /// encoding; other event kinds use canonical JSON.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        event: FileRef,
        /// Private per-call JSONL record, committed with its terminal event.
        /// Absent on nonterminal observations and unmeasured journal adapters.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire | null"))]
        usage: Option<FileRef>,
    },
    /// One small canonical model event stored directly in the journal frame.
    ModelInline {
        /// Zero-based executor step.
        step: u32,
        /// Must match the corresponding admitted request.
        purpose: ModelPurpose,
        /// Exact canonical model-event JSON, bounded by the journal provider.
        event: String,
        /// Private per-call JSONL record, committed with its terminal event.
        /// Absent on nonterminal observations and unmeasured journal adapters.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire | null"))]
        usage: Option<FileRef>,
    },
    /// One allocation-free transport observation in the original model stream.
    /// No payload object, encoded event string or summary record is stored.
    ModelWait {
        /// Zero-based executor step.
        step: u32,
        /// Admitted response or summary attempt.
        purpose: ModelPurpose,
        /// Exact original observation, without remote error text.
        observation: crate::model::ModelWait,
    },
    /// The local model producer stopped after invocation cancellation.
    ModelStopped {
        /// Zero-based executor step.
        step: u32,
        /// Admitted response or summary attempt.
        purpose: ModelPurpose,
        /// Speculative work canceled only because its worker yielded for an
        /// interaction. It is not a failed context preparation on recovery.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        worker_paused: bool,
    },
    /// Semantic output of one completed response, before dispatching its tools.
    /// Transport call identities, usage and provider metadata are excluded.
    ModelOutput {
        /// Original zero-based response step.
        step: u32,
        /// Canonical digest of text and ordered tool names/arguments; null for empty output.
        digest: Option<[u8; 32]>,
    },
    /// Tool dispatch is about to begin.
    ToolStarted {
        /// Zero-based executor step.
        step: u32,
        /// Stable provider/model-owned call identity.
        call_id: String,
        /// Pinned, private JSON file containing the admitted invocation.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        invocation: FileRef,

        /// Identity of original records published atomically with this start.
        #[serde(skip_serializing_if = "Option::is_none")]
        publication: Option<[u8; 32]>,
    },
    /// Tool execution and projection completed.
    ToolCompleted {
        /// Zero-based executor step.
        step: u32,
        /// Stable provider/model-owned call identity.
        call_id: String,
        /// Pinned private JSON file containing the validated result.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        result: FileRef,
        /// Pinned private JSON file containing the model-visible projection.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        projection: FileRef,
        /// Trusted record classification; absent for ordinary tool outputs.
        #[serde(skip_serializing_if = "Option::is_none")]
        record_kind: Option<ToolRecordKind>,
    },
    /// A settled tool failure, returned to the model as the call's result; recorded without
    /// exception bodies or secrets.
    ToolFailed {
        /// Zero-based executor step.
        step: u32,
        /// Stable call identity.
        call_id: String,
        /// Stage that failed.
        reason: ToolFailureKind,
        /// Bounded, sanitized cause shared by the journal and model-visible result.
        cause: ToolFailureCause,
    },
}

/// Stable, non-secret classes of a settled tool failure the model receives as its result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ToolFailureKind {
    /// Admission or execution failed without an uncertain effect.
    ExecutorRejected,
    /// Executor result did not satisfy its pinned output contract.
    InvalidOutput,
    /// A model-visible result could not be projected safely.
    ProjectionRejected,
    /// A result artifact could not be published under the pinned limits.
    PublicationRejected,
    /// The composition binds no granted tool by the called name; recorded without a start.
    Unavailable,
    /// A formerly declared tool was removed from its live source.
    Removed,
    /// The arguments violated the tool's pinned input schema; recorded without a start.
    InvalidInput,
    /// The executing process ended before the outcome was recorded, and the
    /// executor cannot reconcile it. The effect may have run, partly or fully.
    Interrupted,
}

impl ToolFailureKind {
    /// Credential-free, stable diagnostic for a recorded tool failure.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::ExecutorRejected => "tool execution failed",
            Self::InvalidOutput => "tool output violated its pinned schema",
            Self::ProjectionRejected => "tool result projection was rejected",
            Self::PublicationRejected => "tool result artifact could not be published",
            Self::Unavailable => "tool is not available",
            Self::Removed => "tool was removed",
            Self::InvalidInput => "tool arguments violated the input schema",
            Self::Interrupted => {
                "tool was interrupted before its outcome was recorded; it may have run, partly or fully"
            }
        }
    }

    /// Whether the call is settled before any dispatch, so no start precedes the failure.
    #[must_use]
    pub const fn precedes_dispatch(self) -> bool {
        matches!(self, Self::Unavailable | Self::Removed | Self::InvalidInput)
    }
}

#[doc(hidden)]
pub use crate::filesystem::PreparationIo;

#[doc(hidden)]
pub fn with_history_io<T>(
    io: Option<&PreparationIo>,
    run: impl FnOnce(&mut Option<&mut dyn crate::store::history::HistoryIo>) -> T,
) -> T {
    {
        let mut observer = io.map(PreparationIo::history_io);
        run(&mut observer
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo))
    }
}

/// Incremental replay validation retaining only a cursor and retry digests.
pub struct ExecutionReplay {
    operation_id: OperationId,
    after: u64,
    keys: Vec<[u8; 32]>,
}

/// Original accepted execution-journal cut, independent of the current tail.
#[cfg(feature = "research")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct ExecutionJournalCut {
    /// Original cut format, currently one.
    pub version: u32,
    /// Original execution operation, distinct from an actor inbox cursor.
    pub operation: OperationId,
    /// Accepted one-based terminal model observation, including exact replay.
    pub sequence: u64,
}

/// Terminal result produced by an executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
pub struct TurnOutput {
    /// Original accepted terminal observation. Missing means the executor or
    /// journal supplied no cut; it never certifies an empty complete read set.
    #[cfg(feature = "research")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub journal_cut: Option<ExecutionJournalCut>,
    /// User-visible assistant text.
    pub text: String,
    /// Ordered, already staged assistant attachments or published artifacts.
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    /// Provider-owned final metadata.
    pub metadata: Value,
    /// Number of completed model steps.
    pub steps: u32,
    /// Every message the turn added after its input, in request order: tool
    /// notices, each step's assistant parts and tool results, then the final
    /// reply. After working-history compaction, retired steering precedes
    /// the surviving tail; all other messages in the retired prefix stay
    /// retired. The next turn replays this transcript verbatim.
    /// Empty for an output that did not come from a model turn; such a
    /// turn replays as its text. Staged with the output itself, so keeping
    /// them costs no extra write.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript: Vec<ModelMessage>,
}

impl TurnOutput {
    /// Creates an output without a model transcript, attachments or an
    /// accepted execution-journal cut. Adapters can supply their original
    /// transcript and attachments after construction.
    pub fn new(text: String, metadata: Value, steps: u32) -> Self {
        Self {
            #[cfg(feature = "research")]
            journal_cut: None,
            text,
            attachments: Vec::new(),
            metadata,
            steps,
            transcript: Vec::new(),
        }
    }
}

/// Execution progress with its settled value or pending approval operation.
pub enum ExecutionProgress<T> {
    /// Original supervision or file control parked or stopped this execution.
    Controlled(crate::supervision::Control),
    /// Preparation failed before any provider attempt could start.
    PreDispatchFailed(PreDispatchFailure),
    /// The operation settled with its complete result.
    Ready(T),
    /// The original approval operation is pending.
    Pending(OperationId),
    /// A recorded tool failure ended the turn.
    Failed(ToolFailureKind),
}

impl<T> ExecutionProgress<T> {
    /// Preserve an owner decision across replaceable executor/helper layers.
    pub(crate) fn controlled(result: Result<Self>) -> Result<Self> {
        match result {
            Err(Error::ExecutionControlled(control)) => Ok(Self::Controlled(control)),
            result => result,
        }
    }

    fn map<U>(self, ready: impl FnOnce(T) -> U) -> ExecutionProgress<U> {
        match self {
            Self::Controlled(control) => ExecutionProgress::Controlled(control),
            Self::Ready(value) => ExecutionProgress::Ready(ready(value)),
            Self::PreDispatchFailed(failure) => ExecutionProgress::PreDispatchFailed(failure),
            Self::Pending(operation) => ExecutionProgress::Pending(operation),
            Self::Failed(reason) => ExecutionProgress::Failed(reason),
        }
    }
}

impl ExecutionProgress<TurnOutput> {
    pub(crate) fn into_output(self) -> Result<TurnOutput> {
        match self {
            Self::Controlled(control) => Err(Error::ExecutionControlled(control)),
            Self::PreDispatchFailed(failure) => Err(failure.into_error()),
            Self::Ready(output) => Ok(output),
            Self::Pending(operation) => Err(Error::Indeterminate(operation)),
            Self::Failed(reason) => Err(Error::Invalid(reason.message().into())),
        }
    }
}

/// Complete replaceable turn loop. Implementations may own every policy decision.
pub trait Executor: acyclic_stream::ProviderPlatform {
    /// Executes or resumes one turn using only explicit durable host services.
    fn execute<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>>;

    /// Executes or resumes one turn, keeping a pending or refused approval and a
    /// recorded tool failure distinct from an uncertain outcome.
    fn execute_progress<'a>(
        &'a self,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<ExecutionProgress<TurnOutput>>> {
        Box::pin(async move {
            ExecutionProgress::controlled(
                self.execute(input, journal)
                    .await
                    .map(ExecutionProgress::Ready),
            )
        })
    }

    /// Pure audited boundary probe; unsupported custom loops are never invoked.
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }
}

/// A host-owned source of complete, admitted per-step configurations.
/// Selections must be retained under the original `(operation, step)` before
/// returning, so replay never substitutes current files for an admitted cut.
/// The selected binding includes its provider, native declarations, sampling,
/// context capacity and reasoning profile. It cannot replace the running
/// execution's journal, original task authority, context epoch or inherited
/// prefix. Instruction changes enter through ordinary appended source records.
pub trait StepConfiguration: acyclic_stream::ProviderPlatform {
    /// Selects an immutable configuration, or retains the preceding one.
    /// Implementations use `io` for preparation and retain any owned result
    /// storage with its original allocation owner through the returned `Arc`.
    fn select<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        operation: OperationId,
        step: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<Arc<StepBinding>>>>;
}

/// One immutable model-step selection. It carries the provider's complete model
/// profile, sampling, executable declarations and hooks, while turn context,
/// task authority and journal ownership remain with the running executor.
pub struct StepBinding {
    model: ExecutorModel,
    sampling: Option<crate::model::ModelSampling>,
    effort: Option<crate::config::Effort>,
    resolved_sampling: once_cell::sync::OnceCell<crate::model::ModelSampling>,
    provider: Arc<dyn ModelProvider>,
    tools: ToolRegistry,
    #[cfg(not(target_arch = "wasm32"))]
    hooks: Option<Arc<crate::hooks::BoundHooks>>,
    #[cfg(not(target_arch = "wasm32"))]
    // Retain the admitted data and its original engine/allocation custody together.
    plugin_set: Option<Arc<crate::plugins::Enabled>>,
    #[cfg(not(target_arch = "wasm32"))]
    prompt_block: Option<String>,
    // All selected values retire before the original owner's shared storage.
    _custody: Option<crate::filesystem::PreparationCustody>,
}
/// Complete default streaming model/tool loop assembled from replaceable values.
#[derive(Clone)]
pub struct StockExecutor {
    step_configuration: Option<Arc<dyn StepConfiguration>>,
    intervention: Option<Arc<crate::agent::ContinuationOverride>>,
    model: ExecutorModel,
    sampling: Option<crate::model::ModelSampling>,
    resolved_sampling: once_cell::sync::OnceCell<crate::model::ModelSampling>,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
    epoch_tools: Option<Arc<Vec<crate::tool::ToolDefinition>>>,
    #[cfg(not(target_arch = "wasm32"))]
    hooks: Option<Arc<crate::hooks::BoundHooks>>,
    #[cfg(not(target_arch = "wasm32"))]
    plugin_set: Option<Arc<crate::plugins::Enabled>>,
    /// A prompt hook's block: the turn fails with it before any model request.
    #[cfg(not(target_arch = "wasm32"))]
    prompt_block: Option<String>,
    limits: Limits,
    tool_scope: RuntimeScope,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_identity: Option<ComponentIdentity>,
    inherited_prefix: Option<(
        FileRef,
        Arc<dyn crate::conversation::ContentResidencyVerifier>,
    )>,
    task_context: Option<(Arc<crate::runtime::TaskContext>, OperationId)>,
    // `None` is the default: capacity compaction wherever the bound task can
    // publish private summaries, and none elsewhere. An explicit policy is
    // verified at turn entry.
    compaction: Option<crate::context::CompactionPolicy>,
    reconciliation_only: bool,
}

#[derive(Clone)]
struct ExecutorModel(Arc<crate::model::ResolvedModel>);

impl std::ops::Deref for ExecutorModel {
    type Target = Model;

    fn deref(&self) -> &Model {
        &self.0.0
    }
}

/// Journal payload separating canonical history from the exact transformed request.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResponseProjection {
    pub(crate) context: crate::context::Context,
    /// Exact steering before compaction or transforms, covered by this projection.
    pub(crate) steering: Option<crate::agent::SteeringSnapshot>,
    pub(crate) canonical: Option<FileRef>,
    pub(crate) checkpoint: Option<FileRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) working: Option<FileRef>,
    /// Request-bound accounting published atomically with this projection.
    pub(crate) accounting: Option<ContextAccounting>,
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContextAccounting {
    pub(crate) capacity: crate::context::ModelContextCapacity,
    pub(crate) count: InputAccounting,
}

/// A missing tokenizer is distinct from a provider's context-overflow refusal.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum InputAccounting {
    Counted(accounting::ContextCount),
    Overflow,
    Unavailable,
}

impl InputAccounting {
    fn as_ref(&self) -> Option<&accounting::ContextCount> {
        match self {
            Self::Counted(count) => Some(count),
            Self::Overflow | Self::Unavailable => None,
        }
    }
}

/// Validated retained model artifacts without provider dispatch.
pub(crate) struct RetainedModelStep {
    #[cfg(feature = "research")]
    pub(crate) journal_cut: Option<ExecutionJournalCut>,
    pub(crate) request: Option<crate::model::PreparedModelRequest>,
    pub(crate) configuration: Option<crate::model::ConfigurationUpdate>,
    pub(crate) admission: ModelEventAdmission,
    pub(crate) events: Vec<ModelEvent>,
}

pub(crate) use crate::filesystem::PreparationRecord;

#[derive(Default, Serialize, Deserialize, crate::HeapLayout)]
#[doc(hidden)]
pub struct ModelEventAdmission {
    #[doc(hidden)]
    pub count: usize,
    #[doc(hidden)]
    pub calls: Vec<String>,
    #[doc(hidden)]
    pub completed: bool,
    #[doc(hidden)]
    pub text_bytes: u64,
    /// Reasoning delta bytes streamed by the current transport attempt.
    #[doc(hidden)]
    pub reasoning_bytes: u64,
    /// Output streamed by attempts that a restart abandoned.
    #[doc(hidden)]
    pub abandoned: ModelAbandonedOutput,
}

#[cfg(test)]
pub(crate) mod tests;

mod validation;
#[doc(hidden)]
pub use validation::validate_json_schema_value;
#[doc(hidden)]
pub use validation::{check_json_schema_value, compile_json_schema, preparation_format};

/// Bounded JSON shared by the existing IO adapters.
pub mod json;
