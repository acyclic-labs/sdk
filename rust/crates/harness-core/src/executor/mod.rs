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
    stage_json, stock_boundary, stock_into_iter, stock_string_copy, stock_string_push,
    stock_vec_push, stock_vec_reserve,
};

pub(crate) mod execution_claim;
mod journal;
pub use execution_claim::{ExecutionAttempt, ExecutionClaim, ExecutionClaims};
use journal::validate_journal;
pub use journal::{
    ExecutionJournal, ExecutionJournalCacheId, ExecutionRecordQuery, IndexedExecutionFuture,
    IndexedExecutionRecords,
};
mod replay;
pub(crate) use replay::replay_execution;
mod accounting;
use accounting::*;
pub(crate) use accounting::{stock_context_capacity, stock_count_tokens};
mod binding;
#[cfg(feature = "filesystem")]
mod code_mode;
mod compaction;
mod composition;
#[cfg(feature = "filesystem")]
pub use code_mode::{CODE_REVISION, call_tool, code_tool};
mod content;
mod dispatch;
mod live_reply;
pub use live_reply::LiveReply;
mod model_observation;
pub(crate) mod model_usage;
#[cfg(feature = "filesystem")]
pub(crate) use model_usage::validate as validate_model_usage;
pub use model_usage::{ModelTokenUsage, ModelUsagePage, ModelUsageRecord, usage_page};
pub(crate) mod phase;
#[cfg(test)]
pub(crate) use model_observation::append as append_model_event;
mod prefix;
mod record_index;
mod records;
mod request;
mod response;
mod retained_request;
pub(crate) mod transcript;
mod working_tail;
pub(crate) use transcript::load_projection as load_response_projection;
mod summary;
#[cfg(any(test, feature = "filesystem"))]
pub(crate) mod summary_identity;
mod tool_execution;
mod tool_failure;
pub(crate) use tool_execution::tool_failure_content;
pub(crate) use tool_failure::ToolFailureText;
pub use tool_failure::{TOOL_FAILURE_MESSAGE_BYTES, ToolErrorClass, ToolFailureCause};
#[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
mod agent_hooks;
#[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
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
#[cfg(all(any(test, feature = "test-support"), feature = "filesystem"))]
#[doc(hidden)]
pub use content::verify_model_contents_with_summary_authorization;
pub(crate) use tool_settlement::tool_settlement_key;
pub use tool_settlement::{ToolSettlement, ToolSettlementQuery, ToolTerminalOutcome};
mod retained;
mod turn;
pub(crate) use phase::DispatchPhase;
pub use phase::PreDispatchFailure;
pub(crate) use retained::{completed_tool_projection, retained_model_step};
mod checkpoint_selection;
pub use checkpoint_selection::canonical_checkpoint_for_operation;
use checkpoint_selection::*;
mod checkpoint_projection;
pub use checkpoint_projection::load_canonical_checkpoint;
mod payload;
#[cfg(any(test, feature = "filesystem", feature = "wasm"))]
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
pub struct TurnInput {
    /// Stable durable turn execution identity.
    pub operation_id: OperationId,
    /// Typed provider-neutral input; canonical conversation storage still uses file refs.
    pub input: ModelContent,
    /// Exact, separately recorded canonical history selection for this turn.
    #[serde(default)]
    pub selected_context: Option<SelectedModelContext>,
    /// Maximum model/tool steps permitted for this turn.
    pub max_steps: u32,
}

impl TurnInput {
    /// Constructs a turn from an exact, already recorded conversation selection.
    pub fn from_selected_context(
        operation_id: OperationId,
        selected_context: SelectedModelContext,
        max_steps: u32,
    ) -> Result<Self> {
        let input = selected_context
            .messages
            .last()
            .filter(|message| message.role == ModelRole::User)
            .map(|message| message.content.clone())
            .ok_or_else(|| {
                Error::Invalid("selected context must end with a user message".into())
            })?;
        input.validate_user_input(&mut None)?;
        selected_context.validate_for_input(&input, None)?;
        Ok(Self {
            operation_id,
            input,
            selected_context: Some(selected_context),
            max_steps,
        })
    }
}

/// Gapless replay record returned by a durable execution journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
pub struct ExecutionRecord {
    /// Owning turn execution.
    pub operation_id: OperationId,
    /// Gapless one-based journal sequence.
    pub sequence: u64,
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
}

impl ModelPurpose {
    /// Stable shared-budget attempt identity on the optional preparation ledger.
    pub fn attempt_operation(
        self,
        execution: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<OperationId> {
        #[cfg(feature = "filesystem")]
        let charge = acyclic_fs::WorkCounters {
            bytes_hashed: if self == Self::Summary {
                b"harness:summary-attempt:v1".len() as u64 + 16
            } else {
                0
            },
            ..Default::default()
        };
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.preflight(charge)?;
        }
        let operation = match self {
            Self::Response => execution,
            Self::Summary => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"harness:summary-attempt:v1");
                hasher.update(&execution.into_bytes());
                let mut bytes = [0; 16];
                bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
                OperationId::from_bytes(bytes)
            }
        };
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.spent(charge)?;
        }
        #[cfg(not(feature = "filesystem"))]
        let _ = io;
        Ok(operation)
    }
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
    /// Pins a response projection before any summary or response model admission.
    ContextPrepared {
        /// Zero-based response step.
        step: u32,
        /// Exact context; model/tools/options are pinned by the Started composition.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        projection: FileRef,
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
    },
    /// One model stream item was observed.
    Model {
        /// Zero-based executor step.
        step: u32,
        /// Must match the corresponding admitted request.
        purpose: ModelPurpose,
        /// Pinned, private JSON file containing one observed model event.
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
    /// The local model producer stopped after invocation cancellation.
    ModelStopped {
        /// Zero-based executor step.
        step: u32,
        /// Admitted response or summary attempt.
        purpose: ModelPurpose,
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
        /// Bounded, sanitized cause the model receives; the raw error stays in the log.
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
            Self::InvalidInput => "tool arguments violated the input schema",
            Self::Interrupted => {
                "tool was interrupted before its outcome was recorded; it may have run, partly or fully"
            }
        }
    }

    /// Whether the call is settled before any dispatch, so no start precedes the failure.
    #[must_use]
    pub const fn precedes_dispatch(self) -> bool {
        matches!(self, Self::Unavailable | Self::InvalidInput)
    }
}

#[cfg(feature = "filesystem")]
#[doc(hidden)]
pub use crate::filesystem::PreparationIo;

#[cfg(not(feature = "filesystem"))]
#[doc(hidden)]
pub type PreparationIo = ();

#[doc(hidden)]
pub fn with_history_io<T>(
    io: Option<&PreparationIo>,
    run: impl FnOnce(&mut Option<&mut dyn crate::store::history::HistoryIo>) -> T,
) -> T {
    #[cfg(feature = "filesystem")]
    {
        let mut observer = io.map(PreparationIo::history_io);
        run(&mut observer
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo))
    }
    #[cfg(not(feature = "filesystem"))]
    {
        let _ = io;
        run(&mut None)
    }
}

/// Incremental replay validation retaining only a cursor and retry digests.
pub struct ExecutionReplay {
    operation_id: OperationId,
    after: u64,
    keys: Vec<[u8; 32]>,
}

/// Terminal result produced by an executor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
pub struct TurnOutput {
    /// User-visible assistant text.
    pub text: String,
    /// Ordered, already staged assistant attachments or published artifacts.
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    /// Provider-owned final metadata.
    pub metadata: Value,
    /// Number of completed model steps.
    pub steps: u32,
}

/// Execution progress with its settled value or pending approval operation.
pub enum ExecutionProgress<T> {
    /// Preparation failed before any provider attempt could start.
    PreDispatchFailed(PreDispatchFailure),
    /// The operation settled with its complete result.
    Ready(T),
    /// The original approval operation is pending.
    Pending(OperationId),
    /// A recorded tool failure ended the turn.
    Failed(ToolFailureKind),
}

impl ExecutionProgress<TurnOutput> {
    pub(crate) fn into_output(self) -> Result<TurnOutput> {
        match self {
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
            self.execute(input, journal)
                .await
                .map(ExecutionProgress::Ready)
        })
    }

    /// Pure audited boundary probe; unsupported custom loops are never invoked.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }
}

/// Complete default streaming model/tool loop assembled from replaceable values.
#[derive(Clone)]
pub struct StockExecutor {
    model: Model,
    #[cfg(feature = "filesystem")]
    profile: Option<(
        Arc<crate::profile::Profiles>,
        crate::agent::AgentProfilePin,
        String,
    )>,
    #[cfg(all(feature = "code-mode", not(target_arch = "wasm32")))]
    code_runtime: Option<Arc<crate::code_mode::CodeRuntime>>,
    max_output_tokens: Option<u32>,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    hooks: Option<Arc<crate::hooks::BoundHooks>>,
    /// A prompt hook's block: the turn fails with it before any model request.
    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
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
    // `None` is the default: threshold compaction wherever the bound task can
    // publish private summaries, and none elsewhere. An explicit policy is
    // verified at turn entry.
    compaction: Option<crate::context::CompactionPolicy>,
    reconciliation_only: bool,
    live_reply: Option<LiveReply>,
}

/// Journal payload separating canonical history from the exact transformed request.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResponseProjection {
    pub(crate) context: crate::context::Context,
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
    pub(crate) count: crate::context::ModelTokenCount,
}

/// Validated retained model artifacts without provider dispatch.
pub(crate) struct RetainedModelStep {
    pub(crate) request: Option<crate::model::PreparedModelRequest>,
    pub(crate) admission: ModelEventAdmission,
    pub(crate) events: Vec<ModelEvent>,
}

#[cfg(feature = "filesystem")]
pub(crate) use crate::filesystem::PreparationRecord;

#[cfg(not(feature = "filesystem"))]
pub(crate) trait PreparationRecord: serde::de::DeserializeOwned + Serialize {}

#[cfg(not(feature = "filesystem"))]
impl<T: serde::de::DeserializeOwned + Serialize> PreparationRecord for T {}

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
}

#[cfg(test)]
mod tests;

mod validation;
#[doc(hidden)]
pub use validation::validate_json_schema_value;
#[doc(hidden)]
pub use validation::{check_json_schema_value, compile_json_schema, preparation_format};

/// Bounded JSON shared by the existing IO adapters.
pub mod json;
