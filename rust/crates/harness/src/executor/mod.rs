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
        DurableTaskHost, RuntimeScope, ToolPolicy, ToolPolicyDecision, check_tool_approval,
        tool_approval_request, validate_policy_identity,
    },
    tool::{ToolInvocation, ToolRegistry, ToolResult},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

mod headless;
mod native_stock;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native_stock::NativeStockExecutor;
use native_stock::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use native_stock::{
    completed_tool_projection_with_native_work, load_json_with_native_work,
    replay_execution_with_native_work, retained_model_step_with_native_work,
    stage_json_with_native_work,
};
pub(crate) use native_stock::{
    stage_json, stock_boundary, stock_string_copy, stock_string_push, stock_vec_push,
};

mod journal;
pub use journal::ExecutionJournal;
mod replay;
pub(crate) use replay::replay_execution;
mod accounting;
use accounting::*;
mod binding;
mod compaction;
mod composition;
mod content;
mod dispatch;
mod prefix;
mod records;
mod request;
mod response;
mod summary;
mod tool_execution;
mod tool_resolution;
use content::*;
mod retained;
mod turn;
pub(crate) use retained::{completed_tool_projection, retained_model_step};
mod checkpoint_selection;
use checkpoint_selection::*;
pub use checkpoint_selection::{canonical_checkpoint_for_operation, load_canonical_checkpoint_at};
mod checkpoint_projection;
pub use checkpoint_projection::{load_canonical_checkpoint, load_canonical_checkpoint_through};
mod payload;
#[cfg(feature = "filesystem")]
pub(crate) use payload::reserve_preparation_workspace;
pub(crate) use payload::{
    decode_json, load_json, load_model_request, preparation_digest, retain_preparation,
};
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
        /// Provider-owned capacities and exact request token bounds, when enabled.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire | null"))]
        accounting: Option<FileRef>,
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
        /// Final provider-owned request accounting.
        #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
        accounting: FileRef,
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
    /// A terminal tool failure recorded without exception bodies or secrets.
    ToolFailed {
        /// Zero-based executor step.
        step: u32,
        /// Stable call identity.
        call_id: String,
        /// Bounded classification; raw provider errors never enter the journal.
        reason: ToolFailureKind,
    },
}

/// Stable, non-secret terminal tool failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ToolFailureKind {
    /// Executor rejected the already admitted call.
    ExecutorRejected,
    /// Executor result did not satisfy its pinned output contract.
    InvalidOutput,
    /// A model-visible result could not be projected safely.
    ProjectionRejected,
    /// A result artifact could not be published under the pinned limits.
    PublicationRejected,
}

impl ToolFailureKind {
    /// Credential-free, stable diagnostic for a recorded tool failure.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::ExecutorRejected => "tool executor rejected the admitted call",
            Self::InvalidOutput => "tool output violated its pinned schema",
            Self::ProjectionRejected => "tool result projection was rejected",
            Self::PublicationRejected => "tool result artifact could not be published",
        }
    }
}

#[cfg(feature = "filesystem")]
pub(crate) use crate::filesystem::PreparationIo;

#[cfg(not(feature = "filesystem"))]
pub(crate) type PreparationIo = ();

pub(crate) fn with_history_io<T>(
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

/// Execution progress with its settled value or original approval outcome.
pub enum ExecutionProgress<T> {
    /// The operation settled with its complete result.
    Ready(T),
    /// The original approval operation is pending.
    Pending(OperationId),
    /// The original approval operation was rejected.
    Rejected(crate::InteractionRejection),
}

impl ExecutionProgress<TurnOutput> {
    pub(crate) fn into_output(self) -> Result<TurnOutput> {
        match self {
            Self::Ready(output) => Ok(output),
            Self::Pending(operation) => Err(Error::Indeterminate(operation)),
            Self::Rejected(reason) => Err(Error::InteractionRejected(reason)),
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

    /// Runs with the original ordinary task admission.
    fn execute_with_context<'a>(
        &'a self,
        context: &'a crate::runtime::TaskContext,
        input: TurnInput,
        journal: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, Result<TurnOutput>> {
        let _ = context;
        self.execute(input, journal)
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

    /// Audited native execution, sharing the exact issued context and original work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        _context: &'a Arc<crate::runtime::TaskContext>,
        _input: &'a TurnInput,
        _journal: &'a dyn ExecutionJournal,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (TurnOutput, acyclic_fs::NativeAllocation)>> {
        _work.remaining()?;
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native executor is unavailable",
        })
    }
}

/// Complete default streaming model/tool loop assembled from replaceable values.
#[derive(Clone)]
pub struct StockExecutor {
    model: Model,
    max_output_tokens: Option<u32>,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    tools: ToolRegistry,
    limits: Limits,
    tool_scope: RuntimeScope,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_identity: Option<ComponentIdentity>,
    task: Option<(
        Arc<dyn DurableTaskHost>,
        TaskId,
        crate::scheduler::LeaseFence,
    )>,
    inherited_prefix: Option<(
        FileRef,
        Arc<dyn crate::conversation::ContentResidencyVerifier>,
    )>,
    task_context: Option<(Arc<crate::runtime::TaskContext>, OperationId)>,
    compaction: crate::context::CompactionPolicy,
    reconciliation_only: bool,
}

/// Journal payload separating canonical history from the exact transformed request.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResponseProjection {
    pub(crate) context: crate::context::Context,
    pub(crate) canonical: Option<FileRef>,
    pub(crate) checkpoint: Option<FileRef>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextAccounting {
    capacity: crate::context::ModelContextCapacity,
    count: crate::context::ModelTokenCount,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ContextAccounting {
    fn diagnostic_label_bytes() -> u64 {
        native_stock::stock_serde_diagnostic_label_bytes()
    }
    fn maximum_slot_bytes() -> u64 {
        <crate::context::Context as crate::filesystem::PreparationRecord>::maximum_slot_bytes()
            .max(std::mem::size_of::<Self>() as u64)
    }
}

/// Validated retained model artifacts without provider dispatch.
pub(crate) struct RetainedModelStep {
    pub(crate) request: Option<crate::model::PreparedModelRequest>,
    pub(crate) admission: ModelEventAdmission,
    pub(crate) events: Vec<ModelEvent>,
}

struct CheckpointDeltaResolver<'a> {
    inner: &'a dyn crate::conversation::ContentResidencyVerifier,
    scope: &'a RuntimeScope,
    limits: Limits,
}

#[cfg(feature = "filesystem")]
pub(crate) use crate::filesystem::PreparationRecord;

#[cfg(not(feature = "filesystem"))]
pub(crate) trait PreparationRecord: serde::de::DeserializeOwned + Serialize {}

#[cfg(not(feature = "filesystem"))]
impl<T: serde::de::DeserializeOwned + Serialize> PreparationRecord for T {}

#[derive(Default)]
pub(crate) struct ModelEventAdmission {
    pub(crate) count: usize,
    pub(crate) calls: Vec<String>,
    pub(crate) completed: bool,
}

#[cfg(test)]
mod tests;

mod validation;
pub(crate) use validation::{compile_json_schema, preparation_format, validate_json_schema_value};

/// Bounded JSON shared by the existing IO adapters.
pub mod json;
