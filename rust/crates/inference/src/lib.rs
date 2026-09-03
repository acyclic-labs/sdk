//! Public Inference contract, ergonomic client handles, and deterministic memory provider.
//!
//! The public surface describes logical model work and retained Contexts. Placement,
//! batching, physical caches, KV movement, workers, and rebalancing are provider internals.

use acyclic_contracts::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Generated public gRPC messages and client/server bindings.
#[allow(missing_docs, clippy::all)]
pub mod wire {
    /// Versioned public packages.
    pub mod acyclic {
        /// Shared operation and admission messages.
        pub mod harness {
            /// Harness protocol revision 1.
            pub mod v1 {
                tonic::include_proto!("acyclic.harness.v1");
            }
        }

        /// Inference Context and Run messages.
        pub mod inference {
            /// Inference protocol revision 1.
            pub mod v1 {
                tonic::include_proto!("acyclic.inference.v1");
            }
        }
    }
}

mod grpc;
pub use grpc::ManagedInference;

const MAXIMUM_ITEMS: usize = 4_096;
const MAXIMUM_CONTENT_BYTES: usize = 8 * 1024 * 1024;

/// Stable identifier allocated before a logical request is dispatched.
pub type RequestId = String;
/// Stable immutable Context revision identifier.
pub type ContextId = String;
/// Stable recoverable Run identifier.
pub type RunId = String;

/// Canonical Context item kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ItemKind {
    /// Global instruction.
    Instruction,
    /// System-authored content.
    System,
    /// Developer-authored content.
    Developer,
    /// User-authored content.
    User,
    /// Model-authored content.
    Assistant,
    /// Versioned tool definition.
    ToolDefinition,
    /// Complete validated tool call.
    ToolCall,
    /// Result linked to a tool call.
    ToolResult,
    /// Supported image content.
    Image,
    /// Supported audio content.
    Audio,
    /// Supported file content.
    File,
    /// Opaque model-specific continuation content.
    Continuation,
}

/// One immutable canonical Context item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Item {
    /// Identity stable within its lineage.
    pub id: String,
    /// Semantic item kind.
    pub kind: ItemKind,
    /// Exact content bytes, shared across in-process forks without copying.
    pub content: Bytes,
    /// Tool-call relationship when required by the item kind.
    pub link: Option<String>,
    /// Exact originating execution profile for opaque continuation content.
    pub continuation_profile: Option<String>,
}

impl Item {
    /// Creates one text item with a fresh stable identity.
    #[must_use]
    pub fn text(kind: ItemKind, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            kind,
            content: Bytes::from(text.into()),
            link: None,
            continuation_profile: None,
        }
    }
}

/// Explicit durable or admitted warm retention policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Retention {
    /// Canonical content remains recoverable without a residency promise.
    Durable,
    /// Provider-admitted warm policy through an absolute Unix-millisecond deadline.
    WarmUntil(u64),
}

impl Retention {
    /// Requests an admitted warm policy until the supplied Unix-millisecond deadline.
    #[must_use]
    pub const fn warm_until(expires_at_unix_ms: u64) -> Self {
        Self::WarmUntil(expires_at_unix_ms)
    }
}

/// How a Context revision was produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance {
    /// Initial Context creation.
    Created,
    /// Immutable content mutation from a parent revision.
    Derived,
    /// Independent lineage fork over an exact source revision.
    Forked {
        /// Exact source revision.
        source: ContextId,
    },
    /// Canonical content replayed for another model identity.
    Transferred {
        /// Source revision.
        source: ContextId,
        /// Whether compatible provider state was reused.
        reused_compatible_state: bool,
    },
    /// Completed generation produced a continuation revision.
    Generated {
        /// Run that produced this revision.
        run: RunId,
    },
}

/// Immutable logical Context revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextSnapshot {
    /// Revision identity.
    pub id: ContextId,
    /// Independent lineage identity.
    pub lineage: String,
    /// Parent revision when one exists.
    pub parent: Option<ContextId>,
    /// Pinned provider model identity.
    pub model: String,
    /// Ordered canonical content.
    pub items: Arc<[Item]>,
    /// Current retention policy for this reference.
    pub retention: Retention,
    /// Revision provenance.
    pub provenance: Provenance,
}

/// One exact item-addressed Context edit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContextEdit {
    /// Append an item.
    Append(Item),
    /// Insert before the identified item.
    InsertBefore {
        /// Existing item identity.
        target: String,
        /// New item.
        item: Item,
    },
    /// Insert after the identified item.
    InsertAfter {
        /// Existing item identity.
        target: String,
        /// New item.
        item: Item,
    },
    /// Replace content while preserving identity, kind, and link.
    Replace {
        /// Existing item identity.
        target: String,
        /// Replacement bytes.
        content: Bytes,
    },
    /// Delete one identified item.
    Delete {
        /// Existing item identity.
        target: String,
    },
}

impl ContextEdit {
    /// Replaces one item's exact bytes while preserving its semantic identity.
    #[must_use]
    pub fn replace(target: impl Into<String>, text: impl Into<String>) -> Self {
        Self::Replace {
            target: target.into(),
            content: Bytes::from(text.into()),
        }
    }
}

/// Provider-advertised model behavior, never inferred from an endpoint shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelCapabilities {
    /// Exact model identity.
    pub model: String,
    /// Maximum canonical input bytes supported by this profile.
    pub maximum_context_bytes: u64,
    /// Maximum output units supported by this profile.
    pub maximum_output: u64,
    /// Supported logical features.
    pub features: BTreeSet<String>,
}

/// Capability-checked generation controls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationSettings {
    /// Maximum output units.
    pub maximum_output: u64,
    /// Optional reproducibility seed when the selected profile supports it.
    pub seed: Option<u64>,
}

impl Default for GenerationSettings {
    fn default() -> Self {
        Self {
            maximum_output: 1_024,
            seed: None,
        }
    }
}

/// Four work-based logical quantities. Physical replicas never multiply these values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Usage {
    /// Input work newly computed.
    pub new_prefill: u64,
    /// Generated output work.
    pub generated_output: u64,
    /// Effective Context read work.
    pub effective_context_reads: u64,
    /// Retained logical byte-milliseconds settled by this receipt.
    pub retained_byte_millis: u64,
}

/// Immutable settled logical usage receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageReceipt {
    /// Receipt identity.
    pub id: String,
    /// Exact model identity.
    pub model: String,
    /// Meter revision.
    pub meter_revision: String,
    /// Settled quantities.
    pub usage: Usage,
}

/// Factual terminal Run state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunTerminal {
    /// Output and continuation completed.
    Completed,
    /// Output reached its admitted bound.
    OutputLimited,
    /// A complete validated tool call ended the Run.
    ToolCall,
    /// Model refusal.
    Refusal,
    /// Cancellation was confirmed.
    Cancelled,
    /// Execution failed after admission.
    Failed,
    /// Completion cannot be established.
    Indeterminate,
}

/// Ordered replayable Run event payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunEventKind {
    /// Output bytes in order.
    Output(Bytes),
    /// Provisional usage observation.
    Usage(Usage),
    /// Exactly one factual terminal event.
    Terminal(RunTerminal),
}

/// One ordered Run event. Observation may redeliver the same sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunEvent {
    /// Monotonic sequence beginning at zero.
    pub sequence: u64,
    /// Event payload.
    pub kind: RunEventKind,
}

/// Completed Run result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunResult {
    /// Exact output bytes.
    pub output: Bytes,
    /// New continuation Context when valid.
    pub context: Option<ContextSnapshot>,
    /// Factual terminal state.
    pub terminal: RunTerminal,
    /// Immutable settled usage.
    pub receipt: UsageReceipt,
}

/// Recoverable Run observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSnapshot {
    /// Run identity allocated before execution.
    pub id: RunId,
    /// Input Context revision.
    pub input: ContextId,
    /// Committed events.
    pub events: Arc<[RunEvent]>,
    /// Terminal result when settled.
    pub result: Option<RunResult>,
}

/// Idempotent Context creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateContextRequest {
    /// Caller-retained request identity.
    pub request_id: RequestId,
    /// Model alias or exact identity resolved by the provider.
    pub model: String,
    /// Initial canonical items.
    pub items: Vec<Item>,
}

/// Idempotent Context mutation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutateContextRequest {
    /// Caller-retained request identity.
    pub request_id: RequestId,
    /// Immutable source revision.
    pub source: ContextId,
    /// Exact mutation.
    pub mutation: ContextMutation,
}

/// Context mutation preserving an immutable source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContextMutation {
    /// Establish a new independently retained lineage.
    Fork,
    /// Apply exact item edits atomically.
    Edit(Vec<ContextEdit>),
    /// Retain an exact prefix; `None` means empty.
    Truncate(Option<String>),
    /// Replace selected items explicitly.
    Compact {
        /// Exact selected item identities.
        selected: Vec<String>,
        /// Explicit replacement items.
        replacement: Vec<Item>,
    },
    /// Transfer canonical content to another pinned model.
    Transfer {
        /// Exact target model or provider-resolved alias.
        model: String,
    },
}

/// Idempotent Run admission request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerateRequest {
    /// Caller-retained request identity.
    pub request_id: RequestId,
    /// Immutable input Context revision.
    pub context: ContextId,
    /// New user input.
    pub input: Item,
    /// Exact generation controls.
    pub settings: GenerationSettings,
}

/// Logical provider boundary shared by memory, customer-hosted, and managed adapters.
#[async_trait]
pub trait InferenceProvider: Send + Sync {
    /// Lists exact accessible model profiles.
    async fn models(&self) -> Result<Vec<ModelCapabilities>>;
    /// Creates or reconciles one immutable Context.
    async fn create_context(&self, request: CreateContextRequest) -> Result<ContextSnapshot>;
    /// Reads one retained Context revision.
    async fn inspect_context(&self, id: &str) -> Result<ContextSnapshot>;
    /// Applies or reconciles one exact immutable mutation.
    async fn mutate_context(&self, request: MutateContextRequest) -> Result<ContextSnapshot>;
    /// Changes only this revision's retention reference.
    async fn retain_context(&self, id: &str, retention: Retention) -> Result<ContextSnapshot>;
    /// Deletes only this revision's reference.
    async fn delete_context(&self, id: &str) -> Result<bool>;
    /// Admits or reconciles one Run.
    async fn generate(&self, request: GenerateRequest) -> Result<RunSnapshot>;
    /// Observes one existing Run without executing a successor.
    async fn inspect_run(&self, id: &str) -> Result<RunSnapshot>;
    /// Replays committed events from an inclusive sequence and follows through terminal.
    async fn run_events(&self, id: &str, from_sequence: u64) -> Result<Vec<RunEvent>> {
        Ok(self
            .inspect_run(id)
            .await?
            .events
            .iter()
            .filter(|event| event.sequence >= from_sequence)
            .cloned()
            .collect())
    }
    /// Requests cancellation of one existing Run.
    async fn cancel_run(&self, id: &str) -> Result<RunSnapshot>;
}

#[derive(Default)]
struct MemoryState {
    contexts: BTreeMap<ContextId, ContextSnapshot>,
    runs: BTreeMap<RunId, RunSnapshot>,
    creates: BTreeMap<RequestId, (CreateContextRequest, ContextId)>,
    mutations: BTreeMap<RequestId, (MutateContextRequest, ContextId)>,
    generations: BTreeMap<RequestId, (GenerateRequest, RunId)>,
}

/// Deterministic bounded process-local provider for tests and local composition.
#[derive(Clone)]
pub struct MemoryInference {
    state: Arc<RwLock<MemoryState>>,
    models: Arc<[ModelCapabilities]>,
}

impl Default for MemoryInference {
    fn default() -> Self {
        Self::new([ModelCapabilities {
            model: "deterministic".to_owned(),
            maximum_context_bytes: MAXIMUM_CONTENT_BYTES as u64,
            maximum_output: 65_536,
            features: BTreeSet::from([
                "contexts".to_owned(),
                "forks".to_owned(),
                "recovery".to_owned(),
            ]),
        }])
    }
}

impl MemoryInference {
    /// Creates a memory provider with an exact model catalog.
    #[must_use]
    pub fn new(models: impl IntoIterator<Item = ModelCapabilities>) -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::default())),
            models: models.into_iter().collect::<Vec<_>>().into(),
        }
    }

    fn model(&self, model: &str) -> Result<&ModelCapabilities> {
        self.models
            .iter()
            .find(|candidate| candidate.model == model)
            .ok_or_else(|| Error::Unsupported(format!("model {model}")))
    }
}

fn request_id() -> String {
    Uuid::new_v4().to_string()
}

fn context_id() -> String {
    Uuid::new_v4().to_string()
}

fn validate_items(items: &[Item]) -> Result<()> {
    if items.len() > MAXIMUM_ITEMS {
        return Err(Error::Invalid("Context item bound exceeded".to_owned()));
    }
    let mut ids = BTreeSet::new();
    let mut bytes = 0_usize;
    for item in items {
        bytes = bytes
            .checked_add(item.content.len())
            .ok_or_else(|| Error::Invalid("Context byte bound overflowed".to_owned()))?;
        if item.id.is_empty() || !ids.insert(item.id.clone()) {
            return Err(Error::Invalid(
                "Context item identity is invalid".to_owned(),
            ));
        }
        let linked = matches!(item.kind, ItemKind::ToolCall | ItemKind::ToolResult);
        let continuation = item.kind == ItemKind::Continuation;
        if linked != item.link.is_some()
            || continuation != item.continuation_profile.is_some()
            || item
                .continuation_profile
                .as_ref()
                .is_some_and(String::is_empty)
        {
            return Err(Error::Invalid("Context tool link is invalid".to_owned()));
        }
    }
    if bytes > MAXIMUM_CONTENT_BYTES {
        return Err(Error::Invalid("Context byte bound exceeded".to_owned()));
    }
    Ok(())
}

fn content_bytes(items: &[Item]) -> Result<u64> {
    items.iter().try_fold(0_u64, |total, item| {
        let item_bytes = u64::try_from(item.content.len())
            .map_err(|_| Error::Invalid("Context byte bound overflowed".to_owned()))?;
        total
            .checked_add(item_bytes)
            .ok_or_else(|| Error::Invalid("Context byte bound overflowed".to_owned()))
    })
}

fn apply_edits(items: &mut Vec<Item>, edits: Vec<ContextEdit>) -> Result<()> {
    for edit in edits {
        match edit {
            ContextEdit::Append(item) => items.push(item),
            ContextEdit::InsertBefore { target, item } => {
                let index = items
                    .iter()
                    .position(|candidate| candidate.id == target)
                    .ok_or(Error::NotFound(target))?;
                items.insert(index, item);
            }
            ContextEdit::InsertAfter { target, item } => {
                let index = items
                    .iter()
                    .position(|candidate| candidate.id == target)
                    .ok_or(Error::NotFound(target))?;
                items.insert(index + 1, item);
            }
            ContextEdit::Replace { target, content } => {
                let item = items
                    .iter_mut()
                    .find(|candidate| candidate.id == target)
                    .ok_or(Error::NotFound(target))?;
                item.content = content;
            }
            ContextEdit::Delete { target } => {
                let index = items
                    .iter()
                    .position(|candidate| candidate.id == target)
                    .ok_or(Error::NotFound(target))?;
                items.remove(index);
            }
        }
    }
    validate_items(items)
}

#[async_trait]
impl InferenceProvider for MemoryInference {
    async fn models(&self) -> Result<Vec<ModelCapabilities>> {
        Ok(self.models.to_vec())
    }

    async fn create_context(&self, request: CreateContextRequest) -> Result<ContextSnapshot> {
        let model = self.model(&request.model)?;
        validate_items(&request.items)?;
        if content_bytes(&request.items)? > model.maximum_context_bytes {
            return Err(Error::Invalid("model Context bound exceeded".to_owned()));
        }
        let mut state = self.state.write().await;
        if let Some((prior, id)) = state.creates.get(&request.request_id) {
            if prior != &request {
                return Err(Error::Conflict(request.request_id));
            }
            return state
                .contexts
                .get(id)
                .cloned()
                .ok_or_else(|| Error::NotFound(id.clone()));
        }
        let id = context_id();
        let snapshot = ContextSnapshot {
            id: id.clone(),
            lineage: Uuid::new_v4().to_string(),
            parent: None,
            model: request.model.clone(),
            items: request.items.clone().into(),
            retention: Retention::Durable,
            provenance: Provenance::Created,
        };
        state.contexts.insert(id.clone(), snapshot.clone());
        state
            .creates
            .insert(request.request_id.clone(), (request, id));
        Ok(snapshot)
    }

    async fn inspect_context(&self, id: &str) -> Result<ContextSnapshot> {
        self.state
            .read()
            .await
            .contexts
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(id.to_owned()))
    }

    async fn mutate_context(&self, request: MutateContextRequest) -> Result<ContextSnapshot> {
        let mut state = self.state.write().await;
        if let Some((prior, id)) = state.mutations.get(&request.request_id) {
            if prior != &request {
                return Err(Error::Conflict(request.request_id));
            }
            return state
                .contexts
                .get(id)
                .cloned()
                .ok_or_else(|| Error::NotFound(id.clone()));
        }
        let source = state
            .contexts
            .get(&request.source)
            .cloned()
            .ok_or_else(|| Error::NotFound(request.source.clone()))?;
        let mut items = source.items.to_vec();
        let mut lineage = source.lineage.clone();
        let mut model = source.model.clone();
        let provenance = match &request.mutation {
            ContextMutation::Fork => {
                lineage = Uuid::new_v4().to_string();
                Provenance::Forked {
                    source: source.id.clone(),
                }
            }
            ContextMutation::Edit(edits) => {
                apply_edits(&mut items, edits.clone())?;
                Provenance::Derived
            }
            ContextMutation::Truncate(through) => {
                match through {
                    Some(id) => {
                        let index = items
                            .iter()
                            .position(|item| &item.id == id)
                            .ok_or_else(|| Error::NotFound(id.clone()))?;
                        items.truncate(index + 1);
                    }
                    None => items.clear(),
                }
                Provenance::Derived
            }
            ContextMutation::Compact {
                selected,
                replacement,
            } => {
                if selected.is_empty() {
                    return Err(Error::Invalid("compaction selection is empty".to_owned()));
                }
                let selected = selected.iter().collect::<BTreeSet<_>>();
                let first = items
                    .iter()
                    .position(|item| selected.contains(&item.id))
                    .ok_or_else(|| Error::NotFound("compaction selection".to_owned()))?;
                if selected
                    .iter()
                    .any(|id| !items.iter().any(|item| &item.id == *id))
                {
                    return Err(Error::NotFound("compaction selection".to_owned()));
                }
                items.retain(|item| !selected.contains(&item.id));
                items.splice(first..first, replacement.clone());
                validate_items(&items)?;
                Provenance::Derived
            }
            ContextMutation::Transfer { model: target } => {
                self.model(target)?;
                model.clone_from(target);
                lineage = Uuid::new_v4().to_string();
                Provenance::Transferred {
                    source: source.id.clone(),
                    reused_compatible_state: source.model == *target,
                }
            }
        };
        validate_items(&items)?;
        let target = self.model(&model)?;
        if content_bytes(&items)? > target.maximum_context_bytes {
            return Err(Error::Invalid("model Context bound exceeded".to_owned()));
        }
        let id = context_id();
        let snapshot = ContextSnapshot {
            id: id.clone(),
            lineage,
            parent: Some(source.id),
            model,
            items: items.into(),
            retention: Retention::Durable,
            provenance,
        };
        state.contexts.insert(id.clone(), snapshot.clone());
        state
            .mutations
            .insert(request.request_id.clone(), (request, id));
        Ok(snapshot)
    }

    async fn retain_context(&self, id: &str, retention: Retention) -> Result<ContextSnapshot> {
        if matches!(retention, Retention::WarmUntil(0)) {
            return Err(Error::Invalid("warm expiry is invalid".to_owned()));
        }
        let mut state = self.state.write().await;
        let context = state
            .contexts
            .get_mut(id)
            .ok_or_else(|| Error::NotFound(id.to_owned()))?;
        context.retention = retention;
        Ok(context.clone())
    }

    async fn delete_context(&self, id: &str) -> Result<bool> {
        Ok(self.state.write().await.contexts.remove(id).is_some())
    }

    async fn generate(&self, request: GenerateRequest) -> Result<RunSnapshot> {
        if request.settings.maximum_output == 0 {
            return Err(Error::Invalid("maximum output is zero".to_owned()));
        }
        let mut state = self.state.write().await;
        if let Some((prior, id)) = state.generations.get(&request.request_id) {
            if prior != &request {
                return Err(Error::Conflict(request.request_id));
            }
            return state
                .runs
                .get(id)
                .cloned()
                .ok_or_else(|| Error::NotFound(id.clone()));
        }
        let source = state
            .contexts
            .get(&request.context)
            .cloned()
            .ok_or_else(|| Error::NotFound(request.context.clone()))?;
        let model = self.model(&source.model)?;
        if request.settings.maximum_output > model.maximum_output {
            return Err(Error::Unsupported(
                "generation output bound exceeds the model profile".to_owned(),
            ));
        }
        if request.input.kind != ItemKind::User {
            return Err(Error::Invalid(
                "generation input must be user content".to_owned(),
            ));
        }
        let output = request.input.content.clone();
        let output_units = u64::try_from(output.len())
            .map_err(|_| Error::Invalid("output length is excessive".to_owned()))?;
        let terminal = if output_units > request.settings.maximum_output {
            RunTerminal::OutputLimited
        } else {
            RunTerminal::Completed
        };
        let output = if terminal == RunTerminal::OutputLimited {
            let maximum = usize::try_from(request.settings.maximum_output)
                .map_err(|_| Error::Invalid("output limit is excessive".to_owned()))?;
            output.slice(..maximum.min(output.len()))
        } else {
            output
        };
        let id = Uuid::new_v4().to_string();
        let mut items = source.items.to_vec();
        items.push(request.input.clone());
        items.push(Item {
            id: Uuid::new_v4().to_string(),
            kind: ItemKind::Assistant,
            content: output.clone(),
            link: None,
            continuation_profile: None,
        });
        validate_items(&items)?;
        if content_bytes(&items)? > model.maximum_context_bytes {
            return Err(Error::Invalid("model Context bound exceeded".to_owned()));
        }
        let continuation = ContextSnapshot {
            id: context_id(),
            lineage: source.lineage.clone(),
            parent: Some(source.id.clone()),
            model: source.model.clone(),
            items: items.into(),
            retention: Retention::Durable,
            provenance: Provenance::Generated { run: id.clone() },
        };
        let input_bytes = content_bytes(&source.items)?;
        let usage = Usage {
            new_prefill: u64::try_from(request.input.content.len())
                .map_err(|_| Error::Invalid("input work overflowed".to_owned()))?,
            generated_output: u64::try_from(output.len())
                .map_err(|_| Error::Invalid("output work overflowed".to_owned()))?,
            effective_context_reads: input_bytes,
            retained_byte_millis: 0,
        };
        let result = RunResult {
            output: output.clone(),
            context: Some(continuation.clone()),
            terminal,
            receipt: UsageReceipt {
                id: Uuid::new_v4().to_string(),
                model: source.model,
                meter_revision: "memory-v1".to_owned(),
                usage,
            },
        };
        let run = RunSnapshot {
            id: id.clone(),
            input: source.id,
            events: vec![
                RunEvent {
                    sequence: 0,
                    kind: RunEventKind::Output(output),
                },
                RunEvent {
                    sequence: 1,
                    kind: RunEventKind::Usage(usage),
                },
                RunEvent {
                    sequence: 2,
                    kind: RunEventKind::Terminal(terminal),
                },
            ]
            .into(),
            result: Some(result),
        };
        state.contexts.insert(continuation.id.clone(), continuation);
        state.runs.insert(id.clone(), run.clone());
        state
            .generations
            .insert(request.request_id.clone(), (request, id));
        Ok(run)
    }

    async fn inspect_run(&self, id: &str) -> Result<RunSnapshot> {
        self.state
            .read()
            .await
            .runs
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(id.to_owned()))
    }

    async fn cancel_run(&self, id: &str) -> Result<RunSnapshot> {
        self.inspect_run(id).await
    }
}

/// Cloneable customer client over one public provider implementation.
#[derive(Clone)]
pub struct Inference {
    provider: Arc<dyn InferenceProvider>,
}

impl Inference {
    /// Binds a local, customer-hosted, or managed provider.
    #[must_use]
    pub fn new(provider: impl InferenceProvider + 'static) -> Self {
        Self {
            provider: Arc::new(provider),
        }
    }

    /// Creates a deterministic process-local client.
    #[must_use]
    pub fn memory() -> Self {
        Self::new(MemoryInference::default())
    }

    /// Connects to an authenticated managed or customer-hosted Inference endpoint.
    pub async fn connect(endpoint: impl AsRef<str>, bearer_token: impl AsRef<str>) -> Result<Self> {
        Ok(Self::new(
            ManagedInference::connect(endpoint, bearer_token).await?,
        ))
    }

    /// Begins one replay-safe Context creation.
    #[must_use]
    pub fn context(&self, model: impl Into<String>) -> ContextBuilder {
        ContextBuilder {
            provider: Arc::clone(&self.provider),
            request: CreateContextRequest {
                request_id: request_id(),
                model: model.into(),
                items: Vec::new(),
            },
        }
    }

    /// Lists exact provider capabilities.
    pub async fn models(&self) -> Result<Vec<ModelCapabilities>> {
        self.provider.models().await
    }

    /// Attaches to one retained revision without executing work.
    pub async fn attach(&self, id: impl AsRef<str>) -> Result<Context> {
        let snapshot = self.provider.inspect_context(id.as_ref()).await?;
        Ok(Context::new(Arc::clone(&self.provider), snapshot))
    }

    /// Recovers one existing Run without creating a successor.
    pub async fn recover(&self, id: impl AsRef<str>) -> Result<Run> {
        self.provider.inspect_run(id.as_ref()).await?;
        Ok(Run {
            provider: Arc::clone(&self.provider),
            id: id.as_ref().to_owned(),
        })
    }
}

/// Replay-safe Context creation builder.
#[derive(Clone)]
pub struct ContextBuilder {
    provider: Arc<dyn InferenceProvider>,
    request: CreateContextRequest,
}

impl ContextBuilder {
    /// Adds exact instruction content.
    #[must_use]
    pub fn instructions(mut self, text: impl Into<String>) -> Self {
        self.request
            .items
            .push(Item::text(ItemKind::Instruction, text));
        self
    }

    /// Adds one exact typed item.
    #[must_use]
    pub fn item(mut self, item: Item) -> Self {
        self.request.items.push(item);
        self
    }

    /// Returns the pre-dispatch request identity.
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request.request_id
    }

    /// Creates or reconciles this exact request.
    pub async fn create(&self) -> Result<Context> {
        let snapshot = self.provider.create_context(self.request.clone()).await?;
        Ok(Context::new(Arc::clone(&self.provider), snapshot))
    }
}

/// Immutable Context handle.
#[derive(Clone)]
pub struct Context {
    provider: Arc<dyn InferenceProvider>,
    snapshot: ContextSnapshot,
}

impl Context {
    fn new(provider: Arc<dyn InferenceProvider>, snapshot: ContextSnapshot) -> Self {
        Self { provider, snapshot }
    }

    /// Immutable revision identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.snapshot.id
    }

    /// Exact canonical items already observed by this handle.
    #[must_use]
    pub fn items(&self) -> &[Item] {
        &self.snapshot.items
    }

    /// Exact pinned model identity.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.snapshot.model
    }

    /// Revision provenance.
    #[must_use]
    pub const fn provenance(&self) -> &Provenance {
        &self.snapshot.provenance
    }

    async fn mutate(&self, mutation: ContextMutation) -> Result<Self> {
        let snapshot = self
            .provider
            .mutate_context(MutateContextRequest {
                request_id: request_id(),
                source: self.snapshot.id.clone(),
                mutation,
            })
            .await?;
        Ok(Self::new(Arc::clone(&self.provider), snapshot))
    }

    /// Creates an independently retained lineage over this exact revision.
    pub async fn fork(&self) -> Result<Self> {
        self.mutate(ContextMutation::Fork).await
    }

    /// Appends one user item and returns a new immutable revision.
    pub async fn append(&self, text: impl Into<String>) -> Result<Self> {
        self.edit([ContextEdit::Append(Item::text(ItemKind::User, text))])
            .await
    }

    /// Applies exact edits atomically.
    pub async fn edit(&self, edits: impl IntoIterator<Item = ContextEdit>) -> Result<Self> {
        self.mutate(ContextMutation::Edit(edits.into_iter().collect()))
            .await
    }

    /// Retains an exact prefix; `None` means empty.
    pub async fn truncate(&self, through: Option<String>) -> Result<Self> {
        self.mutate(ContextMutation::Truncate(through)).await
    }

    /// Explicitly compacts selected items into caller-supplied replacement content.
    pub async fn compact(&self, selected: Vec<String>, replacement: Vec<Item>) -> Result<Self> {
        self.mutate(ContextMutation::Compact {
            selected,
            replacement,
        })
        .await
    }

    /// Transfers canonical content to another exact model profile.
    pub async fn transfer(&self, model: impl Into<String>) -> Result<Self> {
        self.mutate(ContextMutation::Transfer {
            model: model.into(),
        })
        .await
    }

    /// Changes only this revision's retention reference.
    pub async fn retain(&self, retention: Retention) -> Result<Self> {
        let snapshot = self
            .provider
            .retain_context(&self.snapshot.id, retention)
            .await?;
        Ok(Self::new(Arc::clone(&self.provider), snapshot))
    }

    /// Deletes only this revision's reference.
    pub async fn delete(&self) -> Result<bool> {
        self.provider.delete_context(&self.snapshot.id).await
    }

    /// Admits one generation Run and returns its recoverable identity.
    pub async fn generate(&self, input: impl Into<String>) -> Result<Run> {
        self.generate_with(input, GenerationSettings::default())
            .await
    }

    /// Admits one generation Run with exact capability-checked settings.
    pub async fn generate_with(
        &self,
        input: impl Into<String>,
        settings: GenerationSettings,
    ) -> Result<Run> {
        let snapshot = self
            .provider
            .generate(GenerateRequest {
                request_id: request_id(),
                context: self.snapshot.id.clone(),
                input: Item::text(ItemKind::User, input),
                settings,
            })
            .await?;
        Ok(Run {
            provider: Arc::clone(&self.provider),
            id: snapshot.id,
        })
    }
}

/// Recoverable Run handle. Dropping it never cancels execution.
#[derive(Clone)]
pub struct Run {
    provider: Arc<dyn InferenceProvider>,
    id: RunId,
}

impl Run {
    /// Stable Run identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Replays committed events from an inclusive sequence.
    pub async fn events_from(&self, sequence: u64) -> Result<Vec<RunEvent>> {
        self.provider.run_events(&self.id, sequence).await
    }

    /// Observes the factual result without redispatching generation.
    pub async fn result(&self) -> Result<RunResult> {
        self.provider
            .inspect_run(&self.id)
            .await?
            .result
            .ok_or_else(|| Error::Invalid("Run is not terminal".to_owned()))
    }

    /// Requests cancellation and returns the current factual observation.
    pub async fn cancel(&self) -> Result<RunSnapshot> {
        self.provider.cancel_run(&self.id).await
    }
}

/// Compatibility name used by the assembled in-memory profile.
pub type DeterministicInference = MemoryInference;

/// Runs the public black-box Context and Run conformance core against any provider.
pub async fn conformance(provider: &dyn InferenceProvider) -> std::result::Result<(), String> {
    let create = CreateContextRequest {
        request_id: "conformance-create".to_owned(),
        model: "deterministic".to_owned(),
        items: vec![Item {
            id: "instruction".to_owned(),
            kind: ItemKind::Instruction,
            content: Bytes::from_static(b"exact"),
            link: None,
            continuation_profile: None,
        }],
    };
    let base = provider
        .create_context(create.clone())
        .await
        .map_err(|error| error.to_string())?;
    if provider
        .create_context(create)
        .await
        .map_err(|error| error.to_string())?
        .id
        != base.id
    {
        return Err("Context creation replay changed identity".to_owned());
    }
    let fork = provider
        .mutate_context(MutateContextRequest {
            request_id: "conformance-fork".to_owned(),
            source: base.id.clone(),
            mutation: ContextMutation::Fork,
        })
        .await
        .map_err(|error| error.to_string())?;
    if fork.lineage == base.lineage || fork.items != base.items {
        return Err("fork did not preserve exact content under an independent lineage".to_owned());
    }
    let replacement = Bytes::from_static(b"changed");
    let edited = provider
        .mutate_context(MutateContextRequest {
            request_id: "conformance-edit".to_owned(),
            source: fork.id.clone(),
            mutation: ContextMutation::Edit(vec![ContextEdit::Replace {
                target: "instruction".to_owned(),
                content: replacement.clone(),
            }]),
        })
        .await
        .map_err(|error| error.to_string())?;
    if edited.items[0].content != replacement
        || base.items[0].content != Bytes::from_static(b"exact")
    {
        return Err("item-addressed edit mutated its immutable source".to_owned());
    }
    let compacted = provider
        .mutate_context(MutateContextRequest {
            request_id: "conformance-compact".to_owned(),
            source: edited.id.clone(),
            mutation: ContextMutation::Compact {
                selected: vec!["instruction".to_owned()],
                replacement: vec![Item {
                    id: "summary".to_owned(),
                    kind: ItemKind::Instruction,
                    content: Bytes::from_static(b"summary"),
                    link: None,
                    continuation_profile: None,
                }],
            },
        })
        .await
        .map_err(|error| error.to_string())?;
    if compacted.items.len() != 1 || compacted.items[0].id != "summary" {
        return Err("explicit compaction produced the wrong canonical content".to_owned());
    }
    let transferred = provider
        .mutate_context(MutateContextRequest {
            request_id: "conformance-transfer".to_owned(),
            source: compacted.id.clone(),
            mutation: ContextMutation::Transfer {
                model: "deterministic".to_owned(),
            },
        })
        .await
        .map_err(|error| error.to_string())?;
    if transferred.lineage == compacted.lineage || transferred.items != compacted.items {
        return Err("transfer did not preserve canonical content in a new lineage".to_owned());
    }
    let empty = provider
        .mutate_context(MutateContextRequest {
            request_id: "conformance-truncate".to_owned(),
            source: transferred.id.clone(),
            mutation: ContextMutation::Truncate(None),
        })
        .await
        .map_err(|error| error.to_string())?;
    if !empty.items.is_empty() {
        return Err("exact empty-prefix truncation retained content".to_owned());
    }
    let run = provider
        .generate(GenerateRequest {
            request_id: "conformance-run".to_owned(),
            context: transferred.id.clone(),
            input: Item {
                id: "run-input".to_owned(),
                kind: ItemKind::User,
                content: Bytes::from_static(b"answer"),
                link: None,
                continuation_profile: None,
            },
            settings: GenerationSettings::default(),
        })
        .await
        .map_err(|error| error.to_string())?;
    let result = run.result.ok_or_else(|| "Run did not settle".to_owned())?;
    if run.events.len() != 3
        || run
            .events
            .iter()
            .enumerate()
            .any(|(index, event)| usize::try_from(event.sequence) != Ok(index))
        || result.terminal != RunTerminal::Completed
        || &*result.output != b"answer"
        || result.receipt.usage.generated_output != 6
        || result.context.is_none()
    {
        return Err("Run events, result, or receipt violated conformance".to_owned());
    }
    let replay = provider
        .run_events(&run.id, 1)
        .await
        .map_err(|error| error.to_string())?;
    if replay.len() != 2 || replay[0].sequence != 1 || replay[1].sequence != 2 {
        return Err("inclusive Run event replay violated sequence semantics".to_owned());
    }
    let limited = provider
        .generate(GenerateRequest {
            request_id: "conformance-output-limit".to_owned(),
            context: transferred.id.clone(),
            input: Item {
                id: "limited-input".to_owned(),
                kind: ItemKind::User,
                content: Bytes::from_static(b"bounded"),
                link: None,
                continuation_profile: None,
            },
            settings: GenerationSettings {
                maximum_output: 3,
                seed: None,
            },
        })
        .await
        .map_err(|error| error.to_string())?;
    let limited = limited
        .result
        .ok_or_else(|| "output-limited Run did not settle".to_owned())?;
    if limited.terminal != RunTerminal::OutputLimited
        || limited.output != Bytes::from_static(b"bou")
    {
        return Err("output limit was not enforced exactly".to_owned());
    }
    let conflict = provider
        .create_context(CreateContextRequest {
            request_id: "conformance-create".to_owned(),
            model: "deterministic".to_owned(),
            items: vec![],
        })
        .await;
    if !matches!(conflict, Err(Error::Conflict(_))) {
        return Err("changed idempotent request did not conflict".to_owned());
    }
    let unsupported = provider
        .create_context(CreateContextRequest {
            request_id: "conformance-unsupported".to_owned(),
            model: "unsupported-conformance-model".to_owned(),
            items: vec![],
        })
        .await;
    if !matches!(unsupported, Err(Error::Unsupported(_))) {
        return Err("unsupported model was not rejected explicitly".to_owned());
    }
    provider
        .retain_context(&transferred.id, Retention::WarmUntil(1))
        .await
        .map_err(|error| error.to_string())?;
    provider
        .delete_context(&base.id)
        .await
        .map_err(|error| error.to_string())?;
    if provider.inspect_context(&fork.id).await.is_err() {
        return Err("source deletion invalidated an independently retained fork".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_contexts_fork_edit_transfer_generate_and_recover() -> Result<()> {
        let provider = MemoryInference::new([
            ModelCapabilities {
                model: "deterministic".to_owned(),
                maximum_context_bytes: MAXIMUM_CONTENT_BYTES as u64,
                maximum_output: 65_536,
                features: BTreeSet::new(),
            },
            ModelCapabilities {
                model: "reasoner".to_owned(),
                maximum_context_bytes: MAXIMUM_CONTENT_BYTES as u64,
                maximum_output: 65_536,
                features: BTreeSet::new(),
            },
        ]);
        let inference = Inference::new(provider);
        let create = inference.context("deterministic").instructions("exact");
        let base = create.create().await?;
        assert_eq!(create.create().await?.id(), base.id());
        let first_item = base.items()[0].id.clone();
        let fork = base.fork().await?;
        let edited = fork
            .edit([ContextEdit::replace(first_item, "changed")])
            .await?;
        assert_eq!(&*base.items()[0].content, b"exact");
        assert_eq!(&*edited.items()[0].content, b"changed");
        assert_ne!(base.snapshot.lineage, fork.snapshot.lineage);
        let transferred = edited.transfer("reasoner").await?;
        assert_eq!(transferred.model(), "reasoner");
        let run = transferred.generate("answer").await?;
        let recovered = inference.recover(run.id()).await?;
        assert_eq!(recovered.events_from(1).await?.len(), 2);
        let result = recovered.result().await?;
        assert_eq!(&*result.output, b"answer");
        assert_eq!(result.terminal, RunTerminal::Completed);
        assert_eq!(result.receipt.usage.generated_output, 6);
        base.delete().await?;
        assert_eq!(&*fork.items()[0].content, b"exact");
        Ok(())
    }

    #[tokio::test]
    async fn idempotency_conflicts_and_unsupported_models_are_explicit() -> Result<()> {
        let provider = MemoryInference::default();
        let request = CreateContextRequest {
            request_id: "request".to_owned(),
            model: "deterministic".to_owned(),
            items: vec![Item::text(ItemKind::User, "one")],
        };
        let first = provider.create_context(request.clone()).await?;
        assert_eq!(provider.create_context(request.clone()).await?.id, first.id);
        let mut changed = request;
        changed.items = vec![Item::text(ItemKind::User, "two")];
        assert!(matches!(
            provider.create_context(changed).await,
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            Inference::new(provider).context("unknown").create().await,
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn memory_provider_passes_family_conformance() {
        assert!(conformance(&MemoryInference::default()).await.is_ok());
    }
}
