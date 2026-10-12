//! Stock workflow command dispatch through existing journaled executors.

use super::{FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress, task_runtime::WorkerContext};
use crate::runtime::TaskStateProvider;
use crate::{
    Error, OperationId, Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::FileRef,
    durable_tool::DurableToolRunner,
    executor::{ExecutionJournal, StockTurnProgress, TurnInput},
    model::{Model, ModelContent, ModelProvider},
    projection::SelectedModelContext,
    runtime::{DurableTaskHost, TaskContext, ToolContext},
    scheduler::LeaseFence,
    tool::ToolInvocation,
    workflow::WorkflowCommand,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

mod mail;

macro_rules! command_record {
    ($($record:ty $(=> $schema:ident)?),+ $(,)?) => {
        $(impl super::PreparationRecord for $record {
            $(fn json_value_schema() -> crate::contract::JsonValueSchema {
                crate::contract::JsonValueSchema::$schema
            })?
        })+
    };
}

command_record!(
    TimerTaskCommand, TaskObserveCommand, TaskCancelCommand, CommittedMailResult,
    TaskAdmitCommand => TaskAdmitCommand,
    PreparedTaskAdmitCommand => PreparedTaskAdmitCommand,
    ModelTaskCommand => ModelTaskCommand,
    ToolTaskCommand => ToolTaskCommand,
);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommittedMailResult {
    operation_id: OperationId,
    value: Outcome<crate::scheduler::InboxItem>,
}

/// Versioned stock model command contract.
pub const MODEL_TASK_COMMAND_KIND: &str = "acyclic.model.v1";
/// Versioned pinned-tool command contract.
pub const TOOL_TASK_COMMAND_KIND: &str = "acyclic.tool.v1";
/// Versioned command over the existing conversation effect registry/lifecycle.
pub const EFFECT_TASK_COMMAND_KIND: &str = "acyclic.effect.v1";
/// Versioned nonblocking durable timer command contract.
pub const TIMER_TASK_COMMAND_KIND: &str = "acyclic.timer.v1";

/// A retained timer deadline; scheduling its authenticated wake is caller-owned.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerTaskCommand {
    /// Absolute deadline under the composed host's trusted clock.
    pub deadline_unix_ms: u64,
}

/// Versioned fenced ref-only mail publication command.
pub const MAIL_SEND_TASK_COMMAND_KIND: &str = "acyclic.mail.send.v1";
/// Versioned nonblocking single-item mailbox selection command.
pub const MAIL_RECEIVE_TASK_COMMAND_KIND: &str = "acyclic.mail.receive.v1";

/// Sends a pinned readable file to an admitted recipient. Identity is derived.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailSendTaskCommand {
    /// Recipient's retained task identity.
    pub recipient: TaskId,
    /// Sender-authorized immutable payload; direct transport adds no receiver file grant.
    pub payload: FileRef,
}

/// Selects the first immutable inbox item after an acknowledged sequence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailReceiveTaskCommand {
    /// Exclusive one-based cursor; zero selects the first message.
    pub after: u64,
}

/// Versioned recursive direct-child cancellation intent, not confirmed stop.
pub const TASK_CANCEL_COMMAND_KIND: &str = "acyclic.task.cancel.v1";

/// Selects the retained direct child whose complete subtree is cancelled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCancelCommand {
    /// Exact direct-child identity; never a caller-selected sender.
    pub child: TaskId,
}

/// Versioned child admission through the existing registry and scheduler.
pub const TASK_ADMIT_COMMAND_KIND: &str = "acyclic.task.admit.v1";
/// Versioned nonblocking observation of that command's direct child.
pub const TASK_OBSERVE_COMMAND_KIND: &str = "acyclic.task.observe.v1";

/// Exact registered resumable child with inherited task authority and limits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmitCommand {
    /// Exact logical task name.
    pub name: String,
    /// Exact registered version.
    pub version: String,
    /// Input validated by the registered schema.
    pub input: Value,
}

/// Selects a child derived from a prior admission command in this workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskObserveCommand {
    /// Logical operation ID of the admitting workflow command.
    pub admission_operation: OperationId,
}

/// Versioned creation under the ordinary preparation task's retained request.
pub const FORK_PREPARE_TASK_COMMAND_KIND: &str = "acyclic.fork.prepare.v1";
/// Versioned admission with authenticated exact created-resource proofs.
pub const TASK_ADMIT_PREPARED_COMMAND_KIND: &str = "acyclic.task.admit_prepared.v1";

/// Complete requested child, distinct from its dispatch operation.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedTaskAdmitCommand {
    /// Exact child identity, definitions, input, requested grants and proofs.
    pub admission: crate::runtime::TaskAdmissionRecord,
}
/// Ref-resolved model input. The worker supplies the stable execution identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTaskCommand {
    /// Provider-neutral user input, subject to retained task file grants.
    pub input: ModelContent,
    /// An exact recorded history selection, when available.
    #[serde(default)]
    pub selected_context: Option<SelectedModelContext>,
    /// Model/tool step allowance, bounded by retained task limits.
    pub max_steps: u32,
    /// Caller-selected output ceiling; absent uses the installed compaction policy's ceiling.
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    /// Sole receive command whose result was committed in the immediately prior checkpoint.
    #[serde(default)]
    pub mailbox_operation: Option<OperationId>,
    /// Exact admitted created destination; absent retains the private writer.
    #[serde(default)]
    pub selected_output_volume: Option<crate::conversation::VolumeRef>,
}

/// A pinned revision in the existing tool registry, with schema-defined arguments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolTaskCommand {
    /// Exact logical tool name.
    pub name: String,
    /// Exact registered revision; no live or selected-revision fallback.
    pub revision: String,
    /// Arguments validated against that revision's input schema.
    pub arguments: Value,
}

struct ModelBinding {
    model: Model,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    compaction: crate::context::CompactionPolicy,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
type CachedNativePreparation = (
    Arc<crate::resource_preparation::TaskResourcePreparation>, acyclic_fs::NativeAllocation,
);

#[cfg(not(target_arch = "wasm32"))]
struct NativePreparationCache {
    entries: std::collections::BTreeMap<(TaskId, OperationId), CachedNativePreparation>,
    custody: Option<acyclic_fs::NativeAllocation>,
}

/// Stock adapters borrowing the existing runtime.
pub struct FilesystemTaskCommands<'a, P, A, O> {
    runtime: &'a FilesystemTaskRuntime<P, A, O>,
    model: Option<ModelBinding>,
    effects: Option<Arc<crate::effect_host::ConversationEffectHost<P>>>,
    fork_preparer: Option<Arc<super::FilesystemForkPreparer<A, O>>>,
    #[cfg(not(target_arch = "wasm32"))]
    native_preparations: std::sync::Mutex<NativePreparationCache>,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<'a, P, A, O> FilesystemTaskCommands<'a, P, A, O> {
    pub(super) const fn new(runtime: &'a FilesystemTaskRuntime<P, A, O>) -> Self {
        Self {
            runtime,
            model: None,
            effects: None,
            fork_preparer: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_preparations: std::sync::Mutex::new(NativePreparationCache {
                entries: std::collections::BTreeMap::new(), custody: None,
            }),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        }
    }

    /// Copies the selected model configuration with optional native accounting.
    pub fn with_model(
        mut self, model: &Model, provider: Arc<dyn ModelProvider>,
        context: &ContextPipeline, compaction: &crate::context::CompactionPolicy,
        io: Option<&super::PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| super::task_runtime::observed::boundary(
                Some(io), crate::wire::ErrorCode::Unsupported, "model configuration requires original native work"))?;
            if self.native_custody.as_ref().is_some_and(|custody| !custody.matches_work(work)) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "command configuration belongs to another original work" });
            }
            let before = io.owned_bytes()?;
            let self_bytes = if self.native_custody.is_none() { std::mem::size_of::<Self>() as u64 } else { 0 };
            io.retain_owned(self_bytes)?;
            io.retain_owned(std::mem::size_of::<ModelBinding>() as u64)?;
            let model = crate::model::clone_model(model, Some(io))?;
            let context = context.copy(Some(io))?;
            let compaction = match compaction {
                crate::context::CompactionPolicy::Disabled => crate::context::CompactionPolicy::Disabled,
                crate::context::CompactionPolicy::Threshold(policy) => {
                    let (retention, custody) = crate::context::clone_retention_native(&policy.retention, work)?;
                    io.absorb_native_custody(custody)?;
                    crate::context::CompactionPolicy::Threshold(crate::context::ThresholdCompaction {
                        response_reserve_tokens: policy.response_reserve_tokens, recent_tokens: policy.recent_tokens, retention,
                    })
                }
            };
            let binding_bytes = io.owned_bytes()?.checked_sub(before).and_then(|bytes| bytes.checked_sub(self_bytes))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native model configuration custody underflows" })?;
            self.model = Some(ModelBinding { model, provider, context, compaction,
                native_custody: Some(io.take_native_custody(binding_bytes)?) });
            if self_bytes != 0 { self.native_custody = Some(io.take_native_custody(self_bytes)?); }
            return Ok(self);
        }
        let _ = io;
        self.model = Some(ModelBinding {
            model: model.clone(), provider, context: context.clone(), compaction: compaction.clone(),
            #[cfg(not(target_arch = "wasm32"))] native_custody: None,
        });
        Ok(self)
    }

    /// Explicitly selects effect providers assembled by the embedding host.
    #[must_use]
    pub fn with_effects(
        mut self,
        effects: Arc<crate::effect_host::ConversationEffectHost<P>>,
    ) -> Self {
        self.effects = Some(effects);
        self
    }

    /// Selects one exact parent/source snapshot for the preparation command.
    #[must_use]
    pub fn with_fork_preparer(mut self, preparer: Arc<super::FilesystemForkPreparer<A, O>>) -> Self {
        self.fork_preparer = Some(preparer);
        self
    }
}

impl<P, A, O> TaskCommandHost for FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn execute<'a>(
        &'a self, context: TaskContext, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(self.execute_observed(WorkerContext::Ordinary(context), fence, command, payload, false, None))
    }

    fn reconcile<'a>(
        &'a self, context: TaskContext, fence: LeaseFence,
        command: WorkflowCommand, payload: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(self.execute_observed(WorkerContext::Ordinary(context), fence, command, payload, true, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn execute_with_native_work<'a>(
        &'a self, context: Arc<crate::runtime::NativeTaskContext>, fence: LeaseFence,
        command: WorkflowCommand, payload: Value, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskCommandProgress>> {
        self.validate_native_configuration_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            self.execute_observed(WorkerContext::Native(context), fence, command, payload, false, Some(&io)).await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_work<'a>(
        &'a self, context: Arc<crate::runtime::NativeTaskContext>, fence: LeaseFence,
        command: WorkflowCommand, payload: Value, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskCommandProgress>> {
        self.validate_native_configuration_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            self.execute_observed(WorkerContext::Native(context), fence, command, payload, true, Some(&io)).await
        })
    }
}

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    fn validate_native_configuration_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if self.native_custody.as_ref().is_some_and(|custody| !custody.matches_work(work))
            || self.model.as_ref().and_then(|binding| binding.native_custody.as_ref())
                .is_some_and(|custody| !custody.matches_work(work))
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native command configuration belongs to another original work" });
        }
        let cache = self.native_preparations.lock().map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native preparation cache is poisoned",
        })?;
        if cache.custody.as_ref().is_some_and(|custody| !custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native preparation cache belongs to another original work" });
        }
        Ok(())
    }

    async fn execute_observed(
        &self, context: WorkerContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value, reconciliation_only: bool, io: Option<&super::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use super::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        let task = context.context().durable_task_id().ok_or_else(|| {
            boundary(io, ErrorCode::Unauthorized, "command has no admitted task")
        })?;
        let admission = self.runtime.admission_observed(task, io).await?;
        if context.context().scope().grants() != &admission.grants
            || context.context().scope().limits() != admission.limits
            || context.context().scope().run_limits() != admission.run_limits
        {
            return Err(boundary(io, ErrorCode::Unauthorized, "command scope differs from retained task"));
        }
        if admission.execution.is_some() {
            return Err(boundary(io, ErrorCode::Unsupported, "stock routed execution provider is not composed"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let WorkerContext::Native(shared) = &context {
            if !shared.context().original_task_binding_matches(task, &fence) {
                return Err(boundary(io, ErrorCode::Unauthorized, "command differs from original task binding"));
            }
        }
        self.runtime.owner_observed(task, &fence, reconciliation_only, io).await?;
        let progress = match command.kind.as_str() {
            TOOL_TASK_COMMAND_KIND => self.tool_command(task, context, &fence,
                command.operation_id, payload, reconciliation_only, io).await,
            MODEL_TASK_COMMAND_KIND => {
                let receive = if admission.task.name == "acyclic.stock_turn" {
                    self.stock_mail_operation(task, &fence, command.operation_id, io).await?
                } else { None };
                self.model_command(task, context, &fence, command.operation_id, payload,
                    reconciliation_only, io, receive).await
            }
            EFFECT_TASK_COMMAND_KIND => self.effect_command_with_io(task, &fence, command.operation_id, payload, reconciliation_only, io).await,
            MAIL_SEND_TASK_COMMAND_KIND if reconciliation_only => self.reconcile_mail_with_io(task, &fence, &command, payload, io).await,
            MAIL_SEND_TASK_COMMAND_KIND | MAIL_RECEIVE_TASK_COMMAND_KIND if !reconciliation_only => self.mail_command_with_io(task, &fence, &command, payload, io, admission.task.name == "acyclic.stock_turn").await,
            TASK_ADMIT_COMMAND_KIND | TASK_OBSERVE_COMMAND_KIND => self.task_command_with_io(task, context.context(), &fence, &command, payload, reconciliation_only, io).await,
            FORK_PREPARE_TASK_COMMAND_KIND if !reconciliation_only || io.is_some() => {
                self.prepare_fork_command(task, &context, &fence, &command, payload, admission.input, reconciliation_only, io).await
            }
            TASK_ADMIT_PREPARED_COMMAND_KIND => {
                self.admit_prepared_command(task, &fence, &command, payload, reconciliation_only, io).await
            }
            TASK_CANCEL_COMMAND_KIND if !reconciliation_only => {
                self.cancel_child_command(task, &fence, &command, payload, io).await
            }

            TIMER_TASK_COMMAND_KIND if !reconciliation_only => {
                let input: TimerTaskCommand = mail::decode_command(payload, io)?;
                #[cfg(not(target_arch = "wasm32"))]
                let ready = if let Some(io) = io {
                    let work = io.original_native_work().ok_or(Error::NativeBoundary { code: ErrorCode::Unsupported, reason: "native timer requires original work" })?;
                    self.runtime.task_host().poll_timer(task, &fence, command.operation_id, input.deadline_unix_ms, Some(work)).await?
                } else {
                    self.runtime.task_host().poll_timer(task, &fence, command.operation_id, input.deadline_unix_ms, None).await?
                };
                #[cfg(target_arch = "wasm32")]
                let ready = self.runtime.task_host().poll_timer(task, &fence, command.operation_id, input.deadline_unix_ms, None).await?;
                if ready { self::ready(Outcome::Succeeded(Value::Null), io) }
                else { Ok(TaskCommandProgress::Pending) }
            }
            TIMER_TASK_COMMAND_KIND | MAIL_RECEIVE_TASK_COMMAND_KIND | TASK_CANCEL_COMMAND_KIND if reconciliation_only => mail::reconciled(io),
            _ => Err(if io.is_some() {
                boundary(io, ErrorCode::Unsupported, "stock command kind is not bound")
            } else if reconciliation_only {
                Error::Unsupported(format!("stock command kind {} has no effect reconciliation", command.kind))
            } else {
                Error::Unsupported(format!("stock command kind {} is not bound", command.kind))
            }),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let progress = match progress {
            Err(error) if error.is_native_failure() => return Err(error),
            progress => progress,
        };
        self.runtime.owner_observed(task, &fence, reconciliation_only, io).await?;
        progress
    }

    async fn prepare_fork_command(
        &self, task: TaskId, context: &WorkerContext, fence: &LeaseFence,
        command: &WorkflowCommand, payload: Value, retained_input: Value,
        reconciliation_only: bool, io: Option<&super::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use super::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        let request: crate::resource_preparation::ResourcePreparationRequest =
            mail::decode_command(retained_input, io)?;
        if request.command.as_ref() != Some(command) || request.payload != payload {
            return Err(boundary(io, ErrorCode::Unauthorized,
                "fork command differs from its retained preparation request"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let WorkerContext::Native(shared) = context else {
                return Err(boundary(Some(io), ErrorCode::Unauthorized,
                    "native fork requires its original shared context"));
            };
            let work = io.original_native_work().ok_or_else(|| boundary(
                Some(io), ErrorCode::Unsupported, "native fork requires original work"))?;
            let preparation = if let Some(preparation) = self.cached_native_preparation(
                task, command.operation_id, &request, work,
            )? {
                preparation
            } else if reconciliation_only {
                let Some(preparation) = self.runtime.readonly_fork_preparation_with_native_work(
                    task, shared, &request, work,
                ).await? else { return Ok(TaskCommandProgress::Indeterminate); };
                self.retain_native_preparation(task, command.operation_id, preparation, io, work)?
            } else {
                let request_bytes = (std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>() as u64)
                    .checked_add(request.logical_owned_bytes()?).ok_or_else(|| boundary(
                        Some(io), ErrorCode::Invalid, "fork preparation request ownership overflows"))?;
                let request = (request, io.take_native_custody(request_bytes)?);
                let owner = self.runtime.task_host().journal_owner_with_native_work(task, fence, work).await?
                    .into_native_shared(work)?;
                let mut preparation = owner.resource_preparation_with_native_work(request, work)?.await?;
                let mut header = work.claim(std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>() as u64, u64::from((std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>() as u64) != 0), acyclic_fs::WorkCounters::UNCHARGED)?;
                preparation.native_custody.as_mut().ok_or_else(|| boundary(Some(io), ErrorCode::Unauthorized,
                    "native preparation has no original metadata custody"))?.absorb(&mut header)
                    .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
                let preparation = Arc::new(preparation);
                self.retain_native_preparation(task, command.operation_id, preparation, io, work)?
            };
            let fork: crate::fork::ForkRequest = mail::decode_borrowed_command(&preparation.request().payload, io)?;
            let source = self.runtime.recursive_source().ok_or_else(|| boundary(
                Some(io), ErrorCode::Unsupported, "fork preparation source is not configured"))?;
            let retained = if reconciliation_only {
                source.preparation_for_reconciliation_with_native_work(
                    shared, &preparation, io.cancellation(), work,
                )?.await?
            } else {
                source.preparation_with_native_work(
                    shared, &preparation, io.cancellation(), work,
                )?.await?
            };
            io.absorb_native_custody(retained.1)?;
            if retained.0.request != fork {
                return Err(boundary(Some(io), ErrorCode::Unauthorized,
                    "resolved fork source differs from original request"));
            }
            if reconciliation_only {
                let report = retained.0.preparer.reconcile_for_task_with_native_work(
                    &preparation, &fork, work,
                )?.await?;
                io.absorb_native_custody(report.1)?;
                return match report.0 {
                    Some(_) => mail::reconciled(Some(io)),
                    None => Ok(TaskCommandProgress::Indeterminate),
                };
            }
            let report = retained.0.preparer.prepare_for_task_with_native_work(
                &preparation, &fork, work,
            )?.await?;
            io.absorb_native_custody(report.1)?;
            return ready(Outcome::Succeeded(report.0), Some(io));
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let _guard = super::default_tools::InvocationCancellation(cancellation.clone());
        let owner = self.runtime.task_host().journal_owner(task, fence.clone()).await?
            .with_invocation_cancellation(&cancellation);
        let preparation = Arc::new(owner).resource_preparation(request).await?;
        let fork: crate::fork::ForkRequest = serde_json::from_value(preparation.request().payload.clone())
            .map_err(|cause| Error::Invalid(format!("invalid exact fork request: {cause}")))?;
        let preparer = if let Some(preparer) = &self.fork_preparer {
            preparer.clone()
        } else {
            let source = self.runtime.recursive_source().ok_or_else(|| Error::Unsupported(
                "fork preparation source is not configured".into()))?;
            let retained = source.preparation(context.context(), &preparation, &cancellation).await?;
            if retained.request != fork {
                return Err(Error::Unauthorized("resolved fork source differs from original request".into()));
            }
            retained.preparer
        };
        ready(Outcome::Succeeded(preparer.prepare_for_task(fork, &preparation).await?), None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn cached_native_preparation(
        &self, task: TaskId, operation: OperationId,
        request: &crate::resource_preparation::ResourcePreparationRequest,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Arc<crate::resource_preparation::TaskResourcePreparation>>> {
        let cache = self.native_preparations.lock().map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native preparation cache is poisoned",
        })?;
        let Some(retained) = cache.entries.get(&(task, operation)) else { return Ok(None); };
        if !retained.1.matches_work(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "retained preparation belongs to another original work",
            });
        }
        if retained.0.request() != request {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "retained preparation differs from the original request",
            });
        }
        Ok(Some(Arc::clone(&retained.0)))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn retain_native_preparation(
        &self, task: TaskId, operation: OperationId,
        token: Arc<crate::resource_preparation::TaskResourcePreparation>,
        io: &super::PreparationIo, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::resource_preparation::TaskResourcePreparation>> {
        let mut cache = self.native_preparations.lock().map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native preparation cache is poisoned",
        })?;
        if let Some(retained) = cache.entries.get(&(task, operation)) {
            if !retained.1.matches_work(work) || retained.0.request() != token.request() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "native preparation cache raced a different original request" });
            }
            return Ok(Arc::clone(&retained.0));
        }
        if let Some(custody) = &cache.custody {
            if !custody.matches_work(work) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native preparation cache belongs to another original work" });
            }
        } else if self.native_custody.is_none() {
            let bytes = std::mem::size_of::<NativePreparationCache>() as u64;
            io.retain_owned(bytes)?;
            cache.custody = Some(io.take_native_custody(bytes)?);
        }
        let bytes = std::mem::size_of::<((TaskId, OperationId), CachedNativePreparation)>() as u64;
        io.retain_owned(bytes)?;
        let custody = io.take_native_custody(bytes)?;
        cache.entries.insert((task, operation), (Arc::clone(&token), custody));
        Ok(token)
    }

    async fn admit_prepared_command(
        &self, task: TaskId, fence: &LeaseFence, command: &WorkflowCommand,
        payload: Value, reconciliation_only: bool, io: Option<&super::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use super::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        let request: PreparedTaskAdmitCommand = mail::decode_command(payload, io)?;
        if request.admission.parent != Some(task)
            || request.admission.operation_id.into_bytes() == task.into_bytes()
            || request.admission.operation_id == command.operation_id
        {
            return Err(boundary(io, ErrorCode::Unauthorized,
                "prepared child does not belong to this calling task"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| boundary(
                Some(io), ErrorCode::Unsupported, "native prepared admission requires original work"))?;
            if reconciliation_only {
                let retained = crate::runtime::DurableTaskHost::reconcile_admission_with_native_work(
                    self.runtime.task_host().as_ref(), request.admission.operation_id, work,
                )?.await?;
                io.absorb_native_custody(retained.1)?;
                if let Some((_, record)) = retained.0 && record != request.admission {
                    return Err(boundary(Some(io), ErrorCode::Conflict, "retained child admission differs"));
                }
                return mail::reconciled(Some(io));
            }
            let bytes = (std::mem::size_of::<crate::runtime::TaskAdmissionRecord>() as u64)
                .checked_add(request.admission.logical_owned_bytes()?).ok_or_else(|| boundary(
                    Some(io), ErrorCode::Invalid, "prepared admission custody overflows"))?;
            let admission = (request.admission, io.take_native_custody(bytes)?);
            let fence = super::task_runtime::observed::clone_fence_with_custody(fence, io)?;
            let result = crate::runtime::DurableTaskHost::admit_owned_with_native_work(
                self.runtime.task_host().as_ref(), admission, fence, work,
            )?.await?;
            io.absorb_native_custody(result.1)?;
            return match result.0 {
                crate::Admission::Accepted(child) => ready(Outcome::Succeeded(child), Some(io)),
                crate::Admission::Rejected { reason } => ready(Outcome::<Value>::Failed { message: reason }, Some(io)),
                crate::Admission::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
            };
        }
        if reconciliation_only {
            if let Some((_, retained)) = self.runtime.task_host().reconcile_admission(request.admission.operation_id).await?
                && retained != request.admission
            {
                return Err(Error::Conflict("retained child admission differs".into()));
            }
            return mail::reconciled(io);
        }
        match self.runtime.task_host().admit_owned(request.admission, fence.clone()).await? {
            crate::Admission::Accepted(child) => ready(Outcome::Succeeded(child), None),
            crate::Admission::Rejected { reason } => ready(Outcome::<Value>::Failed { message: reason }, None),
            crate::Admission::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
        }
    }

    async fn cancel_child_command(
        &self, task: TaskId, fence: &LeaseFence, command: &WorkflowCommand,
        payload: Value, io: Option<&super::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        let input: TaskCancelCommand = mail::decode_command(payload, io)?;
        let mut observer = io.map(super::PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let state = self.runtime.task_host().cancel_child_owned(
            task, fence, input.child, command.operation_id, &mut work,
        ).await?;
        ready(Outcome::Succeeded(state), io)
    }

    async fn effect_command_with_io(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: OperationId,
        payload: Value,
        reconciliation_only: bool,
        io: Option<&super::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        let plan: crate::effect_host::TaskEffectPlan = mail::decode_command(payload, io)?;
        let effects = self.effects.as_ref().ok_or_else(|| {
            super::task_runtime::observed::boundary(io, crate::wire::ErrorCode::Unsupported, "effect command provider registry is not selected")
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "effect command requires the original native work",
            })?;
            let owner = self.runtime.task_host().journal_owner_with_native_work(task, fence, work).await?;
            let (status, custody) = if reconciliation_only {
                effects.reconcile_task_effect_with_native_work(&owner, command, &plan, work).await?
            } else {
                effects.run_task_effect_with_native_work(&owner, command, &plan, work).await?
            };
            io.absorb_native_custody(custody)?;
            return match status {
                crate::core::EffectStatus::Succeeded { result } => {
                    ready(Outcome::Succeeded(result), Some(io))
                }
                crate::core::EffectStatus::Failed { message } => {
                    ready(Outcome::<Value>::Failed { message }, Some(io))
                }
                crate::core::EffectStatus::Planned if reconciliation_only => {
                    mail::reconciled(Some(io))
                }
                crate::core::EffectStatus::Planned
                | crate::core::EffectStatus::Dispatched
                | crate::core::EffectStatus::Indeterminate => Ok(TaskCommandProgress::Indeterminate),
            };
        }
        let owner = self.runtime.task_host().journal_owner(task, fence.clone()).await?;
        let status = if reconciliation_only {
            effects.reconcile_task_effect(&owner, command, &plan).await?
        } else {
            effects.run_task_effect(&owner, command, plan).await?
        };
        match status {
            crate::core::EffectStatus::Succeeded { result } => ready(Outcome::Succeeded(result), None),
            crate::core::EffectStatus::Failed { message } => ready(Outcome::<Value>::Failed { message }, None),
            crate::core::EffectStatus::Planned if reconciliation_only => Ok(TaskCommandProgress::Ready(Value::Null)),
            crate::core::EffectStatus::Planned
            | crate::core::EffectStatus::Dispatched
            | crate::core::EffectStatus::Indeterminate => Ok(TaskCommandProgress::Indeterminate),
        }
    }

    pub(super) async fn wait_ready_observed(
        &self,
        context: &TaskContext,
        command: &WorkflowCommand,
        payload: Value,
        io: Option<&super::PreparationIo>,
    ) -> Result<bool> {
        let task = context
            .durable_task_id()
            .ok_or_else(|| super::task_runtime::observed::boundary(io,
                crate::wire::ErrorCode::Unauthorized, "wait has no admitted task"))?;
        match command.kind.as_str() {
            TIMER_TASK_COMMAND_KIND => {
                if !context.scope().grants().contains("timer:wait") {
                    return Err(super::task_runtime::observed::boundary(io,
                        crate::wire::ErrorCode::Unauthorized, "task scope lacks timer:wait"));
                }
                let input: TimerTaskCommand = mail::decode_command(payload, io)?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    let work = io.original_native_work().ok_or_else(|| super::task_runtime::observed::boundary(
                        Some(io), crate::wire::ErrorCode::Unsupported, "native timer wait requires original observer",
                    ))?;
                    return self.runtime.task_host().timer_ready(task, command.operation_id, input.deadline_unix_ms, &mut Some(&mut crate::store::history::native_work_io(work)?)).await;
                }
                self.runtime
                    .task_host()
                    .timer_ready(task, command.operation_id, input.deadline_unix_ms, &mut None)
                    .await
            }
            MAIL_RECEIVE_TASK_COMMAND_KIND => {
                let input: MailReceiveTaskCommand = mail::decode_command(payload, io)?;
                self.wait_mail_with_io(task, input.after, io).await
            }
            TASK_OBSERVE_COMMAND_KIND => {
                let input: TaskObserveCommand = mail::decode_command(payload, io)?;
                let child = TaskId::from_bytes(child_operation(task, input.admission_operation, io)?.into_bytes());
                if self.runtime.admission_observed(child, io).await?.parent != Some(task) {
                    return Err(super::task_runtime::observed::boundary(io,
                        crate::wire::ErrorCode::Unauthorized, "wait is not for a direct child"));
                }
                Ok(matches!(self.runtime.outcome_observed(child, io).await?,
                    Some(outcome) if !matches!(outcome, Outcome::Indeterminate { .. })))
            }
            TOOL_TASK_COMMAND_KIND => {
                let input: ToolTaskCommand = mail::decode_command(payload, io)?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    io.original_native_work().ok_or_else(|| super::task_runtime::observed::boundary(
                        Some(io), crate::wire::ErrorCode::Unsupported, "native tool wait requires original observer"))?;
                }
                let definition = self.runtime.tool_definition(&input.name, &input.revision, io)?;
                let operation = super::task_runtime::execution_operation(task, command.operation_id, io)?;
                let invocation = tool_invocation(context, operation, definition, input.arguments, io)?;
                let ready = self.runtime.tool_approval_ready(context, definition, &invocation, io);
                #[cfg(not(target_arch = "wasm32"))]
                if io.is_some() { return ready.await; }
                Box::pin(ready).await
            }
            MODEL_TASK_COMMAND_KIND => {
                let input: ModelTaskCommand = mail::decode_command(payload, io)?;
                let ready = self.runtime.model_approval_ready(context, command.operation_id, &input, io);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    io.original_native_work().ok_or_else(|| super::task_runtime::observed::boundary(
                        Some(io), crate::wire::ErrorCode::Unsupported, "native model wait requires original observer"))?;
                    return ready.await;
                }
                Box::pin(ready).await
            }
            _ => Ok(false),
        }
    }

    async fn task_command_with_io(
        &self, task: TaskId, context: &TaskContext, fence: &LeaseFence,
        command: &WorkflowCommand, payload: Value, reconciliation_only: bool,
        io: Option<&super::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use super::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        if command.kind == TASK_ADMIT_COMMAND_KIND {
            let input: TaskAdmitCommand = mail::decode_command(payload, io)?;
            let operation = child_operation(task, command.operation_id, io)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io.original_native_work().ok_or(Error::NativeBoundary {
                    code: ErrorCode::Unsupported, reason: "native child requires original work",
                })?;
                let input_bytes = (std::mem::size_of::<Value>() as u64)
                    .checked_add(crate::contract::owned_logical_bytes(&input.input))
                    .ok_or_else(|| boundary(Some(io), ErrorCode::Invalid, "child input ownership overflow"))?;
                let input_custody = io.take_native_custody(input_bytes)?;
                let admission = self.runtime.harness().registered_child_admission_with_native_work(
                    operation, &input.name, &input.version, (input.input, input_custody),
                    task, context.scope(), work,
                )?;
                if reconciliation_only {
                    let (retained, retained_custody) = crate::runtime::DurableTaskHost::reconcile_admission_with_native_work(
                        self.runtime.task_host().as_ref(), operation, work,
                    )?.await?;
                    io.absorb_native_custody(retained_custody)?;
                    if let Some((_, retained)) = retained && retained != admission.0 {
                        return Err(boundary(Some(io), ErrorCode::Conflict, "retained child admission differs"));
                    }
                    return mail::reconciled(Some(io));
                }
                let paid_fence = super::task_runtime::observed::clone_fence_with_custody(fence, io)?;
                let (result, custody) = crate::runtime::DurableTaskHost::admit_owned_with_native_work(
                    self.runtime.task_host().as_ref(), admission, paid_fence, work,
                )?.await?;
                io.absorb_native_custody(custody)?;
                return match result {
                    crate::Admission::Accepted(child) => ready(Outcome::Succeeded(child), Some(io)),
                    crate::Admission::Rejected { reason } => ready(Outcome::<Value>::Failed { message: reason }, Some(io)),
                    crate::Admission::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
                };
            }
            let admission = self.runtime.harness().registered_child_admission(
                operation, &input.name, &input.version, input.input, task, context.scope(),
            )?;
            if reconciliation_only {
                if let Some((_, retained)) = self.runtime.task_host().reconcile_admission(operation).await? && retained != admission {
                    return Err(Error::Conflict("retained child admission differs".into()));
                }
                return mail::reconciled(None);
            }
            match self.runtime.task_host().admit_owned(admission, fence.clone()).await? {
                crate::Admission::Accepted(child) => ready(Outcome::Succeeded(child), None),
                crate::Admission::Rejected { reason } => ready(Outcome::<Value>::Failed { message: reason }, None),
                crate::Admission::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
            }
        } else if reconciliation_only {
            mail::reconciled(io)
        } else {
            let input: TaskObserveCommand = mail::decode_command(payload, io)?;
            let child = TaskId::from_bytes(child_operation(task, input.admission_operation, io)?.into_bytes());
            if self.runtime.admission_observed(child, io).await?.parent != Some(task) {
                return Err(boundary(io, ErrorCode::Unauthorized, "observed task is not this command's direct child"));
            }
            match self.runtime.outcome_observed(child, io).await? {
                None | Some(Outcome::Indeterminate { .. }) => Ok(TaskCommandProgress::Pending),
                Some(outcome) => ready(outcome, io),
            }
        }
    }

    async fn model_command(
        &self, task: TaskId, context: WorkerContext, fence: &LeaseFence,
        turn: OperationId, payload: Value, reconciliation_only: bool,
        io: Option<&super::PreparationIo>, receive: Option<OperationId>,
    ) -> Result<TaskCommandProgress> {
        use super::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        #[cfg(not(target_arch = "wasm32"))]
        if let WorkerContext::Native(_) = &context {
            let io = io.ok_or_else(|| boundary(io, ErrorCode::Unsupported, "native model requires original observer"))?;
            io.original_native_work().ok_or_else(|| boundary(Some(io), ErrorCode::Unsupported, "native model requires original work"))?;
        }
        let mut input: ModelTaskCommand = mail::decode_command(payload, io)?;
        input.mailbox_operation = input.mailbox_operation.or(receive);
        let binding = self.model.as_ref().ok_or_else(|| boundary(
            io, ErrorCode::Unsupported, "model command provider is not bound"))?;
        let (journal, operation) = 'journal: {
            #[cfg(not(target_arch = "wasm32"))]
            if let WorkerContext::Native(_) = &context {
                let work = io.and_then(super::PreparationIo::original_native_work).ok_or_else(|| boundary(
                    io, ErrorCode::Unsupported, "native model requires original work"))?;
                let (journal, operation) = self.runtime.execution_journal_with_native_work(task, fence, turn, work).await?;
                break 'journal (Some(journal), operation);
            }
            let journal = if reconciliation_only {
                Some(self.runtime.execution_journal(task, fence.clone(), turn).await?)
            } else { None };
            break 'journal (journal, super::task_runtime::execution_operation(task, turn, io)?);
        };
        if reconciliation_only && !was_dispatched(journal.as_ref().expect("reconciliation opened its journal").as_ref(),
            operation, MODEL_TASK_COMMAND_KIND, io).await? {
            return mail::reconciled(io);
        }
        if let Some(receive) = input.mailbox_operation {
            let copy = match &context {
                #[cfg(not(target_arch = "wasm32"))]
                WorkerContext::Native(context) => {
                    let io = io.expect("native model has its original observer");
                    let work = io.original_native_work().expect("native model has its original work");
                    let item = self.committed_mail_input(task, fence, turn, receive, context.context(), Some(io)).await?;
                    let copy_operation = crate::durable_host::received_copy_operation(task, receive, Some(io))?;
                    let copy_path = crate::contract::preparation_format(format_args!("mailbox/{receive}/parcel"), Some(io))?;
                    let _path = io.guard_owned(copy_path.capacity() as u64)?;
                    let (call_id, _call_id) = work.display(&copy_operation)?;
                    let publisher = ToolContext::new_with_native_work(Arc::clone(context), copy_operation, &call_id, work)?;
                    let retained = publisher.observe_staged_file_once_with_native_work(copy_operation, work).await?;
                    io.absorb_native_custody(retained.1)?;
                    let copy = match retained.0 {
                        Some(file) => file,
                        None if reconciliation_only => return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Storage,
                            reason: "started steered model has no retained receiver parcel",
                        }),
                        None => {
                            let bytes = context.context().read_received_mail(receive, &item, &mut Some(&mut io.history_io())).await?;
                            self.runtime.owner_observed(task, fence, false, Some(io)).await?;
                            let staged = context.context().stage_file_once_with_native_work(
                                copy_operation, &copy_path, &bytes,
                                item.payload.descriptor().media_type(), item.payload.display_name(), work,
                            ).await?;
                            io.absorb_native_custody(staged.1)?;
                            self.runtime.owner_observed(task, fence, false, Some(io)).await?;
                            staged.0
                        }
                    };
                    if copy.path() != copy_path
                        || copy.descriptor() != item.payload.descriptor()
                        || copy.display_name() != item.payload.display_name()
                    {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                            reason: "receiver parcel receipt differs from original mail" });
                    }
                    copy
                }
                WorkerContext::Ordinary(_) => {
                    let context = self.runtime.task_context(task, fence.clone()).await?;
                    let item = self.committed_mail_input(task, fence, turn, receive, &context, None).await?;
                    let copy_operation = crate::durable_host::received_copy_operation(task, receive, None)?;
                    let copy_path = format!("mailbox/{receive}/parcel");
                    let copy = match context.observe_staged_file_once(copy_operation).await? {
                        Some(file) => file,
                        None if reconciliation_only => return Err(boundary(io, ErrorCode::Storage,
                            "started steered model has no retained receiver parcel")),
                        None => {
                            let bytes = context.read_received_mail(receive, &item, &mut None).await?;
                            self.runtime.task_host().verify_dispatch_owner(task, fence.clone()).await?;
                            let file = context.stage_file_once(copy_operation, &copy_path, &bytes,
                                item.payload.descriptor().media_type(), item.payload.display_name()).await?;
                            self.runtime.task_host().verify_dispatch_owner(task, fence.clone()).await?;
                            file
                        }
                    };
                    if copy.path() != copy_path || copy.descriptor() != item.payload.descriptor()
                        || copy.display_name() != item.payload.display_name() {
                        return Err(boundary(io, ErrorCode::Conflict,
                            "receiver parcel receipt differs from original mail"));
                    }
                    copy
                }
            };
            let mut parts = match input.input {
                ModelContent::Text(text) => {
                    let mut parts = Vec::new();
                    crate::executor::stock_vec_push(&mut parts, crate::model::ModelContentPart::Text { text }, io)?;
                    parts
                }
                ModelContent::Part(part) => {
                    let mut parts = Vec::new();
                    crate::executor::stock_vec_push(&mut parts, part, io)?;
                    parts
                }
                ModelContent::Parts(parts) => parts,
            };
            crate::executor::stock_vec_push(&mut parts, crate::model::ModelContentPart::File {
                file: copy, policy: crate::model::FileProjectionPolicy::BoundedFull,
            }, io)?;
            input.input = ModelContent::Parts(parts);
        }
        let output_volume = input.selected_output_volume;
        let maximum = input.max_output_tokens;
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<TurnInput>().saturating_sub(std::mem::size_of::<ModelTaskCommand>()) as u64)?;
        }
        let input = TurnInput { operation_id: operation, input: input.input,
            selected_context: input.selected_context, max_steps: input.max_steps };
        let progress = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => {
                let io = io.expect("native model has its original observer");
                let work = io.original_native_work().expect("native model has its original work");
                let context = match &output_volume {
                    Some(volume) => context.with_output_volume_with_native_work(volume, work).await?,
                    None => context,
                };
                let journal = journal.as_ref().expect("native model opened its journal");
                let executor = self.runtime.stock_executor_with_native_work(task, fence, operation,
                    &context, &binding.model, Arc::clone(&binding.provider), &binding.context, journal, work)?;
                let executor = executor.with_compaction_policy_with_native_work(&binding.compaction, work)?;
                let executor = match maximum { Some(maximum) => executor.with_max_output_tokens(maximum)?, None => executor };
                let result = if reconciliation_only {
                    executor.execute_reconcile_progress_with_native_context(&context, &input, journal.as_ref(), work)?.await
                } else { executor.execute_progress_with_native_context(&context, &input, journal.as_ref(), work)?.await };
                if reconciliation_only {
                    match result {
                        Ok((progress, custody)) => { drop(progress); drop(custody); }
                        Err(Error::Indeterminate(_)) => {}
                        Err(error) => return Err(error),
                    }
                    return if journal.is_quiescent(Some(io)).await? {
                        mail::reconciled(Some(io))
                    } else { Ok(TaskCommandProgress::Indeterminate) };
                }
                let (progress, custody) = result?;
                io.absorb_native_custody(custody)?;
                progress
            }
            WorkerContext::Ordinary(_) => {
                let execution = self.runtime.stock_execution(task, fence.clone(), turn,
                    binding.model.clone(), Arc::clone(&binding.provider), binding.context.clone(), output_volume.as_ref())
                    .await?.with_compaction_policy(binding.compaction.clone());
                let execution = match maximum { Some(maximum) => execution.with_max_output_tokens(maximum)?, None => execution };
                if let Some(journal) = journal {
                    match execution.reconcile_progress(input).await {
                        Ok(_) | Err(Error::Indeterminate(_)) => {}
                        Err(error) => return Err(error),
                    }
                    return if journal.is_quiescent(io).await? { mail::reconciled(io) }
                        else { Ok(TaskCommandProgress::Indeterminate) };
                }
                execution.execute_progress(input).await?
            }
        };
        match progress {
            StockTurnProgress::Ready(output) => ready(Outcome::Succeeded(output), io),
            StockTurnProgress::Pending(_) => Ok(TaskCommandProgress::Pending),
            StockTurnProgress::Rejected(reason) => rejected(reason, io),
        }
    }

    async fn tool_command(
        &self, task: TaskId, context: WorkerContext, fence: &LeaseFence,
        turn: OperationId, payload: Value, reconciliation_only: bool, io: Option<&super::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use super::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        #[cfg(not(target_arch = "wasm32"))]
        if let WorkerContext::Native(_) = &context {
            let io = io.ok_or_else(|| boundary(io, ErrorCode::Unsupported, "native tool requires original observer"))?;
            io.original_native_work().ok_or_else(|| boundary(Some(io), ErrorCode::Unsupported, "native tool requires original work"))?;
        }
        let input: ToolTaskCommand = mail::decode_command(payload, io)?;
        let definition = self.runtime.tool_definition(&input.name, &input.revision, io)?;
        let (journal, operation) = 'journal: {
            #[cfg(not(target_arch = "wasm32"))]
            if let WorkerContext::Native(_) = &context {
                let work = io.and_then(super::PreparationIo::original_native_work).ok_or_else(|| boundary(
                    io, ErrorCode::Unsupported, "native tool requires original work"))?;
                break 'journal self.runtime.execution_journal_with_native_work(task, fence, turn, work).await?;
            }
            break 'journal (self.runtime.execution_journal(task, fence.clone(), turn).await?,
                super::task_runtime::execution_operation(task, turn, io)?);
        };
        if reconciliation_only && !was_dispatched(journal.as_ref(), operation, TOOL_TASK_COMMAND_KIND, io).await? {
            return mail::reconciled(io);
        }
        let context = match context {
            WorkerContext::Ordinary(context) => WorkerContext::Ordinary(
                self.runtime.bind_context_interactions(context, task, Arc::clone(&journal))?),
            #[cfg(not(target_arch = "wasm32"))]
            context => context,
        };
        let limits = context.context().scope().limits();
        let invocation = tool_invocation(context.context(), operation, definition, input.arguments, io)?;
        let context = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => {
                let work = io.and_then(super::PreparationIo::original_native_work).expect("native tool has its original work");
                let context = ToolContext::new_with_native_work(context, operation, &invocation.call_id, work)?;
                self.runtime.bind_native_tool_interactions(context, task, fence, turn, Arc::clone(&journal), work)?
            }
            WorkerContext::Ordinary(context) => ToolContext::new(context, operation, invocation.call_id.clone())?,
        };
        if !reconciliation_only || io.is_none() {
            let _records;
            let before = io.map(super::PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let records = journal.replay_page(operation, 0, 1, false, io).await?;
            _records = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| boundary(Some(io), ErrorCode::Invalid, "tool replay custody underflows"))?)).transpose()?;
            let empty = records.is_empty();
            drop(records);
            drop(_records);
            if empty {
                let approved = 'approval: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = io.and_then(super::PreparationIo::original_native_work) {
                        break 'approval context.authorize_tool(definition, &invocation, work).await;
                    }
                    break 'approval context.task().authorize_tool(definition, &invocation).await;
                };
                match approved {
                    Ok(Some(_)) => return Ok(TaskCommandProgress::Pending),
                    Ok(None) => {}
                    Err(Error::InteractionRejected(reason)) => return rejected(reason, io),
                    Err(error) => return Err(error),
                }
            }
        }
        let outcome = 'outcome: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io.original_native_work().expect("native tool has its original work");
                let runner = DurableToolRunner::new_with_native_work(self.runtime.tool_registry(), journal, work)?.with_limits(limits)?;
                let (outcome, custody) = if reconciliation_only {
                    runner.reconcile_with_native_context(task, operation, definition, &invocation.arguments, &context, work)?.await?
                } else { runner.run_with_native_context(task, operation, definition, &invocation.arguments, &context, work)?.await? };
                io.absorb_native_custody(custody)?;
                break 'outcome outcome;
            }
            let runner = DurableToolRunner::new(self.runtime.tool_registry().clone(), journal).with_limits(limits)?;
            break 'outcome runner.run_with_context(task, operation, definition.clone(), invocation.arguments, context).await?;
        };
        match outcome {
            Outcome::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
            outcome => ready(outcome, io),
        }
    }

    async fn stock_mail_operation(
        &self, task: TaskId, fence: &LeaseFence, model: OperationId,
        io: Option<&super::PreparationIo>,
    ) -> Result<Option<OperationId>> {
        #[cfg(not(target_arch = "wasm32"))]
        let journal = match io {
            Some(io) => self.runtime.workflow_journal_with_native_work(
                task, fence, io.original_native_work().ok_or(Error::Unsupported("original mail work is absent".into()))?,
            ).await?,
            None => self.runtime.workflow_journal(task, fence.clone()).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let journal = self.runtime.workflow_journal(task, fence.clone()).await?;
        let record = journal.observe_latest_record(io).await?
            .ok_or_else(|| Error::Unauthorized("stock model has no selection checkpoint".into()))?;
        if !record.transition.commands.iter().any(|command| command.operation_id == model
            && command.kind == MODEL_TASK_COMMAND_KIND) {
            return Err(Error::Unauthorized("stock model is not in the selected outbox".into()));
        }
        if record.next.state["mail"].is_null() { return Ok(None); }
        OperationId::deserialize(&record.next.state["receive"]["operation_id"])
            .map(Some).map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn committed_mail_input(
        &self, task: TaskId, fence: &LeaseFence, model_operation: OperationId,
        receive_operation: OperationId, context: &TaskContext, io: Option<&super::PreparationIo>,
    ) -> Result<crate::scheduler::InboxItem> {
        use super::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        #[cfg(not(target_arch = "wasm32"))]
        let journal = match io.and_then(super::PreparationIo::original_native_work) {
            Some(work) => self.runtime.workflow_journal_with_native_work(task, fence, work).await?,
            None => self.runtime.workflow_journal(task, fence.clone()).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let journal = self.runtime.workflow_journal(task, fence.clone()).await?;
        let latest = journal.observe_latest_record(io).await?.ok_or_else(|| boundary(
            io, ErrorCode::Unauthorized, "model steering has no committed checkpoint"))?;
        if !latest.transition.commands.iter().any(|command|
            command.operation_id == model_operation && command.kind == MODEL_TASK_COMMAND_KIND)
        {
            return Err(boundary(io, ErrorCode::Unauthorized, "model is not in the committed steering outbox"));
        }
        let previous = journal.observe_transition(latest.prior.revision, io).await?;
        let [receive] = previous.transition.commands.as_slice() else {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering requires one committed receive"));
        };
        if receive.operation_id != receive_operation || receive.kind != MAIL_RECEIVE_TASK_COMMAND_KIND
            || previous.next != latest.prior
        {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering checkpoint does not consume the receive"));
        }
        let commands = latest.input["commands"].as_array().ok_or_else(|| boundary(
            io, ErrorCode::Unauthorized, "steering checkpoint has no command result"))?;
        let [result] = commands.as_slice() else {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering checkpoint consumes more than one item"));
        };
        let result: CommittedMailResult = match io {
            Some(io) => crate::filesystem::retain_decoded(io.value_decode_preparation(result, ErrorCode::Unauthorized)?),
            None => serde_json::from_value(result.clone())
                .map_err(|cause| Error::Unauthorized(format!("invalid steering receipt: {cause}")))?,
        };
        if result.operation_id != receive_operation {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering result changed its operation"));
        }
        let Outcome::Succeeded(item) = result.value else {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering receive did not succeed"));
        };
        if item.task_id != task || item.sequence == 0 {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering item belongs to another recipient"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let bytes = match io.and_then(super::PreparationIo::original_native_work) {
            Some(work) => context.read_file_with_native_work(&receive.payload, work).await?,
            None => context.read_file(&receive.payload).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let bytes = context.read_file(&receive.payload).await?;
        let selection: MailReceiveTaskCommand = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<MailReceiveTaskCommand>, &bytes, ErrorCode::Storage)?),
            None => crate::contract::json_from_slice(&bytes).map_err(|error| Error::Storage(error.to_string()))?,
        };
        if selection.after.checked_add(1) != Some(item.sequence) {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering receipt changed the selected cursor"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let retained = match io.and_then(super::PreparationIo::original_native_work) {
            Some(work) => {
                let (items, custody) = self.runtime.task_host().inbox_with_native_work(task, item.sequence - 1, 1, work).await?;
                io.expect("native inbox observer").absorb_native_custody(custody)?;
                items
            },
            None => self.runtime.task_host().inbox(task, item.sequence - 1, 1).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let retained = self.runtime.task_host().inbox(task, item.sequence - 1, 1).await?;
        if retained.as_slice() != std::slice::from_ref(&item) {
            return Err(boundary(io, ErrorCode::Conflict, "steering item differs from the original mail"));
        }
        Ok(item)
    }


}

async fn was_dispatched(
    journal: &dyn ExecutionJournal, operation: OperationId, kind: &str, io: Option<&super::PreparationIo>,
) -> Result<bool> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| super::task_runtime::observed::boundary(
            Some(io), crate::wire::ErrorCode::Unsupported, "dispatch replay requires original work"))?;
        if !journal.supports_native_execution(work)? {
            return Err(super::task_runtime::observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported,
                "native execution journal is unavailable"));
        }
    }
    let dispatched = std::sync::atomic::AtomicBool::new(false);
    crate::executor::replay_execution(journal, operation, 0, |event| {
        if matches!((kind, event),
            (MODEL_TASK_COMMAND_KIND, crate::executor::ExecutionEvent::ModelStarted { .. })
            | (TOOL_TASK_COMMAND_KIND, crate::executor::ExecutionEvent::ToolStarted { .. })) {
            dispatched.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        false
    }, io).await?;
    Ok(dispatched.load(std::sync::atomic::Ordering::Relaxed))
}

fn rejected(reason: crate::InteractionRejection, io: Option<&super::PreparationIo>) -> Result<TaskCommandProgress> {
    let message = 'message: {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let (message, custody) = io.original_native_work().expect("native rejection has original work").display(&reason)?;
            io.absorb_native_custody(custody)?;
            break 'message message;
        }
        break 'message reason.to_string();
    };
    ready(Outcome::<Value>::Failed { message }, io)
}

fn tool_invocation(
    context: &TaskContext, operation: OperationId,
    definition: &crate::tool::ToolDefinition, arguments: Value,
    io: Option<&super::PreparationIo>,
) -> Result<ToolInvocation> {
    let grant_hold;
    let grant = crate::contract::preparation_format(format_args!("tool:call:{}", definition.name), io)?;
    grant_hold = io.map(|io| io.guard_owned(grant.capacity() as u64)).transpose()?;
    if !context.scope().grants().contains(&grant) {
        return Err(super::task_runtime::observed::boundary(io,
            crate::wire::ErrorCode::Unauthorized, "task scope lacks tool call grant"));
    }
    drop(grant);
    drop(grant_hold);
    crate::contract::validate_json_schema_value(&definition.input_schema, &arguments, "tool input", io)?;
    if let Some(io) = io { io.retain_owned(std::mem::size_of::<ToolInvocation>() as u64)?; }
    let invocation = ToolInvocation {
        operation_id: operation,
        call_id: crate::contract::preparation_format(format_args!("{operation}"), io)?,
        name: super::fork_preparer_io::clone_text(&definition.name, io)?,
        arguments,
    };
    crate::executor::with_history_io(io, |work|
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work))?;
    Ok(invocation)
}

fn ready(value: impl Serialize, io: Option<&super::PreparationIo>) -> Result<TaskCommandProgress> {
    let Some(io) = io else {
        return serde_json::to_value(value).map(TaskCommandProgress::Ready)
            .map_err(|error| Error::Invalid(format!("invalid command outcome: {error}")));
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = io.original_native_work() {
        let output = super::PreparationIo::from_native_work(native)?;
        let encoded_hold;
        let encoded = output.encode(&value, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
        encoded_hold = output.guard_bytes(&encoded)?;
        let value = crate::filesystem::retain_decoded(output.decode(std::marker::PhantomData::<Value>, &encoded, crate::wire::ErrorCode::Storage)?);
        drop(encoded);
        drop(encoded_hold);
        let retained = output.owned_bytes()?;
        let custody = output.into_native_custody(retained)?;
        return Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody });
    }
    Err(Error::Unsupported("mail result observer requires the original native work".into()))
}


fn child_operation(
    task: TaskId, command: OperationId, io: Option<&super::PreparationIo>,
) -> Result<OperationId> {
    if let Some(io) = io {
        let hashed = acyclic_fs::WorkCounters {
            bytes_hashed: b"harness/v2/task-child\0".len() as u64 + 32,
            ..Default::default()
        };
        io.preflight(hashed)?;
        io.charge(hashed)?;
    }
    let mut digest = blake3::Hasher::new();
    digest.update(b"harness/v2/task-child\0");
    digest.update(&task.into_bytes());
    digest.update(&command.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(bytes))
}
