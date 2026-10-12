//! Stock workflow command dispatch through existing journaled executors.
use crate::runtime::TaskSpawner as _;

use super::{
    FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress, task_runtime::WorkerContext,
};
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

mod dispatch;
mod effects;
mod mail;
mod model;
mod preparation;
mod tool;
use tool::was_dispatched;

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
    /// Exact immutable file, validated against recipient grants and limits.
    pub payload: FileRef,
}

/// Selects the first immutable inbox item after an acknowledged sequence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailReceiveTaskCommand {
    /// Exclusive one-based cursor; zero selects the first message.
    pub after: u64,
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
    /// Retained mailbox selection for this model turn.
    #[serde(default)]
    pub mailbox_operation: Option<OperationId>,
    /// Optional authenticated output binding.
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

/// Versioned direct-child cancellation command.
pub const TASK_CANCEL_COMMAND_KIND: &str = "acyclic.task.cancel.v1";

/// Cancels one retained direct child.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCancelCommand {
    /// Child task identity.
    pub child: TaskId,
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
struct ModelBinding {
    model: Model,
    provider: Arc<dyn ModelProvider>,
    context: ContextPipeline,
    compaction: crate::context::CompactionPolicy,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Stock command adapters over the runtime's existing executors and journal.
pub struct FilesystemTaskCommands<'a, P, A, O> {
    runtime: &'a FilesystemTaskRuntime<P, A, O>,
    model: Option<ModelBinding>,
    effects: Option<Arc<crate::effect_host::ConversationEffectHost<P>>>,
    fork_preparer: Option<Arc<super::FilesystemForkPreparer<A, O>>>,
}
impl<'a, P, A, O> FilesystemTaskCommands<'a, P, A, O> {
    pub(super) const fn new(runtime: &'a FilesystemTaskRuntime<P, A, O>) -> Self {
        Self {
            runtime,
            model: None,
            effects: None,
            fork_preparer: None,
        }
    }
    /// Copies the selected bounded model configuration.
    pub fn with_model(
        mut self,
        model: &Model,
        provider: Arc<dyn ModelProvider>,
        context: &ContextPipeline,
        compaction: &crate::context::CompactionPolicy,
        io: Option<&super::PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                super::task_runtime::observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "model configuration requires original native work",
                )
            })?;
            self.validate_native_configuration_work(work)?;
            self.model = None;
            let before = io.owned_bytes()?;
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.retain_owned(std::mem::size_of::<ModelBinding>() as u64)?;
            let model = crate::model::clone_model(model, Some(io))?;
            let context = context.copy(Some(io))?;
            let compaction = match compaction {
                crate::context::CompactionPolicy::Disabled => {
                    crate::context::CompactionPolicy::Disabled
                }
                crate::context::CompactionPolicy::Threshold(policy) => {
                    let (retention, custody) =
                        crate::context::clone_retention_native(&policy.retention, work)?;
                    io.absorb_native_custody(custody)?;
                    crate::context::CompactionPolicy::Threshold(
                        crate::context::ThresholdCompaction {
                            response_reserve_tokens: policy.response_reserve_tokens,
                            recent_tokens: policy.recent_tokens,
                            retention,
                        },
                    )
                }
            };
            let binding_bytes =
                io.owned_bytes()?
                    .checked_sub(before)
                    .ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "native model configuration custody underflows",
                    })?;
            self.model = Some(ModelBinding {
                model,
                provider,
                context,
                compaction,
                native_custody: Some(io.take_native_custody(binding_bytes)?),
            });
            return Ok(self);
        }
        let _ = io;
        self.model = Some(ModelBinding {
            model: model.clone(),
            provider,
            context: context.clone(),
            compaction: compaction.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        });
        Ok(self)
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn validate_native_configuration_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if self
            .model
            .as_ref()
            .and_then(|binding| binding.native_custody.as_ref())
            .is_some_and(|custody| !custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native command configuration belongs to another original work",
            });
        }
        Ok(())
    }

    /// Binds the existing effect registry.
    pub fn with_effects(
        mut self,
        effects: Arc<crate::effect_host::ConversationEffectHost<P>>,
    ) -> Self {
        self.effects = Some(effects);
        self
    }
    /// Binds the exact fork source.
    pub fn with_fork_preparer(
        mut self,
        preparer: Arc<super::FilesystemForkPreparer<A, O>>,
    ) -> Self {
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
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(self.execute_observed(
            WorkerContext::Ordinary(context),
            fence,
            command,
            payload,
            false,
            None,
        ))
    }

    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(self.execute_observed(
            WorkerContext::Ordinary(context),
            fence,
            command,
            payload,
            true,
            None,
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn execute_with_native_work<'a>(
        &'a self,
        context: Arc<crate::runtime::TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskCommandProgress>> {
        self.validate_native_configuration_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            self.execute_observed(
                WorkerContext::Native(context),
                fence,
                command,
                payload,
                false,
                Some(&io),
            )
            .await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        context: Arc<crate::runtime::TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskCommandProgress>> {
        self.validate_native_configuration_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            self.execute_observed(
                WorkerContext::Native(context),
                fence,
                command,
                payload,
                true,
                Some(&io),
            )
            .await
        })
    }
}

fn rejected(
    reason: crate::InteractionRejection,
    io: Option<&super::PreparationIo>,
) -> Result<TaskCommandProgress> {
    let message = 'message: {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let (message, custody) = io
                .original_native_work()
                .expect("native rejection has original work")
                .display(&reason)?;
            io.absorb_native_custody(custody)?;
            break 'message message;
        }
        break 'message reason.to_string();
    };
    ready(Outcome::<Value>::Failed { message }, io)
}
fn tool_invocation(
    context: &TaskContext,
    operation: OperationId,
    definition: &crate::tool::ToolDefinition,
    arguments: Value,
    io: Option<&super::PreparationIo>,
) -> Result<ToolInvocation> {
    let grant_hold;
    let grant = crate::executor::with_history_io(io, |work| {
        crate::contract::preparation_format(format_args!("tool:call:{}", definition.name), work)
    })?;
    grant_hold = io
        .map(|io| io.guard_owned(grant.capacity() as u64))
        .transpose()?;
    if !context.scope().grants().contains(&grant) {
        return Err(super::task_runtime::observed::boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "task scope lacks tool call grant",
        ));
    }
    drop(grant);
    drop(grant_hold);
    crate::executor::with_history_io(io, |work| {
        crate::contract::validate_json_schema_value(
            &definition.input_schema,
            &arguments,
            "tool input",
            work,
        )
    })?;
    if let Some(io) = io {
        io.retain_owned(std::mem::size_of::<ToolInvocation>() as u64)?;
    }
    let invocation = ToolInvocation {
        operation_id: operation,
        call_id: crate::executor::with_history_io(io, |work| {
            crate::contract::preparation_format(format_args!("{operation}"), work)
        })?,
        name: crate::executor::stock_string_copy(&definition.name, io)?,
        arguments,
    };
    crate::executor::with_history_io(io, |work| {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
    })?;
    Ok(invocation)
}
fn ready(value: impl Serialize, io: Option<&super::PreparationIo>) -> Result<TaskCommandProgress> {
    let Some(io) = io else {
        return serde_json::to_value(value)
            .map(TaskCommandProgress::Ready)
            .map_err(|error| Error::Invalid(format!("invalid command outcome: {error}")));
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = io.original_native_work() {
        let output = super::PreparationIo::from_native_work(native)?;
        let value = output.value_json(&value, acyclic_stream::MAX_RECORD_BYTES as u64)?;
        let retained = output.owned_bytes()?;
        let custody = output.into_native_custody(retained)?;
        return Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody });
    }
    Err(Error::Unsupported(
        "mail result observer requires the original native work".into(),
    ))
}
fn child_operation(
    task: TaskId,
    command: OperationId,
    io: Option<&super::PreparationIo>,
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
