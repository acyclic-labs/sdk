//! Stock workflow command dispatch through existing journaled executors.

use super::{FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress};
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
    tool::{ToolInvocation, validate_value},
    workflow::WorkflowCommand,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Versioned stock model command contract.
pub const MODEL_TASK_COMMAND_KIND: &str = "acyclic.model.v1";
/// Versioned pinned-tool command contract.
pub const TOOL_TASK_COMMAND_KIND: &str = "acyclic.tool.v1";
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
    /// Caller-selected model output token budget; absent adds no Harness ceiling.
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
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
}

/// Stock adapters borrowing the existing runtime. Tool dispatch uses its one
/// registry; model dispatch uses the explicitly selected provider. Each command
/// opens the mandatory task-owned execution journal rather than another ledger.
pub struct FilesystemTaskCommands<'a, P, A, O> {
    runtime: &'a FilesystemTaskRuntime<P, A, O>,
    model: Option<ModelBinding>,
}

impl<'a, P, A, O> FilesystemTaskCommands<'a, P, A, O> {
    pub(super) const fn new(runtime: &'a FilesystemTaskRuntime<P, A, O>) -> Self {
        Self {
            runtime,
            model: None,
        }
    }

    /// Selects the model provider and context pipeline used by stock commands.
    /// Their contracts remain bound by the existing executor request identity.
    #[must_use]
    pub fn with_model(
        mut self,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        context: ContextPipeline,
    ) -> Self {
        self.model = Some(ModelBinding {
            model,
            provider,
            context,
        });
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
        Box::pin(async move {
            let task = context
                .durable_task_id()
                .ok_or_else(|| Error::Unauthorized("command has no admitted task".into()))?;
            let admission = self.runtime.task_host().observe_admission(task).await?;
            if context.scope().grants() != &admission.grants
                || context.scope().limits() != admission.limits
                || context.scope().run_limits() != admission.run_limits
            {
                return Err(Error::Unauthorized(
                    "command scope differs from retained task".into(),
                ));
            }
            if admission.execution.is_some() || admission.run_limits.deadline_epoch_ms.is_some() {
                return Err(Error::Unsupported(
                    "stock command route or deadline runner is not composed".into(),
                ));
            }
            self.runtime
                .task_host()
                .verify_dispatch_owner(task, fence.clone())
                .await?;
            let progress = match command.kind.as_str() {
                MODEL_TASK_COMMAND_KIND => {
                    Box::pin(self.model_command(task, fence.clone(), command.operation_id, payload))
                        .await
                }
                TOOL_TASK_COMMAND_KIND => {
                    self.tool_command(task, context, fence.clone(), command.operation_id, payload)
                        .await
                }
                TASK_ADMIT_COMMAND_KIND | TASK_OBSERVE_COMMAND_KIND => {
                    self.task_command(task, &context, fence.clone(), &command, payload)
                        .await
                }
                MAIL_SEND_TASK_COMMAND_KIND | MAIL_RECEIVE_TASK_COMMAND_KIND => {
                    self.mail_command(task, &context, fence.clone(), &command, payload)
                        .await
                }
                TIMER_TASK_COMMAND_KIND => {
                    let input: TimerTaskCommand =
                        serde_json::from_value(payload).map_err(|error| {
                            Error::Invalid(format!("invalid timer command: {error}"))
                        })?;
                    if self
                        .runtime
                        .task_host()
                        .poll_timer(
                            task,
                            fence.clone(),
                            command.operation_id,
                            input.deadline_unix_ms,
                        )
                        .await?
                    {
                        ready(Outcome::Succeeded(Value::Null))
                    } else {
                        Ok(TaskCommandProgress::Pending)
                    }
                }
                _ => Err(Error::Unsupported(format!(
                    "stock command kind {} is not bound",
                    command.kind
                ))),
            };
            self.runtime
                .task_host()
                .verify_dispatch_owner(task, fence)
                .await?;
            progress
        })
    }
}

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Checks only a retained passive wait. Never dispatches or reconciles a
    /// provider while the task has no execution reservation.
    pub(super) async fn wait_ready(
        &self,
        context: &TaskContext,
        command: &WorkflowCommand,
        payload: Value,
    ) -> Result<bool> {
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("wait has no admitted task".into()))?;
        match command.kind.as_str() {
            TIMER_TASK_COMMAND_KIND => {
                if !context.scope().grants().contains("timer:wait") {
                    return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
                }
                let input: TimerTaskCommand = serde_json::from_value(payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                self.runtime
                    .task_host()
                    .timer_ready(task, command.operation_id, input.deadline_unix_ms)
                    .await
            }
            MAIL_RECEIVE_TASK_COMMAND_KIND => {
                let input: MailReceiveTaskCommand = serde_json::from_value(payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let items = self.runtime.task_host().inbox(task, input.after, 1).await?;
                if let Some(item) = items.first() {
                    context.read_file(&item.payload).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            TASK_OBSERVE_COMMAND_KIND => {
                let input: TaskObserveCommand = serde_json::from_value(payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let child = TaskId::from_bytes(
                    child_operation(task, input.admission_operation).into_bytes(),
                );
                if self
                    .runtime
                    .task_host()
                    .observe_admission(child)
                    .await?
                    .parent
                    != Some(task)
                {
                    return Err(Error::Unauthorized("wait is not for a direct child".into()));
                }
                Ok(
                    matches!(self.runtime.task_host().outcome(child).await?, Some(outcome) if !matches!(outcome, Outcome::Indeterminate { .. })),
                )
            }
            TOOL_TASK_COMMAND_KIND => {
                let input: ToolTaskCommand = serde_json::from_value(payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let definition = self.runtime.tool_definition(&input.name, &input.revision)?;
                let invocation =
                    tool_invocation(context, command.operation_id, &definition, input.arguments)?;
                Box::pin(
                    self.runtime
                        .tool_approval_ready(context, &definition, &invocation),
                )
                .await
            }
            MODEL_TASK_COMMAND_KIND => {
                let input: ModelTaskCommand = serde_json::from_value(payload)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                Box::pin(
                    self.runtime
                        .model_approval_ready(context, command.operation_id, &input),
                )
                .await
            }
            _ => Ok(false),
        }
    }

    async fn task_command(
        &self,
        task: TaskId,
        context: &TaskContext,
        fence: LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        if command.kind == TASK_ADMIT_COMMAND_KIND {
            let input: TaskAdmitCommand = serde_json::from_value(payload)
                .map_err(|error| Error::Invalid(format!("invalid child command: {error}")))?;
            let admission = self.runtime.harness().registered_child_admission(
                child_operation(task, command.operation_id),
                &input.name,
                &input.version,
                input.input,
                task,
                context.scope(),
            )?;
            match self
                .runtime
                .task_host()
                .admit_owned(admission, fence)
                .await?
            {
                crate::Admission::Accepted(child) => ready(Outcome::Succeeded(child)),
                crate::Admission::Rejected { reason } => {
                    ready(Outcome::<Value>::Failed { message: reason })
                }
                crate::Admission::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
            }
        } else {
            let input: TaskObserveCommand = serde_json::from_value(payload)
                .map_err(|error| Error::Invalid(format!("invalid child observation: {error}")))?;
            let child =
                TaskId::from_bytes(child_operation(task, input.admission_operation).into_bytes());
            if self
                .runtime
                .task_host()
                .observe_admission(child)
                .await?
                .parent
                != Some(task)
            {
                return Err(Error::Unauthorized(
                    "observed task is not this command's direct child".into(),
                ));
            }
            match self.runtime.task_host().outcome(child).await? {
                None | Some(Outcome::Indeterminate { .. }) => Ok(TaskCommandProgress::Pending),
                Some(outcome) => ready(outcome),
            }
        }
    }

    async fn mail_command(
        &self,
        task: TaskId,
        context: &TaskContext,
        fence: LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = serde_json::from_value(payload)
                .map_err(|error| Error::Invalid(format!("invalid mail send: {error}")))?;
            self.runtime
                .task_host()
                .send_owned(
                    task,
                    fence,
                    input.recipient,
                    super::task_runtime::execution_operation(task, command.operation_id),
                    input.payload,
                )
                .await?;
            ready(Outcome::Succeeded(Value::Null))
        } else {
            let input: MailReceiveTaskCommand = serde_json::from_value(payload)
                .map_err(|error| Error::Invalid(format!("invalid mail receive: {error}")))?;
            let items = self.runtime.task_host().inbox(task, input.after, 1).await?;
            let Some(item) = items.into_iter().next() else {
                return Ok(TaskCommandProgress::Pending);
            };
            // The immutable first item is stable across a crash before checkpoint
            // publication. Later appends cannot change this selected result.
            context.read_file(&item.payload).await?;
            ready(Outcome::Succeeded(item))
        }
    }

    async fn model_command(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let input: ModelTaskCommand = serde_json::from_value(payload)
            .map_err(|error| Error::Invalid(format!("invalid model command: {error}")))?;
        let binding = self
            .model
            .as_ref()
            .ok_or_else(|| Error::Unsupported("model command provider is not bound".into()))?;
        let execution = self
            .runtime
            .stock_execution(
                task,
                fence,
                turn,
                binding.model.clone(),
                binding.provider.clone(),
                binding.context.clone(),
            )
            .await?;
        let execution = match input.max_output_tokens {
            Some(maximum) => execution.with_max_output_tokens(maximum)?,
            None => execution,
        };
        let progress = execution
            .execute_progress(TurnInput {
                operation_id: execution.operation_id(),
                input: input.input,
                selected_context: input.selected_context,
                max_steps: input.max_steps,
            })
            .await?;
        match progress {
            StockTurnProgress::Ready(output) => ready(Outcome::Succeeded(output)),
            StockTurnProgress::Pending(_) => Ok(TaskCommandProgress::Pending),
            StockTurnProgress::Rejected(reason) => ready(Outcome::<Value>::Failed {
                message: reason.to_string(),
            }),
        }
    }

    async fn tool_command(
        &self,
        task: TaskId,
        context: TaskContext,
        fence: LeaseFence,
        turn: OperationId,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let input: ToolTaskCommand = serde_json::from_value(payload)
            .map_err(|error| Error::Invalid(format!("invalid tool command: {error}")))?;
        let definition = self.runtime.tool_definition(&input.name, &input.revision)?;
        let journal = self.runtime.execution_journal(task, fence, turn).await?;
        let context = self
            .runtime
            .bind_context_interactions(context, task, journal.clone())?;
        let operation = super::task_runtime::execution_operation(task, turn);
        if journal.replay(operation, 0, 1).await?.is_empty() {
            let invocation = tool_invocation(&context, turn, &definition, input.arguments.clone())?;
            match context
                .authorize_tool_pending(&definition, &invocation)
                .await
            {
                Ok(Some(_)) => return Ok(TaskCommandProgress::Pending),
                Ok(None) => {}
                Err(Error::InteractionRejected(reason)) => {
                    return ready(Outcome::<Value>::Failed {
                        message: reason.to_string(),
                    });
                }
                Err(error) => return Err(error),
            }
        }
        let limits = context.scope().limits();
        let runner =
            DurableToolRunner::new(self.runtime.tool_registry(), journal).with_limits(limits)?;
        let outcome = runner
            .run_with_context(
                task,
                operation,
                definition,
                input.arguments,
                ToolContext::new(context, operation, operation.to_string())?,
            )
            .await?;
        match outcome {
            Outcome::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
            outcome => ready(outcome),
        }
    }
}

fn tool_invocation(
    context: &TaskContext,
    turn: OperationId,
    definition: &crate::tool::ToolDefinition,
    arguments: Value,
) -> Result<ToolInvocation> {
    if !context
        .scope()
        .grants()
        .contains(&crate::contract::capability::tool_call(&definition.name))
    {
        return Err(Error::Unauthorized(
            "task scope lacks tool call grant".into(),
        ));
    }
    validate_value(&definition.input_schema, &arguments, "tool input")?;
    let task = context
        .durable_task_id()
        .ok_or_else(|| Error::Unauthorized("tool has no task".into()))?;
    let operation = super::task_runtime::execution_operation(task, turn);
    let invocation = ToolInvocation {
        operation_id: operation,
        call_id: operation.to_string(),
        name: definition.name.clone(),
        arguments,
    };
    invocation.validate()?;
    Ok(invocation)
}

fn ready(value: impl Serialize) -> Result<TaskCommandProgress> {
    serde_json::to_value(value)
        .map(TaskCommandProgress::Ready)
        .map_err(|error| Error::Invalid(format!("invalid command outcome: {error}")))
}

fn child_operation(task: TaskId, command: OperationId) -> OperationId {
    let mut digest = blake3::Hasher::new();
    digest.update(b"harness/v2/task-child\0");
    digest.update(&task.into_bytes());
    digest.update(&command.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
