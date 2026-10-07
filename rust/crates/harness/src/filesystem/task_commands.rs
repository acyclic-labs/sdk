//! Stock workflow command dispatch through existing journaled executors.

use super::{FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress};
use crate::{
    Error, OperationId, Outcome, Result, TaskId,
    context::ContextPipeline,
    durable_tool::DurableToolRunner,
    executor::TurnInput,
    model::{Model, ModelContent, ModelProvider},
    projection::SelectedModelContext,
    runtime::{DurableTaskHost, TaskContext, ToolContext},
    scheduler::LeaseFence,
    workflow::WorkflowCommand,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::StreamProvider;
use futures::future::BoxFuture;
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
    P: StreamProvider + Send + Sync + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
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
            if admission.policy.is_some()
                || admission.execution.is_some()
                || admission.run_limits.deadline_epoch_ms.is_some()
            {
                return Err(Error::Unsupported(
                    "stock command policy, route or deadline runner is not composed".into(),
                ));
            }
            self.runtime
                .task_host()
                .verify_dispatch_owner(task, fence.clone())
                .await?;
            let progress = match command.kind.as_str() {
                MODEL_TASK_COMMAND_KIND => {
                    self.model_command(task, fence.clone(), command.operation_id, payload)
                        .await
                }
                TOOL_TASK_COMMAND_KIND => {
                    self.tool_command(task, context, fence.clone(), command.operation_id, payload)
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
    P: StreamProvider + Send + Sync + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
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
        let output = execution
            .execute(TurnInput {
                operation_id: execution.operation_id(),
                input: input.input,
                selected_context: input.selected_context,
                max_steps: input.max_steps,
            })
            .await?;
        ready(Outcome::Succeeded(output))
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
        let operation = super::task_runtime::execution_operation(task, turn);
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

fn ready(value: impl Serialize) -> Result<TaskCommandProgress> {
    serde_json::to_value(value)
        .map(TaskCommandProgress::Ready)
        .map_err(|error| Error::Invalid(format!("invalid command outcome: {error}")))
}
