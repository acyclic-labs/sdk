//! Pinned, journaled tool calls for resumable tasks.

use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::Limits,
    executor::{ExecutionEvent, ExecutionJournal, ToolFailureKind, load_json, stage_json},
    runtime::ToolContext,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry, ToolResult, validate_value},
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineRegistry, MachineStatus, MachineTransition,
        ResumableMachine, WorkflowAdmission, WorkflowJournal,
    },
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// A version-pinned tool implemented as a deterministic serialized state
/// machine. Unlike an imperative `ToolExecutor`, it can suspend at recorded
/// command/effect boundaries and resume after a process restart.
#[derive(Clone)]
pub struct ResumableTool {
    /// Model-visible input/output contract.
    pub definition: ToolDefinition,
    /// Pinned implementation and durable state schema.
    pub machine: Arc<dyn ResumableMachine>,
}

/// Immutable registry of exact resumable tool revisions. It is separate from
/// live executors so a durable tool cannot silently fall back to imperative
/// execution after a restart.
#[derive(Clone, Default)]
pub struct ResumableToolRegistry(BTreeMap<(String, String), ResumableTool>);

impl ResumableToolRegistry {
    /// Registers one version-pinned tool state machine.
    pub fn register(&mut self, tool: ResumableTool) -> Result<()> {
        tool.validate()?;
        let key = (
            tool.definition.name.clone(),
            tool.definition.revision.clone(),
        );
        if self.0.contains_key(&key) {
            return Err(Error::Conflict(format!(
                "resumable tool {}@{} is already registered",
                key.0, key.1,
            )));
        }
        self.0.insert(key, tool);
        Ok(())
    }

    /// Resolves only the exact admitted revision.
    #[must_use]
    pub fn get(&self, name: &str, revision: &str) -> Option<&ResumableTool> {
        self.0.get(&(name.to_owned(), revision.to_owned()))
    }

    /// Iterates registered definitions in deterministic identity order.
    pub fn definitions(&self) -> impl Iterator<Item = &ToolDefinition> {
        self.0.values().map(|tool| &tool.definition)
    }
}

impl ResumableTool {
    /// Rejects a tool whose machine could be silently swapped under the same
    /// visible revision.
    pub fn validate(&self) -> Result<()> {
        self.definition.validate()?;
        let identity = self.machine.identity();
        if identity.name != self.definition.name || identity.version != self.definition.revision {
            return Err(Error::Conflict(
                "resumable tool machine identity differs from its definition".into(),
            ));
        }
        let mut registry = MachineRegistry::default();
        registry.register(self.machine.clone())
    }

    /// Admits one exact invocation before any transition is committed. The
    /// owner journal must retain the admission even when its reply is lost.
    pub async fn open(
        &self,
        context: ToolContext,
        invocation: ToolInvocation,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableToolSession> {
        self.validate()?;
        invocation.validate()?;
        if invocation.name != self.definition.name {
            return Err(Error::Conflict(
                "resumable tool invocation names another definition".into(),
            ));
        }
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id().is_none()
        {
            return Err(Error::Unauthorized(
                "resumable tool requires its admitted task and exact call context".into(),
            ));
        }
        // RuntimeScope intentionally keeps its authority fields private. Use
        // the immutable accessors here so durable admission cannot couple to
        // the scope's representation (and so the host and WASM facades keep
        // the same boundary).
        let scope = context.task().scope();
        let required = format!("tool:call:{}", self.definition.name);
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        context
            .task()
            .authorize_tool(&self.definition, &invocation)
            .await?;
        validate_value(
            &self.definition.input_schema,
            &invocation.arguments,
            "tool input",
        )?;
        let initial = MachineCheckpoint {
            machine: self.machine.identity().clone(),
            revision: 0,
            state: self.machine.initialize(&invocation.arguments)?,
        };
        let mut registry = MachineRegistry::default();
        registry.register(self.machine.clone())?;
        registry.validate_checkpoint(&initial)?;
        let admission = WorkflowAdmission {
            operation_id: invocation.operation_id,
            request_digest: crate::contract::canonical_json_digest(&(
                context.task().durable_task_id(),
                scope.grants(),
                scope.limits(),
                scope.run_limits(),
                scope.extensions(),
                &self.definition,
                &invocation,
                self.machine.identity(),
            ))?,
            initial: initial.clone(),
        };
        admission.validate()?;
        if journal.admit(admission.clone()).await? != admission {
            return Err(Error::Conflict(
                "tool workflow identity belongs to another admission".into(),
            ));
        }
        let host = DurableWorkflowHost::open(registry, initial, journal).await?;
        Ok(ResumableToolSession {
            definition: self.definition.clone(),
            invocation,
            host,
        })
    }
}

/// Owner-bound durable session for one exact tool invocation.
pub struct ResumableToolSession {
    definition: ToolDefinition,
    invocation: ToolInvocation,
    host: DurableWorkflowHost,
}

impl ResumableToolSession {
    /// Returns the immutable admitted invocation.
    #[must_use]
    pub const fn invocation(&self) -> &ToolInvocation {
        &self.invocation
    }

    /// Returns the latest committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        self.host.checkpoint()
    }

    /// Commits one state transition and its complete ref-only command outbox.
    /// A completed result is schema-checked before commit, including replay.
    pub async fn step(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
    ) -> Result<MachineTransition> {
        let output_schema = &self.definition.output_schema;
        self.host
            .step_checked(operation_id, idempotency_key, input, |transition| {
                if let MachineStatus::Completed { value } = &transition.status {
                    validate_value(output_schema, value, "tool output")?;
                }
                Ok(())
            })
            .await
    }
}

/// Executes only after a durable dispatch boundary; replay observes or
/// reconciles that boundary and never calls `execute` a second time.
pub struct DurableToolRunner {
    tools: ToolRegistry,
    journal: Arc<dyn ExecutionJournal>,
    limits: Limits,
}

impl DurableToolRunner {
    /// Pins the registered definitions and the owner-bound ref-only journal.
    pub fn new(tools: ToolRegistry, journal: Arc<dyn ExecutionJournal>) -> Self {
        Self {
            tools,
            journal,
            limits: Limits::default(),
        }
    }

    /// Narrows the maximum artifact and projection bytes before publication.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    /// Executes with the already admitted typed context supplied by a durable task host.
    #[allow(
        clippy::too_many_lines,
        reason = "the tool journal state machine is one ordered reconciliation"
    )]
    pub async fn run_with_context(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: ToolDefinition,
        arguments: Value,
        context: ToolContext,
    ) -> Result<Outcome<Value>> {
        let tool = self
            .tools
            .get_version(&definition.name, &definition.revision)
            .ok_or_else(|| {
                Error::NotFound(format!("tool {}@{}", definition.name, definition.revision))
            })?;
        if tool.definition != definition {
            return Err(Error::Conflict(
                "durable tool definition changed after admission".into(),
            ));
        }
        validate_value(&definition.input_schema, &arguments, "tool input")?;
        let invocation = ToolInvocation {
            operation_id,
            call_id: operation_id.to_string(),
            name: definition.name.clone(),
            arguments,
        };
        invocation.validate()?;
        if context.operation_id() != operation_id
            || context.call_id() != invocation.call_id.as_str()
            || context.task().durable_task_id() != Some(task_id)
        {
            return Err(Error::Unauthorized(
                "tool context is not bound to this admitted effect".into(),
            ));
        }
        let scope = context.task().scope();
        let required = format!("tool:call:{}", definition.name);
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        context
            .task()
            .authorize_tool(&definition, &invocation)
            .await?;
        tool.executor
            .authorize(Some(context.task().scope()), &invocation)?;
        let content_limit = self.limits.file_bytes.min(scope.limits().file_bytes);
        let projection_limit = self.limits.render_bytes.min(scope.limits().render_bytes);
        let digest = *blake3::hash(&crate::contract::canonical_json_bytes(&(
            &task_id,
            &definition,
            &invocation,
            content_limit,
            projection_limit,
        ))?)
        .as_bytes();
        let records = self.journal.replay(operation_id).await?;
        let mut keys = std::collections::HashSet::new();
        let mut started = false;
        let mut dispatched = false;
        let mut completed = None;
        let mut failed = None;
        for (index, record) in records.iter().enumerate() {
            if record.operation_id != operation_id
                || record.sequence != index as u64 + 1
                || !keys.insert(&record.idempotency_key)
            {
                return Err(Error::Conflict(
                    "durable tool journal identity or sequence is invalid".into(),
                ));
            }
            match &record.event {
                ExecutionEvent::Started { request_digest }
                    if !started
                        && !dispatched
                        && completed.is_none()
                        && failed.is_none()
                        && *request_digest == digest =>
                {
                    started = true;
                }
                ExecutionEvent::ToolStarted {
                    step: 0,
                    call_id,
                    invocation: reference,
                } if started
                    && !dispatched
                    && failed.is_none()
                    && *call_id == invocation.call_id =>
                {
                    let pinned: ToolInvocation =
                        load_json(self.journal.as_ref(), reference).await?;
                    if pinned != invocation {
                        return Err(Error::Conflict("durable tool call identity changed".into()));
                    }
                    dispatched = true;
                }
                ExecutionEvent::ToolCompleted {
                    step: 0,
                    call_id,
                    result,
                    projection,
                } if dispatched
                    && completed.is_none()
                    && failed.is_none()
                    && *call_id == invocation.call_id =>
                {
                    completed = Some((result.clone(), projection.clone()));
                }
                ExecutionEvent::ToolFailed {
                    step: 0,
                    call_id,
                    reason,
                } if dispatched
                    && completed.is_none()
                    && failed.is_none()
                    && *call_id == invocation.call_id =>
                {
                    failed = Some(*reason);
                }
                _ => {
                    return Err(Error::Conflict(
                        "durable tool journal has an incompatible history".into(),
                    ));
                }
            }
        }
        if let Some((result, projection)) = completed {
            let result: ToolResult = load_json(self.journal.as_ref(), &result).await?;
            let projection: Value = load_json(self.journal.as_ref(), &projection).await?;
            if crate::contract::canonical_json_bytes(&result)?.len() as u64 > content_limit
                || crate::contract::canonical_json_bytes(&projection)?.len() as u64
                    > projection_limit
            {
                return Err(Error::Conflict(
                    "durable tool history exceeds admitted limits".into(),
                ));
            }
            validate_value(&definition.output_schema, &result.value, "tool output")?;
            return Ok(Outcome::Succeeded(result.value));
        }
        if let Some(reason) = failed {
            return Ok(Outcome::Failed {
                message: reason.message().into(),
            });
        }
        if !started {
            self.journal
                .append(
                    operation_id,
                    "tool:started".into(),
                    ExecutionEvent::Started {
                        request_digest: digest,
                    },
                )
                .await?;
        }
        let mut claimed = false;
        if !dispatched {
            let reference = stage_json(
                self.journal.as_ref(),
                operation_id,
                "tool:invocation",
                &invocation,
            )
            .await?;
            // Re-read the tail after the Started append. Only a linearizable
            // provider may award execution to this process.
            let current = self.journal.replay(operation_id).await?;
            if matches!(current.as_slice(), [record]
                if matches!(&record.event,
                    ExecutionEvent::Started { request_digest } if *request_digest == digest))
            {
                claimed = match self
                    .journal
                    .append_if_tail(
                        operation_id,
                        1,
                        format!("tool:claim:{}", OperationId::new()),
                        ExecutionEvent::ToolStarted {
                            step: 0,
                            call_id: invocation.call_id.clone(),
                            invocation: reference,
                        },
                    )
                    .await
                {
                    Ok(true) => true,
                    Ok(false) => {
                        // Another owner won the dispatch slot. Re-open its
                        // committed history before asking the executor to
                        // reconcile; it may already contain the result.
                        return Box::pin(self.run_with_context(
                            task_id,
                            operation_id,
                            definition,
                            invocation.arguments.clone(),
                            context,
                        ))
                        .await;
                    }
                    Err(Error::Indeterminate(_)) => {
                        return Ok(Outcome::Indeterminate { operation_id });
                    }
                    Err(error) => return Err(error),
                };
            } else if !matches!(current.get(1).map(|record| &record.event),
                Some(ExecutionEvent::ToolStarted { step: 0, call_id, .. })
                    if *call_id == invocation.call_id)
            {
                return Err(Error::Conflict(
                    "durable tool journal changed before dispatch".into(),
                ));
            } else {
                // The first replay preceded a concurrent claim. Re-validate
                // the entire latest ledger, including a possible terminal
                // result, before effect reconciliation.
                return Box::pin(self.run_with_context(
                    task_id,
                    operation_id,
                    definition,
                    invocation.arguments.clone(),
                    context,
                ))
                .await;
            }
        }
        // Keep an authenticated replay context before handing execution its
        // owned context. A lost terminal CAS must observe the winner's record,
        // never repeat the already dispatched effect.
        let replay_context = context.clone();
        let result = if claimed {
            let executed = tool
                .executor
                .execute_with_context(context, invocation.clone())
                .await;
            match executed {
                Ok(result) => result,
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Ok(Outcome::Indeterminate { operation_id });
                }
                Err(_) => {
                    return self
                        .fail(
                            task_id,
                            operation_id,
                            &definition,
                            &invocation,
                            replay_context,
                            ToolFailureKind::ExecutorRejected,
                        )
                        .await;
                }
            }
        } else {
            let observed = tool
                .executor
                .reconcile_with_context(context, invocation.clone())
                .await;
            let reconciled = match observed {
                Ok(result) => result,
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Ok(Outcome::Indeterminate { operation_id });
                }
                Err(error) => return Err(error),
            };
            match reconciled {
                Some(result) => result,
                None => return Ok(Outcome::Indeterminate { operation_id }),
            }
        };
        if validate_value(&definition.output_schema, &result.value, "tool output").is_err() {
            return self
                .fail(
                    task_id,
                    operation_id,
                    &definition,
                    &invocation,
                    replay_context,
                    ToolFailureKind::InvalidOutput,
                )
                .await;
        }
        if crate::contract::canonical_json_bytes(&result)?.len() as u64 > content_limit {
            return self
                .fail(
                    task_id,
                    operation_id,
                    &definition,
                    &invocation,
                    replay_context,
                    ToolFailureKind::PublicationRejected,
                )
                .await;
        }
        let Ok(projection) = tool.projection.project(&invocation, &result) else {
            return self
                .fail(
                    task_id,
                    operation_id,
                    &definition,
                    &invocation,
                    replay_context,
                    ToolFailureKind::ProjectionRejected,
                )
                .await;
        };
        if crate::contract::canonical_json_bytes(&projection)?.len() as u64 > projection_limit {
            return self
                .fail(
                    task_id,
                    operation_id,
                    &definition,
                    &invocation,
                    replay_context,
                    ToolFailureKind::ProjectionRejected,
                )
                .await;
        }
        let result_ref =
            match stage_json(self.journal.as_ref(), operation_id, "tool:result", &result).await {
                Ok(reference) => reference,
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Ok(Outcome::Indeterminate { operation_id });
                }
                Err(_) => {
                    return self
                        .fail(
                            task_id,
                            operation_id,
                            &definition,
                            &invocation,
                            replay_context,
                            ToolFailureKind::PublicationRejected,
                        )
                        .await;
                }
            };
        let projection_ref = match stage_json(
            self.journal.as_ref(),
            operation_id,
            "tool:projection",
            &projection,
        )
        .await
        {
            Ok(reference) => reference,
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                return Ok(Outcome::Indeterminate { operation_id });
            }
            Err(_) => {
                return self
                    .fail(
                        task_id,
                        operation_id,
                        &definition,
                        &invocation,
                        replay_context,
                        ToolFailureKind::PublicationRejected,
                    )
                    .await;
            }
        };
        let published = self
            .journal
            .append_if_tail(
                operation_id,
                2,
                format!("tool:completed:{}", OperationId::new()),
                ExecutionEvent::ToolCompleted {
                    step: 0,
                    call_id: invocation.call_id,
                    result: result_ref,
                    projection: projection_ref,
                },
            )
            .await;
        match published {
            Ok(true) => {}
            Ok(false) => {
                return Box::pin(self.run_with_context(
                    task_id,
                    operation_id,
                    definition,
                    invocation.arguments.clone(),
                    replay_context,
                ))
                .await;
            }
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                return Ok(Outcome::Indeterminate { operation_id });
            }
            Err(error) => return Err(error),
        }
        Ok(Outcome::Succeeded(result.value))
    }

    async fn fail(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        context: ToolContext,
        reason: ToolFailureKind,
    ) -> Result<Outcome<Value>> {
        match self
            .journal
            .append_if_tail(
                operation_id,
                2,
                format!("tool:failed:{}", OperationId::new()),
                ExecutionEvent::ToolFailed {
                    step: 0,
                    call_id: invocation.call_id.clone(),
                    reason,
                },
            )
            .await
        {
            Ok(true) => Ok(Outcome::Failed {
                message: reason.message().into(),
            }),
            Ok(false) => {
                Box::pin(self.run_with_context(
                    task_id,
                    operation_id,
                    definition.clone(),
                    invocation.arguments.clone(),
                    context,
                ))
                .await
            }
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                Ok(Outcome::Indeterminate { operation_id })
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{MachineIdentity, ResumableMachine};
    use serde_json::json;

    struct TestMachine {
        identity: MachineIdentity,
    }

    impl ResumableMachine for TestMachine {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }

        fn state_schema(&self) -> &Value {
            // The test machine is deliberately tiny; the registry tests are
            // about identity binding, not transition semantics.
            static SCHEMA: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| json!({"type": "object"}))
        }

        fn initialize(&self, _input: &Value) -> Result<Value> {
            Ok(json!({}))
        }

        fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
            Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            })
        }
    }

    fn definition(name: &str, revision: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.into(),
            revision: revision.into(),
            description: "test resumable tool".into(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
        }
    }

    fn tool(name: &str, revision: &str, machine_name: &str) -> ResumableTool {
        ResumableTool {
            definition: definition(name, revision),
            machine: Arc::new(TestMachine {
                identity: MachineIdentity {
                    name: machine_name.into(),
                    version: revision.into(),
                    digest: [7; 32],
                },
            }),
        }
    }

    #[test]
    fn resumable_tool_requires_machine_identity_to_match_definition() {
        assert!(matches!(
            tool("test.echo", "1", "test.other").validate(),
            Err(Error::Conflict(message)) if message.contains("machine identity")
        ));
    }

    #[test]
    fn resumable_registry_rejects_ambiguous_exact_revisions() -> Result<()> {
        let mut registry = ResumableToolRegistry::default();
        registry.register(tool("test.echo", "1", "test.echo"))?;
        assert!(matches!(
            registry.register(tool("test.echo", "1", "test.echo")),
            Err(Error::Conflict(message)) if message.contains("already registered")
        ));
        Ok(())
    }
}
