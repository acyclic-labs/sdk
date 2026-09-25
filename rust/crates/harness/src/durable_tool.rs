//! Pinned, journaled tool calls for resumable tasks.

use crate::{
    Error, OperationId, Outcome, Result, TaskId,
    executor::{ExecutionEvent, ExecutionJournal, ToolFailureKind, load_json, stage_json},
    runtime::ToolContext,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry, ToolResult, validate_value},
};
use serde_json::Value;
use std::sync::Arc;

/// Executes only after a durable dispatch boundary; replay observes or
/// reconciles that boundary and never calls `execute` a second time.
pub struct DurableToolRunner {
    tools: ToolRegistry,
    journal: Arc<dyn ExecutionJournal>,
}

impl DurableToolRunner {
    /// Pins the registered definitions and the owner-bound ref-only journal.
    pub fn new(tools: ToolRegistry, journal: Arc<dyn ExecutionJournal>) -> Self {
        Self { tools, journal }
    }

    /// Returns a terminal value or an explicit unresolved outcome by stable ID.
    pub async fn run(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: ToolDefinition,
        arguments: Value,
    ) -> Result<Outcome<Value>> {
        self.run_with_context(task_id, operation_id, definition, arguments, None)
            .await
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
        context: Option<ToolContext>,
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
            call_id: operation_id.to_string(),
            name: definition.name.clone(),
            arguments,
        };
        invocation.validate()?;
        let digest = *blake3::hash(&crate::contract::canonical_json_bytes(&(
            &task_id,
            &definition,
            &invocation,
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
            let _: Value = load_json(self.journal.as_ref(), &projection).await?;
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
                    Ok(claimed) => claimed,
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
            }
        }
        let result = if claimed {
            let executed = match context {
                Some(context) => {
                    tool.executor
                        .execute_with_context(context, invocation.clone())
                        .await
                }
                None => tool.executor.execute(invocation.clone()).await,
            };
            match executed {
                Ok(result) => result,
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Ok(Outcome::Indeterminate { operation_id });
                }
                Err(_) => {
                    return self
                        .fail(operation_id, &invocation, ToolFailureKind::ExecutorRejected)
                        .await;
                }
            }
        } else {
            let reconciled = match tool.executor.reconcile(invocation.clone()).await {
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
                .fail(operation_id, &invocation, ToolFailureKind::InvalidOutput)
                .await;
        }
        let Ok(projection) = tool.projection.project(&invocation, &result) else {
            return self
                .fail(
                    operation_id,
                    &invocation,
                    ToolFailureKind::ProjectionRejected,
                )
                .await;
        };
        let result_ref =
            match stage_json(self.journal.as_ref(), operation_id, "tool:result", &result).await {
                Ok(reference) => reference,
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Ok(Outcome::Indeterminate { operation_id });
                }
                Err(_) => {
                    return self
                        .fail(
                            operation_id,
                            &invocation,
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
                        operation_id,
                        &invocation,
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
            Ok(false) => return Ok(Outcome::Indeterminate { operation_id }),
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                return Ok(Outcome::Indeterminate { operation_id });
            }
            Err(error) => return Err(error),
        }
        Ok(Outcome::Succeeded(result.value))
    }

    async fn fail(
        &self,
        operation_id: OperationId,
        invocation: &ToolInvocation,
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
            Ok(false) => Ok(Outcome::Indeterminate { operation_id }),
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                Ok(Outcome::Indeterminate { operation_id })
            }
            Err(error) => Err(error),
        }
    }
}
