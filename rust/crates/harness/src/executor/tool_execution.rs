use super::*;
use crate::wire::ErrorCode::{Conflict, Invalid, Unauthorized};

impl StockExecution<'_> {
    pub(super) async fn settle_tool_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: ToolInvocation,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<ExecutionProgress<()>> {
        self.verify_task_context_binding(operation_id)?;
        stock_validate_invocation(&invocation, self.io)?;
        let tool = stock_tool_lookup(&self.stock.tools, &invocation.name, self.io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native_work() {
            tool.executor.supports_native_execution(work)?;
            tool.projection.supports_native_projection(Some(work))?;
        }
        let capability = with_history_io(self.io, |work| {
            preparation_format(format_args!("tool:call:{}", tool.definition.name), work)
        })?;
        if !self.stock.tool_scope.grants().contains(&capability) {
            return Err(if stock_is_native(self.io) {
                stock_boundary(self.io, Unauthorized, "scope lacks the tool capability")
            } else {
                Error::Unauthorized(format!("scope lacks {capability}"))
            });
        }
        stock_json_bound(
            &invocation,
            self.stock
                .limits
                .file_bytes
                .min(self.stock.tool_scope.limits().file_bytes),
            self.io,
        )?;
        let mut context = self.invocation_context(&invocation)?;
        stock_authorize_tool(
            tool,
            context.as_ref(),
            &self.stock.tool_scope,
            &invocation,
            self.io,
        )?;
        with_history_io(self.io, |work| {
            validate_json_schema_value(
                &tool.definition.input_schema,
                &invocation.arguments,
                "tool input",
                work,
            )
        })?;
        let (_, records) = self
            .tool_records(journal, operation_id, step, &invocation.call_id)
            .await?;
        let started = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolStarted {
                step: recorded,
                call_id,
                invocation: reference,
            } if *recorded == step && call_id == &invocation.call_id => Some(reference),
            _ => None,
        });
        if let Some(reference) = started
            && load_json::<ToolInvocation>(journal, reference, self.io)
                .await?
                .0
                != invocation
        {
            return Err(stock_boundary(
                self.io,
                Conflict,
                "tool call identity is bound to another invocation",
            ));
        }
        if let Some(reason) = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolFailed {
                step: recorded,
                call_id,
                reason,
            } if *recorded == step && call_id == &invocation.call_id => Some(*reason),
            _ => None,
        }) {
            return Err(stock_boundary(self.io, Invalid, reason.message()));
        }
        let completed = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                step: recorded,
                call_id,
                result,
                projection,
            } if *recorded == step && call_id == &invocation.call_id => Some((result, projection)),
            _ => None,
        });
        if completed.is_some() && started.is_none() {
            return Err(stock_boundary(
                self.io,
                Invalid,
                "completed tool has no admitted invocation",
            ));
        }
        if self.stock.reconciliation_only && started.is_none() {
            return Err(Error::Indeterminate(operation_id));
        }
        let content = if let Some((result, projection)) = completed {
            let result = load_json::<ToolResult>(journal, result, self.io).await?;
            with_history_io(self.io, |work| {
                validate_json_schema_value(
                    &tool.definition.output_schema,
                    &result.0.value,
                    "tool output",
                    work,
                )
            })?;
            let projection = load_json::<Value>(journal, projection, self.io).await?;
            tool.definition
                .validate_projection(&projection.0, self.stock.limits, self.io)?
        } else {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(progress) = self
                .settle_native_tool_policy(
                    journal,
                    operation_id,
                    &invocation,
                    &tool.definition,
                    started.is_some(),
                )
                .await?
            {
                return Ok(progress);
            }
            if !stock_is_native(self.io) {
                let scope = &self.stock.tool_scope;
                if let Some(policy) = &self.stock.policy {
                    if self.stock.policy_identity.as_ref() != Some(&policy.identity()) {
                        return Err(Error::Conflict("stock policy changed after binding".into()));
                    }
                    let decision = policy.evaluate(&invocation, scope).await?;
                    if self.stock.policy_identity.as_ref() != Some(&policy.identity()) {
                        return Err(Error::Conflict(
                            "stock policy changed during evaluation".into(),
                        ));
                    }
                    match decision {
                        ToolPolicyDecision::Allow => {}
                        ToolPolicyDecision::Deny { reason } => {
                            return Err(Error::Unauthorized(reason));
                        }
                        ToolPolicyDecision::RequireApproval { prompt } => {
                            if !scope.grants().contains(capability::INTERACTION_ROUTE) {
                                return Err(Error::Unauthorized(
                                    "stock tool approval requires interaction:route".into(),
                                ));
                            }
                            let task = self.stock.task.as_ref().map_or(
                                TaskId::from_bytes(operation_id.into_bytes()),
                                |(_, task, _)| *task,
                            );
                            let ((approval_operation, request), _hold) = tool_approval_request(
                                task,
                                self.stock.policy_identity.as_ref().ok_or_else(|| {
                                    Error::Conflict("stock policy identity is missing".into())
                                })?,
                                &tool.definition,
                                &invocation,
                                (prompt, None),
                                None,
                            )?;
                            if let Some(progress) = self
                                .settle_approval(
                                    journal,
                                    operation_id,
                                    approval_operation,
                                    &request,
                                    &invocation,
                                    started.is_some(),
                                )
                                .await?
                            {
                                return Ok(progress);
                            }
                        }
                    }
                }
            }
            let claimed = if started.is_some() {
                false
            } else {
                let key = with_history_io(self.io, |work| {
                    preparation_format(
                        format_args!("tool:{step}:{}:invocation", invocation.call_id),
                        work,
                    )
                })?;
                let reference =
                    stage_json(journal, operation_id, &key, &invocation, self.io).await?;
                let (tail, current) = self
                    .tool_records(journal, operation_id, step, &invocation.call_id)
                    .await?;
                if current.iter().any(|record| {
                    matches!(&record.event,
                    ExecutionEvent::ToolStarted { step: recorded, call_id, .. }
                        if *recorded == step && call_id == &invocation.call_id)
                }) {
                    return Err(Error::Indeterminate(operation_id));
                }
                let key = with_history_io(self.io, |work| {
                    preparation_format(
                        format_args!(
                            "tool:{step}:{}:claim:{}",
                            invocation.call_id,
                            OperationId::new()
                        ),
                        work,
                    )
                })?;
                let event = ExecutionEvent::ToolStarted {
                    step,
                    call_id: stock_string_copy(&invocation.call_id, self.io)?,
                    invocation: reference,
                };
                match journal
                    .append_if_tail(operation_id, tail, &key, &event, self.io)?
                    .await
                {
                    Ok(true) => {
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        if let Some(work) = self.native_work() {
                            context
                                .as_ref()
                                .ok_or_else(|| {
                                    stock_boundary(
                                        self.io,
                                        Unauthorized,
                                        "native tool acceptance has no issued context",
                                    )
                                })?
                                .validate_native_mcp_context(work)?;
                        }
                        true
                    }
                    Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                    Err(error) => return Err(error),
                }
            };
            let execution = if claimed {
                stock_execute_tool(tool, &mut context, &invocation, self.io)
                    .await
                    .map(Some)
            } else {
                stock_reconcile_tool(tool, &mut context, &invocation, self.io).await
            };
            let result = match execution {
                Ok(Some(result)) => result,
                Err(error) if error.is_native_failure() => return Err(error),
                Ok(None) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Err(error) => {
                    return self
                        .reject_tool_call(
                            journal,
                            operation_id,
                            step,
                            &invocation.call_id,
                            ToolFailureKind::ExecutorRejected,
                            error,
                        )
                        .await;
                }
            };
            if let Err(error) = with_history_io(self.io, |work| {
                validate_json_schema_value(
                    &tool.definition.output_schema,
                    &result.value,
                    "tool output",
                    work,
                )
            }) {
                return self
                    .reject_tool_call(
                        journal,
                        operation_id,
                        step,
                        &invocation.call_id,
                        ToolFailureKind::InvalidOutput,
                        error,
                    )
                    .await;
            }
            let (projection, content) =
                match stock_project_tool(tool, &invocation, &result, self.stock.limits, self.io) {
                    Ok(projected) => projected,
                    Err(error) => {
                        return self
                            .reject_tool_call(
                                journal,
                                operation_id,
                                step,
                                &invocation.call_id,
                                ToolFailureKind::ProjectionRejected,
                                error,
                            )
                            .await;
                    }
                };
            let result = self
                .publish_tool_payload(
                    journal,
                    operation_id,
                    step,
                    &invocation.call_id,
                    "result",
                    &result,
                )
                .await?;
            let projection = self
                .publish_tool_payload(
                    journal,
                    operation_id,
                    step,
                    &invocation.call_id,
                    "projection",
                    &projection,
                )
                .await?;
            let (tail, current) = self
                .tool_records(journal, operation_id, step, &invocation.call_id)
                .await?;
            if current.iter().any(|record| {
                matches!(&record.event,
                ExecutionEvent::ToolCompleted { step: recorded, call_id, .. }
                    | ExecutionEvent::ToolFailed { step: recorded, call_id, .. }
                    if *recorded == step && call_id == &invocation.call_id)
            }) {
                return Err(Error::Indeterminate(operation_id));
            }
            let key = with_history_io(self.io, |work| {
                preparation_format(
                    format_args!(
                        "tool:{step}:{}:completed:{}",
                        invocation.call_id,
                        OperationId::new()
                    ),
                    work,
                )
            })?;
            let event = ExecutionEvent::ToolCompleted {
                step,
                call_id: stock_string_copy(&invocation.call_id, self.io)?,
                result,
                projection,
            };
            match journal
                .append_if_tail(operation_id, tail, &key, &event, self.io)?
                .await
            {
                Ok(true) => {}
                Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Err(error) => return Err(error),
            }
            content
        };
        let message = ModelMessage {
            role: ModelRole::Tool,
            content: ModelContent::Part(ModelContentPart::ToolResult {
                call_id: stock_string_copy(&invocation.call_id, self.io)?,
                name: stock_string_copy(&invocation.name, self.io)?,
                content,
            }),
        };
        with_history_io(self.io, |work| {
            message.content.validate_limits(self.stock.limits, work)
        })?;
        stock_vec_push(prior_messages, message, self.io)?;
        Ok(ExecutionProgress::Ready(()))
    }
}
