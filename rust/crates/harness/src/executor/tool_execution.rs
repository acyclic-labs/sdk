use super::*;

impl StockExecution<'_> {
    /// Replays or resolves one tool invocation and appends its message.
    #[allow(clippy::too_many_lines, reason = "atomic tool admission and recovery")]
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
            crate::executor::preparation_format(
                format_args!("tool:call:{}", tool.definition.name),
                work,
            )
        })?;
        if !self.stock.tool_scope.grants().contains(&capability) {
            return Err(if stock_is_native(self.io) {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Unauthorized,
                    "scope lacks the tool capability",
                )
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
        let mut tool_context = self.invocation_context(&invocation)?;
        stock_authorize_tool(
            tool,
            tool_context.as_ref(),
            &self.stock.tool_scope,
            &invocation,
            self.io,
        )?;
        crate::executor::with_history_io(self.io, |work| {
            crate::executor::validate_json_schema_value(
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
                step: event_step,
                call_id,
                invocation: existing,
            } if *event_step == step && call_id == &invocation.call_id => Some(existing),
            _ => None,
        });
        if let Some(existing) = started
            && retain_preparation(load_json::<ToolInvocation>(journal, existing, self.io).await?)
                != invocation
        {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "tool call identity is bound to another invocation",
            ));
        }
        if let Some(reason) = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolFailed {
                step: event_step,
                call_id,
                reason,
            } if *event_step == step && call_id == &invocation.call_id => Some(*reason),
            _ => None,
        }) {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                reason.message(),
            ));
        }
        let completed_tool = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                step: event_step,
                call_id,
                result,
                projection,
            } if *event_step == step && call_id == &invocation.call_id => {
                Some((result, projection))
            }
            _ => None,
        });
        if completed_tool.is_some() && started.is_none() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "completed tool has no admitted invocation",
            ));
        }
        if self.stock.reconciliation_only && started.is_none() {
            return Err(Error::Indeterminate(operation_id));
        }
        let content = if let Some(completed) = completed_tool {
            let result =
                retain_preparation(load_json::<ToolResult>(journal, completed.0, self.io).await?);
            crate::executor::with_history_io(self.io, |work| {
                crate::executor::validate_json_schema_value(
                    &tool.definition.output_schema,
                    &result.value,
                    "tool output",
                    work,
                )
            })?;
            let projection =
                retain_preparation(load_json::<Value>(journal, completed.1, self.io).await?);
            tool.definition
                .validate_projection(&projection, self.stock.limits, self.io)?
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
                            let ((approval_operation, request), _request_hold) =
                                tool_approval_request(
                                    task,
                                    self.stock.policy_identity.as_ref().ok_or_else(|| {
                                        Error::Conflict("stock policy identity is missing".into())
                                    })?,
                                    &tool.definition,
                                    &invocation,
                                    (prompt, None),
                                    None,
                                )?;
                            let approval =
                                crate::executor::with_history_io(self.io, |work| crate::durable_host::task_interaction_id(task, approval_operation, work))?;
                            journal
                                .open_interaction(approval, &request, self.io)?
                                .await?;
                            let Some(outcome) =
                                journal.interaction_outcome(approval, self.io)?.await?
                            else {
                                if started.is_some() {
                                    return Err(Error::Indeterminate(invocation.operation_id));
                                }
                                return Ok(ExecutionProgress::Pending(invocation.operation_id));
                            };
                            match check_tool_approval(outcome) {
                                Ok(()) => {}
                                Err(Error::InteractionRejected(reason)) if started.is_none() => {
                                    return Ok(ExecutionProgress::Rejected(reason));
                                }
                                Err(Error::InteractionRejected(_)) => {
                                    return Err(Error::Indeterminate(invocation.operation_id));
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                }
            }
            let claimed = if started.is_some() {
                false
            } else {
                let key = with_history_io(self.io, |work| {
                    crate::executor::preparation_format(
                        format_args!("tool:{step}:{}:invocation", invocation.call_id),
                        work,
                    )
                })?;
                let invocation_ref =
                    stage_json(journal, operation_id, &key, &invocation, self.io).await?;
                let (tail, current) = self
                    .tool_records(journal, operation_id, step, &invocation.call_id)
                    .await?;
                if current.iter().any(|record| {
                    matches!(&record.event,
                    ExecutionEvent::ToolStarted { step: event_step, call_id, .. }
                        if *event_step == step && call_id == &invocation.call_id)
                }) {
                    return Err(Error::Indeterminate(operation_id));
                }
                let key = with_history_io(self.io, |work| {
                    crate::executor::preparation_format(
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
                    invocation: invocation_ref,
                };
                match journal
                    .append_if_tail(operation_id, tail, &key, &event, self.io)?
                    .await
                {
                    Ok(true) => {
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        if let Some(work) = self.native_work() {
                            tool_context
                                .as_ref()
                                .ok_or_else(|| {
                                    stock_boundary(
                                        self.io,
                                        crate::wire::ErrorCode::Unauthorized,
                                        "native tool acceptance has no issued context",
                                    )
                                })?
                                .record_native_tool_acceptance(work)?;
                        }
                        true
                    }
                    Ok(false) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                    Err(error) => return Err(error),
                }
            };
            let result = if claimed {
                let execution =
                    stock_execute_tool(tool, &mut tool_context, &invocation, self.io).await;
                match execution {
                    Ok(result) => result,
                    Err(error) if error.is_native_failure() => return Err(error),
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                    Err(_) => {
                        self.record_tool_failure(
                            journal,
                            operation_id,
                            step,
                            &invocation.call_id,
                            ToolFailureKind::ExecutorRejected,
                        )
                        .await?;
                        return Err(stock_boundary(
                            self.io,
                            crate::wire::ErrorCode::Invalid,
                            ToolFailureKind::ExecutorRejected.message(),
                        ));
                    }
                }
            } else {
                let reconciliation =
                    stock_reconcile_tool(tool, &mut tool_context, &invocation, self.io).await;
                match reconciliation {
                    Ok(Some(result)) => result,
                    Err(error) if error.is_native_failure() => return Err(error),
                    Ok(None) | Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return Err(Error::Indeterminate(operation_id));
                    }
                    Err(_) => {
                        self.record_tool_failure(
                            journal,
                            operation_id,
                            step,
                            &invocation.call_id,
                            ToolFailureKind::ExecutorRejected,
                        )
                        .await?;
                        return Err(stock_boundary(
                            self.io,
                            crate::wire::ErrorCode::Invalid,
                            ToolFailureKind::ExecutorRejected.message(),
                        ));
                    }
                }
            };
            if let Err(error) = crate::executor::with_history_io(self.io, |work| {
                crate::executor::validate_json_schema_value(
                    &tool.definition.output_schema,
                    &result.value,
                    "tool output",
                    work,
                )
            }) {
                if error.is_native_failure() {
                    return Err(error);
                }
                self.record_tool_failure(
                    journal,
                    operation_id,
                    step,
                    &invocation.call_id,
                    ToolFailureKind::InvalidOutput,
                )
                .await?;
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    ToolFailureKind::InvalidOutput.message(),
                ));
            }
            let (projection, content) =
                match stock_project_tool(tool, &invocation, &result, self.stock.limits, self.io) {
                    Ok(projected) => projected,
                    Err(error) => {
                        return stock_tool_rejection(
                            self,
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
            let key = with_history_io(self.io, |work| {
                crate::executor::preparation_format(
                    format_args!("tool:{step}:{}:result", invocation.call_id),
                    work,
                )
            })?;
            let result_ref = stage_json(journal, operation_id, &key, &result, self.io).await;
            let result_ref = match result_ref {
                Ok(reference) => reference,
                Err(error) if error.is_native_failure() => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Err(_) => {
                    self.record_tool_failure(
                        journal,
                        operation_id,
                        step,
                        &invocation.call_id,
                        ToolFailureKind::PublicationRejected,
                    )
                    .await?;
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        ToolFailureKind::PublicationRejected.message(),
                    ));
                }
            };
            let key = with_history_io(self.io, |work| {
                crate::executor::preparation_format(
                    format_args!("tool:{step}:{}:projection", invocation.call_id),
                    work,
                )
            })?;
            let projection_ref =
                stage_json(journal, operation_id, &key, &projection, self.io).await;
            let projection_ref = match projection_ref {
                Ok(reference) => reference,
                Err(error) if error.is_native_failure() => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return Err(Error::Indeterminate(operation_id));
                }
                Err(_) => {
                    self.record_tool_failure(
                        journal,
                        operation_id,
                        step,
                        &invocation.call_id,
                        ToolFailureKind::PublicationRejected,
                    )
                    .await?;
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        ToolFailureKind::PublicationRejected.message(),
                    ));
                }
            };
            let (tail, current) = self
                .tool_records(journal, operation_id, step, &invocation.call_id)
                .await?;
            if current.iter().any(|record| {
                matches!(&record.event,
                ExecutionEvent::ToolCompleted { step: event_step, call_id, .. }
                    | ExecutionEvent::ToolFailed { step: event_step, call_id, .. }
                    if *event_step == step && call_id == &invocation.call_id)
            }) {
                return Err(Error::Indeterminate(operation_id));
            }
            let key = with_history_io(self.io, |work| {
                crate::executor::preparation_format(
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
                result: result_ref,
                projection: projection_ref,
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
        crate::executor::with_history_io(self.io, |work| {
            message.content.validate_limits(self.stock.limits, work)
        })?;
        stock_vec_push(prior_messages, message, self.io)?;
        Ok(ExecutionProgress::Ready(()))
    }
}
