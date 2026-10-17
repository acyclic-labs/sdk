use super::*;
use crate::wire::ErrorCode::{Conflict, Invalid, Unauthorized};

/// A model tool call refused before admission. It is the call's model-visible result, never an
/// error: it reaches no journal, executor or task outcome, and replay recomputes it from the
/// bound composition.
enum ToolCallRejection {
    /// The composition binds no granted tool by this name.
    Unavailable,
    /// The arguments violate the tool's pinned input schema.
    InvalidInput(Error),
    /// The bound policy denied the call; the model can adapt.
    Refused(Error),
}

/// Why the bound policy stopped a call before dispatch.
enum PolicyStop {
    /// Approval is pending or was rejected; the turn's progress reports it.
    Progress(ExecutionProgress<()>),
    /// The policy denied the call; the model receives the denial as the call's result.
    Refused(Error),
}

/// A recorded tool failure's model-visible content, received instead of a result.
type ToolSettlement<T> = std::result::Result<T, crate::model::ToolResultContent>;

impl StockExecution<'_> {
    pub(super) async fn settle_tool_call(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        invocation: ToolInvocation,
        invocation_index: usize,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<ExecutionProgress<()>> {
        self.verify_task_context_binding(operation_id)?;
        let metadata = metadata_for_call(
            prior_messages,
            invocation_index,
            &invocation.call_id,
            self.io,
        )?;
        invocation.validate_recorded(
            metadata,
            crate::conversation::Limits {
                file_bytes: self
                    .stock
                    .limits
                    .file_bytes
                    .min(self.stock.tool_scope.limits().file_bytes),
                render_bytes: self
                    .stock
                    .limits
                    .render_bytes
                    .min(self.stock.tool_scope.limits().render_bytes),
                ..self.stock.limits
            },
            self.io,
        )?;
        let admitted = match self.stock.tools.get(&invocation.name, None, self.io)? {
            Some(tool) => self
                .authorize_tool_invocation(tool, &invocation)?
                .map(|(context, capability)| (tool, context, capability)),
            None => Err(ToolCallRejection::Unavailable),
        };
        let (tool, mut context, _capability) = match admitted {
            Ok(admitted) => admitted,
            Err(rejection) => {
                let content = self.rejected_tool_content(&invocation, &rejection)?;
                return self.push_tool_result(&invocation, content, prior_messages);
            }
        };
        if let Some(context) = &mut context {
            context.model_step = step;
        }
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
        if let Some(reference) = started {
            let recorded =
                load_json::<crate::tool::RecordedInvocation>(journal, reference, self.io).await?;
            if recorded.0.invocation != invocation || &recorded.0.metadata != metadata {
                return Err(stock_boundary(
                    self.io,
                    Conflict,
                    "tool call identity is bound to another invocation",
                ));
            }
        }
        let failed = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolFailed {
                step: recorded,
                call_id,
                reason,
                cause,
            } if *recorded == step && call_id == &invocation.call_id => Some((*reason, cause)),
            _ => None,
        });
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
        let content = if let Some((reason, cause)) = failed {
            tool_failure_content(reason, cause, self.io)?
        } else if let Some((result, projection)) = completed {
            self.replay_tool_output(journal, result, projection, tool)
                .await?
        } else {
            match self
                .require_tool_policy(journal, operation_id, &invocation, tool, started.is_some())
                .await?
            {
                Some(PolicyStop::Progress(progress)) => return Ok(progress),
                Some(PolicyStop::Refused(denial)) => {
                    let content = self
                        .rejected_tool_content(&invocation, &ToolCallRejection::Refused(denial))?;
                    return self.push_tool_result(&invocation, content, prior_messages);
                }
                None => {}
            }
            match self
                .execute_tool_invocation(
                    journal,
                    (operation_id, step),
                    crate::tool::RecordedInvocation {
                        invocation: &invocation,
                        metadata,
                    },
                    tool,
                    &mut context,
                    started.is_some(),
                )
                .await?
            {
                Ok(content) | Err(content) => content,
            }
        };
        self.push_tool_result(&invocation, content, prior_messages)
    }

    fn push_tool_result(
        &self,
        invocation: &ToolInvocation,
        content: crate::model::ToolResultContent,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<ExecutionProgress<()>> {
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
    fn rejected_tool_content(
        &self,
        invocation: &ToolInvocation,
        rejection: &ToolCallRejection,
    ) -> Result<crate::model::ToolResultContent> {
        let name = &invocation.name;
        let text = with_history_io(self.io, |work| match rejection {
            ToolCallRejection::Unavailable => preparation_format(
                format_args!("tool `{name}` is not available; call one of the declared tools"),
                work,
            ),
            ToolCallRejection::InvalidInput(violation) => preparation_format(
                format_args!(
                    "tool `{name}` rejected its arguments: {}",
                    Sanitized(violation)
                ),
                work,
            ),
            ToolCallRejection::Refused(refusal) => preparation_format(
                format_args!("tool `{name}` refused the call {}", RefusalText(refusal)),
                work,
            ),
        })?;
        Ok(crate::model::ToolResultContent::Error {
            message: text,
            provenance: crate::model::FailureProvenance::RECORDED,
        })
    }

    fn authorize_tool_invocation(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
    ) -> Result<std::result::Result<(Option<crate::runtime::ToolContext>, String), ToolCallRejection>>
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native_work() {
            tool.executor.supports_native_execution(work)?;
            tool.projection.supports_native_projection(Some(work))?;
        }
        let capability = with_history_io(self.io, |work| {
            preparation_format(format_args!("tool:call:{}", tool.definition.name), work)
        })?;
        if !self.stock.tool_scope.grants().contains(&capability) {
            return Ok(Err(ToolCallRejection::Unavailable));
        }
        // Shape precedes resource authorization, so a tool authorizes only well-formed calls.
        if let Err(violation) = with_history_io(self.io, |work| {
            check_json_schema_value(
                &tool.definition.input_schema,
                &invocation.arguments,
                "tool input",
                work,
            )
        })? {
            return Ok(Err(ToolCallRejection::InvalidInput(violation)));
        }
        let context = self.invocation_context(invocation)?;
        stock_authorize_tool(
            tool,
            context.as_ref(),
            &self.stock.tool_scope,
            invocation,
            self.io,
        )?;
        Ok(Ok((context, capability)))
    }

    async fn require_tool_policy(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        invocation: &ToolInvocation,
        tool: &crate::tool::Tool,
        started: bool,
    ) -> Result<Option<PolicyStop>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(progress) = self
            .settle_native_tool_policy(journal, operation_id, invocation, &tool.definition, started)
            .await?
        {
            return Ok(Some(PolicyStop::Progress(progress)));
        }
        if !stock_is_native(self.io) {
            let scope = &self.stock.tool_scope;
            if let Some(policy) = &self.stock.policy {
                if self.stock.policy_identity.as_ref() != Some(&policy.identity()) {
                    return Err(Error::Conflict("stock policy changed after binding".into()));
                }
                let decision = policy.evaluate(invocation, scope, None)?.await?.0;
                if self.stock.policy_identity.as_ref() != Some(&policy.identity()) {
                    return Err(Error::Conflict(
                        "stock policy changed during evaluation".into(),
                    ));
                }
                match decision {
                    ToolPolicyDecision::Allow => {}
                    // A denial before dispatch is the call's result; the pinned policy
                    // re-derives it on replay. After a recorded start it is a conflict.
                    ToolPolicyDecision::Deny { reason } if !started => {
                        return Ok(Some(PolicyStop::Refused(Error::Unauthorized(reason))));
                    }
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
                            invocation,
                            (prompt, None),
                            None,
                        )?;
                        if let Some(progress) = self
                            .settle_approval(
                                journal,
                                operation_id,
                                approval_operation,
                                &request,
                                invocation,
                                started,
                            )
                            .await?
                        {
                            return Ok(Some(PolicyStop::Progress(progress)));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
    async fn execute_tool_invocation(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        record: crate::tool::RecordedInvocation<&ToolInvocation, &Value>,
        tool: &crate::tool::Tool,
        context: &mut Option<crate::runtime::ToolContext>,
        started: bool,
    ) -> Result<ToolSettlement<crate::model::ToolResultContent>> {
        let (operation_id, step) = identity;
        let invocation = record.invocation;
        let claimed = self
            .claim_tool_invocation(
                journal,
                (operation_id, step),
                &record,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                context,
                started,
            )
            .await?;
        let execution = if claimed {
            stock_tool_call(
                context,
                self.io,
                |context| tool.executor.execute(invocation.clone(), context),
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                |context, work| {
                    tool.executor
                        .execute_with_native_context(context, invocation, work)
                },
            )
            .await
            .map(Some)
        } else {
            stock_tool_call(
                context,
                self.io,
                |context| tool.executor.reconcile(invocation.clone(), context),
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                |context, work| {
                    tool.executor
                        .reconcile_with_native_context(context, invocation, work)
                },
            )
            .await
        };
        let result = match execution {
            Ok(Some(result)) => result,
            Err(error) if error.is_native_failure() => return Err(error),
            Err(error) if error.leaves_effect_uncertain() => {
                tracing::error!(%operation_id, call_id = %invocation.call_id, ?error, "tool outcome is uncertain");
                return Err(Error::Indeterminate(operation_id));
            }
            Ok(None) | Err(Error::Indeterminate(_)) => {
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
                    .await
                    .map(Err);
            }
        };
        let (projection, content) = match self
            .project_tool_output(journal, (operation_id, step), invocation, tool, &result)
            .await?
        {
            Ok(projected) => projected,
            Err(reason) => return Ok(Err(reason)),
        };
        Ok(self
            .publish_tool_completion(
                journal,
                (operation_id, step),
                &invocation.call_id,
                &result,
                &projection,
            )
            .await?
            .map(|()| content))
    }

    async fn claim_tool_invocation(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        record: &crate::tool::RecordedInvocation<&ToolInvocation, &Value>,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] context: &Option<
            crate::runtime::ToolContext,
        >,
        started: bool,
    ) -> Result<bool> {
        let (operation_id, step) = identity;
        let invocation = record.invocation;
        let claimed = if started {
            false
        } else {
            let key = with_history_io(self.io, |work| {
                preparation_format(
                    format_args!("tool:{step}:{}:invocation", invocation.call_id),
                    work,
                )
            })?;
            let reference = stage_json(journal, operation_id, &key, record, self.io).await?;
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
                Err(error) if !error.leaves_effect_uncertain() => return Err(error),
                Ok(false) | Err(_) => {
                    return Err(Error::Indeterminate(operation_id));
                }
            }
        };
        Ok(claimed)
    }

    async fn project_tool_output(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        invocation: &ToolInvocation,
        tool: &crate::tool::Tool,
        result: &ToolResult,
    ) -> Result<ToolSettlement<(Value, crate::model::ToolResultContent)>> {
        let (operation_id, step) = identity;
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
                .await
                .map(Err);
        }
        let (projection, content) =
            match stock_project_tool(tool, invocation, result, self.stock.limits, self.io) {
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
                        .await
                        .map(Err);
                }
            };
        Ok(Ok((projection, content)))
    }

    async fn publish_tool_completion(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        call_id: &str,
        result: &ToolResult,
        projection: &Value,
    ) -> Result<ToolSettlement<()>> {
        let (operation_id, step) = identity;
        let result = match self
            .publish_tool_payload(journal, operation_id, step, call_id, "result", result)
            .await?
        {
            Ok(result) => result,
            Err(reason) => return Ok(Err(reason)),
        };
        let projection = match self
            .publish_tool_payload(
                journal,
                operation_id,
                step,
                call_id,
                "projection",
                projection,
            )
            .await?
        {
            Ok(projection) => projection,
            Err(reason) => return Ok(Err(reason)),
        };
        let (tail, current) = self
            .tool_records(journal, operation_id, step, call_id)
            .await?;
        if current.iter().any(|record| {
            matches!(&record.event,
                ExecutionEvent::ToolCompleted { step: recorded, call_id: recorded_call, .. }
                    | ExecutionEvent::ToolFailed { step: recorded, call_id: recorded_call, .. }
                    if *recorded == step && recorded_call == call_id)
        }) {
            return Err(Error::Indeterminate(operation_id));
        }
        let key = with_history_io(self.io, |work| {
            preparation_format(
                format_args!("tool:{step}:{}:completed:{}", call_id, OperationId::new()),
                work,
            )
        })?;
        let event = ExecutionEvent::ToolCompleted {
            step,
            call_id: stock_string_copy(call_id, self.io)?,
            result,
            projection,
        };
        match journal
            .append_if_tail(operation_id, tail, &key, &event, self.io)?
            .await
        {
            Ok(true) => {}
            Err(error) if !error.leaves_effect_uncertain() => return Err(error),
            Ok(false) | Err(_) => {
                return Err(Error::Indeterminate(operation_id));
            }
        }
        Ok(Ok(()))
    }

    async fn replay_tool_output(
        &self,
        journal: &dyn ExecutionJournal,
        result: &FileRef,
        projection: &FileRef,
        tool: &crate::tool::Tool,
    ) -> Result<crate::model::ToolResultContent> {
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
            .validate_projection(&projection.0, self.stock.limits, self.io)
    }
}

/// The model-visible result of a recorded tool failure; replay derives the same content.
pub(super) fn tool_failure_content(
    reason: ToolFailureKind,
    cause: &ToolFailureCause,
    io: Option<&PreparationIo>,
) -> Result<crate::model::ToolResultContent> {
    Ok(crate::model::ToolResultContent::Error {
        message: with_history_io(io, |work| {
            preparation_format(format_args!("{}", ToolFailureText(reason, cause)), work)
        })?,
        provenance: crate::model::FailureProvenance::RECORDED,
    })
}

pub(super) fn metadata_for_call<'a>(
    messages: &'a [ModelMessage],
    index: usize,
    call_id: &str,
    io: Option<&PreparationIo>,
) -> Result<&'a Value> {
    crate::model::charge_items(io, 1)?;
    let Some(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(call)),
    }) = messages.get(index)
    else {
        return Err(stock_boundary(
            io,
            Invalid,
            "tool invocation has no indexed model call",
        ));
    };
    if call.call_id != call_id {
        return Err(stock_boundary(
            io,
            Invalid,
            "tool invocation index refers to another call",
        ));
    }
    Ok(&call.metadata)
}
