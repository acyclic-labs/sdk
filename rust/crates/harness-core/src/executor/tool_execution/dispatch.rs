use super::*;

impl StockExecution<'_> {
    pub(super) async fn execute_tool_invocation(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        record: crate::tool::RecordedInvocation<&ToolInvocation, &Value>,
        tool: &crate::tool::Tool,
        context: &mut Option<crate::runtime::ToolContext>,
        started: bool,
        _hook_context: &mut Vec<ModelMessage>,
    ) -> Result<ToolInvocationOutcome> {
        let (operation_id, step) = identity;
        let invocation = record.invocation;
        let completion = crate::executor::phase::PreparedPhase::new(journal, self.io)?;
        let claimed = crate::executor::phase::claim(self.claim_tool_invocation(
            journal,
            (operation_id, step),
            &record,
            started,
        ))
        .await?;
        let execution = self
            .observe_tool_cancellation(
                journal,
                identity,
                &invocation.call_id,
                tool.executor.cancellable_on_drop(),
                self.call_tool_executor(tool, context, invocation, claimed),
            )
            .await?;
        #[cfg(feature = "test-support")]
        tracing::trace!(
            target: "acyclic_harness::tool_return",
            step,
            executed = claimed,
            succeeded = execution.as_ref().is_ok_and(|result| result.is_some()),
            "tool executor returned"
        );
        let result = match execution {
            Ok(Some(result)) => result,
            // A resumed driver settles an unreconcilable effect as a typed
            // failure instead of running it again; reconciliation records nothing.
            Ok(None) | Err(Error::Indeterminate(_))
                if !claimed
                    && !self.stock.reconciliation_only
                    && !crate::executor::unsettled_resume::enabled() =>
            {
                self.reject_tool_call(
                    journal,
                    operation_id,
                    step,
                    &invocation.call_id,
                    ToolFailureKind::Interrupted,
                    Error::Indeterminate(operation_id),
                )
                .await?;
                return Ok(ToolInvocationOutcome::Interrupted);
            }
            Ok(None) | Err(Error::Indeterminate(_)) => {
                return Err(Error::Indeterminate(operation_id));
            }
            // Keep the original cause and started record for reconciliation. Every
            // definitive refusal settles below, regardless of the tool's adapter.
            Err(error) if !error.is_tool_failure() => return Err(error),
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
                    .map(ToolInvocationOutcome::Settled);
            }
        };
        self.finish_tool_result(
            journal,
            identity,
            invocation,
            tool,
            &result,
            (_hook_context, completion),
        )
        .await
    }

    async fn finish_tool_result(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        invocation: &ToolInvocation,
        tool: &crate::tool::Tool,
        result: &ToolResult,
        completion: (
            &mut Vec<ModelMessage>,
            crate::executor::phase::PreparedPhase<'_>,
        ),
    ) -> Result<ToolInvocationOutcome> {
        let (_hook_context, completion) = completion;
        let (projection, content) = match self
            .project_tool_output(journal, identity, invocation, tool, result)
            .await?
        {
            Ok(projected) => projected,
            Err(content) => return Ok(ToolInvocationOutcome::Settled(content)),
        };
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        {
            match self
                .complete_with_post_hook::<true>(
                    journal,
                    identity,
                    invocation,
                    result,
                    &projection,
                    completion,
                )
                .await?
            {
                Ok(feedback) => self.append_hook_context(_hook_context, feedback)?,
                Err(content) => return Ok(ToolInvocationOutcome::Settled(content)),
            }
            Ok(ToolInvocationOutcome::Settled(content))
        }
        #[cfg(not(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32"))))]
        {
            let publication = self
                .publish_tool_completion(
                    journal,
                    identity,
                    &invocation.call_id,
                    result,
                    &projection,
                    completion,
                )
                .await?;
            Ok(ToolInvocationOutcome::Settled(match publication {
                Ok(()) => content,
                Err(content) => content,
            }))
        }
    }

    async fn call_tool_executor(
        &self,
        tool: &crate::tool::Tool,
        context: &mut Option<crate::runtime::ToolContext>,
        invocation: &ToolInvocation,
        claimed: bool,
    ) -> Result<Option<ToolResult>> {
        if claimed {
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
        }
    }

    async fn observe_tool_cancellation<T>(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        call_id: &str,
        cancellable: bool,
        execution: impl std::future::Future<Output = T>,
    ) -> Result<T> {
        if !cancellable {
            return Ok(execution.await);
        }
        if let Some(result) = cancel_safe_call(execution, journal.model_cancellation()).await? {
            return Ok(result);
        }
        self.reject_tool_call(
            journal,
            identity.0,
            identity.1,
            call_id,
            ToolFailureKind::Interrupted,
            Error::InteractionRejected(crate::InteractionRejection::Cancelled),
        )
        .await?;
        Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        ))
    }

    #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::executor) async fn complete_with_post_hook<const COMMIT_FIRST: bool>(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        invocation: &ToolInvocation,
        result: &ToolResult,
        projection: &Value,
        phase: crate::executor::phase::PreparedPhase<'_>,
    ) -> Result<ToolSettlement<crate::hooks::HookOutput>> {
        let completion = self.publish_tool_completion(
            journal,
            identity,
            &invocation.call_id,
            result,
            projection,
            phase,
        );
        let feedback = self.tool_hook(
            identity.0,
            crate::hooks::HookSite::AfterTool,
            invocation,
            Some(result),
            false,
        );
        if COMMIT_FIRST {
            match completion.await? {
                Ok(()) => feedback.await.map(Ok),
                Err(content) => Ok(Err(content)),
            }
        } else {
            // Negative control: external feedback cannot precede durable settlement.
            let output = feedback.await?;
            completion.await.map(|settled| settled.map(|()| output))
        }
    }

    async fn claim_tool_invocation(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        record: &crate::tool::RecordedInvocation<&ToolInvocation, &Value>,
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
            // A claim acquires the dispatch: its key names this attempt, so a
            // racer at the same tail is refused instead of recognised as a
            // retry and running the tool twice. Settlements stay stable.
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
                Ok(true) => true,
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
        // A violation is the tool's mistake and settles the call, including the native cause;
        // a schema, budget or diagnostic failure still stops the turn.
        if let Err(violation) = with_history_io(self.io, |work| {
            check_json_schema_value(
                &tool.definition.output_schema,
                &result.value,
                "tool output",
                work,
            )
        })? {
            return self
                .settle_tool_failure(
                    journal,
                    operation_id,
                    step,
                    &invocation.call_id,
                    ToolFailureKind::InvalidOutput,
                    violation,
                )
                .await
                .map(Err);
        }
        let (projection, content) =
            match stock_project_tool(tool, invocation, result, self.stock.limits, self.io) {
                Ok(Ok(projected)) => projected,
                // A projection outside its pinned schema is the tool's mistake on either path.
                Ok(Err(violation)) => {
                    return self
                        .settle_tool_failure(
                            journal,
                            operation_id,
                            step,
                            &invocation.call_id,
                            ToolFailureKind::ProjectionRejected,
                            violation,
                        )
                        .await
                        .map(Err);
                }
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
        completion: crate::executor::phase::PreparedPhase<'_>,
    ) -> Result<ToolSettlement<()>> {
        completion
            .run(self.publish_tool_completion_inner(journal, identity, call_id, result, projection))
            .await
    }

    async fn publish_tool_completion_inner(
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
        let key = tool_settlement_key(step, call_id, ToolTerminalOutcome::Completed, self.io)?;
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

    pub(super) async fn replay_tool_output(
        &self,
        journal: &dyn ExecutionJournal,
        _operation: OperationId,
        _invocation: &ToolInvocation,
        result: &FileRef,
        projection: &FileRef,
        tool: &crate::tool::Tool,
        _hook_context: &mut Vec<ModelMessage>,
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
        let content =
            tool.definition
                .validate_projection(&projection.0, self.stock.limits, self.io)?;
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let feedback = self
                .tool_hook(
                    _operation,
                    crate::hooks::HookSite::AfterTool,
                    _invocation,
                    Some(&result.0),
                    false,
                )
                .await?;
            self.append_hook_context(_hook_context, feedback)?;
        }
        Ok(content)
    }
}
