use super::*;

impl StockExecution<'_> {
    pub(super) fn verify_task_context_binding(&self, operation: OperationId) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_work().is_some()
            && let Some((context, expected_operation)) = self.context
        {
            if expected_operation != operation
                || context.durable_task_id() != self.stock.task.as_ref().map(|(_, task, _)| *task)
                || context.scope().run_limits() != self.stock.tool_scope.run_limits()
                || context.scope().extensions() != self.stock.tool_scope.extensions()
                || self.stock.task.as_ref().is_some_and(|(_, task, fence)| {
                    !context.original_task_binding_matches(*task, fence)
                })
            {
                return Err(crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "stock task context differs from execution binding",
                });
            }
            return Ok(());
        }
        if let Some((context, expected_operation)) = self.context
            && (expected_operation != operation
                || context.durable_task_id() != self.stock.task.as_ref().map(|(_, task, _)| *task)
                || context.scope().grants() != self.stock.tool_scope.grants()
                || context.scope().limits() != self.stock.limits
                || context.scope().run_limits() != self.stock.tool_scope.run_limits()
                || context.scope().extensions() != self.stock.tool_scope.extensions())
        {
            return Err(Error::Unauthorized(
                "stock task context differs from execution binding".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn invocation_context(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<Option<crate::runtime::ToolContext>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = self.native_work() {
            let (context, _) = self.context.ok_or(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "native tool context has no issued task",
            })?;
            let mut tool_context = crate::runtime::ToolContext::new(
                Arc::clone(context),
                invocation.operation_id,
                &invocation.call_id,
                self.io,
            )?
            .with_native_scope(&self.stock.tool_scope, self.stock.limits, work)?;
            if let Some(interactions) = self.interactions {
                tool_context =
                    tool_context.with_native_interactions(Arc::clone(interactions), work)?;
            }
            return Ok(Some(tool_context));
        }
        self.context
            .map(|(context, _)| {
                crate::runtime::ToolContext::new(
                    Arc::clone(context),
                    invocation.operation_id,
                    &invocation.call_id,
                    None,
                )
            })
            .transpose()
    }

    pub(super) fn request_digest(&self, input: &TurnInput) -> Result<[u8; 32]> {
        #[derive(Serialize)]
        struct IssuedContext<'a> {
            admission_operation: OperationId,
            execution_operation: OperationId,
            task_id: Option<TaskId>,
            run_limits: crate::runtime::TaskRunLimits,
            extensions: Option<&'a crate::core::ExtensionAdmission>,
        }
        #[derive(Serialize)]
        struct Request<'a> {
            executor: &'static str,
            input: &'a TurnInput,
            model: &'a Model,
            max_output_tokens: Option<u32>,
            context: &'a [Value],
            tools: &'a [crate::tool::ToolDefinition],
            limits: Limits,
            tool_scope: (&'a crate::Capabilities, Limits),
            policy: Option<&'a ComponentIdentity>,
            inherited_prefix: Option<&'a FileRef>,
            compaction: &'a crate::context::CompactionPolicy,
            model_capacity: Option<crate::context::ModelContextCapacity>,
            #[serde(skip_serializing_if = "Option::is_none")]
            task_id: Option<TaskId>,
            #[serde(skip_serializing_if = "Option::is_none")]
            task_context: Option<IssuedContext<'a>>,
        }
        let io = self.io;
        let contracts = self.context_pipeline().contracts(io)?;
        let tools = self.stock.tools.definitions(io)?;
        let task_context = self.context.map(|(context, operation)| IssuedContext {
            admission_operation: context.id(),
            execution_operation: operation,
            task_id: context.durable_task_id(),
            run_limits: context.scope().run_limits(),
            extensions: context.scope().extensions(),
        });
        preparation_digest(
            &Request {
                executor: "acyclic.stock.v6",
                input,
                model: &self.stock.model,
                max_output_tokens: self.output_tokens(),
                context: &contracts,
                tools: &tools,
                limits: self.stock.limits,
                tool_scope: (
                    self.stock.tool_scope.grants(),
                    self.stock.tool_scope.limits(),
                ),
                policy: self.stock.policy_identity.as_ref(),
                inherited_prefix: self.inherited_prefix().map(|(reference, _)| reference),
                compaction: self.compaction_policy(),
                model_capacity: self.model_capacity()?,
                task_id: self.stock.task.as_ref().map(|(_, task, _)| *task),
                task_context,
            },
            io,
        )
    }

    /// Replays the bound turn journal and starts it when empty.
    pub(super) async fn ensure_started(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        self.verify_task_context_binding(input.operation_id)?;
        let (tail, records) = replay_execution(
            journal,
            input.operation_id,
            1,
            |event| matches!(event, ExecutionEvent::Started { .. }),
            self.io,
        )
        .await?;
        if tail != 0 && records.first().is_none_or(|record| record.sequence != 1) {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "execution journal has no initial request binding",
            ));
        }
        let request_digest = self.request_digest(input)?;
        match records.first().map(|record| &record.event) {
            Some(ExecutionEvent::Started {
                request_digest: existing,
            }) if existing == &request_digest => {}
            Some(_) => {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Conflict,
                    "execution identity is bound to another request or configuration",
                ));
            }
            None => {
                if self.stock.reconciliation_only {
                    return Err(Error::Indeterminate(input.operation_id));
                }
                stock_append(
                    journal,
                    input.operation_id,
                    "execution:started",
                    &ExecutionEvent::Started { request_digest },
                    self.io,
                )
                .await?;
            }
        }
        Ok(())
    }
}
