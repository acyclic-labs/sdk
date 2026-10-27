use super::*;

impl StockExecution<'_> {
    pub(super) fn verify_task_context_binding(&self, operation: OperationId) -> Result<()> {
        self.verify_summary_destination()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_work().is_some()
            && let Some((context, expected_operation)) = self.context
        {
            if expected_operation != operation
                || context.scope().run_limits() != self.stock.tool_scope.run_limits()
                || context.scope().extensions() != self.stock.tool_scope.extensions()
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

    /// Compaction inside a task context publishes through the task's private
    /// summary writer. A context without one rejects the turn at entry, before
    /// any model work, instead of failing when the first summary is due.
    fn verify_summary_destination(&self) -> Result<()> {
        self.compaction()?;
        Ok(())
    }

    pub(super) fn invocation_context(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<Option<crate::runtime::ToolContext>> {
        self.context
            .map(|(context, _)| {
                crate::runtime::ToolContext::new(
                    Arc::clone(context),
                    invocation.operation_id,
                    &invocation.call_id,
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
            private_volume: Option<&'a crate::conversation::VolumeRef>,
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
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            #[serde(skip_serializing_if = "Option::is_none")]
            tool_hooks: Option<(&'static str, &'a [u8; 32])>,
            inherited_prefix: Option<&'a FileRef>,
            compaction: &'a crate::context::CompactionPolicy,
            model_capacity: Option<crate::context::ModelContextCapacity>,
            #[serde(skip_serializing_if = "Option::is_none")]
            task_context: Option<IssuedContext<'a>>,
        }
        // Contracts and tool definitions are temporary digest inputs. Keep
        // their native custody on the original work, with a scope that also
        // releases partial preparation when a stage or encoder rejects.
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let temporary_io = self
            .io
            .and_then(PreparationIo::original_native_work)
            .map(PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = temporary_io.as_ref().or(self.io);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = self.io;
        let contracts = self.context_pipeline().contracts(io)?;
        let tools = stock_tool_definitions(&self.stock.tools, &self.stock.tool_scope, io)?;
        let task_context = self.context.map(|(context, operation)| IssuedContext {
            admission_operation: operation,
            execution_operation: operation,
            task_id: None,
            private_volume: context
                .content_binding(None)
                .and_then(|binding| binding.writer.as_ref())
                .map(|writer| writer.volume()),
            run_limits: context.scope().run_limits(),
            extensions: context.scope().extensions(),
        });
        preparation_digest(
            &Request {
                executor: "acyclic.stock.v7",
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
                #[cfg(all(
                    feature = "hooks",
                    feature = "filesystem",
                    not(target_arch = "wasm32")
                ))]
                tool_hooks: self
                    .stock
                    .hooks
                    .as_ref()
                    .map(|hooks| ("1", &hooks.manifest)),
                inherited_prefix: self.inherited_prefix().map(|(reference, _)| reference),
                compaction: self.compaction_policy()?,
                model_capacity: self.model_capacity()?,
                task_context,
            },
            io,
        )
    }

    /// Checks the bound turn journal and starts it when empty.
    pub(super) async fn ensure_started(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        self.verify_task_context_binding(input.operation_id)?;

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _records_hold;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let records_before = self
            .io
            .map(PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let (tail, records) = self
            .step_records(journal, input.operation_id, 0, true, 1, |event| {
                matches!(event, ExecutionEvent::Started { .. })
            })
            .await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            _records_hold = if let Some(io) = self.io.filter(|_| stock_is_native(self.io)) {
                let bytes = io
                    .owned_bytes()?
                    .checked_sub(records_before)
                    .ok_or_else(|| {
                        stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Storage,
                            "binding record ownership underflows",
                        )
                    })?;
                Some(io.guard_owned(bytes)?)
            } else {
                None
            };
        }
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
                journal
                    .append(
                        input.operation_id,
                        "execution:started",
                        &ExecutionEvent::Started { request_digest },
                        self.io,
                    )?
                    .await?;
            }
        }
        Ok(())
    }
}
