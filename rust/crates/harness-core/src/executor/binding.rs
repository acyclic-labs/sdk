use super::*;

impl StockExecution<'_> {
    pub(super) fn verify_task_context_binding(&self, operation: OperationId) -> Result<()> {
        self.verify_summary_destination()?;
        #[cfg(not(target_arch = "wasm32"))]
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

    /// A task needs summary authority before model admission. The summary itself
    /// uses the journal's one immutable publication and authentication path.
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

    /// A turn belongs to its original input and agent volume. Model selection,
    /// executor code and resource policy are replaceable runtime configuration;
    /// each admitted model request separately records its exact immutable bytes.
    pub(super) fn input_digest(&self, input: &TurnInput) -> Result<[u8; 32]> {
        #[derive(Serialize)]
        struct Binding<'a> {
            contract: &'static str,
            operation: OperationId,
            input: &'a ModelContent,
            selection: Option<&'a crate::projection::SelectedModelContext>,
            private_volume: Option<&'a crate::conversation::VolumeRef>,
        }
        preparation_digest(
            &Binding {
                contract: "acyclic.turn.v1",
                operation: input.operation_id,
                input: &input.input,
                selection: input.selected_context.as_ref(),
                private_volume: self.context.and_then(|(context, _)| {
                    context.content_binding(None).map(|binding| &binding.volume)
                }),
            },
            self.io,
        )
    }
    /// Checks the bound turn journal and starts it when empty.
    pub(super) async fn ensure_started(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        self.verify_task_context_binding(input.operation_id)?;

        #[cfg(not(target_arch = "wasm32"))]
        let _records_hold;
        #[cfg(not(target_arch = "wasm32"))]
        let records_before = self
            .io
            .map(PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let (tail, records) = self
            .step_records(journal, input.operation_id, 0, true, Some(1), |event| {
                matches!(event, ExecutionEvent::Started { .. })
            })
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
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
        let request_digest = self.input_digest(input)?;
        match records.first().map(|record| &record.event) {
            Some(ExecutionEvent::Started {
                request_digest: existing,
            }) if existing == &request_digest => {}
            Some(_) => {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Conflict,
                    "turn identity is bound to another input or agent volume",
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
