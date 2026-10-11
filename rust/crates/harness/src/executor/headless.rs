//! Custom-loop entrypoints reuse stock admission and tool settlement.
use super::*;

/// Result of settling the ordered tools admitted by one retained model step.
pub enum ModelToolsProgress {
    /// Complete assistant-call/tool-result messages to supply to the next step.
    Ready(Vec<ModelMessage>),
    /// The existing approval ticket is pending; no fake tool result is supplied.
    Pending(OperationId),
    /// The admitted approval was rejected.
    Rejected(crate::InteractionRejection),
}

impl StockExecutor {
    /// Resolves ONLY calls from an already completed retained model response.
    /// Call identities, pinned contracts, grants, policy and provider recovery
    /// are derived by the same resolver used by the stock control loop.
    pub async fn resolve_model_tools(
        &self, journal: &dyn ExecutionJournal, input: &TurnInput, step: u32,
    ) -> Result<ModelToolsProgress> {
        StockExecution::ordinary(self).resolve_model_tools(journal, input, step).await
    }
}

impl StockExecution<'_> {
    async fn resolve_model_tools(
        &self, journal: &dyn ExecutionJournal, input: &TurnInput, step: u32,
    ) -> Result<ModelToolsProgress> {
        self.verify_task_context_binding(input.operation_id)?;
        self.validate_turn_input(journal, input).await?;
        if step >= input.max_steps {
            return Err(stock_boundary(self.io(), crate::wire::ErrorCode::Invalid,
                "model step is outside admitted turn bounds"));
        }
        // Unlike model_step this cannot create Started or start a model attempt.
        let records = self.prefix_records(journal, input.operation_id, step).await?;
        match records.first() {
            Some(ExecutionRecord { sequence: 1, event: ExecutionEvent::Started { request_digest }, .. })
                if self.matches_request_digest(input, request_digest)? => {}
            _ => return Err(stock_boundary(self.io(), crate::wire::ErrorCode::Conflict,
                "tool settlement has another execution identity")),
        }
        let retained = retained_model_step_with_io(
            journal, &records, step, ModelPurpose::Response, self.limits, self.io(),
        ).await?;
        if !retained.admission.completed() {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let mut messages = Vec::new();
        for event in retained.events {
            if let ModelEvent::ToolCall { call_id, name, arguments } = event {
                let invocation = stock_model_call(
                    input.operation_id, step, call_id, name, arguments, self.io(),
                )?;
                let message = ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: stock_string_copy(&invocation.call_id, self.io())?,
                        name: stock_string_copy(&invocation.name, self.io())?,
                        arguments: stock_value_copy(&invocation.arguments, self.io())?,
                    }),
                };
                message.content.validate_limits_with_io(self.limits, self.io())?;
                stock_vec_push(&mut messages, message, self.io())?;
                match self.resolve_tool_call(journal, input.operation_id, step, invocation, &mut messages).await? {
                    ToolCallProgress::Settled => {}
                    ToolCallProgress::Pending(operation) => return Ok(ModelToolsProgress::Pending(operation)),
                    ToolCallProgress::Rejected(reason) => return Ok(ModelToolsProgress::Rejected(reason)),
                }
            }
        }
        Ok(ModelToolsProgress::Ready(messages))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeStockExecutor {
    /// Settles retained model calls through the same stock resolver and original work.
    pub fn resolve_model_tools_with_native_context<'a>(
        &'a self, context: &'a Arc<crate::runtime::NativeTaskContext>,
        journal: &'a dyn ExecutionJournal, input: &'a TurnInput, step: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ModelToolsProgress, acyclic_fs::NativeAllocation)>> {
        self.admit_execution(context, input, journal, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            io.retain_owned(std::mem::size_of::<ModelToolsProgress>() as u64)?;
            let progress = {
                let session = native_session(&self.stock, context, input.operation_id, &io,
                    self.stock.reconciliation_only, self.interactions.as_ref());
                session.resolve_model_tools(journal, input, step).await?
            };
            let retained = native_model_tools_owned_bytes(&progress)?;
            work.remaining()?;
            Ok((progress, io.into_native_custody(retained)?))
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_model_tools_owned_bytes(progress: &ModelToolsProgress) -> Result<u64> {
    let root = std::mem::size_of::<ModelToolsProgress>() as u64;
    match progress {
        ModelToolsProgress::Ready(messages) => {
            let slots = (messages.capacity() as u64)
                .checked_mul(std::mem::size_of::<ModelMessage>() as u64)
                .ok_or_else(|| native_boundary(crate::wire::ErrorCode::Storage,
                    "native tool messages capacity overflows"))?;
            messages.iter().try_fold(native_size_add(root, slots)?, |bytes, message| {
                let payload = message.content.logical_owned_bytes()?.checked_sub(
                    std::mem::size_of::<ModelContent>() as u64,
                ).ok_or_else(|| native_boundary(crate::wire::ErrorCode::Storage,
                    "native tool message metadata extent differs"))?;
                native_size_add(bytes, payload)
            })
        }
        ModelToolsProgress::Pending(_) | ModelToolsProgress::Rejected(_) => Ok(root),
    }
}
