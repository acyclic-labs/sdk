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
        let records = self.prefix_records(journal, input.operation_id, step).await?;
        match records.first() {
            Some(ExecutionRecord { sequence: 1, event: ExecutionEvent::Started { request_digest }, .. })
                if self.matches_request_digest(input, request_digest)? => {}
            _ => return Err(stock_boundary(self.io(), crate::wire::ErrorCode::Conflict,
                "tool settlement has another execution identity")),
        }
        let retained = retained_model_step(
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
                        arguments: crate::model::clone_value(&invocation.arguments, self.io())?,
                    }),
                };
                crate::executor::with_history_io(self.io(), |work| message.content.validate_limits(self.limits, work))?;
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
