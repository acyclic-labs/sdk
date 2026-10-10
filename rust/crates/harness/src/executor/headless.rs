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
        self.verify_task_context_binding(input.operation_id)?;
        self.validate_turn_input(journal, input).await?;
        if step >= input.max_steps {
            return Err(Error::Invalid("model step is outside admitted turn bounds".into()));
        }
        // Unlike model_step this cannot create Started or start a model attempt.
        let records = self.prefix_records(journal, input.operation_id, step).await?;
        match records.first() {
            Some(ExecutionRecord { sequence: 1, event: ExecutionEvent::Started { request_digest }, .. })
                if self.matches_request_digest(input, request_digest)? => {}
            _ => return Err(Error::Conflict("tool settlement has another execution identity".into())),
        }
        let retained = retained_model_step(journal, &records, step, ModelPurpose::Response, self.limits).await?;
        if !retained.admission.completed() {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let mut messages = Vec::new();
        for event in retained.events {
            if let ModelEvent::ToolCall { call_id, name, arguments } = event {
                let invocation = ToolInvocation::for_model_call(input.operation_id, step, call_id, name, arguments);
                let message = ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: invocation.call_id.clone(), name: invocation.name.clone(),
                        arguments: invocation.arguments.clone(),
                    }),
                };
                message.content.validate_limits(self.limits)?;
                messages.push(message);
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
