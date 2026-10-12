use super::*;

impl StockExecution<'_> {
    pub(super) async fn execute_progress(
        &self,
        input: &TurnInput,
        journal: &dyn ExecutionJournal,
    ) -> Result<ExecutionProgress<TurnOutput>> {
        self.verify_task_context_binding(input.operation_id)?;
        self.verify_execution_owner().await?;
        self.validate_turn_input(journal, input).await?;
        self.ensure_started(journal, input).await?;
        let mut prior_messages = Vec::new();
        let mut text = String::new();
        for step in 0..input.max_steps {
            let mut calls = Vec::new();
            let mut completed = None;
            let model_events = self
                .run_model_step(
                    journal,
                    input,
                    step,
                    &prior_messages,
                    ModelPurpose::Response,
                )?
                .await?;
            for event in model_events {
                match event {
                    ModelEvent::Content { delta } => {
                        if text
                            .len()
                            .checked_add(delta.len())
                            .is_none_or(|size| size as u64 > self.stock.limits.file_bytes)
                        {
                            return Err(stock_boundary(
                                self.io,
                                crate::wire::ErrorCode::Invalid,
                                "assistant output exceeds file limit",
                            ));
                        }
                        stock_string_push(&mut text, &delta, self.io)?;
                    }
                    ModelEvent::ToolCall {
                        call_id,
                        name,
                        arguments,
                    } => {
                        let invocation = stock_model_call(
                            input.operation_id,
                            step,
                            call_id,
                            name,
                            arguments,
                            self.io,
                        )?;
                        stock_vec_push(&mut calls, invocation, self.io)?;
                    }
                    ModelEvent::Completed { metadata } => completed = Some(metadata),
                    ModelEvent::Reasoning { .. } => {}
                }
            }
            let Some(metadata) = completed else {
                return Err(Error::Indeterminate(input.operation_id));
            };
            if calls.is_empty() {
                return Ok(ExecutionProgress::Ready(TurnOutput {
                    text,
                    attachments: Vec::new(),
                    metadata,
                    steps: step + 1,
                }));
            }
            for invocation in calls {
                let message = ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall {
                        call_id: stock_string_copy(&invocation.call_id, self.io)?,
                        name: stock_string_copy(&invocation.name, self.io)?,
                        arguments: crate::model::clone_value(&invocation.arguments, self.io)?,
                    }),
                };
                crate::executor::with_history_io(self.io, |work| {
                    message.content.validate_limits(self.stock.limits, work)
                })?;
                stock_vec_push(&mut prior_messages, message, self.io)?;
                match self
                    .resolve_tool_call(
                        journal,
                        input.operation_id,
                        step,
                        invocation,
                        &mut prior_messages,
                    )
                    .await?
                {
                    ExecutionProgress::Ready(()) => {}
                    ExecutionProgress::Pending(wait) => {
                        return Ok(ExecutionProgress::Pending(wait));
                    }
                    ExecutionProgress::Rejected(reason) => {
                        return Ok(ExecutionProgress::Rejected(reason));
                    }
                }
            }
        }
        Err(stock_boundary(
            self.io,
            crate::wire::ErrorCode::Conflict,
            "executor step limit reached",
        ))
    }
}
