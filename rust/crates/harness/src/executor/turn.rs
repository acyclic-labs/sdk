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
            let model_events = self
                .run_model_step(
                    journal,
                    input,
                    step,
                    &prior_messages,
                    ModelPurpose::Response,
                )?
                .await?;
            let (calls, metadata) =
                self.collect_turn_events(input, step, model_events, &mut text)?;
            if calls.is_empty() {
                return Ok(ExecutionProgress::Ready(TurnOutput {
                    text,
                    attachments: Vec::new(),
                    metadata,
                    steps: step + 1,
                }));
            }
            for (invocation, metadata) in calls {
                let message = ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(
                        crate::model::ToolCall::new(
                            stock_string_copy(&invocation.call_id, self.io)?,
                            stock_string_copy(&invocation.name, self.io)?,
                            crate::model::clone_value(&invocation.arguments, self.io)?,
                        )
                        .with_metadata(metadata),
                    )),
                };
                crate::executor::with_history_io(self.io, |work| {
                    message.content.validate_limits(self.stock.limits, work)
                })?;
                let invocation_index = prior_messages.len();
                stock_vec_push(&mut prior_messages, message, self.io)?;
                match self
                    .resolve_tool_call(
                        journal,
                        input.operation_id,
                        step,
                        invocation,
                        invocation_index,
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

    fn collect_turn_events(
        &self,
        input: &TurnInput,
        step: u32,
        model_events: Vec<ModelEvent>,
        text: &mut String,
    ) -> Result<(Vec<(ToolInvocation, Value)>, Value)> {
        let mut calls = Vec::new();
        let mut completed = Value::Null;
        for event in model_events {
            match event {
                ModelEvent::Content { delta } => {
                    stock_string_push(
                        text,
                        &delta,
                        self.stock.limits.file_bytes,
                        "assistant output exceeds file limit",
                        self.io,
                    )?;
                }
                ModelEvent::ToolCall(crate::model::ToolCall {
                    call_id,
                    name,
                    arguments,
                    metadata,
                }) => {
                    let invocation = ToolInvocation::for_model_call(
                        input.operation_id,
                        step,
                        call_id,
                        name,
                        arguments,
                        self.native_work(),
                    )?;
                    stock_vec_push(&mut calls, (invocation, metadata), self.io)?;
                }
                ModelEvent::Completed { metadata } => completed = metadata,
                ModelEvent::Reasoning { .. } => {}
            }
        }
        Ok((calls, completed))
    }
}
