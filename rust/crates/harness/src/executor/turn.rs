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
        if let Some(source) = self.stock.summary_source.as_deref() {
            return crate::conversation::BoundedContentFuture::new(self.io, || {
                self.summarize_file(journal, input, source)
            })?
            .await;
        }
        self.ensure_started(journal, input).await?;
        let mut prior_messages = Vec::new();
        let mut hook_context = Vec::new();
        let mut text = String::new();
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        let mut stop_active = false;
        for step in 0..input.max_steps {
            #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
            let answer_start = text.len();
            let model_events = self
                .run_model_step(
                    journal,
                    input,
                    step,
                    &prior_messages,
                    ModelPurpose::Response,
                )?
                .await?;
            let (calls, metadata) = self.collect_turn_events(
                input,
                step,
                model_events,
                &mut text,
                &mut prior_messages,
            )?;
            if calls.is_empty() {
                // A Stop block prevents settlement and consumes the step budget.
                #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(reason) =
                    Box::pin(self.stop_hook(input.operation_id, step, stop_active)).await?
                {
                    // Every denial continues. At the final step, falling out
                    // of the loop reports the same budget exhaustion as tools.
                    // The superseded answer moves into history; the turn's
                    // output is the answer given after the hook's reason.
                    let answer = text.split_off(answer_start);
                    self.continue_after_stop(&mut prior_messages, &answer, reason)?;
                    stop_active = true;
                    continue;
                }
                return Ok(ExecutionProgress::Ready(TurnOutput {
                    text,
                    attachments: Vec::new(),
                    metadata,
                    steps: step + 1,
                }));
            }
            for (invocation, invocation_index) in calls {
                match self
                    .resolve_tool_call(
                        journal,
                        input.operation_id,
                        step,
                        invocation,
                        invocation_index,
                        &mut prior_messages,
                        &mut hook_context,
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
            append_step_context(&mut prior_messages, &mut hook_context, self.io)?;
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
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<(Vec<(ToolInvocation, usize)>, Value)> {
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
                    let call = crate::model::ToolCall::new(
                        stock_string_copy(&invocation.call_id, self.io)?,
                        stock_string_copy(&invocation.name, self.io)?,
                        crate::model::clone_value(&invocation.arguments, self.io)?,
                    )
                    .with_metadata(metadata);
                    let index = prior_messages.len();
                    self.append_assistant_part(prior_messages, ModelContentPart::ToolCall(call))?;
                    stock_vec_push(&mut calls, (invocation, index), self.io)?;
                }
                ModelEvent::Completed { metadata } => completed = metadata,
                ModelEvent::ReasoningBlock { block } => {
                    self.append_assistant_part(
                        prior_messages,
                        ModelContentPart::Reasoning { block },
                    )?;
                }
                ModelEvent::Reasoning { .. } => {}
            }
        }
        Ok((calls, completed))
    }

    pub(super) fn append_assistant_part(
        &self,
        messages: &mut Vec<ModelMessage>,
        part: ModelContentPart,
    ) -> Result<()> {
        let message = ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(part),
        };
        with_history_io(self.io, |work| {
            message.content.validate_limits(self.stock.limits, work)
        })?;
        stock_vec_push(messages, message, self.io)
    }
}

/// Results go straight into history; hook context is deferred until every
/// result is present. No sorting scratch allocation or reordering is needed.
pub(super) fn append_step_context(
    messages: &mut Vec<ModelMessage>,
    context: &mut Vec<ModelMessage>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    for message in context.drain(..) {
        stock_vec_push(messages, message, io)?;
    }
    Ok(())
}
