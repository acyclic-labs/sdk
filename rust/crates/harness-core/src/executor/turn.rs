use super::*;

pub(super) struct CollectedStep {
    pub(super) calls: Vec<(ToolInvocation, usize)>,
    pub(super) text: String,
    pub(super) metadata: Value,
    /// Whether the provider emitted the step's text as ordered parts.
    pub(super) parts: bool,
}

impl StockExecution<'_> {
    pub(super) async fn execute_progress(
        &self,
        input: &TurnInput,
        journal: &dyn ExecutionJournal,
    ) -> Result<ExecutionProgress<TurnOutput>> {
        let _execution = journal.claim_execution(input.operation_id, self.io)?;
        self.verify_task_context_binding(input.operation_id)?;
        let mut phase = DispatchPhase::new();
        let result = self.execute_turn(input, journal, &mut phase).await;
        phase
            .finish(result, journal, input.operation_id, self.io)
            .await
    }

    async fn execute_turn(
        &self,
        input: &TurnInput,
        journal: &dyn ExecutionJournal,
        phase: &mut DispatchPhase,
    ) -> Result<ExecutionProgress<TurnOutput>> {
        self.validate_turn_input(journal, input).await?;
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(reason) = &self.stock.prompt_block {
            return Err(Error::Invalid(reason.clone()));
        }
        self.ensure_started(journal, input).await?;
        self.begin_working_history().await;
        self.begin_request_continuation().await;
        let mut prior_messages = Vec::new();
        let mut hook_context = Vec::new();
        #[cfg(all(feature = "hooks", feature = "filesystem", not(target_arch = "wasm32")))]
        let mut stop_active = false;
        for step in 0..input.max_steps {
            if let Some(boundary) = journal.model_boundary() {
                let (_, records) = self
                    .model_records(journal, input.operation_id, step, ModelPurpose::Response)
                    .await?;
                let frozen = records.iter().find_map(|record| match record.event {
                    ExecutionEvent::ContextPrepared {
                        step: recorded,
                        steering,
                        ..
                    } if recorded == step => Some(steering),
                    _ => None,
                });
                if matches!(frozen, Some(None)) {
                    return Err(Error::Storage(
                        "agent request has no frozen inbox range".into(),
                    ));
                }
                for message in boundary
                    .messages(input.operation_id, step, frozen.flatten())
                    .await?
                {
                    with_history_io(self.io, |work| {
                        message.content.validate_limits(self.stock.limits, work)
                    })?;
                    stock_vec_push(&mut prior_messages, message, self.io)?;
                }
            }
            let model_events = self
                .run_model_step(
                    journal,
                    input,
                    step,
                    &prior_messages,
                    ModelPurpose::Response,
                    phase,
                )?
                .await?;
            self.retire_working_history(&mut prior_messages).await?;
            let CollectedStep {
                calls,
                text,
                metadata,
                parts,
            } = self.collect_step_events(input, step, model_events, &mut prior_messages)?;
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
                    let answer = if parts { "" } else { text.as_str() };
                    self.continue_after_stop(&mut prior_messages, answer, reason)?;
                    self.retire_step_text(text)?;
                    stop_active = true;
                    continue;
                }
                let transcript = self.settled_transcript(prior_messages, parts, &text)?;
                return Ok(ExecutionProgress::Ready(TurnOutput {
                    text,
                    attachments: Vec::new(),
                    metadata,
                    steps: step + 1,
                    transcript,
                }));
            }
            self.retire_step_text(text)?;
            for (invocation, invocation_index) in stock_into_iter(calls, self.io)? {
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
                    ExecutionProgress::Failed(reason) => {
                        return Ok(ExecutionProgress::Failed(reason));
                    }
                    ExecutionProgress::PreDispatchFailed(failure) => {
                        return Err(failure.into_error());
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

    pub(super) fn has_completed_message(&self, events: &[ModelEvent]) -> Result<bool> {
        for event in events {
            crate::model::charge_items(self.io, 1)?;
            if matches!(event, ModelEvent::MessageCompleted { .. }) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn retire_step_text(&self, text: String) -> Result<()> {
        #[cfg(feature = "filesystem")]
        let custody = self
            .io
            .filter(|_| text.capacity() != 0)
            .map(|io| io.guard_owned(text.capacity() as u64))
            .transpose()?;
        drop(text);
        #[cfg(feature = "filesystem")]
        drop(custody);
        Ok(())
    }

    pub(super) fn collect_step_events(
        &self,
        input: &TurnInput,
        step: u32,
        model_events: Vec<ModelEvent>,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<CollectedStep> {
        let completed_message = self.has_completed_message(&model_events)?;
        let mut parts = false;
        let mut calls = Vec::new();
        let mut text = String::new();
        let mut completed = Value::Null;
        for event in stock_into_iter(model_events, self.io)? {
            match event {
                ModelEvent::Content { delta } if !completed_message => {
                    stock_string_push(
                        &mut text,
                        &delta,
                        self.stock.limits.file_bytes,
                        "assistant output exceeds file limit",
                        self.io,
                    )?;
                }
                ModelEvent::MessageCompleted { text: completed } => {
                    self.retire_step_text(text)?;
                    text = completed;
                }
                ModelEvent::ToolCall(crate::model::ToolCall {
                    call_id,
                    name,
                    arguments,
                    metadata,
                    native,
                }) => {
                    if self.single_request {
                        return Err(stock_boundary(
                            self.io,
                            crate::wire::ErrorCode::Unauthorized,
                            "single inference request attempted a tool call",
                        ));
                    }
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
                    .with_metadata(metadata)
                    .with_native(native);
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
                ModelEvent::TextPart { text, native } => {
                    parts = true;
                    self.append_assistant_part(
                        prior_messages,
                        ModelContentPart::Text { text, native },
                    )?;
                }
                ModelEvent::Content { .. } | ModelEvent::Reasoning { .. } => {}
            }
        }
        Ok(CollectedStep {
            calls,
            text,
            metadata: completed,
            parts,
        })
    }

    /// The turn's transcript after its input, exactly as its requests sent
    /// it, closed by the final reply. Tool notices lead it, as every request
    /// placed them directly after the input.
    fn settled_transcript(
        &self,
        messages: Vec<ModelMessage>,
        parts: bool,
        text: &str,
    ) -> Result<Vec<ModelMessage>> {
        let mut notices = crate::context::Context::default();
        #[cfg(not(target_arch = "wasm32"))]
        {
            let tools = self.request_tools(&messages)?;
            if self.code_composition(&tools)? {
                self.code_tools_notice(&mut notices, &tools)?;
            }
        }
        let mut settled = notices.messages;
        for message in messages {
            stock_vec_push(&mut settled, message, self.io)?;
        }
        if !parts && !text.is_empty() {
            let reply = ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text(stock_string_copy(text, self.io)?),
            };
            stock_vec_push(&mut settled, reply, self.io)?;
        }
        Ok(settled)
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
