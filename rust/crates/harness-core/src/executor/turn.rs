use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StepCompletion {
    Complete,
    Incomplete,
}

pub(super) struct CollectedStep {
    pub(super) calls: Vec<(ToolInvocation, usize)>,
    pub(super) text: String,
    pub(super) metadata: Value,
    pub(super) completion: StepCompletion,
    /// Whether the provider emitted the step's text as ordered parts.
    pub(super) parts: bool,
}

struct OutputCalls<'a>(&'a [(ToolInvocation, usize)]);

impl Serialize for OutputCalls<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for (invocation, _) in self.0 {
            sequence.serialize_element(&(&invocation.name, &invocation.arguments))?;
        }
        sequence.end()
    }
}

#[derive(Default)]
struct OutputDigest {
    text: blake3::Hasher,
    visible: bool,
}

impl OutputDigest {
    fn text(&mut self, value: &str, io: Option<&PreparationIo>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                bytes_hashed: value.len() as u64,
                ..Default::default()
            })?;
        }
        let _ = io;
        self.visible |= !value.is_empty();
        self.text.update(value.as_bytes());
        Ok(())
    }

    // Page boundaries must not alter the semantic output hash. Keep only the
    // incremental hash across pages, without copying or rescanning history.
    fn include(
        &mut self,
        page: &CollectedStep,
        parts: &[ModelMessage],
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if !page.text.is_empty() || !page.parts {
            self.text(&page.text, io)?;
        } else {
            for part in parts {
                crate::model::charge_items(io, 1)?;
                if let ModelContent::Part(ModelContentPart::Text { text, .. }) = &part.content {
                    self.text(text, io)?;
                }
            }
        }
        Ok(())
    }

    fn finish(
        &mut self,
        calls: &[(ToolInvocation, usize)],
        io: Option<&PreparationIo>,
    ) -> Result<Option<[u8; 32]>> {
        let text = *std::mem::take(&mut self.text).finalize().as_bytes();
        if !std::mem::take(&mut self.visible) && calls.is_empty() {
            return Ok(None);
        }
        with_history_io(io, |work| {
            json::canonical_json_digest(
                &("harness:model-output:v2", text, OutputCalls(calls)),
                None,
                work,
            )
            .map(Some)
        })
    }
}
impl CollectedStep {
    fn retained_payload_bytes(&self) -> Result<u64> {
        let slots = self
            .calls
            .capacity()
            .checked_mul(std::mem::size_of::<(ToolInvocation, usize)>())
            .ok_or_else(|| {
                stock_boundary(
                    None,
                    crate::wire::ErrorCode::Invalid,
                    "call storage overflows",
                )
            })? as u64;
        let calls = self.calls.iter().try_fold(slots, |bytes, (call, _)| {
            crate::core::owned_sum([bytes, call.retained_payload_bytes()?])
        })?;
        crate::core::owned_sum([
            calls,
            self.text.capacity() as u64,
            json::json_value_retained_owned_bytes(&self.metadata)?,
        ])
    }
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
        let result =
            ExecutionProgress::controlled(self.execute_turn(input, journal, &mut phase).await);
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
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(reason) = &self.stock.prompt_block {
            return Err(Error::Invalid(reason.clone()));
        }
        self.ensure_started(journal, input).await?;
        self.begin_working_history().await;
        self.begin_request_continuation().await;
        let mut prior_messages = Vec::new();
        if let Some(crate::agent::ContinuationOverride::Instruction { text }) =
            self.stock.intervention.as_deref()
        {
            stock_vec_push(
                &mut prior_messages,
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(stock_string_copy(text, self.io)?),
                },
                self.io,
            )?;
        }
        let mut steering = working_tail::retirement::SteeringHistory::default();
        let mut hook_context = Vec::new();
        let mut unfinished = String::new();
        let mut output = OutputDigest::default();
        #[cfg(not(target_arch = "wasm32"))]
        let mut stop_active = false;
        #[cfg(not(target_arch = "wasm32"))]
        let mut stop_check = None;
        #[cfg(not(target_arch = "wasm32"))]
        let mut after_answer = None;
        let mut steps = 0_u32;
        let progress = async {
            while input.max_steps.is_none_or(|maximum| steps < maximum) {
                if let Some(control) = journal.supervision().await? {
                    return Ok(ExecutionProgress::Controlled(control));
                }
                let step = steps;
                steps = steps.checked_add(1).ok_or_else(|| {
                    stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "model step identity exhausted",
                    )
                })?;
                let model_events = async {
                    self.run_model_step(
                        journal,
                        input,
                        step,
                        request::ModelStepRequest {
                            history: request::ModelStepHistory::Turn(
                                &mut prior_messages,
                                &mut steering,
                            ),
                            prepared: None,
                            publish_reply: {
                                #[cfg(not(target_arch = "wasm32"))]
                                {
                                    after_answer.is_none()
                                }
                                #[cfg(target_arch = "wasm32")]
                                {
                                    true
                                }
                            },
                        },
                        ModelPurpose::Response,
                        phase,
                    )?
                    .await
                }
                .await;
                let model_events = match model_events {
                    Ok(events) => events,
                    Err(error) => {
                        #[cfg(not(target_arch = "wasm32"))]
                        if after_answer.is_some()
                            && error.is_model_failure()
                            && self
                                .stop_allows_completion(
                                    input.operation_id,
                                    step,
                                    stop_active,
                                    stop_check,
                                )
                                .await?
                        {
                            return Ok(ExecutionProgress::Ready((
                                String::new(),
                                Value::Null,
                                true,
                            )));
                        }
                        return Err(error);
                    }
                };
                self.retire_working_history(&mut prior_messages, Some(&mut steering))
                    .await?;
                if let Some(control) = journal.supervision().await? {
                    return Ok(ExecutionProgress::Controlled(control));
                }
                let part_start = prior_messages.len();
                let collected =
                    self.collect_step_events(input, step, model_events, &mut prior_messages)?;
                output.include(&collected, &prior_messages[part_start..], self.io)?;
                let CollectedStep {
                    calls,
                    text,
                    metadata,
                    completion,
                    parts,
                } = collected;
                if completion == StepCompletion::Incomplete {
                    if !parts {
                        self.append_assistant_part(
                            &mut prior_messages,
                            ModelContentPart::text(stock_string_copy(&text, self.io)?),
                            self.io,
                        )?;
                    }
                    stock_string_push(
                        &mut unfinished,
                        &text,
                        self.stock.limits.file_bytes,
                        "assistant output exceeds file limit",
                        self.io,
                    )?;
                    stock_retire_text(text, self.io)?;
                    stock_retire_event(ModelEvent::Completed { metadata }, self.io)?;
                    continue;
                }
                let digest = output.finish(&calls, self.io)?;
                let key = with_history_io(self.io, |work| {
                    preparation_format(format_args!("model:{step}:output"), work)
                })?;
                #[cfg(not(target_arch = "wasm32"))]
                let _key_hold = self
                    .io
                    .map(|io| io.guard_owned(key.capacity() as u64))
                    .transpose()?;
                let append = crate::conversation::BoundedContentFuture::new(self.io, || async {
                    journal
                        .append(
                            input.operation_id,
                            &key,
                            &ExecutionEvent::ModelOutput { step, digest },
                            self.io,
                        )?
                        .await
                })?;
                phase::PreparedPhase::new(journal, self.io)?
                    .run(append)
                    .await?;
                if let Some(control) = journal.supervision().await? {
                    return Ok(ExecutionProgress::Controlled(control));
                }
                if calls.is_empty() {
                    // An explicit Stop denial supersedes the candidate answer.
                    #[cfg(not(target_arch = "wasm32"))]
                    let stop =
                        Box::pin(self.stop_hook(input.operation_id, step, stop_active)).await?;
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        stop_check = Some((step, stop.is_none()));
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(reason) = stop {
                        let answer = if parts { "" } else { text.as_str() };
                        self.continue_after_stop(&mut prior_messages, answer, reason)?;
                        stock_retire_text(text, self.io)?;
                        stock_retire_text(std::mem::take(&mut unfinished), self.io)?;
                        stop_active = true;
                        continue;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    if after_answer.is_none() {
                        let mut notices = Vec::new();
                        for hook in &self.stock.after_hooks {
                            for text in hook.settlement() {
                                let text = stock_string_copy(text, self.io)?;
                                stock_vec_push(&mut notices, text, self.io)?;
                            }
                        }
                        if !notices.is_empty() {
                            let answer = if parts { "" } else { text.as_str() };
                            self.continue_after_stop(
                                &mut prior_messages,
                                answer,
                                crate::hooks::HookOutput {
                                    additional_context: notices,
                                    ..Default::default()
                                },
                            )?;
                            // Audit replies such as "none" do not replace the
                            // already accepted task answer. Its original text is
                            // retained while audit tools and model replies drain.
                            after_answer = Some((
                                self.finish_assistant_output(&mut unfinished, text)?,
                                metadata,
                            ));
                            continue;
                        }
                    }
                    return Ok(ExecutionProgress::Ready((text, metadata, parts)));
                }
                stock_retire_text(text, self.io)?;
                stock_retire_text(std::mem::take(&mut unfinished), self.io)?;
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
                        ExecutionProgress::Controlled(control) => {
                            return Ok(ExecutionProgress::Controlled(control));
                        }
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
            #[cfg(not(target_arch = "wasm32"))]
            if after_answer.is_some()
                && self
                    .stop_allows_completion(input.operation_id, steps - 1, stop_active, stop_check)
                    .await?
            {
                return Ok(ExecutionProgress::Ready((String::new(), Value::Null, true)));
            }
            Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "executor step limit reached",
            ))
        }
        .await?;
        match progress {
            ExecutionProgress::Ready((text, metadata, parts)) => {
                let transcript =
                    self.settled_transcript(steering.into_retired(), prior_messages, parts, &text)?;
                let text = self.finish_assistant_output(&mut unfinished, text)?;
                #[cfg(not(target_arch = "wasm32"))]
                let (text, metadata) = match after_answer {
                    Some(answer) => {
                        stock_retire_text(text, self.io)?;
                        answer
                    }
                    None => (text, metadata),
                };
                Ok(ExecutionProgress::Ready(TurnOutput {
                    #[cfg(feature = "research")]
                    journal_cut: phase.1.take(),
                    text,
                    attachments: Vec::new(),
                    metadata,
                    steps,
                    transcript,
                }))
            }
            ExecutionProgress::Controlled(control) => Ok(ExecutionProgress::Controlled(control)),
            ExecutionProgress::Pending(wait) => Ok(ExecutionProgress::Pending(wait)),
            ExecutionProgress::Failed(reason) => Ok(ExecutionProgress::Failed(reason)),
            ExecutionProgress::PreDispatchFailed(failure) => Err(failure.into_error()),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn stop_allows_completion(
        &self,
        operation: OperationId,
        step: u32,
        active: bool,
        checked: Option<(u32, bool)>,
    ) -> Result<bool> {
        if let Some((checked_step, allowed)) = checked
            && checked_step == step
        {
            return Ok(allowed);
        }
        Ok(Box::pin(self.stop_hook(operation, step, active))
            .await?
            .is_none())
    }
    fn finish_assistant_output(&self, unfinished: &mut String, text: String) -> Result<String> {
        if unfinished.is_empty() {
            stock_retire_text(std::mem::take(unfinished), self.io)?;
            return Ok(text);
        }
        stock_string_push(
            unfinished,
            &text,
            self.stock.limits.file_bytes,
            "assistant output exceeds file limit",
            self.io,
        )?;
        stock_retire_text(text, self.io)?;
        Ok(std::mem::take(unfinished))
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

    pub(super) fn collect_step_events(
        &self,
        input: &TurnInput,
        step: u32,
        model_events: Vec<ModelEvent>,
        prior_messages: &mut Vec<ModelMessage>,
    ) -> Result<CollectedStep> {
        let scope = crate::filesystem::PreparationScope::with_input(
            self.io,
            self.io
                .map(|_| stock_events_payload_bytes(&model_events))
                .transpose()?
                .unwrap_or(0),
        )?;
        let io = scope.io();
        // Inputs and all unpublished outputs die before their shared custody.
        let model_events = model_events;
        let completed_message = self.has_completed_message(&model_events)?;
        let completion = if model_events.iter().any(|event| {
            matches!(event, ModelEvent::Completed { metadata } if metadata["finish_reason"] == "length")
        }) {
            StepCompletion::Incomplete
        } else {
            StepCompletion::Complete
        };
        let mut parts = false;
        let mut calls = Vec::new();
        let mut text = String::new();
        let mut completed = Value::Null;
        for event in stock_into_iter(model_events, io)? {
            match event {
                ModelEvent::Content { delta } if !completed_message => {
                    stock_string_push(
                        &mut text,
                        &delta,
                        self.stock.limits.file_bytes,
                        "assistant output exceeds file limit",
                        io,
                    )?;
                    stock_retire_text(delta, io)?;
                }
                ModelEvent::MessageCompleted { text: completed } => {
                    stock_retire_text(text, io)?;
                    text = completed;
                }
                event @ ModelEvent::ToolCall(_) if completion == StepCompletion::Incomplete => {
                    stock_retire_event(event, io)?;
                }
                ModelEvent::ToolCall(call) => {
                    if self.single_request {
                        return Err(stock_boundary(
                            io,
                            crate::wire::ErrorCode::Unauthorized,
                            "single inference request attempted a tool call",
                        ));
                    }
                    let crate::model::ToolCall {
                        call_id,
                        name,
                        arguments,
                        metadata,
                        native,
                    } = call;
                    let invocation = ToolInvocation::for_model_call(
                        input.operation_id,
                        step,
                        call_id,
                        name,
                        arguments,
                        self.native_work(),
                    )?;
                    let call = crate::model::ToolCall::new(
                        stock_string_copy(&invocation.call_id, io)?,
                        stock_string_copy(&invocation.name, io)?,
                        crate::model::clone_value(&invocation.arguments, io)?,
                    )
                    .with_metadata(metadata)
                    .with_native(native);
                    let index = prior_messages.len();
                    self.append_assistant_part(
                        prior_messages,
                        ModelContentPart::ToolCall(call),
                        io,
                    )?;
                    stock_vec_push(&mut calls, (invocation, index), io)?;
                }
                ModelEvent::Completed { metadata } => {
                    stock_retire_event(
                        ModelEvent::Completed {
                            metadata: completed,
                        },
                        io,
                    )?;
                    completed = metadata;
                }
                ModelEvent::ReasoningBlock { block } => {
                    self.append_assistant_part(
                        prior_messages,
                        ModelContentPart::Reasoning { block },
                        io,
                    )?;
                }
                ModelEvent::TextPart { text, native } => {
                    parts = true;
                    self.append_assistant_part(
                        prior_messages,
                        ModelContentPart::Text { text, native },
                        io,
                    )?;
                }
                event @ (ModelEvent::Restarted
                | ModelEvent::Wait(_)
                | ModelEvent::Trace { .. }
                | ModelEvent::Counted { .. }
                | ModelEvent::Content { .. }
                | ModelEvent::Reasoning { .. }) => {
                    stock_retire_event(event, io)?;
                }
            }
        }
        let collected = CollectedStep {
            calls,
            text,
            metadata: completed,
            completion,
            parts,
        };
        scope.finish(
            io.map(|_| collected.retained_payload_bytes())
                .transpose()?
                .unwrap_or(0),
        )?;
        Ok(collected)
    }

    /// The surviving working tail and final reply, preceded by any steering
    /// retired during compaction. Tool notices lead it, as every request
    /// placed them directly after the input. Steering in the live tail stays
    /// at its original position and is never duplicated.
    pub(super) fn settled_transcript(
        &self,
        retired_steering: Vec<ModelMessage>,
        messages: Vec<ModelMessage>,
        parts: bool,
        text: &str,
    ) -> Result<Vec<ModelMessage>> {
        let mut settled = Vec::new();
        for message in retired_steering {
            stock_vec_push(&mut settled, message, self.io)?;
        }
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
        source: Option<&PreparationIo>,
    ) -> Result<()> {
        let content = ModelContent::Part(part);
        let retained = source
            .map(|_| content.retained_payload_bytes())
            .transpose()?
            .unwrap_or(0);
        let scope = crate::filesystem::PreparationScope::with_input(source, retained)?;
        let io = scope.io();
        // The incoming message dies before its custody if validation or storage
        // admission fails; existing history never moves into this scope.
        let message = ModelMessage {
            role: ModelRole::Assistant,
            content,
        };
        with_history_io(io, |work| {
            message.content.validate_limits(self.stock.limits, work)
        })?;
        stock_vec_prepare_push(messages, self.io)?;
        scope.finish_into(self.io, retained)?;
        // Storage and custody are admitted; insertion cannot fail or allocate.
        messages.push(message);
        Ok(())
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
