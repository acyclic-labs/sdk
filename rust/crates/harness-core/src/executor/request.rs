use super::*;

/// The response loop owns growing history; standalone and summary calls borrow
/// their already selected history without collecting another inbox boundary.
pub(super) enum ModelStepHistory<'a> {
    Turn(
        &'a mut TurnHistory,
        &'a mut working_tail::retirement::SteeringHistory,
    ),
    Selected(&'a [ModelMessage]),
}

impl ModelStepHistory<'_> {
    pub(super) fn messages(&self) -> &[ModelMessage] {
        match self {
            Self::Turn(history, _) => history.messages(),
            Self::Selected(messages) => messages,
        }
    }

    pub(super) fn append_configuration(
        &mut self,
        update: crate::model::ConfigurationUpdate,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Self::Turn(history, retained) = self {
            let from = history.messages().len();
            history.push_message(update.message(), io)?;
            retained.admit(from..history.messages().len(), io)?;
        }
        Ok(())
    }
}

/// Append-only access to a step's growing history: the only change a reply,
/// tool result or hook makes, so an admission of the existing messages holds.
pub(super) trait AppendHistory: Send {
    fn messages(&self) -> &[ModelMessage];
    fn push_message(&mut self, message: ModelMessage, io: Option<&PreparationIo>) -> Result<()>;
    /// Admits storage for one message, so the next [`Self::push_reserved`]
    /// can neither fail nor allocate.
    fn reserve_message(&mut self, io: Option<&PreparationIo>) -> Result<()>;
    fn push_reserved(&mut self, message: ModelMessage);
}

impl AppendHistory for Vec<ModelMessage> {
    fn messages(&self) -> &[ModelMessage] {
        self
    }
    fn push_message(&mut self, message: ModelMessage, io: Option<&PreparationIo>) -> Result<()> {
        stock_vec_push(self, message, io)
    }
    fn reserve_message(&mut self, io: Option<&PreparationIo>) -> Result<()> {
        stock_vec_prepare_push(self, io)
    }
    fn push_reserved(&mut self, message: ModelMessage) {
        self.push(message);
    }
}

/// A stock turn's history and the one admission of its canonical base. The
/// history only appends, so each step bounds only the messages appended since
/// the admission. Retiring a prefix, the one rewrite, drops the admission
/// first, and settlement consumes the history.
#[derive(Default)]
pub(super) struct TurnHistory {
    messages: Vec<ModelMessage>,
    base: Option<CarriedBase>,
}

/// The admitted base of one operation's input and the messages it covers.
struct CarriedBase {
    operation: OperationId,
    base: crate::context::AdmittedContext,
    covered: usize,
}

impl From<Vec<ModelMessage>> for TurnHistory {
    fn from(messages: Vec<ModelMessage>) -> Self {
        Self {
            messages,
            base: None,
        }
    }
}

impl TurnHistory {
    pub(super) fn messages(&self) -> &[ModelMessage] {
        &self.messages
    }

    /// Mutable positions, after the admission that relied on them is dropped.
    pub(super) fn rewrite(&mut self) -> &mut Vec<ModelMessage> {
        self.base = None;
        &mut self.messages
    }

    pub(super) fn into_messages(self) -> Vec<ModelMessage> {
        self.messages
    }

    /// Appends `messages` as one insertion, all of them or none.
    pub(super) fn push_batch(
        &mut self,
        messages: Vec<ModelMessage>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let messages = stock_into_iter(messages, io)?;
        let mut context = crate::context::Context {
            messages: std::mem::take(&mut self.messages),
            ..Default::default()
        };
        let end = context.messages.len();
        let result =
            crate::context::ContextInsertion::insert_batch(messages, end, &mut context, 0, io);
        self.messages = context.messages;
        result
    }

    /// The stage input and canonical base of `turn` over this history, for a
    /// selection without a checkpoint. The admission carried from the last
    /// call is extended by only the messages appended since; the first call
    /// or another operation assembles the base once.
    pub(super) fn base(
        &mut self,
        turn: &TurnInput,
        step: u32,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<(ContextInput, crate::context::AdmittedContext)> {
        let input = ContextInput::copy_for_turn(turn, step, &self.messages, io)?;
        let base = match self.base.take() {
            Some(carried) if carried.operation == turn.operation_id => {
                let appended = self.messages.get(carried.covered..).ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "carried base exceeds its turn history",
                    )
                })?;
                let appended = crate::model::clone_messages(appended, io)?;
                carried
                    .base
                    .under(limits, io)?
                    .extend(stock_into_iter(appended, io)?, io)?
            }
            _ => ContextPipeline::base_context(&input, limits, io)?,
        };
        let copy = base.copy(io)?;
        self.base = Some(CarriedBase {
            operation: turn.operation_id,
            base,
            covered: self.messages.len(),
        });
        Ok((input, copy))
    }
}

impl AppendHistory for TurnHistory {
    fn messages(&self) -> &[ModelMessage] {
        &self.messages
    }
    fn push_message(&mut self, message: ModelMessage, io: Option<&PreparationIo>) -> Result<()> {
        stock_vec_push(&mut self.messages, message, io)
    }
    fn reserve_message(&mut self, io: Option<&PreparationIo>) -> Result<()> {
        stock_vec_prepare_push(&mut self.messages, io)
    }
    fn push_reserved(&mut self, message: ModelMessage) {
        self.messages.push(message);
    }
}

/// An optional admission owner moves into the same dispatch path as its history.
pub(super) struct ModelStepRequest<'a> {
    pub history: ModelStepHistory<'a>,
    pub prepared: Option<crate::model::PreparedModelRequest>,
    pub publish_reply: bool,
}

impl StockExecution<'_> {
    /// Runs one admitted model step independently of the stock control loop.
    pub async fn model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<Vec<ModelEvent>> {
        let execution = journal.claim_execution(input.operation_id, self.io)?;
        let phase = &mut DispatchPhase::new();
        self.verify_task_context_binding(input.operation_id)?;
        self.validate_turn_input(journal, input).await?;
        if input.max_steps.is_some_and(|maximum| step >= maximum) {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "model step is outside admitted turn bounds",
            ));
        }
        self.ensure_started(journal, input).await?;
        let events = self
            .run_model_step(
                journal,
                input,
                step,
                ModelStepRequest {
                    history: ModelStepHistory::Selected(prior_messages),
                    prepared: None,
                    publish_reply: true,
                },
                ModelPurpose::Response,
                phase,
            )?
            .await?;
        execution.release();
        Ok(events)
    }

    pub(super) fn run_model_step<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        input: &'a TurnInput,
        step: u32,
        request: ModelStepRequest<'a>,
        purpose: ModelPurpose,
        phase: &'a mut DispatchPhase,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelEvent>>> {
        crate::conversation::BoundedContentFuture::new(self.io, || {
            let span = obs_span!(
                "acyclic.harness.executor.model_step",
                step = step,
                model = self.selected_model().name.as_str(),
                phase = crate::obs::Empty,
                items = crate::obs::Empty,
            );
            #[cfg(not(target_arch = "wasm32"))]
            {
                crate::obs::traced_future(span, async move {
                    self.cancellable_model_step(journal, input, step, request, purpose, phase)
                        .await
                        .and_then(|events| completed_model_events(events, input.operation_id))
                })
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = span;
                async move {
                    let events = self
                        .cancellable_model_step(journal, input, step, request, purpose, phase)
                        .await?;
                    completed_model_events(events, input.operation_id)
                }
            }
        })
    }

    pub(super) async fn prepare_request(
        &self,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<crate::model::PreparedModelRequest> {
        let context_input = ContextInput::copy_for_turn(input, step, prior_messages, self.io)?;
        let mut base = ContextPipeline::base_context(&context_input, self.stock.limits, self.io)?;
        if self.summary.is_some()
            && let Some(update) = self.configuration_delta(&base.context().messages)?
        {
            base = base.push(update.message(), self.io)?;
        }
        self.prepare_transformed_request(&context_input, base)
            .await
            .map(|(_, prepared)| prepared)
    }

    #[cfg(test)]
    pub(super) async fn canonical_base(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<crate::context::AdmittedContext> {
        let mut context_input = ContextInput::copy_for_turn(input, step, prior_messages, self.io)?;
        self.canonical_base_for_input(journal, &mut context_input)
            .await
    }

    pub(super) async fn canonical_base_for_input(
        &self,
        journal: &dyn ExecutionJournal,
        context_input: &mut ContextInput,
    ) -> Result<crate::context::AdmittedContext> {
        // A stage observes the same input used to assemble its base. Temporarily
        // remove the owner-resolved checkpoint, then restore its exact reference
        // before any fallible load or stage can observe the input.
        let checkpoint = context_input
            .selected_context
            .as_mut()
            .and_then(|selected| selected.selection.checkpoint.take());
        let delta = ContextPipeline::base_context(context_input, self.stock.limits, self.io);
        if let Some(selected) = context_input.selected_context.as_mut() {
            selected.selection.checkpoint = checkpoint;
        }
        let delta = delta?;
        let Some(selected) = context_input.selected_context.as_ref() else {
            return Ok(delta);
        };
        let Some(reference) = selected.selection.checkpoint.as_ref() else {
            return Ok(delta);
        };
        let (envelope, retained) = load_canonical_checkpoint(
            journal,
            reference,
            self.stock.limits,
            self.io.map(|_| &self.stock.tool_scope),
            self.io,
        )
        .await?;
        if selected.selection.conversation_revision <= envelope.selection.conversation_revision {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "checkpoint covers the new turn input",
            ));
        }
        let retained =
            crate::context::AdmittedContext::admit(retained, self.stock.limits, self.io)?;
        ContextPipeline::continue_base(retained, delta, self.stock.limits, self.io)
    }

    #[cfg(test)]
    pub(super) async fn transform_projection(
        &self,
        input: &ContextInput,
        base: crate::context::AdmittedContext,
    ) -> Result<crate::context::Context> {
        let (mut context, prepared) = self.prepare_transformed_request(input, base).await?;
        context.messages = prepared.into_messages();
        Ok(context)
    }

    pub(super) async fn prepare_transformed_request(
        &self,
        input: &ContextInput,
        base: crate::context::AdmittedContext,
    ) -> Result<(crate::context::Context, crate::model::PreparedModelRequest)> {
        let before = base.context().current_input_index.unwrap_or(0);
        let mut context = self
            .context_pipeline()
            .transform_required(input, base, self.stock.limits, self.io)
            .await?
            .into_context();
        let prepended = context
            .current_input_index
            .unwrap_or(0)
            .saturating_sub(before) as usize;
        let tools = self.request_tools()?;
        let compose_tools = self.code_composition(&tools)?;
        let message_count = context.messages.len();
        let request =
            self.request_from_parts(std::mem::take(&mut context.messages), tools, compose_tools)?;
        let mut request = if let Some((prefix, verifier)) = self.inherited_prefix() {
            crate::model::PreparedModelRequest::inherit_request(
                request,
                prefix,
                verifier.as_ref(),
                self.stock.limits,
                self.io,
            )
            .await?
        } else {
            request
        };
        let prefix_count = request.messages.len() - message_count;
        if let Some(index) = context.current_input_index {
            context.current_input_index = Some(
                u32::try_from(prefix_count)
                    .ok()
                    .and_then(|count| index.checked_add(count))
                    .ok_or_else(|| {
                        stock_boundary(
                            self.io,
                            crate::wire::ErrorCode::Invalid,
                            "inherited input position exceeds portable count",
                        )
                    })?,
            );
        }
        let request_frame = if self.context_pipeline().has_optional() {
            with_history_io(self.io, |work| {
                let request_bytes = json::json_byte_length(&request, None, work)?;
                let messages = json::json_byte_length(&request.messages, None, work)?;
                Ok::<_, Error>(request_bytes - messages)
            })?
        } else {
            0
        };
        context.messages = std::mem::take(&mut request.messages);
        #[cfg(not(target_arch = "wasm32"))]
        if self.summary.is_none() {
            // Compare against the inherited table, preserving the shared
            // prefix and announcing only an actual capability change.
            self.tools_notice(&mut context, &request.tools)?;
        }
        self.context_pipeline().project_optional(
            &mut context,
            prefix_count,
            prepended,
            self.stock.limits,
            request_frame,
            self.io,
        )?;
        request.messages = std::mem::take(&mut context.messages);
        // The request admission bounds every message, including the inherited
        // prefix and optional insertions; only projection metadata remains.
        let prepared =
            crate::model::PreparedModelRequest::prepare(request, self.stock.limits, self.io)?;
        crate::context::validate_projected_request(&context, &prepared, self.io)?;
        Ok((context, prepared))
    }

    pub(super) fn request_from_context(
        &self,
        context: &crate::context::Context,
    ) -> Result<ModelRequest> {
        self.request_from_messages(crate::model::clone_messages(&context.messages, self.io)?)
    }

    #[cfg(test)]
    pub(super) fn request_taking_context(
        &self,
        context: &mut crate::context::Context,
    ) -> Result<ModelRequest> {
        if self.io.is_some() {
            return self.request_from_context(context);
        }
        self.request_from_messages(std::mem::take(&mut context.messages))
    }

    pub(super) fn request_from_messages(
        &self,
        messages: Vec<ModelMessage>,
    ) -> Result<ModelRequest> {
        let tools = self.request_tools()?;
        let compose_tools = self.code_composition(&tools)?;
        self.request_from_parts(messages, tools, compose_tools)
    }

    pub(super) fn request_tools(&self) -> Result<Vec<crate::tool::ToolDefinition>> {
        // Native adapters bind after the epoch's canonical selection. Their
        // declarations and executors are one pair; never replace just a schema.
        stock_tool_definitions(self.selected_tools(), &self.stock.tool_scope, self.io)
    }

    fn request_from_parts(
        &self,
        messages: Vec<ModelMessage>,
        tools: Vec<crate::tool::ToolDefinition>,
        compose_tools: bool,
    ) -> Result<ModelRequest> {
        Ok(ModelRequest {
            summary: self.summary.is_some(),
            compose_tools,
            model: crate::model::clone_model(self.selected_model(), self.io)?,
            messages,
            tools,
            sampling: self.sampling()?,
            // Assembly preserves settings. Turn entry and fresh dispatch own
            // provider admission with the original work, including its count.
        })
    }

    pub(super) fn code_composition(&self, tools: &[crate::tool::ToolDefinition]) -> Result<bool> {
        Ok(!cfg!(target_arch = "wasm32")
            && tools.iter().any(|tool| tool.name == "code")
                & self
                    .selected_tools()
                    .get("code", None, self.io)?
                    .is_some_and(|tool| tool.executor.is_code_mode()))
    }

    pub(super) fn validate_response_request(
        &self,
        prepared: &crate::model::PreparedModelRequest,
        tools: &[crate::tool::ToolDefinition],
    ) -> Result<()> {
        let request = prepared.request();
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        let io = scope.io();
        // The authenticated prepared request owns its historical live tables.
        // Comparing those schemas with today's catalog would revoke a saved
        // result after removal. Fixed declarations must still match exactly.
        let historical = request.messages.iter().any(|message| {
            message.content.notice_parts().is_some_and(|(notices, _)| {
                notices.iter().any(|notice| {
                    notice.source == "tools"
                        && matches!(notice.kind.as_str(), "tools.changed" | "tools.bound")
                })
            })
        });
        let definitions_match = request.compose_tools
            || if historical {
                self.selected_tools()
                    .fixed_definitions(self.stock.tool_scope.grants(), io)?
                    .iter()
                    .all(|fixed| {
                        request
                            .tools
                            .binary_search_by(|retained| retained.name.cmp(&fixed.name))
                            .ok()
                            .and_then(|index| request.tools.get(index))
                            == Some(fixed)
                    })
            } else {
                request.tools == tools
            };
        if request.summary != self.summary.is_some()
            || request.model != *self.selected_model()
            || (!request.compose_tools && !definitions_match)
            || request.compose_tools != self.code_composition(tools)?
            || request.sampling != self.sampling()?
        {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "retained response request differs from selected model or tools",
            ));
        }
        Ok(())
    }
}

fn completed_model_events(
    events: Vec<ModelEvent>,
    operation_id: OperationId,
) -> Result<Vec<ModelEvent>> {
    if !matches!(events.last(), Some(ModelEvent::Completed { .. })) {
        return Err(Error::Indeterminate(operation_id));
    }
    Ok(events)
}
