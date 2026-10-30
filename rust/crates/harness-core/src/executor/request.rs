use super::*;

impl StockExecution<'_> {
    /// Runs one admitted model step independently of the stock control loop.
    pub async fn model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<Vec<ModelEvent>> {
        let _execution = journal.claim_execution(input.operation_id, self.io)?;
        let phase = &mut DispatchPhase::new();
        self.verify_task_context_binding(input.operation_id)?;
        self.validate_turn_input(journal, input).await?;
        if step >= input.max_steps {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "model step is outside admitted turn bounds",
            ));
        }
        self.ensure_started(journal, input).await?;
        self.run_model_step(
            journal,
            input,
            step,
            prior_messages,
            ModelPurpose::Response,
            phase,
        )?
        .await
    }

    pub(super) fn run_model_step<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        input: &'a TurnInput,
        step: u32,
        prior_messages: &'a [ModelMessage],
        purpose: ModelPurpose,
        phase: &'a mut DispatchPhase,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelEvent>>> {
        crate::conversation::BoundedContentFuture::new(self.io, || {
            let span = obs_span!(
                "acyclic.harness.executor.model_step",
                step = step,
                model = self.stock.model.name.as_str(),
                phase = crate::obs::Empty,
                items = crate::obs::Empty,
            );
            #[cfg(not(target_arch = "wasm32"))]
            {
                crate::obs::traced_future(span, async move {
                    self.cancellable_model_step(
                        journal,
                        input,
                        step,
                        prior_messages,
                        purpose,
                        phase,
                    )
                    .await
                    .and_then(|events| completed_model_events(events, input.operation_id))
                })
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = span;
                async move {
                    let events = self
                        .cancellable_model_step(
                            journal,
                            input,
                            step,
                            prior_messages,
                            purpose,
                            phase,
                        )
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
        let base = ContextPipeline::base_context(&context_input, self.stock.limits, self.io)?;
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
    ) -> Result<crate::context::Context> {
        let mut context_input = ContextInput::copy_for_turn(input, step, prior_messages, self.io)?;
        self.canonical_base_for_input(journal, &mut context_input)
            .await
    }

    pub(super) async fn canonical_base_for_input(
        &self,
        journal: &dyn ExecutionJournal,
        context_input: &mut ContextInput,
    ) -> Result<crate::context::Context> {
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
        ContextPipeline::continue_base(retained, delta, self.stock.limits, self.io)
    }

    #[cfg(test)]
    pub(super) async fn transform_projection(
        &self,
        input: &ContextInput,
        base: crate::context::Context,
    ) -> Result<crate::context::Context> {
        let (mut context, prepared) = self.prepare_transformed_request(input, base).await?;
        context.messages = prepared.into_messages();
        Ok(context)
    }

    pub(super) async fn prepare_transformed_request(
        &self,
        input: &ContextInput,
        base: crate::context::Context,
    ) -> Result<(crate::context::Context, crate::model::PreparedModelRequest)> {
        let mut context = self
            .context_pipeline()
            .transform_required(input, base, self.stock.limits, self.io)
            .await?;
        let message_count = context.messages.len();
        let request = self.request_from_messages(std::mem::take(&mut context.messages))?;
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
                let request_bytes = json::json_byte_length(&request, u64::MAX, work)?;
                let messages = json::json_byte_length(&request.messages, u64::MAX, work)?;
                Ok::<_, Error>(request_bytes - messages)
            })?
        } else {
            0
        };
        context.messages = std::mem::take(&mut request.messages);
        self.context_pipeline().project_optional(
            &mut context,
            prefix_count,
            self.stock.limits,
            request_frame,
            self.io,
        )?;
        if prefix_count != 0 || self.context_pipeline().has_optional() {
            crate::context::validate_projected_context(&context, self.stock.limits, self.io)?;
        }
        request.messages = std::mem::take(&mut context.messages);
        let prepared =
            crate::model::PreparedModelRequest::prepare(request, self.stock.limits, self.io)?;
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
        let tools_required = self.request_tools_required(&messages)?;
        Ok(ModelRequest {
            model: crate::model::clone_model(&self.stock.model, self.io)?,
            messages,
            tools: if tools_required {
                stock_tool_definitions(&self.stock.tools, &self.stock.tool_scope, self.io)?
            } else {
                Vec::new()
            },
            max_output_tokens: self.output_budget()?,
        })
    }

    fn request_tools_required(&self, messages: &[ModelMessage]) -> Result<bool> {
        let mut tools_required = self.summary.is_none();
        if !tools_required {
            for message in messages {
                crate::model::charge_items(self.io, 1)?;
                for part in message.content.parts() {
                    crate::model::charge_items(self.io, 1)?;
                    if matches!(part, crate::model::ModelContentPart::ToolCall(_)) {
                        tools_required = true;
                        break;
                    }
                }
                if tools_required {
                    break;
                }
            }
        }
        Ok(tools_required)
    }

    pub(super) fn validate_response_request(
        &self,
        prepared: &crate::model::PreparedModelRequest,
    ) -> Result<()> {
        let request = prepared.request();
        #[cfg(feature = "filesystem")]
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        #[cfg(feature = "filesystem")]
        let io = scope.io();
        #[cfg(not(feature = "filesystem"))]
        let io = self.io;
        let tools = if self.request_tools_required(&request.messages)? {
            stock_tool_definitions(&self.stock.tools, &self.stock.tool_scope, io)?
        } else {
            Vec::new()
        };
        if request.model != self.stock.model
            || request.tools != tools
            || request.max_output_tokens != self.output_budget()?
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
