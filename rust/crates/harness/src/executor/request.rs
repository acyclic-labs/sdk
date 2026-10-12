use super::*;

impl StockExecution<'_> {
    pub(super) async fn verify_execution_owner(&self) -> Result<()> {
        if let Some((host, task_id, fence)) = &self.stock.task {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = self.native_work() {
                return host
                    .verify_execution_owner_with_native_work(*task_id, fence, work)?
                    .await;
            }
            host.verify_execution_owner(*task_id, fence.clone()).await?;
        }
        Ok(())
    }

    /// Runs one admitted model step independently of the stock control loop.
    pub async fn model_step(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<Vec<ModelEvent>> {
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
        self.run_model_step(journal, input, step, prior_messages, ModelPurpose::Response)?
            .await
    }

    pub(super) fn run_model_step<'a>(
        &'a self,
        journal: &'a dyn ExecutionJournal,
        input: &'a TurnInput,
        step: u32,
        prior_messages: &'a [ModelMessage],
        purpose: ModelPurpose,
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
                let operation = span.clone();
                span.scope(async move {
                    let result = self
                        .dispatch_model_step(journal, input, step, prior_messages, purpose)
                        .await;
                    operation.record("outcome", if result.is_ok() { "ok" } else { "err" });
                    if let Err(error) = &result {
                        operation.record("error.kind", error.kind());
                    }
                    result
                })
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = span;
                self.dispatch_model_step(journal, input, step, prior_messages, purpose)
            }
        })
    }

    pub(super) async fn prepare_request(
        &self,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<crate::model::PreparedModelRequest> {
        let projection = self.prepare_projection(input, step, prior_messages).await?;
        crate::model::PreparedModelRequest::prepare(
            self.request_from_context(&projection)?,
            self.stock.limits,
            self.io,
        )
    }

    pub(super) async fn prepare_projection(
        &self,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<crate::context::Context> {
        let context_input = ContextInput::copy_for_turn(input, step, prior_messages, self.io)?;
        let base = ContextPipeline::base_context(&context_input, self.stock.limits, self.io)?;
        self.transform_projection(&context_input, base).await
    }

    pub(super) async fn canonical_base(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<crate::context::Context> {
        let mut context_input = ContextInput::copy_for_turn(input, step, prior_messages, self.io)?;
        let checkpoint = context_input
            .selected_context
            .as_mut()
            .and_then(|selected| selected.selection.checkpoint.take());
        let delta = ContextPipeline::base_context(&context_input, self.stock.limits, self.io)?;
        let Some(reference) = checkpoint else {
            return Ok(delta);
        };
        let (envelope, retained) = load_canonical_checkpoint(
            journal,
            &reference,
            self.stock.limits,
            self.io.map(|_| &self.stock.tool_scope),
            self.io,
        )
        .await?;
        let selected = input.selected_context.as_ref().ok_or_else(|| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "checkpoint has no canonical delta",
            )
        })?;
        if selected.selection.conversation_revision <= envelope.selection.conversation_revision {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "checkpoint covers the new turn input",
            ));
        }
        ContextPipeline::continue_base(retained, delta, self.stock.limits, self.io)
    }

    pub(super) async fn transform_projection(
        &self,
        input: &ContextInput,
        base: crate::context::Context,
    ) -> Result<crate::context::Context> {
        let mut context = self
            .context_pipeline()
            .transform(input, base, self.stock.limits, self.io)
            .await?;
        let request = self.request_from_context(&context)?;
        let prepared = if let Some((prefix, verifier)) = self.inherited_prefix() {
            crate::model::PreparedModelRequest::inherit(
                request,
                prefix,
                verifier.as_ref(),
                self.stock.limits,
                self.io,
            )
            .await?
        } else {
            crate::model::PreparedModelRequest::prepare(request, self.stock.limits, self.io)?
        };
        let prefix_count = prepared.request().messages.len() - context.messages.len();
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
        if prefix_count != 0 {
            context.messages = crate::model::clone_messages(&prepared.request().messages, self.io)?;
        }
        crate::context::validate_projected_context(&context, self.stock.limits, self.io)?;
        Ok(context)
    }

    pub(super) fn request_from_context(
        &self,
        context: &crate::context::Context,
    ) -> Result<ModelRequest> {
        Ok(ModelRequest {
            model: crate::model::clone_model(&self.stock.model, self.io)?,
            messages: crate::model::clone_messages(&context.messages, self.io)?,
            tools: stock_tool_definitions(&self.stock.tools, &self.stock.tool_scope, self.io)?,
            max_output_tokens: self.output_budget()?,
        })
    }
}
