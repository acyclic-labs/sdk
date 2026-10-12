use super::*;

impl StockExecution<'_> {
    pub(super) async fn summarize(
        &self,
        journal: &dyn ExecutionJournal,
        parent: Option<&TurnInput>,
        operation_id: OperationId,
        step: u32,
        source: crate::context::Context,
        instruction: ModelContent,
    ) -> Result<crate::context::ContextSummary> {
        if parent.is_some_and(|parent| step >= parent.max_steps) {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary step exceeds parent turn bounds",
            ));
        }
        crate::context::validate_projected_context(&source, self.stock.limits, self.io)?;
        crate::executor::with_history_io(self.io, |work| {
            instruction.validate_limits(self.stock.limits, work)
        })?;
        stock_json_bound(&source, self.stock.limits.file_bytes, self.io)?;
        stock_json_bound(&instruction, self.stock.limits.file_bytes, self.io)?;
        let summary_output = self.output_budget()?;
        if summary_output.is_none() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary requires a caller-selected output token budget",
            ));
        }
        let source_bytes: acyclic_stream::OwnedBytes = match self.io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.encode(&source, self.stock.limits.file_bytes, true)?,
            _ => crate::contract::canonical_json_bytes(&source)?.into(),
        };
        let source_digest = crate::model::hash_bytes(&source_bytes, self.io)?;
        let source_messages = u32::try_from(source.messages.len()).map_err(|_| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary source exceeds portable message count",
            )
        })?;
        if source_messages == 0 {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary source is empty",
            ));
        }
        let revision = with_history_io(self.io, |work| {
            crate::contract::preparation_format(
                format_args!("{}", blake3::Hash::from(source_digest).to_hex()),
                work,
            )
        })?;
        let executor = StockExecution {
            stock: self.stock,
            context: self.context,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            interactions: self.interactions,
            io: self.io,
            summary: Some((
                ContextPipeline::summary_source(source, revision, self.io)?,
                summary_output,
            )),
        };
        let input = TurnInput {
            operation_id,
            input: instruction,
            selected_context: None,
            max_steps: parent.map_or(1, |parent| parent.max_steps),
        };
        self.validate_summary_capacity(&executor, journal, &input, step)
            .await?;
        executor.validate_turn_input(journal, &input).await?;
        self.verify_execution_owner().await?;
        if let Some(parent) = parent {
            self.validate_turn_input(journal, parent).await?;
            self.ensure_started(journal, parent).await?;
        } else {
            executor.ensure_started(journal, &input).await?;
        }
        let source_key = with_history_io(self.io, |work| {
            crate::contract::preparation_format(format_args!("summary:{step}:source"), work)
        })?;
        stage_bytes(journal, operation_id, &source_key, &source_bytes, self.io).await?;
        let events = executor
            .run_model_step(journal, &input, step, &[], ModelPurpose::Summary)?
            .await?;
        let mut text = String::new();
        for event in events {
            match event {
                ModelEvent::Content { delta } => {
                    if (text.len() as u64)
                        .checked_add(delta.len() as u64)
                        .is_none_or(|length| length > self.stock.limits.render_bytes)
                    {
                        return Err(stock_boundary(
                            self.io,
                            crate::wire::ErrorCode::Invalid,
                            "summary output exceeds caller render budget",
                        ));
                    }
                    stock_string_push(&mut text, &delta, self.io)?;
                }
                ModelEvent::ToolCall { .. } => {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "summary requested a tool operation",
                    ));
                }
                ModelEvent::Reasoning { .. } | ModelEvent::Completed { .. } => {}
            }
        }
        if text.is_empty() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary operation returned no content",
            ));
        }
        let output_key = with_history_io(self.io, |work| {
            crate::contract::preparation_format(format_args!("summary:{step}:output"), work)
        })?;
        let output = journal
            .stage(
                operation_id,
                &output_key,
                text.as_bytes(),
                "text/plain",
                self.io,
            )?
            .await?;
        let summary = crate::context::ContextSummary {
            operation_id,
            step,
            source_messages,
            source_digest,
            output,
        };
        let provenance_key = with_history_io(self.io, |work| {
            crate::contract::preparation_format(format_args!("summary:{step}:provenance"), work)
        })?;
        stage_json(journal, operation_id, &provenance_key, &summary, self.io).await?;
        Ok(summary)
    }
}
