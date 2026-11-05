use super::*;

impl StockExecution<'_> {
    pub(super) async fn summarize(
        &self,
        journal: &dyn ExecutionJournal,
        parent: Option<&TurnInput>,
        operation_id: OperationId,
        step: u32,
        material: (crate::context::Context, ModelContent),
        phase: &mut DispatchPhase,
    ) -> Result<(crate::context::ContextSummary, String)> {
        let (source, instruction) = material;
        // Validate the original task's summary authority before model admission,
        // even though the summary session disables recursive compaction.
        if let Some(context) = self.original_context() {
            match self.compaction()?.destination {
                Some(destination) => destination.require(self.io)?,
                None => context.private_summaries(self.io)?,
            };
        }
        crate::context::validate_projected_context(&source, self.stock.limits, self.io)?;
        crate::executor::with_history_io(self.io, |work| {
            instruction.validate_limits(self.stock.limits, work)
        })?;
        stock_json_bound(&instruction, self.stock.limits.file_bytes, self.io)?;
        let (source_bytes, source_digest, source_messages) =
            self.summary_source_identity(&source)?;
        let summary_output = self.output_budget()?;
        let revision = with_history_io(self.io, |work| {
            crate::executor::preparation_format(
                format_args!("{}", blake3::Hash::from(source_digest).to_hex()),
                work,
            )
        })?;
        let executor = StockExecution {
            stock: self.stock,
            context: self.context,
            io: self.io,
            summary: Some((
                ContextPipeline::summary_source(source, revision, self.io)?,
                summary_output,
            )),
            single_request: false,
            compaction: once_cell::sync::OnceCell::new(),
            records: tokio::sync::Mutex::new(Default::default()),
            working_tail: tokio::sync::Mutex::new(Default::default()),
            request_cache: tokio::sync::Mutex::new(Default::default()),
            content_admission: Default::default(),
        };
        let input = TurnInput {
            max_steps: Some(1),
            operation_id,
            input: instruction,
            selected_context: None,
        };
        let prepared = self
            .prepare_summary_admission(&executor, journal, &input, step)
            .await?;
        executor.validate_turn_input(journal, &input).await?;
        if let Some(parent) = parent {
            // Public in-turn summaries validate at entry; automatic compaction
            // is already inside the validated parent's execution.
            self.ensure_started(journal, parent).await?;
        } else {
            executor.ensure_started(journal, &input).await?;
        }
        let source_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("summary:{step}:source"), work)
        })?;
        stage_bytes(
            journal,
            operation_id,
            &source_key,
            &source_bytes,
            true,
            self.io,
        )
        .await?;
        drop(source_bytes);
        drop(source_key);
        let events = executor
            .run_model_step(
                journal,
                &input,
                step,
                request::ModelStepRequest {
                    history: request::ModelStepHistory::Selected(&[]),
                    prepared,
                },
                ModelPurpose::Summary,
                phase,
            )?
            .await?;
        let text = self.collect_summary_text(events)?;
        let output_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("summary:{step}:output"), work)
        })?;
        let summary = self
            .publish_summary(
                journal,
                (operation_id, step),
                (source_messages, source_digest),
                (&output_key, text.as_bytes()),
            )
            .await?;
        Ok((summary, text))
    }

    fn summary_source_identity(
        &self,
        source: &crate::context::Context,
    ) -> Result<(acyclic_stream::OwnedBytes, [u8; 32], u32)> {
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
        let source_bytes: acyclic_stream::OwnedBytes = match self.io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.encode(&source, self.stock.limits.file_bytes, true)?,
            _ => {
                stock_json_bound(source, self.stock.limits.file_bytes, self.io)?;
                crate::contract::canonical_json_bytes(&source)?.into()
            }
        };
        let source_digest = crate::model::hash_bytes(&source_bytes, self.io)?;
        Ok((source_bytes, source_digest, source_messages))
    }

    fn collect_summary_text(&self, events: Vec<ModelEvent>) -> Result<String> {
        #[cfg(feature = "filesystem")]
        let scope = crate::filesystem::PreparationScope::with_input(
            self.io,
            self.io
                .map(|_| stock_events_payload_bytes(&events))
                .transpose()?
                .unwrap_or(0),
        )?;
        #[cfg(feature = "filesystem")]
        let io = scope.io();
        #[cfg(not(feature = "filesystem"))]
        let io = self.io;
        // Input and output die before the scope on every early return.
        let events = events;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        let completed_message = self.has_completed_message(&events)?;
        let mut text = String::new();
        for event in stock_into_iter(events, io)? {
            match event {
                ModelEvent::Content { delta } if !completed_message => {
                    stock_string_push(
                        &mut text,
                        &delta,
                        self.stock.limits.render_bytes,
                        "summary output exceeds caller render budget",
                        io,
                    )?;
                    stock_retire_text(delta, io)?;
                }
                ModelEvent::MessageCompleted { text: completed } => {
                    if self
                        .stock
                        .limits
                        .render_bytes
                        .is_some_and(|maximum| completed.len() as u64 > maximum)
                    {
                        return Err(stock_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "summary output exceeds caller render budget",
                        ));
                    }
                    stock_retire_text(text, io)?;
                    text = completed;
                }
                ModelEvent::ToolCall(_) => {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "summary requested a tool operation",
                    ));
                }
                event @ (ModelEvent::Content { .. }
                | ModelEvent::TextPart { .. }
                | ModelEvent::Reasoning { .. }
                | ModelEvent::ReasoningBlock { .. }
                | ModelEvent::Completed { .. }) => stock_retire_event(event, io)?,
            }
        }
        if text.is_empty() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "summary operation returned no content",
            ));
        }
        #[cfg(feature = "filesystem")]
        scope.finish(text.capacity() as u64)?;
        Ok(text)
    }

    /// Publish both authored summaries and kernel markers with the same exact
    /// output provenance. Their original keys remain distinct for replay.
    pub(super) async fn publish_summary(
        &self,
        journal: &dyn ExecutionJournal,
        identity: (OperationId, u32),
        source: (u32, [u8; 32]),
        output: (&str, &[u8]),
    ) -> Result<crate::context::ContextSummary> {
        let (operation_id, step) = identity;
        let (source_messages, source_digest) = source;
        let (output_key, bytes) = output;
        // Every producer uses the immutable publication authenticated by the
        // response commit; task-local mutable paths cannot carry that receipt.
        let output = journal
            .stage(operation_id, output_key, bytes, "text/plain", self.io)?
            .await?;
        let summary = crate::context::ContextSummary {
            operation_id,
            step,
            source_messages,
            source_digest,
            output,
        };
        stage_summary_provenance(journal, &summary, self.io).await?;
        Ok(summary)
    }
}

#[cfg(test)]
mod tests;

pub(super) async fn stage_summary_provenance(
    journal: &dyn ExecutionJournal,
    summary: &crate::context::ContextSummary,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let key = with_history_io(io, |work| summary.output.read_capability(work))?;
    #[cfg(feature = "filesystem")]
    let key_hold = io
        .map(|io| io.guard_owned(key.capacity() as u64))
        .transpose()?;
    let bytes: acyclic_stream::OwnedBytes = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => io.encode(summary, Some(io.remaining()?.bytes_encoded), true)?,
        _ => crate::contract::canonical_json_bytes(summary)?.into(),
    };
    #[cfg(feature = "filesystem")]
    let bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    // The exact output identity is already the immutable retry key. Appending
    // the provenance payload's digest would make this file unqueryable by ref.
    stage_bytes(journal, summary.operation_id, &key, &bytes, false, io).await?;
    drop(bytes);
    #[cfg(feature = "filesystem")]
    drop(bytes_hold);
    drop(key);
    #[cfg(feature = "filesystem")]
    drop(key_hold);
    Ok(())
}
