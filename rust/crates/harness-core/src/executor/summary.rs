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
        self.summarize_part(
            journal,
            parent,
            operation_id,
            step,
            material,
            phase,
            ModelPurpose::Summary,
        )
        .await
    }

    fn summary_executor(
        &self,
        source: crate::context::Context,
        revision: String,
    ) -> Result<StockExecution<'_>> {
        Ok(StockExecution {
            stock: self.stock,
            selected: self.selected.clone(),
            configuration: None,
            context: self.context,
            io: self.io,
            summary: Some(ContextPipeline::summary_source(source, revision, self.io)?),
            single_request: false,
            journal_writes: self.journal_writes.borrowed(),
            compaction: once_cell::sync::OnceCell::new(),
            records: tokio::sync::Mutex::new(Default::default()),
            working_tail: tokio::sync::Mutex::new(Default::default()),
            request_cache: tokio::sync::Mutex::new(Default::default()),
            content_admission: Default::default(),
            review: Default::default(),
        })
    }

    pub(super) async fn admit_summary_window(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        source: crate::context::Context,
        instruction: ModelContent,
        purpose: ModelPurpose,
    ) -> Result<()> {
        if let Some(maximum) = self.stock.limits.file_bytes {
            let bytes = with_history_io(self.io, |work| {
                crate::executor::json::json_byte_length(&source, None, work)
            })?;
            if bytes > maximum {
                return Err(Error::ModelContextOverflow);
            }
        }
        let (_, digest, _) = self.summary_source_identity(&source)?;
        let revision = with_history_io(self.io, |work| {
            preparation_format(
                format_args!("{}", blake3::Hash::from(digest).to_hex()),
                work,
            )
        })?;
        let executor = self.summary_executor(source, revision)?;
        let input = TurnInput {
            operation_id: turn.0.operation_id,
            input: instruction,
            max_steps: Some(1),
            selected_context: None,
        };
        self.prepare_summary_admission(&executor, journal, &input, turn.1, purpose)
            .await?;
        Ok(())
    }

    pub(super) async fn summarize_part(
        &self,
        journal: &dyn ExecutionJournal,
        parent: Option<&TurnInput>,
        operation_id: OperationId,
        step: u32,
        material: (crate::context::Context, ModelContent),
        phase: &mut DispatchPhase,
        purpose: ModelPurpose,
    ) -> Result<(crate::context::ContextSummary, String)> {
        self.summarize_output(
            journal,
            parent,
            operation_id,
            step,
            material,
            phase,
            purpose,
        )
        .await?
        .map_err(|defect| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, defect))
    }

    /// The inner error is a defect of the model's own completed output (no
    /// text, a tool request, a truncated or oversized reply). Its usage is
    /// already journaled, so a caller that owns the decision may re-ask
    /// instead of failing; the outer error is every other failure.
    pub(super) async fn summarize_output(
        &self,
        journal: &dyn ExecutionJournal,
        parent: Option<&TurnInput>,
        operation_id: OperationId,
        step: u32,
        material: (crate::context::Context, ModelContent),
        phase: &mut DispatchPhase,
        purpose: ModelPurpose,
    ) -> Result<std::result::Result<(crate::context::ContextSummary, String), &'static str>> {
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
        let revision = with_history_io(self.io, |work| {
            crate::executor::preparation_format(
                format_args!("{}", blake3::Hash::from(source_digest).to_hex()),
                work,
            )
        })?;
        let executor = self.summary_executor(source, revision)?;
        let input = TurnInput {
            max_steps: Some(1),
            operation_id,
            input: instruction,
            selected_context: None,
        };
        let prepared = self
            .prepare_summary_admission(&executor, journal, &input, step, purpose)
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
            crate::executor::preparation_format(
                format_args!("{}:{step}:source", dispatch::model_purpose_key(purpose)),
                work,
            )
        })?;
        stage_bytes(
            journal,
            operation_id,
            &source_key,
            (&source_bytes, "application/json"),
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
                    publish_reply: false,
                },
                purpose,
                phase,
            )?
            .await?;
        if events.iter().any(|event| {
            matches!(event, ModelEvent::Completed { metadata } if metadata["finish_reason"] == "length")
        }) {
            // The original usage stays in the journal, but an incomplete
            // summary cannot authorize replacing its source history.
            for event in stock_into_iter(events, self.io)? {
                stock_retire_event(event, self.io)?;
            }
            return Ok(Err(
                "summary reached its output-token limit; request a shorter summary",
            ));
        }
        let text = match self.summary_text(events)? {
            Ok(text) => text,
            Err(defect) => return Ok(Err(defect)),
        };
        let output_key = with_history_io(self.io, |work| match purpose {
            ModelPurpose::SummaryPart(part) => {
                preparation_format(format_args!("summary:{step}:part:{part}:output"), work)
            }
            _ => preparation_format(format_args!("summary:{step}:output"), work),
        })?;
        let summary = self
            .publish_summary(
                journal,
                (operation_id, step),
                (source_messages, source_digest),
                (&output_key, text.as_bytes()),
            )
            .await?;
        Ok(Ok((summary, text)))
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
        self.summary_text(events)?
            .map_err(|defect| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, defect))
    }

    /// The inner error is a defect of the model's output, never of this host.
    fn summary_text(
        &self,
        events: Vec<ModelEvent>,
    ) -> Result<std::result::Result<String, &'static str>> {
        let scope = crate::filesystem::PreparationScope::with_input(
            self.io,
            self.io
                .map(|_| stock_events_payload_bytes(&events))
                .transpose()?
                .unwrap_or(0),
        )?;
        let io = scope.io();
        // Input and output die before the scope on every early return.
        let events = events;
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
                        return Ok(Err("summary output exceeds caller render budget"));
                    }
                    stock_retire_text(text, io)?;
                    text = completed;
                }
                ModelEvent::ToolCall(_) => {
                    return Ok(Err("summary requested a tool operation"));
                }
                event @ (ModelEvent::Restarted
                | ModelEvent::Wait(_)
                | ModelEvent::Trace { .. }
                | ModelEvent::Counted { .. }
                | ModelEvent::Content { .. }
                | ModelEvent::TextPart { .. }
                | ModelEvent::Reasoning { .. }
                | ModelEvent::ReasoningBlock { .. }
                | ModelEvent::Completed { .. }) => stock_retire_event(event, io)?,
            }
        }
        if text.is_empty() {
            return Ok(Err("summary operation returned no content"));
        }
        scope.finish(text.capacity() as u64)?;
        Ok(Ok(text))
    }

    /// Publish a selected summary with the exact source and output provenance
    /// replay requires. Manager responses retain their own original keys.
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
    let key_hold = io
        .map(|io| io.guard_owned(key.capacity() as u64))
        .transpose()?;
    let bytes: acyclic_stream::OwnedBytes = match io {
        Some(io) => io.encode(summary, io.remaining()?.bytes_encoded, true)?,
        _ => crate::contract::canonical_json_bytes(summary)?.into(),
    };
    let bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    // The exact output identity is already the immutable retry key. Appending
    // the provenance payload's digest would make this file unqueryable by ref.
    stage_bytes(
        journal,
        summary.operation_id,
        &key,
        (&bytes, "application/json"),
        false,
        io,
    )
    .await?;
    drop(bytes);
    drop(bytes_hold);
    drop(key);
    drop(key_hold);
    Ok(())
}
