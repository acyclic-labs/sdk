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
    ) -> Result<(crate::context::ContextSummary, String)> {
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
        let pinned = self.stock.summary_source.as_deref();
        if pinned.is_none() {
            stock_json_bound(&source, self.stock.limits.file_bytes, self.io)?;
        }
        stock_json_bound(&instruction, self.stock.limits.file_bytes, self.io)?;
        let summary_output = self.output_budget()?;
        if summary_output.is_none() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary requires a caller-selected output token budget",
            ));
        }
        let source_bytes: Option<acyclic_stream::OwnedBytes> = if pinned.is_some() {
            None
        } else {
            Some(match self.io {
                #[cfg(feature = "filesystem")]
                Some(io) => io.encode(&source, self.stock.limits.file_bytes, true)?,
                _ => crate::contract::canonical_json_bytes(&source)?.into(),
            })
        };
        let source_digest = match pinned {
            Some(file) => *file.descriptor().sha256(),
            None => crate::model::hash_bytes(source_bytes.as_deref().unwrap(), self.io)?,
        };
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
            crate::executor::preparation_format(
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
            // Public in-turn summaries validate at entry; automatic compaction
            // is already inside the validated parent's execution.
            self.ensure_started(journal, parent).await?;
        } else {
            executor.ensure_started(journal, &input).await?;
        }
        if let Some(source_bytes) = &source_bytes {
            let source_key = with_history_io(self.io, |work| {
                crate::executor::preparation_format(format_args!("summary:{step}:source"), work)
            })?;
            stage_bytes(
                journal,
                operation_id,
                &source_key,
                source_bytes,
                true,
                self.io,
            )
            .await?;
        }
        let events = executor
            .run_model_step(journal, &input, step, &[], ModelPurpose::Summary)?
            .await?;
        let text = self.collect_summary_text(events)?;
        self.publish_summary(
            journal,
            operation_id,
            step,
            (source_messages, source_digest),
            text,
        )
        .await
    }

    fn collect_summary_text(&self, events: Vec<ModelEvent>) -> Result<String> {
        let mut text = String::new();
        for event in events {
            match event {
                ModelEvent::Content { delta } => {
                    stock_string_push(
                        &mut text,
                        &delta,
                        self.stock.limits.render_bytes,
                        "summary output exceeds caller render budget",
                        self.io,
                    )?;
                }
                ModelEvent::ToolCall(_) => {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Invalid,
                        "summary requested a tool operation",
                    ));
                }
                ModelEvent::Reasoning { .. }
                | ModelEvent::ReasoningBlock { .. }
                | ModelEvent::Completed { .. } => {}
            }
        }
        if text.is_empty() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary operation returned no content",
            ));
        }
        Ok(text)
    }

    async fn publish_summary(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        source: (u32, [u8; 32]),
        text: String,
    ) -> Result<(crate::context::ContextSummary, String)> {
        let (source_messages, source_digest) = source;
        let output = if let Some(context) = self.original_context() {
            let path = with_history_io(self.io, |work| {
                crate::executor::preparation_format(
                    format_args!("summaries/{operation_id}/{step}.txt"),
                    work,
                )
            })?;
            // A turn may compact more than once. Its file receipt must not alias
            // another step or any of the task's tool publications.
            let publication = summary_publication(operation_id, step, self.io)?;
            self.verify_execution_owner().await?;
            let summaries = context.private_summaries(self.io)?;
            let output = context
                .stage_private_file_once(summaries, publication, &path, text.as_bytes(), self.io)
                .await?;
            self.verify_execution_owner().await?;
            output
        } else {
            let output_key = with_history_io(self.io, |work| {
                crate::executor::preparation_format(format_args!("summary:{step}:output"), work)
            })?;
            journal
                .stage(
                    operation_id,
                    &output_key,
                    text.as_bytes(),
                    "text/plain",
                    self.io,
                )?
                .await?
        };
        let summary = crate::context::ContextSummary {
            operation_id,
            step,
            source_messages,
            source_digest,
            output,
        };
        stage_summary_provenance(journal, &summary, self.io).await?;
        Ok((summary, text))
    }
}

fn summary_publication(
    operation: OperationId,
    step: u32,
    io: Option<&PreparationIo>,
) -> Result<OperationId> {
    const DOMAIN: &[u8] = b"harness/v2/summary-content\0";
    #[cfg(feature = "filesystem")]
    let charge = acyclic_fs::WorkCounters {
        bytes_hashed: DOMAIN.len() as u64 + 16 + 4,
        ..Default::default()
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(charge)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(&operation.into_bytes());
    hasher.update(&step.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.charge(charge)?;
    }
    let _ = io;
    Ok(OperationId::from_bytes(bytes))
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
        Some(io) => io.encode(summary, io.remaining()?.bytes_encoded, true)?,
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
