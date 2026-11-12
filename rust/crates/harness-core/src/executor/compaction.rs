use super::*;

#[derive(serde::Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct ManagerRewrite {
    version: u32,
    drop_through: u32,
    summary: String,
}

impl PreparationRecord for ManagerRewrite {}

pub(super) enum CompactionSource {
    Projected,
    Base { context: crate::context::Context },
}

impl StockExecution<'_> {
    pub(super) fn compaction_message_limit(
        &self,
        selected_base: bool,
        stage_messages: usize,
    ) -> Option<usize> {
        self.stock.limits.context_messages.map(|maximum| {
            maximum
                .get()
                .saturating_sub(if selected_base { 2 } else { 0 })
                .saturating_sub(stage_messages)
        })
    }

    pub(super) async fn compact_response(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32, usize),
        captured: (ResponseProjection, ContextAccounting),
        source: CompactionSource,
        policy: &crate::context::CapacityCompaction,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (input, step, prior) = turn;
        let (mut projection, accounting) = captured;
        let (stage_messages, context, pre_stage) = match source {
            CompactionSource::Projected => (0, projection.context, false),
            CompactionSource::Base { context } => {
                let stage_messages = projection
                    .context
                    .messages
                    .len()
                    .saturating_sub(context.messages.len());
                drop((projection.context, accounting.count));
                (stage_messages, context, true)
            }
        };
        let message_limit = self.compaction_message_limit(
            step == 0 && input.selected_context.is_some(),
            stage_messages,
        );
        if self.stock.reconciliation_only {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let (eligible, _) = self
            .summary_budget(
                accounting.capacity,
                &context,
                policy,
                message_limit,
                journal,
                (input.operation_id, step),
            )
            .await?;
        let files = self.rewrite_files(journal, (input, step)).await?;
        let (summary, maximum) = self
            .managed_summary(
                journal,
                (input, step),
                &context,
                policy,
                (eligible, message_limit),
            )
            .await?;
        let (compacted, reference) =
            crate::context::compact_projection(&context, maximum, Some(summary), self.io)?;
        let (compaction, working) = if pre_stage {
            let (compaction, working) = self
                .stage_working_cut(
                    journal,
                    working_tail::WorkingCompaction {
                        input,
                        step,
                        prior,
                        source: &context,
                        retained: &compacted,
                        proof: &reference,
                    },
                )
                .await?;
            (compaction, Some(working))
        } else {
            let key = with_history_io(self.io, |work| {
                crate::executor::preparation_format(format_args!("context:{step}:compaction"), work)
            })?;
            let compaction =
                stage_json(journal, input.operation_id, &key, &reference, self.io).await?;
            (compaction, projection.working.take())
        };
        let (compacted, mut request) = self
            .prepare_compacted_request(input, step, compacted, pre_stage)
            .await?;
        let compacted = ResponseProjection {
            context: compacted,
            steering: projection.steering.take(),
            canonical: None,
            checkpoint: None,
            working,
            accounting: None,
        };
        if self
            .finish_compaction(
                journal,
                (input, step),
                &compacted,
                compaction,
                (
                    accounting.capacity,
                    files
                        .as_ref()
                        .map(|files| context_files::RewritePublication {
                            files,
                            proof: &reference,
                        }),
                ),
                &mut request,
            )
            .await?
        {
            return Ok(request);
        }
        Err(stock_boundary(
            self.io,
            crate::wire::ErrorCode::Invalid,
            "context rewrite does not fit the selected model window",
        ))
    }

    /// The manager chooses the cut. The kernel only validates dependency
    /// closure and the final request's capacity, never a retention heuristic.
    async fn managed_summary(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        context: &crate::context::Context,
        policy: &crate::context::CapacityCompaction,
        selection: (usize, Option<usize>),
    ) -> Result<(crate::context::ContextSummary, usize)> {
        let (input, step) = turn;
        let (source_messages, message_limit) = selection;
        let invalid = |reason| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, reason);
        let (source, _) = with_history_io(self.io, |work| {
            crate::context::clone_context(context, Some(source_messages), work)
        })?;
        let instruction = with_history_io(self.io, |work| {
            crate::executor::preparation_format(
                format_args!(
                    "{}\nSource count: {source_messages}.",
                    crate::seed::CONTEXT_MANAGER
                ),
                work,
            )
        })?;
        let mut phase = DispatchPhase::new();
        let (_, text) = self
            .summarize(
                journal,
                Some(input),
                input.operation_id,
                step,
                (source, ModelContent::Text(instruction)),
                &mut phase,
            )
            .await?;
        let rewrite: ManagerRewrite = match self.io {
            Some(io) => {
                let (value, hold) = io.decode(
                    std::marker::PhantomData,
                    text.as_bytes(),
                    crate::wire::ErrorCode::Invalid,
                )?;
                retain_preparation((value, Some(hold)))?
            }
            None => crate::contract::json_from_slice(text.as_bytes())
                .map_err(|_| invalid("context manager returned an invalid rewrite"))?,
        };
        let requested = rewrite.drop_through as usize;
        if rewrite.version != 1 || requested == 0 || requested > source_messages {
            return Err(invalid("context manager selected an invalid source range"));
        }
        let (through, maximum) = crate::context::plan_compaction_budget(
            policy,
            context,
            message_limit,
            requested,
            self.io,
        )?;
        if through != requested {
            return Err(invalid("context manager cut splits a dependency group"));
        }
        let source_digest = crate::context::prefix_digest(context, through, self.io)?;
        let key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("manager:{step}:summary"), work)
        })?;
        let summary = self
            .publish_summary(
                journal,
                (input.operation_id, step),
                (rewrite.drop_through, source_digest),
                (&key, rewrite.summary.as_bytes()),
            )
            .await?;
        Ok((summary, maximum))
    }

    async fn prepare_compacted_request(
        &self,
        input: &TurnInput,
        step: u32,
        compacted: crate::context::Context,
        pre_stage: bool,
    ) -> Result<(crate::context::Context, crate::model::PreparedModelRequest)> {
        if pre_stage {
            let context_input = ContextInput::copy_for_turn(input, step, &[], self.io)?;
            self.prepare_transformed_request(&context_input, compacted)
                .await
        } else {
            let request = crate::model::PreparedModelRequest::prepare(
                self.request_from_context(&compacted)?,
                self.stock.limits,
                self.io,
            )?;
            Ok((compacted, request))
        }
    }

    pub(super) async fn summary_budget(
        &self,
        capacity: crate::context::ModelContextCapacity,
        context: &crate::context::Context,
        policy: &crate::context::CapacityCompaction,
        message_limit: Option<usize>,
        journal: &dyn ExecutionJournal,
        turn: (OperationId, u32),
    ) -> Result<(usize, usize)> {
        let (through, maximum) = crate::context::plan_compaction_budget(
            policy,
            context,
            message_limit,
            context
                .current_input_index
                .map_or(context.messages.len().saturating_sub(1), |index| {
                    index as usize
                }),
            self.io,
        )?;
        let mandatory = with_history_io(self.io, |work| {
            crate::context::mandatory_messages(context, through, work)
        })?;
        let mandatory = crate::model::PreparedModelRequest::prepare(
            self.request_from_messages(mandatory)?,
            self.stock.limits,
            self.io,
        )?;
        let mandatory_accounting = self.response_accounting(&mandatory, journal, turn).await?;
        let Some(mandatory_count) = mandatory_accounting
            .count
            .input_tokens(&mandatory, self.io)?
        else {
            return Ok((through, maximum));
        };
        if policy.needs_compaction(capacity, mandatory_count)? {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "mandatory content exceeds selected model capacity",
            ));
        }
        Ok((through, maximum))
    }

    pub(super) async fn publish_compacted_context(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        compacted: (&ResponseProjection, &crate::model::PreparedModelRequest),
        compaction: FileRef,
        accounting: &ContextAccounting,
    ) -> Result<()> {
        let _writer = self.journal_writes.lock().await;
        let (compacted, request) = compacted;
        let projection_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("context:{step}:compacted"), work)
        })?;
        let previous = self
            .response_projection(journal, input.operation_id, step)
            .await?
            .ok_or_else(|| self.invalid_working_cut())?;
        let projection = transcript::stage_projection(
            journal,
            input.operation_id,
            &projection_key,
            (compacted, request),
            Some(accounting),
            Some(&previous),
            self.io,
        )
        .await?;
        let (tail, _) = self
            .model_records(journal, input.operation_id, step, ModelPurpose::Response)
            .await?;
        if !journal
            .append_if_tail(
                input.operation_id,
                tail,
                &projection_key,
                &ExecutionEvent::ContextCompacted {
                    step,
                    projection,
                    compaction,
                },
                self.io,
            )?
            .await?
        {
            return Err(Error::Indeterminate(input.operation_id));
        }
        Ok(())
    }

    async fn finish_compaction(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        compacted: &ResponseProjection,
        compaction: FileRef,
        admission: (
            crate::context::ModelContextCapacity,
            Option<context_files::RewritePublication<'_>>,
        ),
        request: &mut crate::model::PreparedModelRequest,
    ) -> Result<bool> {
        let (input, step) = turn;
        let (capacity, publication) = admission;
        let crate::context::CompactionPolicy::Capacity(policy) = self.compaction_policy()? else {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "automatic compaction is disabled",
            ));
        };
        let accounting = self
            .response_accounting(request, journal, (input.operation_id, step))
            .await?;
        match accounting.count.input_tokens(request, self.io) {
            Ok(Some(final_count)) => {
                if policy.needs_compaction(capacity, final_count)? {
                    return Ok(false);
                }
                request.retain_context_window(crate::context::ContextWindow {
                    input_tokens: final_count,
                    capacity_tokens: capacity.context_tokens,
                })?;
            }
            Ok(None) => {}
            Err(Error::ModelContextOverflow) => return Ok(false),
            Err(error) => return Err(error),
        }
        if let Some(publication) = publication {
            self.publish_rewrite_files(journal, turn, publication)
                .await?;
        }
        self.publish_compacted_context(
            journal,
            input,
            step,
            (compacted, request),
            compaction,
            &accounting,
        )
        .await?;
        Ok(true)
    }
}
