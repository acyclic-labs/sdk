use super::*;

/// The backstop's one fixed marker, in place of every evicted range.
pub(crate) const BACKSTOP_MARKER: &str = crate::seed::CONTEXT_BACKSTOP;

pub(super) enum CompactionSource {
    Projected,
    Base {
        context: crate::context::Context,
        // Selected bases are counted only after the final pressure decision.
        // A working-base probe transfers its already validated count here.
        count: Option<crate::context::ModelTokenCount>,
    },
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
        policy: &crate::context::ThresholdCompaction,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (input, step, prior) = turn;
        let (projection, accounting) = captured;
        let (stage_messages, context, count, pre_stage) = match source {
            CompactionSource::Projected => (0, projection.context, Some(accounting.count), false),
            CompactionSource::Base { context, count } => {
                let stage_messages = projection
                    .context
                    .messages
                    .len()
                    .saturating_sub(context.messages.len());
                drop((projection.context, accounting.count));
                (stage_messages, context, count, true)
            }
        };
        let message_limit = self.compaction_message_limit(
            step == 0 && input.selected_context.is_some(),
            stage_messages,
        );
        let (through, maximum) = self
            .summary_budget(count, accounting.capacity, &context, policy, message_limit)
            .await?;
        let summary = self
            .backstop_marker(journal, input.operation_id, step, (&context, through))
            .await?;
        if self.stock.reconciliation_only {
            return Err(Error::Indeterminate(input.operation_id));
        }
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
            (compaction, projection.working)
        };
        let (compacted, request) = self
            .prepare_compacted_request(input, step, compacted, pre_stage)
            .await?;
        let compacted = ResponseProjection {
            context: compacted,
            steering: projection.steering,
            canonical: None,
            checkpoint: None,
            working,
            accounting: None,
        };
        self.finish_compaction(
            journal,
            (input, step),
            compacted,
            compaction,
            accounting.capacity,
            request,
        )
        .await
    }

    /// The fixed marker standing in for the evicted `[0, through)`, staged
    /// with the provenance replay verifies. No model call: the backstop is
    /// mechanical, and deliberate summaries are the agent's own program.
    async fn backstop_marker(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        source: (&crate::context::Context, usize),
    ) -> Result<crate::context::ContextSummary> {
        let (context, through) = source;
        let invalid = |reason| stock_boundary(self.io, crate::wire::ErrorCode::Invalid, reason);
        let source_messages =
            u32::try_from(through).map_err(|_| invalid("evicted range exceeds portable count"))?;
        let source_digest = crate::context::prefix_digest(context, through, self.io)?;
        let key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("backstop:{step}:marker"), work)
        })?;
        self.publish_summary(
            journal,
            (operation_id, step),
            (source_messages, source_digest),
            (&key, BACKSTOP_MARKER.as_bytes()),
        )
        .await
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

    async fn summary_budget(
        &self,
        count: Option<crate::context::ModelTokenCount>,
        capacity: crate::context::ModelContextCapacity,
        context: &crate::context::Context,
        policy: &crate::context::ThresholdCompaction,
        message_limit: Option<usize>,
    ) -> Result<(usize, usize)> {
        let ceiling = policy.input_ceiling(capacity, self.output_tokens())?;
        let count = if let Some(count) = count {
            count
        } else {
            let canonical_request = crate::model::PreparedModelRequest::prepare(
                self.request_from_context(context)?,
                self.stock.limits,
                self.io,
            )?;
            let count = stock_count_tokens(
                self.stock.provider.as_ref(),
                &canonical_request,
                ceiling,
                self.io,
            )
            .await?;
            with_history_io(self.io, |work| count.validate(&canonical_request, work))?;
            count
        };
        let available = capacity
            .context_tokens
            .checked_sub(
                policy
                    .response_reserve_tokens
                    .max(capacity.output_reservation(self.output_tokens())?),
            )
            .and_then(|tokens| tokens.checked_sub(count.fixed_tokens))
            .filter(|tokens| *tokens >= 2)
            .ok_or_else(|| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "fixed request content leaves no summary capacity",
                )
            })?;
        let recent_tokens = policy.recent_tokens.min(available / 2);
        let (through, maximum) = crate::context::plan_compaction_budget(
            policy,
            context,
            &count,
            message_limit.unwrap_or(context.messages.len()),
            recent_tokens,
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
        let mandatory_count =
            stock_count_tokens(self.stock.provider.as_ref(), &mandatory, ceiling, self.io).await?;
        let mandatory_count =
            with_history_io(self.io, |work| mandatory_count.validate(&mandatory, work))?;
        if policy.needs_compaction(capacity, mandatory_count, self.output_tokens())? {
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
        let (compacted, request) = compacted;
        let projection_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("context:{step}:compacted"), work)
        })?;
        let projection = transcript::stage_projection(
            journal,
            input.operation_id,
            &projection_key,
            compacted,
            request,
            Some(accounting),
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
        compacted: ResponseProjection,
        compaction: FileRef,
        capacity: crate::context::ModelContextCapacity,
        request: crate::model::PreparedModelRequest,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (input, step) = turn;
        let crate::context::CompactionPolicy::Threshold(policy) = self.compaction_policy()? else {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "automatic compaction is disabled",
            ));
        };
        let ceiling = policy.input_ceiling(capacity, self.output_tokens())?;
        let count =
            stock_count_tokens(self.stock.provider.as_ref(), &request, ceiling, self.io).await?;
        let final_count = with_history_io(self.io, |work| count.validate(&request, work))?;
        if policy.needs_compaction(capacity, final_count, self.output_tokens())? {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "context backstop: the irreducible set exceeds the model window",
            ));
        }
        self.publish_compacted_context(
            journal,
            input,
            step,
            (&compacted, &request),
            compaction,
            &ContextAccounting { capacity, count },
        )
        .await?;
        Ok(request)
    }
}
