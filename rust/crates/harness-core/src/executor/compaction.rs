use super::*;

/// The backstop's one fixed marker, in place of every evicted range.
pub(crate) const BACKSTOP_MARKER: &str = "[context backstop: earlier messages were evicted to fit the model's context window; they remain in this agent's history]";

impl StockExecution<'_> {
    pub(super) fn compaction_message_limit(
        &self,
        selected_base: bool,
        stage_messages: usize,
    ) -> usize {
        self.stock
            .limits
            .context_messages
            .saturating_sub(if selected_base { 2 } else { 0 })
            .saturating_sub(stage_messages)
    }

    pub(super) async fn compact_response(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32, usize),
        captured: (ResponseProjection, ContextAccounting),
        source: (crate::context::Context, bool),
        policy: &crate::context::ThresholdCompaction,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (input, step, prior) = turn;
        let (context, pre_stage) = source;
        let (projection, accounting) = captured;
        let message_limit = self.compaction_message_limit(
            step == 0 && input.selected_context.is_some(),
            projection
                .context
                .messages
                .len()
                .saturating_sub(context.messages.len()),
        );
        let (through, maximum) = self
            .summary_budget(
                accounting.count,
                accounting.capacity,
                &context,
                pre_stage,
                policy,
                message_limit,
            )
            .await?;
        let summary = self
            .backstop_marker(journal, input.operation_id, step, (&context, through))
            .await?;
        if self.stock.reconciliation_only {
            return Err(Error::Indeterminate(input.operation_id));
        }
        let (compacted, reference) =
            crate::context::compact_projection(&context, maximum, Some(summary), self.io)?;
        let compaction_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("context:{step}:compaction"), work)
        })?;
        let compaction = stage_json(
            journal,
            input.operation_id,
            &compaction_key,
            &reference,
            self.io,
        )
        .await?;
        let working = if pre_stage {
            Some(
                self.stage_working_cut(
                    journal,
                    working_tail::WorkingCompaction {
                        input,
                        step,
                        prior,
                        source: &context,
                        retained: &compacted,
                        compaction: &compaction,
                    },
                )
                .await?,
            )
        } else {
            projection.working
        };
        let (compacted, request) = self
            .prepare_compacted_request(input, step, compacted, pre_stage)
            .await?;
        let compacted = ResponseProjection {
            context: compacted,
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
        let output = journal
            .stage(
                operation_id,
                &key,
                BACKSTOP_MARKER.as_bytes(),
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
        super::summary::stage_summary_provenance(journal, &summary, self.io).await?;
        Ok(summary)
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
        count: crate::context::ModelTokenCount,
        capacity: crate::context::ModelContextCapacity,
        context: &crate::context::Context,
        pre_stage: bool,
        policy: &crate::context::ThresholdCompaction,
        message_limit: usize,
    ) -> Result<(usize, usize)> {
        let canonical_request = crate::model::PreparedModelRequest::prepare(
            self.request_from_context(context)?,
            self.stock.limits,
            self.io,
        )?;
        let count = if pre_stage {
            stock_count_tokens(self.stock.provider.as_ref(), &canonical_request, self.io).await?
        } else {
            count
        };
        with_history_io(self.io, |work| count.validate(&canonical_request, work))?;
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
            message_limit,
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
            stock_count_tokens(self.stock.provider.as_ref(), &mandatory, self.io).await?;
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
        let count = stock_count_tokens(self.stock.provider.as_ref(), &request, self.io).await?;
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
