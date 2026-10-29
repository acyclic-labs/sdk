use super::*;

mod compaction_source;

impl StockExecution<'_> {
    async fn prepare_response_context(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
    ) -> Result<(ResponseProjection, crate::model::PreparedModelRequest)> {
        if let Some(continued) = self
            .continue_response_request(journal, input, step, prior_messages)
            .await?
        {
            return Ok(continued);
        }
        let (context_input, base, working) = self
            .response_base(journal, input, step, prior_messages)
            .await?;
        let (context, prepared) = self
            .prepare_transformed_request(&context_input, base)
            .await?;
        Ok((
            ResponseProjection {
                context,
                canonical: None,
                checkpoint: None,
                working,
            },
            prepared,
        ))
    }

    pub(super) async fn capture_response(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        tail: u64,
    ) -> Result<(
        ResponseProjection,
        Option<ContextAccounting>,
        crate::model::PreparedModelRequest,
    )> {
        let (projection, prepared) = self
            .prepare_response_context(journal, input, step, prior_messages)
            .await?;
        let accounting = if let Some(capacity) = self.model_capacity()? {
            let count =
                stock_count_tokens(self.stock.provider.as_ref(), &prepared, self.io).await?;
            with_history_io(self.io, |work| count.validate(&prepared, work))?;
            Some(ContextAccounting { capacity, count })
        } else {
            None
        };
        let projection_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("context:{step}"), work)
        })?;
        let reference = transcript::stage_projection(
            journal,
            input.operation_id,
            &projection_key,
            &projection,
            &prepared,
            self.io,
        )
        .await?;
        let accounting_ref = if let Some(accounting) = &accounting {
            let accounting_key = with_history_io(self.io, |work| {
                crate::executor::preparation_format(format_args!("context:{step}:accounting"), work)
            })?;
            Some(
                stage_json(
                    journal,
                    input.operation_id,
                    &accounting_key,
                    accounting,
                    self.io,
                )
                .await?,
            )
        } else {
            None
        };
        let prepared_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("context:{step}:prepared"), work)
        })?;
        if !journal
            .append_if_tail(
                input.operation_id,
                tail,
                &prepared_key,
                &ExecutionEvent::ContextPrepared {
                    step,
                    projection: reference,
                    accounting: accounting_ref,
                },
                self.io,
            )?
            .await?
        {
            return Err(Error::Indeterminate(input.operation_id));
        }
        Ok((projection, accounting, prepared))
    }

    pub(super) async fn prepared_response(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        phase: &mut DispatchPhase,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (tail, records) = self
            .model_records(journal, input.operation_id, step, ModelPurpose::Response)
            .await?;
        let mut retained = records.iter().filter_map(|record| match &record.event {
            ExecutionEvent::ContextPrepared {
                step: recorded,
                projection,
                accounting,
            } if *recorded == step => Some((projection, accounting.as_ref())),
            _ => None,
        });
        let first = retained.next();
        if retained.next().is_some() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "response projection was prepared more than once",
            ));
        }
        let (projection, accounting, prepared) = match first {
            None => {
                if self.stock.reconciliation_only {
                    return Err(Error::Indeterminate(input.operation_id));
                }
                self.capture_response(journal, input, step, prior_messages, tail)
                    .await?
            }
            Some((reference, accounting)) => {
                let (projection, prepared) =
                    self.load_response_projection(journal, reference).await?;
                self.validate_response_request(&prepared)?;
                let accounting = match accounting {
                    Some(reference) => Some(retain_preparation(
                        load_json::<ContextAccounting>(journal, reference, self.io).await?,
                    )?),
                    None => None,
                };
                (projection, accounting, prepared)
            }
        };
        if projection.checkpoint.is_some() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "prepared context contains a later checkpoint",
            ));
        }
        if projection.context.current_input_index.is_none() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "prepared response projection differs from its request",
            ));
        }
        self.resolve_response_accounting(
            journal,
            (input, step, prior_messages),
            (projection, accounting),
            prepared,
            &records,
            phase,
        )
        .await
    }

    pub(super) async fn retained_compacted_response(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        projection: &ResponseProjection,
        records: &[ExecutionRecord],
    ) -> Result<Option<crate::model::PreparedModelRequest>> {
        let mut compacted = records.iter().filter_map(|record| match &record.event {
            ExecutionEvent::ContextCompacted {
                step: recorded,
                projection,
                compaction,
                accounting,
            } if *recorded == step => Some((projection, compaction, accounting)),
            _ => None,
        });
        let first = compacted.next();
        if compacted.next().is_some() {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "response context was compacted more than once",
            ));
        }
        if let Some((retained, proof, final_accounting)) = first {
            let (mut retained, request) = self.load_response_projection(journal, retained).await?;
            self.validate_response_request(&request)?;
            retained.context.messages =
                crate::model::clone_messages(&request.request().messages, self.io)?;
            let proof = retain_preparation(
                load_json::<crate::context::CompactionReference>(journal, proof, self.io).await?,
            )?;
            let final_accounting = retain_preparation(
                load_json::<ContextAccounting>(journal, final_accounting, self.io).await?,
            )?;
            crate::context::validate_projected_context(
                &retained.context,
                self.stock.limits,
                self.io,
            )?;
            if retained.canonical != projection.canonical {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "compacted canonical source changed",
                ));
            }
            self.validate_compacted_checkpoint(
                journal,
                (input, step),
                projection,
                &retained,
                &proof,
            )
            .await?;
            let crate::context::CompactionPolicy::Threshold(policy) = self.compaction_policy()?
            else {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "compacted context requires threshold policy",
                ));
            };
            if self.model_capacity()? != Some(final_accounting.capacity)
                || proof.retention != policy.retention
                || proof.summary.as_ref().is_none_or(|summary| {
                    summary.operation_id != input.operation_id || summary.step != step
                })
            {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "compacted context differs from its admission",
                ));
            }
            if policy.needs_compaction(
                final_accounting.capacity,
                with_history_io(self.io, |work| {
                    final_accounting.count.validate(&request, work)
                })?,
            )? {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "retained compacted context exceeds selected capacity",
                ));
            }
            return Ok(Some(request));
        }
        Ok(None)
    }

    async fn resolve_response_accounting(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32, &[ModelMessage]),
        captured: (ResponseProjection, Option<ContextAccounting>),
        prepared: crate::model::PreparedModelRequest,
        records: &[ExecutionRecord],
        phase: &mut DispatchPhase,
    ) -> Result<crate::model::PreparedModelRequest> {
        let (input, step, prior_messages) = turn;
        let (mut projection, accounting) = captured;
        match (self.compaction_policy()?, accounting) {
            (crate::context::CompactionPolicy::Disabled, None) => Ok(prepared),
            (crate::context::CompactionPolicy::Threshold(policy), Some(accounting)) => {
                if self.model_capacity()? != Some(accounting.capacity) {
                    return Err(stock_boundary(
                        self.io,
                        crate::wire::ErrorCode::Conflict,
                        "selected capacity differs from captured accounting",
                    ));
                }
                let input_tokens =
                    with_history_io(self.io, |work| accounting.count.validate(&prepared, work))?;
                let compacted = records.iter().any(|record| {
                    matches!(record.event,
                    ExecutionEvent::ContextCompacted { step: recorded, .. } if recorded == step)
                });
                if compacted {
                    projection.context.messages =
                        crate::model::clone_messages(&prepared.request().messages, self.io)?;
                }
                if let Some(retained) = self
                    .retained_compacted_response(journal, input, step, &projection, records)
                    .await?
                {
                    return Ok(retained);
                }
                let token_pressure = policy.needs_compaction(accounting.capacity, input_tokens)?;
                let selected_base = step == 0 && input.selected_context.is_some();
                let possible_message_pressure = selected_base
                    && prepared.request().messages.len() > self.compaction_message_limit(true, 0);
                if !possible_message_pressure && !token_pressure {
                    return Ok(prepared);
                }
                if !compacted {
                    projection.context.messages =
                        crate::model::clone_messages(&prepared.request().messages, self.io)?;
                }
                let (source, pre_stage) = self
                    .response_compaction_source(journal, turn, &projection, &accounting, policy)
                    .await?;
                let stage_messages = projection
                    .context
                    .messages
                    .len()
                    .saturating_sub(source.messages.len());
                let message_pressure = selected_base
                    && source.messages.len() > 1
                    && source.messages.len() > self.compaction_message_limit(true, stage_messages);
                if !message_pressure && !token_pressure {
                    return Ok(prepared);
                }
                self.compact_response(
                    journal,
                    (input, step, prior_messages.len()),
                    (projection, accounting),
                    (source, pre_stage),
                    policy,
                    phase,
                )
                .await
            }
            _ => Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "captured accounting differs from compaction policy",
            )),
        }
    }

    async fn validate_compacted_checkpoint(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        projection: &ResponseProjection,
        retained: &ResponseProjection,
        proof: &crate::context::CompactionReference,
    ) -> Result<()> {
        let (input, step) = turn;
        let working_changed = retained.working != projection.working;
        if working_changed {
            let reference = retained
                .working
                .as_ref()
                .ok_or_else(|| self.invalid_working_cut())?;
            self.validate_compacted_working_cut(journal, input, step, reference, proof)
                .await?;
        }
        if let Some(checkpoint) = &retained.checkpoint {
            let envelope = retain_preparation(
                load_json::<crate::context::CanonicalContextCheckpoint>(
                    journal, checkpoint, self.io,
                )
                .await?,
            )?;
            envelope.validate(self.stock.limits, self.io)?;
            let source = retain_preparation(
                load_json::<crate::context::Context>(journal, &envelope.source, self.io).await?,
            )?;
            let base = retain_preparation(
                load_json::<crate::context::Context>(journal, &envelope.retained, self.io).await?,
            )?;
            envelope.validate_projection(&source, &base, proof, self.stock.limits, self.io)?;
            if projection.canonical.as_ref() != Some(&envelope.source)
                || envelope.operation_id != input.operation_id
                || input
                    .selected_context
                    .as_ref()
                    .is_none_or(|selected| selected.selection != envelope.selection)
            {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "canonical checkpoint differs from captured selection",
                ));
            }
        } else {
            if projection.canonical.is_some() {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "compacted canonical source has no checkpoint",
                ));
            }
            if !working_changed {
                crate::context::validate_compaction(
                    proof,
                    &projection.context,
                    &retained.context,
                    self.io,
                )?;
            }
        }
        Ok(())
    }
}
