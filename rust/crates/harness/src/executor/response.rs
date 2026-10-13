use super::*;

impl StockExecution<'_> {
    pub(super) async fn capture_response(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
        tail: u64,
    ) -> Result<(ResponseProjection, Option<ContextAccounting>)> {
        let base = self
            .canonical_base(journal, input, step, prior_messages)
            .await?;
        let canonical = if step == 0 && input.selected_context.is_some() {
            Some(
                stage_json(
                    journal,
                    input.operation_id,
                    "context:0:canonical",
                    &base,
                    self.io,
                )
                .await?,
            )
        } else {
            None
        };
        let context_input = ContextInput::copy_for_turn(input, step, prior_messages, self.io)?;
        let projection = self.transform_projection(&context_input, base).await?;
        let accounting = if let Some(capacity) = self.model_capacity()? {
            let prepared = crate::model::PreparedModelRequest::prepare(
                self.request_from_context(&projection)?,
                self.stock.limits,
                self.io,
            )?;
            let count = stock_count_tokens(self.stock.provider.as_ref(), &prepared, self.io)?;
            with_history_io(self.io, |work| count.validate(&prepared, work))?;
            Some(ContextAccounting { capacity, count })
        } else {
            None
        };
        let projection = ResponseProjection {
            context: projection,
            canonical,
            checkpoint: None,
        };
        let projection_key = with_history_io(self.io, |work| {
            crate::executor::preparation_format(format_args!("context:{step}"), work)
        })?;
        let reference = stage_json(
            journal,
            input.operation_id,
            &projection_key,
            &projection,
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
        self.verify_execution_owner().await?;
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
        Ok((projection, accounting))
    }

    pub(super) async fn prepared_response(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prior_messages: &[ModelMessage],
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
        let (projection, accounting) = match first {
            None => {
                if self.stock.reconciliation_only {
                    return Err(Error::Indeterminate(input.operation_id));
                }
                self.capture_response(journal, input, step, prior_messages, tail)
                    .await?
            }
            Some((reference, accounting)) => (
                retain_preparation(
                    load_json::<ResponseProjection>(journal, reference, self.io).await?,
                ),
                match accounting {
                    Some(reference) => Some(retain_preparation(
                        load_json::<ContextAccounting>(journal, reference, self.io).await?,
                    )),
                    None => None,
                },
            ),
        };
        crate::context::validate_projected_context(
            &projection.context,
            self.stock.limits,
            self.io,
        )?;
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
        let prepared = crate::model::PreparedModelRequest::prepare(
            self.request_from_context(&projection.context)?,
            self.stock.limits,
            self.io,
        )?;
        match (self.compaction_policy(), accounting) {
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
                if let Some(retained) = self
                    .retained_compacted_response(journal, input, step, &projection, &records)
                    .await?
                {
                    return Ok(retained);
                }
                let token_pressure = policy.needs_compaction(accounting.capacity, input_tokens)?;
                let possible_message_pressure = projection.canonical.is_some()
                    && projection.context.messages.len() > self.compaction_message_limit(true, 0);
                if !possible_message_pressure && !token_pressure {
                    return Ok(prepared);
                }
                let source = match &projection.canonical {
                    Some(reference) => retain_preparation(
                        load_json::<crate::context::Context>(journal, reference, self.io).await?,
                    ),
                    None => crate::context::copy_context(&projection.context, self.io)?,
                };
                let stage_messages = projection
                    .context
                    .messages
                    .len()
                    .saturating_sub(source.messages.len());
                let message_pressure = projection.canonical.is_some()
                    && source.messages.len() > 1
                    && source.messages.len() > self.compaction_message_limit(true, stage_messages);
                if !message_pressure && !token_pressure {
                    return Ok(prepared);
                }
                self.compact_response(journal, input, step, projection, accounting, source)
                    .await
            }
            _ => Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Storage,
                "captured accounting differs from compaction policy",
            )),
        }
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
            let retained = retain_preparation(
                load_json::<ResponseProjection>(journal, retained, self.io).await?,
            );
            let proof = retain_preparation(
                load_json::<crate::context::CompactionReference>(journal, proof, self.io).await?,
            );
            let final_accounting = retain_preparation(
                load_json::<ContextAccounting>(journal, final_accounting, self.io).await?,
            );
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
            if let Some(checkpoint) = &retained.checkpoint {
                let envelope = retain_preparation(
                    load_json::<crate::context::CanonicalContextCheckpoint>(
                        journal, checkpoint, self.io,
                    )
                    .await?,
                );
                envelope.validate(self.stock.limits, self.io)?;
                let source = retain_preparation(
                    load_json::<crate::context::Context>(journal, &envelope.source, self.io)
                        .await?,
                );
                let base = retain_preparation(
                    load_json::<crate::context::Context>(journal, &envelope.retained, self.io)
                        .await?,
                );
                envelope.validate_projection(&source, &base, &proof, self.stock.limits, self.io)?;
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
                crate::context::validate_compaction(
                    &proof,
                    &projection.context,
                    &retained.context,
                    self.io,
                )?;
            }
            let crate::context::CompactionPolicy::Threshold(policy) = self.compaction_policy()
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
            let request = crate::model::PreparedModelRequest::prepare(
                self.request_from_context(&retained.context)?,
                self.stock.limits,
                self.io,
            )?;
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
}
