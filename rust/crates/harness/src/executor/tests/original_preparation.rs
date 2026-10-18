use super::*;

// Original factories from 3d2f72e2a; no candidate preparation code is called.
impl StockExecution<'_> {
    pub(super) async fn transform_projection_original(
        &self,
        input: &ContextInput,
        base: crate::context::Context,
    ) -> Result<crate::context::Context> {
        let mut context = self
            .context_pipeline()
            .transform(input, base, self.stock.limits, self.io)
            .await?;
        let message_count = context.messages.len();
        let request = self.request_taking_context(&mut context)?;
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
        let prefix_count = prepared.request().messages.len() - message_count;
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
        if self.io.is_none() {
            context.messages = prepared.into_messages();
        } else if prefix_count != 0 {
            context.messages = crate::model::clone_messages(&prepared.request().messages, self.io)?;
        }
        crate::context::validate_projected_context(&context, self.stock.limits, self.io)?;
        Ok(context)
    }

    pub(super) async fn capture_response_original(
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
        let mut projection = self
            .transform_projection_original(&context_input, base)
            .await?;
        let accounting = if let Some(capacity) = self.model_capacity()? {
            let prepared = crate::model::PreparedModelRequest::prepare(
                self.request_taking_context(&mut projection)?,
                self.stock.limits,
                self.io,
            )?;
            let count =
                stock_count_tokens(self.stock.provider.as_ref(), &prepared, self.io).await?;
            with_history_io(self.io, |work| count.validate(&prepared, work))?;
            if self.io.is_none() {
                projection.messages = prepared.into_messages();
            }
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

    pub(super) async fn prepared_response_original(
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
        let (mut projection, accounting) = match first {
            None => {
                if self.stock.reconciliation_only {
                    return Err(Error::Indeterminate(input.operation_id));
                }
                self.capture_response_original(journal, input, step, prior_messages, tail)
                    .await?
            }
            Some((reference, accounting)) => (
                retain_preparation(
                    load_json::<ResponseProjection>(journal, reference, self.io).await?,
                )?,
                match accounting {
                    Some(reference) => Some(retain_preparation(
                        load_json::<ContextAccounting>(journal, reference, self.io).await?,
                    )?),
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
        let request = if matches!(
            self.compaction_policy(),
            crate::context::CompactionPolicy::Disabled
        ) {
            self.request_taking_context(&mut projection.context)?
        } else {
            self.request_from_context(&projection.context)?
        };
        let prepared =
            crate::model::PreparedModelRequest::prepare(request, self.stock.limits, self.io)?;
        self.resolve_response_accounting_original(
            journal,
            (input, step),
            projection,
            accounting,
            prepared,
            &records,
        )
        .await
    }

    async fn resolve_response_accounting_original(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        projection: ResponseProjection,
        accounting: Option<ContextAccounting>,
        prepared: crate::model::PreparedModelRequest,
        records: &[ExecutionRecord],
    ) -> Result<crate::model::PreparedModelRequest> {
        let (input, step) = turn;
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
                    .retained_compacted_response(journal, input, step, &projection, records)
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
                    )?,
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
}
