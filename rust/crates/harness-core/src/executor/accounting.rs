use super::*;

impl InputAccounting {
    /// Verified input size, a real overflow, or provider-admitted unknown size.
    pub(super) fn input_tokens(
        &self,
        request: &crate::model::PreparedModelRequest,
        io: Option<&PreparationIo>,
    ) -> Result<Option<u64>> {
        match self {
            Self::Counted(count) => {
                with_history_io(io, |work| count.validate(request, work)).map(Some)
            }
            Self::Overflow => Err(Error::ModelContextOverflow),
            Self::Unavailable => Ok(None),
        }
    }
}

/// Durable admission needs the bound total, not the provider's per-message
/// planning weights. Repeating those weights would itself be quadratic history.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContextCount {
    pub(super) request_digest: [u8; 32],
    pub(super) input_tokens: u64,
    message_count: u64,
}

impl From<crate::context::ModelTokenCount> for ContextCount {
    fn from(count: crate::context::ModelTokenCount) -> Self {
        Self::from(&count)
    }
}

impl From<&crate::context::ModelTokenCount> for ContextCount {
    fn from(count: &crate::context::ModelTokenCount) -> Self {
        Self {
            request_digest: count.request_digest,
            input_tokens: count.input_tokens,
            message_count: count.message_tokens.len() as u64,
        }
    }
}

impl ContextCount {
    pub(super) fn validate(
        &self,
        request: &crate::model::PreparedModelRequest,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        crate::context::validate_token_count(
            self.request_digest,
            self.message_count,
            self.input_tokens,
            request,
            work,
        )
    }
}

pub(super) fn stock_context_capacity(
    provider: &dyn ModelProvider,
    model: &Model,
    io: Option<&PreparationIo>,
) -> Result<crate::context::ModelContextCapacity> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| {
            stock_boundary(
                io.into(),
                crate::wire::ErrorCode::Unsupported,
                "stock model capacity requires original native work",
            )
        })?;
        io.check_cancellation()?;
        return provider.context_capacity_with_native_work(model, work);
    }
    #[cfg(target_arch = "wasm32")]
    if io.is_some() {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "native stock model capacity is unavailable",
        ));
    }
    provider.context_capacity(model)
}

/// `ceiling` is the most input that changes no decision; a locally proven
/// bound within it spares the provider count request on the turn's critical path.
pub(super) async fn stock_count_tokens(
    provider: &dyn ModelProvider,
    request: &crate::model::PreparedModelRequest,
    ceiling: u64,
    io: Option<&PreparationIo>,
    journal: &dyn ExecutionJournal,
    turn: (OperationId, u32),
    limits: Limits,
) -> Result<ContextCount> {
    #[cfg(not(target_arch = "wasm32"))]
    let stream = if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "stock token accounting requires original native work",
            )
        })?;
        io.check_cancellation()?;
        futures::future::Either::Right(
            provider
                .count_tokens_with_native_work(request, ceiling, work)?
                .await?
                .map(|result| stock_admit_model_event(result, Some(io))),
        )
    } else {
        futures::future::Either::Left(provider.count_tokens(request, ceiling))
    };
    #[cfg(target_arch = "wasm32")]
    let stream = {
        if io.is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "native stock token accounting is unavailable",
            ));
        }
        provider.count_tokens(request, ceiling)
    };
    let mut stream = core::pin::pin!(stream);
    // Preparation may repeat a read after interruption. Its new source
    // observations get a distinct append identity, not a fabricated replay.
    let mut call = None;
    let mut sequence = 0_u64;
    let mut admission = ModelEventAdmission::default();
    while let Some(event) = stream.next().await {
        let event = event?;
        if let Err(error) = admission.observe_count(&event, limits, io) {
            stock_retire_event(event, io)?;
            return Err(error);
        }
        match &event {
            ModelEvent::Wait(_) | ModelEvent::Trace { .. } => {
                let call = call.get_or_insert_with(OperationId::new);
                let key = with_history_io(io, |work| {
                    preparation_format(format_args!("count:{}:{call}:{sequence}", turn.1), work)
                })?;
                let hold = io
                    .map(|io| io.guard_owned(key.capacity() as u64))
                    .transpose()?;
                let appended = model_observation::append(
                    journal,
                    turn.0,
                    &key,
                    (turn.1, ModelPurpose::Count),
                    &event,
                    None,
                    io,
                )
                .await;
                stock_retire_event(event, io)?;
                drop(key);
                drop(hold);
                appended?;
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "count observation sequence overflows",
                    )
                })?;
            }
            ModelEvent::Counted { count } => {
                let counted = ContextCount::from(count);
                // Planning weights are not retained in durable accounting.
                // Return their original credit as the actual DTO is dropped.
                stock_retire_event(event, io)?;
                return Ok(counted);
            }
            _ => {
                stock_retire_event(event, io)?;
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "token-count stream emitted generation output",
                ));
            }
        }
    }
    Err(stock_boundary(
        io,
        crate::wire::ErrorCode::Invalid,
        "token-count stream ended without its verified count",
    ))
}

pub(super) fn stock_json_bound<T: Serialize>(
    value: &T,
    maximum: impl Into<Option<u64>>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let maximum = maximum.into();
    if maximum.is_none() && io.is_none() {
        return Ok(());
    }
    if let Some(io) = io {
        io.check_cancellation()?;
        return crate::executor::json::count_json_bytes_trusted(
            value,
            crate::executor::json::JsonBounds {
                maximum_bytes: maximum,
                maximum_items: Some(io.remaining()?.items_examined),
                maximum_depth: 256,
            },
            io.original_native_work()
                .is_some()
                .then_some(crate::wire::ErrorCode::Invalid),
            &mut |next| io.codec_charge(next, false),
        )
        .map(|_| ());
    }
    crate::executor::json::json_byte_length(value, maximum, &mut None).map(|_| ())
}

impl StockExecution<'_> {
    pub(super) async fn response_accounting(
        &self,
        prepared: &crate::model::PreparedModelRequest,
        journal: &dyn ExecutionJournal,
        turn: (OperationId, u32),
    ) -> Result<ContextAccounting> {
        let capacity = self.model_capacity()?;
        let ceiling = self.input_ceiling(capacity)?;
        let count = match stock_count_tokens(
            self.stock.provider.as_ref(),
            prepared,
            ceiling,
            self.io,
            journal,
            turn,
            self.stock.limits,
        )
        .await
        {
            Ok(count) => {
                with_history_io(self.io, |work| count.validate(prepared, work))?;
                InputAccounting::Counted(count)
            }
            Err(Error::ModelContextOverflow) if !self.single_request => InputAccounting::Overflow,
            // No tokenizer means the generation endpoint admits its own input.
            // Never generate a throwaway completion to obtain a prompt count.
            Err(Error::ModelTokenCountUnavailable) => InputAccounting::Unavailable,
            Err(error) => return Err(error),
        };
        Ok(ContextAccounting { capacity, count })
    }

    /// Disabled compaction sends the installed pipeline's ordinary requests
    /// unchecked: no capacity read and no token count on the turn's path. The
    /// provider remains the authority on its window. Single inference and
    /// capacity-policy turns pass capacity admission.
    pub(super) fn unchecked(&self) -> Result<bool> {
        Ok(!self.single_request
            && matches!(
                self.compaction_policy()?,
                crate::context::CompactionPolicy::Disabled
            ))
    }

    pub(super) fn model_capacity(&self) -> Result<crate::context::ModelContextCapacity> {
        let capacity =
            stock_context_capacity(self.stock.provider.as_ref(), &self.stock.model, self.io)?;
        capacity.validate()?;
        Ok(capacity)
    }

    /// The most input this execution admits without compacting or refusing.
    pub(super) fn input_ceiling(
        &self,
        capacity: crate::context::ModelContextCapacity,
    ) -> Result<u64> {
        capacity.validate()?;
        if self.single_request {
            return Ok(u64::from(capacity.context_tokens));
        }
        match self.compaction_policy()? {
            crate::context::CompactionPolicy::Capacity(policy) => policy.input_ceiling(capacity),
            crate::context::CompactionPolicy::Disabled => Ok(u64::from(capacity.context_tokens)),
        }
    }

    pub(super) fn sampling(&self) -> Result<crate::model::ModelSampling> {
        self.stock
            .resolved_sampling
            .get_or_try_init(|| {
                let profile = self.stock.provider.model_profile(&self.stock.model)?;
                crate::model::ModelSampling::admit(
                    self.stock.sampling,
                    profile.map(|profile| profile.sampling()),
                )
            })
            .copied()
    }

    /// A fresh summary dispatch consumes the same request that passed capacity
    /// admission. A retained attempt instead checks its source during recovery.
    pub(super) async fn prepare_summary_admission(
        &self,
        executor: &StockExecution<'_>,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        purpose: ModelPurpose,
    ) -> Result<Option<crate::model::PreparedModelRequest>> {
        let (_, records) = self
            .model_records(journal, input.operation_id, step, purpose)
            .await?;
        if records
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
        {
            return Ok(None);
        }
        let capacity =
            stock_context_capacity(self.stock.provider.as_ref(), &self.stock.model, self.io)?;
        capacity.validate()?;
        let mut prepared = executor.prepare_request(input, step, &[]).await?;
        self.verify_model_request_content(journal, &mut prepared)
            .await?;
        let ceiling = crate::context::CapacityCompaction::default().input_ceiling(capacity)?;
        let count = match stock_count_tokens(
            self.stock.provider.as_ref(),
            &prepared,
            ceiling,
            self.io,
            journal,
            (input.operation_id, step),
            self.stock.limits,
        )
        .await
        {
            Ok(count) => count,
            Err(Error::ModelTokenCountUnavailable) => {
                return Ok(Some(prepared));
            }
            Err(error) => return Err(error),
        };
        let count = with_history_io(self.io, |work| count.validate(&prepared, work))?;
        if count > ceiling {
            return Err(Error::ModelContextOverflow);
        }
        Ok(Some(prepared))
    }
}
