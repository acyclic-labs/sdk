use super::*;

pub(super) fn stock_context_capacity(
    provider: &dyn ModelProvider,
    model: &Model,
    io: Option<&PreparationIo>,
) -> Result<crate::context::ModelContextCapacity> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
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
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    if io.is_some() {
        return Err(stock_boundary(
            io.into(),
            crate::wire::ErrorCode::Unsupported,
            "native stock model capacity is unavailable",
        ));
    }
    provider.context_capacity(model)
}

pub(super) fn stock_count_tokens(
    provider: &dyn ModelProvider,
    request: &crate::model::PreparedModelRequest,
    io: Option<&PreparationIo>,
) -> Result<crate::context::ModelTokenCount> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| {
            stock_boundary(
                io.into(),
                crate::wire::ErrorCode::Unsupported,
                "stock token accounting requires original native work",
            )
        })?;
        io.check_cancellation()?;
        let (count, custody) = provider.count_tokens_with_native_work(request, work)?;
        io.absorb_native_custody(custody)?;
        return Ok(count);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    if io.is_some() {
        return Err(stock_boundary(
            io.into(),
            crate::wire::ErrorCode::Unsupported,
            "native stock token accounting is unavailable",
        ));
    }
    provider.count_tokens(request)
}

pub(super) fn stock_context_prefix(
    context: &crate::context::Context,
    through: usize,
    io: Option<&PreparationIo>,
) -> Result<crate::context::Context> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        io.original_native_work().ok_or_else(|| {
            stock_boundary(
                io.into(),
                crate::wire::ErrorCode::Unsupported,
                "stock summary prefix requires original native work",
            )
        })?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    if io.is_some() {
        return Err(stock_boundary(
            io.into(),
            crate::wire::ErrorCode::Unsupported,
            "native stock summary prefix is unavailable",
        ));
    }
    with_history_io(io, |work| {
        crate::context::clone_context(context, Some(through), work).map(|(prefix, _)| prefix)
    })
}

pub(super) fn stock_json_bound<T: Serialize>(
    value: &T,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        return crate::executor::json::validate_json_byte_bound_native_trusted(
            value,
            crate::executor::json::JsonBounds {
                maximum_bytes: maximum,
                maximum_items: io.remaining()?.items_examined,
                maximum_depth: 256,
            },
            crate::wire::ErrorCode::Invalid,
            &mut |next| io.codec_charge(next, false),
        );
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let _bytes_hold;
        let bytes = io.encode(value, maximum, false)?;
        _bytes_hold = io.guard_bytes(&bytes)?;
        return Ok(());
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    crate::executor::json::json_byte_length(value, maximum, &mut None).map(|_| ())
}

pub(super) fn stock_compact(
    context: &crate::context::Context,
    maximum: usize,
    summary: Option<crate::context::ContextSummary>,
    retention: &crate::context::CompactionRetention,
    io: Option<&PreparationIo>,
) -> Result<(crate::context::Context, crate::context::CompactionReference)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| {
            stock_boundary(
                io.into(),
                crate::wire::ErrorCode::Unsupported,
                "stock compaction requires original native work",
            )
        })?;
        let (retention, custody) = crate::context::clone_retention_native(retention, work)?;
        io.absorb_native_custody(custody)?;
        return crate::context::DurableContextProvider::compact(
            context,
            maximum,
            summary,
            retention,
            Some(io),
        );
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    if io.is_some() {
        return Err(stock_boundary(
            io.into(),
            crate::wire::ErrorCode::Unsupported,
            "native stock compaction is unavailable",
        ));
    }
    crate::context::DurableContextProvider::compact(
        context,
        maximum,
        summary,
        retention.clone(),
        None,
    )
}

impl StockExecution<'_> {
    pub(super) fn model_capacity(&self) -> Result<Option<crate::context::ModelContextCapacity>> {
        match self.compaction_policy() {
            crate::context::CompactionPolicy::Disabled => Ok(None),
            crate::context::CompactionPolicy::Threshold(policy) => {
                let capacity = stock_context_capacity(
                    self.stock.provider.as_ref(),
                    &self.stock.model,
                    self.io,
                )?;
                policy.validate(capacity, self.output_tokens())?;
                Ok(Some(capacity))
            }
        }
    }

    pub(super) fn output_budget(&self) -> Result<Option<u32>> {
        match (self.compaction_policy(), self.model_capacity()?) {
            (crate::context::CompactionPolicy::Threshold(policy), Some(capacity)) => {
                Ok(Some(policy.validate(capacity, self.output_tokens())?))
            }
            _ => Ok(self.output_tokens()),
        }
    }

    pub(super) async fn validate_summary_capacity(
        &self,
        executor: &StockExecution<'_>,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
    ) -> Result<()> {
        let (_, records) = self
            .model_records(journal, input.operation_id, step, ModelPurpose::Summary)
            .await?;
        if records
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ModelStarted { .. }))
        {
            return Ok(());
        }
        let capacity =
            stock_context_capacity(self.stock.provider.as_ref(), &self.stock.model, self.io)?;
        capacity.validate()?;
        let output = executor.output_tokens().ok_or_else(|| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary requires a caller-selected output token budget",
            )
        })?;
        if output == 0 || output > capacity.output_tokens {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary output exceeds selected model capacity",
            ));
        }
        let prepared = executor.prepare_request(input, step, &[]).await?;
        self.verify_model_request_content(journal, &prepared)
            .await?;
        let count = with_history_io(self.io, |work| {
            stock_count_tokens(self.stock.provider.as_ref(), &prepared, self.io)?
                .validate(&prepared, work)
        })?;
        if count + u64::from(output) > u64::from(capacity.context_tokens) {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "summary input exceeds selected model capacity",
            ));
        }
        Ok(())
    }
}
