//! One request through the executor's original admission and terminal journal.
use super::*;

/// Terminal content from one committed model dispatch. Replays use that same dispatch.
pub struct InferenceResult<'a> {
    /// Exact authoritative assistant text, or deltas when no final message exists.
    text: String,
    /// Original provider completion metadata, including reported usage.
    metadata: Value,
    _custody: Option<crate::conversation::ContentHold<'a>>,
}

impl InferenceResult<'_> {
    /// Borrows the committed dispatch's authoritative text.
    pub fn text(&self) -> &str {
        &self.text
    }
    /// Borrows the original provider completion metadata.
    pub fn metadata(&self) -> &Value {
        &self.metadata
    }

    pub(crate) fn decode<T: PreparationRecord>(
        &self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<T> {
        if self.text.len() as u64 > limits.render_bytes {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "inference result exceeds render bound",
            ));
        }
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let (value, hold) = io.decode(
                std::marker::PhantomData::<T>,
                self.text.as_bytes(),
                crate::wire::ErrorCode::Invalid,
            )?;
            hold.into_retained()?;
            return Ok(value);
        }
        crate::contract::json_from_slice(self.text.as_bytes())
            .map_err(|error| Error::Invalid(format!("inference result is invalid: {error}")))
    }
}

impl StockExecutor {
    /// Runs one tools-disabled request, without a tool loop or compaction.
    /// Capacity, counting, claim, recovery and terminal publication are the ordinary executor's.
    pub async fn infer<'io>(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        io: Option<&'io PreparationIo>,
    ) -> Result<InferenceResult<'io>> {
        validate_journal(journal, io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work)
            && !self.supports_native_execution(work)?
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "inference provider has no native admission",
            ));
        }
        let mut execution = StockExecution::ordinary(self);
        execution.io = io;
        execution.single_request = true;
        let events = execution.model_step(journal, input, 0, &[]).await?;
        let (_, text, metadata) =
            execution.collect_step_events(input, 0, events, &mut Vec::new())?;
        #[cfg(feature = "filesystem")]
        let custody = io
            .map(|io| {
                let bytes = (text.capacity() as u64)
                    .checked_add(crate::executor::json::json_value_logical_owned_bytes(
                        &metadata,
                    )?)
                    .ok_or_else(|| {
                        stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "inference result ownership overflows",
                        )
                    })?;
                io.guard_owned(bytes)
            })
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let custody = None;
        Ok(InferenceResult {
            text,
            metadata,
            _custody: custody,
        })
    }
}
