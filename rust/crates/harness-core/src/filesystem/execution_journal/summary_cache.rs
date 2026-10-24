//! The authenticated suffix and its original accounting context live together.
use super::*;

/// Entry cache, not an authority: evicted keys remain in the authenticated roots.
pub(super) const HOT_SUMMARY_ENTRIES: usize = 64;

#[derive(Default)]
pub(super) struct SummaryCache {
    summary: ExecutionSummary,
    io: Option<PreparationIo>,
}

impl SummaryCache {
    pub(super) fn bind(
        &mut self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io
            && let Err(error) = io.check_cancellation()
        {
            self.clear();
            return Err(error);
        }
        let same_ledger = match (&self.io, io) {
            (None, None) => true,
            (Some(retained), Some(io)) => retained.shares_ledger(io),
            _ => false,
        };
        if self.summary.operation != Some(operation) || !same_ledger {
            self.clear();
            self.io = io.map(PreparationIo::scoped).transpose()?;
            self.summary.operation = Some(operation);
        } else if let (Some(retained), Some(current)) = (&mut self.io, io) {
            retained.use_current_context(current);
        }
        self.trim_published(io)?;
        Ok(())
    }

    fn trim_published(&mut self, current: Option<&PreparationIo>) -> Result<()> {
        let summary = &self.summary;
        if summary.retries.len()
            + summary.contexts.len()
            + summary.models.len()
            + summary.tools.len()
            < HOT_SUMMARY_ENTRIES
            || !summary
                .recovery
                .is_some_and(|cut| cut.indexed && cut.state.tail == summary.tail)
        {
            return Ok(());
        }
        // Allocate the replacement pool before releasing anything. Pages and
        // staging own separate pools; pending entries belong to this one and
        // can retire only after the cut contains their complete publication.
        let next_io = current.map(PreparationIo::scoped).transpose()?;
        let previous = std::mem::take(&mut self.summary);
        self.summary = ExecutionSummary {
            operation: previous.operation,
            tail: previous.tail,
            pending_dispatches: previous.pending_dispatches,
            latest_response: previous.latest_response,
            model_dispatched: previous.model_dispatched,
            recovery: previous.recovery,
            pages: previous.pages,
            scratch: previous.scratch,
            partial: true,
            ..Default::default()
        };
        drop(previous.retries);
        drop(previous.contexts);
        drop(previous.models);
        drop(previous.tools);
        drop(previous.pending_steps);
        if let Some(io) = std::mem::replace(&mut self.io, next_io) {
            drop(io);
        }
        Ok(())
    }

    fn clear(&mut self) {
        // Free values before their claims, including when an advance is cancelled.
        self.summary = ExecutionSummary::default();
        drop(self.io.take());
    }

    pub(super) fn reset(
        &mut self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.clear();
        self.bind(operation, io)
    }

    pub(super) fn summary(&self) -> &ExecutionSummary {
        &self.summary
    }

    /// The proof includes the entire event, so altered references cannot reuse it.
    pub(super) fn authenticates(
        &self,
        operation: OperationId,
        sequence: u64,
        retry: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        if self.summary.operation != Some(operation) || sequence > self.summary.tail {
            return Ok(false);
        }
        let key = crate::model::hash_bytes(retry.as_bytes(), io)?;
        let digest = crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                event,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })?;
        let indexed = self.summary.retries.get(&key, io)?;
        if indexed.is_none()
            && self
                .summary
                .recovery
                .is_some_and(|recovery| recovery.indexed)
        {
            return Ok(false);
        }
        if indexed == Some(&(sequence, digest)) {
            Ok(true)
        } else {
            Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "authenticated execution prefix changed",
            ))
        }
    }

    pub(super) fn edit(&mut self) -> SummaryEdit<'_> {
        SummaryEdit {
            cache: self,
            complete: false,
        }
    }
}

impl Drop for SummaryCache {
    fn drop(&mut self) {
        self.clear();
    }
}

pub(super) struct SummaryEdit<'a> {
    cache: &'a mut SummaryCache,
    complete: bool,
}

impl SummaryEdit<'_> {
    pub(super) fn parts(&mut self) -> (&mut ExecutionSummary, Option<&PreparationIo>) {
        (&mut self.cache.summary, self.cache.io.as_ref())
    }

    pub(super) fn finish(mut self) {
        self.complete = true;
    }
}

impl Drop for SummaryEdit<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.cache.clear();
        }
    }
}

#[cfg(test)]
#[path = "summary_cache_tests.rs"]
mod tests;
