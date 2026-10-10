//! Receipts at the existing SDK Stream boundary, not provider-internal OS IO.

use super::{HistoryCursor, HistoryIo, HistoryPage, HistoryReadLimits, HistoryReader};
use crate::{Error, Result, core::{Authority, Event}};
use acyclic_fs::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use acyclic_stream::StreamProvider;
use std::mem::size_of;

impl<P: StreamProvider> HistoryReader<P> {
    /// Pins the same committed Stream cut with a finite SDK-boundary receipt.
    /// Tail metadata exposes no encoded byte size; none is invented here.
    pub async fn pin_measured(
        &self,
        after_revision: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<HistoryCursor>, Error> {
        let mut work = HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            self.pin_with_work(after_revision, Some(&mut work)).await
        }.await;
        work.finish(result)
    }

    /// Reads through the original authenticated page engine with exact visible
    /// SDK tail/read counts and original encoded record bytes, including failures.
    /// `output_bytes` is the source canonical bytes represented by returned events,
    /// not a re-encoding. Memory counts cover explicit vector capacity and logical
    /// raw-byte retention; opaque provider/serde/allocator internals are excluded.
    pub async fn read_page_measured(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<HistoryPage>, Error> {
        let mut work = HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            self.read_page_with_work(cursor, limits, false, Some(&mut work))
                .await.map(|read| read.page)
        }.await;
        work.finish(result)
    }
}

pub(super) struct HistoryWork<'a> {
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    work: WorkCounters,
    live_bytes: u64,
}

impl<'a> HistoryWork<'a> {
    fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self { budget, cancellation, work: WorkCounters::default(), live_bytes: 0 }
    }

    fn preflight(&self) -> Result<()> {
        if !self.budget.has_finite_allowances() {
            return Err(Error::Invalid("history work allowances must be finite".into()));
        }
        self.check_cancellation()
    }

    pub(super) fn check_cancellation(&self) -> Result<()> {
        self.cancellation.check().map_err(|_| Error::Invalid("history read cancelled".into()))
    }

    fn verify(&self, work: WorkCounters) -> Result<()> {
        work.verify(self.budget).map_err(|error| Error::Invalid(error.to_string()))
    }

    fn combined(&self, delta: WorkCounters) -> Result<WorkCounters> {
        self.work.checked_add(delta).map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Admits a known SDK invocation before making it; failed invocations count.
    pub(super) fn begin_call(&mut self) -> Result<()> {
        self.check_cancellation()?;
        let next = self.combined(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        self.verify(next)?;
        self.work = next;
        Ok(())
    }

    /// Raw source bytes have already arrived. Preserve them even when their
    /// actual size exceeds the budget, before validating or decoding the record.
    pub(super) fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.work = self.combined(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: bytes,
            source_bytes_read: bytes,
            items_examined: 1,
            ..WorkCounters::default()
        })?;
        self.retain(bytes)?;
        self.verify(self.work)
    }

    fn retain(&mut self, bytes: u64) -> Result<()> {
        self.live_bytes = self.live_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history live byte count overflows".into()))?;
        self.work.peak_allocation_bytes = self.work.peak_allocation_bytes.max(self.live_bytes);
        self.verify(self.work)
    }

    pub(super) fn release_record(&mut self, bytes: u64) -> Result<()> {
        self.live_bytes = self.live_bytes.checked_sub(bytes)
            .ok_or_else(|| Error::Invalid("history live byte count underflows".into()))?;
        Ok(())
    }

    pub(super) fn clone_authority(&mut self, authority: &Authority) -> Result<Authority> {
        let bytes = authority.id.len() as u64;
        let mut next = self.combined(WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            bytes_copied: bytes,
            ..WorkCounters::default()
        })?;
        next.peak_allocation_bytes = next.peak_allocation_bytes.max(self.live_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history authority byte count overflows".into()))?);
        self.verify(next)?;
        self.work = next;
        let cloned = authority.clone();
        self.retain(cloned.id.capacity() as u64)?;
        Ok(cloned)
    }

    pub(super) fn reserve_item<T>(&mut self, items: &mut Vec<T>, maximum: usize) -> Result<()> {
        if items.len() < items.capacity() { return Ok(()); }
        let capacity = items.capacity();
        let target = capacity.checked_mul(2).unwrap_or(maximum).max(4).min(maximum);
        let bytes = target.checked_sub(capacity)
            .and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| Error::Invalid("history vector byte count overflows".into()))? as u64;
        let mut planned = self.combined(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        planned.peak_allocation_bytes = planned.peak_allocation_bytes.max(self.live_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history vector live byte count overflows".into()))?);
        self.verify(planned)?;
        self.work.allocation_operations = planned.allocation_operations;
        items.try_reserve_exact(target - items.len())
            .map_err(|error| Error::Storage(format!("history output allocation failed: {error}")))?;
        let actual = items.capacity().checked_sub(capacity)
            .and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| Error::Invalid("history vector capacity overflows".into()))? as u64;
        self.retain(actual)
    }

    pub(super) fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.verify(self.combined(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })?)
    }

    pub(super) fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.admit_output(bytes, count)?;
        self.work = self.combined(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })?;
        Ok(())
    }

    fn finish<T>(self, result: Result<T>) -> MeasuredResult<OperationReceipt<T>, Error> {
        result.map(|value| OperationReceipt { value, work: self.work })
            .map_err(|error| OperationFailure::new(error, self.work))
    }
}

impl HistoryIo for HistoryWork<'_> {
    fn check_cancellation(&self) -> Result<()> { HistoryWork::check_cancellation(self) }
    fn begin_call(&mut self) -> Result<()> { HistoryWork::begin_call(self) }
    fn receive_record(&mut self, bytes: u64) -> Result<()> { HistoryWork::receive_record(self, bytes) }
    fn release_record(&mut self, bytes: u64) -> Result<()> { HistoryWork::release_record(self, bytes) }
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority> { HistoryWork::clone_authority(self, authority) }
    fn reserve_events(&mut self, events: &mut Vec<Event>, maximum: usize) -> Result<()> { self.reserve_item(events, maximum) }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> { HistoryWork::admit_output(self, bytes, count) }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> { HistoryWork::return_output(self, bytes, count) }
}
