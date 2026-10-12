//! Receipts at the existing SDK Stream boundary, not provider-internal OS IO.

use super::HistoryIo;
use crate::{Error, Result, core::{Authority, Event}};
use acyclic_fs::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use std::mem::size_of;

/// Bounded work and output accounting for history operations.
pub struct HistoryWork<'a> {
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    work: WorkCounters,
    live_bytes: u64,
    staged_bytes: u64,
    staged_items: u64,
}

impl<'a> HistoryWork<'a> {
    /// Starts accounting against the supplied budget and cancellation.
    pub fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self { budget, cancellation, work: WorkCounters::default(), live_bytes: 0, staged_bytes: 0, staged_items: 0 }
    }

    /// Rejects unbounded allowances and cancellation before provider work.
    pub fn preflight(&self) -> Result<()> {
        if !self.budget.has_finite_allowances() {
            return Err(Error::Invalid("history work allowances must be finite".into()));
        }
        self.check_cancellation()
    }

    fn verify(&self, work: WorkCounters) -> Result<()> {
        work.verify(self.budget).map_err(|error| Error::Invalid(error.to_string()))
    }

    fn combined(&self, delta: WorkCounters) -> Result<WorkCounters> {
        self.work.checked_add(delta).map_err(|error| Error::Invalid(error.to_string()))
    }

    fn observed(&mut self, delta: WorkCounters) -> Result<()> {
        self.work = self.combined(delta)?;
        self.verify(self.work)
    }

    fn planned(&mut self, delta: WorkCounters) -> Result<()> {
        self.check_cancellation()?;
        let next = self.combined(delta)?;
        self.verify(next)?;
        self.work = next;
        Ok(())
    }

    pub(crate) fn receipt(&self) -> WorkCounters { self.work }

    pub(crate) fn return_staged_output(&mut self) -> Result<()> {
        self.return_output(self.staged_bytes, self.staged_items)?;
        self.staged_bytes = 0;
        self.staged_items = 0;
        Ok(())
    }

    fn retain(&mut self, bytes: u64) -> Result<()> {
        self.live_bytes = self.live_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history live byte count overflows".into()))?;
        self.work.peak_allocation_bytes = self.work.peak_allocation_bytes.max(self.live_bytes);
        self.verify(self.work)
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

    /// Returns the output or failure with its accumulated work.
    pub fn finish<T>(mut self, result: Result<T>) -> MeasuredResult<OperationReceipt<T>, Error> {
        let result = result.and_then(|value| self.return_staged_output().map(|()| value));
        result.map(|value| OperationReceipt { value, work: self.work })
            .map_err(|error| OperationFailure::new(error, self.work))
    }
}

impl HistoryIo for HistoryWork<'_> {
    fn check_cancellation(&self) -> Result<()> {
        self.cancellation.check().map_err(|_| Error::Invalid("history read cancelled".into()))
    }
    fn begin_call(&mut self) -> Result<()> {
        self.planned(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })
    }
    fn begin_write(&mut self) -> Result<()> {
        self.planned(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })
    }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.work = self.combined(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: bytes,
            source_bytes_read: bytes,
            items_examined: 1,
            ..WorkCounters::default()
        })?;
        let live = self.live_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history live byte count overflows".into()))?;
        self.work.peak_allocation_bytes = self.work.peak_allocation_bytes.max(live);
        self.verify(self.work)?;
        self.live_bytes = live;
        Ok(())
    }
    fn release_record(&mut self, bytes: u64) -> Result<()> {
        self.live_bytes = self.live_bytes.checked_sub(bytes)
            .ok_or_else(|| Error::Invalid("history live byte count underflows".into()))?;
        Ok(())
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.retain(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        let mut planned = self.work;
        planned.peak_allocation_bytes = planned.peak_allocation_bytes.max(self.live_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history allocation admission overflows".into()))?);
        self.verify(planned)
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> { self.release_record(bytes) }
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority> {
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
    fn clone_text(&mut self, value: &str) -> Result<String> {
        let bytes = value.len() as u64;
        self.admit_owned(bytes)?;
        self.planned(WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            bytes_copied: bytes,
            ..WorkCounters::default()
        })?;
        let mut copy = String::new();
        copy.try_reserve_exact(value.len())
            .map_err(|error| Error::Storage(format!("history text allocation failed: {error}")))?;
        copy.push_str(value);
        self.retain(copy.capacity() as u64)?;
        Ok(copy)
    }
    fn reserve_events(&mut self, events: &mut Vec<Event>, maximum: usize) -> Result<()> { self.reserve_item(events, maximum) }
    fn reserve_records(&mut self, records: &mut Vec<super::super::operations::ObservedRecord>, maximum: usize) -> Result<()> { self.reserve_item(records, maximum) }
    fn reserve_messages(&mut self, messages: &mut Vec<crate::conversation::ConversationMessage>, maximum: usize) -> Result<()> { self.reserve_item(messages, maximum) }
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, maximum: usize) -> Result<()> { self.reserve_item(values, maximum) }
    fn reserve_bytes(&mut self, bytes: &mut Vec<u8>, target: usize) -> Result<()> {
        if target <= bytes.capacity() { return Ok(()); }
        let previous = bytes.capacity();
        let growth = (target - previous) as u64;
        let mut planned = self.combined(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        planned.peak_allocation_bytes = planned.peak_allocation_bytes.max(self.live_bytes.checked_add(growth)
            .ok_or_else(|| Error::Invalid("history byte vector live count overflows".into()))?);
        self.check_cancellation()?;
        self.verify(planned)?;
        self.work.allocation_operations = planned.allocation_operations;
        bytes.try_reserve_exact(target - bytes.len())
            .map_err(|error| Error::Storage(format!("history byte allocation failed: {error}")))?;
        self.retain((bytes.capacity() - previous) as u64)
    }
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() { return Ok(()); }
        let old = text.capacity();
        self.admit_owned((capacity - old) as u64)?;
        self.planned(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        text.try_reserve_exact(capacity - text.len())
            .map_err(|error| Error::Storage(format!("history text allocation failed: {error}")))?;
        self.retain((text.capacity() - old) as u64)
    }
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        self.planned(WorkCounters { allocation_operations: u64::from(bytes != 0), ..WorkCounters::default() })
    }
    fn admit_copy(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        self.verify(self.combined(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })?)
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> { self.planned(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }) }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> { self.observed(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }) }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        self.verify(self.combined(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() })?)
    }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        use crate::contract::JsonCodecCharge;
        if let JsonCodecCharge::Release { bytes } = charge {
            return self.release_record(bytes);
        }
        self.check_cancellation()?;
        match charge {
            JsonCodecCharge::Work { bytes, items } => self.planned(WorkCounters {
                bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
            }),
            JsonCodecCharge::Inspect { items } => self.planned(WorkCounters {
                items_examined: items, ..WorkCounters::default()
            }),
            JsonCodecCharge::Retain { bytes } => { self.admit_owned(bytes)?; self.retain(bytes) }
            JsonCodecCharge::Release { bytes } => self.release_record(bytes),
            JsonCodecCharge::Copy { bytes } => self.copy_bytes(bytes),
            JsonCodecCharge::CopyAdmission { bytes } => self.admit_copy(bytes),
            JsonCodecCharge::AllocationOperation { .. } => self.planned(WorkCounters {
                allocation_operations: 1, ..WorkCounters::default()
            }),
            JsonCodecCharge::Read { .. } => Ok(()),
            JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                self.admit_owned(maximum_owned_bytes)?;
                self.retain(maximum_owned_bytes)
            }
        }
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> { self.planned(WorkCounters { bytes_hashed: bytes, ..WorkCounters::default() }) }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        self.verify(self.combined(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default() })?)
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.observed(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default() })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.verify(self.combined(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })?)
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        let staged_bytes = self.staged_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history staged bytes overflow".into()))?;
        let staged_items = self.staged_items.checked_add(count)
            .ok_or_else(|| Error::Invalid("history staged item count overflows".into()))?;
        self.admit_output(staged_bytes, staged_items)?;
        self.staged_bytes = staged_bytes;
        self.staged_items = staged_items;
        Ok(())
    }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.admit_output(bytes, count)?;
        self.work = self.combined(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })?;
        Ok(())
    }
    fn remaining(&self) -> Result<WorkBudget> {
        self.check_cancellation()?;
        let mut budget = self.work.remaining(self.budget).map_err(|error| Error::Invalid(error.to_string()))?;
        budget.peak_allocation_bytes = budget.peak_allocation_bytes.checked_sub(self.live_bytes)
            .ok_or_else(|| Error::Invalid("history live allocation exceeds allowance".into()))?;
        budget.output_bytes = budget.output_bytes.checked_sub(self.staged_bytes)
            .ok_or_else(|| Error::Invalid("history staged output exceeds allowance".into()))?;
        budget.items_returned = budget.items_returned.checked_sub(self.staged_items)
            .ok_or_else(|| Error::Invalid("history staged items exceed allowance".into()))?;
        Ok(budget)
    }
    fn cancellation(&self) -> &CancellationToken { self.cancellation }
    fn receive_work(&mut self, mut delta: WorkCounters) -> Result<()> {
        let peak = self.live_bytes.checked_add(delta.peak_allocation_bytes)
            .ok_or_else(|| Error::Invalid("nested history peak allocation overflows".into()))?;
        delta.output_bytes = 0;
        delta.items_returned = 0;
        delta.peak_allocation_bytes = peak;
        self.observed(delta)
    }
    fn verify_content<'a>(
        &'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<super::HistoryContentFuture<'a, ()>> {
        let build = |work: &'a mut Self| async move {
            let io = crate::filesystem::PreparationIo::from_budget(work.remaining()?, work.cancellation)?;
            let result = async { verifier.verify(reference, reference.descriptor().byte_length(), Some(&io))?.await }.await;
            let received = work.receive_work(io.receipt());
            result.and(received)
        };
        let frame_bytes = super::content_frame_bytes(&build);
        self.admit_owned(frame_bytes)?;
        self.admit_allocation(frame_bytes)?;
        self.retain(frame_bytes)?;
        Ok(super::HistoryContentFuture::Ordinary {
            future: Box::pin(build(self)),
            frame_bytes,
        })
    }
    fn read_content<'a>(
        &'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<super::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        let build = |work: &'a mut Self| async move {
            let io = crate::filesystem::PreparationIo::from_budget(work.remaining()?, work.cancellation)?;
            let result = async { verifier.read(reference, reference.descriptor().byte_length(), Some(&io))?.await }.await;
            let received = work.receive_work(io.receipt());
            let bytes = result?;
            received?;
            let retained_bytes = bytes.retained_capacity().unwrap_or(0) as u64;
            work.retain(retained_bytes)?;
            Ok((bytes, retained_bytes))
        };
        let frame_bytes = super::content_frame_bytes(&build);
        self.admit_owned(frame_bytes)?;
        self.admit_allocation(frame_bytes)?;
        self.retain(frame_bytes)?;
        Ok(super::HistoryContentFuture::Ordinary {
            future: Box::pin(build(self)),
            frame_bytes,
        })
    }
}
