//! Receipts at the existing SDK Stream boundary, not provider-internal OS IO.

use super::{HistoryCursor, HistoryIo, HistoryPage, HistoryReadLimits, HistoryReader};
use crate::{Error, Result, core::{Authority, Event}};
use acyclic_fs::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use acyclic_stream::StreamProvider;
use std::mem::size_of;

impl<P: StreamProvider> HistoryReader<P> {
    /// Pins the original committed cut using the caller's native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn pin_with_native_work(
        &self,
        after_revision: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(HistoryCursor, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let cursor = self.pin_with_work(after_revision, Some(&mut native_io)).await?;
        Ok((cursor, native_io.into_custody()))
    }

    /// Reads the original authenticated bounded page on the same native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn read_page_with_native_work(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(HistoryPage, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let read = self.read_page_with_work(cursor, limits, false, Some(&mut native_io)).await?;
        native_io.return_staged_output()?;
        Ok((read.page, native_io.into_custody()))
    }

    /// Reads the original authenticated immutable operation locator and event.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn operation_event_bounded_with_native_work(
        &self,
        operation: crate::OperationId,
        maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<Event>, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let (event, _) = super::super::operations::find_operation_bounded_with_work(
            &self.client, self.verifier.audience(), &self.verifier, operation,
            maximum_bytes, true, &mut Some(&mut native_io),
        ).await?;
        native_io.return_staged_output()?;
        Ok((event, native_io.into_custody()))
    }

    /// Reads the original authenticated effect prefix at the supplied pinned cut.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn effect_events_with_native_work(
        &self,
        cursor: &HistoryCursor,
        effect: crate::EffectId,
        limits: HistoryReadLimits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<Event>, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let (events, _) = super::super::effects::read(
            &self.client, &self.verifier, cursor, effect, limits,
            &mut Some(&mut native_io),
        ).await?;
        native_io.return_staged_output()?;
        Ok((events, native_io.into_custody()))
    }

    /// Projects the archived effect through the original bounded state engine.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn effect_with_native_work(
        &self,
        cursor: &HistoryCursor,
        effect: crate::EffectId,
        limits: HistoryReadLimits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<crate::core::EffectState>, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let state = self.effect_with_retained_quota(
            cursor, effect, limits, Some(&mut native_io),
        ).await?.map(|(state, _)| state);
        native_io.return_staged_output()?;
        Ok((state, native_io.into_custody()))
    }

    /// Pins the same committed Stream cut with a finite SDK-boundary receipt.
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

    /// Reads through the original authenticated page engine with exact visible SDK tail/read counts and original encoded record bytes, including failures.
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

    /// Original immutable operation locator and canonical event, without replay.
    pub async fn operation_event_bounded_measured(
        &self,
        operation: crate::OperationId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<(Option<Event>, u64)>, Error> {
        let mut work = HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            super::super::operations::find_operation_bounded_with_work(
                &self.client, self.verifier.audience(), &self.verifier, operation,
                maximum_bytes, true, &mut Some(&mut work),
            ).await
        }.await;
        work.finish(result)
    }

    /// Reads one effect's authenticated transition prefix at the exact pinned cut, including issuer-authenticated absence.
    pub async fn effect_events_measured(
        &self, cursor: &HistoryCursor, effect: crate::EffectId,
        limits: HistoryReadLimits, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<Vec<Event>>, Error> {
        let mut work = HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            super::super::effects::read(
                &self.client, &self.verifier, cursor, effect, limits,
                &mut Some(&mut work),
            ).await.map(|(events, _)| events)
        }.await;
        work.finish(result)
    }

}

pub(crate) struct HistoryWork<'a> {
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    work: WorkCounters,
    live_bytes: u64,
    staged_bytes: u64,
    staged_items: u64,
}

impl<'a> HistoryWork<'a> {
    pub(crate) fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self { budget, cancellation, work: WorkCounters::default(), live_bytes: 0, staged_bytes: 0, staged_items: 0 }
    }

    pub(crate) fn preflight(&self) -> Result<()> {
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

    fn begin_write(&mut self) -> Result<()> {
        self.check_cancellation()?;
        let next = self.combined(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
        self.verify(next)?;
        self.work = next;
        Ok(())
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

    pub(crate) fn remaining(&self) -> Result<WorkBudget> {
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

    pub(crate) fn cancellation(&self) -> &'a CancellationToken { self.cancellation }

    pub(crate) fn receipt(&self) -> WorkCounters { self.work }

    pub(crate) fn receive_work(&mut self, mut delta: WorkCounters) -> Result<()> {
        let peak = self.live_bytes.checked_add(delta.peak_allocation_bytes)
            .ok_or_else(|| Error::Invalid("nested history peak allocation overflows".into()))?;
        delta.output_bytes = 0;
        delta.items_returned = 0;
        delta.peak_allocation_bytes = peak;
        self.observed(delta)
    }

    pub(crate) fn reserve_bytes(&mut self, bytes: &mut Vec<u8>, target: usize) -> Result<()> {
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

    pub(crate) fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        let staged_bytes = self.staged_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history staged bytes overflow".into()))?;
        let staged_items = self.staged_items.checked_add(count)
            .ok_or_else(|| Error::Invalid("history staged item count overflows".into()))?;
        self.admit_output(staged_bytes, staged_items)?;
        self.staged_bytes = staged_bytes;
        self.staged_items = staged_items;
        Ok(())
    }

    pub(crate) fn return_staged_output(&mut self) -> Result<()> {
        self.return_output(self.staged_bytes, self.staged_items)?;
        self.staged_bytes = 0;
        self.staged_items = 0;
        Ok(())
    }

    /// Raw source bytes have already arrived.
    pub(super) fn receive_record(&mut self, bytes: u64) -> Result<()> {
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

    pub(crate) fn finish<T>(mut self, result: Result<T>) -> MeasuredResult<OperationReceipt<T>, Error> {
        let result = result.and_then(|value| self.return_staged_output().map(|()| value));
        result.map(|value| OperationReceipt { value, work: self.work })
            .map_err(|error| OperationFailure::new(error, self.work))
    }
}

impl HistoryIo for HistoryWork<'_> {
    fn check_cancellation(&self) -> Result<()> { HistoryWork::check_cancellation(self) }
    fn begin_call(&mut self) -> Result<()> { HistoryWork::begin_call(self) }
    fn begin_write(&mut self) -> Result<()> { HistoryWork::begin_write(self) }
    fn receive_record(&mut self, bytes: u64) -> Result<()> { HistoryWork::receive_record(self, bytes) }
    fn release_record(&mut self, bytes: u64) -> Result<()> { HistoryWork::release_record(self, bytes) }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.retain(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        let mut planned = self.work;
        planned.peak_allocation_bytes = planned.peak_allocation_bytes.max(self.live_bytes.checked_add(bytes)
            .ok_or_else(|| Error::Invalid("history allocation admission overflows".into()))?);
        self.verify(planned)
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> { self.release_record(bytes) }
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority> { HistoryWork::clone_authority(self, authority) }
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
    fn reserve_bytes(&mut self, bytes: &mut Vec<u8>, capacity: usize) -> Result<()> { HistoryWork::reserve_bytes(self, bytes, capacity) }
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
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> { HistoryWork::admit_output(self, bytes, count) }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> { HistoryWork::stage_output(self, bytes, count) }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> { HistoryWork::return_output(self, bytes, count) }
    fn remaining(&self) -> Result<WorkBudget> { HistoryWork::remaining(self) }
    fn cancellation(&self) -> &CancellationToken { self.cancellation }
    fn receive_work(&mut self, work: WorkCounters) -> Result<()> { HistoryWork::receive_work(self, work) }
    fn verify_content<'a>(
        &'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<super::HistoryContentFuture<'a, ()>> {
        let build = |work: &'a mut Self| async move {
            let receipt = verifier.verify_measured(reference, reference.descriptor().byte_length(),
                work.remaining()?, work.cancellation).await;
            match receipt {
                Ok(receipt) => work.receive_work(receipt.work),
                Err(failure) => { let _ = work.receive_work(*failure.work); Err(failure.error) }
            }
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
            let receipt = verifier.read_measured(reference, reference.descriptor().byte_length(),
                work.remaining()?, work.cancellation).await;
            match receipt {
                Ok(receipt) => {
                    let retained_bytes = receipt.value.retained_capacity().unwrap_or(0) as u64;
                    work.receive_work(receipt.work)?;
                    work.retain(retained_bytes)?;
                    Ok((receipt.value, retained_bytes))
                }
                Err(failure) => { let _ = work.receive_work(*failure.work); Err(failure.error) }
            }
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
