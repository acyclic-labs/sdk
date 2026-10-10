//! SDK boundaries on the original native ledger, without another budget/meter.

use super::HistoryIo;
use crate::{Error, Result, core::{Authority, Event}};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::mem::size_of;

pub(crate) struct NativeHistoryIo<'a> {
    work: &'a NativeMaterializationWork,
    custody: NativeAllocation,
}

fn error(error: impl std::fmt::Display) -> Error { Error::Storage(format!("native history work: {error}")) }

pub(crate) fn native_work_io(work: &NativeMaterializationWork) -> Result<NativeHistoryIo<'_>> {
    Ok(NativeHistoryIo { work, custody: work.retain_owned(0).map_err(error)? })
}

impl NativeHistoryIo<'_> {
    fn charge(&self, delta: WorkCounters) -> Result<()> { self.work.charge(delta).map_err(error) }
    fn observe(&self, delta: WorkCounters) -> Result<()> {
        self.work.spent(delta).map_err(error)?;
        self.work.remaining().map_err(error)?;
        Ok(())
    }
    fn retain(&mut self, bytes: u64) -> Result<()> {
        let bytes = self.custody.bytes().checked_add(bytes)
            .ok_or_else(|| Error::Storage("native history custody overflows".into()))?;
        self.custody.resize_retained(bytes).map_err(error)
    }
    fn reserve<T>(&mut self, values: &mut Vec<T>, maximum: usize) -> Result<()> {
        if values.len() < values.capacity() { return Ok(()); }
        let old = values.capacity();
        let target = old.checked_mul(2).unwrap_or(maximum).max(4).min(maximum);
        let extra = target.checked_sub(old).and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| Error::Storage("native history vector capacity overflows".into()))? as u64;
        self.admit_owned(extra)?;
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        values.try_reserve_exact(target - values.len()).map_err(error)?;
        let actual = values.capacity().checked_sub(old).and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| Error::Storage("native history vector capacity overflows".into()))? as u64;
        self.retain(actual)
    }
}

impl HistoryIo for NativeHistoryIo<'_> {
    fn check_cancellation(&self) -> Result<()> { self.work.cancellation().check().map_err(error) }
    fn begin_call(&mut self) -> Result<()> { self.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() }) }
    fn begin_write(&mut self) -> Result<()> { self.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() }) }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        // Receiver prefix precedes cancellation, quota checks and all decoding.
        self.observe(WorkCounters { authority_records_read: 1, bytes_read: bytes, items_examined: 1, ..WorkCounters::default() })?;
        self.retain(bytes)
    }
    fn release_record(&mut self, bytes: u64) -> Result<()> { self.custody.release(bytes).map_err(error) }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.retain(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        if bytes > self.work.remaining().map_err(error)?.peak_allocation_bytes {
            return Err(Error::Storage("native history exceeds live logical byte allowance".into()));
        }
        Ok(())
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> { self.release_record(bytes) }
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority> {
        Ok(Authority { kind: authority.kind, id: self.clone_text(&authority.id)? })
    }
    fn clone_text(&mut self, text: &str) -> Result<String> {
        self.admit_owned(text.len() as u64)?;
        self.charge(WorkCounters { bytes_copied: text.len() as u64, allocation_operations: u64::from(!text.is_empty()), ..WorkCounters::default() })?;
        let mut copy = String::new();
        copy.try_reserve_exact(text.len()).map_err(error)?;
        copy.push_str(text);
        self.retain(copy.capacity() as u64)?;
        Ok(copy)
    }
    fn reserve_events(&mut self, values: &mut Vec<Event>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_records(&mut self, values: &mut Vec<acyclic_stream::Record>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_messages(&mut self, values: &mut Vec<crate::conversation::ConversationMessage>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_bytes(&mut self, values: &mut Vec<u8>, capacity: usize) -> Result<()> {
        if capacity <= values.capacity() { return Ok(()); }
        let old = values.capacity();
        self.admit_owned((capacity - old) as u64)?;
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        values.try_reserve_exact(capacity - values.len()).map_err(error)?;
        self.retain((values.capacity() - old) as u64)
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> { self.charge(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }) }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> { self.observe(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }) }
    fn admit_encoding(&self, bytes: u64) -> Result<()> { self.work.admit(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }).map_err(error) }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        use crate::contract::JsonCodecCharge;
        if let JsonCodecCharge::Release { bytes } = charge { return self.release_owned(bytes); }
        self.check_cancellation()?;
        match charge {
            JsonCodecCharge::Work { bytes, items } => self.charge(WorkCounters { bytes_encoded: bytes, items_examined: items, ..WorkCounters::default() }),
            JsonCodecCharge::Retain { bytes } => { self.admit_owned(bytes)?; self.retain(bytes) }
            JsonCodecCharge::Admission { maximum_owned_bytes } => { self.admit_owned(maximum_owned_bytes)?; self.retain(maximum_owned_bytes) }
            JsonCodecCharge::Copy { bytes } => self.copy_bytes(bytes),
            JsonCodecCharge::Read { .. } => Ok(()),
            JsonCodecCharge::Release { bytes } => self.release_owned(bytes),
        }
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> { self.charge(WorkCounters { bytes_hashed: bytes, ..WorkCounters::default() }) }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        self.work.admit(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default() }).map_err(error)
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.observe(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default() })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.work.admit(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() }).map_err(error)
    }
    fn stage_output(&mut self, _bytes: u64, _count: u64) -> Result<()> {
        // This borrowed adapter verifies guards; no archive values escape it.
        self.check_cancellation()
    }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charge(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })
    }
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget> { self.work.remaining().map_err(error) }
    fn cancellation(&self) -> &acyclic_fs::CancellationToken { self.work.cancellation() }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.observe(work)
    }
    fn verify_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef) -> crate::conversation::ContentFuture<'a, Result<()>> {
        Box::pin(async move {
            let result = verifier.verify_measured(reference, reference.descriptor().byte_length(), self.remaining()?, self.cancellation()).await;
            match result {
                Ok(receipt) => self.receive_work(receipt.work),
                Err(failure) => { let _ = self.receive_work(*failure.work); Err(failure.error) }
            }
        })
    }
    fn read_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef) -> crate::conversation::ContentFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let result = verifier.read_measured(reference, reference.descriptor().byte_length(), self.remaining()?, self.cancellation()).await;
            match result {
                Ok(receipt) => { self.receive_work(receipt.work)?; self.retain(receipt.value.capacity() as u64)?; Ok(receipt.value) }
                Err(failure) => { let _ = self.receive_work(*failure.work); Err(failure.error) }
            }
        })
    }
}
