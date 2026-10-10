//! SDK boundaries on the original native ledger, without another budget/meter.

use super::HistoryIo;
use crate::{Error, Result, core::{Authority, Event}};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, WorkCounters};
use std::mem::size_of;
use std::cell::RefCell;

pub(crate) struct NativeHistoryIo<'a> {
    work: &'a NativeMaterializationWork,
    custody: NativeAllocation,
    native_failure: RefCell<Option<NativeTreeMaterializationError>>,
}

fn error(error: impl std::fmt::Display) -> Error { Error::Storage(format!("native history work: {error}")) }

pub(crate) fn native_work_io(
    work: &NativeMaterializationWork,
) -> std::result::Result<NativeHistoryIo<'_>, NativeTreeMaterializationError> {
    Ok(NativeHistoryIo { work, custody: work.retain_owned(0)?, native_failure: RefCell::new(None) })
}

impl NativeHistoryIo<'_> {
    // Only the immediate native caller may unwrap this scoped marker. It is
    // never publication/reconciliation evidence or a transport error.
    pub(crate) fn take_native_failure(&self) -> Option<NativeTreeMaterializationError> {
        self.native_failure.borrow_mut().take()
    }
    fn native_error(&self, error: NativeTreeMaterializationError) -> Error {
        let mut first = self.native_failure.borrow_mut();
        if first.is_none() { *first = Some(error); }
        Error::Indeterminate(crate::OperationId::from_bytes([0; 16]))
    }
    fn charge(&self, delta: WorkCounters) -> Result<()> {
        self.work.charge(delta).map_err(|error| self.native_error(error))
    }
    fn observe(&self, delta: WorkCounters) -> Result<()> {
        self.work.spent(delta).map_err(|error| self.native_error(error))?;
        self.work.remaining().map_err(|error| self.native_error(error))?;
        Ok(())
    }
    fn retain(&mut self, bytes: u64) -> Result<()> {
        let bytes = self.custody.bytes().checked_add(bytes)
            .ok_or_else(|| self.native_error(NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
        self.custody.resize_retained(bytes).map_err(|error| self.native_error(error))
    }
    fn reserve<T>(&mut self, values: &mut Vec<T>, maximum: usize) -> Result<()> {
        if values.len() < values.capacity() { return Ok(()); }
        let old = values.capacity();
        let target = old.checked_mul(2).unwrap_or(maximum).max(4).min(maximum);
        let extra = target.checked_sub(old).and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| self.native_error(NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))? as u64;
        self.admit_owned(extra)?;
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        values.try_reserve_exact(target - values.len()).map_err(error)?;
        let actual = values.capacity().checked_sub(old).and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| self.native_error(NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))? as u64;
        self.retain(actual)
    }
}

impl HistoryIo for NativeHistoryIo<'_> {
    fn check_cancellation(&self) -> Result<()> {
        self.work.cancellation().check()
            .map_err(|_| self.native_error(NativeTreeMaterializationError::Cancelled))
    }
    fn begin_call(&mut self) -> Result<()> { self.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() }) }
    fn begin_write(&mut self) -> Result<()> { self.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() }) }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        // Receiver prefix precedes cancellation, quota checks and all decoding.
        self.work.spent(WorkCounters { authority_records_read: 1, authority_bytes_read: bytes,
            items_examined: 1, peak_allocation_bytes: bytes, ..WorkCounters::default() })
            .map_err(|error| self.native_error(error))?;
        self.retain(bytes)?;
        self.work.remaining().map_err(|error| self.native_error(error))?;
        Ok(())
    }
    fn release_record(&mut self, bytes: u64) -> Result<()> {
        self.custody.release(bytes).map_err(|error| self.native_error(error))
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.retain(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.work.admit_retained(bytes).map_err(|error| self.native_error(error))
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
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() { return Ok(()); }
        let old = text.capacity();
        self.admit_owned((capacity - old) as u64)?;
        self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
        text.try_reserve_exact(capacity - text.len()).map_err(error)?;
        self.retain((text.capacity() - old) as u64)
    }
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        self.charge(WorkCounters { allocation_operations: u64::from(bytes != 0), ..WorkCounters::default() })
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> { self.charge(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }) }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> { self.observe(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() }) }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.work.admit(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() })
            .map_err(|error| self.native_error(error))
    }
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
        self.work.admit(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default() })
            .map_err(|error| self.native_error(error))
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.observe(WorkCounters { authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default() })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.work.admit(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })
            .map_err(|error| self.native_error(error))
    }
    fn stage_output(&mut self, _bytes: u64, _count: u64) -> Result<()> {
        // This borrowed adapter verifies guards; no archive values escape it.
        self.check_cancellation()
    }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charge(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })
    }
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget> {
        self.work.remaining().map_err(|error| self.native_error(error))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{CancellationToken, WorkBudget, WorkError};

    fn control_budget() -> WorkBudget {
        WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            ..WorkBudget::default()
        }
    }

    #[test]
    fn native_guard_denial_preserves_original_typed_cause_without_error_allocation() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(control_budget(), &cancellation)
            .expect("original meter control allocation");
        let prefix = work.receipt();
        let mut io = native_work_io(&work).expect("zero-byte original custody transfer");
        assert!(matches!(io.begin_call(), Err(Error::Indeterminate(operation))
            if operation == crate::OperationId::from_bytes([0; 16])));
        assert!(matches!(io.take_native_failure(),
            Some(NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations", ..
            }))));
        assert_eq!(work.receipt(), prefix);
        assert!(io.take_native_failure().is_none());
        assert!(io.admit_owned(1).is_err());
        assert!(matches!(io.take_native_failure(),
            Some(NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }))));
        assert_eq!(work.receipt(), prefix);
    }

    #[test]
    fn original_native_cancellation_keeps_typed_cause_and_cleanup_live() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(control_budget(), &cancellation)
            .expect("original meter control allocation");
        let mut io = native_work_io(&work).expect("zero-byte original custody transfer");
        let prefix = work.receipt();
        cancellation.cancel();
        assert!(io.check_cancellation().is_err());
        assert!(matches!(io.take_native_failure(), Some(NativeTreeMaterializationError::Cancelled)));
        io.release_owned(0).expect("custody cleanup does not consult cancellation");
        assert_eq!(work.receipt(), prefix);
        assert!(matches!(native_work_io(&work), Err(NativeTreeMaterializationError::Cancelled)));
    }
}
