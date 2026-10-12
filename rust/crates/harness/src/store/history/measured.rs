//! Aggregate accounting on the existing IO context.
use super::HistoryIo;
use crate::filesystem::PreparationIo;
use crate::{
    Error, Result,
    core::{Authority, Event},
};
use acyclic_fs::{
    CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkCounters,
};

/// Aggregate work with deferred output admission.
pub struct HistoryWork {
    io: PreparationIo,
    staged_bytes: u64,
    staged_items: u64,
}

impl HistoryWork {
    /// Uses the caller's checked IO context.
    pub fn new(io: PreparationIo) -> Self {
        Self {
            io,
            staged_bytes: 0,
            staged_items: 0,
        }
    }
    pub(crate) fn receipt(&self) -> WorkCounters {
        self.io.receipt()
    }
    /// Checks cancellation before aggregate work.
    pub fn preflight(&self) -> Result<()> {
        self.check_cancellation()
    }
    /// Commits the deferred output counters.
    pub fn return_staged_output(&mut self) -> Result<()> {
        self.return_output(self.staged_bytes, self.staged_items)?;
        self.staged_bytes = 0;
        self.staged_items = 0;
        Ok(())
    }
    /// Returns the result and its actual work.
    pub fn finish<T>(mut self, result: Result<T>) -> MeasuredResult<OperationReceipt<T>, Error> {
        let result = result.and_then(|value| self.return_staged_output().map(|()| value));
        let work = self.io.receipt();
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }
    /// Transfers the retained native allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_custody(self) -> Result<acyclic_fs::NativeAllocation> {
        let held = self.io.owned_bytes()?;
        self.io.into_native_custody(held)
    }

    fn overflow(&self) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.original_native_work().is_some() {
            return Error::NativeWork {
                cause: acyclic_fs::WorkError::Overflow.into(),
                custody: None,
            };
        }
        self.io.invalid("history output count overflows")
    }
}

impl HistoryIo for HistoryWork {
    #[cfg(not(target_arch = "wasm32"))]
    fn original_native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        self.io.original_native_work()
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn absorb_native_custody(&mut self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        self.io.absorb_native_custody(incoming)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn take_native_custody(&mut self, bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        self.io.take_native_custody(bytes)
    }
    fn check_cancellation(&self) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.original_native_work().is_some() {
            return self.io.check_cancellation();
        }
        self.io
            .cancellation()
            .check()
            .map_err(|_| Error::Invalid("history read cancelled".into()))
    }
    fn begin_call(&mut self) -> Result<()> {
        HistoryIo::begin_call(&mut &self.io)
    }
    fn begin_write(&mut self) -> Result<()> {
        HistoryIo::begin_write(&mut &self.io)
    }
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.original_native_work().is_some() {
            return Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause),
                custody: None,
            };
        }
        Error::Allocation(cause)
    }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.io.spent(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: bytes,
            source_bytes_read: if self.io.original_native_work().is_some() {
                0
            } else {
                bytes
            },
            peak_allocation_bytes: bytes,
            items_examined: 1,
            ..Default::default()
        })?;
        self.io.remaining()?;
        self.io.retain_owned(bytes)
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> {
        self.io.retain_owned(bytes)
    }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.io.original_native_work() {
            return work.admit_retained(bytes).map_err(Error::from);
        }
        HistoryIo::admit_owned(&&self.io, bytes)
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> {
        self.io.release(bytes)
    }
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority> {
        Ok(Authority {
            kind: authority.kind,
            id: self.clone_text(&authority.id)?,
        })
    }
    fn clone_text(&mut self, text: &str) -> Result<String> {
        self.admit_copy(text.len() as u64)?;
        let mut value = String::new();
        self.reserve_text(&mut value, text.len())?;
        value.push_str(text);
        if let Err(error) = self.copy_bytes(text.len() as u64) {
            let held = value.capacity() as u64;
            drop(value);
            self.release_owned(held)?;
            return Err(error);
        }
        Ok(value)
    }

    fn allocation_attempt(&mut self, bytes: u64) -> Result<()> {
        self.io.allocation_attempt(bytes)
    }
    fn reserve_text(&mut self, values: &mut String, capacity: usize) -> Result<()> {
        let old = values.capacity();
        if capacity <= old {
            return Ok(());
        }
        let extra = (capacity - old) as u64;
        self.retain_owned(extra)?;
        let reserved = self.io.allocation_attempt(extra).and_then(|()| {
            values
                .try_reserve_exact(capacity - values.len())
                .map_err(|cause| self.allocation_failure(cause))
        });
        if let Err(error) = reserved {
            self.release_owned(extra)?;
            return Err(error);
        }
        self.retain_owned((values.capacity() - capacity) as u64)
    }

    fn admit_copy(&self, bytes: u64) -> Result<()> {
        HistoryIo::admit_copy(&&self.io, bytes)
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> {
        HistoryIo::copy_bytes(&mut &self.io, bytes)
    }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> {
        HistoryIo::encoded_bytes(&mut &self.io, bytes)
    }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        HistoryIo::admit_encoding(&&self.io, bytes)
    }
    fn codec_charge(&mut self, charge: crate::executor::json::JsonCodecCharge) -> Result<()> {
        use crate::executor::json::JsonCodecCharge;
        if let JsonCodecCharge::Release { bytes } = charge {
            return self.release_owned(bytes);
        }
        self.check_cancellation()?;
        match charge {
            JsonCodecCharge::Admission {
                maximum_owned_bytes,
                ..
            }
            | JsonCodecCharge::Retain {
                bytes: maximum_owned_bytes,
            } => {
                self.admit_owned(maximum_owned_bytes)?;
                self.retain_owned(maximum_owned_bytes)
            }
            JsonCodecCharge::Work { bytes, items } => {
                let next = WorkCounters {
                    bytes_encoded: bytes,
                    items_examined: items,
                    ..Default::default()
                };
                self.io.preflight(next)?;
                self.io.charge(next)
            }
            JsonCodecCharge::Read { .. } => Ok(()),
            _ => HistoryIo::codec_charge(&mut &self.io, charge),
        }
    }
    fn decode_json_value(&mut self, bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        HistoryIo::decode_json_value(&mut &self.io, bytes)
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> {
        HistoryIo::hashed_bytes(&mut &self.io, bytes)
    }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        HistoryIo::admit_acknowledgment(&&self.io, bytes, count)
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        HistoryIo::acknowledged(&mut &self.io, bytes, count)
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        HistoryIo::admit_output(&&self.io, bytes, count)
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        let bytes = self
            .staged_bytes
            .checked_add(bytes)
            .ok_or_else(|| self.overflow())?;
        let count = self
            .staged_items
            .checked_add(count)
            .ok_or_else(|| self.overflow())?;
        self.admit_output(bytes, count)?;
        self.staged_bytes = bytes;
        self.staged_items = count;
        Ok(())
    }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        HistoryIo::return_output(&mut &self.io, bytes, count)
    }
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget> {
        let mut remaining = self.io.remaining()?;
        remaining.output_bytes = remaining
            .output_bytes
            .checked_sub(self.staged_bytes)
            .ok_or_else(|| self.overflow())?;
        remaining.items_returned = remaining
            .items_returned
            .checked_sub(self.staged_items)
            .ok_or_else(|| self.overflow())?;
        Ok(remaining)
    }
    fn cancellation(&self) -> &CancellationToken {
        self.io.cancellation()
    }
    fn receive_work(&mut self, mut delta: WorkCounters) -> Result<()> {
        delta.output_bytes = 0;
        delta.items_returned = 0;
        self.io.charge(delta)
    }
    fn verify_content<'a>(
        &'a mut self,
        verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.original_native_work().is_some()
            && !verifier.supports_read(reference.volume(), Some(&self.io))?
        {
            return Err(self
                .io
                .invalid("content verifier has no native read support"));
        }
        verifier.verify(
            reference,
            reference.descriptor().byte_length(),
            Some(&self.io),
        )
    }
    fn read_content<'a>(
        &'a mut self,
        verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.original_native_work().is_some()
            && !verifier.supports_read(reference.volume(), Some(&self.io))?
        {
            return Err(self
                .io
                .invalid("content verifier has no native read support"));
        }
        verifier.read(
            reference,
            reference.descriptor().byte_length(),
            Some(&self.io),
        )
    }
}
