//! The existing aggregate IO boundary on the SDK context.
use super::*;
use crate::store::history::HistoryIo;
use acyclic_fs::WorkCounters;

macro_rules! charge {
    ($name:ident($($arg:ident),*) => $($field:ident = $value:expr),*) => {
        fn $name(&mut self, $($arg: u64),*) -> Result<()> {
            self.charge(WorkCounters { $($field: $value,)* ..Default::default() })
        }
    };
}
macro_rules! admit {
    ($name:ident($($arg:ident),*) => $($field:ident = $value:expr),*) => {
        fn $name(&self, $($arg: u64),*) -> Result<()> {
            self.preflight(WorkCounters { $($field: $value,)* ..Default::default() })
        }
    };
}
impl HistoryIo for &PreparationIo {
    charge!(begin_call() => backend_read_operations = 1);
    charge!(begin_write() => backend_write_operations = 1);
    charge!(copy_bytes(bytes) => bytes_copied = bytes);
    charge!(encoded_bytes(bytes) => bytes_encoded = bytes);
    charge!(hashed_bytes(bytes) => bytes_hashed = bytes);
    charge!(acknowledged(bytes,count) => authority_bytes_written = bytes, authority_records_appended = count);
    charge!(return_output(bytes,count) => output_bytes = bytes, items_returned = count);
    admit!(admit_copy(bytes) => bytes_copied = bytes);
    admit!(admit_encoding(bytes) => bytes_encoded = bytes);
    admit!(admit_acknowledgment(bytes,count) => authority_bytes_written = bytes, authority_records_appended = count);
    admit!(admit_output(bytes,count) => output_bytes = bytes, items_returned = count);
    fn check_cancellation(&self) -> Result<()> {
        PreparationIo::check_cancellation(self)
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> {
        PreparationIo::retain_owned(self, bytes)
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> {
        PreparationIo::release(self, bytes)
    }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.preflight(WorkCounters {
            peak_allocation_bytes: bytes,
            ..Default::default()
        })
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.admit_output(bytes, count)
    }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.spent(WorkCounters {
            authority_records_read: 1,
            authority_bytes_read: bytes,
            items_examined: 1,
            peak_allocation_bytes: bytes,
            ..Default::default()
        })?;
        self.remaining()?;
        self.retain_owned(bytes)
    }
    fn allocation_attempt(&mut self, bytes: u64) -> Result<()> {
        PreparationIo::allocation_attempt(self, bytes)
    }
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error {
        Error::Allocation(cause)
    }
    fn clone_authority(&mut self, source: &Authority) -> Result<Authority> {
        Ok(Authority {
            kind: source.kind,
            id: self.clone_text(&source.id)?,
        })
    }
    fn clone_text(&mut self, source: &str) -> Result<String> {
        crate::executor::stock_string_copy(source, Some(self))
    }
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() {
            return Ok(());
        }
        let requested = (capacity - text.capacity()) as u64;
        self.retain_owned(requested)?;
        let result = self.allocation_attempt(requested).and_then(|()| {
            text.try_reserve_exact(capacity - text.len())
                .map_err(Error::Allocation)
        });
        if let Err(error) = result {
            self.release(requested)?;
            return Err(error);
        }
        self.retain_owned((text.capacity() - capacity) as u64)
    }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        if let crate::contract::JsonCodecCharge::Admission {
            maximum_owned_bytes,
            value_shape,
        } = charge
        {
            return self.retain_owned(
                maximum_owned_bytes
                    .max(crate::contract::json_value_decode_owned_bytes(value_shape)?),
            );
        }
        PreparationIo::codec_charge(self, charge, true)
    }
    fn decode_json_value(&mut self, bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        let decoded = self.decode(
            std::marker::PhantomData,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        let retained = decoded.1.bytes();
        Ok((retain_decoded(decoded), retained))
    }
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget> {
        PreparationIo::remaining(self)
    }
    fn cancellation(&self) -> &acyclic_fs::CancellationToken {
        PreparationIo::cancellation(self)
    }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.spent(work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn original_native_work(&self) -> crate::NativeWork<'_> {
        PreparationIo::original_native_work(self)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn absorb_native_custody(&mut self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        PreparationIo::absorb_native_custody(self, incoming)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn take_native_custody(&mut self, bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        PreparationIo::take_native_custody(self, bytes)
    }
    fn verify_content<'a>(
        &'a mut self,
        verifier: &'a dyn ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        verifier.verify(reference, reference.descriptor().byte_length(), Some(*self))
    }
    fn read_content<'a>(
        &'a mut self,
        verifier: &'a dyn ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        verifier.read(reference, reference.descriptor().byte_length(), Some(*self))
    }
}
