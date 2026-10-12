//! The preparation context at the existing history boundary.
use super::*;
use crate::store::history::HistoryIo;

impl PreparationIo {
    pub(crate) fn history_io(&self) -> &Self {
        self
    }
    pub(super) fn charged(&self, work: WorkCounters) -> Result<()> {
        self.preflight(work)?;
        self.charge(work).map_err(Into::into)
    }
}

impl HistoryIo for &PreparationIo {
    #[cfg(not(target_arch = "wasm32"))]
    fn original_native_work(&self) -> Option<&acyclic_fs::NativeMaterializationWork> {
        PreparationIo::original_native_work(self)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn absorb_native_custody(&mut self, incoming: acyclic_fs::NativeAllocation) -> Result<()> {
        PreparationIo::absorb_native_custody(self, incoming)
    }
    fn check_cancellation(&self) -> Result<()> {
        PreparationIo::check_cancellation(self)
    }
    fn begin_call(&mut self) -> Result<()> {
        self.charged(WorkCounters {
            backend_read_operations: 1,
            ..Default::default()
        })
    }
    fn begin_write(&mut self) -> Result<()> {
        self.charged(WorkCounters {
            backend_write_operations: 1,
            ..Default::default()
        })
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
        self.retain_owned(bytes).map_err(Into::into)
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> {
        PreparationIo::retain_owned(self, bytes).map_err(Into::into)
    }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        if bytes > self.remaining()?.peak_allocation_bytes {
            return Err(self.invalid("history exceeds live logical byte allowance"));
        }
        Ok(())
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> {
        self.release(bytes).map_err(Into::into)
    }
    fn decode_json_value(&mut self, bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        let decoded = self.decode(
            std::marker::PhantomData::<serde_json::Value>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        let retained = decoded.1.bytes();
        Ok((retain_decoded(decoded), retained))
    }
    fn clone_authority(
        &mut self,
        authority: &crate::core::Authority,
    ) -> Result<crate::core::Authority> {
        Ok(crate::core::Authority {
            kind: authority.kind,
            id: self.clone_text(&authority.id)?,
        })
    }
    fn clone_text(&mut self, text: &str) -> Result<String> {
        stock_string_copy(text, Some(self))
    }

    fn allocation_attempt(&mut self, bytes: u64) -> Result<()> {
        PreparationIo::allocation_attempt(self, bytes)
    }
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error {
        Error::Allocation(cause)
    }
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() {
            return Ok(());
        }
        let old = text.capacity();
        self.retain_owned((capacity - old) as u64)?;
        self.allocation_attempt((capacity - old) as u64)?;
        text.try_reserve_exact(capacity - text.len())
            .map_err(Error::Allocation)?;
        self.retain_owned((text.capacity() - capacity) as u64)
    }

    fn admit_copy(&self, bytes: u64) -> Result<()> {
        self.preflight(WorkCounters {
            bytes_copied: bytes,
            ..Default::default()
        }).map_err(Into::into)
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters {
            bytes_copied: bytes,
            ..Default::default()
        })
    }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charge(WorkCounters {
            bytes_encoded: bytes,
            ..Default::default()
        }).map_err(Into::into)
    }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.preflight(WorkCounters {
            bytes_encoded: bytes,
            ..Default::default()
        }).map_err(Into::into)
    }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        match charge {
            crate::contract::JsonCodecCharge::Admission {
                maximum_owned_bytes,
                value_shape,
            } => self.retain_owned(
                maximum_owned_bytes
                    .max(crate::contract::json_value_decode_owned_bytes(value_shape)?),
            ).map_err(Into::into),
            charge => PreparationIo::codec_charge(self, charge, true),
        }
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters {
            bytes_hashed: bytes,
            ..Default::default()
        })
    }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        self.preflight(WorkCounters {
            authority_bytes_written: bytes,
            authority_records_appended: count,
            ..Default::default()
        }).map_err(Into::into)
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charge(WorkCounters {
            authority_bytes_written: bytes,
            authority_records_appended: count,
            ..Default::default()
        }).map_err(Into::into)
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.preflight(WorkCounters {
            output_bytes: bytes,
            items_returned: count,
            ..Default::default()
        }).map_err(Into::into)
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.admit_output(bytes, count)
    }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charged(WorkCounters {
            output_bytes: bytes,
            items_returned: count,
            ..Default::default()
        })
    }
    fn remaining(&self) -> Result<WorkBudget> {
        PreparationIo::remaining(self).map_err(Into::into)
    }
    fn cancellation(&self) -> &CancellationToken {
        PreparationIo::cancellation(self)
    }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.spent(work).map_err(Into::into)
    }
    fn verify_content<'a>(
        &'a mut self,
        verifier: &'a dyn ContentResidencyVerifier,
        file: &'a FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        verifier.verify(file, file.descriptor().byte_length(), Some(*self))
    }
    fn read_content<'a>(
        &'a mut self,
        verifier: &'a dyn ContentResidencyVerifier,
        file: &'a FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        verifier.read(file, file.descriptor().byte_length(), Some(*self))
    }
}

use crate::executor::stock_string_copy;
