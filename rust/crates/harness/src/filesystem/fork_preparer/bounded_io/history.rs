//! The preparation context at the existing history boundary.
use super::*;
use crate::store::history::{HistoryContentFuture, HistoryIo};

impl PreparationIo {
    pub(crate) fn history_io(&self) -> &Self {
        self
    }
    pub(super) fn charged(&self, work: WorkCounters) -> Result<()> {
        self.preflight(work)?;
        self.charge(work)
    }
    fn reserve_history<T>(&self, values: &mut Vec<T>, maximum: usize) -> Result<()> {
        if values.len() < values.capacity() {
            return Ok(());
        }
        let target = values
            .capacity()
            .checked_mul(2)
            .unwrap_or(maximum)
            .max(4)
            .min(maximum);
        if target <= values.capacity() {
            return Err(self.invalid("history vector exceeds declared maximum"));
        }
        reserve(values, target, Some(self))
    }
    pub(crate) fn history_future<'a, T, F>(
        &'a self,
        build: impl FnOnce() -> F,
    ) -> Result<HistoryContentFuture<'a, T>>
    where
        F: Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
    {
        let bytes = std::mem::size_of::<F>() as u64;
        self.retain_owned(bytes)?;
        self.allocation_attempt(bytes)?;
        Ok(HistoryContentFuture::Ordinary {
            future: Box::pin(build()),
            frame_bytes: bytes,
        })
    }
    pub(crate) async fn finish_future<T>(&self, future: HistoryContentFuture<'_, T>) -> Result<T> {
        let held = match &future {
            HistoryContentFuture::Ordinary { frame_bytes, .. } => {
                Some(self.guard_owned(*frame_bytes)?)
            }
            #[cfg(not(target_arch = "wasm32"))]
            HistoryContentFuture::Native(_) => None,
        };
        let result = future.await;
        drop(held);
        result
    }
    pub(crate) async fn verify_publication_lease(
        &self,
        lease: &dyn crate::resource_preparation::CreatedTaskLease,
    ) -> Result<()> {
        self.finish_future(lease.verify_with_io(self)?).await
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
        self.retain_owned(bytes)
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> {
        PreparationIo::retain_owned(self, bytes)
    }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        if bytes > self.remaining()?.peak_allocation_bytes {
            return Err(self.invalid("history exceeds live logical byte allowance"));
        }
        Ok(())
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> {
        self.release(bytes)
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
        clone_text(text, Some(self))
    }
    fn reserve_events(
        &mut self,
        values: &mut Vec<crate::core::Event>,
        maximum: usize,
    ) -> Result<()> {
        self.reserve_history(values, maximum)
    }
    fn reserve_records(
        &mut self,
        values: &mut Vec<crate::store::operations::ObservedRecord>,
        maximum: usize,
    ) -> Result<()> {
        self.reserve_history(values, maximum)
    }
    fn reserve_messages(
        &mut self,
        values: &mut Vec<crate::conversation::ConversationMessage>,
        maximum: usize,
    ) -> Result<()> {
        self.reserve_history(values, maximum)
    }
    fn reserve_values(
        &mut self,
        values: &mut Vec<serde_json::Value>,
        maximum: usize,
    ) -> Result<()> {
        self.reserve_history(values, maximum)
    }
    fn reserve_bytes(&mut self, values: &mut Vec<u8>, capacity: usize) -> Result<()> {
        if capacity <= values.capacity() {
            return Ok(());
        }
        let old = values.capacity();
        self.retain_owned((capacity - old) as u64)?;
        self.allocation_attempt((capacity - old) as u64)?;
        values
            .try_reserve_exact(capacity - values.len())
            .map_err(Error::Allocation)?;
        self.retain_owned((values.capacity() - capacity) as u64)
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
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        self.allocation_attempt(bytes)
    }
    fn admit_copy(&self, bytes: u64) -> Result<()> {
        self.preflight(WorkCounters {
            bytes_copied: bytes,
            ..Default::default()
        })
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
        })
    }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.preflight(WorkCounters {
            bytes_encoded: bytes,
            ..Default::default()
        })
    }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        match charge {
            crate::contract::JsonCodecCharge::Admission {
                maximum_owned_bytes,
                value_shape,
            } => self.retain_owned(
                maximum_owned_bytes
                    .max(crate::contract::json_value_decode_owned_bytes(value_shape)?),
            ),
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
        })
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.charge(WorkCounters {
            authority_bytes_written: bytes,
            authority_records_appended: count,
            ..Default::default()
        })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.preflight(WorkCounters {
            output_bytes: bytes,
            items_returned: count,
            ..Default::default()
        })
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
        PreparationIo::remaining(self)
    }
    fn cancellation(&self) -> &CancellationToken {
        PreparationIo::cancellation(self)
    }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.spent(work)
    }
    fn verify_content<'a>(
        &'a mut self,
        verifier: &'a dyn ContentResidencyVerifier,
        file: &'a FileRef,
    ) -> Result<HistoryContentFuture<'a, ()>> {
        let io = *self;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                verifier
                    .verify(file, file.descriptor().byte_length(), Some(io))?
                    .await
            })
            .map(HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            verifier
                .verify(file, file.descriptor().byte_length(), Some(io))?
                .await
        })
    }
    fn read_content<'a>(
        &'a mut self,
        verifier: &'a dyn ContentResidencyVerifier,
        file: &'a FileRef,
    ) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        let io = *self;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                Ok((
                    verifier
                        .read(file, file.descriptor().byte_length(), Some(io))?
                        .await?,
                    0,
                ))
            })
            .map(HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let bytes = verifier
                .read(file, file.descriptor().byte_length(), Some(io))?
                .await?;
            let retained = bytes.retained_capacity().unwrap_or(0) as u64;
            io.retain_owned(retained)?;
            Ok((bytes, retained))
        })
    }
}
