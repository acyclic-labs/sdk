use super::*;
use crate::core::Authority;

/// Observes only a protected completion accepted under pre-action custody.
pub(crate) struct AcceptedCompletionIo {
    original: NativeMaterializationWork,
    reservation: NativeWorkReservation,
    retained: u64,
    original_pool_bytes: u64,
    staged_bytes: u64,
    staged_items: u64,
    custody: NativeAllocation,
}

impl AcceptedCompletionIo {
    pub(crate) fn new(
        original: &NativeMaterializationWork,
        reservation: NativeWorkReservation,
        custody: NativeAllocation,
    ) -> Result<Self> {
        if !reservation.work().shares_allocation_context(original)
            || !custody.work().shares_allocation_context(original)
        {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::InvalidLayout,
                custody: Some(custody),
            });
        }
        if original.cancellation().check().is_err() {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Cancelled,
                custody: Some(custody),
            });
        }
        Ok(Self {
            original: original.clone(),
            reservation,
            retained: 0,
            original_pool_bytes: custody.bytes(),
            staged_bytes: 0,
            staged_items: 0,
            custody,
        })
    }

    pub(crate) fn into_custody(self) -> NativeAllocation {
        self.custody
    }

    /// Seeds virtual ownership of a request already covered by the real pool.
    pub(crate) fn adopt_preheld(&mut self, bytes: u64) -> Result<()> {
        self.retain(bytes)
    }

    pub(crate) fn remaining_work(&self) -> WorkCounters {
        self.reservation.remaining_work()
    }

    /// Reads the original durable prefix; this is not a fresh work admission.
    pub(crate) fn completion_work(&self) -> Result<WorkCounters> {
        self.original.journal_work().map_err(Self::native_error)
    }

    pub(crate) fn record_completed(&mut self, actual: WorkCounters) -> Result<()> {
        self.reservation
            .record_completed(actual)
            .map_err(Self::native_error)
    }

    pub(super) fn native_error(cause: NativeTreeMaterializationError) -> Error {
        Error::NativeWork {
            cause,
            custody: None,
        }
    }

    pub(super) fn boundary(reason: &'static str) -> Error {
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        }
    }

    pub(super) fn unsupported() -> Error {
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "accepted completion does not support allocation, cloning, content, or decoding",
        }
    }

    pub(super) fn admit(&self, actual: WorkCounters) -> Result<()> {
        actual
            .verify(self.remaining_work())
            .map_err(|cause| Self::native_error(NativeTreeMaterializationError::Work(cause)))
    }

    pub(super) fn available_pool_bytes(&self) -> u64 {
        self.original_pool_bytes.min(self.custody.bytes()) - self.retained
    }

    pub(super) fn retain(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        self.retained += bytes;
        Ok(())
    }

    pub(super) fn release(&mut self, bytes: u64) -> Result<()> {
        self.retained = self.retained.checked_sub(bytes).ok_or_else(|| {
            Self::boundary("accepted completion releases more than its live virtual custody")
        })?;
        Ok(())
    }
}

impl HistoryIo for AcceptedCompletionIo {
    fn original_native_work(&self) -> Option<&NativeMaterializationWork> {
        Some(&self.original)
    }
    fn requires_prepared_native_dispatch(&self) -> bool {
        true
    }
    fn absorb_native_custody(&mut self, mut incoming: NativeAllocation) -> Result<()> {
        if !incoming.work().shares_allocation_context(&self.original) {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::InvalidLayout,
                custody: Some(incoming),
            });
        }
        let bytes = incoming.bytes();
        let maximum = self.available_pool_bytes();
        if bytes > maximum {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        observed: bytes,
                        maximum,
                    },
                ),
                custody: Some(incoming),
            });
        }
        let retained = self.retained + bytes;
        self.custody
            .absorb(&mut incoming)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: Some(incoming),
            })?;
        self.retained = retained;
        Ok(())
    }
    fn take_native_custody(&mut self, bytes: u64) -> Result<NativeAllocation> {
        let retained = self.retained.checked_sub(bytes).ok_or_else(|| {
            Self::boundary("accepted completion transfers more than its live virtual custody")
        })?;
        let guard = self.custody.split_off(bytes).map_err(Self::native_error)?;
        self.retained = retained;
        Ok(guard)
    }
    fn check_cancellation(&self) -> Result<()> {
        Ok(())
    }
    fn begin_call(&mut self) -> Result<()> {
        self.record_completed(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        })
    }
    fn begin_write(&mut self) -> Result<()> {
        self.record_completed(WorkCounters {
            backend_write_operations: 1,
            ..WorkCounters::default()
        })
    }
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error {
        Error::NativeWork {
            cause: NativeTreeMaterializationError::AllocationFailed(cause),
            custody: None,
        }
    }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.reservation
            .record_completed(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: bytes,
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(Self::native_error)?;
        self.retain(bytes)
    }
    fn receive_native_record(&mut self, bytes: u64) -> Result<()> {
        self.reservation
            .record_completed(WorkCounters {
                authority_records_read: 1,
                authority_bytes_read: bytes,
                ..WorkCounters::default()
            })
            .map_err(Self::native_error)
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> {
        self.retain(bytes)
    }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        if bytes <= self.available_pool_bytes() {
            Ok(())
        } else {
            Err(Self::boundary(
                "accepted completion exceeds its preheld allocation pool",
            ))
        }
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> {
        self.release(bytes)
    }
    fn clone_authority(&mut self, _: &Authority) -> Result<Authority> {
        Err(Self::unsupported())
    }
    fn clone_text(&mut self, _: &str) -> Result<String> {
        Err(Self::unsupported())
    }

    fn allocation_attempt(&mut self, _: u64) -> Result<()> {
        Err(Self::unsupported())
    }
    fn reserve_text(&mut self, value: &mut String, capacity: usize) -> Result<()> {
        if capacity <= value.capacity() {
            Ok(())
        } else {
            Err(Self::unsupported())
        }
    }
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        if bytes == 0 {
            Ok(())
        } else {
            Err(Self::unsupported())
        }
    }
    fn admit_copy(&self, bytes: u64) -> Result<()> {
        self.admit(WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::default()
        })
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> {
        self.record_completed(WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::default()
        })
    }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> {
        self.record_completed(WorkCounters {
            bytes_encoded: bytes,
            ..WorkCounters::default()
        })
    }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.admit(WorkCounters {
            bytes_encoded: bytes,
            ..WorkCounters::default()
        })
    }
    fn codec_charge(&mut self, charge: crate::executor::json::JsonCodecCharge) -> Result<()> {
        match charge {
            crate::executor::json::JsonCodecCharge::Work { bytes, items } => {
                self.record_completed(WorkCounters {
                    bytes_encoded: bytes,
                    items_examined: items,
                    ..WorkCounters::default()
                })
            }
            crate::executor::json::JsonCodecCharge::Copy { bytes } => self.copy_bytes(bytes),
            _ => Err(Self::unsupported()),
        }
    }
    fn decode_json_value(&mut self, _: &[u8]) -> Result<(serde_json::Value, u64)> {
        Err(Self::unsupported())
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> {
        self.record_completed(WorkCounters {
            bytes_hashed: bytes,
            ..WorkCounters::default()
        })
    }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        self.admit(WorkCounters {
            authority_bytes_written: bytes,
            authority_records_appended: count,
            ..WorkCounters::default()
        })
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.record_completed(WorkCounters {
            authority_bytes_written: bytes,
            authority_records_appended: count,
            ..WorkCounters::default()
        })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.admit(WorkCounters {
            output_bytes: bytes,
            items_returned: count,
            ..WorkCounters::default()
        })
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        let staged_bytes = self.staged_bytes.checked_add(bytes).ok_or_else(|| {
            Self::native_error(NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::Overflow,
            ))
        })?;
        let staged_items = self.staged_items.checked_add(count).ok_or_else(|| {
            Self::native_error(NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::Overflow,
            ))
        })?;
        self.admit_output(staged_bytes, staged_items)?;
        self.staged_bytes = staged_bytes;
        self.staged_items = staged_items;
        Ok(())
    }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.record_completed(WorkCounters {
            output_bytes: bytes,
            items_returned: count,
            ..WorkCounters::default()
        })
    }
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget> {
        let mut remaining = self.remaining_work();
        remaining.peak_allocation_bytes = self.available_pool_bytes();
        Ok(remaining)
    }
    fn cancellation(&self) -> &acyclic_fs::CancellationToken {
        self.original.cancellation()
    }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.record_completed(work)
    }
    fn verify_content<'a>(
        &'a mut self,
        _: &'a dyn crate::conversation::ContentResidencyVerifier,
        _: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(Self::unsupported())
    }
    fn read_content<'a>(
        &'a mut self,
        _: &'a dyn crate::conversation::ContentResidencyVerifier,
        _: &'a crate::conversation::FileRef,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        Err(Self::unsupported())
    }
}
