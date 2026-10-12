//! SDK boundaries on the original native ledger, without another budget/meter.

use super::{HistoryCursor, HistoryIo, HistoryPage, HistoryReadLimits, HistoryReader};
use acyclic_stream::StreamProvider;
use crate::{Error, Result, core::{Authority, Event}};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, NativeWorkReservation, WorkCounters};
use std::mem::size_of;

/// Observes aggregate work on the supplied native ledger.
pub struct NativeHistoryIo<'a> {
    work: &'a NativeMaterializationWork,
    custody: NativeAllocation,
    staged_bytes: u64,
    staged_items: u64,
}

fn error(cause: std::collections::TryReserveError) -> Error {
    Error::NativeWork {
        cause: NativeTreeMaterializationError::AllocationFailed(cause),
        custody: None,
    }
}

/// Starts an observer on the supplied native ledger.
pub fn native_work_io(
    work: &NativeMaterializationWork,
) -> std::result::Result<NativeHistoryIo<'_>, NativeTreeMaterializationError> {
    Ok(NativeHistoryIo {
        work, custody: work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?, staged_bytes: 0, staged_items: 0,
    })
}

/// Moves an actual SDK output claim into this observer without another claim or a cancellation check.
pub(crate) fn adopt_native_sdk_output<T>(
    original: &NativeMaterializationWork,
    output: acyclic_stream::NativeStreamOutput<T>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(T, u64)> {
    let (value, incoming) = output.into_parts();
    let Some(observer) = work.as_deref_mut().filter(|observer|
        observer.original_native_work().is_some_and(|work|
            work.shares_allocation_context(original))) else {
        drop(value);
        drop(incoming);
        return Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::InvalidLayout, custody: None,
        });
    };
    let incoming = match NativeAllocation::from_runtime_allocation(original, incoming) {
        Ok(incoming) => incoming,
        Err((cause, incoming)) => {
            drop(value);
            drop(incoming);
            return Err(Error::NativeWork { cause, custody: None });
        },
    };
    let retained = incoming.bytes();
    match observer.absorb_native_custody(incoming) {
        Ok(()) => Ok((value, retained)),
        Err(error) => {
            drop(value);
            Err(error)
        },
    }
}

/// Preserves the native SDK's owned semantic failure and original allocation guard without formatting it or losing its cause during policy inspection.
pub(crate) fn native_sdk_result<T>(
    original: &NativeMaterializationWork,
    result: acyclic_stream::NativeStreamResult<T>,
) -> Result<acyclic_stream::NativeStreamOutput<T>> {
    match result {
        Err(error) => Err(Error::NativeWork {
            cause: original.runtime_error(error), custody: None,
        }),
        Ok(Ok(output)) => Ok(output),
        Ok(Err(failure)) => {
            let source = failure;
            let code = if source == acyclic_stream::StreamError::IdempotencyMismatch {
                crate::wire::ErrorCode::Conflict
            } else {
                crate::wire::ErrorCode::Storage
            };
            Err(Error::NativeStream { code, source, custody: None })
        },
    }
}

impl NativeHistoryIo<'_> {
    /// Transfers the same ledger's retained source payloads to their last user.
    pub fn into_custody(self) -> NativeAllocation {
        self.custody
    }
    /// Returns the staged output under its original allowance.
    pub fn return_staged_output(&mut self) -> Result<()> {
        self.return_output(self.staged_bytes, self.staged_items)?;
        self.staged_bytes = 0;
        self.staged_items = 0;
        Ok(())
    }
    fn native_error(&self, cause: NativeTreeMaterializationError) -> Error {
        Error::NativeWork { cause, custody: None }
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
        if target <= old {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "vector exceeds declared bound",
            });
        }
        let extra = target.checked_sub(old).and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| self.native_error(NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))? as u64;
        self.retain(extra)?;
        if let Err(cause) = self.charge(WorkCounters {
            allocation_operations: u64::from(extra != 0), ..WorkCounters::default()
        }) {
            self.release_owned(extra)?;
            return Err(cause);
        }
        if let Err(cause) = values.try_reserve_exact(target - values.len()) {
            self.release_owned(extra)?;
            return Err(error(cause));
        }
        let actual = values.capacity().checked_sub(old).and_then(|count| count.checked_mul(size_of::<T>()))
            .ok_or_else(|| self.native_error(NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))? as u64;
        if actual > extra { self.retain(actual - extra) }
        else { self.release_owned(extra - actual) }
    }
}

impl HistoryIo for NativeHistoryIo<'_> {
    fn original_native_work(&self) -> Option<&NativeMaterializationWork> { Some(self.work) }
    fn absorb_native_custody(&mut self, mut incoming: NativeAllocation) -> Result<()> {
        if !self.work.shares_ledger(incoming.work()) {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::InvalidLayout, custody: Some(incoming),
            });
        }
        self.custody.absorb(&mut incoming).map_err(|cause| Error::NativeWork {
            cause, custody: Some(incoming),
        })
    }
    fn take_native_custody(&mut self, bytes: u64) -> Result<NativeAllocation> {
        self.custody.split_off(bytes).map_err(|cause| self.native_error(cause))
    }
    fn check_cancellation(&self) -> Result<()> {
        self.work.cancellation().check()
            .map_err(|_| self.native_error(NativeTreeMaterializationError::Cancelled))
    }
    fn begin_call(&mut self) -> Result<()> { self.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() }) }
    fn begin_write(&mut self) -> Result<()> { self.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() }) }
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error { error(cause) }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.work.spent(WorkCounters { authority_records_read: 1, authority_bytes_read: bytes,
            items_examined: 1, peak_allocation_bytes: bytes, ..WorkCounters::default() })
            .map_err(|error| self.native_error(error))?;
        self.work.remaining().map_err(|error| self.native_error(error))?;
        self.retain(bytes)?;
        Ok(())
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> {
        self.custody.release(bytes).map_err(|error| self.native_error(error))
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.retain(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        self.work.admit_retained(bytes).map_err(|error| self.native_error(error))
    }
    fn clone_authority(&mut self, authority: &Authority) -> Result<Authority> {
        Ok(Authority { kind: authority.kind, id: self.clone_text(&authority.id)? })
    }
    fn clone_text(&mut self, text: &str) -> Result<String> {
        let (copy, custody) =
            crate::conversation::clone_native_reference_text(text, self.work)?;
        self.absorb_native_custody(custody)?;
        Ok(copy)
    }
    fn reserve_events(&mut self, values: &mut Vec<Event>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_records(&mut self, values: &mut Vec<super::super::operations::ObservedRecord>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_messages(&mut self, values: &mut Vec<crate::conversation::ConversationMessage>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_bytes(&mut self, values: &mut Vec<u8>, capacity: usize) -> Result<()> {
        if capacity <= values.capacity() { return Ok(()); }
        let old = values.capacity();
        let extra = (capacity - old) as u64;
        self.retain(extra)?;
        if let Err(cause) = self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() }) {
            self.release_owned(extra)?;
            return Err(cause);
        }
        if let Err(cause) = values.try_reserve_exact(capacity - values.len()) {
            self.release_owned(extra)?;
            return Err(error(cause));
        }
        let actual = (values.capacity() - old) as u64;
        if actual > extra { self.retain(actual - extra) }
        else { self.release_owned(extra - actual) }
    }
    fn reserve_text(&mut self, text: &mut String, capacity: usize) -> Result<()> {
        if capacity <= text.capacity() { return Ok(()); }
        let old = text.capacity();
        let extra = (capacity - old) as u64;
        self.retain(extra)?;
        if let Err(cause) = self.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() }) {
            self.release_owned(extra)?;
            return Err(cause);
        }
        if let Err(cause) = text.try_reserve_exact(capacity - text.len()) {
            self.release_owned(extra)?;
            return Err(error(cause));
        }
        let actual = (text.capacity() - old) as u64;
        if actual > extra { self.retain(actual - extra) }
        else { self.release_owned(extra - actual) }
    }
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        self.charge(WorkCounters { allocation_operations: u64::from(bytes != 0), ..WorkCounters::default() })
    }
    fn admit_copy(&self, bytes: u64) -> Result<()> {
        self.work.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })
            .map_err(|error| self.native_error(error))
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
            JsonCodecCharge::Admission { maximum_owned_bytes, .. } => { self.admit_owned(maximum_owned_bytes)?; self.retain(maximum_owned_bytes) }
            JsonCodecCharge::Copy { bytes } => self.copy_bytes(bytes),
            JsonCodecCharge::CopyAdmission { bytes } => self.admit_copy(bytes),
            JsonCodecCharge::AllocationOperation { .. } => self.charge(WorkCounters {
                allocation_operations: 1, ..WorkCounters::default()
            }),
            JsonCodecCharge::Read { .. } => Ok(()),
            JsonCodecCharge::Inspect { items } => self.charge(WorkCounters {
                items_examined: items, ..WorkCounters::default()
            }),
            JsonCodecCharge::Release { bytes } => self.release_owned(bytes),
        }
    }
    fn decode_json_value(&mut self, bytes: &[u8]) -> Result<(serde_json::Value, u64)> {
        let io = crate::filesystem::PreparationIo::from_native_work(self.work)?;
        let decoded = io.decode(std::marker::PhantomData::<serde_json::Value>, bytes, crate::wire::ErrorCode::Storage)?;
        let retained = io.owned_bytes()?;
        let value = crate::filesystem::retain_decoded(decoded);
        let incoming = io.into_native_custody(retained)?;
        let held = incoming.bytes();
        self.absorb_native_custody(incoming)?;
        Ok((value, held))
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
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        let overflow = || self.native_error(
            NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow));
        let staged_bytes = self.staged_bytes.checked_add(bytes).ok_or_else(overflow)?;
        let staged_items = self.staged_items.checked_add(count).ok_or_else(overflow)?;
        self.admit_output(staged_bytes, staged_items)?;
        self.staged_bytes = staged_bytes;
        self.staged_items = staged_items;
        Ok(())
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
        reference: &'a crate::conversation::FileRef) -> Result<super::HistoryContentFuture<'a, ()>> {
        let work = self.work;
        let future = crate::NativeFutureBundle::new(work, || async move {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            if !verifier.supports_read(reference.volume(), Some(&io))? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "content verifier does not expose the original native work boundary",
                });
            }
            verifier.verify(
                reference, reference.descriptor().byte_length(), Some(&io),
            )?.await
        })?;
        Ok(super::HistoryContentFuture::Native(future))
    }
    fn read_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef) -> Result<super::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        let work = self.work;
        let future = crate::NativeFutureBundle::new(work, || async move {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            if !verifier.supports_read(reference.volume(), Some(&io))? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "content verifier does not expose the original native work boundary",
                });
            }
            let bytes = verifier.read(
                reference, reference.descriptor().byte_length(), Some(&io),
            )?.await?;
            Ok((bytes, 0))
        })?;
        Ok(super::HistoryContentFuture::Native(future))
    }
}

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
        if !reservation.matches_work(original) || !custody.matches_work(original) {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::InvalidLayout,
                custody: Some(custody),
            });
        }
        if original.cancellation().check().is_err() {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Cancelled, custody: Some(custody),
            });
        }
        Ok(Self {
            original: original.clone(), reservation, retained: 0, original_pool_bytes: custody.bytes(),
            staged_bytes: 0, staged_items: 0, custody,
        })
    }

    pub(crate) fn into_custody(self) -> NativeAllocation { self.custody }

    /// Seeds virtual ownership of a request already covered by the real pool.
    pub(crate) fn adopt_preheld(&mut self, bytes: u64) -> Result<()> { self.retain(bytes) }

    pub(crate) fn remaining_work(&self) -> WorkCounters { self.reservation.remaining_work() }

    /// Reads the original durable prefix; this is not a fresh work admission.
    pub(crate) fn completion_work(&self) -> Result<WorkCounters> {
        self.original.journal_work().map_err(Self::native_error)
    }

    pub(crate) fn record_completed(&mut self, actual: WorkCounters) -> Result<()> {
        self.reservation.record_completed(actual).map_err(Self::native_error)
    }

    fn native_error(cause: NativeTreeMaterializationError) -> Error {
        Error::NativeWork { cause, custody: None }
    }

    fn boundary(reason: &'static str) -> Error {
        Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason }
    }

    fn unsupported() -> Error {
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "accepted completion does not support allocation, cloning, content, or decoding",
        }
    }

    fn admit(&self, actual: WorkCounters) -> Result<()> {
        actual.verify(self.remaining_work())
            .map_err(|cause| Self::native_error(NativeTreeMaterializationError::Work(cause)))
    }

    fn available_pool_bytes(&self) -> u64 {
        self.original_pool_bytes.min(self.custody.bytes()) - self.retained
    }

    fn retain(&mut self, bytes: u64) -> Result<()> {
        self.admit_owned(bytes)?;
        self.retained += bytes;
        Ok(())
    }

    fn release(&mut self, bytes: u64) -> Result<()> {
        self.retained = self.retained.checked_sub(bytes)
            .ok_or_else(|| Self::boundary("accepted completion releases more than its live virtual custody"))?;
        Ok(())
    }

    fn reserved_slot<T>(&self, values: &[T], capacity: usize) -> Result<()> {
        if values.len() < capacity { Ok(()) } else { Err(Self::unsupported()) }
    }
}

impl HistoryIo for AcceptedCompletionIo {
    fn original_native_work(&self) -> Option<&NativeMaterializationWork> { Some(&self.original) }
    fn requires_prepared_native_dispatch(&self) -> bool { true }
    fn absorb_native_custody(&mut self, mut incoming: NativeAllocation) -> Result<()> {
        if !incoming.matches_work(&self.original) {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::InvalidLayout, custody: Some(incoming),
            });
        }
        let bytes = incoming.bytes();
        let maximum = self.available_pool_bytes();
        if bytes > maximum {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes", observed: bytes, maximum,
                }),
                custody: Some(incoming),
            });
        }
        let retained = self.retained + bytes;
        self.custody.absorb(&mut incoming).map_err(|cause| Error::NativeWork {
            cause, custody: Some(incoming),
        })?;
        self.retained = retained;
        Ok(())
    }
    fn take_native_custody(&mut self, bytes: u64) -> Result<NativeAllocation> {
        let retained = self.retained.checked_sub(bytes)
            .ok_or_else(|| Self::boundary("accepted completion transfers more than its live virtual custody"))?;
        let guard = self.custody.split_off(bytes).map_err(Self::native_error)?;
        self.retained = retained;
        Ok(guard)
    }
    fn check_cancellation(&self) -> Result<()> { Ok(()) }
    fn begin_call(&mut self) -> Result<()> {
        self.record_completed(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })
    }
    fn begin_write(&mut self) -> Result<()> {
        self.record_completed(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })
    }
    fn allocation_failure(&self, cause: std::collections::TryReserveError) -> Error { error(cause) }
    fn receive_record(&mut self, bytes: u64) -> Result<()> {
        self.reservation.record_completed(WorkCounters {
            authority_records_read: 1, authority_bytes_read: bytes,
            items_examined: 1, ..WorkCounters::default()
        }).map_err(Self::native_error)?;
        self.retain(bytes)
    }
    fn receive_native_record(&mut self, bytes: u64) -> Result<()> {
        self.reservation.record_completed(WorkCounters {
            authority_records_read: 1, authority_bytes_read: bytes,
            ..WorkCounters::default()
        }).map_err(Self::native_error)
    }
    fn retain_owned(&mut self, bytes: u64) -> Result<()> { self.retain(bytes) }
    fn admit_owned(&self, bytes: u64) -> Result<()> {
        if bytes <= self.available_pool_bytes() { Ok(()) }
        else { Err(Self::boundary("accepted completion exceeds its preheld allocation pool")) }
    }
    fn release_owned(&mut self, bytes: u64) -> Result<()> { self.release(bytes) }
    fn clone_authority(&mut self, _: &Authority) -> Result<Authority> { Err(Self::unsupported()) }
    fn clone_text(&mut self, _: &str) -> Result<String> { Err(Self::unsupported()) }
    fn reserve_events(&mut self, values: &mut Vec<Event>, _: usize) -> Result<()> {
        self.reserved_slot(values, values.capacity())
    }
    fn reserve_records(&mut self, values: &mut Vec<super::super::operations::ObservedRecord>, _: usize) -> Result<()> {
        self.reserved_slot(values, values.capacity())
    }
    fn reserve_messages(&mut self, values: &mut Vec<crate::conversation::ConversationMessage>, _: usize) -> Result<()> {
        self.reserved_slot(values, values.capacity())
    }
    fn reserve_values(&mut self, values: &mut Vec<serde_json::Value>, _: usize) -> Result<()> {
        self.reserved_slot(values, values.capacity())
    }
    fn reserve_bytes(&mut self, values: &mut Vec<u8>, capacity: usize) -> Result<()> {
        if capacity <= values.capacity() { Ok(()) } else { Err(Self::unsupported()) }
    }
    fn reserve_text(&mut self, value: &mut String, capacity: usize) -> Result<()> {
        if capacity <= value.capacity() { Ok(()) } else { Err(Self::unsupported()) }
    }
    fn admit_allocation(&mut self, bytes: u64) -> Result<()> {
        if bytes == 0 { Ok(()) } else { Err(Self::unsupported()) }
    }
    fn admit_copy(&self, bytes: u64) -> Result<()> {
        self.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })
    }
    fn copy_bytes(&mut self, bytes: u64) -> Result<()> {
        self.record_completed(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })
    }
    fn encoded_bytes(&mut self, bytes: u64) -> Result<()> {
        self.record_completed(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() })
    }
    fn admit_encoding(&self, bytes: u64) -> Result<()> {
        self.admit(WorkCounters { bytes_encoded: bytes, ..WorkCounters::default() })
    }
    fn codec_charge(&mut self, charge: crate::contract::JsonCodecCharge) -> Result<()> {
        match charge {
            crate::contract::JsonCodecCharge::Work { bytes, items } => self.record_completed(WorkCounters {
                bytes_encoded: bytes, items_examined: items, ..WorkCounters::default()
            }),
            crate::contract::JsonCodecCharge::Copy { bytes } => self.copy_bytes(bytes),
            _ => Err(Self::unsupported()),
        }
    }
    fn decode_json_value(&mut self, _: &[u8]) -> Result<(serde_json::Value, u64)> {
        Err(Self::unsupported())
    }
    fn hashed_bytes(&mut self, bytes: u64) -> Result<()> {
        self.record_completed(WorkCounters { bytes_hashed: bytes, ..WorkCounters::default() })
    }
    fn admit_acknowledgment(&self, bytes: u64, count: u64) -> Result<()> {
        self.admit(WorkCounters {
            authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default()
        })
    }
    fn acknowledged(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.record_completed(WorkCounters {
            authority_bytes_written: bytes, authority_records_appended: count, ..WorkCounters::default()
        })
    }
    fn admit_output(&self, bytes: u64, count: u64) -> Result<()> {
        self.admit(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })
    }
    fn stage_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        let staged_bytes = self.staged_bytes.checked_add(bytes)
            .ok_or_else(|| Self::native_error(NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
        let staged_items = self.staged_items.checked_add(count)
            .ok_or_else(|| Self::native_error(NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
        self.admit_output(staged_bytes, staged_items)?;
        self.staged_bytes = staged_bytes;
        self.staged_items = staged_items;
        Ok(())
    }
    fn return_output(&mut self, bytes: u64, count: u64) -> Result<()> {
        self.record_completed(WorkCounters { output_bytes: bytes, items_returned: count, ..WorkCounters::default() })
    }
    fn remaining(&self) -> Result<acyclic_fs::WorkBudget> {
        let mut remaining = self.remaining_work();
        remaining.peak_allocation_bytes = self.available_pool_bytes();
        Ok(remaining)
    }
    fn cancellation(&self) -> &acyclic_fs::CancellationToken { self.original.cancellation() }
    fn receive_work(&mut self, mut work: WorkCounters) -> Result<()> {
        work.output_bytes = 0;
        work.items_returned = 0;
        self.record_completed(work)
    }
    fn verify_content<'a>(
        &'a mut self, _: &'a dyn crate::conversation::ContentResidencyVerifier,
        _: &'a crate::conversation::FileRef,
    ) -> Result<super::HistoryContentFuture<'a, ()>> { Err(Self::unsupported()) }
    fn read_content<'a>(
        &'a mut self, _: &'a dyn crate::conversation::ContentResidencyVerifier,
        _: &'a crate::conversation::FileRef,
    ) -> Result<super::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> { Err(Self::unsupported()) }
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
    fn native_string_copy_and_vector_growth_denials_preserve_actual_prefix() {
        let cancellation = CancellationToken::new();
        let mut budget = control_budget();
        budget.peak_allocation_bytes += 128;
        budget.bytes_copied = 32;
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original accounting state");
        let mut io = native_work_io(&work).expect("original observer");
        let before = work.receipt();
        assert!(matches!(io.clone_text("original payload"), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "allocation_operations", ..
            }), custody: None,
        })));
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(io.custody.bytes(), 0);

        let mut bytes = Vec::new();
        assert!(matches!(io.reserve_bytes(&mut bytes, 16), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "allocation_operations", ..
            }), custody: None,
        })));
        assert_eq!(bytes.capacity(), 0);
        assert_eq!(io.custody.bytes(), 0);
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
    }

    #[test]
    fn native_observer_borrows_exact_ledger_and_preserves_rejected_custody() {
        let cancellation = CancellationToken::new();
        let mut budget = control_budget();
        budget.peak_allocation_bytes += 16;
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original ledger");
        let foreign = NativeMaterializationWork::new(budget, &cancellation)
            .expect("distinct original ledger");
        let mut io = native_work_io(&work).expect("original observer");
        assert!(std::ptr::eq(io.original_native_work().expect("native ledger borrow"), &work));
        let incoming = foreign.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("foreign held allowance");
        let prefix = work.receipt();
        let rejection = io.absorb_native_custody(incoming).expect_err("distinct ledger rejects");
        let Error::NativeWork {
            cause: NativeTreeMaterializationError::InvalidLayout, custody: Some(guard),
        } = rejection else { panic!("original typed cause and guard must survive"); };
        assert_eq!(guard.bytes(), 16);
        assert!(guard.work().shares_ledger(&foreign));
        assert_eq!(io.custody.bytes(), 0);
        assert_eq!(work.receipt(), prefix);
        drop(guard);
        let incoming = work.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("same-ledger held allowance");
        let prefix = work.receipt();
        io.absorb_native_custody(incoming).expect("same-ledger transfer");
        assert_eq!(io.custody.bytes(), 16);
        assert_eq!(work.receipt(), prefix, "adoption makes no new claim or call");
    }

    struct RuntimeOutputDropWitness {
        bytes: Vec<u8>,
        original: NativeMaterializationWork,
        remaining_while_owned: u64,
        dropped_under_custody: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for RuntimeOutputDropWitness {
        fn drop(&mut self) {
            use std::sync::atomic::Ordering;
            self.dropped_under_custody.store(
                self.original.remaining().expect("original uncancelled ledger")
                    .peak_allocation_bytes == self.remaining_while_owned
                    && !self.bytes.is_empty(),
                Ordering::SeqCst,
            );
        }
    }

    fn runtime_output_drop_witness(
        original: &NativeMaterializationWork,
        dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> acyclic_stream::NativeStreamOutput<RuntimeOutputDropWitness> {
        let meter = original.runtime_meter_owner();
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(&meter, 8)
            .expect("pre-admitted concrete byte allocation");
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(8).expect("real test byte buffer");
        allocation.reconcile_capacity(bytes.capacity() as u64)
            .expect("actual byte capacity");
        meter.admit_codec(0, 8, 0).expect("actual source copy admission");
        bytes.extend_from_slice(b"original");
        meter.record_codec(0, 8, 0).expect("actual completed byte copy");
        let remaining_while_owned = original.remaining().expect("original ledger").peak_allocation_bytes;
        acyclic_stream::NativeStreamOutput::new(RuntimeOutputDropWitness {
            bytes, original: original.clone(), remaining_while_owned,
            dropped_under_custody: dropped,
        }, allocation)
    }

    #[test]
    fn sdk_output_adoption_preserves_exact_claim_and_failure_drop_order() {
        use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
        let cancellation = CancellationToken::new();
        let mut budget = control_budget();
        budget.allocation_operations += 1;
        budget.peak_allocation_bytes += 128;
        budget.bytes_copied = 8;
        let original = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let foreign = NativeMaterializationWork::new(budget, &cancellation).expect("independent query ledger");
        let mut io = native_work_io(&original).expect("original observer");
        let dropped = Arc::new(AtomicBool::new(false));
        let output = runtime_output_drop_witness(&original, dropped.clone());
        let prefix = original.receipt();
        let (value, retained) = adopt_native_sdk_output(&original, output, &mut Some(&mut io))
            .expect("move genuine original SDK claim");
        assert_eq!(retained, value.bytes.capacity() as u64);
        assert_eq!(original.receipt(), prefix, "transfer neither rebills nor releases/reclaims");
        drop(value);
        assert!(dropped.load(Ordering::SeqCst));
        io.release_owned(retained).expect("release only after physical output drop");

        let dropped = Arc::new(AtomicBool::new(false));
        let before = foreign.remaining().expect("foreign ledger").peak_allocation_bytes;
        let output = runtime_output_drop_witness(&foreign, dropped.clone());
        assert!(matches!(adopt_native_sdk_output(&original, output, &mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::InvalidLayout, custody: None })));
        assert!(dropped.load(Ordering::SeqCst), "rejected DTO drops before its foreign SDK guard");
        assert_eq!(foreign.remaining().expect("released foreign output").peak_allocation_bytes, before);
        assert_eq!(io.custody.bytes(), 0);
    }

    #[test]
    fn guarded_native_value_decode_keeps_original_source_counters_and_custody() {
        let cancellation = CancellationToken::new();
        let mut budget = control_budget();
        budget.peak_allocation_bytes += 1024 * 1024;
        budget.source_bytes_read = 1024;
        budget.items_examined = 128;
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original meter control allocation");
        let mut io = native_work_io(&work).expect("original decoder custody");
        let source = br#"{"scope":"historical","limit":8}"#;
        let (value, held) = io.decode_json_value(source).expect("audited original Value decoder");
        assert_eq!(value["scope"].as_str(), Some("historical"));
        assert_eq!(value["limit"].as_u64(), Some(8));
        assert!(io.custody.bytes() > 0, "decoded allowance stays with original history custody");
        assert_eq!(held, io.custody.bytes(), "returned metadata is the original transferred allowance");
        assert_eq!(work.receipt().source_bytes_read, source.len() as u64);
        assert_eq!(work.receipt().authority_bytes_read, 0);
        assert_eq!(work.receipt().bytes_encoded, 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        drop(value);
        drop(io);
    }

    #[test]
    fn guarded_native_value_decode_denies_before_opaque_serde_allocation() {
        let cancellation = CancellationToken::new();
        let mut budget = control_budget();
        budget.source_bytes_read = 1024;
        budget.items_examined = 128;
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original meter control allocation");
        let mut io = native_work_io(&work).expect("original decoder custody");
        let prefix = work.receipt();
        assert!(matches!(io.decode_json_value(br#"{"scope":"historical","limit":8}"#),
            Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes", ..
                }), custody: None,
            })));
        assert_eq!(io.custody.bytes(), 0);
        assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
        assert_eq!(work.receipt().bytes_encoded, 0);
        assert!(work.receipt().source_bytes_read > 0);
        cancellation.cancel();
        let cancelled = work.receipt();
        assert!(matches!(io.decode_json_value(b"null"), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled, custody: None,
        })));
        assert_eq!(work.receipt(), cancelled);
    }

    #[test]
    fn allocator_failure_preserves_original_inline_cause_without_diagnostic_heap() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(control_budget(), &cancellation)
            .expect("original meter control allocation");
        let io = native_work_io(&work).expect("zero-byte original custody");
        let prefix = work.receipt();
        let mut bytes = Vec::<u8>::new();
        let cause = bytes.try_reserve_exact(usize::MAX)
            .expect_err("capacity above isize::MAX is rejected without OS allocation");
        assert!(matches!(io.allocation_failure(cause), Error::NativeWork {
            cause: NativeTreeMaterializationError::AllocationFailed(_), custody: None,
        }));
        assert_eq!(work.receipt(), prefix);
        assert_eq!(bytes.capacity(), 0);
    }

    #[test]
    fn native_guard_denial_preserves_original_typed_cause_without_error_allocation() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(control_budget(), &cancellation)
            .expect("original meter control allocation");
        let prefix = work.receipt();
        let mut io = native_work_io(&work).expect("zero-byte original custody transfer");
        assert!(matches!(io.begin_call(), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations", ..
            }), custody: None,
        })));
        assert_eq!(work.receipt(), prefix);
        assert!(matches!(io.admit_owned(1), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }), custody: None,
        })));
        assert_eq!(work.receipt(), prefix);
    }

    #[test]
    fn original_codec_reserve_is_denied_without_counting_retention_as_allocation() {
        let cancellation = CancellationToken::new();
        let mut budget = control_budget();
        budget.peak_allocation_bytes += 4096;
        budget.bytes_copied = 128;
        budget.bytes_encoded = 128;
        budget.items_examined = 8;
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original meter control allocation");
        let mut io = native_work_io(&work).expect("original codec custody");
        let prefix = work.receipt();
        let encoded = crate::contract::canonical_json_bytes_bounded(
            &"borrowed original payload",
            crate::contract::JsonBounds {
                maximum_bytes: 128, maximum_items: 8, maximum_depth: 4,
            },
            None, &mut |charge| io.codec_charge(charge),
        );
        assert!(matches!(encoded, Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "allocation_operations", ..
            }), custody: None,
        })));
        assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
        assert_eq!(work.receipt().bytes_copied, prefix.bytes_copied);
        assert_eq!(work.receipt().backend_read_operations, prefix.backend_read_operations);
        assert_eq!(work.receipt().backend_write_operations, prefix.backend_write_operations);
    }

    #[test]
    fn original_codec_copy_admission_rejects_before_reserve_or_copy() {
        let cancellation = CancellationToken::new();
        let mut budget = control_budget();
        budget.allocation_operations += 64;
        budget.peak_allocation_bytes += 4096;
        budget.bytes_encoded = 128;
        budget.items_examined = 8;
        let work = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original meter control allocation");
        let mut io = native_work_io(&work).expect("original codec custody");
        let prefix = work.receipt();
        let encoded = crate::contract::canonical_json_bytes_bounded(
            &"borrowed original payload",
            crate::contract::JsonBounds {
                maximum_bytes: 128, maximum_items: 8, maximum_depth: 4,
            },
            None, &mut |charge| io.codec_charge(charge),
        );
        assert!(matches!(encoded, Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied", ..
            }), custody: None,
        })));
        assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
        assert_eq!(work.receipt().bytes_copied, prefix.bytes_copied);
    }

    #[test]
    fn original_native_cancellation_keeps_typed_cause_and_cleanup_live() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(control_budget(), &cancellation)
            .expect("original meter control allocation");
        let mut io = native_work_io(&work).expect("zero-byte original custody transfer");
        let prefix = work.receipt();
        cancellation.cancel();
        assert!(matches!(io.check_cancellation(), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled, custody: None,
        })));
        io.release_owned(0).expect("custody cleanup does not consult cancellation");
        assert_eq!(work.receipt(), prefix);
        assert!(matches!(native_work_io(&work), Err(NativeTreeMaterializationError::Cancelled)));
    }

    fn completion_budget(maximum: WorkCounters, pool: u64) -> WorkBudget {
        let mut budget = control_budget().checked_add(maximum).expect("finite test grant");
        budget.peak_allocation_bytes += pool;
        budget
    }

    #[test]
    fn accepted_completion_spends_owned_credit_after_cancellation_without_aliasing() {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters {
            backend_read_operations: 1, backend_write_operations: 1,
            authority_records_read: 1, authority_bytes_read: 3,
            authority_records_appended: 1, authority_bytes_written: 3,
            bytes_copied: 3, bytes_encoded: 3, bytes_hashed: 3,
            items_examined: 2, items_returned: 1, output_bytes: 3,
            source_bytes_read: 7, source_path_components: 2, source_entries_visited: 1,
            materializations: 1, ..WorkCounters::default()
        };
        let work = NativeMaterializationWork::new(completion_budget(maximum, 16), &cancellation)
            .expect("original ledger");
        let mut ordinary = native_work_io(&work).expect("ordinary observer before acceptance");
        let reservation = work.reserve_work(maximum).expect("pre-action additive credit");
        let pool = work.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("pre-action allocation pool");
        let durable = WorkCounters { materializations: 4, ..WorkCounters::default() };
        work.set_original_work(durable);
        let prefix = work.receipt();
        let mut io = AcceptedCompletionIo::new(&work, reservation, pool).expect("accepted observer");
        assert!(io.original_native_work().expect("exact original identity").shares_allocation_context(&work));
        cancellation.cancel();
        assert!(matches!(ordinary.check_cancellation(), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled, custody: None,
        })), "ordinary native work keeps its cancellation gate");
        io.check_cancellation().expect("only accepted completion has no late cancellation gate");
        assert_eq!(io.completion_work().expect("pure prefix after cancellation"),
            durable.checked_add(prefix).expect("durable and current prefix"));
        io.begin_call().expect("actual recovery call");
        io.begin_write().expect("actual SDK commit call");
        io.receive_record(3).expect("actual returned raw record");
        io.admit_acknowledgment(3, 1).expect("owned acknowledgment credit");
        io.acknowledged(3, 1).expect("confirmed SDK acknowledgment");
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 3, items: 1 })
            .expect("prepaid writer work");
        io.codec_charge(crate::contract::JsonCodecCharge::Copy { bytes: 3 })
            .expect("prepaid writer actual copy");
        io.hashed_bytes(3).expect("actual canonical SDK digest input");
        io.receive_work(WorkCounters {
            source_bytes_read: 7, source_path_components: 2, source_entries_visited: 1,
            materializations: 1, output_bytes: 99, items_returned: 99,
            ..WorkCounters::default()
        }).expect("nested actual source work excludes unreturned output");
        io.stage_output(3, 1).expect("owned final output credit");
        io.return_output(3, 1).expect("actual returned output");
        assert_eq!(io.remaining_work(), WorkCounters::default());
        assert_eq!(work.receipt(), prefix.checked_add(maximum).expect("actual additive prefix"));
        assert_eq!(io.completion_work().expect("completed pure original prefix"),
            durable.checked_add(work.receipt()).expect("durable and actual completed prefix"));
        io.release_owned(3).expect("virtual cleanup remains cancellation independent");
        let pool = io.into_custody();
        assert_eq!(pool.bytes(), 16, "virtual cleanup must not release the real pool");
        assert!(pool.matches_work(&work));
        drop(ordinary);
        drop(pool);
    }

    #[test]
    fn accepted_completion_token_cannot_spend_fresh_original_allowance() {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters { bytes_copied: 4, ..WorkCounters::default() };
        let work = NativeMaterializationWork::new(completion_budget(maximum, 16), &cancellation)
            .expect("original ledger");
        let reservation = work.reserve_work(maximum).expect("owned copy credit");
        let pool = work.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("preheld pool");
        assert!(matches!(work.charge(WorkCounters {
            bytes_copied: 1, ..WorkCounters::default()
        }), Err(NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied", ..
        }))), "reserved work is not available to ordinary callers");
        let mut io = AcceptedCompletionIo::new(&work, reservation, pool).expect("accepted observer");
        io.copy_bytes(3).expect("actual copy prefix");
        let prefix = work.receipt();
        assert!(matches!(io.admit_copy(2), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied", ..
            }), custody: None,
        })));
        assert!(matches!(io.copy_bytes(2), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied", ..
            }), custody: None,
        })));
        assert_eq!(io.remaining_work().bytes_copied, 1);
        assert_eq!(work.receipt(), prefix, "denial cannot manufacture a completed copy");
        drop(io);
        work.charge(WorkCounters { bytes_copied: 1, ..WorkCounters::default() })
            .expect("unspent original token credit returns on drop");
        assert_eq!(work.receipt().bytes_copied, 4);
    }

    #[test]
    fn accepted_completion_rejects_foreign_tokens_and_preserves_foreign_guard() {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters { backend_write_operations: 1, ..WorkCounters::default() };
        let budget = completion_budget(maximum, 16);
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let foreign = NativeMaterializationWork::new(budget, &cancellation).expect("foreign ledger");
        let foreign_token = foreign.reserve_work(maximum).expect("foreign owned credit");
        let pool = work.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("original guard");
        let prefix = work.receipt();
        let rejection = AcceptedCompletionIo::new(&work, foreign_token, pool)
            .err().expect("foreign token denied");
        let Error::NativeWork {
            cause: NativeTreeMaterializationError::InvalidLayout, custody: Some(pool),
        } = rejection else { panic!("rejection preserves the genuine typed guard"); };
        assert!(pool.matches_work(&work));
        assert_eq!(pool.bytes(), 16);
        assert_eq!(work.receipt(), prefix);
        drop(pool);

        let token = work.reserve_work(maximum).expect("original token");
        let foreign_pool = foreign.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("foreign guard");
        let rejection = AcceptedCompletionIo::new(&work, token, foreign_pool)
            .err().expect("foreign guard denied");
        let Error::NativeWork {
            cause: NativeTreeMaterializationError::InvalidLayout, custody: Some(pool),
        } = rejection else { panic!("foreign guard must not be retired or adopted"); };
        assert!(pool.matches_work(&foreign));
        assert_eq!(pool.bytes(), 16);
        drop(pool);

        let token = work.reserve_work(maximum).expect("refunded original token");
        let pool = work.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("original pool");
        let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
        let foreign_guard = foreign.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("foreign buffer guard");
        let rejection = io.absorb_native_custody(foreign_guard).expect_err("closed pool denies adoption");
        let Error::NativeWork {
            cause: NativeTreeMaterializationError::InvalidLayout, custody: Some(guard),
        } = rejection else { panic!("foreign custody remains with its real source"); };
        assert!(guard.matches_work(&foreign));
        assert_eq!(io.custody.bytes(), 16);
        drop(guard);
    }

    #[test]
    fn accepted_preheld_sdk_guard_transfers_without_new_admission_or_counter_aliases() {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters {
            authority_records_read: 1, authority_bytes_read: 4, ..WorkCounters::default()
        };
        let mut budget = completion_budget(maximum, 48);
        budget.allocation_operations += 1;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let token = work.reserve_work(maximum).expect("pre-action authority receive credit");
        let pool = work.claim(32, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("closed completion prefix pool");
        let mut preparation = native_work_io(&work).expect("pre-action original SDK buffer allocator");
        assert!(!preparation.requires_prepared_native_dispatch(), "ordinary dispatch is unchanged");
        let mut sdk_bytes = Vec::new();
        preparation.reserve_bytes(&mut sdk_bytes, 16).expect("actual prepaid SDK buffer");
        let sdk_guard = preparation.into_custody();
        let prefix = work.receipt();
        let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
        assert!(io.requires_prepared_native_dispatch(), "identity alone cannot dispatch accepted work");
        io.adopt_preheld(8).expect("original prepared request scalar");
        cancellation.cancel();
        io.absorb_native_custody(sdk_guard).expect("transfer actual preheld SDK output guard");
        assert_eq!(work.receipt(), prefix, "guard transfer is not another allocation or charge");
        assert_eq!(io.custody.bytes(), 48);
        assert_eq!(io.retained, 24, "SDK buffer scalar is adopted once");
        assert_eq!(io.remaining().expect("owned remaining pool").peak_allocation_bytes, 8);
        assert!(matches!(io.admit_owned(9), Err(Error::NativeBoundary { .. })),
            "merged physical custody cannot widen the original completion prefix allowance");
        io.receive_native_record(4).expect("authority receive under its owned token");
        assert_eq!(work.receipt(), prefix.checked_add(maximum).expect("authority-only prefix"));
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(work.receipt().items_examined, 0);
        assert_eq!(io.retained, 24, "SDK custody cannot be retained again as raw authority bytes");
        let sdk_guard = io.take_native_custody(16).expect("transfer genuine SDK DTO custody");
        assert!(sdk_guard.matches_work(&work));
        assert_eq!(sdk_guard.bytes(), sdk_bytes.capacity() as u64);
        assert_eq!(io.custody.bytes(), 32);
        io.release_owned(8).expect("virtual request retirement");
        drop(sdk_bytes);
        drop(sdk_guard);
        let pool = io.into_custody();
        assert_eq!(pool.bytes(), 32);
        assert!(pool.matches_work(&work));
        drop(pool);
    }

    #[test]
    fn accepted_sdk_guard_overprefix_keeps_source_guard_and_never_widens_closed_pool() {
        let cancellation = CancellationToken::new();
        let mut budget = completion_budget(WorkCounters::default(), 32);
        budget.allocation_operations += 2;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let token = work.reserve_work(WorkCounters::default()).expect("pre-action zero token");
        let pool = work.claim(8, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("closed eight-byte completion pool");
        let mut oversized_preparation = native_work_io(&work).expect("first original SDK allocator");
        let mut oversized = Vec::new();
        oversized_preparation.reserve_bytes(&mut oversized, 16).expect("real oversized SDK backing");
        let oversized_guard = oversized_preparation.into_custody();
        let mut bounded_preparation = native_work_io(&work).expect("second original SDK allocator");
        let mut bounded = Vec::new();
        bounded_preparation.reserve_bytes(&mut bounded, 8).expect("real bounded SDK backing");
        let bounded_guard = bounded_preparation.into_custody();
        let prefix = work.receipt();
        let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
        cancellation.cancel();
        let rejection = io.absorb_native_custody(oversized_guard).expect_err("closed prefix rejects oversized SDK result");
        assert_eq!(io.retained, 0);
        assert_eq!(io.custody.bytes(), 8);
        assert_eq!(work.receipt(), prefix, "rejected transfer cannot claim or release original bytes");
        drop(oversized);
        let Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", observed: 16, maximum: 8,
            }), custody: Some(guard),
        } = rejection else { panic!("oversized DTO retains its real source guard and static typed bound"); };
        assert!(guard.matches_work(&work));
        assert_eq!(guard.bytes(), 16);
        drop(guard);
        io.absorb_native_custody(bounded_guard).expect("already-held bounded SDK transfer");
        assert_eq!(io.retained, 8);
        assert_eq!(io.custody.bytes(), 16);
        assert!(matches!(io.admit_owned(1), Err(Error::NativeBoundary { .. })));
        drop(bounded);
        io.release_owned(8).expect("retire virtual SDK DTO after backing drops");
        assert_eq!(io.remaining().expect("closed pool after release").peak_allocation_bytes, 8);
        assert!(matches!(io.retain_owned(9), Err(Error::NativeBoundary { .. })),
            "retiring a merged SDK result must not enlarge later recovery allowance");
        assert_eq!(work.receipt(), prefix);
    }

    #[test]
    fn accepted_receiver_over_token_preserves_the_actual_raw_prefix_before_denial() {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters {
            authority_records_read: 1, authority_bytes_read: 3, items_examined: 1,
            ..WorkCounters::default()
        };
        let work = NativeMaterializationWork::new(completion_budget(maximum, 8), &cancellation)
            .expect("original ledger");
        let token = work.reserve_work(maximum).expect("pre-action receiver credit");
        let pool = work.claim(8, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("pool can cover the actual raw record");
        let prefix = work.receipt();
        let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
        cancellation.cancel();
        assert!(matches!(io.receive_record(5), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                counter: "authority_bytes_read", observed: 5, maximum: 3,
            }), custody: None,
        })), "receiver credit denial keeps its exact original counter");
        assert_eq!(work.receipt(), prefix.checked_add(WorkCounters {
            authority_records_read: 1, authority_bytes_read: 5, items_examined: 1,
            ..WorkCounters::default()
        }).expect("actual completed receiver prefix"));
        assert_eq!(io.remaining_work(), WorkCounters::default());
        assert_eq!(io.retained, 0, "denied record creates no virtual retention");
        assert_eq!(io.custody.bytes(), 8, "real pool remains guarded through raw-buffer drop");
    }

    #[test]
    fn accepted_completion_overpool_records_receiver_prefix_before_static_denial() {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters {
            authority_records_read: 1, authority_bytes_read: 5, items_examined: 1,
            ..WorkCounters::default()
        };
        let work = NativeMaterializationWork::new(completion_budget(maximum, 4), &cancellation)
            .expect("original ledger");
        let token = work.reserve_work(maximum).expect("owned raw receiver credit");
        let pool = work.claim(4, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("closed four-byte pool");
        let prefix = work.receipt();
        let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
        assert!(matches!(io.receive_record(5), Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "accepted completion exceeds its preheld allocation pool",
        })));
        assert_eq!(work.receipt(), prefix.checked_add(maximum).expect("actual malformed prefix"));
        assert_eq!(io.retained, 0, "failed retention leaves no virtual record claim");
        assert_eq!(io.custody.bytes(), 4);
        assert!(matches!(io.release_owned(5), Err(Error::NativeBoundary { .. })));
        io.retain_owned(4).expect("existing pool remains usable");
        assert!(matches!(io.retain_owned(1), Err(Error::NativeBoundary { .. })));
        assert!(matches!(io.take_native_custody(5), Err(Error::NativeBoundary { .. })));
        assert_eq!(io.retained, 4);
    }

    #[test]
    fn accepted_virtual_release_keeps_actual_buffer_claim_until_genuine_guard_transfer() {
        let cancellation = CancellationToken::new();
        let maximum = WorkCounters::default();
        let mut budget = completion_budget(maximum, 16);
        budget.allocation_operations += 1;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let token = work.reserve_work(maximum).expect("zero additive token");
        let mut preparation = native_work_io(&work).expect("original pre-action allocator");
        let mut dto = Vec::new();
        preparation.reserve_bytes(&mut dto, 16).expect("actual original-ledger buffer allocation");
        assert_eq!(dto.capacity(), 16);
        let pool = preparation.into_custody();
        let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
        let prefix = work.receipt();
        io.adopt_preheld(16).expect("already-paid request ownership");
        io.release_owned(16).expect("virtual SDK request retirement");
        assert_eq!(io.custody.bytes(), 16);
        assert!(matches!(work.claim(1, 0, acyclic_fs::WorkCounters::UNCHARGED), Err(NativeTreeMaterializationError::Work(
            WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }
        ))), "virtual release cannot fund another live allocation");
        io.retain_owned(16).expect("returned DTO draws the same pool");
        let dto_guard = io.take_native_custody(16).expect("transfer original real pool claim");
        assert_eq!(io.retained, 0);
        assert_eq!(io.custody.bytes(), 0);
        assert!(dto_guard.matches_work(&work));
        assert_eq!(dto_guard.bytes(), 16);
        assert_eq!(work.receipt(), prefix, "transfer is not a new allocation or work event");
        drop(io);
        assert!(work.claim(1, 0, acyclic_fs::WorkCounters::UNCHARGED).is_err(), "DTO's genuine claim survives observer drop");
        drop(dto);
        drop(dto_guard);
        work.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("only real guard retirement frees the original allowance");
    }

    #[test]
    fn accepted_observer_rejects_unused_allocating_boundaries_without_heap_diagnostics() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(completion_budget(WorkCounters::default(), 16), &cancellation)
            .expect("original ledger");
        let token = work.reserve_work(WorkCounters::default()).expect("zero additive token");
        let pool = work.claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED).expect("preheld pool");
        let mut io = AcceptedCompletionIo::new(&work, token, pool).expect("accepted observer");
        let prefix = work.receipt();
        let mut bytes = Vec::new();
        let mut text = String::new();
        assert!(matches!(io.reserve_bytes(&mut bytes, 1), Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported, ..
        })));
        assert!(matches!(io.reserve_text(&mut text, 1), Err(Error::NativeBoundary { .. })));
        assert!(matches!(io.clone_text("owned clone"), Err(Error::NativeBoundary { .. })));
        assert!(matches!(io.decode_json_value(b"null"), Err(Error::NativeBoundary { .. })));
        for charge in [
            crate::contract::JsonCodecCharge::Retain { bytes: 1 },
            crate::contract::JsonCodecCharge::Release { bytes: 1 },
            crate::contract::JsonCodecCharge::Read { bytes: 1 },
            crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes: 1 },
        ] {
            assert!(matches!(io.codec_charge(charge), Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, ..
            })), "only the closed prepaid writer's Work and Copy events are accepted");
        }
        assert!(matches!(io.admit_allocation(1), Err(Error::NativeBoundary { .. })));
        let cause = bytes.try_reserve_exact(usize::MAX)
            .expect_err("capacity overflow produces an inline allocator cause");
        assert!(matches!(io.allocation_failure(cause), Error::NativeWork {
            cause: NativeTreeMaterializationError::AllocationFailed(_), custody: None,
        }));
        assert_eq!(bytes.capacity(), 0);
        assert_eq!(text.capacity(), 0);
        assert_eq!(io.retained, 0);
        assert_eq!(io.custody.bytes(), 16);
        assert_eq!(work.receipt(), prefix);
    }
}

impl<P: StreamProvider> HistoryReader<P> {
    /// Pins the original committed cut using the caller's native ledger.
    pub async fn pin_with_native_work(
        &self,
        after_revision: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(HistoryCursor, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let cursor = self.pin(after_revision, Some(&mut native_io)).await?;
        Ok((cursor, native_io.into_custody()))
    }

    /// Reads the original authenticated bounded page on the same native ledger.
    pub async fn read_page_with_native_work(
        &self,
        cursor: &HistoryCursor,
        limits: HistoryReadLimits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(HistoryPage, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let page = self.read_page(cursor, limits, Some(&mut native_io)).await?;
        native_io.return_staged_output()?;
        Ok((page, native_io.into_custody()))
    }

    /// Reads the original authenticated immutable operation locator and event.
    pub async fn operation_event_bounded_with_native_work(
        &self,
        operation: crate::OperationId,
        maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<Event>, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let (event, _) = self.operation_event(operation, maximum_bytes, &mut Some(&mut native_io)).await?;
        native_io.return_staged_output()?;
        Ok((event, native_io.into_custody()))
    }

    /// Projects the archived effect through the original bounded state engine.
    pub async fn effect_with_native_work(
        &self,
        cursor: &HistoryCursor,
        effect: crate::EffectId,
        limits: HistoryReadLimits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<crate::core::EffectState>, acyclic_fs::NativeAllocation)> {
        let mut native_io = super::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let state = self.effect(
            cursor, effect, limits, Some(&mut native_io),
        ).await?.map(|(state, _)| state);
        native_io.return_staged_output()?;
        Ok((state, native_io.into_custody()))
    }

}
