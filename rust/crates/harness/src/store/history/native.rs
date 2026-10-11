//! SDK boundaries on the original native ledger, without another budget/meter.

use super::HistoryIo;
use crate::{Error, Result, core::{Authority, Event}};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, WorkCounters};
use std::mem::size_of;

pub(crate) struct NativeHistoryIo<'a> {
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

pub(crate) fn native_work_io(
    work: &NativeMaterializationWork,
) -> std::result::Result<NativeHistoryIo<'_>, NativeTreeMaterializationError> {
    Ok(NativeHistoryIo {
        work, custody: work.retain_owned(0)?, staged_bytes: 0, staged_items: 0,
    })
}


impl NativeHistoryIo<'_> {
    /// Transfers the same ledger's retained source payloads to their last user.
    pub(crate) fn into_custody(self) -> NativeAllocation {
        self.custody
    }
    pub(crate) fn return_staged_output(&mut self) -> Result<()> {
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
        // Receiver prefix precedes cancellation, quota checks and all decoding.
        self.work.spent(WorkCounters { authority_records_read: 1, authority_bytes_read: bytes,
            items_examined: 1, peak_allocation_bytes: bytes, ..WorkCounters::default() })
            .map_err(|error| self.native_error(error))?;
        // Any failure leaves no new record claim. The raw receiver prefix is
        // already observable, but quota/cancellation must be checked before
        // custody is retained so callers can drop a rejected record exactly.
        self.work.remaining().map_err(|error| self.native_error(error))?;
        self.retain(bytes)?;
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
        let (copy, custody) =
            crate::conversation::clone_native_reference_text(text, self.work)?;
        // The scalar factory gates the actual reserve and copy separately.
        // Failed growth cannot spend a copy which never happened.
        self.absorb_native_custody(custody)?;
        Ok(copy)
    }
    fn reserve_events(&mut self, values: &mut Vec<Event>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
    fn reserve_records(&mut self, values: &mut Vec<acyclic_stream::Record>, maximum: usize) -> Result<()> { self.reserve(values, maximum) }
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
        let decoded = io.decode_preparation::<serde_json::Value>(bytes)?;
        let retained = io.owned_bytes()?;
        let value = decoded.into_retained();
        let incoming = io.into_native_custody(retained)?;
        let held = incoming.bytes();
        // Both guards originate from this exact finite native ledger; merging
        // transfers already admitted custody, without another claim or copy.
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
            if !verifier.supports_native_read(reference.volume(), work)? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "content verifier does not expose the original native work boundary",
                });
            }
            verifier.verify_with_native_work(
                reference, reference.descriptor().byte_length(), work,
            )?.await
        })?;
        Ok(super::HistoryContentFuture::Native(future))
    }
    fn read_content<'a>(&'a mut self, verifier: &'a dyn crate::conversation::ContentResidencyVerifier,
        reference: &'a crate::conversation::FileRef) -> Result<super::HistoryContentFuture<'a, Vec<u8>>> {
        let work = self.work;
        let future = crate::NativeFutureBundle::new(work, || async move {
            if !verifier.supports_native_read(reference.volume(), work)? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "content verifier does not expose the original native work boundary",
                });
            }
            let (bytes, guard) = verifier.read_with_native_work(
                reference, reference.descriptor().byte_length(), work,
            )?.await?;
            self.absorb_native_custody(guard)?;
            Ok(bytes)
        })?;
        Ok(super::HistoryContentFuture::Native(future))
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
        let incoming = foreign.retain_owned(16).expect("foreign held allowance");
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
        let incoming = work.retain_owned(16).expect("same-ledger held allowance");
        let prefix = work.receipt();
        io.absorb_native_custody(incoming).expect("same-ledger transfer");
        assert_eq!(io.custody.bytes(), 16);
        assert_eq!(work.receipt(), prefix, "adoption makes no new claim or call");
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
            &mut |charge| io.codec_charge(charge),
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
            &mut |charge| io.codec_charge(charge),
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
}
