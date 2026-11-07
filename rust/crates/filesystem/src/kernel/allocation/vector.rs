//! One fallible vector growth boundary with original allocation and copy custody.

#[cfg(not(target_arch = "wasm32"))]
use super::effects;
use super::{AllocationError, KernelAccounting, KernelOwnedAllocation};
use crate::foundation::usize_to_u64;
use crate::performance::WorkCounters;
use std::mem::size_of;

pub(super) enum VecAllocationStep {
    CopyAdmission {
        bytes: u64,
    },
    Claim {
        bytes: u64,
    },
    ReserveFailed {
        bytes: u64,
        error: std::collections::TryReserveError,
    },
    Capacity {
        requested: u64,
        actual: u64,
    },
    CopyComplete {
        bytes: u64,
    },
}

pub(super) fn reserve_vec_using<T, E>(
    values: &mut Vec<T>,
    minimum: usize,
    map_error: impl Fn(AllocationError) -> E,
    allocation: &mut impl FnMut(VecAllocationStep) -> Result<(), E>,
) -> Result<(), E> {
    if values.capacity() >= minimum {
        return Ok(());
    }
    let old_bytes = values
        .capacity()
        .checked_mul(size_of::<T>())
        .map(usize_to_u64)
        .ok_or_else(|| map_error(AllocationError::Overflow))?;
    let requested = minimum
        .checked_mul(size_of::<T>())
        .map(usize_to_u64)
        .and_then(|bytes| bytes.checked_sub(old_bytes))
        .ok_or_else(|| map_error(AllocationError::Overflow))?;
    // The allocator may relocate the complete old backing, including spare
    // capacity. Withhold that allowance before it can enter the allocator.
    if old_bytes != 0 {
        allocation(VecAllocationStep::CopyAdmission { bytes: old_bytes })?;
    }
    allocation(VecAllocationStep::Claim { bytes: requested })?;
    let old_pointer = values.as_ptr();
    if let Err(error) = values.try_reserve_exact(minimum - values.len()) {
        return allocation(VecAllocationStep::ReserveFailed {
            bytes: requested,
            error,
        });
    }
    let actual = values
        .capacity()
        .checked_mul(size_of::<T>())
        .map(usize_to_u64)
        .and_then(|bytes| bytes.checked_sub(old_bytes))
        .ok_or_else(|| map_error(AllocationError::Overflow))?;
    let reconciled = allocation(VecAllocationStep::Capacity { requested, actual });
    // Record completed relocation even if reconciliation reports a late error.
    let copied = if old_pointer == values.as_ptr() {
        0
    } else {
        old_bytes
    };
    let completed = if old_bytes == 0 {
        Ok(())
    } else {
        allocation(VecAllocationStep::CopyComplete { bytes: copied })
    };
    reconciled.and(completed)
}

pub(crate) fn reserve_vec_with_accounting<T>(
    values: &mut Vec<T>,
    minimum: usize,
    owner: &mut KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
    reserve_error: impl Fn(std::collections::TryReserveError) -> crate::FsError,
) -> Result<(), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    let mut credit = effects::EffectCredit::for_accounting(accounting);
    reserve_vec_using(
        values,
        minimum,
        |error| error.into_work_or(crate::FsError::PendingMutationAllocationFailed),
        &mut |step| match step {
            VecAllocationStep::CopyAdmission { bytes } => {
                let delta = WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::UNCHARGED
                };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(credit) = &mut credit {
                    return credit.reserve(delta, false);
                }
                accounting.admit(delta)
            }
            VecAllocationStep::Claim { bytes } => {
                let mut claim = accounting.claim_owned(bytes, u64::from(bytes != 0))?;
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(owner, KernelOwnedAllocation::Standalone)
                    && matches!(claim, KernelOwnedAllocation::NativeOriginal(_))
                {
                    *owner = claim;
                    return Ok(());
                }
                if let Err(error) = owner.absorb(&mut claim) {
                    accounting.release_owned(&mut claim, bytes)?;
                    return Err(error);
                }
                Ok(())
            }
            VecAllocationStep::ReserveFailed { bytes, error } => {
                // The old backing is unchanged. Preserve the allocator's cause
                // even if retiring the unused admission reports another error.
                let _ = accounting.release_owned(owner, bytes);
                Err(reserve_error(error))
            }
            VecAllocationStep::Capacity { requested, actual } => {
                accounting.reconcile_owned(owner, requested, actual)
            }
            VecAllocationStep::CopyComplete { bytes } => {
                let delta = WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::UNCHARGED
                };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(credit) = &mut credit {
                    return credit.complete(delta);
                }
                accounting.record_completed(delta)
            }
        },
    )
}
