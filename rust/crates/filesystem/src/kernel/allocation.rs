//! Deterministic logical allocation accounting for bounded kernel work.

use crate::foundation::usize_to_u64;
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::ObjectId;
use std::mem::size_of;
use thiserror::Error;

pub(crate) fn bytes_owner_control_bytes<T>() -> Result<u64, WorkError> {
    let (layout, _) = std::alloc::Layout::new::<std::sync::atomic::AtomicUsize>()
        .extend(std::alloc::Layout::new::<T>())
        .map_err(|_| WorkError::Overflow)?;
    Ok(usize_to_u64(layout.pad_to_align().size()))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Tracks live and reserved logical allocation bytes.
pub struct AllocationLedger {
    live_bytes: u64,
    // Accepted future payload/control capacity, not a physical allocation or peak.
    #[cfg(not(target_arch = "wasm32"))]
    reserved_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LogicalVecCapacity {
    elements: usize,
    logical_bytes: u64,
}

enum LogicalVecAllocationStep {
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
}

impl LogicalVecCapacity {
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn ensure_for_push<T>(
        &mut self,
        values: &mut Vec<T>,
        maximum_elements: usize,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<(), AllocationError> {
        self.ensure_for_push_using(
            values,
            maximum_elements,
            |error| error,
            |step| match step {
                LogicalVecAllocationStep::Claim { bytes } => {
                    ledger.claim_bytes(bytes, u64::from(bytes != 0), work, budget)
                }
                LogicalVecAllocationStep::ReserveFailed { bytes, .. } => {
                    ledger.release(bytes)?;
                    Err(AllocationError::AllocationFailed)
                }
                LogicalVecAllocationStep::Capacity { requested, actual } => {
                    ledger.record_owned_bytes(actual - requested, work)?;
                    work.verify(budget)?;
                    Ok(())
                }
            },
        )
    }

    pub(crate) fn ensure_for_push_with_accounting<T>(
        &mut self,
        values: &mut Vec<T>,
        maximum_elements: usize,
        field_guard: &mut KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if self.logical_bytes == 0 && matches!(&*field_guard, KernelOwnedAllocation::Standalone) {
            *field_guard = accounting.claim_owned(0, 0)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let original = matches!(&*field_guard, KernelOwnedAllocation::NativeOriginal(_));
        self.ensure_for_push_using(
            values,
            maximum_elements,
            |error| error.into_work_or(crate::FsError::PendingMutationAllocationFailed),
            |step| match step {
                LogicalVecAllocationStep::Claim { bytes } => {
                    let mut claim = accounting.claim_owned(bytes, u64::from(bytes != 0))?;
                    if let Err(error) = field_guard.absorb(&mut claim) {
                        accounting.release_owned(&mut claim, bytes)?;
                        return Err(error);
                    }
                    Ok(())
                }
                LogicalVecAllocationStep::ReserveFailed { bytes, error } => {
                    // This admission never became physical; the old vector is unchanged.
                    accounting.release_owned(field_guard, bytes)?;
                    #[cfg(not(target_arch = "wasm32"))]
                    if original {
                        return Err(
                            crate::NativeTreeMaterializationError::AllocationFailed(error).into(),
                        );
                    }
                    let _ = error;
                    Err(crate::FsError::PendingMutationAllocationFailed)
                }
                LogicalVecAllocationStep::Capacity { requested, actual } => {
                    accounting.reconcile_owned(field_guard, requested, actual)
                }
            },
        )
    }

    fn ensure_for_push_using<T, E>(
        &mut self,
        values: &mut Vec<T>,
        maximum_elements: usize,
        map_error: impl Fn(AllocationError) -> E,
        mut allocation: impl FnMut(LogicalVecAllocationStep) -> Result<(), E>,
    ) -> Result<(), E> {
        if values.len() < self.elements {
            return Ok(());
        }
        if self.elements >= maximum_elements {
            return Err(map_error(AllocationError::CapacityExceeded));
        }
        let next = if self.elements == 0 {
            1
        } else {
            self.elements.saturating_mul(2).min(maximum_elements)
        };
        if values.capacity() >= next {
            self.elements = next;
            return Ok(());
        }
        let requested_bytes = next
            .checked_mul(size_of::<T>())
            .map(usize_to_u64)
            .ok_or_else(|| map_error(AllocationError::Overflow))?;
        let old_bytes = self.logical_bytes;
        let requested = requested_bytes
            .checked_sub(old_bytes)
            .ok_or_else(|| map_error(AllocationError::Overflow))?;
        let additional = next
            .checked_sub(values.len())
            .ok_or_else(|| map_error(AllocationError::InvalidCapacity))?;
        allocation(LogicalVecAllocationStep::Claim { bytes: requested })?;
        if let Err(error) = values.try_reserve_exact(additional) {
            return allocation(LogicalVecAllocationStep::ReserveFailed {
                bytes: requested,
                error,
            });
        }
        let actual_bytes = values
            .capacity()
            .checked_mul(size_of::<T>())
            .map(usize_to_u64)
            .ok_or_else(|| map_error(AllocationError::Overflow))?;
        let actual = actual_bytes
            .checked_sub(old_bytes)
            .ok_or_else(|| map_error(AllocationError::Overflow))?;
        // Completed backing and logical growth survive a late quota/cancellation error.
        self.elements = next;
        self.logical_bytes = actual_bytes;
        allocation(LogicalVecAllocationStep::Capacity { requested, actual })
    }

    pub(crate) const fn logical_bytes(self) -> u64 {
        self.logical_bytes
    }
}

impl AllocationLedger {
    #[inline]
    pub(crate) fn claim_elements<T>(
        &mut self,
        count: usize,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<u64, AllocationError> {
        let bytes = count
            .checked_mul(size_of::<T>())
            .map(usize_to_u64)
            .ok_or(AllocationError::Overflow)?;
        self.claim_bytes(bytes, u64::from(count != 0), work, budget)?;
        Ok(bytes)
    }

    #[inline]
    pub(crate) fn claim_bytes(
        &mut self,
        bytes: u64,
        operations: u64,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<(), AllocationError> {
        let next_live = self
            .live_bytes
            .checked_add(bytes)
            .ok_or(AllocationError::Overflow)?;
        #[cfg(not(target_arch = "wasm32"))]
        if self.reserved_bytes != 0 {
            work.admit(
                &WorkCounters {
                    allocation_operations: operations,
                    peak_allocation_bytes: next_live
                        .checked_add(self.reserved_bytes)
                        .ok_or(AllocationError::Overflow)?,
                    ..WorkCounters::UNCHARGED
                },
                &budget,
            )?;
        }
        work.charge_allocation(operations, next_live, &budget)?;
        self.live_bytes = next_live;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn reserve_allocation(
        &mut self,
        bytes: u64,
        work: &WorkCounters,
        budget: WorkBudget,
    ) -> Result<(), AllocationError> {
        let reserved = self
            .reserved_bytes
            .checked_add(bytes)
            .ok_or(AllocationError::Overflow)?;
        work.admit(
            &WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: self
                    .live_bytes
                    .checked_add(reserved)
                    .ok_or(AllocationError::Overflow)?,
                ..WorkCounters::UNCHARGED
            },
            &budget,
        )?;
        self.reserved_bytes = reserved;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn cancel_reserved_allocation(&mut self, bytes: u64) -> Result<(), AllocationError> {
        let reserved = self
            .reserved_bytes
            .checked_sub(bytes)
            .ok_or(AllocationError::ReleaseInvariant)?;
        self.reserved_bytes = reserved;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn record_reserved_allocation(
        &mut self,
        bytes: u64,
        work: &mut WorkCounters,
    ) -> Result<(), AllocationError> {
        let reserved = self
            .reserved_bytes
            .checked_sub(bytes)
            .ok_or(AllocationError::ReleaseInvariant)?;
        let live = self
            .live_bytes
            .checked_add(bytes)
            .ok_or(AllocationError::Overflow)?;
        work.try_add_assign(&WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: live,
            ..WorkCounters::UNCHARGED
        })?;
        self.reserved_bytes = reserved;
        self.live_bytes = live;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) const fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    pub(crate) fn record_owned_bytes(
        &mut self,
        bytes: u64,
        work: &mut WorkCounters,
    ) -> Result<(), AllocationError> {
        let next_live = self
            .live_bytes
            .checked_add(bytes)
            .ok_or(AllocationError::Overflow)?;
        work.peak_allocation_bytes = work.peak_allocation_bytes.max(next_live);
        self.live_bytes = next_live;
        Ok(())
    }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<(), AllocationError> {
        self.live_bytes = self
            .live_bytes
            .checked_sub(bytes)
            .ok_or(AllocationError::ReleaseInvariant)?;
        Ok(())
    }

    /// Returns currently retained logical bytes.
    pub const fn live_bytes(self) -> u64 {
        self.live_bytes
    }
}

/// Retains the allocation backing a value returned by a kernel operation.
#[cfg_attr(target_arch = "wasm32", derive(Clone, Copy))]
pub enum KernelOwnedAllocation {
    /// Backing tracked by the standalone accounting context.
    Standalone,
    /// Backing retained on the original native invocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    NativeOriginal(crate::NativeAllocation),
}

impl KernelOwnedAllocation {
    pub(crate) fn absorb(&mut self, other: &mut Self) -> Result<(), crate::FsError> {
        match (self, other) {
            (Self::Standalone, Self::Standalone) => Ok(()),
            #[cfg(not(target_arch = "wasm32"))]
            (Self::NativeOriginal(owned), Self::NativeOriginal(other)) => {
                owned.absorb(other).map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            _ => Err(AllocationError::ReleaseInvariant
                .into_work_or(crate::FsError::PendingMutationAllocationFailed)),
        }
    }
}

#[doc(hidden)]
pub enum KernelAccounting<'a> {
    Standalone {
        allocations: AllocationLedger,
        work: WorkCounters,
        budget: WorkBudget,
        cancellation: &'a crate::CancellationToken,
    },
    BorrowedStandalone {
        allocations: &'a mut AllocationLedger,
        work: &'a mut WorkCounters,
        budget: WorkBudget,
        cancellation: &'a crate::CancellationToken,
    },
    #[cfg(not(target_arch = "wasm32"))]
    NativeOriginal {
        work: &'a crate::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    },
}

mod accounting;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod effects;

pub(crate) struct VisitedObjectSet {
    slots: Vec<Option<ObjectId>>,
    maximum_entries: usize,
    length: usize,
    logical_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VisitedInsert {
    pub(crate) inserted: bool,
    pub(crate) probes: u64,
}

impl VisitedObjectSet {
    const LOAD_NUMERATOR: usize = 2;
    const INITIAL_ENTRIES: usize = 8;

    pub(crate) fn new_with_accounting(
        maximum_entries: usize,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<super::persistent_point::Owned<Self>, crate::FsError> {
        use super::persistent_point::Owned;
        if maximum_entries == 0 {
            return Err(crate::FsError::PendingMutationAllocationFailed);
        }
        let slot_count = Self::slot_count(maximum_entries.min(Self::INITIAL_ENTRIES))
            .map_err(|error| error.into_work_or(crate::FsError::PendingMutationAllocationFailed))?;
        let mut slots = Owned::<Vec<Option<ObjectId>>>::allocate(slot_count, accounting)?;
        slots.value.resize(slot_count, None);
        let Owned {
            value,
            allocation,
            bytes,
        } = slots;
        Ok(Owned::new(
            Self {
                slots: value,
                maximum_entries,
                length: 0,
                logical_bytes: bytes,
            },
            allocation,
            bytes,
        ))
    }

    pub(crate) fn insert_with_accounting(
        set: &mut super::persistent_point::Owned<Self>,
        object: ObjectId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<VisitedInsert, crate::FsError> {
        let result = set
            .value
            .insert_accounted(object, &mut set.allocation, accounting);
        set.bytes = set.value.logical_bytes;
        result
    }

    fn insert_accounted(
        &mut self,
        object: ObjectId,
        allocation: &mut KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<VisitedInsert, crate::FsError> {
        let mut probes = 0_u64;
        loop {
            let mask = self.slots.len().checked_sub(1).ok_or(WorkError::Overflow)?;
            let mut index = object_hash(object) & mask;
            loop {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                probes = probes.checked_add(1).ok_or(WorkError::Overflow)?;
                match self.slots.get(index).ok_or(WorkError::Overflow)? {
                    Some(existing) if *existing == object => {
                        return Ok(VisitedInsert {
                            inserted: false,
                            probes,
                        });
                    }
                    Some(_) => index = (index + 1) & mask,
                    None if self.length == self.maximum_entries => {
                        return Err(crate::FsError::PendingMutationAllocationFailed);
                    }
                    None if self.length < self.entry_capacity() => {
                        *self.slots.get_mut(index).ok_or(WorkError::Overflow)? = Some(object);
                        self.length += 1;
                        return Ok(VisitedInsert {
                            inserted: true,
                            probes,
                        });
                    }
                    None => break,
                }
            }
            probes = probes
                .checked_add(self.grow_with_accounting(accounting, allocation)?)
                .ok_or(WorkError::Overflow)?;
        }
    }

    fn grow_with_accounting(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
        allocation: &mut KernelOwnedAllocation,
    ) -> Result<u64, crate::FsError> {
        use super::persistent_point::{Owned, retire_allocation};
        let next_entries = self
            .entry_capacity()
            .checked_mul(2)
            .ok_or(WorkError::Overflow)?
            .min(self.maximum_entries);
        let slot_count = Self::slot_count(next_entries)
            .map_err(|error| error.into_work_or(crate::FsError::PendingMutationAllocationFailed))?;
        let mut slots = Owned::<Vec<Option<ObjectId>>>::allocate(slot_count, accounting)?;
        slots.value.resize(slot_count, None);
        let mask = slot_count - 1;
        let mut probes = 0_u64;
        let rehashed = (|| {
            for object in self.slots.iter().flatten().copied() {
                let mut index = object_hash(object) & mask;
                loop {
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    probes = probes.checked_add(1).ok_or(WorkError::Overflow)?;
                    if slots.value.get(index).ok_or(WorkError::Overflow)?.is_none() {
                        let copied = WorkCounters {
                            bytes_copied: usize_to_u64(size_of::<ObjectId>()),
                            ..WorkCounters::UNCHARGED
                        };
                        accounting.admit(copied)?;
                        *slots.value.get_mut(index).ok_or(WorkError::Overflow)? = Some(object);
                        accounting.record_completed(copied)?;
                        break;
                    }
                    index = (index + 1) & mask;
                }
            }
            Ok::<(), crate::FsError>(())
        })();
        if let Err(error) = rehashed {
            slots.retire(accounting)?;
            return Err(error);
        }
        let Owned {
            value,
            allocation: next_allocation,
            bytes,
        } = slots;
        let old_slots = std::mem::replace(&mut self.slots, value);
        let old_allocation = std::mem::replace(allocation, next_allocation);
        let old_bytes = std::mem::replace(&mut self.logical_bytes, bytes);
        drop(old_slots);
        retire_allocation(old_allocation, old_bytes, accounting)?;
        Ok(probes)
    }

    fn entry_capacity(&self) -> usize {
        (self.slots.len() / Self::LOAD_NUMERATOR).min(self.maximum_entries)
    }

    fn slot_count(entries: usize) -> Result<usize, AllocationError> {
        entries
            .checked_mul(Self::LOAD_NUMERATOR)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(AllocationError::Overflow)
    }
}

fn object_hash(object: ObjectId) -> usize {
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&object.digest.as_bytes()[..8]);
    let value = u64::from_le_bytes(prefix) ^ u64::from(object.kind.canonical_tag());
    #[cfg(target_pointer_width = "64")]
    {
        usize::from_ne_bytes(value.to_ne_bytes())
    }
    #[cfg(target_pointer_width = "32")]
    {
        let bytes = value.to_ne_bytes();
        usize::from_ne_bytes([
            bytes[0] ^ bytes[4],
            bytes[1] ^ bytes[5],
            bytes[2] ^ bytes[6],
            bytes[3] ^ bytes[7],
        ])
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum AllocationError {
    #[error("logical allocation arithmetic overflowed")]
    Overflow,
    #[error("logical allocation release exceeded live bytes")]
    ReleaseInvariant,
    #[error("visited-set capacity must be positive")]
    InvalidCapacity,
    #[error("visited-set entry capacity was exceeded")]
    CapacityExceeded,
    #[error("bounded logical allocation failed")]
    #[cfg(any(test, not(target_arch = "wasm32")))]
    #[cfg(any(test, feature = "native-watch"))]
    AllocationFailed,
    #[error(transparent)]
    Work(#[from] WorkError),
}

impl AllocationError {
    pub(crate) fn into_work_or<E: From<WorkError>>(self, failed: E) -> E {
        match self {
            Self::Work(error) => error.into(),
            Self::Overflow | Self::ReleaseInvariant => WorkError::Overflow.into(),
            _ => failed,
        }
    }
}

#[cfg(test)]
#[path = "tests/allocation.rs"]
mod tests;
