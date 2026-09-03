//! Deterministic logical allocation accounting for bounded kernel work.

use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::ObjectId;
use std::mem::size_of;
use thiserror::Error;

/// Logical owned-buffer bytes, excluding allocator headers and stack storage.
///
/// Every claim is admitted before allocation. Callers retain the returned byte
/// count and release it when that owned capacity ceases to be live. The peak is
/// operation-wide and is mirrored into the canonical work receipt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AllocationLedger {
    live_bytes: u64,
}

/// Deterministic logical capacity for a fallibly growing operation vector.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LogicalVecCapacity {
    elements: usize,
    logical_bytes: u64,
}

impl LogicalVecCapacity {
    pub(crate) fn ensure_for_push<T>(
        &mut self,
        values: &mut Vec<T>,
        maximum_elements: usize,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<(), AllocationError> {
        if values.len() < self.elements {
            return Ok(());
        }
        if self.elements >= maximum_elements {
            return Err(AllocationError::CapacityExceeded);
        }
        let next = if self.elements == 0 {
            1
        } else {
            self.elements.saturating_mul(2).min(maximum_elements)
        };
        let delta = next
            .checked_sub(self.elements)
            .ok_or(AllocationError::Overflow)?;
        let bytes = ledger.claim_elements::<T>(delta, work, budget)?;
        if values.try_reserve_exact(delta).is_err() {
            ledger.release(bytes)?;
            return Err(AllocationError::AllocationFailed);
        }
        self.elements = next;
        self.logical_bytes = self
            .logical_bytes
            .checked_add(bytes)
            .ok_or(AllocationError::Overflow)?;
        Ok(())
    }

    pub(crate) const fn logical_bytes(self) -> u64 {
        self.logical_bytes
    }
}

impl AllocationLedger {
    pub(crate) fn claim_elements<T>(
        &mut self,
        count: usize,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<u64, AllocationError> {
        let bytes = count
            .checked_mul(size_of::<T>())
            .map(crate::foundation::usize_to_u64)
            .ok_or(AllocationError::Overflow)?;
        self.claim_bytes(bytes, u64::from(count != 0), work, budget)?;
        Ok(bytes)
    }

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
        let mut prospective = work.checked_add(WorkCounters {
            allocation_operations: operations,
            ..WorkCounters::default()
        })?;
        prospective.peak_allocation_bytes = prospective.peak_allocation_bytes.max(next_live);
        prospective.verify(budget)?;
        self.live_bytes = next_live;
        *work = prospective;
        Ok(())
    }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<(), AllocationError> {
        self.live_bytes = self
            .live_bytes
            .checked_sub(bytes)
            .ok_or(AllocationError::ReleaseInvariant)?;
        Ok(())
    }

    pub(crate) const fn live_bytes(self) -> u64 {
        self.live_bytes
    }
}

/// Deterministic fixed-capacity cycle/alias set for authenticated object walks.
pub(crate) struct VisitedObjectSet {
    slots: Vec<Option<ObjectId>>,
    maximum_entries: usize,
    length: usize,
    logical_bytes: u64,
}

/// Result of one deterministic visited-set probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VisitedInsert {
    pub(crate) inserted: bool,
    pub(crate) probes: u64,
}

impl VisitedObjectSet {
    const LOAD_NUMERATOR: usize = 2;
    const INITIAL_ENTRIES: usize = 8;

    pub(crate) fn new(
        maximum_entries: usize,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<Self, AllocationError> {
        if maximum_entries == 0 {
            return Err(AllocationError::InvalidCapacity);
        }
        let slot_count = Self::slot_count(maximum_entries.min(Self::INITIAL_ENTRIES))?;
        let logical_bytes = ledger.claim_elements::<Option<ObjectId>>(slot_count, work, budget)?;
        let mut slots = Vec::new();
        if slots.try_reserve_exact(slot_count).is_err() {
            ledger.release(logical_bytes)?;
            return Err(AllocationError::AllocationFailed);
        }
        slots.resize(slot_count, None);
        Ok(Self {
            slots,
            maximum_entries,
            length: 0,
            logical_bytes,
        })
    }

    pub(crate) fn insert(
        &mut self,
        object: ObjectId,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<VisitedInsert, AllocationError> {
        let mut probes = 0_u64;
        loop {
            let mask = self.slots.len() - 1;
            let mut index = object_hash(object) & mask;
            loop {
                Self::charge_items(work, budget, 1)?;
                probes = probes.checked_add(1).ok_or(AllocationError::Overflow)?;
                match self.slots[index] {
                    Some(existing) if existing == object => {
                        return Ok(VisitedInsert {
                            inserted: false,
                            probes,
                        });
                    }
                    Some(_) => index = (index + 1) & mask,
                    None if self.length == self.maximum_entries => {
                        return Err(AllocationError::CapacityExceeded);
                    }
                    None if self.length < self.entry_capacity() => {
                        self.slots[index] = Some(object);
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
                .checked_add(self.grow(ledger, work, budget)?)
                .ok_or(AllocationError::Overflow)?;
        }
    }

    fn grow(
        &mut self,
        ledger: &mut AllocationLedger,
        work: &mut WorkCounters,
        budget: WorkBudget,
    ) -> Result<u64, AllocationError> {
        let next_entries = self
            .entry_capacity()
            .checked_mul(2)
            .ok_or(AllocationError::Overflow)?
            .min(self.maximum_entries);
        let slot_count = Self::slot_count(next_entries)?;
        let new_bytes = ledger.claim_elements::<Option<ObjectId>>(slot_count, work, budget)?;
        let mut slots = Vec::new();
        if slots.try_reserve_exact(slot_count).is_err() {
            ledger.release(new_bytes)?;
            return Err(AllocationError::AllocationFailed);
        }
        slots.resize(slot_count, None);
        let mask = slot_count - 1;
        let mut probes = 0_u64;
        for object in self.slots.iter().flatten().copied() {
            let mut index = object_hash(object) & mask;
            loop {
                if let Err(error) = Self::charge_items(work, budget, 1) {
                    ledger.release(new_bytes)?;
                    return Err(error);
                }
                let Some(next_probes) = probes.checked_add(1) else {
                    ledger.release(new_bytes)?;
                    return Err(AllocationError::Overflow);
                };
                probes = next_probes;
                if slots[index].is_none() {
                    if let Err(error) = Self::charge_copied(work, budget) {
                        ledger.release(new_bytes)?;
                        return Err(error);
                    }
                    slots[index] = Some(object);
                    break;
                }
                index = (index + 1) & mask;
            }
        }
        let old_bytes = self.logical_bytes;
        self.slots = slots;
        self.logical_bytes = new_bytes;
        ledger.release(old_bytes)?;
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

    fn charge_items(
        work: &mut WorkCounters,
        budget: WorkBudget,
        count: u64,
    ) -> Result<(), AllocationError> {
        let prospective = work.checked_add(WorkCounters {
            items_examined: count,
            ..WorkCounters::default()
        })?;
        prospective.verify(budget)?;
        *work = prospective;
        Ok(())
    }

    fn charge_copied(work: &mut WorkCounters, budget: WorkBudget) -> Result<(), AllocationError> {
        let prospective = work.checked_add(WorkCounters {
            bytes_copied: u64::try_from(size_of::<ObjectId>())
                .map_err(|_| AllocationError::Overflow)?,
            ..WorkCounters::default()
        })?;
        prospective.verify(budget)?;
        *work = prospective;
        Ok(())
    }

    pub(crate) fn release(self, ledger: &mut AllocationLedger) -> Result<(), AllocationError> {
        ledger.release(self.logical_bytes)
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

/// Stable logical-allocation failures.
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
    AllocationFailed,
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(test)]
#[path = "tests/allocation.rs"]
mod tests;
