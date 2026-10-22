use super::*;
use std::{alloc::Layout, collections::hash_map::RandomState, hash::BuildHasher};

pub(super) fn hash_state() -> RandomState {
    #[cfg(all(test, feature = "memory"))]
    {
        // Independent differential fixtures use identical hash keys, never
        // shared table contents, so their exact probe receipts are comparable.
        thread_local! { static HASHES: RandomState = RandomState::new(); }
        HASHES.with(Clone::clone)
    }
    #[cfg(not(all(test, feature = "memory")))]
    RandomState::new()
}

// One exact slot allocation, with no insertion-time growth or hidden metadata.
// Keep backing storage before its original allocation custody in drop order.
pub(super) struct Index<'a, T> {
    slots: Vec<Option<(ObjectId, &'a T)>>,
    hashes: RandomState,
    allocation: KernelOwnedAllocation,
    bytes: u64,
}

impl<'a, T> Index<'a, T> {
    pub(super) fn new(
        count: usize,
        work: &mut KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        if count == 0 {
            return Err(ObjectStoreError::EmptyWriteBatch.into());
        }
        let slots = count
            .checked_mul(8)
            .and_then(|count| count.checked_add(6))
            .map(|count| count / 7)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(WorkError::Overflow)?;
        let bytes = u64::try_from(
            Layout::array::<Option<(ObjectId, &T)>>(slots)
                .map_err(|_| WorkError::Overflow)?
                .size(),
        )
        .map_err(|_| WorkError::Overflow)?;
        let mut allocation = work.claim_owned(bytes, 1)?;
        let mut backing = Vec::new();
        if let Err(error) = backing.try_reserve_exact(slots) {
            work.release_owned(&mut allocation, bytes)?;
            return Err(ObjectStoreError::MemoryTableAllocationFailed(error).into());
        }
        backing.resize_with(slots, || None);
        Ok(Self {
            slots: backing,
            hashes: hash_state(),
            allocation,
            bytes,
        })
    }

    pub(super) fn insert(
        &mut self,
        id: ObjectId,
        value: &'a T,
        work: &mut KernelAccounting<'_>,
    ) -> Result<Option<&'a T>, crate::FsError> {
        self.insert_metered::<true>(id, value, work)
    }

    // Only the public-to-parent insert above can select the production path.
    // The private false instantiation restores the one-charge-per-insert bug.
    fn insert_metered<const METER: bool>(
        &mut self,
        id: ObjectId,
        value: &'a T,
        work: &mut KernelAccounting<'_>,
    ) -> Result<Option<&'a T>, crate::FsError> {
        if !METER {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        let mask = self.slots.len() - 1;
        let mut position = usize::try_from(self.hashes.hash_one(id) & mask as u64)
            .map_err(|_| WorkError::Overflow)?;
        for _ in 0..self.slots.len() {
            if METER {
                work.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            match &mut self.slots[position] {
                Some((found, first)) if *found == id => return Ok(Some(*first)),
                empty @ None => {
                    *empty = Some((id, value));
                    return Ok(None);
                }
                _ => position = (position + 1) & mask,
            }
        }
        Err(WorkError::Overflow.into())
    }

    pub(super) fn release(self, work: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        let Self {
            slots,
            allocation,
            bytes,
            ..
        } = self;
        drop(slots);
        release(work, allocation, bytes)
    }
}

#[cfg(test)]
#[cfg(all(feature = "memory", not(target_arch = "wasm32")))]
#[path = "index_tests.rs"]
mod tests;

#[cfg(test)]
#[cfg(feature = "memory")]
#[path = "index_properties.rs"]
mod properties;
