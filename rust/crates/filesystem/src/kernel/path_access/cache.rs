use super::*;

struct CachedObject {
    object_id: ObjectId,
    bytes: OwnedBytes,
}
pub(super) struct CacheState {
    slots: ReadOwned<Vec<Option<CachedObject>>>,

    entry_count: usize,
    pub(super) retained_owned_bytes: u64,
    /// Standalone slots already present in the caller's allocation ledger.
    pub(super) accounted_bytes: u64,
    pub(super) accounted_slot_bytes: u64,
}
pub(super) struct OperationReadCache<'a, S> {
    pub(super) store: &'a S,
    // Operation cache (45) follows any held view state (20) and precedes the
    // original demand leaf (50).
    // Keep one guard across lookup, optional rehash and insertion.
    pub(super) state: Mutex<CacheState>,
    maximum_entries: usize,
    slot_limit: usize,
    pub(super) data_custody: Option<&'a crate::kernel::ProofDataCustody>,
}
const INITIAL_CACHE_SLOTS: usize = 16;
impl<'a, S> OperationReadCache<'a, S> {
    pub(super) fn new(
        store: &'a S,
        maximum_entries: usize,
        accounting: &mut KernelAccounting<'_>,
        data_custody: Option<&'a crate::kernel::ProofDataCustody>,
    ) -> Result<Self, crate::FsError> {
        let slot_limit = maximum_entries
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(WorkError::Overflow)?
            .max(1);
        usize_to_u64(slot_limit)
            .checked_mul(size_of::<Option<CachedObject>>() as u64)
            .ok_or(WorkError::Overflow)?;
        let mut slots = reserve_slots::<Option<CachedObject>>(
            slot_limit.min(INITIAL_CACHE_SLOTS),
            accounting,
            data_custody,
        )?;
        let bytes = usize_to_u64(slots.value.capacity())
            .checked_mul(size_of::<Option<CachedObject>>() as u64)
            .ok_or(WorkError::Overflow)?;
        slots
            .value
            .resize_with(slot_limit.min(INITIAL_CACHE_SLOTS), || None);
        if let Some(owner) = data_custody {
            owner.add_operation_cache_bytes(bytes)?;
        }
        Ok(Self {
            store,
            state: Mutex::new(CacheState {
                slots,
                entry_count: 0,
                retained_owned_bytes: 0,
                accounted_bytes: bytes,
                accounted_slot_bytes: bytes,
            }),
            maximum_entries,
            slot_limit,
            data_custody,
        })
    }
    #[track_caller]
    pub(super) fn lock_state(&self) -> MutexGuard<'_, CacheState> {
        crate::lock_order::lock::<_, { crate::lock_order::OPERATION_CACHE }>(&self.state)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn slot_bytes(state: &CacheState) -> Result<u64, WorkError> {
        usize_to_u64(state.slots.value.capacity())
            .checked_mul(size_of::<Option<CachedObject>>() as u64)
            .ok_or(WorkError::Overflow)
    }
    pub(super) fn extra_resident(&self) -> Result<u64, ObjectStoreError> {
        let state = self.lock_state();
        Self::slot_bytes(&state)?
            .checked_add(state.retained_owned_bytes)
            .and_then(|bytes| bytes.checked_sub(state.accounted_bytes))
            .ok_or(WorkError::Overflow.into())
    }
    pub(super) fn probe(
        &self,
        object_id: ObjectId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<ObjectRead>, crate::FsError> {
        let state = self.lock_state();
        Ok(probe_slots(&state.slots.value, object_id, accounting, self.data_custody)?.0)
    }
    pub(super) fn admit(
        &self,
        object_id: ObjectId,
        bytes: &OwnedBytes,
        owned_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, crate::FsError> {
        self.admit_with_slot::<true>(object_id, bytes, owned_bytes, accounting)
    }
    fn admit_with_slot<const REPROBE_GROWTH: bool>(
        &self,
        object_id: ObjectId,
        bytes: &OwnedBytes,
        owned_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, crate::FsError> {
        self.admit_with_rehash::<REPROBE_GROWTH, true>(object_id, bytes, owned_bytes, accounting)
    }
    fn admit_with_rehash<const REPROBE_GROWTH: bool, const CHARGE_REHASH: bool>(
        &self,
        object_id: ObjectId,
        bytes: &OwnedBytes,
        owned_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, crate::FsError> {
        let mut state = self.lock_state();
        if state.entry_count >= self.maximum_entries {
            return Ok(false);
        }
        let (existing, mut vacant) = probe_slots(&state.slots.value, object_id, accounting, None)?;
        if existing.is_some() {
            return Ok(false);
        }
        if state.entry_count.saturating_mul(2) >= state.slots.value.len()
            && state.slots.value.len() < self.slot_limit
        {
            let length = state
                .slots
                .value
                .len()
                .checked_mul(2)
                .ok_or(WorkError::Overflow)?
                .min(self.slot_limit);
            let mut grown =
                reserve_slots::<Option<CachedObject>>(length, accounting, self.data_custody)?;
            grown.value.resize_with(length, || None);
            let old_bytes = Self::slot_bytes(&state)?;
            // Charge all rehash probes before moving each old entry. On failure,
            // the remaining original slots still own their untouched payloads.
            for entry in state.slots.value.iter_mut() {
                if let Some(value) = entry.as_ref() {
                    let vacant = probe_slots_with_charge::<CHARGE_REHASH>(
                        &grown.value,
                        value.object_id,
                        accounting,
                        None,
                    )?
                    .1;
                    *grown
                        .value
                        .get_mut(vacant)
                        .ok_or(ObjectStoreError::Corrupt)? = entry.take();
                }
            }
            let old = std::mem::replace(&mut state.slots, grown);
            drop(old);
            release_standalone(
                accounting,
                old_bytes.saturating_sub(state.accounted_slot_bytes),
            )?;
            if let Some(owner) = self.data_custody {
                owner.add_operation_cache_bytes(
                    Self::slot_bytes(&state)?
                        .checked_sub(old_bytes)
                        .ok_or(WorkError::Overflow)?,
                )?;
            }
            if REPROBE_GROWTH {
                vacant = probe_slots(&state.slots.value, object_id, accounting, None)?.1;
            }
        }
        *state
            .slots
            .value
            .get_mut(vacant)
            .ok_or(ObjectStoreError::Corrupt)? = Some(CachedObject {
            object_id,
            bytes: bytes.clone(),
        });
        state.entry_count += 1;
        state.retained_owned_bytes = state
            .retained_owned_bytes
            .checked_add(owned_bytes)
            .ok_or(WorkError::Overflow)?;
        if let Some(owner) = self.data_custody {
            owner.add_operation_cache_bytes(owned_bytes)?;
        }
        Ok(true)
    }
    pub(super) fn reconcile(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => Ok(()),
            _ => {
                let mut state = self.lock_state();
                let actual = Self::slot_bytes(&state)?
                    .checked_add(state.retained_owned_bytes)
                    .ok_or(WorkError::Overflow)?;
                // Growth's standalone callback releases its own old-slot claim;
                // the caller retains the old baseline until this short leaf.
                let (allocations, work, budget, _) = accounting
                    .standalone_parts()
                    .ok_or(crate::ObjectStoreError::Corrupt)?;
                if actual >= state.accounted_bytes {
                    allocations
                        .record_owned_bytes(actual - state.accounted_bytes, work)
                        .map_err(|error| {
                            error.into_work_or(crate::FsError::HandleAllocationFailed)
                        })?;
                } else {
                    allocations
                        .release(state.accounted_bytes - actual)
                        .map_err(|error| {
                            error.into_work_or(crate::FsError::HandleAllocationFailed)
                        })?;
                }
                work.verify(budget)?;
                state.accounted_bytes = actual;
                state.accounted_slot_bytes = Self::slot_bytes(&state)?;
                Ok(())
            }
        }
    }
}
impl<S> Drop for OperationReadCache<'_, S> {
    #[allow(
        clippy::expect_used,
        reason = "Every cached capacity and payload is counted before it becomes resident."
    )]
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bytes = Self::slot_bytes(state)
            .and_then(|slots| {
                slots
                    .checked_add(state.retained_owned_bytes)
                    .ok_or(WorkError::Overflow)
            })
            .expect("cache residency was admitted before retaining each payload");
        drop(std::mem::take(&mut state.slots.value));
        if let Some(owner) = self.data_custody {
            owner.release_operation_cache_bytes(bytes);
        }
    }
}
fn probe_slots(
    slots: &[Option<CachedObject>],
    object_id: ObjectId,
    accounting: &mut KernelAccounting<'_>,
    custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(Option<ObjectRead>, usize), crate::FsError> {
    probe_slots_with_charge::<true>(slots, object_id, accounting, custody)
}
fn probe_slots_with_charge<const CHARGE: bool>(
    slots: &[Option<CachedObject>],
    object_id: ObjectId,
    accounting: &mut KernelAccounting<'_>,
    custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<(Option<ObjectRead>, usize), crate::FsError> {
    let mask = slots
        .len()
        .checked_sub(1)
        .ok_or(ObjectStoreError::Corrupt)?;
    let mut index = object_hash(object_id) & mask;
    for _ in 0..slots.len() {
        let slot = slots.get(index).ok_or(ObjectStoreError::Corrupt)?;
        let examine = || {
            // Observe the actual inspection, including deliberately uncharged
            // probes, after admission and independently of accounting.
            #[cfg(all(test, not(acyclic_fs_loom)))]
            tests::record_slot_access();
            match slot {
                Some(entry) if entry.object_id == object_id => (
                    Some(ObjectRead {
                        bytes: entry.bytes.clone(),
                        retention: ObjectReadRetention::Shared,
                    }),
                    index,
                    false,
                ),
                Some(_) => (None, (index + 1) & mask, false),
                None => (None, index, true),
            }
        };
        let (found, next, vacant) = if CHARGE {
            examine_item(accounting, custody, examine)?
        } else {
            examine()
        };
        if found.is_some() || vacant {
            return Ok((found, index));
        }
        index = next;
    }
    Err(ObjectStoreError::Corrupt.into())
}
pub(super) fn object_hash(object_id: ObjectId) -> usize {
    let mut hash = usize::from(object_id.kind.canonical_tag());
    for byte in object_id.digest.as_bytes() {
        hash ^= usize::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}
pub(super) fn compare_object_id(left: &ObjectId, right: &ObjectId) -> Ordering {
    left.kind
        .canonical_tag()
        .cmp(&right.kind.canonical_tag())
        .then_with(|| left.digest.cmp(&right.digest))
}

#[cfg(test)]
#[cfg(acyclic_fs_loom)]
mod loom_tests;
#[cfg(test)]
#[cfg(not(acyclic_fs_loom))]
mod tests;
