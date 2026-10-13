use super::*;

#[derive(Default)]
pub(super) struct Holds {
    pub(super) slots: Vec<Slot>,
    pub(super) free: Option<usize>,
}

pub(super) struct Slot {
    pub(super) held: Option<Held>,
    pub(super) next_free: Option<usize>,
}

impl Collection {
    pub(super) fn register_with_accounting(
        self: &Arc<Self>,
        held: Held,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Hold, crate::FsError> {
        accounting.check()?;
        let mut holds = self.holds.lock().unwrap_or_else(PoisonError::into_inner);
        if holds.free.is_none() && holds.slots.len() == holds.slots.capacity() {
            let old_capacity = holds.slots.capacity();
            let next_capacity = old_capacity
                .checked_mul(2)
                .map(|capacity| capacity.max(1))
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            let old_bytes = crate::foundation::usize_to_u64(
                old_capacity
                    .checked_mul(std::mem::size_of::<Slot>())
                    .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?,
            );
            let new_bytes = next_capacity
                .checked_mul(std::mem::size_of::<Slot>())
                .map(crate::foundation::usize_to_u64)
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            // Cached backing predates this invocation and is borrowed on the
            // native root. Only the replacement is allocated by this operation.
            #[cfg(not(target_arch = "wasm32"))]
            let old_overlap_bytes = if accounting.native_work().is_some() {
                0
            } else {
                old_bytes
            };
            #[cfg(target_arch = "wasm32")]
            let old_overlap_bytes = old_bytes;
            let copied_bytes = holds
                .slots
                .len()
                .checked_mul(std::mem::size_of::<Slot>())
                .map(crate::foundation::usize_to_u64)
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            let mut old_overlap = accounting.claim_owned(old_overlap_bytes, 0)?;
            let replacement_guard =
                match accounting.claim_owned(new_bytes, u64::from(new_bytes != 0)) {
                    Ok(guard) => guard,
                    Err(error) => {
                        accounting.release_owned(&mut old_overlap, old_overlap_bytes)?;
                        return Err(error);
                    }
                };
            let mut replacement = (Vec::new(), replacement_guard);
            if let Err(_error) = replacement.0.try_reserve_exact(next_capacity) {
                drop(replacement.0);
                accounting.release_owned(&mut replacement.1, new_bytes)?;
                accounting.release_owned(&mut old_overlap, old_overlap_bytes)?;
                #[cfg(not(target_arch = "wasm32"))]
                if accounting.native_work().is_some() {
                    return Err(crate::FsError::NativeKernel(
                        crate::NativeTreeMaterializationError::AllocationFailed(_error),
                    ));
                }
                return Err(crate::FsError::HandleAllocationFailed);
            }
            let actual = replacement
                .0
                .capacity()
                .checked_mul(std::mem::size_of::<Slot>())
                .map(crate::foundation::usize_to_u64)
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            let completion = accounting
                .reconcile_owned(&mut replacement.1, new_bytes, actual)
                .and_then(|()| {
                    accounting.admit(WorkCounters {
                        bytes_copied: copied_bytes,
                        ..WorkCounters::UNCHARGED
                    })
                });
            if let Err(error) = completion {
                drop(replacement.0);
                accounting.release_owned(&mut replacement.1, actual)?;
                accounting.release_owned(&mut old_overlap, old_overlap_bytes)?;
                return Err(error);
            }
            replacement.0.append(&mut holds.slots);
            let old = std::mem::replace(&mut holds.slots, replacement.0);
            drop(old);
            accounting.release_owned(&mut old_overlap, old_overlap_bytes)?;
            // Cached backing is global; only exclusive occupied-slot quota
            // remains standing on a handle, not a new physical custody tag.
            accounting.release_owned(&mut replacement.1, actual)?;
            accounting.record_completed(WorkCounters {
                bytes_copied: copied_bytes,
                ..WorkCounters::UNCHARGED
            })?;
        }
        let slot_quota = accounting.claim_owned(
            crate::foundation::usize_to_u64(std::mem::size_of::<Slot>()),
            0,
        )?;
        let id = if let Some(id) = holds.free {
            let slot = holds
                .slots
                .get_mut(id)
                .ok_or(crate::FsError::HandleAllocationFailed)?;
            let next = slot.next_free.take();
            slot.held = Some(held);
            holds.free = next;
            id
        } else {
            let id = holds.slots.len();
            holds.slots.push(Slot {
                held: Some(held),
                next_free: None,
            });
            id
        };
        Ok(Hold {
            collection: Arc::clone(self),
            id,
            _slot_quota: slot_quota,
        })
    }

    pub(crate) fn hold_record_with_accounting(
        collection: Option<&Arc<Self>>,
        record: crate::kernel::FileRecord,
        config: crate::model::VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Hold>, crate::FsError> {
        accounting.check()?;
        collection
            .map(|collection| {
                collection.register_with_accounting(Held::Record { record, config }, accounting)
            })
            .transpose()
    }
}

/// An open handle's registration; dropping it releases what it holds.
pub(crate) struct Hold {
    pub(super) collection: Arc<Collection>,
    pub(super) id: usize,
    // Last: Drop removes the actual held payload before this exclusive quota.
    _slot_quota: KernelOwnedAllocation,
}

impl Hold {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let holds = self
            .collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let held = holds
            .slots
            .get(self.id)
            .and_then(|slot| slot.held.as_ref())
            .expect("live hold remains registered until drop");
        held_owned_bytes(held)
    }

    pub(super) fn update(&self, held: Held) {
        let mut holds = self
            .collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = holds.slots.get_mut(self.id) {
            slot.held = Some(held);
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut holds = self
            .collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let next = holds.free;
        if let Some(slot) = holds.slots.get_mut(self.id) {
            slot.held = None;
            slot.next_free = next;
            holds.free = Some(self.id);
        }
    }
}
