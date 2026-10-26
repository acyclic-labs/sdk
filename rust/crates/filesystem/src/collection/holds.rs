use super::*;

#[derive(Default)]
pub(super) struct Holds {
    pub(super) slots: Vec<Slot>,
    pub(super) free: Option<usize>,
}

pub(super) enum Slot {
    Occupied(Held),
    Free(Option<usize>),
}

impl Collection {
    pub(super) fn register_with_accounting(
        self: &Arc<Self>,
        held: Held,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Hold, crate::FsError> {
        accounting.check()?;
        let mut holds = self.holds();
        if holds.free.is_none() && holds.slots.len() == holds.slots.capacity() {
            let old_capacity = holds.slots.capacity();
            let next_capacity = old_capacity
                .checked_mul(2)
                .map(|capacity| capacity.max(1))
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            let old_bytes =
                crate::foundation::usize_to_u64(old_capacity * std::mem::size_of::<Slot>());
            let new_bytes = next_capacity
                .checked_mul(std::mem::size_of::<Slot>())
                .map(crate::foundation::usize_to_u64)
                .ok_or(crate::FsError::Work(crate::WorkError::Overflow))?;
            // Native work borrows the existing cache backing.
            let old_overlap_bytes = if accounting.standalone_parts().is_some() {
                old_bytes
            } else {
                0
            };
            let copied_bytes =
                crate::foundation::usize_to_u64(holds.slots.len() * std::mem::size_of::<Slot>());
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
            let actual = crate::foundation::usize_to_u64(
                replacement.0.capacity() * std::mem::size_of::<Slot>(),
            );
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
            // Only the occupied-slot quota stands on a handle.
            accounting.release_owned(&mut replacement.1, actual)?;
            accounting.record_completed(WorkCounters {
                bytes_copied: copied_bytes,
                ..WorkCounters::UNCHARGED
            })?;
        }
        let slot_quota = crate::kernel::allocation::retained::RetainedAllocation::claim(
            crate::foundation::usize_to_u64(std::mem::size_of::<Slot>()),
            0,
            accounting,
        )?;
        let id = if let Some(id) = holds.free {
            let slot = holds
                .slots
                .get_mut(id)
                .ok_or(crate::FsError::HandleAllocationFailed)?;
            let Slot::Free(next) = slot else {
                unreachable!("free list names a vacant slot");
            };
            let next = *next;
            *slot = Slot::Occupied(held);
            holds.free = next;
            id
        } else {
            let id = holds.slots.len();
            holds.slots.push(Slot::Occupied(held));
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

    /// Turn an admitted closure into a root pin before releasing its sweep
    /// barrier. An active collection already remembers the admitted objects;
    /// later collections see the same ordinary root registry.
    pub(crate) async fn hold_generation_with_accounting(
        self: &Arc<Self>,
        proof: &crate::kernel::GenerationProof,
        config: crate::model::VolumeConfig,
        proven_at: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Hold, crate::FsError> {
        let admitted = self.admit(&proof.objects, |_| false, proven_at).await?;
        let hold = self.register_with_accounting(
            Held::Generation {
                root: ObjectId {
                    kind: crate::ObjectKind::GenerationRoot,
                    digest: proof.generation_id.digest(),
                },
                config,
            },
            accounting,
        )?;
        drop(admitted);
        Ok(hold)
    }
}

/// An open handle's registration; dropping it releases what it holds.
pub(crate) struct Hold {
    pub(super) collection: Arc<Collection>,
    pub(super) id: usize,
    // Last: Drop removes the actual held payload before this exclusive quota.
    _slot_quota: crate::kernel::allocation::retained::RetainedAllocation,
}

impl Hold {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        let holds = self.collection.holds();
        let Slot::Occupied(held) = &holds.slots[self.id] else {
            unreachable!("live hold remains registered until drop");
        };
        held_owned_bytes(held)
    }

    pub(super) fn update(&self, held: Held) {
        let mut holds = self.collection.holds();
        if let Some(slot) = holds.slots.get_mut(self.id) {
            *slot = Slot::Occupied(held);
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut holds = self.collection.holds();
        let next = holds.free;
        if let Some(slot) = holds.slots.get_mut(self.id) {
            *slot = Slot::Free(next);
            holds.free = Some(self.id);
        }
    }
}
