//! Keep live actor identities exact; bound only the idle retention cache.
use super::*;

const REMEMBERED_VOLUMES: usize = 256;

struct Entries {
    live: FixedIndex<RetainedWeak<Volume>, 32>,
    idle: [Option<RetainedArc<Volume>>; REMEMBERED_VOLUMES],
}

struct Registry {
    entries: RetainedArc<Mutex<Entries>>,
    _allocation: RetainedAllocation,
}

pub(super) struct Registration {
    entries: RetainedWeak<Mutex<Entries>>,
    key: [u8; 32],
}

impl Registration {
    pub(super) fn retire(&self, volume: &Volume) {
        if let Some(entries) = self.entries.upgrade() {
            entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .live
                .retire_if(self.key, |current| std::ptr::eq(current.as_ptr(), volume));
        }
    }
}

impl Registry {
    fn new(accounting: &mut KernelAccounting<'_>) -> Result<Arc<Self>, crate::FsError> {
        let bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Self>())
            .map_err(|_| WorkError::Overflow)? as u64;
        let allocation = RetainedAllocation::claim(bytes, 1, accounting)?;
        Ok(Arc::new(Self {
            entries: RetainedArc::new(
                Mutex::new(Entries {
                    live: FixedIndex::default(),
                    idle: std::array::from_fn(|_| None),
                }),
                accounting,
            )?,
            _allocation: allocation,
        }))
    }

    fn actor(
        self: &Arc<Self>,
        key: [u8; 32],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<RetainedArc<Volume>, crate::FsError> {
        accounting.charge_items(key.len() as u64)?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let existing = entries
            .live
            .get(key, accounting)?
            .and_then(RetainedWeak::upgrade);
        let volume = match existing {
            Some(volume) => volume,
            None => {
                // An old owner's destructor may be waiting for this lock.
                // Replacing its dead weak entry is safe: retirement compares
                // the exact allocation, rather than just the reused key.
                let volume = Volume::new(accounting)?;
                let slot = entries
                    .live
                    .get_or_try_insert_with(key, accounting, |_| Ok(RetainedWeak::default()))?;
                *slot = volume.downgrade();
                volume
                    .registration
                    .set(Registration {
                        entries: self.entries.downgrade(),
                        key,
                    })
                    .unwrap_or_else(|_| unreachable!("a new volume registers once"));
                volume
            }
        };
        // This bucket chooses retention only. Identity lookup always uses the
        // full scoped key, including when arbitrarily many live actors collide.
        let bucket = key
            .iter()
            .fold(0_u8, |slot, byte| slot.rotate_left(1).wrapping_add(*byte))
            as usize;
        let retired = entries.idle[bucket].replace(volume.clone());
        drop(entries);
        // The last reference unregisters itself. Never run that destructor
        // while holding the registry lock, including a cache hit/replacement.
        drop(retired);
        Ok(volume)
    }
}

pub(crate) fn authority_actor(
    owner: &acyclic_stream::OperationClaims,
    scope: [u8; 16],
    key: AuthorityId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<RetainedArc<Volume>, crate::FsError> {
    // Shared owners retain separate actors for distinct provider placements.
    let mut slot_key = [0; 32];
    slot_key[..16].copy_from_slice(&scope);
    slot_key[16..].copy_from_slice(&key.into_bytes());
    let owned = owner.shared_resource(
        accounting,
        |accounting| accounting.charge_items(1),
        |bytes, accounting| RetainedAllocation::claim(bytes, 1, accounting),
        Registry::new,
    )?;
    owned.actor(slot_key, accounting)
}

#[cfg(test)]
mod tests;
