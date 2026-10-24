use super::*;
use crate::access::CompiledPolicy;
use crate::sync::{Arc as SharedArc, Mutex as SharedMutex};

const SLOTS: usize = 16;
type Revision = crate::kernel::MetadataField<ObjectId>;

struct Entry {
    volume: VolumeId,
    revision: Revision,
    view: Arc<CompiledPolicy>,
    used: u64,
}

struct Memory {
    maximum: u64,
    live: SharedMutex<u64>,
}

pub(crate) struct CacheCharge {
    memory: SharedArc<Memory>,
    bytes: u64,
}

impl Drop for CacheCharge {
    fn drop(&mut self) {
        if let Ok(mut live) = self.memory.live.lock() {
            *live -= self.bytes;
        }
    }
}

struct State {
    entries: [Option<Entry>; SLOTS],
    clock: u64,
    memory: Option<SharedArc<Memory>>,
}

/// A byte-bounded LRU shared by every workspace acquired from this Fs. A removed
/// entry's charge lives until its last view drops, so eviction cannot manufacture
/// memory credit while an open handle still uses the policy.
pub(super) struct PolicyCache {
    state: SharedMutex<State>,
    pub(super) compilation: SharedMutex<()>,
}

impl Default for PolicyCache {
    fn default() -> Self {
        Self {
            state: SharedMutex::new(State {
                entries: std::array::from_fn(|_| None),
                clock: 0,
                memory: None,
            }),
            compilation: SharedMutex::new(()),
        }
    }
}

impl PolicyCache {
    pub(super) fn get(&self, volume: VolumeId, revision: Revision) -> Option<Arc<CompiledPolicy>> {
        let mut state = self.state.lock().ok()?;
        // Renormalize without changing order before the counter can wrap.
        if state.clock == u64::MAX {
            state
                .entries
                .sort_by_key(|entry| entry.as_ref().map(|entry| entry.used));
            for (rank, entry) in state.entries.iter_mut().enumerate() {
                if let Some(entry) = entry {
                    entry.used = rank as u64;
                }
            }
            state.clock = SLOTS as u64;
        }
        state.clock += 1;
        let clock = state.clock;
        state
            .entries
            .iter_mut()
            .flatten()
            .find(|entry| entry.volume == volume && entry.revision == revision)
            .map(|entry| {
                entry.used = clock;
                Arc::clone(&entry.view)
            })
    }

    pub(super) fn initialize(&self, capacity: u64) -> Result<(), FsError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FsError::InvalidAccessPolicy)?;
        if state.memory.is_none() {
            let header = Self::accounting_bytes()?;
            let maximum = capacity
                .checked_sub(header)
                .ok_or(FsError::AccessPolicyCacheCapacity)?;
            state.memory = Some(SharedArc::new(Memory {
                maximum,
                live: SharedMutex::new(0),
            }));
        }
        Ok(())
    }

    pub(super) fn accounting_bytes() -> Result<u64, FsError> {
        crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Memory>())
            .map(|bytes| (bytes + size_of::<Self>()) as u64)
            .map_err(|_| FsError::AccessPolicyCacheCapacity)
    }

    pub(super) fn charge(&self, bytes: u64) -> Result<CacheCharge, FsError> {
        let memory = {
            let state = self
                .state
                .lock()
                .map_err(|_| FsError::InvalidAccessPolicy)?;
            SharedArc::clone(
                state
                    .memory
                    .as_ref()
                    .ok_or(FsError::AccessPolicyCacheCapacity)?,
            )
        };
        if bytes > memory.maximum {
            return Err(FsError::AccessPolicyCacheCapacity);
        }
        loop {
            {
                let mut live = memory
                    .live
                    .lock()
                    .map_err(|_| FsError::InvalidAccessPolicy)?;
                if bytes <= memory.maximum.saturating_sub(*live) {
                    *live += bytes;
                    let charge = CacheCharge {
                        memory: SharedArc::clone(&memory),
                        bytes,
                    };
                    return Ok(charge);
                }
            }
            let retired = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| FsError::InvalidAccessPolicy)?;
                let oldest = state
                    .entries
                    .iter()
                    .enumerate()
                    .filter_map(|(index, entry)| entry.as_ref().map(|entry| (index, entry.used)))
                    .min_by_key(|(_, used)| *used);
                oldest.and_then(|(index, _)| state.entries.get_mut(index).and_then(Option::take))
            };
            if retired.is_none() {
                return Err(FsError::AccessPolicyCacheCapacity);
            }
            // Destruction releases the cache's own byte credit. No cache lock
            // is held while retiring the evicted program's original storage.
            drop(retired);
        }
    }

    pub(super) fn insert(
        &self,
        volume: VolumeId,
        revision: Revision,
        view: Arc<CompiledPolicy>,
    ) -> Result<(), FsError> {
        let retired = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| FsError::InvalidAccessPolicy)?;
            let index = state
                .entries
                .iter()
                .position(Option::is_none)
                .unwrap_or_else(|| {
                    state
                        .entries
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, entry)| entry.as_ref().map(|entry| entry.used))
                        .map_or(0, |(index, _)| index)
                });
            let used = state.clock;
            state
                .entries
                .get_mut(index)
                .ok_or(FsError::InvalidAccessPolicy)?
                .replace(Entry {
                    volume,
                    revision,
                    view,
                    used,
                })
        };
        drop(retired);
        Ok(())
    }
}

#[cfg(all(test, acyclic_fs_loom))]
mod loom_tests;
#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests;
