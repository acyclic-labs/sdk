use crate::{
    StreamError,
    change::{
        Change, Subscription,
        sync::{Arc, Mutex},
    },
};
use std::collections::{BTreeMap, btree_map::Entry};

#[derive(Clone)]
pub(crate) struct Interests<K> {
    // Lock order: registry, refresh and Change locks never nest. In particular,
    // subscription creation/destruction and all wakers run outside this lock.
    entries: Arc<Mutex<BTreeMap<K, (Change, usize)>>>,
}
pub(crate) struct Interest<K: Ord + Clone> {
    registry: Interests<K>,
    key: K,
    changed: Option<Subscription>,
}
impl<K: Ord + Clone> Interests<K> {
    pub(crate) fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub(crate) fn subscribe(&self, key: K) -> Result<Interest<K>, StreamError> {
        let changed = {
            let mut entries = self.entries.lock().map_err(|_| StreamError::Unavailable)?;
            match entries.entry(key.clone()) {
                Entry::Vacant(entry) => entry.insert((Change::new(), 1)).0.clone(),
                Entry::Occupied(mut entry) => {
                    let (changed, count) = entry.get_mut();
                    *count = count.checked_add(1).ok_or(StreamError::Capacity)?;
                    changed.clone()
                }
            }
        };
        let mut interest = Interest {
            registry: self.clone(),
            key,
            changed: None,
        };
        // The guard above has dropped; failure also releases the reservation.
        interest.changed = Some(changed.subscribe()?);
        Ok(interest)
    }
    pub(crate) fn notify(&self, key: &K) -> Result<(), StreamError> {
        let changed = self
            .entries
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .get(key)
            .map(|(changed, _)| changed.clone());
        if let Some(changed) = changed {
            changed.notify()?;
        }
        Ok(())
    }
    pub(crate) fn notify_all(&self) -> Result<(), StreamError> {
        let changed = self
            .entries
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .values()
            .map(|(changed, _)| changed.clone())
            .collect::<Vec<_>>();
        for changed in changed {
            changed.notify()?;
        }
        Ok(())
    }
}
impl<K: Ord + Clone> Interest<K> {
    pub(crate) async fn next<T>(
        &mut self,
        snapshot: impl AsyncFnMut() -> Result<Option<T>, StreamError>,
    ) -> Result<T, StreamError> {
        self.changed
            .as_mut()
            .ok_or(StreamError::Unavailable)?
            .next(snapshot)
            .await
    }
}
impl<K: Ord + Clone> Drop for Interest<K> {
    fn drop(&mut self) {
        drop(self.changed.take());
        let removed = if let Ok(mut entries) = self.registry.entries.lock() {
            if let Some((_, count)) = entries.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    entries.remove(&self.key)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        drop(removed);
    }
}

pub(crate) struct Refresh {
    // One lock, never nested with the registry, Change or provider cache.
    state: Mutex<RefreshState>,
}
#[derive(Default)]
struct RefreshState {
    running: bool,
    dirty: bool,
    force: bool,
}
impl Refresh {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(RefreshState::default()),
        }
    }
    pub(crate) fn request(&self, force: bool) -> Result<bool, StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        state.dirty = true;
        state.force |= force;
        let start = !state.running;
        state.running = true;
        Ok(start)
    }
    pub(crate) fn take(&self) -> Result<Option<bool>, StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        if !state.dirty {
            state.running = false;
            return Ok(None);
        }
        state.dirty = false;
        Ok(Some(std::mem::take(&mut state.force)))
    }
}

#[cfg(test)]
#[cfg(acyclic_stream_loom)]
#[path = "../publication-models/interest.rs"]
mod models;
