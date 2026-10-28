use crate::StreamError;
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
use crate::change::{
    Change, Subscription,
    sync::{Arc, Mutex},
};
use std::collections::{BTreeMap, btree_map::Entry};
#[cfg(not(any(all(feature = "wasm", target_arch = "wasm32"), loom)))]
use std::sync::{Arc, Mutex};

struct Registration<V> {
    value: V,
    count: usize,
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    changed: Change,
}
#[derive(Clone)]
pub(crate) struct Interests<K, V = ()> {
    // Provider -> registry is the registration order. Registry code never
    // acquires a provider or event lock; publish and destroy outside this lock.
    entries: Arc<Mutex<BTreeMap<K, Registration<V>>>>,
}
pub(crate) struct Interest<K: Ord + Clone, V: Clone = ()> {
    registry: Interests<K, V>,
    key: K,
    value: V,
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    changed: Option<Subscription>,
}
impl<K: Ord + Clone, V: Clone> Default for Interests<K, V> {
    fn default() -> Self {
        Self::new()
    }
}
impl<K: Ord + Clone, V: Clone> Interests<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub(crate) fn subscribe_with(
        &self,
        key: K,
        create: impl FnOnce() -> V,
    ) -> Result<Interest<K, V>, StreamError> {
        let value = {
            let mut entries = self.entries.lock().map_err(|_| StreamError::Unavailable)?;
            let entry = match entries.entry(key.clone()) {
                Entry::Vacant(entry) => entry.insert(Registration {
                    value: create(),
                    count: 0,
                    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
                    changed: Change::new(),
                }),
                Entry::Occupied(entry) => entry.into_mut(),
            };
            entry.count = entry.count.checked_add(1).ok_or(StreamError::Capacity)?;
            entry.value.clone()
        };
        let interest = Interest {
            registry: self.clone(),
            key,
            value,
            #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
            changed: None,
        };
        #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
        let interest = {
            let mut interest = interest;
            let changed = self
                .entries
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .get(&interest.key)
                .ok_or(StreamError::Unavailable)?
                .changed
                .clone();
            interest.changed = Some(changed.subscribe()?);
            interest
        };
        Ok(interest)
    }
    pub(crate) fn get(&self, key: &K) -> Result<Option<V>, StreamError> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .get(key)
            .map(|entry| entry.value.clone()))
    }
    /// Snapshot active subscriptions only, never the persisted namespace.
    pub(crate) fn keys_in<Q: Ord + ?Sized, R: std::ops::RangeBounds<Q>>(
        &self,
        range: R,
    ) -> Result<Vec<K>, StreamError>
    where
        K: std::borrow::Borrow<Q>,
    {
        Ok(self
            .entries
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .range::<Q, _>(range)
            .map(|(key, _)| key.clone())
            .collect())
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    pub(crate) fn notify(&self, key: &K) -> Result<(), StreamError> {
        let changed = self
            .entries
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .get(key)
            .map(|entry| entry.changed.clone());
        if let Some(changed) = changed {
            changed.notify()?;
        }
        Ok(())
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
    pub(crate) fn notify_all(&self) -> Result<(), StreamError> {
        let changed = self
            .entries
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .values()
            .map(|entry| entry.changed.clone())
            .collect::<Vec<_>>();
        for changed in changed {
            changed.notify()?;
        }
        Ok(())
    }
}
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
impl<K: Ord + Clone> Interests<K> {
    pub(crate) fn subscribe(&self, key: K) -> Result<Interest<K>, StreamError> {
        self.subscribe_with(key, || ())
    }
}
impl<K: Ord + Clone, V: Clone> Interest<K, V> {
    pub(crate) fn value(&self) -> &V {
        &self.value
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
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
impl<K: Ord + Clone, V: Clone> Drop for Interest<K, V> {
    fn drop(&mut self) {
        #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
        drop(self.changed.take());
        let removed = if let Ok(mut entries) = self.registry.entries.lock() {
            if let Some(entry) = entries.get_mut(&self.key) {
                entry.count -= 1;
                if entry.count == 0 {
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

#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
pub(crate) struct Refresh {
    // One lock, never nested with the registry, Change or provider cache.
    state: Mutex<RefreshState>,
}
#[derive(Default)]
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
struct RefreshState {
    running: bool,
    dirty: bool,
    force: bool,
}
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
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
#[cfg(loom)]
#[path = "../publication-models/interest.rs"]
mod models;
