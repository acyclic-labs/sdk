use crate::StreamError;
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
use crate::change::{
    Change, Subscription,
    sync::{Arc, Mutex},
};
#[cfg(not(any(all(feature = "wasm", target_arch = "wasm32"), loom)))]
use crate::query_sync::{Arc, SyncMutex as Mutex};
use std::collections::{BTreeMap, btree_map::Entry};

struct Registration<V> {
    value: V,
    count: usize,
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    changed: Change,
}
#[derive(Clone)]
pub struct Interests<K, V = ()> {
    // Provider -> registry is the registration order. Registry code never
    // acquires a provider or event lock; publish and destroy outside this lock.
    entries: Arc<Mutex<BTreeMap<K, Registration<V>>>>,
}
pub struct Interest<K: Ord + Clone, V: Clone = ()> {
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
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub fn subscribe_with(
        &self,
        key: K,
        create: impl FnOnce() -> V,
    ) -> Result<Interest<K, V>, StreamError> {
        let value = {
            let mut entries = self.entries.lock()?;
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
                .lock()?
                .get(&interest.key)
                .ok_or_else(|| {
                    StreamError::unavailable(acyclic_auth::refusal::Cause::new(
                        "stream.interests",
                        "registration_missing",
                    ))
                })?
                .changed
                .clone();
            interest.changed = Some(changed.subscribe()?);
            interest
        };
        Ok(interest)
    }
    pub fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Result<Option<V>, StreamError>
    where
        K: std::borrow::Borrow<Q>,
    {
        Ok(self
            .entries
            .lock()?
            .get(key)
            .map(|entry| entry.value.clone()))
    }
    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub fn contains(&self, key: &K) -> Result<bool, StreamError> {
        Ok(self.entries.lock()?.contains_key(key))
    }
    /// Snapshot active subscriptions only, never the persisted namespace.
    pub fn keys_in<Q: Ord + ?Sized, R: std::ops::RangeBounds<Q>>(
        &self,
        range: R,
    ) -> Result<Vec<K>, StreamError>
    where
        K: std::borrow::Borrow<Q>,
    {
        Ok(self
            .entries
            .lock()?
            .range::<Q, _>(range)
            .map(|(key, _)| key.clone())
            .collect())
    }
    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub fn values(&self) -> Result<Vec<V>, StreamError> {
        self.snapshot(|entry| entry.value.clone())
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
    fn snapshot<T>(
        &self,
        project: impl FnMut(&Registration<V>) -> T,
    ) -> Result<Vec<T>, StreamError> {
        Ok(self.entries.lock()?.values().map(project).collect())
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    pub fn notify(&self, key: &K) -> Result<(), StreamError> {
        let changed = self
            .entries
            .lock()?
            .get(key)
            .map(|entry| entry.changed.clone());
        if let Some(changed) = changed {
            changed.notify()?;
        }
        Ok(())
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
    pub fn notify_all(&self) -> Result<(), StreamError> {
        for changed in self.snapshot(|entry| entry.changed.clone())? {
            changed.notify()?;
        }
        Ok(())
    }
}
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
impl<K: Ord + Clone> Interests<K> {
    pub fn subscribe(&self, key: K) -> Result<Interest<K>, StreamError> {
        self.subscribe_with(key, || ())
    }
}
impl<K: Ord + Clone, V: Clone> Interest<K, V> {
    pub fn value(&self) -> &V {
        &self.value
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    pub async fn next<T>(
        &mut self,
        snapshot: impl AsyncFnMut() -> Result<Option<T>, StreamError>,
    ) -> Result<T, StreamError> {
        self.changed
            .as_mut()
            .ok_or_else(|| {
                StreamError::unavailable(acyclic_auth::refusal::Cause::new(
                    "stream.interests",
                    "subscription_missing",
                ))
            })?
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

/// Coalescing wake for a wasm follow's refresh loop.
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
pub struct Refresh {
    // One lock, never nested with the registry, Change or provider cache.
    state: Mutex<RefreshState>,
}
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), all(loom, test)))]
impl Default for Refresh {
    fn default() -> Self {
        Self::new()
    }
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
    /// A refresh with no pending request.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(RefreshState::default()),
        }
    }
    /// Records a refresh request; returns whether the caller must start the loop.
    pub fn request(&self, force: bool) -> Result<bool, StreamError> {
        let mut state = self.state.lock()?;
        state.dirty = true;
        state.force |= force;
        let start = !state.running;
        state.running = true;
        Ok(start)
    }
    pub fn take(&self) -> Result<Option<bool>, StreamError> {
        let mut state = self.state.lock()?;
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
#[path = "../../stream/publication-models/interest.rs"]
mod models;

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn failed_registration_preserves_poison_attribution_for_every_reader() {
        let registry = Interests::<u8>::new();
        assert!(registry.get(&1).unwrap().is_none());
        // Run the original value constructor under the production registry lock.
        // Its unwind poisons that lock; no injected replacement error is involved.
        assert!(
            std::panic::catch_unwind(|| {
                let _ = registry.subscribe_with(1, || panic!("registration construction failed"));
            })
            .is_err()
        );
        let errors = [
            registry
                .subscribe_with(2, || ())
                .err()
                .expect("poisoned registration"),
            registry.get(&1).unwrap_err(),
            registry.keys_in::<u8, _>(..).unwrap_err(),
        ];
        for error in errors {
            assert_eq!(error.category(), crate::StreamCategory::Unavailable);
            assert!(error.uncertain());
            let cause = error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<acyclic_auth::refusal::Cause>()
                .unwrap();
            assert_eq!(cause.component, "stream.lock");
            assert_eq!(cause.reason, "poisoned");
        }
    }
}
