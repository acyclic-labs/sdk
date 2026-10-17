//! Cached task bindings serialize one command slot without serializing tasks.
use crate::sync::owned::{Arc, Mutex, OwnedMutexGuard, lock_owned};
use crate::{TaskId, sync::SyncMutex};
use std::collections::BTreeMap;

pub(crate) struct TaskBindings<T> {
    // Lock order: index -> clone task gate -> release index -> await gate.
    // A task gate may enter providers; providers never enter this index.
    // Entries retain only provider handles, for this LocalHarness's lifetime.
    index: SyncMutex<BTreeMap<TaskId, Arc<Mutex<T>>>>,
}

impl<T> Default for TaskBindings<T> {
    fn default() -> Self {
        Self {
            index: SyncMutex::new(BTreeMap::new()),
        }
    }
}

impl<T: Default> TaskBindings<T> {
    pub(crate) async fn lock(&self, task: TaskId) -> OwnedMutexGuard<T> {
        let gate = {
            let mut index = self.index.lock().unwrap();
            #[cfg(acyclic_harness_loom)]
            if model::uncached() {
                Arc::new(Mutex::new(T::default()))
            } else {
                Arc::clone(
                    index
                        .entry(task)
                        .or_insert_with(|| Arc::new(Mutex::new(T::default()))),
                )
            }
            #[cfg(not(acyclic_harness_loom))]
            Arc::clone(
                index
                    .entry(task)
                    .or_insert_with(|| Arc::new(Mutex::new(T::default()))),
            )
        };
        lock_owned(gate).await
    }
}

#[cfg(acyclic_harness_loom)]
mod model;
