//! Runner-owned scenario registrations. This is also the code included by the Loom model.
#[cfg(acyclic_e2e_progress_loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(acyclic_e2e_progress_loom))]
use std::sync::{Mutex, MutexGuard};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    task::{Wake, Waker},
};

/// The guarded state stays consistent across a panicking holder: each critical
/// section is a few plain field writes, so a poisoned lock is still current.
fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
struct State {
    next: usize,
    revision: usize,
    open: BTreeMap<usize, Entry>,
}

struct Entry {
    path: PathBuf,
    label: &'static str,
    ready: bool,
    polling: bool,
}

#[derive(Default)]
pub struct Registry {
    // Sole lock: never nest it with another lock or hold it across callbacks,
    // awaits, formatting, sends or wakes. The model instruments this exact lock.
    state: Mutex<State>,
}

pub struct Registration {
    registry: Arc<Registry>,
    id: usize,
}

impl Registry {
    pub fn register(self: &Arc<Self>, path: &Path) -> Registration {
        let mut state = lock(&self.state);
        let id = state.next;
        state.next += 1;
        state.revision += 1;
        assert!(
            state
                .open
                .insert(
                    id,
                    Entry {
                        path: path.into(),
                        label: "admitted",
                        ready: true,
                        polling: false,
                    }
                )
                .is_none()
        );
        Registration {
            registry: self.clone(),
            id,
        }
    }

    pub fn snapshot(&self) -> (usize, Vec<(PathBuf, &'static str)>) {
        let state = lock(&self.state);
        (
            state.revision,
            state
                .open
                .values()
                .map(|entry| (entry.path.clone(), entry.label))
                .collect(),
        )
    }

    pub fn blocked_snapshot(&self, revision: usize) -> Option<Vec<(PathBuf, &'static str)>> {
        let state = lock(&self.state);
        (state.revision == revision
            && state
                .open
                .values()
                .all(|entry| !entry.ready && !entry.polling))
        .then(|| {
            state
                .open
                .values()
                .map(|entry| (entry.path.clone(), entry.label))
                .collect()
        })
    }
}

impl Registration {
    pub fn waiting(&self, label: &'static str) -> Waiting {
        let mut state = lock(&self.registry.state);
        // An open entry outlives its sole Registration, which removes it on drop.
        let previous = state
            .open
            .get_mut(&self.id)
            .map_or(label, |entry| std::mem::replace(&mut entry.label, label));
        state.revision += 1;
        Waiting {
            registry: self.registry.clone(),
            id: self.id,
            previous,
        }
    }

    pub fn polling(&self) -> Polling<'_> {
        let mut state = lock(&self.registry.state);
        if let Some(entry) = state.open.get_mut(&self.id) {
            assert!(!entry.polling);
            entry.ready = false;
            entry.polling = true;
        }
        // A reporter may cause another poll of an unchanged Pending future.
        // Polling alone is not progress; only observed wakes and lifecycle
        // changes advance the idle revision.
        Polling(self)
    }

    pub fn observed_waker(&self, parent: &Waker) -> Waker {
        Waker::from(Arc::new(Ready {
            registry: self.registry.clone(),
            id: self.id,
            parent: parent.clone(),
        }))
    }
}

pub struct Waiting {
    registry: Arc<Registry>,
    id: usize,
    previous: &'static str,
}
impl Drop for Waiting {
    fn drop(&mut self) {
        let mut state = lock(&self.registry.state);
        if let Some(entry) = state.open.get_mut(&self.id) {
            entry.label = self.previous;
            state.revision += 1;
        }
    }
}

pub struct Polling<'a>(&'a Registration);
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.0.registry.state);
        if let Some(entry) = state.open.get_mut(&self.0.id) {
            entry.polling = false;
        }
        // Preserve readiness and its revision if a wake raced this return.
    }
}

struct Ready {
    registry: Arc<Registry>,
    id: usize,
    parent: Waker,
}
impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        {
            let mut state = lock(&self.registry.state);
            if let Some(entry) = state.open.get_mut(&self.id) {
                entry.ready = true;
                state.revision += 1;
            }
        }
        self.parent.wake_by_ref();
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = lock(&self.registry.state);
        assert!(state.open.remove(&self.id).is_some());
        state.revision += 1;
    }
}
