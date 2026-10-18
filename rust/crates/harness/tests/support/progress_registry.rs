//! Runner-owned scenario registrations. This is also the code included by the Loom model.
#[cfg(acyclic_e2e_progress_loom)]
use loom::sync::Mutex;
#[cfg(not(acyclic_e2e_progress_loom))]
use std::sync::Mutex;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    task::{Wake, Waker},
};

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
        let mut state = self.state.lock().unwrap();
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
        let state = self.state.lock().unwrap();
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
        let state = self.state.lock().unwrap();
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
        let mut state = self.registry.state.lock().unwrap();
        let entry = state.open.get_mut(&self.id).expect("retired registration");
        let previous = std::mem::replace(&mut entry.label, label);
        state.revision += 1;
        Waiting {
            registry: self.registry.clone(),
            id: self.id,
            previous,
        }
    }

    pub fn polling(&self) -> Polling<'_> {
        let mut state = self.registry.state.lock().unwrap();
        let entry = state.open.get_mut(&self.id).expect("retired registration");
        assert!(!entry.polling);
        entry.ready = false;
        entry.polling = true;
        state.revision += 1;
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
        let mut state = self.registry.state.lock().unwrap();
        if let Some(entry) = state.open.get_mut(&self.id) {
            entry.label = self.previous;
            state.revision += 1;
        }
    }
}

pub struct Polling<'a>(&'a Registration);
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        let mut state = self.0.registry.state.lock().unwrap();
        state
            .open
            .get_mut(&self.0.id)
            .expect("retired registration")
            .polling = false;
        // Preserve a wake racing with this poll's return or panic.
        state.revision += 1;
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
            let mut state = self.registry.state.lock().unwrap();
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
        let mut state = self.registry.state.lock().unwrap();
        assert!(state.open.remove(&self.id).is_some());
        state.revision += 1;
    }
}
