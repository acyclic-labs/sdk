//! Runner-owned scenario registrations. This is also the code included by the Loom model.
#[cfg(acyclic_e2e_progress_loom)]
use loom::sync::Mutex;
#[cfg(not(acyclic_e2e_progress_loom))]
use std::sync::Mutex;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Default)]
struct State {
    next: usize,
    revision: usize,
    open: BTreeMap<usize, (PathBuf, &'static str)>,
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
        assert!(state.open.insert(id, (path.into(), "admitted")).is_none());
        Registration {
            registry: self.clone(),
            id,
        }
    }

    pub fn snapshot(&self) -> (usize, Vec<(PathBuf, &'static str)>) {
        let state = self.state.lock().unwrap();
        (state.revision, state.open.values().cloned().collect())
    }
}

impl Registration {
    pub fn waiting(&self, label: &'static str) {
        let mut state = self.registry.state.lock().unwrap();
        state
            .open
            .get_mut(&self.id)
            .expect("retired registration")
            .1 = label;
        state.revision += 1;
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self.registry.state.lock().unwrap();
        assert!(state.open.remove(&self.id).is_some());
        state.revision += 1;
    }
}
