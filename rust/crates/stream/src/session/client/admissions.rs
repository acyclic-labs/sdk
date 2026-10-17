use super::*;

struct Pending {
    node: String,
    progress: Arc<Progress>,
}
#[derive(Default)]
struct State {
    home: String,
    entries: BTreeMap<u64, Pending>,
}
pub(super) struct Admissions {
    // Lock order: never nested with cursor, signal, cache, or transport locks.
    // Only cancellation atomics are read while holding this lock; no awaits,
    // waker operations, tracing callbacks or provider calls occur under it.
    state: Mutex<State>,
}
impl Admissions {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }
    pub(super) fn insert(&self, id: u64, progress: Arc<Progress>) -> Result<(), StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        let node = state.home.clone();
        state.entries.insert(id, Pending { node, progress });
        Ok(())
    }
    pub(super) fn remove(&self, id: u64) -> Result<(), StreamError> {
        self.state
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .entries
            .remove(&id);
        Ok(())
    }
    pub(super) fn bind_home(&self, node: &str) -> Result<(), StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        let previous = std::mem::replace(&mut state.home, node.into());
        for entry in state
            .entries
            .values_mut()
            .filter(|entry| entry.node == previous)
        {
            entry.node = node.into();
        }
        Ok(())
    }
    pub(super) fn interested(&self, node: &str) -> bool {
        // Poisoning fails closed: an unverifiable admission cannot own a dial.
        self.state.lock().is_ok_and(|state| {
            state
                .entries
                .values()
                .any(|entry| entry.node == node && !entry.progress.cancelled())
        })
    }
}

#[cfg(acyclic_stream_loom)]
/// Home binding and admission registration serialize on the same real lock.
pub fn verify_admission_binding(broken: bool) {
    loom::model(move || {
        let admissions = Arc::new(Admissions::new());
        let registering = Arc::clone(&admissions);
        let insert =
            loom::thread::spawn(move || registering.insert(1, Arc::new(Progress::new(0))).unwrap());
        if broken {
            admissions.state.lock().unwrap().home = "node".into();
        } else {
            admissions.bind_home("node").unwrap();
        }
        insert.join().unwrap();
        assert!(
            admissions.interested("node"),
            "binding home must preserve each owned pending Open"
        );
        assert!(!admissions.interested(""));
        admissions.remove(1).unwrap();
        assert!(!admissions.interested("node"));
    });
}
