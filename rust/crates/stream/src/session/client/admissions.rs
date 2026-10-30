use super::*;

#[derive(Default)]
struct State {
    home: String,
    // Every queued Open follows the current home binding. Dispatch removes its
    // membership before installing a route; cancellation removes it before
    // marking progress cancelled. No per-entry node or cancellation scan is needed.
    entries: BTreeMap<u64, ()>,
}
pub(super) struct Admissions {
    // Lock order: never nested with cursor, signal, cache, or transport locks.
    // No awaits, waker operations, tracing callbacks or provider calls occur
    // under it.
    state: Mutex<State>,
}
impl Admissions {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }
    pub(super) fn insert(&self, id: u64) -> Result<(), StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        state.entries.insert(id, ());
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
    pub(super) fn cancel(&self, id: u64, progress: &Progress) {
        // Withdrawal precedes visible cancellation, including when an Open has
        // not reached the pool. Poisoning already makes interested fail closed.
        let _ = self.remove(id);
        progress.cancel();
    }
    pub(super) fn bind_home(&self, node: &str) -> Result<(), StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        state.home.clear();
        state.home.push_str(node);
        Ok(())
    }
    pub(super) fn interested(&self, node: &str) -> bool {
        // Poisoning fails closed: an unverifiable admission cannot own a dial.
        self.state
            .lock()
            .is_ok_and(|state| state.home == node && !state.entries.is_empty())
    }
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
#[path = "admissions/tests.rs"]
mod tests;

#[cfg(loom)]
/// Withdrawal must precede visible cancellation even while home is rebound.
pub fn verify_admission_cancellation(broken: bool) {
    loom::model(move || {
        let admissions = Arc::new(Admissions::new());
        let progress = Arc::new(Progress::new(0));
        admissions.insert(1).unwrap();
        let withdrawing = Arc::clone(&admissions);
        let cancelled = Arc::clone(&progress);
        let withdrawal = loom::thread::spawn(move || {
            if broken {
                cancelled.cancel();
                withdrawing.remove(1).unwrap();
            } else {
                withdrawing.cancel(1, &cancelled);
            }
        });
        admissions.bind_home("node").unwrap();
        if progress.cancelled() {
            assert!(
                !admissions.interested("node"),
                "cancelled pending Open must not own a dial"
            );
        }
        withdrawal.join().unwrap();
        assert!(!admissions.interested("node"));
    });
}

#[cfg(loom)]
/// Home binding and admission registration serialize on the same real lock.
pub fn verify_admission_binding(broken: bool) {
    loom::model(move || {
        let admissions = Arc::new(Admissions::new());
        let registering = Arc::clone(&admissions);
        let insert = loom::thread::spawn(move || registering.insert(1).unwrap());
        if !broken {
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
