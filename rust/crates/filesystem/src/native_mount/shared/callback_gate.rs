//! The in-flight callback count every backend drains through.
//!
//! A callback holds a [`CallbackPass`] while it runs. [`CallbackGate::drain`]
//! waits only for the callbacks admitted before it began: each pass is
//! counted under the epoch it entered in, and a drain opens a new epoch and
//! waits for the older ones to empty. Callbacks that keep arriving join the
//! new epoch and never extend the wait, so a drain finishes under any load.
//! The wait blocks on a condition variable; nothing polls. Once admission
//! closes, no new callback is admitted at all.

use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex, PoisonError};

pub(crate) struct CallbackGate {
    state: Mutex<GateState>,
    drained: Condvar,
}

struct GateState {
    epoch: u64,
    /// Passes held per epoch; an epoch is removed once empty.
    active: BTreeMap<u64, usize>,
    open: bool,
}

/// One admitted callback. Dropping it counts the callback out.
pub(crate) struct CallbackPass<'a> {
    gate: &'a CallbackGate,
    epoch: u64,
}

impl CallbackGate {
    pub(crate) const fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                epoch: 0,
                active: BTreeMap::new(),
                open: true,
            }),
            drained: Condvar::new(),
        }
    }

    /// Admits a callback, or `None` once admission is closed.
    pub(crate) fn enter(&self) -> Option<CallbackPass<'_>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.open {
            return None;
        }
        let epoch = state.epoch;
        *state.active.entry(epoch).or_insert(0) += 1;
        Some(CallbackPass { gate: self, epoch })
    }

    /// Admits no callback from now on; admitted ones run to completion.
    #[cfg(windows)]
    pub(crate) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open = false;
    }

    /// Waits until every callback admitted before this call has finished.
    /// `before_waiting` runs once, only if some such callback is still
    /// running, and its failure abandons the wait.
    pub(crate) fn drain<E>(&self, before_waiting: impl FnOnce() -> Result<(), E>) -> Result<(), E> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let target = state.epoch;
        state.epoch += 1;
        if state.active.range(..=target).next().is_none() {
            return Ok(());
        }
        drop(state);
        before_waiting()?;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.active.range(..=target).next().is_some() {
            state = self
                .drained
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        Ok(())
    }

    /// Callbacks running now.
    #[cfg(test)]
    pub(crate) fn active(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active
            .values()
            .sum()
    }
}

impl Drop for CallbackPass<'_> {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let emptied = match state.active.get_mut(&self.epoch) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            _ => {
                state.active.remove(&self.epoch);
                true
            }
        };
        drop(state);
        if emptied {
            self.gate.drained.notify_all();
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A drain waits for the callback admitted before it, and finishes
    /// while a later callback remains active.
    /// Negative control: with no earlier callback it returns at once,
    /// without running `before_waiting`.
    #[test]
    fn a_drain_finishes_while_a_later_callback_remains_active() {
        let gate = Arc::new(CallbackGate::new());
        assert_eq!(gate.drain(|| Err::<(), _>("waited")), Ok(()));
        let earlier = gate.enter().expect("open");
        let (entered, waiting) = std::sync::mpsc::channel();
        let (sent, finished) = std::sync::mpsc::channel();
        let barrier = std::thread::spawn({
            let gate = Arc::clone(&gate);
            move || {
                let waited =
                    gate.drain(|| entered.send(()).map_err(|_| "settle receiver disappeared"));
                let _ = sent.send(waited);
            }
        });
        waiting.recv().expect("barrier entered its new epoch");
        assert!(
            finished.try_recv().is_err(),
            "the earlier callback is still running"
        );
        // This pass is admitted after the barrier's explicit settle event.
        // Holding it through completion rejects a single-counter drain.
        let later = gate.enter().expect("open");
        drop(earlier);
        let waited = finished.recv();
        assert_eq!(waited, Ok(Ok(())), "a drain starved behind later callbacks");
        drop(later);
        barrier.join().expect("barrier");
        assert_eq!(gate.active(), 0);
    }

    /// A failed wait admission releases the drain's epoch.
    #[test]
    fn failed_wait_admission_does_not_hold_a_callback_epoch() {
        let gate = CallbackGate::new();
        let pass = gate.enter().expect("open");
        assert_eq!(gate.drain(|| Err("over budget")), Err("over budget"));
        drop(pass);
        assert_eq!(gate.drain(|| Err("over budget")), Ok(()));
    }
}
