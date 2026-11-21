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
    #[cfg(any(windows, test))]
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

#[cfg(test)]
mod tests {
    use super::CallbackGate;

    /// A closed gate admits no callback, and one admitted before the close still drains.
    #[test]
    fn a_closed_gate_admits_nothing_and_drains_earlier_callbacks() {
        let gate = CallbackGate::new();
        let earlier = gate.enter().expect("open");
        gate.close();
        assert!(gate.enter().is_none(), "a closed gate admitted a callback");
        drop(earlier);
        assert_eq!(gate.drain(|| Err("waited")), Ok(()));
    }
}
