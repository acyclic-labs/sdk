//! Console reads register cancellation before admission; restoration waits for acknowledgement.
use std::io;

#[cfg(acyclic_harness_tui_loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(acyclic_harness_tui_loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

struct State<C> {
    closed: bool,
    reader: Option<C>,
}

/// Lock order: fence -> native cancellation. Cancellation never acquires the
/// fence; the read runs outside it and unregisters before returning to its caller.
pub(crate) struct InputFence<C> {
    state: Mutex<State<C>>,
    idle: Condvar,
}

impl<C> Default for InputFence<C> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                closed: false,
                reader: None,
            }),
            idle: Condvar::new(),
        }
    }
}

struct Admitted<'a, C>(&'a InputFence<C>);
impl<C> Drop for Admitted<'_, C> {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.reader = None;
        self.0.idle.notify_all();
    }
}

impl<C> InputFence<C> {
    pub(crate) fn read<T>(
        &self,
        cancellation: C,
        read: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        {
            let mut state = self.lock()?;
            if state.closed {
                return Err(io::ErrorKind::NotConnected.into());
            }
            if state.reader.is_some() {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            state.reader = Some(cancellation);
        }
        let _admitted = Admitted(self);
        read()
    }

    /// CancelSynchronousIo does not retain an early cancellation. Retry across
    /// read admission, and keep its thread registered until the read acknowledges.
    pub(crate) fn close(&self, cancel: impl Fn(&C)) -> io::Result<()> {
        let mut state = self.shut()?;
        while let Some(reader) = state.reader.as_ref() {
            cancel(reader);
            #[cfg(not(acyclic_harness_tui_loom))]
            {
                state = self
                    .idle
                    .wait_timeout(state, std::time::Duration::from_millis(1))
                    .map_err(|_| io::ErrorKind::Other)?
                    .0;
            }
            #[cfg(acyclic_harness_tui_loom)]
            {
                drop(state);
                loom::thread::yield_now();
                state = self.lock()?;
            }
        }
        Ok(())
    }

    fn shut(&self) -> io::Result<MutexGuard<'_, State<C>>> {
        let mut state = self.lock()?;
        state.closed = true;
        Ok(state)
    }
    fn lock(&self) -> io::Result<MutexGuard<'_, State<C>>> {
        self.state.lock().map_err(|_| io::ErrorKind::Other.into())
    }
}

#[cfg(all(test, acyclic_harness_tui_loom))]
#[path = "input_fence_tests.rs"]
mod tests;
