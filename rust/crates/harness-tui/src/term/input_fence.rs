//! Console input admission. Restoration must not race a read: a read that
//! starts after cooked mode returns would consume the shell's input.
use std::io;

#[cfg(acyclic_harness_tui_loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(acyclic_harness_tui_loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

#[derive(Default)]
struct State {
    closed: bool,
    reading: bool,
}

/// Admits reads until close. `close` returns only once no admitted read is in
/// flight, so the caller restores modes with input fenced and acknowledged.
/// Lock order: this lock is a leaf; reads run outside it.
#[derive(Default)]
pub(crate) struct InputFence {
    state: Mutex<State>,
    idle: Condvar,
}

impl InputFence {
    /// Run one read unless the fence is closed. The read must return once the
    /// closer's wake is signalled.
    pub(crate) fn read<T>(&self, read: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        {
            let mut state = self.lock()?;
            if state.closed {
                return Err(io::ErrorKind::NotConnected.into());
            }
            state.reading = true;
        }
        let result = read();
        let mut state = self.lock()?;
        state.reading = false;
        self.idle.notify_all();
        result
    }

    /// Admit no further reads and wait for the admitted one to finish. The
    /// caller signals the read's wake first.
    pub(crate) fn close(&self) -> io::Result<()> {
        let mut state = self.shut()?;
        while state.reading {
            state = self.idle.wait(state).map_err(|_| io::ErrorKind::Other)?;
        }
        Ok(())
    }

    fn shut(&self) -> io::Result<MutexGuard<'_, State>> {
        let mut state = self.lock()?;
        state.closed = true;
        Ok(state)
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| io::ErrorKind::Other.into())
    }
}

#[cfg(all(test, acyclic_harness_tui_loom))]
mod tests {
    use super::*;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    // A reader loops on the fence while another thread closes it and then
    // restores modes. No read may observe restored modes.
    fn model(broken: bool) {
        loom::model(move || {
            let fence = Arc::new(InputFence::default());
            let restored = Arc::new(AtomicBool::new(false));
            let (reading, observed) = (Arc::clone(&fence), Arc::clone(&restored));
            let reader = loom::thread::spawn(move || {
                for _ in 0..2 {
                    let read = reading.read(|| {
                        loom::thread::yield_now();
                        assert!(!observed.load(Ordering::SeqCst), "read ran after restore");
                        Ok(())
                    });
                    if read.is_err() {
                        return;
                    }
                }
            });
            if broken {
                // Negative control: closing without waiting for the admitted read.
                drop(fence.shut().unwrap());
            } else {
                fence.close().unwrap();
            }
            restored.store(true, Ordering::SeqCst);
            reader.join().unwrap();
        });
    }

    #[test]
    fn close_waits_for_the_admitted_read_before_restore() {
        model(false);
    }
    #[test]
    fn negative_control_detects_restore_racing_an_admitted_read() {
        assert!(std::panic::catch_unwind(|| model(true)).is_err());
    }
}
