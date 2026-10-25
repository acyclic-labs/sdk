//! Durable wake tokens shared by synchronous native completion owners.
#[cfg(acyclic_native_runtime_loom)]
use loom::sync::{Condvar, Mutex};
use std::sync::Arc;
#[cfg(not(acyclic_native_runtime_loom))]
use std::sync::{Condvar, Mutex};

/// A coalescing notification retained until one waiter consumes it.
/// Its mutex is independent of the state protected by the caller.
#[derive(Default)]
pub struct NativeWake {
    notified: Mutex<bool>,
    changed: Condvar,
}

impl NativeWake {
    /// Retains a notification, including one delivered before waiting.
    pub fn notify(&self) {
        *self
            .notified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.changed.notify_one();
    }

    /// Consumes a retained notification, waiting when none has arrived.
    pub fn wait(&self, deadline: Option<std::time::Instant>) {
        let mut notified = self
            .notified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*notified {
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let (next, timeout) = self
                    .changed
                    .wait_timeout(notified, remaining)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                notified = next;
                if timeout.timed_out() {
                    break;
                }
            } else {
                notified = self
                    .changed
                    .wait(notified)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        *notified = false;
    }
}

impl std::task::Wake for NativeWake {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

#[cfg(all(test, not(acyclic_native_runtime_loom)))]
mod tests {
    use super::*;

    #[test]
    fn native_wake_retains_inline_completion_under_an_unrelated_lock() {
        let owner = std::sync::Mutex::new(());
        let wake = Arc::new(NativeWake::default());
        let state = owner.lock().unwrap();
        std::task::Waker::from(Arc::clone(&wake)).wake_by_ref();
        drop(state);
        wake.wait(None);
    }

    #[test]
    fn native_wake_retains_cross_thread_completion() {
        let wake = Arc::new(NativeWake::default());
        let producer = Arc::clone(&wake);
        let thread = std::thread::spawn(move || producer.notify());
        wake.wait(None);
        assert!(thread.join().is_ok());
    }
}
