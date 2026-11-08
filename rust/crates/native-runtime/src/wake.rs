//! Durable wake tokens shared by synchronous native completion owners.
#[cfg(loom)]
use loom::sync::{Condvar, Mutex};
use std::sync::Arc;
#[cfg(not(loom))]
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

/// Registers one observer without invoking its clone or retirement callbacks
/// under the owner's lock. Unchanged observers and terminal reads use one
/// lock and no clone. A replacement clones outside the lock, then rechecks
/// publication under the same lock before installing its wake. Readiness is
/// observed without taking the outcome; a returned guard retains its custody
/// until every unused incoming observer has retired outside the lock.
pub(crate) fn poll_registered<S, G>(
    mut lock: impl FnMut() -> G,
    waker: &std::task::Waker,
    ready: impl Fn(&S) -> bool,
    slot: impl Fn(&mut S) -> &mut Option<std::task::Waker>,
) -> std::task::Poll<G>
where
    G: std::ops::DerefMut<Target = S>,
{
    let mut state = lock();
    if ready(&state) {
        return std::task::Poll::Ready(state);
    }
    if slot(&mut state)
        .as_ref()
        .is_some_and(|current| current.will_wake(waker))
    {
        return std::task::Poll::Pending;
    }
    drop(state);
    let waker = waker.clone();
    let mut state = lock();
    if ready(&state) {
        drop(state);
        drop(waker);
        return std::task::Poll::Ready(lock());
    }
    let retired = slot(&mut state).replace(waker);
    drop(state);
    drop(retired);
    std::task::Poll::Pending
}

#[cfg(all(test, not(loom)))]
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
