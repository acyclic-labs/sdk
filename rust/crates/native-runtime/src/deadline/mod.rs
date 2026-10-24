//! Monotonic deadlines and retained wakeups for host and simulated waits.
use crate::sync::{Condvar, Mutex};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(all(test, acyclic_native_runtime_loom))]
mod models;
#[cfg(feature = "test-support")]
pub mod simulation;

/// Reads the monotonic clock belonging to this execution.
pub fn now() -> Instant {
    #[cfg(feature = "test-support")]
    if let Some(now) = simulation::current_now() {
        return now;
    }
    Instant::now()
}

/// Time elapsed on the same monotonic clock used to create `started`.
pub fn elapsed(started: Instant) -> Duration {
    now().saturating_duration_since(started)
}

/// An absolute deadline on the execution's monotonic clock.
#[derive(Clone, Copy, Debug)]
pub struct Deadline(Instant);
impl Deadline {
    /// Retains an existing deadline without changing its bound.
    pub const fn at(instant: Instant) -> Self {
        Self(instant)
    }
    /// Computes a deadline relative to this execution's clock.
    pub fn after(duration: Duration) -> Self {
        Self(now() + duration)
    }
    /// Remaining time, saturated at zero.
    pub fn remaining(self) -> Duration {
        self.0.saturating_duration_since(now())
    }
    /// Whether this deadline has expired.
    pub fn expired(self) -> bool {
        self.remaining().is_zero()
    }
    /// Absolute value for platform APIs that require an Instant.
    pub const fn instant(self) -> Instant {
        self.0
    }
}

/// Parks the calling actor for a duration on its execution's clock.
pub fn sleep(duration: Duration) {
    #[cfg(feature = "test-support")]
    if simulation::current_now().is_some() {
        Arc::new(Waiter::new()).wait(Some(Deadline::after(duration)));
        return;
    }
    std::thread::sleep(duration);
}

struct State {
    generation: u64,
    notified: bool,
    #[cfg(feature = "test-support")]
    parked: Vec<std::sync::Weak<simulation::Actor>>,
}

/// A retained notification plus an OS park, or a virtual-time park in simulation.
/// Lock order: waiter state, then simulation state. No notification is invoked
/// under simulation state; advancing time releases it before locking a waiter.
pub struct Waiter {
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(all(test, acyclic_native_runtime_loom))]
    forget_notification: bool,
}
impl Default for Waiter {
    fn default() -> Self {
        Self::new()
    }
}
impl Waiter {
    /// Creates a waiter with no pending notification.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                generation: 0,
                notified: false,
                #[cfg(feature = "test-support")]
                parked: Vec::new(),
            }),
            changed: Condvar::new(),
            #[cfg(all(test, acyclic_native_runtime_loom))]
            forget_notification: false,
        }
    }
    /// Captures notification order before examining an external predicate.
    pub fn listen(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
    }
    /// Retains a wake even when it arrives before the actor parks.
    pub fn notify(&self) {
        self.notify_all();
    }
    /// Wakes every observer of the previous generation.
    pub fn notify_all(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(all(test, acyclic_native_runtime_loom))]
        if self.forget_notification {
            drop(state);
            self.changed.notify_all();
            return;
        }
        state.generation = state
            .generation
            .checked_add(1)
            .expect("wait notification generation exhausted");
        state.notified = true;
        #[cfg(feature = "test-support")]
        let actors: Vec<_> = state
            .parked
            .drain(..)
            .filter_map(|actor| actor.upgrade())
            .collect();
        #[cfg(feature = "test-support")]
        for actor in &actors {
            actor.resume();
        }
        drop(state);
        self.changed.notify_all();
        #[cfg(feature = "test-support")]
        drop(actors);
    }
    /// Consumes a retained wake, or parks until a wake or deadline.
    pub fn wait(self: &Arc<Self>, deadline: Option<Deadline>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.notified {
            state.notified = false;
            return;
        }
        let generation = state.generation;
        state = self.park(state, generation, deadline);
        state.notified = false;
    }
    /// Parks only if no notification followed `generation`. Callers capture it
    /// before testing their predicate, then release the predicate lock to wait.
    pub fn wait_after(self: &Arc<Self>, generation: u64, deadline: Option<Deadline>) {
        drop(
            self.park(
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                generation,
                deadline,
            ),
        );
    }
    fn park<'a>(
        self: &'a Arc<Self>,
        mut state: crate::sync::MutexGuard<'a, State>,
        generation: u64,
        deadline: Option<Deadline>,
    ) -> crate::sync::MutexGuard<'a, State> {
        while state.generation == generation && !deadline.is_some_and(Deadline::expired) {
            #[cfg(feature = "test-support")]
            let parked = simulation::park(self.clone(), deadline);
            #[cfg(feature = "test-support")]
            if let Some(parked) = parked {
                let weak = Arc::downgrade(&parked.0);
                state
                    .parked
                    .retain(|actor| actor.strong_count() > 0 && !actor.ptr_eq(&weak));
                state.parked.push(weak);
                // The final blocked actor may itself be the thread polling
                // the driver. Release this latch before advancing, since an
                // expired waiter acquires it to publish its wake.
                drop(state);
                parked.advance_if_idle();
                state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.generation == generation && !deadline.is_some_and(Deadline::expired) {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                drop(parked);
                continue;
            }
            state = match deadline {
                Some(deadline) => {
                    self.changed
                        .wait_timeout(state, deadline.remaining())
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .0
                }
                None => self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            };
        }
        state
    }
    #[cfg(feature = "test-support")]
    fn time_changed(&self) {
        // This latch is the same mutex released atomically by Condvar::wait.
        // Acquiring it prevents an advance/park race from losing a wake.
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.changed.notify_all();
        drop(state);
    }
}
impl std::task::Wake for Waiter {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}
