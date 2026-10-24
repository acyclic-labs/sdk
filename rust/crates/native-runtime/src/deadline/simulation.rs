//! Scoped virtual time. Accepted actors prevent time advancement until parked.
use super::{Deadline, Waiter};
use crate::sync::Mutex;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

thread_local! { static CURRENT: RefCell<Option<Arc<Actor>>> = const { RefCell::new(None) }; }

#[derive(Clone)]
struct Blocked {
    deadline: Option<Instant>,
    waiter: Option<Weak<Waiter>>,
}
struct Entry {
    generation: u64,
    blocked: Option<Blocked>,
    suspended: usize,
    worker: Option<Worker>,
}
#[derive(Clone, Copy)]
struct Worker {
    pool: usize,
    capacity: usize,
    running: bool,
}
struct State {
    now: Instant,
    host_after: Option<usize>,
    next: u64,
    actors: BTreeMap<u64, Entry>,
    advances: Vec<Duration>,
    #[cfg(test)]
    advance_while_running: bool,
    #[cfg(test)]
    host_clock: bool,
    #[cfg(test)]
    duplicate_entry: bool,
    #[cfg(test)]
    queued_is_runnable: bool,
    #[cfg(test)]
    forget_parent_suspend: bool,
    #[cfg(test)]
    native_runnable: bool,
}
struct Inner {
    origin: Instant,
    state: Mutex<State>,
}

/// One virtual clock and actor inventory, owned by the simulation driver.
/// Sole lock; never held while notifying a waiter, waking or polling a future.
#[derive(Clone)]
pub struct Clock(Arc<Inner>);
impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}
impl Clock {
    /// Creates a clock. Only elapsed virtual time appears in its trace.
    pub fn new() -> Self {
        Self::at(super::now())
    }
    /// Creates a clock with an explicit monotonic origin.
    pub fn at(origin: Instant) -> Self {
        Self(Arc::new(Inner {
            origin,
            state: Mutex::new(State {
                now: origin,
                host_after: None,
                next: 0,
                actors: BTreeMap::new(),
                advances: Vec::new(),
                #[cfg(test)]
                advance_while_running: false,
                #[cfg(test)]
                host_clock: false,
                #[cfg(test)]
                duplicate_entry: false,
                #[cfg(test)]
                queued_is_runnable: false,
                #[cfg(test)]
                forget_parent_suspend: false,
                #[cfg(test)]
                native_runnable: false,
            }),
        }))
    }
    /// Captures the caller's virtual clock before handing work to another thread.
    pub fn capture() -> Option<Self> {
        CURRENT.with(|current| current.borrow().as_ref().map(|actor| actor.clock.clone()))
    }
    /// Runs a synchronous callback on this clock. Reentrant callbacks retain
    /// their existing actor, so an outer callback cannot prevent its own wait
    /// from advancing virtual time.
    pub fn enter<T>(&self, callback: impl FnOnce() -> T) -> T {
        let entered = CURRENT.with(|current| {
            current
                .borrow()
                .as_ref()
                .is_some_and(|actor| Arc::ptr_eq(&actor.clock.0, &self.0))
        });
        #[cfg(test)]
        let entered = entered && !self.0.state.lock().unwrap().duplicate_entry;
        if entered {
            callback()
        } else {
            self.register().enter(callback)
        }
    }
    /// Monotonic value without a host clock read.
    pub fn now(&self) -> Instant {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(reads) = &mut state.host_after {
            if *reads == 0 {
                drop(state);
                return Instant::now();
            }
            *reads -= 1;
        }
        state.now
    }
    /// Negative control: restore host reads after creating a virtual deadline.
    #[doc(hidden)]
    pub fn restore_host_clock_after(&self, reads: usize) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .host_after = Some(reads);
    }
    /// Time elapsed since this simulation's monotonic origin.
    pub fn elapsed(&self) -> Duration {
        self.now().saturating_duration_since(self.0.origin)
    }
    /// Exact virtual-time advancement trace.
    pub fn advances(&self) -> Vec<Duration> {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .advances
            .clone()
    }
    /// Registers an async actor. Wakeups become runnable before the driver can advance time.
    pub fn scope<'a, F: Future + 'a>(
        &self,
        future: F,
    ) -> Scoped<dyn Future<Output = F::Output> + 'a> {
        Scoped {
            actor: Some(self.register()),
            future: Box::pin(future),
        }
    }
    fn register(&self) -> Arc<Actor> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = state.next;
        state.next = state
            .next
            .checked_add(1)
            .expect("simulation actor IDs exhausted");
        state.actors.insert(
            id,
            Entry {
                generation: 0,
                blocked: None,
                suspended: 0,
                worker: None,
            },
        );
        Arc::new(Actor {
            clock: self.clone(),
            id,
        })
    }
    /// Advances to the earliest deadline only when every accepted actor is blocked.
    /// Returns false if an actor is runnable, or no bounded wait exists.
    pub fn advance_if_idle(&self) -> bool {
        let waiters = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let running = state.actors.values().any(|actor| {
                if actor.suspended != 0 || actor.blocked.is_some() {
                    return false;
                }
                #[cfg(test)]
                if state.queued_is_runnable {
                    return true;
                }
                if let Some(worker) = actor.worker.filter(|worker| !worker.running) {
                    // A queued job can run only when its original OS pool has
                    // a free worker. Blocked workers still occupy their slots.
                    // Pool identities never enter the replay trace or ordering.
                    let occupied = state
                        .actors
                        .values()
                        .filter(|actor| {
                            actor
                                .worker
                                .is_some_and(|active| active.running && active.pool == worker.pool)
                        })
                        .count();
                    return occupied < worker.capacity;
                }
                true
            });
            #[cfg(test)]
            let running = running && !state.advance_while_running;
            if running {
                return false;
            }
            let Some(next) = state
                .actors
                .values()
                .filter_map(|actor| actor.blocked.as_ref()?.deadline)
                .min()
            else {
                return false;
            };
            state.now = state.now.max(next);
            let elapsed = state.now.saturating_duration_since(self.0.origin);
            state.advances.push(elapsed);
            let now = state.now;
            let mut waiters = Vec::new();
            for actor in state.actors.values_mut() {
                if actor
                    .blocked
                    .as_ref()
                    .and_then(|blocked| blocked.deadline)
                    .is_some_and(|deadline| deadline <= now)
                {
                    let blocked = actor.blocked.take().expect("blocked deadline");
                    actor.generation += 1;
                    if let Some(waiter) = blocked.waiter.and_then(|waiter| waiter.upgrade()) {
                        waiters.push(waiter);
                    }
                }
            }
            waiters
        };
        for waiter in waiters {
            waiter.time_changed();
        }
        true
    }
}

pub(crate) struct Actor {
    clock: Clock,
    id: u64,
}
impl Actor {
    pub(crate) fn enter<T>(self: &Arc<Self>, work: impl FnOnce() -> T) -> T {
        struct Restore {
            previous: Option<Arc<Actor>>,
            suspended: Option<Arc<Actor>>,
        }
        impl Drop for Restore {
            fn drop(&mut self) {
                if let Some(parent) = self.suspended.take() {
                    parent
                        .clock
                        .0
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .actors
                        .get_mut(&parent.id)
                        .expect("live parent actor")
                        .suspended -= 1;
                }
                CURRENT.with(|current| {
                    current.replace(self.previous.take());
                });
            }
        }
        let previous = CURRENT.with(|current| current.replace(Some(self.clone())));
        let suspended = previous
            .as_ref()
            .filter(|parent| parent.id != self.id && Arc::ptr_eq(&parent.clock.0, &self.clock.0))
            .cloned();
        #[cfg(test)]
        let suspended =
            suspended.filter(|_| !self.clock.0.state.lock().unwrap().forget_parent_suspend);
        if let Some(parent) = &suspended {
            parent
                .clock
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .actors
                .get_mut(&parent.id)
                .expect("live parent actor")
                .suspended += 1;
        }
        let _restore = Restore {
            previous,
            suspended,
        };
        work()
    }
    pub(crate) fn queued(&self, pool: usize, capacity: usize) {
        let mut state = self
            .clock
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state.actors.get_mut(&self.id).expect("live queued actor");
        entry.blocked = None;
        entry.generation += 1;
        entry.worker = Some(Worker {
            pool,
            capacity,
            running: false,
        });
    }
    pub(crate) fn started(&self) {
        let mut state = self
            .clock
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state.actors.get_mut(&self.id).expect("live worker actor");
        if let Some(worker) = &mut entry.worker {
            worker.running = true;
        }
        entry.blocked = None;
        entry.generation += 1;
    }
    pub(super) fn resume(&self) {
        let mut state = self
            .clock
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(actor) = state.actors.get_mut(&self.id) {
            actor.blocked = None;
            actor.generation += 1;
        }
    }
    fn block(
        &self,
        deadline: Option<Deadline>,
        waiter: Option<Weak<Waiter>>,
        generation: Option<u64>,
    ) {
        {
            let mut state = self
                .clock
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(actor) = state.actors.get_mut(&self.id) {
                if generation.is_none_or(|generation| generation == actor.generation) {
                    actor.blocked = Some(Blocked {
                        deadline: deadline.map(Deadline::instant),
                        waiter,
                    });
                }
            }
        }
    }
}
impl Drop for Actor {
    fn drop(&mut self) {
        self.clock
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .actors
            .remove(&self.id);
        // Retiring physical work may be the final runnable actor. Wakes and
        // waiter latches are handled only after releasing the clock lock.
        self.clock.advance_if_idle();
    }
}

struct ScopedWake {
    actor: Weak<Actor>,
    task: Waker,
}
impl Wake for ScopedWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(actor) = self.actor.upgrade() {
            actor.resume();
        }
        self.task.wake_by_ref();
    }
}

/// A scoped original future; its type remains shallow across caller layers.
pub struct Scoped<F: Future + ?Sized> {
    actor: Option<Arc<Actor>>,
    future: Pin<Box<F>>,
}
impl<F: Future + ?Sized> Future for Scoped<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(actor) = this.actor.as_ref().cloned() else {
            return this.future.as_mut().poll(cx);
        };
        let generation = {
            let mut state = actor
                .clock
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = state.actors.get_mut(&actor.id).expect("live scoped actor");
            entry.blocked = None;
            entry.generation
        };
        let waker = Waker::from(Arc::new(ScopedWake {
            actor: Arc::downgrade(&actor),
            task: cx.waker().clone(),
        }));
        let result = actor.enter(|| this.future.as_mut().poll(&mut Context::from_waker(&waker)));
        match result {
            Poll::Ready(_) => {
                this.actor.take();
            }
            Poll::Pending => {
                actor.block(None, None, Some(generation));
                actor.clock.advance_if_idle();
            }
        }
        result
    }
}

pub(super) fn current_now() -> Option<Instant> {
    CURRENT.with(|current| {
        current.borrow().as_ref().map(|actor| {
            #[cfg(test)]
            if actor.clock.0.state.lock().unwrap().host_clock {
                return Instant::now();
            }
            actor.clock.now()
        })
    })
}
pub(crate) fn accept() -> Option<Arc<Actor>> {
    CURRENT.with(|current| {
        current.borrow().as_ref().map(|owner| {
            let actor = owner.clock.register();
            // Constructing a lazy native future does not submit its job. It
            // may await a file predecessor, or never be polled at all. Queue
            // admission (or inline execution) publishes its runnable state.
            #[cfg(test)]
            let dormant = !owner.clock.0.state.lock().unwrap().native_runnable;
            #[cfg(not(test))]
            let dormant = true;
            if dormant {
                actor.block(None, None, None);
            }
            actor
        })
    })
}
pub(crate) fn inherit_future<F: Future + Send + 'static>(
    future: F,
) -> Scoped<dyn Future<Output = F::Output> + Send> {
    let actor = CURRENT.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|actor| actor.clock.register())
    });
    Scoped {
        actor,
        future: Box::pin(future),
    }
}
pub(super) struct Parked(pub(super) Arc<Actor>);
impl Parked {
    pub(super) fn advance_if_idle(&self) {
        self.0.clock.advance_if_idle();
    }
}
impl Drop for Parked {
    fn drop(&mut self) {
        self.0.resume();
    }
}
pub(super) fn park(waiter: Arc<Waiter>, deadline: Option<Deadline>) -> Option<Parked> {
    let actor = CURRENT.with(|current| current.borrow().clone())?;
    actor.block(deadline, Some(Arc::downgrade(&waiter)), None);
    Some(Parked(actor))
}

#[cfg(all(test, not(acyclic_native_runtime_loom)))]
mod tests;

/// Whether this thread currently executes a simulated actor.
pub fn is_active() -> bool {
    CURRENT.with(|current| current.borrow().is_some())
}

#[cfg(all(test, acyclic_native_runtime_loom))]
mod models;

// Publish the observer's runnable state before retiring its physical owner.
// Returning an unwoken waker here would let retirement advance another timer
// between publication and the worker loop's eventual wake.
pub(crate) fn completed(actor: Option<Arc<Actor>>, waker: Option<Waker>) -> Option<Waker> {
    if actor.is_none() {
        return waker;
    }
    if let Some(waker) = waker {
        waker.wake();
    }
    drop(actor);
    None
}
