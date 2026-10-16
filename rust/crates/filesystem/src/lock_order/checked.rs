use crate::sync::{Condvar as RawCondvar, Mutex as RawMutex, MutexGuard as RawMutexGuard};
#[cfg(acyclic_fs_loom)]
use loom::thread_local;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::panic::Location;
use std::sync::{LockResult, PoisonError};
#[cfg(not(acyclic_fs_loom))]
use std::thread_local;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

#[cfg(not(acyclic_fs_loom))]
thread_local! {
    static HELD: RefCell<Vec<(u16, &'static Location<'static>)>> = const { RefCell::new(Vec::new()) };
}

#[cfg(acyclic_fs_loom)]
thread_local! {
    static HELD: RefCell<Vec<(u16, &'static Location<'static>)>> = RefCell::new(Vec::new());
}

fn with_held(edit: &mut dyn FnMut(&mut Vec<(u16, &'static Location<'static>)>)) {
    HELD.with(|held| edit(&mut held.borrow_mut()));
}

struct Held(u16, &'static Location<'static>);

impl Held {
    #[track_caller]
    fn check(rank: u16) {
        #[cfg(all(test, acyclic_fs_loom))]
        if model::fault() == model::Fault::MissingCheck {
            return;
        }
        let requested = Location::caller();
        with_held(&mut |held| {
            if let Some((previous, site)) = held.iter().find(|(previous, _)| rank <= *previous) {
                assert!(
                    rank > *previous,
                    "lock order: rank {rank} at {} while holding rank {previous} from {site}",
                    requested
                );
            }
        });
    }

    fn enter(self) -> Self {
        with_held(&mut |held| held.push((self.0, self.1)));
        self
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        with_held(&mut |held| {
            if let Some(index) = held.iter().position(|(rank, _)| *rank == self.0) {
                held.remove(index);
            }
        });
    }
}

pub(crate) struct TrackedGuard<G> {
    // Unlock before removing the thread's held-rank record.
    inner: G,
    held: Held,
}

impl<G: Deref> Deref for TrackedGuard<G> {
    type Target = G::Target;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<G: DerefMut> DerefMut for TrackedGuard<G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub(crate) type MutexGuard<'a, T> = TrackedGuard<RawMutexGuard<'a, T>>;

#[track_caller]
fn acquire<G>(rank: u16, acquire: impl FnOnce() -> LockResult<G>) -> LockResult<TrackedGuard<G>> {
    Held::check(rank);
    let site = Location::caller();
    let wrap = |inner| TrackedGuard {
        inner,
        held: Held(rank, site).enter(),
    };
    match acquire() {
        Ok(inner) => Ok(wrap(inner)),
        Err(error) => Err(PoisonError::new(wrap(error.into_inner()))),
    }
}

#[track_caller]
pub(crate) fn lock<T, const RANK: u16>(mutex: &RawMutex<T>) -> LockResult<MutexGuard<'_, T>> {
    acquire(RANK, || mutex.lock())
}

#[derive(Debug, Default)]
pub(crate) struct Mutex<T, const RANK: u16>(RawMutex<T>);

impl<T, const RANK: u16> Mutex<T, RANK> {
    #[cfg(not(acyclic_fs_loom))]
    pub(crate) const fn new(value: T) -> Self {
        Self(RawMutex::new(value))
    }
    #[cfg(acyclic_fs_loom)]
    pub(crate) fn new(value: T) -> Self {
        Self(RawMutex::new(value))
    }

    #[track_caller]
    pub(crate) fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
        lock::<_, RANK>(&self.0)
    }
}

#[derive(Default)]
pub(crate) struct Condvar(RawCondvar);

impl Condvar {
    #[cfg(not(acyclic_fs_loom))]
    pub(crate) const fn new() -> Self {
        Self(RawCondvar::new())
    }
    #[cfg(acyclic_fs_loom)]
    pub(crate) fn new() -> Self {
        Self(RawCondvar::new())
    }

    pub(crate) fn notify_all(&self) {
        self.0.notify_all();
    }

    #[track_caller]
    pub(crate) fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        wait_plain(guard, |inner| self.0.wait(inner))
    }

    #[track_caller]
    pub(crate) fn wait_while<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        condition: impl FnMut(&mut T) -> bool,
    ) -> LockResult<MutexGuard<'a, T>> {
        while_locked(guard, condition, |guard| self.wait(guard))
    }

    #[track_caller]
    pub(crate) fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        duration: Duration,
    ) -> LockResult<(MutexGuard<'a, T>, WaitTimeoutResult)> {
        let result = wait(guard, |inner| self.0.wait_timeout(inner, duration));
        #[cfg(not(test))]
        {
            result
        }
        #[cfg(test)]
        {
            match result {
                Ok((guard, timeout)) => Ok((guard, WaitTimeoutResult::from(timeout))),
                Err(error) => {
                    let (guard, timeout) = error.into_inner();
                    Err(PoisonError::new((guard, WaitTimeoutResult::from(timeout))))
                }
            }
        }
    }

    #[cfg(test)]
    #[track_caller]
    pub(crate) fn wait_timeout_while<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        duration: Duration,
        condition: impl FnMut(&mut T) -> bool,
    ) -> LockResult<(MutexGuard<'a, T>, WaitTimeoutResult)> {
        while_timed(guard, duration, condition, |guard, remaining| {
            self.wait_timeout(guard, remaining)
        })
    }
}

#[cfg(not(test))]
type WaitTimeoutResult = crate::sync::WaitTimeoutResult;

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct WaitTimeoutResult(bool);

#[cfg(test)]
impl From<crate::sync::WaitTimeoutResult> for WaitTimeoutResult {
    fn from(timeout: crate::sync::WaitTimeoutResult) -> Self {
        Self(timeout.timed_out())
    }
}

#[cfg(test)]
impl WaitTimeoutResult {
    pub(crate) fn timed_out(&self) -> bool {
        self.0
    }
}

fn predicate<T: ?Sized>(value: &mut T, condition: &mut impl FnMut(&mut T) -> bool) -> bool {
    #[cfg(all(test, acyclic_fs_loom))]
    if model::fault() == model::Fault::UntrackedPredicate {
        let saved = HELD.with(|held| std::mem::take(&mut *held.borrow_mut()));
        let result = condition(value);
        HELD.with(|held| *held.borrow_mut() = saved);
        return result;
    }
    condition(value)
}

#[track_caller]
fn while_locked<G: DerefMut>(
    mut guard: TrackedGuard<G>,
    mut condition: impl FnMut(&mut G::Target) -> bool,
    mut wait: impl FnMut(TrackedGuard<G>) -> LockResult<TrackedGuard<G>>,
) -> LockResult<TrackedGuard<G>> {
    while predicate(&mut *guard, &mut condition) {
        guard = wait(guard)?;
    }
    Ok(guard)
}

#[cfg(test)]
#[track_caller]
fn while_timed<G: DerefMut>(
    mut guard: TrackedGuard<G>,
    duration: Duration,
    mut condition: impl FnMut(&mut G::Target) -> bool,
    mut wait: impl FnMut(TrackedGuard<G>, Duration) -> LockResult<(TrackedGuard<G>, WaitTimeoutResult)>,
) -> LockResult<(TrackedGuard<G>, WaitTimeoutResult)> {
    let started = Instant::now();
    while predicate(&mut *guard, &mut condition) {
        let remaining = duration.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Ok((guard, WaitTimeoutResult(true)));
        }
        (guard, _) = wait(guard, remaining)?;
    }
    Ok((guard, WaitTimeoutResult(false)))
}

#[track_caller]
fn wait_plain<G>(
    guard: TrackedGuard<G>,
    operation: impl FnOnce(G) -> LockResult<G>,
) -> LockResult<TrackedGuard<G>> {
    match wait(guard, |inner| match operation(inner) {
        Ok(inner) => Ok((inner, ())),
        Err(error) => Err(PoisonError::new((error.into_inner(), ()))),
    }) {
        Ok((guard, ())) => Ok(guard),
        Err(error) => Err(PoisonError::new(error.into_inner().0)),
    }
}

#[track_caller]
fn wait<G, R>(
    guard: TrackedGuard<G>,
    wait: impl FnOnce(G) -> LockResult<(G, R)>,
) -> LockResult<(TrackedGuard<G>, R)> {
    let TrackedGuard { inner, held } = guard;
    let rank = held.0;
    let site = held.1;
    drop(held);
    // Waiting releases this lock, then acquires it again under all other held locks.
    Held::check(rank);
    let wrap = |(inner, timeout)| {
        let held = Held(rank, site);
        #[cfg(all(test, acyclic_fs_loom))]
        let held = if model::fault() == model::Fault::MissingRestore {
            held
        } else {
            held.enter()
        };
        #[cfg(not(all(test, acyclic_fs_loom)))]
        let held = held.enter();
        (TrackedGuard { inner, held }, timeout)
    };
    match wait(inner) {
        Ok(result) => Ok(wrap(result)),
        Err(error) => Err(PoisonError::new(wrap(error.into_inner()))),
    }
}

#[cfg(all(test, acyclic_fs_loom))]
mod model;
