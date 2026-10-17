#[cfg(any(
    all(test, not(target_arch = "wasm32")),
    all(feature = "native-mount", target_os = "linux")
))]
use crate::sync::{Condvar as RawCondvar, WaitTimeoutResult};
use crate::sync::{Mutex as RawMutex, MutexGuard as RawMutexGuard};
#[cfg(acyclic_fs_loom)]
use loom::thread_local;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};
use std::panic::Location;
use std::sync::{LockResult, PoisonError};
#[cfg(not(acyclic_fs_loom))]
use std::thread_local;
#[cfg(any(
    all(test, not(target_arch = "wasm32")),
    all(feature = "native-mount", target_os = "linux")
))]
use std::time::Duration;

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
    _held: Held,
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
        _held: Held(rank, site).enter(),
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
#[cfg(any(
    test,
    all(
        any(feature = "local", feature = "native-watch"),
        not(target_arch = "wasm32")
    )
))]
pub(crate) struct Mutex<T, const RANK: u16>(RawMutex<T>);

#[cfg(any(
    test,
    all(
        any(feature = "local", feature = "native-watch"),
        not(target_arch = "wasm32")
    )
))]
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
#[cfg(any(
    all(test, not(target_arch = "wasm32")),
    all(feature = "native-mount", target_os = "linux")
))]
pub(crate) struct Condvar(RawCondvar);

#[cfg(any(
    all(test, not(target_arch = "wasm32")),
    all(feature = "native-mount", target_os = "linux")
))]
impl Condvar {
    #[cfg(not(acyclic_fs_loom))]
    pub(crate) const fn new() -> Self {
        Self(RawCondvar::new())
    }
    #[cfg(acyclic_fs_loom)]
    pub(crate) fn new() -> Self {
        Self(RawCondvar::new())
    }

    #[cfg(any(acyclic_fs_loom, all(feature = "native-mount", target_os = "linux")))]
    pub(crate) fn notify_all(&self) {
        self.0.notify_all();
    }

    #[cfg(any(acyclic_fs_loom, all(feature = "native-mount", target_os = "linux")))]
    #[track_caller]
    pub(crate) fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        wait_plain(guard, |inner| self.0.wait(inner))
    }

    #[track_caller]
    pub(crate) fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        duration: Duration,
    ) -> LockResult<(MutexGuard<'a, T>, WaitTimeoutResult)> {
        wait(guard, |inner| self.0.wait_timeout(inner, duration))
    }
}

#[track_caller]
#[cfg(any(
    all(test, acyclic_fs_loom),
    all(feature = "native-mount", target_os = "linux")
))]
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
#[cfg(any(
    all(test, not(target_arch = "wasm32")),
    all(feature = "native-mount", target_os = "linux")
))]
fn wait<G, R>(
    guard: TrackedGuard<G>,
    wait: impl FnOnce(G) -> LockResult<(G, R)>,
) -> LockResult<(TrackedGuard<G>, R)> {
    let TrackedGuard { inner, _held: held } = guard;
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
        (TrackedGuard { inner, _held: held }, timeout)
    };
    match wait(inner) {
        Ok(result) => Ok(wrap(result)),
        Err(error) => Err(PoisonError::new(wrap(error.into_inner()))),
    }
}

#[cfg(all(test, acyclic_fs_loom))]
mod model;
