//! The process's shared instances, one per root directory.
//!
//! The registry keeps weak observer references. The factory owns the root's
//! exclusive registration and retains it through native startup and physical
//! teardown. Starts wait for that registration's publication or removal, so
//! two instances never serve one root, including while physical cleanup
//! outlives the last observer. Failed startup disposes its partial native
//! owner outside the registry mutex before releasing the registration.

use crate::native_mount::NativeMountError;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, LazyLock, Mutex, PoisonError, Weak};

/// The live instances of one backend; a `static` per adapter.
pub(crate) struct Instances<I: 'static> {
    slots: LazyLock<Mutex<HashMap<PathBuf, Slot<I>>>>,
}

struct Slot<I: 'static> {
    instance: Weak<I>,
    // An empty weak reference means startup or physical teardown is pending.
    // The same registration publishes startup and acknowledges teardown.
    state: Arc<RegistrationState>,
}

/// The native owner's exclusive registration for one root. Keep it through
/// partial startup cleanup and physical teardown; dropping it permits the
/// next instance to start.
pub(crate) struct Registration<I: 'static> {
    state: Arc<RegistrationState>,
    registry: &'static Instances<I>,
}

struct RegistrationState {
    root: PathBuf,
    changed: Condvar,
}

impl<I> Registration<I> {
    pub(crate) fn root(&self) -> &Path {
        &self.state.root
    }
}

impl<I> Drop for Registration<I> {
    fn drop(&mut self) {
        {
            let mut slots = self
                .registry
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if slots
                .get(&self.state.root)
                .is_some_and(|slot| Arc::ptr_eq(&slot.state, &self.state))
            {
                slots.remove(&self.state.root);
            }
        }
        self.state.changed.notify_all();
    }
}

impl<I> Instances<I> {
    pub(crate) const fn new() -> Self {
        Self {
            slots: LazyLock::new(|| Mutex::new(HashMap::new())),
        }
    }

    /// Returns the live instance for `root`, or starts one with `start`,
    /// which owns the canonical root registration until physical teardown.
    pub(crate) fn acquire(
        &'static self,
        root: &Path,
        start: impl FnOnce(Registration<I>) -> Result<I, NativeMountError>,
    ) -> Result<Arc<I>, NativeMountError> {
        // Canonicalize the parent only: a crash-left root may be a dead
        // mount whose own lookup fails, and its start must still reap it.
        let (Some(parent), Some(name)) = (root.parent(), root.file_name()) else {
            return Err(NativeMountError::InvalidDestination);
        };
        let root = parent
            .canonicalize()
            .map_err(|_| NativeMountError::InvalidDestination)?
            .join(name);
        loop {
            let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(slot) = slots.get(&root) {
                if let Some(instance) = slot.instance.upgrade() {
                    return Ok(instance);
                }
                // The registration owns either startup publication or physical
                // teardown. Both change this predicate under the same mutex;
                // constructor failure or unwind removes the slot as well.
                let state = Arc::clone(&slot.state);
                #[cfg(all(test, not(loom)))]
                tests::before_wait();
                drop(
                    state
                        .changed
                        .wait(slots)
                        .unwrap_or_else(PoisonError::into_inner),
                );
                continue;
            }
            let state = Arc::new(RegistrationState {
                root,
                changed: Condvar::new(),
            });
            slots.insert(
                state.root.clone(),
                Slot {
                    instance: Weak::new(),
                    state: Arc::clone(&state),
                },
            );
            drop(slots);
            // The placeholder excludes another start for this root without
            // holding the registry across native startup or partial cleanup.
            let instance = Arc::new(start(Registration {
                state: Arc::clone(&state),
                registry: self,
            })?);
            let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
            slots
                .get_mut(&state.root)
                .expect("startup owns the root registration")
                .instance = Arc::downgrade(&instance);
            drop(slots);
            state.changed.notify_all();
            return Ok(instance);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static STARTS: AtomicUsize = AtomicUsize::new(0);
    static INSTANCES: Instances<Counted> = Instances::new();

    thread_local! {
        static WAITING: std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>> = const {
            std::cell::RefCell::new(None)
        };
    }

    pub(super) fn before_wait() {
        WAITING.with(|waiting| {
            if let Some(waiting) = waiting.borrow_mut().take() {
                waiting.send(()).expect("wait observer is present");
            }
        });
    }

    #[derive(Clone, Copy)]
    enum Startup {
        Publish,
        Refuse,
        Unwind,
    }

    struct Counted {
        root: PathBuf,
        _registration: Registration<Self>,
    }

    fn start(registration: Registration<Counted>) -> Result<Counted, NativeMountError> {
        STARTS.fetch_add(1, Ordering::SeqCst);
        Ok(Counted {
            root: registration.root().to_owned(),
            _registration: registration,
        })
    }

    impl Drop for Counted {
        fn drop(&mut self) {
            let slots = INSTANCES
                .slots
                .try_lock()
                .expect("native cleanup must run outside the registry mutex");
            assert!(
                slots.contains_key(&self.root),
                "registration released the root before native cleanup"
            );
            drop(slots);
            STARTS.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Holders of one root share one instance; the last drop tears it down
    /// before the next start. A held constructor excludes its own root while
    /// another root progresses; partial starts release their registration.
    #[test]
    fn one_live_instance_per_root() -> Result<(), NativeMountError> {
        let directory = tempfile::tempdir()?;
        let (first_root, second_root) = (directory.path().join("a"), directory.path().join("b"));
        for startup in [Startup::Publish, Startup::Refuse, Startup::Unwind] {
            let (entered, entering) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel();
            let (waiting, waited) = std::sync::mpsc::channel();
            let (first, other) = std::thread::scope(|scope| {
                let starting_root = &first_root;
                let pending = scope.spawn(move || {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        INSTANCES.acquire(starting_root, |registration| {
                            let instance = start(registration)?;
                            entered.send(()).expect("startup observer is present");
                            released.recv().expect("startup release is present");
                            match startup {
                                Startup::Publish => Ok(instance),
                                Startup::Refuse => Err(NativeMountError::ControlPending),
                                Startup::Unwind => panic!("interrupt partial startup"),
                            }
                        })
                    }))
                });
                entering.recv().expect("constructor produces startup event");
                let unlocked = INSTANCES.slots.try_lock().is_ok();
                if !unlocked {
                    // Drain the constructor before reporting a regression.
                    release.send(()).expect("constructor is present");
                    drop(pending.join().expect("constructor returns"));
                    panic!("startup must release the registry mutex");
                }
                let same = scope.spawn(move || {
                    WAITING.with(|observer| *observer.borrow_mut() = Some(waiting));
                    INSTANCES.acquire(starting_root, start)
                });
                let waiting = waited.recv();
                // The event is sent under the actual registry mutex. Taking
                // that mutex proves the waiter has atomically released it at
                // Condvar::wait before startup is allowed to publish or fail.
                drop(
                    INSTANCES
                        .slots
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner),
                );
                let other = INSTANCES.acquire(&second_root, start);
                release.send(()).expect("constructor is present");
                let original = pending.join().expect("constructor returns");
                let first = same.join().expect("same-root acquirer returns")?;
                assert!(
                    waiting.is_ok(),
                    "same-root acquisition must wait for startup"
                );
                match (startup, original) {
                    (Startup::Publish, Ok(Ok(original))) => assert!(Arc::ptr_eq(&first, &original)),
                    (Startup::Refuse, Ok(Err(NativeMountError::ControlPending)))
                    | (Startup::Unwind, Err(_)) => {}
                    _ => panic!("constructor outcome changed"),
                }
                Ok::<_, NativeMountError>((first, other?))
            })?;
            let again =
                INSTANCES.acquire(&first_root, |_| unreachable!("one instance per root"))?;
            assert!(Arc::ptr_eq(&first, &again));
            assert!(!Arc::ptr_eq(&first, &other));
            assert_eq!(STARTS.load(Ordering::SeqCst), 2);
            drop((first, again));
            assert_eq!(STARTS.load(Ordering::SeqCst), 1, "last holder tears down");
            let fresh = INSTANCES.acquire(&first_root, start)?;
            assert_eq!(STARTS.load(Ordering::SeqCst), 2);
            drop((fresh, other));
            assert_eq!(STARTS.load(Ordering::SeqCst), 0);
        }
        Ok(())
    }
}
