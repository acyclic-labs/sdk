//! The process's shared instances, one per root directory.
//!
//! An instance exists only while a view holds it: the registry keeps weak
//! references, every view keeps a strong one, and dropping the last view
//! tears the instance down, then unregisters it. A start for a root whose
//! teardown is running waits for it, so two instances never serve one root.
//! A failed start drops the half-built instance outside the registry, so
//! its teardown undoes only the steps it took.

use crate::native_mount::NativeMountError;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, PoisonError, Weak};

/// The live instances of one backend; a `static` per adapter.
pub(crate) struct Instances<I: 'static> {
    slots: Mutex<BTreeMap<PathBuf, Slot<I>>>,
}

struct Slot<I: 'static> {
    instance: Weak<Instance<I>>,
    retired: Arc<Retired>,
}

/// Signalled once an instance's teardown is complete.
#[derive(Default)]
struct Retired {
    done: Mutex<bool>,
    changed: Condvar,
}

impl Retired {
    fn wait(&self) {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        while !*done {
            done = self
                .changed
                .wait(done)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn finish(&self) {
        *self.done.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_all();
    }
}

/// One registered instance. Dropping the last reference runs the adapter's
/// teardown (its `Drop`) and only then frees the root for a new start.
pub(crate) struct Instance<I: 'static> {
    // Rust drops fields in declaration order: adapter teardown finishes before
    // the registration releases the root to another instance.
    inner: I,
    _registration: Registration<I>,
}

struct Registration<I: 'static> {
    root: PathBuf,
    retired: Arc<Retired>,
    registry: &'static Instances<I>,
}

impl<I> std::ops::Deref for Instance<I> {
    type Target = I;

    fn deref(&self) -> &I {
        &self.inner
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
                .get(&self.root)
                .is_some_and(|slot| Arc::ptr_eq(&slot.retired, &self.retired))
            {
                slots.remove(&self.root);
            }
        }
        self.retired.finish();
    }
}

impl<I> Instances<I> {
    pub(crate) const fn new() -> Self {
        Self {
            slots: Mutex::new(BTreeMap::new()),
        }
    }

    /// Returns the live instance for `root`, or starts one with `start`,
    /// which receives the canonical root.
    pub(crate) fn acquire(
        &'static self,
        root: &Path,
        start: impl FnOnce(&Path) -> Result<I, NativeMountError>,
    ) -> Result<Arc<Instance<I>>, NativeMountError> {
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
                // The last view dropped and teardown is running; a new start
                // would race it.
                let retired = Arc::clone(&slot.retired);
                drop(slots);
                retired.wait();
                continue;
            }
            // Starting under the registry lock serializes starts per root.
            let retired = Arc::new(Retired::default());
            let instance = Arc::new(Instance {
                inner: start(&root)?,
                _registration: Registration {
                    root: root.clone(),
                    retired: Arc::clone(&retired),
                    registry: self,
                },
            });
            slots.insert(
                root,
                Slot {
                    instance: Arc::downgrade(&instance),
                    retired,
                },
            );
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

    struct Counted;

    impl Drop for Counted {
        fn drop(&mut self) {
            STARTS.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn adapter_teardown_precedes_releasing_the_registration() -> Result<(), NativeMountError> {
        struct Checked {
            retired: Arc<std::sync::OnceLock<Arc<Retired>>>,
        }
        impl Drop for Checked {
            fn drop(&mut self) {
                let Some(retired) = self.retired.get() else {
                    panic!("fixture registration is absent");
                };
                assert!(
                    !*retired.done.lock().unwrap_or_else(PoisonError::into_inner),
                    "adapter must drop before registration releases the root"
                );
            }
        }
        static CHECKED: Instances<Checked> = Instances::new();
        let directory = tempfile::tempdir()?;
        let retired = Arc::new(std::sync::OnceLock::new());
        let instance = CHECKED.acquire(&directory.path().join("root"), |_| {
            Ok(Checked {
                retired: Arc::clone(&retired),
            })
        })?;
        assert!(
            retired
                .set(Arc::clone(&instance._registration.retired))
                .is_ok()
        );
        drop(instance);
        let Some(retired) = retired.get() else {
            panic!("fixture registration is absent");
        };
        assert!(*retired.done.lock().unwrap_or_else(PoisonError::into_inner));
        Ok(())
    }

    /// Holders of one root share one instance; the last drop tears it down
    /// and the next acquire starts a fresh one. A failed start registers
    /// nothing. Negative control: a second root gets its own instance.
    #[test]
    fn one_live_instance_per_root() -> Result<(), NativeMountError> {
        let directory = tempfile::tempdir()?;
        let (first_root, second_root) = (directory.path().join("a"), directory.path().join("b"));
        let start = |_: &Path| {
            STARTS.fetch_add(1, Ordering::SeqCst);
            Ok(Counted)
        };
        assert!(
            INSTANCES
                .acquire(&first_root, |_| Err(NativeMountError::ControlPending))
                .is_err()
        );
        let first = INSTANCES.acquire(&first_root, start)?;
        let again = INSTANCES.acquire(&first_root, |_| unreachable!("one instance per root"))?;
        assert!(Arc::ptr_eq(&first, &again));
        let other = INSTANCES.acquire(&second_root, start)?;
        assert!(!Arc::ptr_eq(&first, &other));
        assert_eq!(STARTS.load(Ordering::SeqCst), 2);
        drop((first, again));
        assert_eq!(
            STARTS.load(Ordering::SeqCst),
            1,
            "the last holder tears down"
        );
        let fresh = INSTANCES.acquire(&first_root, start)?;
        assert_eq!(STARTS.load(Ordering::SeqCst), 2);
        drop((fresh, other));
        assert_eq!(STARTS.load(Ordering::SeqCst), 0);
        Ok(())
    }
}
