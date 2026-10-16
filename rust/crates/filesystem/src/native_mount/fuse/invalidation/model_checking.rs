use super::*;
use crate::sync::{Mutex, mpsc};
use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};

loom::thread_local! {
    static OMIT_NOTIFY: Cell<bool> = Cell::new(false);
    static INVALIDATOR_WAIT: RefCell<Option<mpsc::Sender<()>>> = RefCell::new(None);
    static WAITER_WAIT: RefCell<Option<mpsc::Sender<()>>> = RefCell::new(None);
}

pub(in crate::native_mount::fuse) fn omit_guard_notification() -> bool {
    OMIT_NOTIFY.with(Cell::get)
}

pub(in crate::native_mount::fuse) fn before_invalidator_wait() {
    INVALIDATOR_WAIT.with(|hook| {
        if let Some(ready) = hook.borrow_mut().take() {
            ready.send(()).unwrap();
        }
    });
}

pub(in crate::native_mount::fuse) fn before_waiter_wait() {
    WAITER_WAIT.with(|hook| {
        if let Some(ready) = hook.borrow_mut().take() {
            ready.send(()).unwrap();
        }
    });
}

struct RefusesFirst {
    sent: Arc<Mutex<Vec<(u64, bool)>>>,
    first: Cell<bool>,
    attempted: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl KernelCacheNotifier for RefusesFirst {
    fn inval_inode(&self, inode: u64) -> std::io::Result<()> {
        let refused = self.first.replace(false);
        self.sent.lock().unwrap().push((inode, !refused));
        if refused {
            self.attempted.send(()).unwrap();
            self.release.recv().unwrap();
            Err(std::io::Error::from_raw_os_error(libc::EIO))
        } else {
            Ok(())
        }
    }

    fn inval_entry(&self, _: u64, _: &[u8]) -> std::io::Result<()> {
        unreachable!("this pass contains inode notifications")
    }
}

#[test]
fn production_guard_wakes_invalidator_and_waiters_retry_a_refused_tail() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        let volume = super::super::tests::model_checking::volume();
        loom::model(move || {
            let (core, work) = super::super::tests::model_checking::fixture(&volume);
            // Failed controls leave sleepers behind. Keep accounting owners
            // alive rather than locking their meters during Loom teardown.
            let keeper = ManuallyDrop::new((Arc::clone(&core), work.clone()));
            let sent = Arc::new(Mutex::new(Vec::new()));
            let (attempted, attempt) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let (sleeping, asleep) = mpsc::channel();
            let invalidator = loom::thread::spawn({
                let (core, sent) = (Arc::clone(&core), Arc::clone(&sent));
                move || {
                    INVALIDATOR_WAIT.with(|hook| *hook.borrow_mut() = Some(sleeping));
                    run_invalidator(
                        &core,
                        &RefusesFirst {
                            sent,
                            first: Cell::new(true),
                            attempted,
                            release: released,
                        },
                    );
                }
            });
            asleep.recv().unwrap();
            // Taking the state after the checkpoint ensures the production
            // invalidator actually released it in Condvar::wait.
            let mut state = core.state().unwrap();
            fuse_reserve(&mut state.invalidation.deferred, 2, &work).unwrap();
            state
                .invalidation
                .deferred
                .extend([KernelCacheItem::Inode(10), KernelCacheItem::Inode(11)]);
            OMIT_NOTIFY.with(|fault| fault.set(broken));
            drop(state);
            OMIT_NOTIFY.with(|fault| fault.set(false));
            // No waiter notification can rescue a missing producer wakeup.
            attempt.recv().unwrap();
            assert!(core.state().unwrap().invalidation.in_flight);
            let (waiting, waits) = mpsc::channel();
            let waiter = loom::thread::spawn({
                let (core, work) = (Arc::clone(&core), work.clone());
                move || {
                    WAITER_WAIT.with(|hook| *hook.borrow_mut() = Some(waiting));
                    core.wait_for_invalidation(core.state().unwrap(), None, &work)
                }
            });
            waits.recv().unwrap();
            release.send(()).unwrap();
            assert!(matches!(
                waiter.join().unwrap(),
                Err(NativeMountError::Lifecycle(_))
            ));
            {
                let state = core.state().unwrap();
                assert!(state.invalidation.denied);
                assert!(state.invalidation.is_pending(None));
                assert_eq!(
                    state.invalidation.pending_tail.as_slice(),
                    &[KernelCacheItem::Inode(10), KernelCacheItem::Inode(11),]
                );
                assert!(state.invalidation.deferred.is_empty());
            }
            core.wait_for_invalidation(core.state().unwrap(), None, &work)
                .unwrap();
            assert!(!core.state().unwrap().invalidation.is_pending(None));
            core.stop();
            invalidator.join().unwrap();
            assert_eq!(*sent.lock().unwrap(), [(10, false), (10, true), (11, true)]);
            drop(ManuallyDrop::into_inner(keeper));
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("a missing guard notification must strand the invalidator");
    let message = error
        .downcast_ref::<String>()
        .expect("Loom deadlock message");
    assert!(message.contains("deadlock"), "{message}");
}
