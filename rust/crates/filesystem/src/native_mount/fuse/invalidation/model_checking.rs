use super::*;
use crate::sync::mpsc;
use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};

loom::thread_local! {
    static OMIT_NOTIFY: Cell<bool> = Cell::new(false);
    static INVALIDATOR_WAIT: RefCell<Option<(mpsc::Sender<()>, u8)>> = RefCell::new(None);
    static WAITER_WAIT: RefCell<Option<mpsc::Sender<()>>> = RefCell::new(None);
}

pub(in crate::native_mount::fuse) fn omit_guard_notification() -> bool {
    OMIT_NOTIFY.with(Cell::get)
}

pub(in crate::native_mount::fuse) fn before_invalidator_wait() {
    INVALIDATOR_WAIT.with(|hook| {
        let mut checkpoint = hook.borrow_mut();
        if let Some((ready, remaining)) = checkpoint.as_mut() {
            ready.send(()).unwrap();
            *remaining -= 1;
            if *remaining == 0 {
                *checkpoint = None;
            }
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

#[derive(Default)]
struct RefusesFirst {
    sent: RefCell<Vec<(u64, bool)>>,
    first_gate: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
}

impl KernelCacheNotifier for RefusesFirst {
    fn inval_inode(&self, inode: u64) -> std::io::Result<()> {
        let refused = self.sent.borrow().is_empty();
        self.sent.borrow_mut().push((inode, !refused));
        if refused {
            if let Some((attempted, release)) = &self.first_gate {
                attempted.send(()).unwrap();
                release.recv().unwrap();
            }
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
fn production_guard_wakes_a_sleeping_invalidator_and_waiter() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let (core, work) = super::super::tests::model_checking::fixture();
            // Failed controls leave sleepers behind. Keep accounting owners
            // alive rather than locking their meters during Loom teardown.
            let keeper = ManuallyDrop::new((Arc::clone(&core), work.clone()));
            let (attempted, attempt) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let (sleeping, asleep) = mpsc::channel();
            let invalidator = loom::thread::spawn({
                let core = Arc::clone(&core);
                move || {
                    INVALIDATOR_WAIT.with(|hook| *hook.borrow_mut() = Some((sleeping, 2)));
                    let notifier = RefusesFirst {
                        first_gate: Some((attempted, released)),
                        ..Default::default()
                    };
                    run_invalidator(&core, &notifier);
                    notifier.sent.into_inner()
                }
            });
            asleep.recv().unwrap();
            // Taking state after the checkpoint ensures the production
            // invalidator actually released it in Condvar::wait.
            let mut state = core.state().unwrap();
            fuse_reserve(&mut state.invalidation.deferred, 1, &work).unwrap();
            state.invalidation.deferred.push(KernelCacheItem::Inode(10));
            OMIT_NOTIFY.with(|fault| fault.set(broken));
            drop(state);
            OMIT_NOTIFY.with(|fault| fault.set(false));
            // No waiter notification can rescue a missing producer wakeup.
            attempt.recv().unwrap();
            // The real waiter releases the refusal while holding state,
            // immediately before its atomic Condvar wait.
            WAITER_WAIT.with(|hook| *hook.borrow_mut() = Some(release));
            assert!(matches!(
                core.wait_for_invalidation(core.state().unwrap(), None, &work),
                Err(NativeMountError::Lifecycle(_))
            ));
            // The refusal and waiter wake have both completed. Park the worker
            // before stop; unrelated teardown interleavings add no wake coverage.
            asleep.recv().unwrap();
            {
                let state = core.state().unwrap();
                assert!(state.invalidation.denied);
                assert_eq!(
                    state.invalidation.pending_tail.as_slice(),
                    &[KernelCacheItem::Inode(10)]
                );
            }
            core.stop();
            assert_eq!(invalidator.join().unwrap(), [(10, false)]);
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

#[test]
fn production_waiter_retries_refused_notifications_in_fifo_order() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let (core, work) = super::super::tests::model_checking::fixture();
            let keeper = ManuallyDrop::new((Arc::clone(&core), work.clone()));
            {
                let mut state = core.state().unwrap();
                fuse_reserve(&mut state.invalidation.deferred, 2, &work).unwrap();
                state
                    .invalidation
                    .deferred
                    .extend([KernelCacheItem::Inode(10), KernelCacheItem::Inode(11)]);
            }
            let (sleeping, asleep) = mpsc::channel();
            let invalidator = loom::thread::spawn({
                let core = Arc::clone(&core);
                move || {
                    OMIT_NOTIFY.with(|fault| fault.set(broken));
                    INVALIDATOR_WAIT.with(|hook| *hook.borrow_mut() = Some((sleeping, 2)));
                    let notifier = RefusesFirst::default();
                    run_invalidator(&core, &notifier);
                    notifier.sent.into_inner()
                }
            });
            // The first production wait occurs after refusal has settled.
            // Retain state from the inspection through retry admission.
            asleep.recv().unwrap();
            let state = core.state().unwrap();
            assert!(state.invalidation.denied);
            assert_eq!(
                state.invalidation.pending_tail.as_slice(),
                &[KernelCacheItem::Inode(10), KernelCacheItem::Inode(11)],
                "refused notification tail was lost"
            );
            assert!(state.invalidation.is_pending(None));
            assert!(state.invalidation.deferred.is_empty());
            core.wait_for_invalidation(state, None, &work).unwrap();
            // Retry completion and its real wake have been observed. Let the
            // invalidator finish the pass and park before unrelated stop/teardown
            // bookkeeping; those operations add no FIFO or waiter-wake coverage.
            asleep.recv().unwrap();
            core.stop();
            assert_eq!(
                invalidator.join().unwrap(),
                [(10, false), (10, true), (11, true)]
            );
            assert!(!core.state_to_finish().invalidation.is_pending(None));
            drop(ManuallyDrop::into_inner(keeper));
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("a missing completion notification must strand the retry waiter");
    let message = error
        .downcast_ref::<String>()
        .expect("Loom deadlock message");
    assert!(message.contains("deadlock"), "{message}");
}
