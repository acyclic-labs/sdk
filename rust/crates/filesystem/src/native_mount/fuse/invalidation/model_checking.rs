use super::*;
use crate::sync::mpsc;
use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};

loom::thread_local! {
    static OMIT_NOTIFY: Cell<bool> = Cell::new(false);
    static OMIT_TERMINAL_NOTIFY: Cell<bool> = Cell::new(false);
    static INVALIDATOR_WAIT: RefCell<Option<(mpsc::Sender<()>, u8)>> = RefCell::new(None);
    static WAITER_WAIT: RefCell<Option<mpsc::Sender<()>>> = RefCell::new(None);
    static AFTER_GUARD_DROP: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}

pub(in crate::native_mount::fuse) fn after_guard_drop() {
    AFTER_GUARD_DROP.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
}

#[test]
fn a_request_replies_while_reverse_invalidation_waits_for_its_kernel_lock() {
    crate::initialize_test_native_io();
    let exercise = |inline| {
        loom::model(move || {
            let (core, work) = super::super::tests::model_checking::fixture();
            let keeper = ManuallyDrop::new((Arc::clone(&core), work.clone()));
            let (replied, reply) = mpsc::channel();
            let reply = Arc::new(crate::sync::Mutex::new(reply));
            let delivered = Arc::new(crate::sync::atomic::AtomicBool::new(false));
            struct KernelLock {
                reply: Arc<crate::sync::Mutex<mpsc::Receiver<()>>>,
                delivered: Arc<crate::sync::atomic::AtomicBool>,
                core: Arc<ProjectionCore>,
            }
            impl KernelCacheNotifier for KernelLock {
                fn inval_attributes(&self, _: u64) -> std::io::Result<()> {
                    // The kernel releases its inode lock only after the
                    // original userspace request has published its reply.
                    self.reply.lock().unwrap().recv().unwrap();
                    self.delivered.store(true, Ordering::Release);
                    self.core.stop();
                    Ok(())
                }
                fn inval_inode(&self, _: u64) -> std::io::Result<()> {
                    unreachable!("this request only supersedes attributes")
                }
                fn inval_entry(&self, _: u64, _: &[u8]) -> std::io::Result<()> {
                    unreachable!("this request only supersedes attributes")
                }
            }
            let notifier = loom::thread::spawn({
                let core = Arc::clone(&core);
                let delivered = Arc::clone(&delivered);
                let reply = Arc::clone(&reply);
                move || {
                    run_invalidator(
                        &core,
                        &KernelLock {
                            reply,
                            delivered,
                            core: Arc::clone(&core),
                        },
                    )
                }
            });
            let mut state = core.state().unwrap();
            state.invalidate_attributes(10, &work).unwrap();
            if inline {
                // Negative control restores the former synchronous device
                // wait at the actual production request-guard drop boundary.
                AFTER_GUARD_DROP.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        reply.lock().unwrap().recv().unwrap();
                    }))
                });
            }
            drop(state);
            replied.send(()).unwrap();
            notifier.join().unwrap();
            assert!(delivered.load(Ordering::Acquire));
            drop(ManuallyDrop::into_inner(keeper));
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("inline invalidation must deadlock behind the request's kernel lock");
    let message = error
        .downcast_ref::<String>()
        .expect("Loom deadlock message");
    assert!(message.contains("deadlock"), "{message}");
}

pub(in crate::native_mount::fuse) fn omit_guard_notification() -> bool {
    OMIT_NOTIFY.with(Cell::get)
}

pub(in crate::native_mount::fuse) fn omit_terminal_notification() -> bool {
    OMIT_TERMINAL_NOTIFY.with(Cell::get)
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

#[test]
fn callback_failure_wakes_the_original_invalidation_waiter() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let (core, work) = super::super::tests::model_checking::fixture();
            let keeper = ManuallyDrop::new((Arc::clone(&core), work.clone()));
            let (sleeping, asleep) = mpsc::channel();
            let producer = loom::thread::spawn({
                let core = Arc::clone(&core);
                move || {
                    asleep.recv().unwrap();
                    OMIT_TERMINAL_NOTIFY.with(|fault| fault.set(broken));
                    core.retain_handler_failure((libc::EIO.into(), WorkCounters::UNCHARGED));
                }
            });
            let mut state = core.state().unwrap();
            fuse_reserve(&mut state.invalidation.deferred, 1, &work).unwrap();
            state.invalidation.deferred.push(KernelCacheItem::Inode(10));
            WAITER_WAIT.with(|hook| *hook.borrow_mut() = Some(sleeping));
            assert!(matches!(
                core.wait_for_invalidation(state, None, &work),
                Err(NativeMountError::Lifecycle(
                    "a FUSE callback failure stopped this view"
                ))
            ));
            producer.join().unwrap();
            assert!(core.handler_error.lock().unwrap().first_fatal.is_some());
            drop(ManuallyDrop::into_inner(keeper));
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("withdrawing the terminal wake must strand the original waiter");
    let message = error
        .downcast_ref::<String>()
        .expect("Loom deadlock message");
    assert!(message.contains("deadlock"), "{message}");
}

impl KernelCacheNotifier for RefusesFirst {
    fn inval_attributes(&self, inode: u64) -> std::io::Result<()> {
        self.inval_inode(inode)
    }
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
fn production_guard_wakes_invalidation_failure_and_its_waiter() {
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
                    INVALIDATOR_WAIT.with(|hook| *hook.borrow_mut() = Some((sleeping, 1)));
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
            assert_eq!(invalidator.join().unwrap(), [(10, false)]);
            {
                let state = core.state_to_finish();
                assert!(state.invalidation.denied);
                assert!(!state.invalidation.in_flight);
            }
            assert!(core.handler_failed.load(Ordering::Acquire));
            assert!(core.take_handler_failure().is_some());
            assert!(
                core.wait_for_invalidation(core.state_to_finish(), None, &work)
                    .is_err()
            );
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
fn a_waiter_finishes_before_notifications_admitted_after_its_cut() {
    crate::initialize_test_native_io();
    loom::model(|| {
        let (core, work) = super::super::tests::model_checking::fixture();
        let (sleeping, asleep) = mpsc::channel();
        let producer = loom::thread::spawn({
            let core = Arc::clone(&core);
            let work = work.clone();
            move || {
                asleep.recv().unwrap();
                struct AddsLater<'a> {
                    core: &'a ProjectionCore,
                    work: &'a Work,
                }
                impl KernelCacheNotifier for AddsLater<'_> {
                    fn inval_attributes(&self, inode: u64) -> std::io::Result<()> {
                        self.inval_inode(inode)
                    }
                    fn inval_inode(&self, inode: u64) -> std::io::Result<()> {
                        assert_eq!(inode, 10);
                        let mut state = self.core.state_to_finish();
                        fuse_reserve(&mut state.invalidation.deferred, 1, self.work).unwrap();
                        state.invalidation.deferred.push(KernelCacheItem::Inode(20));
                        Ok(())
                    }
                    fn inval_entry(&self, _: u64, _: &[u8]) -> std::io::Result<()> {
                        unreachable!("this pass contains inode notifications")
                    }
                }
                invalidation_pass(
                    &core,
                    &AddsLater {
                        core: &core,
                        work: &work,
                    },
                );
            }
        });
        let mut state = core.state().unwrap();
        fuse_reserve(&mut state.invalidation.deferred, 1, &work).unwrap();
        state.invalidation.deferred.push(KernelCacheItem::Inode(10));
        WAITER_WAIT.with(|hook| *hook.borrow_mut() = Some(sleeping));
        core.wait_for_invalidation(state, None, &work).unwrap();
        producer.join().unwrap();
        let state = core.state_to_finish();
        assert_eq!(state.invalidation.deferred.len(), 1);
        assert!(state.invalidation.is_pending(None));
        assert_eq!(state.invalidation.deferred_processed, 1);
        drop(state);
        core.stop();
    });
}
