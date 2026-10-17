mod cancellation {
    include!("../../src/cancellation.rs");

    pub fn poll_racy(value: &Cancellation, cx: &Context<'_>) -> Poll<()> {
        let cancelled = value.0.lock().unwrap().cancelled;
        if cancelled {
            return Poll::Ready(());
        }
        let next = cx.waker().clone();
        let old = value.0.lock().unwrap().waiter.replace(next);
        drop(old);
        Poll::Pending
    }

    pub fn poll_drop_locked(value: &Cancellation, cx: &Context<'_>) -> Poll<()> {
        let mut state = value.0.lock().unwrap();
        if state.cancelled {
            Poll::Ready(())
        } else {
            state.waiter = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    pub fn unlocked(value: &Cancellation) -> bool {
        value.0.try_lock().is_ok()
    }
}

use crate::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use cancellation::Cancellation;
use std::task::{Context, Wake, Waker};

struct Waiter {
    cancellation: Arc<Cancellation>,
    woken: AtomicBool,
}
impl Wake for Waiter {
    fn wake(self: std::sync::Arc<Self>) {
        let waker = Waker::from(self.clone());
        assert!(
            self.cancellation
                .poll(&Context::from_waker(&waker))
                .is_ready()
        );
        self.woken.store(true, Ordering::Release);
    }
}
impl Drop for Waiter {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn registration_model(broken: bool) {
    loom::model(move || {
        let cancellation = Arc::new(Cancellation::default());
        let waiter = std::sync::Arc::new(Waiter {
            cancellation: cancellation.clone(),
            woken: AtomicBool::new(false),
        });
        let canceller = cancellation.clone();
        let other = loom::thread::spawn(move || {
            canceller.cancel();
            canceller.cancel();
        });
        let waker = Waker::from(waiter.clone());
        let cx = Context::from_waker(&waker);
        let ready = if broken {
            cancellation::poll_racy(&cancellation, &cx)
        } else {
            cancellation.poll(&cx)
        }
        .is_ready();
        other.join().unwrap();
        let notified = ready || waiter.woken.load(Ordering::Acquire);
        // Retire a mutant's stranded waker before reporting its lost notification.
        cancellation.cancel();
        assert!(notified, "cancellation lost a wakeup");
        assert!(cancellation.poll(&cx).is_ready());
    });
}

#[test]
fn registration_and_cancellation_have_no_lost_wakeup_or_lock_cycle() {
    registration_model(false);
}

#[test]
#[should_panic(expected = "cancellation lost a wakeup")]
fn negative_control_detects_split_check_and_registration() {
    registration_model(true);
}

struct DropProbe {
    cancellation: Arc<Cancellation>,
    violation: Arc<AtomicBool>,
}
impl Wake for DropProbe {
    fn wake(self: std::sync::Arc<Self>) {}
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        // This model has one caller, so contention identifies reentrant lock order.
        // Record the violation instead of panicking/poisoning while holding a guard.
        if cancellation::unlocked(&self.cancellation) {
            self.cancellation.cancel();
        } else {
            self.violation.store(true, Ordering::Release);
        }
    }
}

fn drop_order_model(broken: bool) {
    loom::model(move || {
        let cancellation = Arc::new(Cancellation::default());
        let violation = Arc::new(AtomicBool::new(false));
        for _ in 0..2 {
            let waker = Waker::from(std::sync::Arc::new(DropProbe {
                cancellation: cancellation.clone(),
                violation: violation.clone(),
            }));
            let cx = Context::from_waker(&waker);
            let result = if broken {
                cancellation::poll_drop_locked(&cancellation, &cx)
            } else {
                cancellation.poll(&cx)
            };
            assert!(result.is_pending());
        }
        cancellation.cancel();
        assert!(
            !violation.load(Ordering::Acquire),
            "cancellation callback ran under its lock"
        );
    });
}

#[test]
fn replacing_a_waiter_drops_its_callback_after_unlocking() {
    drop_order_model(false);
}

#[test]
#[should_panic(expected = "cancellation callback ran under its lock")]
fn negative_control_detects_callback_drop_under_lock() {
    drop_order_model(true);
}
