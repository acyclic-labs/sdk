//! The actual accept/cancel/drain/drop path, with its mutex replaced by Loom.
use super::*;
use loom::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};
use std::task::{Context as PollContext, Wake};

struct WakeFlag(AtomicBool);
impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn context(schedule: &Schedule) -> Context {
    Context {
        schedule: schedule.clone(),
        owned: false,
        detail: Arc::new(Mutex::new(Arc::from(""))),
    }
}

#[test]
fn native_progress_reporting_races_physical_completion_and_unwind() {
    for panic in [false, true] {
        loom::model(move || {
            let (schedule, _) = Schedule::new();
            let task = context(&schedule).accept("task");
            assert_eq!(
                schedule.pending_native_work(),
                0,
                "a blocked follow hid idle"
            );
            let native = task.0.accept("native");
            assert_eq!(schedule.pending_native_work(), 1);
            let blocking = task.0.accept("blocking");
            assert_eq!(schedule.pending_native_work(), 2);
            let worker = thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _native = native;
                    if panic {
                        panic!("native completion unwind control");
                    }
                }));
                assert_eq!(result.is_err(), panic);
            });
            assert!((1..=2).contains(&schedule.pending_native_work()));
            worker.join().unwrap();
            assert_eq!(
                schedule.pending_native_work(),
                1,
                "completion lost another owner"
            );
            drop(blocking);
            assert_eq!(schedule.pending_native_work(), 0, "native cleanup hid idle");
            drop(task);
        });
    }
}

#[test]
#[should_panic(expected = "native cleanup hid idle")]
fn missing_native_cleanup_is_detected_on_the_actual_tracker() {
    loom::model(|| {
        let (schedule, _) = Schedule::new();
        let native = context(&schedule).accept("native");
        // Deliberately omit the real completion guard's Drop path.
        std::mem::forget(native);
        assert_eq!(schedule.pending_native_work(), 0, "native cleanup hid idle");
    });
}

#[test]
fn accepted_descendants_cannot_escape_cancel_or_lose_the_terminal_wake() {
    loom::model(|| {
        let (schedule, _) = Schedule::new();
        let root = context(&schedule).accept("root");
        let owned = root.0.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let done = finished.clone();
        let worker = thread::spawn(move || {
            let child = owned.accept("child");
            drop(root);
            // This independent fact precedes the final physical completion.
            done.store(true, Ordering::SeqCst);
            drop(child);
        });
        schedule.cancel();
        let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Waker::from(flag.clone());
        let mut cx = PollContext::from_waker(&waker);
        let mut drained = Box::pin(schedule.drained());
        let pending = match drained.as_mut().poll(&mut cx) {
            Poll::Pending => true,
            Poll::Ready(()) => {
                assert!(
                    finished.load(Ordering::SeqCst),
                    "drain escaped accepted work"
                );
                false
            }
        };
        worker.join().unwrap();
        if pending {
            assert!(flag.0.load(Ordering::SeqCst), "terminal wake lost");
            assert!(drained.as_mut().poll(&mut cx).is_ready());
        }
    });
}

#[test]
#[should_panic(expected = "missing registration was accepted")]
fn missing_registration_is_detected_on_the_real_tracker() {
    loom::model(|| {
        let (schedule, _) = Schedule::new();
        let root = context(&schedule).accept("root");
        // Broken tracker loses precisely the registration made by accept.
        schedule.0.state.lock().unwrap().active = 0;
        schedule.cancel();
        let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Waker::from(flag);
        let mut cx = PollContext::from_waker(&waker);
        let ready = Box::pin(schedule.drained())
            .as_mut()
            .poll(&mut cx)
            .is_ready();
        schedule.0.state.lock().unwrap().active = 1;
        drop(root);
        assert!(!ready, "missing registration was accepted");
    });
}

#[test]
#[should_panic(expected = "terminal wake lost")]
fn missing_terminal_wake_is_detected_on_the_real_tracker() {
    loom::model(|| {
        let (schedule, _) = Schedule::new();
        let root = context(&schedule).accept("root");
        schedule.cancel();
        let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Waker::from(flag.clone());
        let mut cx = PollContext::from_waker(&waker);
        let mut drained = Box::pin(schedule.drained());
        assert!(drained.as_mut().poll(&mut cx).is_pending());
        // Broken completion drops the registered terminal waiter.
        schedule.0.state.lock().unwrap().idle = None;
        drop(root);
        assert!(drained.as_mut().poll(&mut cx).is_ready());
        assert!(flag.0.load(Ordering::SeqCst), "terminal wake lost");
    });
}
