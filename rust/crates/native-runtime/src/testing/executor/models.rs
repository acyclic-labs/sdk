//! The executor's real readiness and cancellation locks, through the crate shim.
use super::*;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn shutdown_racing_a_pending_poll_cannot_restore_owned_work() {
    loom::model(|| {
        struct DropProbe(Arc<Executor>, Arc<AtomicUsize>);
        impl Future for DropProbe {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut PollContext<'_>) -> Poll<()> {
                Poll::Pending
            }
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                // A destructor can reenter the executor; never run it locked.
                self.0.trace();
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
        let executor = Executor::new(17);
        let dropped = Arc::new(AtomicUsize::new(0));
        let task = executor.register(Some(Box::pin(DropProbe(executor.clone(), dropped.clone()))));
        let cx = PollContext::from_waker(Waker::noop());
        let (id, work, _) = executor.next(&cx, None).unwrap();
        let polling = executor.clone();
        let thread = loom::thread::spawn(move || polling.polled(id, work, "pending"));
        executor.shutdown();
        thread.join().unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(lock(&executor.0).tasks.is_empty());
        assert!(executor.next(&cx, None).is_none());
        task.wake_by_ref();
        assert!(lock(&executor.0).ready.is_empty());
    });
}

struct Flag(AtomicBool);
impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn readiness(forget_wake: bool) {
    loom::model(move || {
        let executor = Executor::new(11);
        let published = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let input = published.clone();
        let output = completed.clone();
        let task = executor.register(Some(Box::pin(std::future::poll_fn(move |_| {
            if input.load(Ordering::SeqCst) {
                output.store(true, Ordering::SeqCst);
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))));
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(flag.clone());
        let cx = PollContext::from_waker(&waker);
        executor.poll_owned(&cx);
        lock(&executor.0).forget_wake = forget_wake;
        let producer = loom::thread::spawn(move || {
            published.store(true, Ordering::SeqCst);
            task.wake_by_ref();
        });
        // Race the producer with the driver's transition into its idle state.
        executor.poll_owned(&cx);
        producer.join().unwrap();
        assert!(flag.0.load(Ordering::SeqCst), "driver wake was lost");
        executor.poll_owned(&cx);
        assert!(completed.load(Ordering::SeqCst), "ready task was lost");
        assert!(lock(&executor.0).live.is_empty());
    });
}

#[test]
fn ready_publication_cannot_lose_a_task_or_driver_wake() {
    readiness(false);
}

#[test]
#[should_panic(expected = "driver wake was lost")]
fn missing_ready_publication_negative_control() {
    readiness(true);
}

#[test]
fn concurrent_roots_retain_both_driver_wakes() {
    loom::model(|| {
        let executor = Executor::new(11);
        let roots = [executor.register(None), executor.register(None)];
        let flags = [
            Arc::new(Flag(AtomicBool::new(false))),
            Arc::new(Flag(AtomicBool::new(false))),
        ];
        let wakers = [Waker::from(flags[0].clone()), Waker::from(flags[1].clone())];
        for index in 0..2 {
            let cx = PollContext::from_waker(&wakers[index]);
            let (id, task, cancelled) = executor.next(&cx, Some(roots[index].id)).unwrap();
            assert_eq!(id, roots[index].id);
            assert!(task.is_none() && !cancelled);
            flags[index].0.store(false, Ordering::SeqCst);
        }
        let root = roots[0].clone();
        let producer = loom::thread::spawn(move || root.wake_by_ref());
        roots[1].wake_by_ref();
        producer.join().unwrap();
        for index in 0..2 {
            assert!(
                flags[index].0.load(Ordering::SeqCst),
                "root driver wake was lost"
            );
            let cx = PollContext::from_waker(&wakers[index]);
            assert_eq!(
                executor.next(&cx, Some(roots[index].id)).unwrap().0,
                roots[index].id
            );
            executor.retire(roots[index].id);
        }
    });
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn concurrent_abort_drops_the_original_future_once() {
    loom::model(|| {
        let executor = Executor::new(11);
        let dropped = Arc::new(AtomicUsize::new(0));
        let guard = DropCount(dropped.clone());
        let task = executor.register(Some(Box::pin(async {
            let _guard = guard;
            std::future::pending::<()>().await;
        })));
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(flag);
        let cx = PollContext::from_waker(&waker);
        let producer = loom::thread::spawn(move || {
            task.executor.abort(task.id);
        });
        executor.poll_owned(&cx);
        producer.join().unwrap();
        executor.poll_owned(&cx);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let state = lock(&executor.0);
        assert!(state.tasks.is_empty() && state.ready.is_empty() && state.live.is_empty());
    });
}

fn completion(omit_latch: bool) {
    loom::model(move || {
        let (sender, receiver) = loom::sync::mpsc::channel::<Waker>();
        let published = Arc::new(AtomicBool::new(false));
        let ready = published.clone();
        let worker = loom::thread::spawn(move || {
            let wake = receiver.recv().unwrap();
            ready.store(true, Ordering::SeqCst);
            wake.wake();
        });
        let mut sender = Some(sender);
        let wake = Arc::new(CompletionWake {
            ready: Mutex::new(false),
            signal: Condvar::new(),
            omit_latch,
        });
        wait_io(
            std::future::poll_fn(|cx| {
                if let Some(sender) = sender.take() {
                    sender.send(cx.waker().clone()).unwrap();
                }
                if published.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
            wake,
        );
        worker.join().unwrap();
    });
}

#[test]
fn real_completion_wait_latches_early_and_late_wakes() {
    completion(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn missing_real_completion_latch_negative_control() {
    completion(true);
}
