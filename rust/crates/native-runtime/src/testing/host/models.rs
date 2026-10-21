use super::*;
use loom::sync::atomic::{AtomicBool, Ordering};

#[test]
fn abandoned_driver_and_original_wake_release_the_parked_slot() {
    loom::model(|| {
        let host = Host::new(1);
        let driver = Driver(host.clone());
        let ready = Arc::new(AtomicBool::new(false));
        let published = ready.clone();
        let (sent, waiting) = loom::sync::mpsc::channel();
        let worker = host.clone();
        let thread = loom::thread::spawn(move || {
            worker.run(|| {
                let mut sent = Some(sent);
                worker.block(std::future::poll_fn(|cx| {
                    if let Some(sent) = sent.take() {
                        sent.send(cx.waker().clone()).unwrap();
                    }
                    if ready.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }));
            });
        });
        let waking = waiting.recv().unwrap();
        let release = loom::thread::spawn(move || {
            published.store(true, Ordering::SeqCst);
            waking.wake();
        });
        drop(driver);
        release.join().unwrap();
        thread.join().unwrap();
        assert!(lock(&host.state).phase == Phase::Finished);
        assert!(
            host.pool.park(),
            "abandonment retained or returned a slot twice"
        );
        host.pool.resume();
    });
}

fn handoff(forget_ready: bool) {
    loom::model(move || {
        let host = Host::new(1);
        lock(&host.state).forget_ready = forget_ready;
        let source = Arc::new(AtomicBool::new(false));
        let published = source.clone();
        let (sender, receiver) = loom::sync::mpsc::channel::<Waker>();
        let worker = host.clone();
        let thread = loom::thread::spawn(move || {
            worker.run(|| {
                let mut sender = Some(sender);
                worker.block(std::future::poll_fn(|cx| {
                    if let Some(sender) = sender.take() {
                        sender.send(cx.waker().clone()).unwrap();
                    }
                    if source.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }));
            })
        });
        let waking = receiver.recv().unwrap();
        published.store(true, Ordering::SeqCst);
        waking.wake();
        executor::complete_io(host.drive(std::future::poll_fn(|_| {
            if lock(&host.state).phase == Phase::Finished {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })));
        thread.join().unwrap();
    });
}

#[test]
fn readiness_racing_worker_parking_cannot_be_lost() {
    handoff(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_forgetting_worker_readiness_deadlocks() {
    handoff(true);
}

fn queue_wakes(forget_io: bool) {
    loom::model(move || {
        let host = Host::new(1);
        lock(&host.state).forget_io = forget_io;
        // The original queue has three monotone states. Register the actual
        // polling waker before both capacity and terminal publications;
        // no permutation, branch or preemption bound trims this state space.
        let queue = Arc::new(Mutex::new((0u8, None::<Waker>)));
        let (started, start) = loom::sync::mpsc::channel();
        let (admitted, admission) = loom::sync::mpsc::channel();
        let worker = host.clone();
        let published = queue.clone();
        let thread = loom::thread::spawn(move || {
            start.recv().unwrap();
            let signal = {
                let mut queue = lock(&published);
                queue.0 = 1;
                queue.1.clone().unwrap()
            };
            signal.wake();
            admission.recv().unwrap();
            worker.run(|| ());
            let signal = {
                let mut queue = lock(&published);
                queue.0 = 2;
                queue.1.clone().unwrap()
            };
            signal.wake();
        });
        let mut started = Some(started);
        let mut admitted = Some(admitted);
        executor::complete_io(host.drive(std::future::poll_fn(|cx| {
            let phase = {
                let mut queue = lock(&queue);
                queue.1 = Some(cx.waker().clone());
                queue.0
            };
            if let Some(started) = started.take() {
                started.send(()).unwrap();
            }
            if phase >= 1
                && let Some(admitted) = admitted.take()
            {
                admitted.send(()).unwrap();
            }
            if phase == 2 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })));
        thread.join().unwrap();
    });
}

#[test]
fn queue_capacity_and_physical_terminal_wakes_are_latched() {
    queue_wakes(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_forgetting_io_readiness_deadlocks() {
    queue_wakes(true);
}

fn unscoped_occupant(forget_occupant: bool) {
    loom::model(move || {
        let host = Host::new(1);
        let worker = host.clone();
        let (started, start) = loom::sync::mpsc::channel();
        let (release, released) = loom::sync::mpsc::channel();
        let thread = loom::thread::spawn(move || {
            let occupant = (!forget_occupant).then(|| worker.pool.unscoped());
            started.send(()).unwrap();
            released.recv().unwrap();
            drop(occupant);
            worker.run(|| ());
        });
        start.recv().unwrap();
        let mut drive = std::pin::pin!(host.drive(std::future::poll_fn(|_| {
            if lock(&host.state).phase == Phase::Finished {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })));
        let mut cx = PollContext::from_waker(Waker::noop());
        assert!(drive.as_mut().poll(&mut cx).is_pending());
        release.send(()).unwrap();
        executor::complete_io(drive);
        thread.join().unwrap();
    });
}

#[test]
fn physical_occupants_outside_the_schedule_cannot_block_the_driver() {
    unscoped_occupant(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_forgetting_physical_occupancy_deadlocks() {
    unscoped_occupant(true);
}
