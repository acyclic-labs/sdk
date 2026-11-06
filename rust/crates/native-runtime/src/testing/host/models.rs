use super::*;
use loom::sync::atomic::{AtomicBool, Ordering};

#[test]
fn callback_readiness_cannot_overtake_its_driver_notification() {
    loom::model(|| {
        struct Publishing {
            started: loom::sync::mpsc::Sender<()>,
            published: Arc<AtomicBool>,
        }
        impl Wake for Publishing {
            fn wake(self: Arc<Self>) {
                self.started.send(()).unwrap();
                loom::thread::yield_now();
                self.published.store(true, Ordering::SeqCst);
            }
        }
        let host = Host::new(1);
        let ready = Arc::new(AtomicBool::new(false));
        let callback_ready = ready.clone();
        let worker = host.clone();
        let thread = loom::thread::spawn(move || {
            worker.run(|| {
                worker.block(std::future::poll_fn(|_| {
                    if callback_ready.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }));
            });
        });
        let mut state = host.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.phase != Phase::Parked {
            state = host
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        let published = Arc::new(AtomicBool::new(false));
        let (started, notification) = loom::sync::mpsc::channel();
        state.driver = Some(Waker::from(Arc::new(Publishing {
            started,
            published: published.clone(),
        })));
        drop(state);
        let waking = host.clone();
        let notify = loom::thread::spawn(move || {
            ready.store(true, Ordering::SeqCst);
            waking.wake();
        });
        notification.recv().unwrap();
        executor::complete_io(host.drive(async {}));
        assert!(
            published.load(Ordering::SeqCst),
            "callback readiness overtook its driver notification"
        );
        notify.join().unwrap();
        thread.join().unwrap();
        assert!(host.pool.park(), "callback returned its parked slot twice");
        host.pool.resume();
    });
}

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

#[test]
fn readiness_racing_worker_parking_cannot_be_lost() {
    loom::model(|| {
        let host = Host::new(1);
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
fn queue_capacity_and_physical_terminal_wakes_are_latched() {
    loom::model(|| {
        let host = Host::new(1);
        // A scripted capacity/terminal source exercises the actual Host latch.
        // Native admission and retirement have their own queue properties.
        // Register the real polling waker before both publications; no
        // permutation, branch or preemption bound trims this state space.
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
fn physical_occupants_outside_the_schedule_cannot_block_the_driver() {
    for abandon in [false, true] {
        loom::model(move || {
            struct DriverWake(Mutex<bool>, Condvar);
            impl Wake for DriverWake {
                fn wake(self: Arc<Self>) {
                    *lock(&self.0) = true;
                    self.1.notify_one();
                }
            }
            let host = Host::new(1);
            let worker = host.clone();
            let reply = Arc::new(AtomicBool::new(false));
            let callback = reply.clone();
            let ran = Arc::new(AtomicBool::new(false));
            let callback_ran = ran.clone();
            let (started, start) = loom::sync::mpsc::channel();
            let (release, released) = loom::sync::mpsc::channel();
            let (callback_wake, callback_wait) = loom::sync::mpsc::channel();
            let thread = loom::thread::spawn(move || {
                let occupant = worker.pool.unscoped();
                started.send(()).unwrap();
                released.recv().unwrap();
                drop(occupant);
                worker.run(|| {
                    callback_ran.store(true, Ordering::SeqCst);
                    let mut callback_wake = Some(callback_wake);
                    worker.block(std::future::poll_fn(|cx| {
                        if let Some(wake) = callback_wake.take() {
                            wake.send(cx.waker().clone()).unwrap();
                        }
                        if callback.load(Ordering::SeqCst) {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    }));
                });
            });
            start.recv().unwrap();
            let mut drive = Box::pin(host.drive(std::future::poll_fn(|_| {
                if lock(&host.state).phase == Phase::Finished {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })));
            let awakened = Arc::new(DriverWake(Mutex::new(false), Condvar::new()));
            let waker = Waker::from(awakened.clone());
            let mut cx = PollContext::from_waker(&waker);
            assert!(drive.as_mut().poll(&mut cx).is_pending());
            release.send(()).unwrap();
            // The queued worker starts after its driver yielded to a different
            // occupant, then needs that SAME driver to produce its callback reply.
            // Observe the actual handoff before publishing any reply readiness.
            let mut resumed = awakened.0.lock().unwrap_or_else(PoisonError::into_inner);
            while !*resumed {
                resumed = awakened.1.wait(resumed).unwrap();
            }
            drop(resumed);
            assert!(
                !ran.load(Ordering::SeqCst),
                "worker ran before its original driver admitted the first step"
            );
            if abandon {
                // Accepted work still owns its completion after its driver leaves.
                drop(drive);
                reply.store(true, Ordering::SeqCst);
                callback_wait.recv().unwrap().wake();
            } else {
                // The original driver transfers the first-step permit, then waits
                // for this callback's actual reply readiness.
                assert!(drive.as_mut().poll(&mut cx).is_pending());
                reply.store(true, Ordering::SeqCst);
                callback_wait.recv().unwrap().wake();
                executor::complete_io(drive);
            }
            thread.join().unwrap();
            assert!(
                host.pool.park(),
                "startup retained or returned a slot twice"
            );
            host.pool.resume();
        });
    }
}

#[test]
fn physical_terminal_cannot_overtake_its_worker_step() {
    loom::model(|| {
        use crate::TaskTerminal;
        let host = Host::new(1);
        let terminal = Arc::new(crate::CompletionMutex::new(crate::CompletionState::<()> {
            task_pending: true,
            result: Some(Ok(())),
            ..Default::default()
        }));
        let retired = Arc::new(AtomicBool::new(false));
        let (published, publication) = loom::sync::mpsc::channel();
        let (release, retirement) = loom::sync::mpsc::channel();
        let worker = host.clone();
        let completed = terminal.clone();
        let physical_retired = retired.clone();
        let thread = loom::thread::spawn(move || {
            worker.run(|| {
                // The actual terminal publisher precedes Host's Finished guard.
                // Retain that publication window while its original observer runs.
                drop(completed.publish(&tracing::Span::none()));
                published.send(()).unwrap();
                retirement.recv().unwrap();
                physical_retired.store(true, Ordering::SeqCst);
            });
        });
        publication.recv().unwrap();
        let mut release = Some(release);
        executor::complete_io(host.drive(std::future::poll_fn(|cx| {
            if let Some(release) = release.take() {
                release.send(()).unwrap();
            }
            terminal.test_poll_completion(cx.waker())
        })));
        assert!(
            retired.load(Ordering::SeqCst),
            "physical terminal overtook its worker step"
        );
        assert!(lock(&host.state).phase == Phase::Finished);
        thread.join().unwrap();
    });
}
