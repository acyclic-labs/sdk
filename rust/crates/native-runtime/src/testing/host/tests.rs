use super::*;

#[test]
fn abandoned_replay_releases_parked_native_and_blocking_workers() {
    for native in [false, true] {
        for wake in [0, 1, 2] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let (schedule, mut events) = Schedule::seeded(29);
            let (answer, waiting) = oneshot::channel::<()>();
            let (started, running) = oneshot::channel();
            let (finished, completion) = std::sync::mpsc::channel();
            let work = move || {
                let host = Host::current().expect("receiving host custody");
                started.send(host).ok().unwrap();
                let result = blocking_on(waiting);
                finished.send(result.is_ok()).unwrap();
            };
            let host = runtime.block_on(async {
                let mut root = Box::pin(schedule.scope(async {
                    if native {
                        crate::run_blocking_io(work, None).unwrap().await.unwrap();
                    } else {
                        spawn_blocking(work).await.unwrap();
                    }
                }));
                let mut running = std::pin::pin!(running);
                let host = loop {
                    tokio::select! {
                        biased;
                        _ = &mut root => panic!("worker completed before its original wake"),
                        event = events.recv() => event.unwrap().release(),
                        host = &mut running => break host.unwrap(),
                    }
                };
                // The worker hands over its host before it blocks; wait for
                // it to reach the original future's Pending.
                let mut state = host.state.lock().unwrap_or_else(PoisonError::into_inner);
                while matches!(state.phase, Phase::Starting | Phase::Running) {
                    state = host
                        .changed
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                assert!(state.phase == Phase::Parked);
                drop(state);
                drop(root);
                host
            });
            if wake == 0 {
                answer.send(()).unwrap();
                drop(schedule);
            } else {
                drop(schedule);
                if wake == 1 {
                    answer.send(()).unwrap();
                } else {
                    drop(answer);
                }
            }
            // The abandoned host resumes its worker on the original wake
            // itself, before the worker's result is awaited.
            assert!(
                lock(&host.state).phase != Phase::Parked,
                "the original wake left the abandoned worker parked"
            );
            assert_eq!(
                completion.recv().unwrap(),
                wake != 2,
                "original channel result changed after abandonment"
            );
            // Every parked slot was returned exactly once, including a wake
            // already latched when the last controller was dropped.
            let threads = crate::workers(&crate::BLOCKING_WORKERS, false)
                .unwrap()
                .host_pool
                .clone();
            assert!(Arc::ptr_eq(&host.pool, &threads));
        }
    }
}

#[test]
fn parking_a_worker_does_not_invent_callback_readiness() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let host = Host::with_pool(Pool::new(1));
    let wakes = Arc::new(Count(AtomicUsize::new(0)));
    {
        let mut state = lock(&host.state);
        state.waiting_start = true;
        state.driver = Some(Waker::from(wakes.clone()));
    }
    let ready = Arc::new(AtomicBool::new(false));
    let work_ready = ready.clone();
    let worker = host.clone();
    let thread = std::thread::spawn(move || {
        worker.run(|| {
            worker.block(std::future::poll_fn(|_| {
                if work_ready.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }));
        })
    });
    let mut state = host.state.lock().unwrap_or_else(PoisonError::into_inner);
    while state.phase != Phase::Parked {
        state = host
            .changed
            .wait(state)
            .unwrap_or_else(PoisonError::into_inner);
    }
    // Readiness has not been published by the original future yet.
    let invented = wakes.0.load(Ordering::SeqCst);
    drop(state);
    ready.store(true, Ordering::SeqCst);
    host.wake_by_ref();
    executor::complete_io(host.drive(std::future::poll_fn(|_| {
        if lock(&host.state).phase == Phase::Finished {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })));
    thread.join().unwrap();
    assert_eq!(invented, 0, "parking enqueued an extra seeded task poll");
}

#[test]
fn file_batches_never_claim_a_callback_dependency() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    let file = crate::NativeFile::from_file(std::fs::File::create(path).unwrap(), None)
        .map_err(|(error, _)| error)
        .unwrap();
    let read = file.read_batch_async(vec![crate::OwnedRead {
        offset: 0,
        length: 1,
    }]);
    let write = file.write_all_batch_async(vec![crate::OwnedWrite {
        offset: 0,
        bytes: bytes::Bytes::from_static(b"x"),
    }]);
    assert!(!read.pending.as_ref().unwrap().may_wait_for_driver());
    assert!(!write.pending.as_ref().unwrap().may_wait_for_driver());
    let task = crate::run_blocking_io(|| (), None).unwrap();
    assert!(task.pending.as_ref().unwrap().may_wait_for_driver());
}

fn direct_callback(erase_scope: bool, prepared: bool) -> bool {
    crate::initialize_native_io_services().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(29);
    let meter = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (requests, mut requested) = mpsc::unbounded_channel();
            if erase_scope {
                FORGET_HOST_CONTEXT.with(|fault| fault.set(true));
            }
            let completion = crate::run_blocking_io(
                move || {
                    // Losing custody is a finite failure, rather than hanging the
                    // physical worker on the original root-response dependency.
                    let Some(context) = Context::simulation() else {
                        return false;
                    };
                    let Some(host) = Host::current() else {
                        return false;
                    };
                    if !context.owned
                        || !context
                            .host
                            .as_ref()
                            .is_some_and(|current| Arc::ptr_eq(current, &host))
                    {
                        return false;
                    }
                    let (answer, waiting) = oneshot::channel();
                    requests.send(answer).unwrap();
                    blocking_on(waiting).unwrap() == 73
                },
                Some((meter, prepared)),
            )
            .unwrap();
            let responder = async {
                if let Some(answer) = requested.recv().await {
                    answer.send(73).unwrap();
                }
            };
            tokio::join!(completion, responder).0.unwrap()
        }));
        let result = loop {
            tokio::select! {
                result = &mut root => break result,
                event = events.recv() => event.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        result
    })
}

#[test]
fn direct_native_callbacks_borrow_the_receiving_host() {
    for prepared in [false, true] {
        assert!(direct_callback(false, prepared));
        assert!(
            !direct_callback(true, prepared),
            "erased custody escaped the control"
        );
    }
}

fn inline_metered_callback(erase_scope: bool, prepared: bool) -> bool {
    crate::initialize_native_io_services().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(29);
    let meter = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (requests, mut requested) = mpsc::unbounded_channel();
            let worker = spawn_blocking(move || {
                let original = Host::current().unwrap();
                let thread = std::thread::current().id();
                if erase_scope {
                    FORGET_HOST_CONTEXT.with(|fault| fault.set(true));
                }
                crate::run_blocking_io(
                    move || {
                        let Some(context) = Context::simulation() else {
                            return false;
                        };
                        if !context
                            .host
                            .as_ref()
                            .is_some_and(|host| Arc::ptr_eq(host, &original))
                        {
                            return false;
                        }
                        let (answer, waiting) = oneshot::channel();
                        requests.send(answer).unwrap();
                        blocking_on(waiting).unwrap() == 73 && std::thread::current().id() == thread
                    },
                    Some((meter, prepared)),
                )
                .unwrap()
                // Poll directly under the physical worker's context; this
                // measured entry point must retain the same host permit.
                .wait(None, std::future::pending(), || {
                    panic!("unexpected deadline")
                })
            });
            if let Some(answer) = requested.recv().await {
                answer.send(73).unwrap();
            }
            worker.await.unwrap().unwrap()
        }));
        let result = loop {
            tokio::select! {
                result = &mut root => break result,
                event = events.recv() => event.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        result
    })
}

#[test]
fn inline_metered_waits_retain_the_original_host() {
    for prepared in [false, true] {
        assert!(inline_metered_callback(false, prepared));
        assert!(!inline_metered_callback(true, prepared));
    }
}

#[test]
fn unscoped_physical_workers_leave_the_driver_available_to_reply() {
    // Occupies every worker of the process-global host pool, so runs alone.
    if crate::tests::passed_in_exclusive_process(
        "testing::host::tests::unscoped_physical_workers_leave_the_driver_available_to_reply",
    )
    .unwrap()
    {
        return;
    }
    let workers = crate::workers(&crate::BLOCKING_WORKERS, false).unwrap();
    let (started, receive) = std::sync::mpsc::channel();
    let mut occupants = Vec::new();
    let mut replies = Vec::new();
    let mut cx = std::task::Context::from_waker(Waker::noop());
    let threads = workers.exclusive_threads();
    for index in 0..threads {
        let (reply, waiting) = std::sync::mpsc::channel();
        replies.push(reply);
        let started = started.clone();
        let mut completion = Box::pin(
            crate::run_blocking_io(
                move || {
                    started.send(()).unwrap();
                    assert_eq!(waiting.recv().unwrap(), index);
                    index
                },
                None,
            )
            .unwrap(),
        );
        assert!(completion.as_mut().poll(&mut cx).is_pending());
        occupants.push(completion);
    }
    for _ in 0..threads {
        receive.recv().unwrap();
    }
    let host = Host::for_workers(workers);
    let mut completion = crate::run_blocking_io(|| 42, None).unwrap();
    let mut job = completion.pending.take().unwrap();
    Work::bind(&mut job, None, Some(host.clone()));
    let mut drive = Box::pin(host.drive(queued(job, workers)));
    // Every physical worker is waiting for this driver. Its first poll must
    // yield before any reply is sent, even though none belongs to this Host.
    assert!(drive.as_mut().poll(&mut cx).is_pending());
    for (index, reply) in replies.into_iter().enumerate() {
        reply.send(index).unwrap();
    }
    executor::complete_io(drive).unwrap();
    assert_eq!(executor::complete_io(completion).unwrap(), 42);
    for (index, completion) in occupants.into_iter().enumerate() {
        assert_eq!(executor::complete_io(completion).unwrap(), index);
    }
}

#[test]
fn queued_workers_leave_the_executor_available_to_reply() {
    // Occupies every worker of the process-global host pool, so runs alone.
    if crate::tests::passed_in_exclusive_process(
        "testing::host::tests::queued_workers_leave_the_executor_available_to_reply",
    )
    .unwrap()
    {
        return;
    }
    let count = crate::workers(&crate::BLOCKING_WORKERS, false)
        .unwrap()
        .exclusive_threads();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(17);
    let mut completed = runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (send, mut receive) = mpsc::unbounded_channel();
            let mut workers = Vec::new();
            for index in 0..=count {
                let send = send.clone();
                workers.push(spawn_blocking(move || {
                    let (reply, waiting) = oneshot::channel();
                    send.send((index, reply)).unwrap();
                    assert_eq!(blocking_on(waiting).unwrap(), index);
                    index
                }));
            }
            drop(send);
            // Occupy every physical worker before allowing any reply. Another
            // accepted job must queue while the root remains able to answer.
            let mut replies = Vec::new();
            for _ in 0..count {
                replies.push(receive.recv().await.unwrap());
            }
            for (index, reply) in replies {
                reply.send(index).unwrap();
            }
            let (index, reply) = receive.recv().await.unwrap();
            reply.send(index).unwrap();
            let mut completed = Vec::new();
            for worker in workers {
                completed.push(worker.await.unwrap());
            }
            completed
        }));
        let result = loop {
            tokio::select! {
                result = &mut root => break result,
                event = events.recv() => event.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        result
    });
    completed.sort_unstable();
    assert_eq!(completed, (0..=count).collect::<Vec<_>>());
}

fn exchanges(seed: u64) -> (Vec<u32>, Vec<(u64, &'static str)>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(seed);
    let values = runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (send, mut receive) = mpsc::channel(1);
            let worker = spawn_blocking(move || {
                for value in 0..8 {
                    let (reply, waiting) = oneshot::channel();
                    blocking_on(send.send((value, reply))).unwrap();
                    assert_eq!(blocking_on(waiting).unwrap(), value * 2);
                }
                8
            });
            let mut values = Vec::new();
            while let Some((value, reply)) = receive.recv().await {
                values.push(value);
                reply.send(value * 2).unwrap();
            }
            assert_eq!(worker.await.unwrap(), 8);
            values
        }));
        let result = loop {
            tokio::select! {
                result = &mut root => break result,
                boundary = events.recv() => boundary.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        result
    });
    (values, schedule.polls())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn original_bounded_channels_and_replies_replay_without_blocking_the_driver(seed in proptest::prelude::any::<u64>()) {
        let first = exchanges(seed);
        proptest::prop_assert_eq!(&first.0, &(0..8).collect::<Vec<_>>());
        proptest::prop_assert_eq!(first, exchanges(seed));
    }
}

fn native_callback(
    seed: u64,
    erase_scope: bool,
) -> (Vec<u8>, Vec<String>, Vec<(u64, &'static str)>) {
    use crate::{NativeFile, OwnedRead};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"seed").unwrap();
    let file = NativeFile::from_file(std::fs::File::open(path).unwrap(), None)
        .map_err(|(error, _)| error)
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(seed);
    let bytes = runtime.block_on(async {
        tokio::time::pause();
        let mut root = Box::pin(schedule.scope(async {
            spawn_blocking(move || {
                let mut bytes = Vec::new();
                for offset in 0..4 {
                    let read = file.read_batch_async(vec![OwnedRead { offset, length: 1 }]);
                    let batch = if erase_scope {
                        // Erase only callback task ownership, retaining its
                        // seeded worker, clock, entropy and original device I/O.
                        blocking_on(CURRENT.scope(None, read))
                    } else {
                        blocking_on(read)
                    }
                    .unwrap();
                    bytes.extend_from_slice(&batch[0]);
                }
                bytes
            })
            .await
            .unwrap()
        }));
        let bytes = loop {
            tokio::select! {
                bytes = &mut root => break bytes,
                event = events.recv() => event.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        bytes
    });
    (bytes, schedule.trace(), schedule.polls())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn original_native_io_in_a_worker_callback_retains_seeded_task_ownership(
        seed in proptest::prelude::any::<u64>(),
    ) {
        let first = native_callback(seed, false);
        proptest::prop_assert_eq!(&first.0, b"seed");
        proptest::prop_assert_eq!(&first, &native_callback(seed, false));
        let owned = |trace: &[String]| trace.iter()
            .filter(|event| event.as_str() == "native:completion").count() == 4;
        proptest::prop_assert!(owned(&first.1));
        let erased = native_callback(seed, true);
        proptest::prop_assert_eq!(&erased.0, b"seed");
        proptest::prop_assert!(!owned(&erased.1), "scope-erased callback escaped the ownership oracle");
    }
}

#[test]
fn saturated_native_submission_leaves_the_simulated_deadline_available() {
    // Occupies every worker of the process-global host pool, so runs alone.
    if crate::tests::passed_in_exclusive_process(
        "testing::host::tests::saturated_native_submission_leaves_the_simulated_deadline_available",
    )
    .unwrap()
    {
        return;
    }
    let count = crate::workers(&crate::BLOCKING_WORKERS, false)
        .unwrap()
        .exclusive_threads();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(23);
    runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (send, mut receive) = mpsc::unbounded_channel();
            let mut workers = Vec::new();
            for index in 0..count {
                let send = send.clone();
                workers.push(spawn_blocking(move || {
                    let (reply, waiting) = oneshot::channel();
                    send.send((index, reply)).unwrap();
                    assert_eq!(blocking_on(waiting).unwrap(), index);
                    index
                }));
            }
            drop(send);
            let mut replies = Vec::new();
            for _ in 0..count {
                replies.push(receive.recv().await.unwrap());
            }
            // Unlike spawn_blocking, this uses submit's direct native-job path.
            // All physical workers await this root; its timer must still run.
            let mut native = Box::pin(crate::run_blocking_io(|| 42, None).unwrap());
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(1), &mut native)
                    .await
                    .is_err()
            );
            for (index, reply) in replies {
                reply.send(index).unwrap();
            }
            assert_eq!(native.await.unwrap(), 42);
            for (index, worker) in workers.into_iter().enumerate() {
                assert_eq!(worker.await.unwrap(), index);
            }
        }));
        loop {
            tokio::select! {
                () = &mut root => break,
                event = events.recv() => event.unwrap().release(),
            }
        }
        drop(root);
        schedule.drained().await;
    });
}
