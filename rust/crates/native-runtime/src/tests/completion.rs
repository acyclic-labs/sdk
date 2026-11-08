use super::*;

#[test]
fn original_driver_keeps_publication_when_unused_observer_retirement_unwinds() -> io::Result<()> {
    for terminal in 0..3 {
        let budget = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = budget.clone();
        let (mut observation, producer) = NativeCompletion::<usize>::from_driver(&meter)?;
        let producer = Arc::new(Mutex::new(Some(producer)));
        let armed = Arc::new(AtomicBool::new(false));
        let waker = acyclic_test_support::waker::callbacks(
            {
                let producer = Arc::clone(&producer);
                let armed = Arc::clone(&armed);
                move || {
                    let producer = producer.lock().unwrap().take().unwrap();
                    match terminal {
                        0 => producer.complete(Ok(37)),
                        1 => producer.complete(Err(io::ErrorKind::PermissionDenied.into())),
                        _ => drop(producer),
                    }
                    armed.store(true, Ordering::Release);
                }
            },
            {
                let armed = Arc::clone(&armed);
                move || {
                    if armed.swap(false, Ordering::AcqRel) {
                        std::panic::panic_any("original unused observer retirement");
                    }
                }
            },
        );
        let retired = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Pin::new(&mut observation).poll(&mut Context::from_waker(&waker))
        }));
        assert_eq!(
            retired.unwrap_err().downcast_ref::<&str>(),
            Some(&"original unused observer retirement")
        );
        let Poll::Ready(result) =
            Pin::new(&mut observation).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("unused observer retirement discarded the original publication");
        };
        match terminal {
            0 => assert_eq!(result?, 37),
            1 => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied),
            _ => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe),
        }
        drop(observation);
        drop(waker);
        drop(producer);
        assert_eq!(budget.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn original_driver_replacement_preserves_reentrant_publication_and_unwind() -> io::Result<()> {
    struct Retired {
        original: std::sync::Weak<CompletionMutex<CompletionState<usize>>>,
        publication: Arc<Mutex<Option<NativeCompletionProducer<usize>>>>,
        panic: bool,
    }
    impl Wake for Retired {
        fn wake(self: Arc<Self>) {
            panic!("unpublished original observer was woken");
        }
    }
    impl Drop for Retired {
        fn drop(&mut self) {
            let original = self.original.upgrade().expect("original producer remains");
            assert!(
                original.try_lock().is_ok(),
                "original observer replacement retained the completion lock"
            );
            // Publication reenters the real completion while its old observer
            // retires. No timer, thread or substitute completion drives it.
            let producer = self.publication.lock().unwrap().take().unwrap();
            producer.complete(Ok(37));
            if self.panic {
                std::panic::panic_any("original replacement observer retirement");
            }
        }
    }
    struct Notified(AtomicU64);
    impl Wake for Notified {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    for panic in [false, true] {
        let budget = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = budget.clone();
        let (mut observation, producer) = NativeCompletion::<usize>::from_driver(&meter)?;
        let original = Arc::clone(observation.state.as_ref().unwrap());
        // Retain this owner outside the callback so the negative control's
        // assertion can unwind before the original producer is dropped.
        let publication = Arc::new(Mutex::new(Some(producer)));
        let first = Waker::from(Arc::new(Retired {
            original: Arc::downgrade(&original),
            publication: Arc::clone(&publication),
            panic,
        }));
        assert!(
            Pin::new(&mut observation)
                .poll(&mut Context::from_waker(&first))
                .is_pending()
        );
        drop(first);
        let notified = Arc::new(Notified(AtomicU64::new(0)));
        let next = Waker::from(Arc::clone(&notified));
        let replacement = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Pin::new(&mut observation).poll(&mut Context::from_waker(&next))
        }));
        assert!(
            original.try_lock().is_ok(),
            "original observer replacement poisoned its completion"
        );
        if panic {
            assert_eq!(
                replacement.unwrap_err().downcast_ref::<&str>(),
                Some(&"original replacement observer retirement")
            );
        } else {
            assert!(matches!(replacement, Ok(Poll::Pending)));
        }
        assert_eq!(notified.0.load(Ordering::Acquire), 1);
        assert!(
            publication
                .lock()
                .map_err(|_| io::Error::other("original publication lock poisoned"))?
                .is_none()
        );
        assert!(matches!(
            Pin::new(&mut observation).poll(&mut Context::from_waker(&next)),
            Poll::Ready(Ok(37))
        ));
        drop(observation);
        drop(original);
        assert_eq!(budget.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn cancelled_original_driver_retires_observer_outside_completion_lock() -> io::Result<()> {
    struct Observer {
        dropped: Arc<AtomicBool>,
        woken: Arc<AtomicBool>,
        panic: bool,
    }
    impl Wake for Observer {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Release);
        }
    }
    impl Drop for Observer {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
            if self.panic {
                std::panic::panic_any("original driver observer retirement");
            }
        }
    }

    for panic in [false, true] {
        let budget = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = budget.clone();
        let (mut observation, producer) = NativeCompletion::<usize>::from_driver(&meter)?;
        let original = Arc::clone(observation.state.as_ref().unwrap());
        let dropped = Arc::new(AtomicBool::new(false));
        let woken = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(Observer {
            dropped: Arc::clone(&dropped),
            woken: Arc::clone(&woken),
            panic,
        }));
        assert!(
            Pin::new(&mut observation)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        let retired = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(observation)));
        assert!(
            dropped.load(Ordering::Acquire),
            "original observer was retained"
        );
        assert!(
            original.try_lock().is_ok(),
            "original observer was destroyed under the completion lock"
        );
        assert!(
            !woken.load(Ordering::Acquire),
            "cancelled observer was woken"
        );
        if panic {
            assert_eq!(
                retired.unwrap_err().downcast_ref::<&str>(),
                Some(&"original driver observer retirement")
            );
        } else {
            assert!(retired.is_ok());
        }
        // Observer cancellation cannot consume the original driver's physical
        // terminal custody or release its allocation before publication.
        assert!(budget.memory.load(Ordering::Acquire) > 0);
        producer.complete(Ok(37));
        drop(original);
        assert_eq!(budget.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn original_driver_publication_preserves_terminal_wake_and_allocation_custody() -> io::Result<()> {
    #[derive(Default)]
    struct ObservedWake(AtomicU64);
    impl Wake for ObservedWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    for registered in [false, true] {
        for observed in [false, true] {
            for terminal in 0..3 {
                let budget = Arc::new(ReadBudgetMeter::new(0, 0));
                let meter: Arc<dyn NativeReadMeter> = budget.clone();
                let mut pair = None;
                let allocated = acyclic_test_support::measure(|| {
                    pair = Some(NativeCompletion::<usize>::from_driver(&meter));
                });
                let (mut task, producer) = pair.ok_or(io::ErrorKind::Other)??;
                assert_eq!(allocated.count_current, 2);
                assert_eq!(
                    budget.memory.load(Ordering::Acquire),
                    allocated.bytes_current as u64
                );
                assert_eq!(budget.used_operations.load(Ordering::Acquire), 0);
                let allocations = budget.used_allocations.load(Ordering::Acquire);
                budget.deny_allocations.store(true, Ordering::Release);
                let wake = Arc::new(ObservedWake::default());
                let waker = Waker::from(wake.clone());
                let mut context = Context::from_waker(&waker);
                if registered {
                    // Both cancelled waits retain the original producer and
                    // reuse their notifier; neither may allocate after birth.
                    for _ in 0..2 {
                        assert!(task.wait_interruptible(std::future::ready(()))?.is_none());
                    }
                    assert!(Pin::new(&mut task).poll(&mut context).is_pending());
                }
                let mut task = observed.then_some(task);
                if !observed {
                    assert!(budget.memory.load(Ordering::Acquire) > 0);
                }
                match terminal {
                    0 => producer.complete(Ok(37)),
                    1 => producer.complete(Err(io::ErrorKind::PermissionDenied.into())),
                    _ => drop(producer),
                }
                if let Some(task) = &mut task {
                    if registered {
                        assert_eq!(wake.0.load(Ordering::Acquire), 1);
                    }
                    // A missing publication fails here without waiting for a
                    // timeout, including the lost-producer negative control.
                    let Poll::Ready(result) = Pin::new(task).poll(&mut context) else {
                        panic!("original driver terminal did not publish");
                    };
                    match terminal {
                        0 => assert_eq!(result?, 37),
                        1 => {
                            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied)
                        }
                        _ => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe),
                    }
                }
                drop(task);
                assert_eq!(budget.used_allocations.load(Ordering::Acquire), allocations);
                assert_eq!(budget.used_operations.load(Ordering::Acquire), 0);
                assert_eq!(budget.memory.load(Ordering::Acquire), 0);
            }
        }
    }
    Ok(())
}

fn aborted_child(
    test: &str,
    marker: &str,
    message: &str,
    run: impl FnOnce() -> io::Result<()> + std::panic::UnwindSafe,
) -> io::Result<()> {
    if std::env::var_os(marker).is_some() {
        // Only process termination can satisfy the parent. A returned error
        // or an unwound body must not become a failing libtest exit instead.
        let _ = std::panic::catch_unwind(run);
        std::process::exit(0);
    }
    let output = crate::process_output(
        crate::ProcessCommand::new(std::env::current_exe()?)
            .args(["--exact", test, "--nocapture"])
            .env(marker, "1"),
        CaptureSchedule::Unbounded,
        8 * 1024 * 1024,
    )?;
    assert!(
        !output.status.success(),
        "fail-stop body returned or unwound into its original caller"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains(message));
    Ok(())
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn accepted_callback_cannot_disappear_without_terminal_completion() -> io::Result<()> {
    const MARKER: &str = "ACYCLIC_DROPPED_COMPLETION_CHILD";
    aborted_child(
        "tests::completion::accepted_callback_cannot_disappear_without_terminal_completion",
        MARKER,
        "dropping accepted native callback",
        || {
            eprintln!("dropping accepted native callback");
            let _ = submit_file_io(
                FileCompletion::new(
                    Arc::new(CompletionMutex::new(CompletionState::<()>::default())),
                    None,
                    None,
                    None,
                    obs::OperationSpan::new(tracing::Span::none()),
                ),
                |callback| {
                    drop(callback);
                    Ok(())
                },
            );
            Ok(())
        },
    )
}

#[test]
fn native_file_worker_unwind_is_fail_stop() -> io::Result<()> {
    const MARKER: &str = "ACYCLIC_NATIVE_UNWIND_CHILD_TEST";
    aborted_child(
        "tests::completion::native_file_worker_unwind_is_fail_stop",
        MARKER,
        "injected native worker unwind",
        || {
            let file =
                NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
            let _ = complete(file.control_async(
                |_, _| {
                    eprintln!("injected native worker unwind");
                    std::panic::resume_unwind(Box::new("injected native worker unwind"));
                },
                None,
            )?);
            Ok(())
        },
    )
}

#[test]
fn host_task_panic_preserves_payload_and_retires_controls() -> io::Result<()> {
    struct RetainedPanic(mpsc::Sender<()>);
    impl Drop for RetainedPanic {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    initialize_native_io_services()?;
    for observed in [false, true] {
        let temporary = tempfile::NamedTempFile::new()?;
        std::fs::write(temporary.path(), b"abc")?;
        let mut file = temporary.reopen()?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let body_meter = meter.clone();
        let (retired, dropped) = mpsc::channel();
        let payload = Box::new(RetainedPanic(retired));
        let address = std::ptr::from_ref(payload.as_ref()) as usize;
        let (entered_tx, entered) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let mut task = run_blocking_io(
            move || -> io::Result<()> {
                let mut buffer = [0; 3];
                body_meter.admit_read(buffer.len())?;
                let completed = file.read(&mut buffer)?;
                body_meter.record_read(completed)?;
                assert_eq!(completed, 3);
                assert_eq!(&buffer, b"abc");
                entered_tx.send(()).map_err(io::Error::other)?;
                released.recv().map_err(io::Error::other)?;
                std::panic::resume_unwind(payload);
            },
            Some((meter.clone(), observed)),
        )?;
        submit(&mut task);
        entered.recv().map_err(io::Error::other)?;
        if observed {
            release.send(()).map_err(io::Error::other)?;
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                task.wait(None, std::future::pending(), || {})
            }))
            .expect_err("original panic");
            assert_eq!(
                std::ptr::from_ref(panic.downcast_ref::<RetainedPanic>().unwrap()) as usize,
                address
            );
            drop(panic);
        } else {
            drop(task);
            release.send(()).map_err(io::Error::other)?;
        }
        dropped.recv().map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "panic payload retirement lost")
        })?;
        meter.wait_for_memory_retirement()?;
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
    }
    Ok(())
}

#[test]
fn interrupted_wait_retains_the_native_producer_until_terminal_completion() -> io::Result<()> {
    initialize_native_io_services()?;
    for dropped in [false, true] {
        let meter = Arc::new(ReadBudgetMeter::new(0, 0));
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
        let (registered, waiting) = mpsc::channel();
        let mut task = run_blocking_io(
            move || -> io::Result<usize> {
                entered.send(()).map_err(io::Error::other)?;
                blocked.recv().map_err(io::Error::other)?;
                Ok(37)
            },
            Some((meter.clone(), true)),
        )?;
        submit(&mut task);
        started.recv().map_err(io::Error::other)?;
        let allocations = meter.used_allocations.load(Ordering::Acquire);
        meter.deny_allocations.store(true, Ordering::Release);
        let caller = std::thread::spawn(move || {
            let mut registered = Some(registered);
            let result = task.wait_interruptible(std::future::poll_fn(|context| {
                match Pin::new(&mut cancelled).poll(context) {
                    Poll::Ready(_) => Poll::Ready(()),
                    Poll::Pending => {
                        if let Some(registered) = registered.take() {
                            let _ = registered.send(());
                        }
                        Poll::Pending
                    }
                }
            }));
            (task, result)
        });
        waiting.recv().map_err(io::Error::other)?;
        cancel
            .send(())
            .map_err(|()| io::Error::other("cancellation waiter disappeared"))?;
        let (task, result) = caller
            .join()
            .map_err(|_| io::Error::other("waiter panicked"))?;
        assert!(result?.is_none());
        if !dropped {
            release.send(()).map_err(io::Error::other)?;
        }
        drop(release);
        let result = task.wait(None, std::future::pending(), || {})?;
        if dropped {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
        } else {
            assert_eq!(result?, 37);
        }
        assert_eq!(meter.used_allocations.load(Ordering::Acquire), allocations);
    }
    Ok(())
}

#[test]
fn measured_sync_wait_expired_deadline_stops_before_body() -> io::Result<()> {
    initialize_native_io_services()?;
    let meter = Arc::new(ReadBudgetMeter::new(0, 0));
    let called = Arc::new(AtomicBool::new(false));
    let body_called = called.clone();
    let task = run_blocking_io(
        move || body_called.store(true, Ordering::Release),
        Some((meter.clone(), false)),
    )?;
    let mut deadline_calls = 0;
    let error = task
        .wait(Some(Instant::now()), std::future::pending(), || {
            deadline_calls += 1;
            meter.cancelled.store(true, Ordering::Release);
        })
        .unwrap_err();
    assert_eq!(deadline_calls, 1);
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn measured_sync_wait_cancellation_wake_drains_real_read_prefix() -> io::Result<()> {
    initialize_native_io_services()?;
    for expired in [false, true] {
        let temporary = tempfile::NamedTempFile::new()?;
        std::fs::write(temporary.path(), b"abc")?;
        let mut file = temporary.reopen()?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let body_meter = meter.clone();
        let (entered_tx, entered) = mpsc::sync_channel(1);
        let (release_tx, released) = mpsc::sync_channel(1);
        let (retired_tx, retired) = mpsc::channel();
        let terminal = Arc::new(AtomicBool::new(false));
        let body_terminal = terminal.clone();
        let mut task = run_blocking_io(
            move || -> io::Result<usize> {
                let mut buffer = [0; 3];
                body_meter.admit_read(buffer.len())?;
                let count = file.read(&mut buffer)?;
                body_meter.record_read(count)?;
                entered_tx.send(()).map_err(io::Error::other)?;
                released.recv().map_err(io::Error::other)?;
                body_terminal.store(true, Ordering::Release);
                Ok(count)
            },
            Some((meter.clone(), false)),
        )?;
        submit(&mut task);
        let release = || {
            entered.recv().unwrap();
            *meter
                .memory_retired
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(retired_tx.clone());
            meter.cancelled.store(true, Ordering::Release);
            release_tx.send(()).unwrap();
        };
        let mut first = true;
        let cancellation = std::future::poll_fn(|context| {
            if expired {
                return Poll::Pending;
            }
            if std::mem::take(&mut first) {
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                release();
                Poll::Ready(())
            }
        });
        assert_eq!(
            task.wait(expired.then(Instant::now), cancellation, release)??,
            3
        );
        assert!(terminal.load(Ordering::Acquire));
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        // Result visibility can precede the worker dropping its final waker.
        // Join actual retirement rather than assuming an early meter refund.
        retired.recv().map_err(io::Error::other)?;
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}
