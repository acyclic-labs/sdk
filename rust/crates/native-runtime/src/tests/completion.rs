use super::*;

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn accepted_callback_cannot_disappear_without_terminal_completion() -> io::Result<()> {
    const MARKER: &str = "ACYCLIC_DROPPED_COMPLETION_CHILD";
    if std::env::var_os(MARKER).is_some() {
        let state = Arc::new(Mutex::new(CompletionState::<()>::default()));
        eprintln!("dropping accepted native callback");
        let _ = submit_file_io(
            FileCompletion::new(
                state,
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
        return Err(io::Error::other("missing terminal callback was accepted"));
    }
    let output = crate::process_output(
        std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg(
                "tests::completion::accepted_callback_cannot_disappear_without_terminal_completion",
            )
            .arg("--nocapture")
            .env(MARKER, "1"),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("dropping accepted native callback"));
    Ok(())
}

#[test]
fn native_file_worker_unwind_is_fail_stop() -> io::Result<()> {
    const CHILD_MARKER: &str = "ACYCLIC_NATIVE_UNWIND_CHILD_TEST";
    if std::env::var_os(CHILD_MARKER).is_some() {
        let directory = tempfile::tempdir()?;
        let file = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(directory.path().join("panic-worker"))?,
            None,
        )
        .map_err(|(error, _)| error)?;
        let _ = complete(
            file.control_async(
                |_, _| {
                    eprintln!("injected native worker unwind");
                    std::panic::resume_unwind(Box::new("injected native worker unwind"));
                },
                None,
            )
            .expect("unmetered control construction cannot fail"),
        );
        return Err(io::Error::other(
            "native worker unwind returned to observer",
        ));
    }
    let output = crate::process_output(
        std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("tests::completion::native_file_worker_unwind_is_fail_stop")
            .arg("--nocapture")
            .env(CHILD_MARKER, "1"),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(
        !output.status.success(),
        "native worker unwind escaped fail-stop"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("injected native worker unwind"),
        "child did not reach the injected worker panic"
    );
    Ok(())
}

#[test]
fn dropped_host_observer_retires_retained_panic_after_actual_read() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("unobserved-panic-prefix");
    std::fs::write(&path, b"abc")?;
    let mut file = File::open(path)?;
    let meter = Arc::new(ReadBudgetMeter::new(1, 3));
    let body_meter = Arc::clone(&meter);
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let body_dropped = Arc::clone(&payload_dropped);
    struct RetainedHostPanic {
        dropped: Arc<AtomicBool>,
    }
    impl Drop for RetainedHostPanic {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }
    let (prefix_sender, prefix_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let mut task = run_blocking_io(
        move || -> io::Result<()> {
            let mut buffer = [0_u8; 3];
            body_meter.admit_read(buffer.len())?;
            let completed = file.read(&mut buffer)?;
            body_meter.record_read(completed)?;
            assert_eq!(completed, 3);
            assert_eq!(&buffer, b"abc");
            prefix_sender.send(()).map_err(io::Error::other)?;
            release_receiver.recv().map_err(io::Error::other)?;
            std::panic::panic_any(RetainedHostPanic {
                dropped: body_dropped,
            });
        },
        Some((meter.clone(), false)),
    )?;
    let waker = completion_waker();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            Pin::new(&mut task)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        match prefix_receiver.try_recv() {
            Ok(()) => break,
            Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                std::thread::park_timeout(Duration::from_millis(1))
            }
            Err(error) => return Err(io::Error::other(error)),
        }
    }
    drop(task);
    release_sender.send(()).map_err(io::Error::other)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while (!payload_dropped.load(Ordering::Acquire) || meter.memory.load(Ordering::Acquire) != 0)
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert!(payload_dropped.load(Ordering::Acquire));
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    assert_eq!(meter.read.load(Ordering::Acquire), 3);
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
        .expect_err("expired deadline must stop the unsubmitted body");
    assert_eq!(deadline_calls, 1);
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn measured_sync_wait_cancellation_wake_drains_real_read_prefix() -> io::Result<()> {
    initialize_native_io_services()?;
    for expired_deadline in [false, true] {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("sync-cancel-wake");
        std::fs::write(&path, b"abc")?;
        let mut file = File::open(path)?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let terminal = Arc::new(AtomicBool::new(false));
        let body_gate = gate.clone();
        let body_terminal = terminal.clone();
        let body_meter = meter.clone();
        let mut task = run_blocking_io(
            move || -> io::Result<usize> {
                let mut buffer = [0_u8; 3];
                body_meter.admit_read(buffer.len())?;
                let completed = file.read(&mut buffer)?;
                body_meter.record_read(completed)?;
                let (lock, changed) = &*body_gate;
                let mut state = lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.0 = true;
                changed.notify_all();
                while !state.1 {
                    state = changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                body_terminal.store(true, Ordering::Release);
                Ok(completed)
            },
            Some((meter.clone(), false)),
        )?;
        let waker = completion_waker();
        let mut context = Context::from_waker(&waker);
        loop {
            assert!(Pin::new(&mut task).poll(&mut context).is_pending());
            if task.pending.is_none() {
                break;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let release = || {
            let (lock, changed) = &*gate;
            let mut state = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !state.0 {
                state = changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            meter.cancelled.store(true, Ordering::Release);
            state.1 = true;
            changed.notify_all();
        };
        let mut first_poll = true;
        let cancellation = std::future::poll_fn(|context| {
            if expired_deadline {
                return Poll::Pending;
            }
            if std::mem::take(&mut first_poll) {
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                release();
                Poll::Ready(())
            }
        });
        let completed = task.wait(expired_deadline.then(Instant::now), cancellation, release)??;
        assert!(terminal.load(Ordering::Acquire));
        assert_eq!(completed, 3);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn observed_host_task_panic_preserves_payload_and_retires_controls() -> io::Result<()> {
    initialize_native_io_services()?;
    let meter = Arc::new(ReadBudgetMeter::new(0, 0));
    let payload: Box<dyn std::any::Any + Send> = Box::new(0x5a5a_u64);
    let address = payload.as_ref() as *const dyn std::any::Any as *const () as usize;
    let task = run_blocking_io(
        move || -> () { std::panic::resume_unwind(payload) },
        Some((meter.clone(), true)),
    )?;
    let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        task.wait(None, std::future::pending(), || {})
    }))
    .expect_err("original panic reaches observer");
    assert_eq!(observed.downcast_ref::<u64>(), Some(&0x5a5a));
    assert_eq!(
        observed.as_ref() as *const dyn std::any::Any as *const () as usize,
        address
    );
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}
