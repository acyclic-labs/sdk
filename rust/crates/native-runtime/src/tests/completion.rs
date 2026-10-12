use super::*;

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn accepted_callback_cannot_disappear_without_terminal_completion() -> io::Result<()> {
    const MARKER: &str = "ACYCLIC_DROPPED_COMPLETION_CHILD";
    if std::env::var_os(MARKER).is_some() {
        eprintln!("dropping accepted native callback");
        let _ = submit_file_io(
            FileCompletion::new(
                Arc::new(Mutex::new(CompletionState::<()>::default())),
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
        return Err(io::Error::other("missing terminal callback"));
    }
    let output = crate::process_output(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "tests::completion::accepted_callback_cannot_disappear_without_terminal_completion",
                "--nocapture",
            ])
            .env(MARKER, "1"),
        Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("dropping accepted native callback"));
    Ok(())
}

#[test]
fn native_file_worker_unwind_is_fail_stop() -> io::Result<()> {
    const MARKER: &str = "ACYCLIC_NATIVE_UNWIND_CHILD_TEST";
    if std::env::var_os(MARKER).is_some() {
        let file =
            NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
        let _ = complete(file.control_async(
            |_, _| {
                eprintln!("injected native worker unwind");
                std::panic::resume_unwind(Box::new("injected native worker unwind"));
            },
            None,
        )?);
        return Err(io::Error::other("worker unwind returned"));
    }
    let output = crate::process_output(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "tests::completion::native_file_worker_unwind_is_fail_stop",
                "--nocapture",
            ])
            .env(MARKER, "1"),
        Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("injected native worker unwind"));
    Ok(())
}

#[test]
fn host_task_panic_preserves_payload_and_retires_controls() -> io::Result<()> {
    struct RetainedPanic(Arc<AtomicBool>);
    impl Drop for RetainedPanic {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    initialize_native_io_services()?;
    for observed in [false, true] {
        let temporary = tempfile::NamedTempFile::new()?;
        std::fs::write(temporary.path(), b"abc")?;
        let mut file = temporary.reopen()?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let body_meter = meter.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let payload = Box::new(RetainedPanic(dropped.clone()));
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
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                Pin::new(&mut task)
                    .poll(&mut Context::from_waker(&completion_waker()))
                    .is_pending()
            );
            match entered.try_recv() {
                Ok(()) => break,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    std::thread::park_timeout(Duration::from_millis(1))
                }
                Err(error) => return Err(io::Error::other(error)),
            }
        }
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
        let deadline = Instant::now() + Duration::from_secs(10);
        while (!dropped.load(Ordering::Acquire) || meter.memory.load(Ordering::Acquire) != 0)
            && Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
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
        let deadline = Instant::now() + Duration::from_secs(10);
        while task.pending.is_some() {
            assert!(
                Pin::new(&mut task)
                    .poll(&mut Context::from_waker(&completion_waker()))
                    .is_pending()
            );
            if Instant::now() >= deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let release = || {
            entered.recv_timeout(Duration::from_secs(10)).unwrap();
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
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}
