use super::*;

#[test]
fn saturated_host_transactions_do_not_starve_native_file_io() -> io::Result<()> {
    struct GateRelease(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for GateRelease {
        fn drop(&mut self) {
            let (lock, ready) = &*self.0;
            *lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            ready.notify_all();
        }
    }

    let parallelism = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let count = parallelism.saturating_mul(2).clamp(2, 16);
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = GateRelease(Arc::clone(&gate));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let mut observers = Vec::new();
    let waker = completion_waker();
    for _ in 0..count {
        let gate = Arc::clone(&gate);
        let entered_tx = entered_tx.clone();
        let finished_tx = finished_tx.clone();
        let mut task = run_blocking_io(
            move || {
                let _ = entered_tx.send(());
                let (lock, ready) = &*gate;
                let mut released = lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                while !*released {
                    released = ready
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                let _ = finished_tx.send(());
            },
            None,
        )
        .expect("unmetered task construction cannot fail");
        assert!(matches!(
            Pin::new(&mut task).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        observers.push(task);
    }
    for _ in 0..count {
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|error| io::Error::other(error.to_string()))?;
    }
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    complete(read_batch_async(
        file,
        vec![OwnedRead {
            offset: 0,
            length: 1,
        }],
    ))?;
    drop(release);
    for _ in 0..count {
        finished_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|error| io::Error::other(error.to_string()))?;
    }
    drop(observers);
    Ok(())
}

#[cfg(unix)]
#[test]
#[allow(unsafe_code)]
fn staged_admission_failure_waits_for_actual_payload_and_job_retirement() -> io::Result<()> {
    use std::os::fd::AsRawFd;
    struct HeldFile {
        file: Option<File>,
        entered: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Drop for HeldFile {
        fn drop(&mut self) {
            self.entered
                .send(())
                .expect("actual payload-drop observer remains owned");
            self.release
                .recv()
                .expect("actual destructor release remains owned");
            drop(self.file.take());
        }
    }
    initialize_native_io_services()?;
    let file = tempfile::tempfile()?;
    let fd = file.as_raw_fd();
    let (entered, entered_drop) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let held = HeldFile {
        file: Some(file),
        entered,
        release: released,
    };
    let policy = ReadBudgetMeter::new(0, 0);
    let observer = Arc::new(policy);
    let mut task = run_blocking_io(
        move || {
            drop(held);
        },
        Some((observer.clone(), true)),
    )?;
    let prepared = observer.memory.load(Ordering::Acquire);
    let job = task
        .pending
        .take()
        .expect("same canonical prepared ingress");
    observer.cancelled.store(true, Ordering::Release);
    let worker = std::thread::spawn(move || wake_capacity_waiter(job.run()));
    entered_drop
        .recv_timeout(Duration::from_secs(10))
        .map_err(io::Error::other)?;
    assert!(
        Pin::new(&mut task)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(
        observer.memory.load(Ordering::Acquire),
        prepared,
        "the real closure allocation remains owned while its payload destructor is pending"
    );
    assert_ne!(
        unsafe { libc::fcntl(fd, libc::F_GETFD) },
        -1,
        "the original held file remains physically open before destructor release"
    );
    release.send(()).map_err(io::Error::other)?;
    worker
        .join()
        .map_err(|_| io::Error::from(io::ErrorKind::Other))?;
    match Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(error)) => assert_eq!(error.kind(), io::ErrorKind::Interrupted),
        _ => panic!("the actual terminal admission error must now be observable"),
    }
    assert!(
        observer.memory.load(Ordering::Acquire) < prepared,
        "job backing retires before terminal publication"
    );
    drop(task);
    assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn inactive_admission_waiter_cannot_strand_other_files() -> io::Result<()> {
    use std::sync::atomic::AtomicUsize;

    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (sender, receiver) = mpsc::sync_channel(1);
    let workers = NativeWorkers {
        sender,
        admission: Arc::new(Mutex::new(AdmissionState::new())),
    };
    let job = || NativeJob::Task {
        operation: Box::new(|| ()),
        meter: None,
        terminal: Arc::new(Mutex::new(CompletionState::<()> {
            task_pending: true,
            ..CompletionState::default()
        })),
        _allocation: None,
        health: None,
        completion: None,
    };
    workers
        .sender
        .try_send(job())
        .map_err(|_| io::Error::other("failed to saturate admission queue"))?;
    workers
        .admission
        .lock()
        .map_err(|_| io::Error::other("admission mutex poisoned"))?
        .queued_jobs += 1;

    let mut inactive = Some(job());
    let mut inactive_waiter = None;
    assert!(matches!(
        poll_submission_with(
            &workers,
            &mut inactive,
            &mut inactive_waiter,
            &Context::from_waker(Waker::noop()),
        ),
        Poll::Pending
    ));
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let active_waker = Waker::from(Arc::clone(&wakes));
    let context = Context::from_waker(&active_waker);
    let mut active = Some(job());
    let mut active_waiter = None;
    assert!(matches!(
        poll_submission_with(&workers, &mut active, &mut active_waiter, &context),
        Poll::Pending
    ));

    let _ = receiver.recv().map_err(io::Error::other)?;
    notify_capacity_released(&workers.admission);
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert!(matches!(
        poll_submission_with(&workers, &mut active, &mut active_waiter, &context),
        Poll::Ready(Ok(()))
    ));
    let _ = receiver.recv().map_err(io::Error::other)?;
    notify_capacity_released(&workers.admission);
    assert!(matches!(
        poll_submission_with(
            &workers,
            &mut inactive,
            &mut inactive_waiter,
            &Context::from_waker(Waker::noop()),
        ),
        Poll::Ready(Ok(()))
    ));
    let _ = receiver.recv().map_err(io::Error::other)?;
    notify_capacity_released(&workers.admission);
    Ok(())
}

#[test]
fn skipped_fence_chain_completes_iteratively() -> io::Result<()> {
    std::thread::Builder::new()
        .stack_size(64 * 1024)
        .spawn(|| {
            let first = Arc::new(OperationFence::default());
            let mut prior = first.clone();
            for _ in 0..100_000 {
                let next = Arc::new(OperationFence::default());
                next.complete_after(Some(prior));
                prior = next;
            }
            first.complete();
            assert!(prior.ready_or_register(Waker::noop()));
        })?
        .join()
        .map_err(|_| io::Error::from(io::ErrorKind::Other))?;
    Ok(())
}
