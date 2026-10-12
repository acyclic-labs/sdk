use super::*;

#[test]
fn owned_file_operations_follow_first_poll_order() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let write = native.write_all_batch_async(vec![OwnedWrite {
        offset: 0,
        bytes: Bytes::from_static(b"ordered"),
    }]);
    complete(native.sync_async(Durability::Full, None)?)?;
    complete(write)?;
    assert_eq!(std::fs::read(temporary.path())?, b"ordered");
    let discarded = native.write_all_batch_async(vec![OwnedWrite {
        offset: 0,
        bytes: Bytes::from_static(b"discarded"),
    }]);
    let resize = native.set_len_async(16, None)?;
    drop(discarded);
    drop(native);
    complete(resize)?;
    assert_eq!(std::fs::metadata(temporary.path())?.len(), 16);
    Ok(())
}

#[test]
fn platform_control_is_offloaded_and_file_sequenced() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let mut write = native.write_all_batch_async(vec![OwnedWrite {
        offset: 0,
        bytes: Bytes::from_static(b"ready"),
    }]);
    let deadline = Instant::now() + Duration::from_secs(10);
    let completed = loop {
        match Pin::new(&mut write).poll(&mut Context::from_waker(&completion_waker())) {
            Poll::Ready(result) => {
                result?;
                break true;
            }
            Poll::Pending if write.pending.is_none() => break false,
            Poll::Pending if Instant::now() < deadline => {
                std::thread::park_timeout(Duration::from_millis(1))
            }
            Poll::Pending => return Err(io::ErrorKind::TimedOut.into()),
        }
    };
    let (sender, receiver) = mpsc::channel();
    complete(native.control_async(
        move |file, _| {
            let mut bytes = [0; 5];
            read_at(file, 0, &mut bytes)?;
            sender.send(bytes).map_err(io::Error::other)
        },
        None,
    )?)?;
    assert_eq!(receiver.recv().map_err(io::Error::other)?, *b"ready");
    if !completed {
        complete(write)?;
    }
    Ok(())
}

#[test]
fn admitted_write_outlives_dropped_observer() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let mut write = native.write_all_batch_async(vec![OwnedWrite {
        offset: 0,
        bytes: Bytes::from_static(b"persisted"),
    }]);
    let job = write.pending.take().expect("pending write");
    let state = write.state.as_ref().unwrap().clone();
    drop(write);
    drop(native);
    let _ = job.run();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(result) = state.lock().unwrap().result.take() {
            result?;
            break;
        }
        if Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(std::fs::read(temporary.path())?, b"persisted");
    Ok(())
}

#[test]
fn saturated_host_transactions_do_not_starve_native_file_io() -> io::Result<()> {
    struct GateRelease(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for GateRelease {
        fn drop(&mut self) {
            let (lock, ready) = &*self.0;
            *lock.lock().unwrap() = true;
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
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                let _ = finished_tx.send(());
            },
            None,
        )?;
        assert!(matches!(
            Pin::new(&mut task).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        observers.push(task);
    }
    for _ in 0..count {
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(io::Error::other)?;
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
            .map_err(io::Error::other)?;
    }
    drop(observers);
    Ok(())
}

#[test]
fn dropped_admission_waiters_do_not_block_later_reads() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    write_all_at(&file, 0, b"x", None)?;
    let mut reads = Vec::new();
    for _ in 0..64 {
        let mut read = read_batch_async(
            file.try_clone()?,
            vec![OwnedRead {
                offset: 0,
                length: 1,
            }],
        );
        reads.push(
            match Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(result) => {
                    assert_eq!(result?, [Bytes::from_static(b"x")]);
                    None
                }
                Poll::Pending => Some(read),
            },
        );
    }
    reads.truncate(8);
    for read in reads.into_iter().flatten() {
        assert_eq!(complete(read)?, [Bytes::from_static(b"x")]);
    }
    let final_read = read_batch_async(
        file,
        vec![OwnedRead {
            offset: 0,
            length: 1,
        }],
    );
    assert_eq!(complete(final_read)?, [Bytes::from_static(b"x")]);
    Ok(())
}

#[test]
fn prepared_blocking_controls_are_paid_before_submit_and_wait_needs_no_new_allocation()
-> io::Result<()> {
    initialize_native_io_services()?;
    let file = tempfile::tempfile()?;
    file.set_len(17)?;
    let meter = Arc::new(ReadBudgetMeter::new(1, 0));
    let task = prepared_metadata_task(file, meter.clone())?;
    assert!(meter.memory.load(Ordering::Acquire) > 0);
    let stages = meter.used_allocations.load(Ordering::Acquire);
    meter.deny_allocations.store(true, Ordering::Release);
    assert_eq!(task.wait(None, std::future::pending(), || {})??, 17);
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    for accepted in 0..stages {
        let mut limits = ReadBudgetMeter::new(1, 0);
        limits.allocation_limit = accepted;
        let denied = Arc::new(limits);
        assert!(prepared_metadata_task(tempfile::tempfile()?, denied.clone()).is_err());
        assert_eq!(denied.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn prepared_capacity_pressure_and_abandoned_observer_keep_original_job_custody() -> io::Result<()> {
    initialize_native_io_services()?;
    let limits = ReadBudgetMeter::new(1, 0);
    let observed = Arc::new(limits);
    let file = tempfile::tempfile()?;
    file.set_len(23)?;
    let mut task = prepared_metadata_task(file, observed.clone())?;
    let prepared_bytes = observed.memory.load(Ordering::Acquire);
    observed.deny_allocations.store(true, Ordering::Release);
    let (sender, receiver) = mpsc::sync_channel(1);
    let workers = NativeWorkers {
        sender,
        admission: Arc::new(Mutex::new(AdmissionState::new())),
    };
    workers
        .sender
        .try_send(run_blocking_io(|| (), None)?.pending.take().unwrap())
        .map_err(|_| io::Error::other("failed to saturate canonical test queue"))?;
    workers.admission.lock().unwrap().queued_jobs = 1;
    let context = Context::from_waker(Waker::noop());
    assert!(matches!(
        poll_submission_with(&workers, &mut task.pending, &mut task.waiter, &context,),
        Poll::Pending
    ));
    assert_eq!(observed.memory.load(Ordering::Acquire), prepared_bytes);
    let filler = receiver.recv().map_err(io::Error::other)?;
    notify_capacity_released(&workers.admission);
    let _ = filler.run();
    assert!(matches!(
        poll_submission_with(&workers, &mut task.pending, &mut task.waiter, &context,),
        Poll::Ready(Ok(()))
    ));
    drop(task);
    assert!(observed.memory.load(Ordering::Acquire) > 0);
    let accepted = receiver.recv().map_err(io::Error::other)?;
    notify_capacity_released(&workers.admission);
    let _ = accepted.run();
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
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
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
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
    let job = task.pending.take().unwrap();
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
    assert_eq!(observer.memory.load(Ordering::Acquire), prepared);
    assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
    release.send(()).map_err(io::Error::other)?;
    worker
        .join()
        .map_err(|_| io::Error::from(io::ErrorKind::Other))?;
    match Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(error)) => assert_eq!(error.kind(), io::ErrorKind::Interrupted),
        _ => panic!("the actual terminal admission error must now be observable"),
    }
    assert!(observer.memory.load(Ordering::Acquire) < prepared);
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
    let job = || run_blocking_io(|| (), None).map(|mut task| task.pending.take().unwrap());
    workers
        .sender
        .try_send(job()?)
        .map_err(|_| io::Error::other("failed to saturate admission queue"))?;
    workers
        .admission
        .lock()
        .map_err(|_| io::Error::other("admission mutex poisoned"))?
        .queued_jobs += 1;

    let mut inactive = Some(job()?);
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
    let mut active = Some(job()?);
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
