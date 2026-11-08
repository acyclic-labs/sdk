use super::*;

#[test]
fn original_submission_replacement_retires_observer_outside_admission() -> io::Result<()> {
    if passed_in_exclusive_process(
        "tests::admission::original_submission_replacement_retires_observer_outside_admission",
    )? {
        return Ok(());
    }
    struct Retired {
        admission: Weak<crate::sync::Mutex<AdmissionState>>,
        retired: Arc<AtomicBool>,
        panic: bool,
    }
    impl Wake for Retired {
        fn wake(self: Arc<Self>) {
            panic!("replaced capacity observer was notified");
        }
    }
    impl Drop for Retired {
        fn drop(&mut self) {
            self.retired.store(true, Ordering::Release);
            let original = self.admission.upgrade().expect("original pool remains");
            let admission = original
                .try_lock()
                .expect("replacement retained admission lock");
            let node = admission
                .head
                .as_ref()
                .expect("original pending submission");
            assert!(
                node.state.try_lock().is_ok(),
                "replacement retained capacity observer lock"
            );
            drop(admission);
            if self.panic {
                std::panic::panic_any("original capacity replacement panic");
            }
        }
    }
    let _defer = InlineBlocking::defer();
    initialize_native_io_services()?;
    let original = workers(&BLOCKING_WORKERS, true)?;
    for panic in [false, true] {
        let saturation = Saturation::new(&BLOCKING_WORKERS)?;
        let retired = Arc::new(AtomicBool::new(false));
        let budget = Arc::new(ReadBudgetMeter::new(0, 0));
        let mut task = run_blocking_io(|| 37, Some((budget.clone(), true)))?;
        let first = Waker::from(Arc::new(Retired {
            admission: Arc::downgrade(&original.admission),
            retired: retired.clone(),
            panic,
        }));
        assert!(
            Pin::new(&mut task)
                .poll(&mut Context::from_waker(&first))
                .is_pending()
        );
        drop(first);
        let next = completion_waker();
        let replacement = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Pin::new(&mut task).poll(&mut Context::from_waker(&next))
        }));
        assert!(
            original.admission.try_lock().is_ok(),
            "replacement poisoned admission"
        );
        assert!(retired.load(Ordering::Acquire));
        if panic {
            let Err(payload) = replacement else {
                panic!("original capacity replacement panic missing");
            };
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"original capacity replacement panic")
            );
        } else {
            assert!(matches!(replacement, Ok(Poll::Pending)));
        }
        assert!(task.pending.is_some() && task.waiter.is_some());
        drop(saturation);
        assert_eq!(complete(task)?, 37);
        assert_eq!(budget.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn cancelled_original_submission_retires_waiter_outside_admission() -> io::Result<()> {
    if passed_in_exclusive_process(
        "tests::admission::cancelled_original_submission_retires_waiter_outside_admission",
    )? {
        return Ok(());
    }
    struct Retired {
        admission: Weak<crate::sync::Mutex<crate::workers::AdmissionState>>,
        retired: Arc<AtomicBool>,
        woken: Arc<AtomicBool>,
        panic: bool,
    }
    impl std::task::Wake for Retired {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Release);
        }
    }
    impl Drop for Retired {
        fn drop(&mut self) {
            self.retired.store(true, Ordering::Release);
            let admission = self.admission.upgrade().expect("original pool owner");
            assert!(
                admission.try_lock().is_ok(),
                "original waiter retired under the admission lock"
            );
            if self.panic {
                std::panic::panic_any("original waiter retirement panic");
            }
        }
    }
    struct Successor(Arc<AtomicBool>);
    impl Wake for Successor {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }
    let _defer = InlineBlocking::defer();
    initialize_native_io_services()?;
    let original = workers(&BLOCKING_WORKERS, true)?;
    for panic in [false, true] {
        // The configured production workers are physically occupied and their
        // channel is full before this public submission registers its observer.
        let saturation = Saturation::new(&BLOCKING_WORKERS)?;
        let budget = Arc::new(ReadBudgetMeter::new(0, 0));
        let retired = Arc::new(AtomicBool::new(false));
        let woken = Arc::new(AtomicBool::new(false));
        let body = Arc::new(AtomicBool::new(false));
        let entered = Arc::clone(&body);
        let mut task = run_blocking_io(
            move || entered.store(true, Ordering::Release),
            Some((budget.clone(), true)),
        )?;
        let waker = Waker::from(Arc::new(Retired {
            admission: Arc::downgrade(&original.admission),
            retired: Arc::clone(&retired),
            woken: Arc::clone(&woken),
            panic,
        }));
        assert!(
            Pin::new(&mut task)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(task.waiter.is_some() && task.pending.is_some());
        let notified = Arc::new(AtomicBool::new(false));
        let next = Waker::from(Arc::new(Successor(Arc::clone(&notified))));
        let mut successor = run_blocking_io(|| 37, None)?;
        assert!(
            Pin::new(&mut successor)
                .poll(&mut Context::from_waker(&next))
                .is_pending()
        );
        drop(waker);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(task)));
        assert!(
            original.admission.try_lock().is_ok(),
            "original waiter retirement poisoned admission"
        );
        if panic {
            let Err(payload) = result else {
                panic!("original waiter retirement panic missing");
            };
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"original waiter retirement panic")
            );
        } else {
            assert!(result.is_ok());
        }
        assert!(retired.load(Ordering::Acquire));
        assert!(!woken.load(Ordering::Acquire));
        assert!(!body.load(Ordering::Acquire));
        assert!(
            notified.load(Ordering::Acquire),
            "retirement lost its successor wake"
        );
        assert_eq!(budget.memory.load(Ordering::Acquire), 0);
        drop(saturation);
        assert_eq!(complete(successor)?, 37);
    }
    Ok(())
}

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
    let waker = completion_waker();
    let completed = loop {
        match Pin::new(&mut write).poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => {
                result?;
                break true;
            }
            Poll::Pending if write.pending.is_none() => break false,
            // Admission wakes this thread when the queue has room.
            Poll::Pending => std::thread::park(),
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
    // The observer is gone, so stand in for its waker: the completion
    // (inline, or a platform callback) wakes this thread when it publishes.
    state.lock().map_err(|_| io::ErrorKind::Other)?.waker = Some(completion_waker());
    let _ = job.run();
    loop {
        if let Some(result) = state
            .lock()
            .map_err(|_| io::ErrorKind::Other)?
            .result
            .take()
        {
            result?;
            break;
        }
        std::thread::park();
    }
    assert_eq!(std::fs::read(temporary.path())?, b"persisted");
    Ok(())
}

#[test]
fn saturated_host_transactions_do_not_starve_native_file_io() -> io::Result<()> {
    // Occupying the whole process-global host pool must not share it: two
    // such fixtures deadlock on each other, and other tests' host jobs queue
    // behind it.
    if passed_in_exclusive_process(
        "tests::admission::saturated_host_transactions_do_not_starve_native_file_io",
    )? {
        return Ok(());
    }
    // Every host worker holds a gated transaction and the host queue is full.
    let saturation = Saturation::new(&BLOCKING_WORKERS)?;
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    complete(read_batch_async(
        file,
        vec![OwnedRead {
            offset: 0,
            length: 1,
        }],
    ))?;
    drop(saturation);
    Ok(())
}

/// Holds host jobs until the test opens it.
#[derive(Clone)]
struct Gate(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);

impl Gate {
    fn new() -> Self {
        Self(Arc::new((
            std::sync::Mutex::new(false),
            std::sync::Condvar::new(),
        )))
    }

    fn wait(&self) {
        let (open, changed) = &*self.0;
        let mut open = open.lock().unwrap();
        while !*open {
            open = changed.wait(open).unwrap();
        }
    }

    fn open(&self) {
        let (open, changed) = &*self.0;
        *open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
    }
}

fn tally(entered: &mut [usize; 2], fixture: usize) {
    for (slot, count) in entered.iter_mut().enumerate() {
        *count += usize::from(slot == fixture);
    }
}

#[test]
fn cancelled_file_successor_links_before_observer_retirement() -> io::Result<()> {
    struct Release(Gate);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.open();
        }
    }
    struct Retired {
        predecessor: Weak<OperationFence>,
        retired: Arc<AtomicBool>,
        panic: bool,
    }
    impl Wake for Retired {
        fn wake(self: Arc<Self>) {
            panic!("unpublished file successor was woken");
        }
    }
    impl Drop for Retired {
        fn drop(&mut self) {
            self.retired.store(true, Ordering::Release);
            let predecessor = self.predecessor.upgrade().expect("original predecessor");
            assert!(
                predecessor.state.try_lock().is_ok(),
                "cancelled file observer retired under predecessor lock"
            );
            if self.panic {
                std::panic::panic_any("cancelled file observer retirement panic");
            }
        }
    }
    let _defer = InlineBlocking::defer();
    initialize_native_io_services()?;
    for panic in [false, true] {
        let file =
            NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
        let gate = Gate::new();
        let release = Release(gate.clone());
        let (started, entered) = mpsc::sync_channel(1);
        let mut first = file.control_async(
            move |_, _| {
                started.send(()).map_err(io::Error::other)?;
                gate.wait();
                Ok(())
            },
            None,
        )?;
        let wake = completion_waker();
        let mut context = Context::from_waker(&wake);
        assert!(Pin::new(&mut first).poll(&mut context).is_pending());
        entered.recv().map_err(io::Error::other)?;
        let predecessor = first.completion.as_ref().expect("original file fence");
        let retired = Arc::new(AtomicBool::new(false));
        let observer = Waker::from(Arc::new(Retired {
            predecessor: Arc::downgrade(predecessor),
            retired: retired.clone(),
            panic,
        }));
        let mut second = file.set_len_async(99, None)?;
        assert!(
            Pin::new(&mut second)
                .poll(&mut Context::from_waker(&observer))
                .is_pending()
        );
        let mut third = file.set_len_async(37, None)?;
        assert!(Pin::new(&mut third).poll(&mut context).is_pending());
        drop(observer);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(second)));
        let original = predecessor.state.lock().map_err(|_| {
            io::Error::other("cancelled file observer poisoned original predecessor")
        })?;
        assert!(
            original.skipped_successor.as_ref().is_some_and(|fence| {
                third
                    .predecessor
                    .as_ref()
                    .is_some_and(|next| Arc::ptr_eq(fence, next))
            }),
            "cancelled file successor lost its original completion link"
        );
        drop(original);
        if panic {
            let Err(payload) = result else {
                panic!("cancelled file observer panic missing");
            };
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"cancelled file observer retirement panic")
            );
        } else {
            assert!(result.is_ok());
        }
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(file.as_file().metadata()?.len(), 0);
        drop(release);
        complete(first)?;
        complete(third)?;
        assert_eq!(file.as_file().metadata()?.len(), 37);
    }
    Ok(())
}

/// Negative control for the shared-pool hazard that `exclusive_threads`
/// rules out. Forced order: two fixtures that each occupy every worker of the
/// global host pool interleave their submissions, as concurrent tests do.
/// FIFO admission starts the first `threads` jobs, part of each fixture, and
/// every one waits for its fixture's remaining jobs: a circular wait that no
/// worker can break. Each fixture's wait for its last job to start is what
/// timed out (`Timeout`), and a third test's host body queued behind them
/// never entered (`Empty`). Only releasing a fixture, as its timeout did, lets
/// the others proceed.
#[test]
fn whole_pool_fixtures_sharing_a_process_deadlock_and_starve_other_tests() -> io::Result<()> {
    if passed_in_exclusive_process(
        "tests::admission::whole_pool_fixtures_sharing_a_process_deadlock_and_starve_other_tests",
    )? {
        return Ok(());
    }
    let threads = workers(&BLOCKING_WORKERS, false)?.exclusive_threads();
    let gates = [Gate::new(), Gate::new()];
    let (started_tx, started) = mpsc::channel();
    let mut observers = Vec::new();
    for index in 0..threads {
        for (fixture, gate) in gates.iter().enumerate() {
            let gate = gate.clone();
            let started_tx = started_tx.clone();
            let mut task = run_blocking_io(
                move || {
                    started_tx.send((fixture, index)).unwrap();
                    gate.wait();
                },
                None,
            )?;
            submit(&mut task);
            observers.push(task);
        }
    }
    let mut entered = [0; 2];
    for _ in 0..threads {
        tally(&mut entered, started.recv().map_err(io::Error::other)?.0);
    }
    // Every worker now runs a gated job, so no queued job can start.
    assert_eq!(entered, [threads.div_ceil(2), threads / 2]);
    assert!(entered.iter().all(|&count| count < threads));
    assert_eq!(started.try_recv(), Err(mpsc::TryRecvError::Empty));
    let (body_tx, body) = mpsc::sync_channel(1);
    let mut victim = run_blocking_io(move || body_tx.send(()).unwrap(), None)?;
    submit(&mut victim);
    assert_eq!(body.try_recv(), Err(mpsc::TryRecvError::Empty));
    // Releasing one fixture runs its remaining jobs and starts all of the
    // other's, which then holds every worker ahead of the queued body.
    gates[0].open();
    for _ in 0..threads {
        tally(&mut entered, started.recv().map_err(io::Error::other)?.0);
    }
    assert_eq!(entered, [threads; 2]);
    assert_eq!(body.try_recv(), Err(mpsc::TryRecvError::Empty));
    gates[1].open();
    body.recv().map_err(io::Error::other)?;
    drop(observers);
    Ok(())
}

#[test]
#[should_panic(expected = "must run alone")]
fn whole_pool_occupation_outside_an_exclusive_process_is_refused() {
    assert!(!in_exclusive_process());
    let _ = workers(&BLOCKING_WORKERS, false)
        .unwrap()
        .exclusive_threads();
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
    let mut result = Err(io::Error::from(io::ErrorKind::Other));
    let waiting = acyclic_test_support::measure(|| {
        result = task.wait(None, std::future::pending(), || {});
    });
    assert_eq!(result??, 17);
    assert_eq!(waiting.count_total, 0);
    assert_eq!(meter.used_allocations.load(Ordering::Acquire), stages);
    // Publication can precede the worker's final shared-state or notifier
    // release. Observe that original release after measuring the caller's wait.
    meter.wait_for_memory_retirement()?;
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
    let (sender, receiver) = crate::sync::mpsc::sync_channel(1);
    let workers = NativeWorkers::single(sender, AdmissionState::new());
    workers
        .sender
        .try_send(run_blocking_io(|| (), None)?.pending.take().unwrap())
        .map_err(|_| io::Error::other("failed to saturate canonical test queue"))?;
    workers
        .admission
        .lock()
        .map_err(|_| io::ErrorKind::Other)?
        .queued_jobs = 1;
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
#[allow(
    unsafe_code,
    reason = "the fixture observes descriptor retirement without taking ownership"
)]
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
    entered_drop.recv().map_err(io::Error::other)?;
    assert!(
        Pin::new(&mut task)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(observer.memory.load(Ordering::Acquire), prepared);
    // SAFETY: F_GETFD only queries the descriptor table entry.
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

    for capacity in [1, 2] {
        let (sender, receiver) = crate::sync::mpsc::sync_channel(capacity);
        let workers = NativeWorkers::single(sender, AdmissionState::new());
        let job = || run_blocking_io(|| (), None).map(|mut task| task.pending.take().unwrap());
        for _ in 0..capacity {
            workers
                .sender
                .try_send(job()?)
                .map_err(|_| io::Error::other("failed to saturate admission queue"))?;
        }
        workers
            .admission
            .lock()
            .map_err(|_| io::Error::other("admission mutex poisoned"))?
            .queued_jobs += capacity;

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
        assert!(
            wakes.0.load(Ordering::Relaxed) > 0,
            "inactive waiter stranded an active successor"
        );
        assert!(
            matches!(
                poll_submission_with(&workers, &mut active, &mut active_waiter, &context),
                Poll::Ready(Ok(()))
            ),
            "available native queue slot waited for an inactive head"
        );
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
        for _ in 0..capacity {
            let _ = receiver.recv().map_err(io::Error::other)?;
            notify_capacity_released(&workers.admission);
        }
    }
    Ok(())
}

#[test]
fn capacity_release_reaches_a_repolled_original_submission() -> io::Result<()> {
    struct Count(AtomicU64);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    struct Reenter(Mutex<Option<Box<dyn FnOnce() + Send>>>);
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            let action = self.0.lock().expect("original queue owner").take();
            if let Some(action) = action {
                action();
            }
        }
    }
    let (sender, receiver) = crate::sync::mpsc::sync_channel(1);
    let workers = Arc::new(NativeWorkers::single(sender, AdmissionState::new()));
    let receiver = Arc::new(crate::sync::Mutex::new(receiver));
    let mut filler = run_blocking_io(|| (), None)?;
    assert!(
        workers
            .sender
            .try_send(filler.pending.take().ok_or(io::ErrorKind::BrokenPipe)?)
            .is_ok()
    );
    workers
        .admission
        .lock()
        .map_err(|error| io::Error::other(error.to_string()))?
        .queued_jobs = 1;
    let mut prior = run_blocking_io(|| 37, None)?;
    let mut pending = prior.pending.take();
    let mut waiter = None;
    let notified = Arc::new(Count(AtomicU64::new(0)));
    let waker = Waker::from(notified.clone());
    let context = Context::from_waker(&waker);
    assert!(poll_submission_with(&workers, &mut pending, &mut waiter, &context).is_pending());
    let submission = Arc::new(Mutex::new((pending, waiter)));
    let reenter = {
        let workers = workers.clone();
        let receiver = receiver.clone();
        let submission = submission.clone();
        let waker = waker.clone();
        Waker::from(Arc::new(Reenter(Mutex::new(Some(Box::new(move || {
            let mut filler = run_blocking_io(|| (), None).expect("original queue owner");
            assert!(
                workers
                    .sender
                    .try_send(filler.pending.take().expect("original queue owner"))
                    .is_ok()
            );
            workers
                .admission
                .lock()
                .expect("original queue owner")
                .queued_jobs += 1;
            let mut submission = submission.lock().expect("original queue owner");
            let (pending, waiter) = &mut *submission;
            let context = Context::from_waker(&waker);
            assert!(poll_submission_with(&workers, pending, waiter, &context).is_pending());
            drop(submission);
            // The real owner has reinstalled its consumed observer while the
            // channel is full. This original receive releases that capacity
            // during the same broadcast and must revisit that live owner.
            let filler = crate::workers::receive_job(&receiver, &workers.admission)
                .expect("original queue owner");
            wake_capacity_waiter(filler.run());
        }))))))
    };
    let mut later = run_blocking_io(|| (), None)?;
    let mut pending = later.pending.take();
    let mut waiter = None;
    let context = Context::from_waker(&reenter);
    assert!(poll_submission_with(&workers, &mut pending, &mut waiter, &context).is_pending());
    let filler = crate::workers::receive_job(&receiver, &workers.admission)
        .ok_or(io::ErrorKind::BrokenPipe)?;
    wake_capacity_waiter(filler.run());
    assert_eq!(
        notified.0.load(Ordering::Acquire),
        2,
        "repolled capacity observer lost its release"
    );
    remove_capacity_waiter(&workers, waiter.take());
    drop(pending);
    let mut submission = submission
        .lock()
        .map_err(|error| io::Error::other(error.to_string()))?;
    let (pending, waiter) = &mut *submission;
    let context = Context::from_waker(&waker);
    assert!(matches!(
        poll_submission_with(&workers, pending, waiter, &context),
        Poll::Ready(Ok(()))
    ));
    let accepted = crate::workers::receive_job(&receiver, &workers.admission)
        .ok_or(io::ErrorKind::BrokenPipe)?;
    wake_capacity_waiter(accepted.run());
    assert_eq!(complete(prior)?, 37);
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

#[test]
fn metered_task_receipts_do_not_depend_on_queue_capacity() -> io::Result<()> {
    initialize_native_io_services()?;
    let file = tempfile::tempfile()?;
    let native = NativeFile::from_file(file, None).map_err(|(error, _)| error)?;
    type Submit = fn(&NativeFile, Arc<dyn NativeReadMeter>) -> io::Result<NativeCompletion<()>>;
    let submissions: [Submit; 3] = [
        |_, meter| run_blocking_io(|| (), Some((meter, false))),
        |_, meter| run_blocking_io(|| (), Some((meter, true))),
        |native, meter| native.control_async(|_, _| Ok(()), Some(meter)),
    ];
    for submit in submissions {
        let meter = Arc::new(ReadBudgetMeter::new(1, 0));
        let mut task = submit(&native, meter.clone())?;
        let allocations = meter.used_allocations.load(Ordering::Acquire);
        let memory = meter.memory.load(Ordering::Acquire);
        // Waiting for capacity is host load, not request work: it may not
        // allocate, so a full queue cannot change or refuse the receipt.
        meter.deny_allocations.store(true, Ordering::Release);
        let (sender, receiver) = crate::sync::mpsc::sync_channel(1);
        let workers = NativeWorkers::single(sender, AdmissionState::new());
        workers
            .sender
            .try_send(run_blocking_io(|| (), None)?.pending.take().unwrap())
            .map_err(|_| io::Error::other("failed to saturate the test queue"))?;
        workers
            .admission
            .lock()
            .map_err(|_| io::ErrorKind::Other)?
            .queued_jobs = 1;
        let context = Context::from_waker(Waker::noop());
        assert!(matches!(
            poll_submission_with(&workers, &mut task.pending, &mut task.waiter, &context),
            Poll::Pending
        ));
        assert_eq!(meter.used_allocations.load(Ordering::Acquire), allocations);
        assert_eq!(meter.memory.load(Ordering::Acquire), memory);
        let filler = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        let _ = filler.run();
        assert!(matches!(
            poll_submission_with(&workers, &mut task.pending, &mut task.waiter, &context),
            Poll::Ready(Ok(()))
        ));
        let admitted = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        let _ = admitted.run();
        complete(task)?;
        assert_eq!(meter.used_allocations.load(Ordering::Acquire), allocations);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}
