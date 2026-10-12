use super::*;

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn accepted_callback_cannot_disappear_without_terminal_completion() -> io::Result<()> {
    const MARKER: &str = "ACYCLIC_DROPPED_COMPLETION_CHILD";
    if std::env::var_os(MARKER).is_some() {
        let state = Arc::new(Mutex::new(CompletionState::<()>::default()));
        eprintln!("dropping accepted native callback");
        let _ = submit_file_io(
            FileCompletion::new(state, None, None, None, obs::OperationSpan::new(tracing::Span::none())),
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
            .arg("runtime::tests::file::accepted_callback_cannot_disappear_without_terminal_completion")
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
            OpenOptions::new().create_new(true).read(true).write(true).open(directory.path().join("panic-worker"))?,
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
        return Err(io::Error::other("native worker unwind returned to observer"));
    }
    let output = crate::process_output(
        std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("runtime::tests::file::native_file_worker_unwind_is_fail_stop")
            .arg("--nocapture")
            .env(CHILD_MARKER, "1"),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    assert!(!output.status.success(), "native worker unwind escaped fail-stop");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("injected native worker unwind"),
        "child did not reach the injected worker panic"
    );
    Ok(())
}

#[test]
fn no_replace_preserves_both_entries() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    let destination = directory.path().join("destination");
    std::fs::write(&source, b"source")?;
    std::fs::write(&destination, b"destination")?;
    let Err(error) = durable_rename(&source, &destination, RenameMode::NoReplace, None) else {
        return Err(io::Error::other("existing destination accepted no-replace rename"));
    };
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(publish_new(&source, &destination, None).unwrap_err().kind(), io::ErrorKind::AlreadyExists);
    let mut limits = ReadBudgetMeter::new(0, 0);
    limits.memory_limit = 0;
    let observer = Arc::new(limits);
    let meter: Arc<dyn NativeReadMeter> = observer.clone();
    assert_eq!(publish_new(&source, &destination, Some(&meter)).unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(
        durable_rename(&source, &destination, RenameMode::Replace, Some(&meter),).unwrap_err().kind(),
        io::ErrorKind::Other
    );
    assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    assert_eq!(std::fs::read(source)?, b"source");
    assert_eq!(std::fs::read(destination)?, b"destination");
    Ok(())
}

#[cfg(unix)]
#[test]
fn callback_cancellation_retains_captured_prefix_and_original_owner() -> io::Result<()> {
    let mut command = std::process::Command::new("/bin/sh");
    command
        .args(["-c", "printf out; printf err >&2; sleep 60"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let mut meter = ReadBudgetMeter::new(1024, 32);
    meter.memory_limit = 0;
    meter.allocation_limit = 0;
    let failure = tree
        .wait_with_output(
            std::time::Duration::from_secs(5),
            16,
            Some(CaptureInput::Exchange { initial: b"", maximum: 1 }),
            || Ok(()),
            |_| {
                meter.cancelled.store(true, std::sync::atomic::Ordering::Release);
                Err(io::ErrorKind::Interrupted.into())
            },
            Some(&meter),
            Some([Vec::with_capacity(16), Vec::with_capacity(16), Vec::with_capacity(1)]),
        )
        .expect_err("caller cancels after physical stdout delivery");
    assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(failure.stdout, b"out");
    assert!(b"err".starts_with(&failure.stderr));
    assert_eq!(meter.read.load(Ordering::Acquire), (failure.stdout.len() + failure.stderr.len()) as u64);
    assert!(!failure.cleanup_completed);
    assert!(!tree.is_reaped());
    tree.terminate(None)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn callback_panic_preserves_original_meter_work_and_pipe_owner() -> io::Result<()> {
    let mut command = std::process::Command::new("/bin/sh");
    command
        .args(["-c", "printf out; sleep 60"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let mut meter = ReadBudgetMeter::new(1024, 32);
    meter.memory_limit = 0;
    meter.allocation_limit = 0;
    let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tree.wait_with_output(
            std::time::Duration::from_secs(5),
            16,
            Some(CaptureInput::Exchange { initial: b"", maximum: 1 }),
            || Ok(()),
            |_| panic!("real protocol observer panic"),
            Some(&meter),
            Some([Vec::with_capacity(16), Vec::with_capacity(16), Vec::with_capacity(1)]),
        )
    }));
    assert!(stopped.is_err());
    assert_eq!(meter.read.load(Ordering::Acquire), 3);
    assert!(!tree.is_reaped());
    tree.terminate(None)?;
    Ok(())
}

#[test]
fn allocation_split_absorb_and_mismatch_move_only_original_custody() -> io::Result<()> {
    let observer = Arc::new(ReadBudgetMeter::new(0, 0));
    let meter: Arc<dyn NativeReadMeter> = observer.clone();
    let mut payload = Vec::<u8>::new();
    payload.try_reserve_exact(23).map_err(io::Error::other)?;
    let actual = payload.capacity();
    let mut allocation = ReadAllocation::adopt(&meter, actual)?;
    let mut split = allocation.split(7)?;
    assert_eq!(split.bytes(), 7);
    assert_eq!(observer.memory.load(Ordering::Acquire), actual as u64);
    let other_meter: Arc<dyn NativeReadMeter> = Arc::new(ReadBudgetMeter::new(0, 0));
    let mut other = ReadAllocation::empty(&other_meter);
    assert_eq!(other.absorb(&mut split).unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(split.bytes(), 7);
    assert_eq!(other.bytes(), 0);
    allocation.absorb(&mut split)?;
    assert_eq!(split.bytes(), 0);
    assert_eq!(allocation.bytes(), actual as u64);
    assert_eq!(allocation.split(actual + 1).unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(allocation.bytes(), actual as u64);
    drop(payload);
    allocation.release(actual)?;
    assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn denied_file_import_returns_original_handle_without_fabricated_io() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("retained-import-denial");
    std::fs::write(&path, b"original")?;
    for owned in [false, true] {
        let mut policy = ReadBudgetMeter::new(16, 8);
        policy.memory_limit = 0;
        let observer = Arc::new(policy);
        let file = File::open(&path)?;
        let result = if owned {
            let meter: Arc<dyn NativeReadMeter> = observer.clone();
            NativeFile::from_file(file, Some(NativeFileMeter::Owned(meter)))
        } else {
            NativeFile::from_file(file, Some(NativeFileMeter::Borrowed(observer.as_ref())))
        };
        let (error, mut file) = match result {
            Err(failure) => failure,
            Ok(_) => panic!("actual zero-memory import admission must refuse"),
        };
        assert_eq!(error.kind(), io::ErrorKind::Other);
        let mut original = [0_u8; 8];
        std::io::Read::read_exact(&mut file, &mut original)?;
        assert_eq!(&original, b"original");
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        drop(file);
    }
    Ok(())
}

#[test]
fn measured_sync_wait_expired_deadline_stops_before_body() -> io::Result<()> {
    initialize_native_io_services()?;
    let meter = Arc::new(ReadBudgetMeter::new(0, 0));
    let called = Arc::new(AtomicBool::new(false));
    let body_called = called.clone();
    let task = run_blocking_io(move || body_called.store(true, Ordering::Release), Some((meter.clone(), false)))?;
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
fn owned_file_operation_keeps_its_handle_after_owner_drop() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("owned-file-lifetime");
    let file = OpenOptions::new().create_new(true).read(true).write(true).open(&path)?;
    let native = NativeFile::from_file(file, None).map_err(|(error, _)| error)?;
    let resize = native.set_len_async(64, None).expect("unmetered unit construction cannot fail");
    drop(native);
    complete(resize)?;
    assert_eq!(std::fs::metadata(&path)?.len(), 64);
    Ok(())
}

#[cfg(windows)]
#[test]
fn healthy_registry_does_not_retain_exclusive_handle() -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;

    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("exclusive-native-file");
    let file = OpenOptions::new().create_new(true).read(true).write(true).share_mode(0).open(&path)?;
    let native = NativeFile::from_file(file, None).map_err(|(error, _)| error)?;
    drop(native);
    File::open(&path)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn uncertain_completion_retains_identity_after_owner_drop() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("uncertain-identity");
    let native = NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&path)?, None)
        .map_err(|(error, _)| error)?;
    let completion = FileCompletion::new(
        Arc::new(Mutex::new(CompletionState::<()>::default())),
        None,
        Some(Arc::clone(&native.health)),
        Some(Arc::clone(&native.file)),
        obs::OperationSpan::new(tracing::Span::none()),
    );
    completion.finish(Err(linux::uncertain_completion(io::ErrorKind::TimedOut.into())));
    drop(native);
    let alias = NativeFile::from_file(OpenOptions::new().read(true).open(&path)?, None).map_err(|(error, _)| error)?;
    assert!(alias.health.uncertain.load(Ordering::Acquire));
    assert!(complete(alias.read_batch_async(vec![OwnedRead { offset: 0, length: 1 }])).is_err());
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn independently_opened_aliases_share_fencing_and_uncertainty() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("identity");
    let alias = temporary.path().join("hard-link");
    let first = NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&path)?, None)
        .map_err(|(error, _)| error)?;
    std::fs::hard_link(&path, &alias)?;
    let second = NativeFile::from_file(OpenOptions::new().read(true).write(true).open(&alias)?, None)
        .map_err(|(error, _)| error)?;
    assert!(Arc::ptr_eq(&first.health.tail, &second.health.tail));
    assert!(Arc::ptr_eq(&first.health.uncertain, &second.health.uncertain));
    poison_file_health(&first.file, &first.health.uncertain);
    drop(first);
    let result = complete(second.set_len_async(1, None).expect("unmetered unit construction cannot fail"));
    assert!(result.is_err(), "an alias bypassed an uncertain completion");
    drop(second);
    let bare_result = complete(read_batch_async(
        OpenOptions::new().read(true).open(&path)?,
        vec![OwnedRead { offset: 0, length: 1 }],
    ));
    assert!(bare_result.is_err(), "the free read API bypassed the file fence");
    let direct_alias = OpenOptions::new().read(true).write(true).open(&alias)?;
    let mut byte = [0];
    assert!(read_at(&direct_alias, 0, &mut byte).is_err());
    assert!(write_all_at(&direct_alias, 0, b"x", None).is_err());
    assert!(sync_file(&direct_alias, Durability::Full, None).is_err());
    assert!(sync_data(&direct_alias, Durability::Full).is_err());
    assert_eq!(std::fs::metadata(&path)?.len(), 0);

    let independent = NativeFile::from_file(
        OpenOptions::new().create_new(true).read(true).write(true).open(temporary.path().join("clean"))?,
        None,
    )
    .map_err(|(error, _)| error)?;
    complete(independent.set_len_async(1, None).expect("unmetered unit construction cannot fail"))?;
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn admitted_job_keeps_alias_registry_alive_after_owner_drop() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("outstanding");
    let alias_path = temporary.path().join("outstanding-link");
    let native = NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&path)?, None)
        .map_err(|(error, _)| error)?;

    std::fs::hard_link(&path, &alias_path)?;
    let tail = Arc::downgrade(&native.health.tail);
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel::<()>(1);
    let mut pending = native
        .control_async(
            move |_, _| {
                started_sender.send(()).map_err(io::Error::other)?;
                release_receiver.recv().map_err(io::Error::other)?;
                Ok(())
            },
            None,
        )
        .expect("unmetered control construction cannot fail");
    let waker = completion_waker();
    assert!(matches!(Pin::new(&mut pending).poll(&mut Context::from_waker(&waker)), Poll::Pending));
    started_receiver.recv_timeout(Duration::from_secs(5)).map_err(io::Error::other)?;
    drop(native);
    let alias = NativeFile::from_file(OpenOptions::new().read(true).write(true).open(&alias_path)?, None)
        .map_err(|(error, _)| error)?;
    assert!(Arc::ptr_eq(
        &tail.upgrade().ok_or_else(|| io::Error::other("admitted job lost its alias tail"))?,
        &alias.health.tail
    ));
    release_sender.send(()).map_err(io::Error::other)?;
    complete(pending)?;
    Ok(())
}

#[test]
fn owned_file_operations_follow_first_poll_order() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("owned-file-order");
    let native = NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&path)?, None)
        .map_err(|(error, _)| error)?;
    let write = native.write_all_batch_async(vec![OwnedWrite { offset: 0, bytes: Bytes::from_static(b"ordered") }]);
    let mut sync = native.sync_async(Durability::Full, None).expect("unmetered unit construction cannot fail");
    let waker = completion_waker();
    match Pin::new(&mut sync).poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(result) => result?,
        Poll::Pending => complete(sync)?,
    }
    complete(write)?;
    assert_eq!(std::fs::read(&path)?, b"ordered");

    let discarded =
        native.write_all_batch_async(vec![OwnedWrite { offset: 0, bytes: Bytes::from_static(b"discarded") }]);
    let resize = native.set_len_async(16, None).expect("unmetered unit construction cannot fail");
    drop(discarded);
    complete(resize)?;
    assert_eq!(std::fs::metadata(&path)?.len(), 16);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn uncertain_completion_poison_is_file_local_and_fences_successors() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let poisoned_path = temporary.path().join("poisoned");
    let poisoned =
        NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&poisoned_path)?, None)
            .map_err(|(error, _)| error)?;
    let prior = Arc::new(OperationFence::default());
    *poisoned.health.tail.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&prior));
    FileCompletion::new(
        Arc::new(Mutex::new(CompletionState::<()>::default())),
        Some(prior),
        Some(Arc::clone(&poisoned.health)),
        Some(Arc::clone(&poisoned.file)),
        obs::OperationSpan::new(tracing::Span::none()),
    )
    .finish(Err(linux::uncertain_completion(io::Error::other("injected ambiguous completion"))));
    let Err(_) = complete(poisoned.set_len_async(4, None).expect("unmetered unit construction cannot fail")) else {
        return Err(io::Error::other("an uncertain prior operation did not fence its successor"));
    };
    assert_eq!(std::fs::metadata(poisoned_path)?.len(), 0);

    let unrelated_path = temporary.path().join("unrelated");
    let unrelated =
        NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&unrelated_path)?, None)
            .map_err(|(error, _)| error)?;
    complete(unrelated.set_len_async(4, None).expect("unmetered unit construction cannot fail"))?;
    assert_eq!(std::fs::metadata(unrelated_path)?.len(), 4);
    Ok(())
}

#[test]
fn platform_control_is_offloaded_and_file_sequenced() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let mut write = native.write_all_batch_async(vec![OwnedWrite { offset: 0, bytes: Bytes::from_static(b"ready") }]);
    let waker = completion_waker();
    let deadline = Instant::now() + Duration::from_secs(10);
    let write_completed = loop {
        match Pin::new(&mut write).poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => {
                result?;
                break true;
            }
            Poll::Pending if write.pending.is_none() => break false,
            Poll::Pending if Instant::now() < deadline => {
                std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
            }
            Poll::Pending => {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "write admission stalled"));
            }
        }
    };
    let (sender, receiver) = mpsc::channel();
    let control = native
        .control_async(
            move |file, _| {
                let mut bytes = [0; 5];
                read_at(file, 0, &mut bytes)?;
                sender.send(bytes).map_err(|error| io::Error::other(error.to_string()))
            },
            None,
        )
        .expect("unmetered control construction cannot fail");
    complete(control)?;
    assert_eq!(receiver.recv().map_err(io::Error::other)?, *b"ready");
    if !write_completed {
        complete(write)?;
    }
    Ok(())
}

#[test]
fn unpolled_operation_does_not_delay_a_later_awaited_operation() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("skipped-operation-order");
    let native = NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&path)?, None)
        .map_err(|(error, _)| error)?;
    let never_polled = native.set_len_async(8, None).expect("unmetered unit construction cannot fail");
    let last = native.set_len_async(32, None).expect("unmetered unit construction cannot fail");
    complete(last)?;
    assert_eq!(std::fs::metadata(&path)?.len(), 32);
    drop(never_polled);
    assert_eq!(std::fs::metadata(&path)?.len(), 32);
    Ok(())
}

#[test]
fn prepared_blocking_controls_are_paid_before_submit_and_wait_needs_no_new_allocation() -> io::Result<()> {
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
    let workers = NativeWorkers { sender, admission: Arc::new(Mutex::new(AdmissionState::new())) };
    workers
        .sender
        .try_send(NativeJob::Task {
            operation: Box::new(|| ()),
            meter: None,
            terminal: Arc::new(Mutex::new(CompletionState::<()> { task_pending: true, ..CompletionState::default() })),
            _allocation: None,
            health: None,
            completion: None,
        })
        .map_err(|_| io::Error::other("failed to saturate canonical test queue"))?;
    workers.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).queued_jobs = 1;
    let context = Context::from_waker(Waker::noop());
    assert!(matches!(poll_submission_with(&workers, &mut task.pending, &mut task.waiter, &context,), Poll::Pending));
    assert_eq!(observed.memory.load(Ordering::Acquire), prepared_bytes);
    let filler = receiver.recv().map_err(io::Error::other)?;
    notify_capacity_released(&workers.admission);
    let _ = filler.run();
    assert!(matches!(
        poll_submission_with(&workers, &mut task.pending, &mut task.waiter, &context,),
        Poll::Ready(Ok(()))
    ));
    drop(task);
    assert!(
        observed.memory.load(Ordering::Acquire) > 0,
        "accepted job owns original completion and exact closure beyond observer drop"
    );
    let accepted = receiver.recv().map_err(io::Error::other)?;
    notify_capacity_released(&workers.admission);
    let _ = accepted.run();
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn observed_host_task_panic_preserves_payload_and_retires_controls() -> io::Result<()> {
    initialize_native_io_services()?;
    let meter = Arc::new(ReadBudgetMeter::new(0, 0));
    let payload: Box<dyn std::any::Any + Send> = Box::new(0x5a5a_u64);
    let address = payload.as_ref() as *const dyn std::any::Any as *const () as usize;
    let task = run_blocking_io(move || -> () { std::panic::resume_unwind(payload) }, Some((meter.clone(), true)))?;
    let observed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task.wait(None, std::future::pending(), || {})))
            .expect_err("original panic reaches observer");
    assert_eq!(observed.downcast_ref::<u64>(), Some(&0x5a5a));
    assert_eq!(observed.as_ref() as *const dyn std::any::Any as *const () as usize, address);
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn allocation_reconciliation_retains_capacity_on_refusal_and_cancellation() -> io::Result<()> {
    for cancelled in [false, true] {
        let mut limits = ReadBudgetMeter::new(0, 0);
        if !cancelled {
            limits.memory_limit = 3;
        }
        let observer = Arc::new(limits);
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let mut allocation = ReadAllocation::new(&meter, 3)?;
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(if cancelled { 17 } else { 64 }).map_err(io::Error::other)?;
        observer.cancelled.store(cancelled, Ordering::Release);
        let actual = payload.capacity() as u64;
        assert_eq!(
            allocation.reconcile_capacity(payload.capacity()).unwrap_err().kind(),
            if cancelled { io::ErrorKind::Interrupted } else { io::ErrorKind::Other }
        );
        assert_eq!(allocation.bytes(), actual);
        assert_eq!(observer.memory.load(Ordering::Acquire), actual);
        drop(payload);
        drop(allocation);
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}
