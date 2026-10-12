use super::*;

#[test]
fn ownership_release_barrier_waits_for_the_final_clone() {
    let anchor = OwnershipAnchor::new(());
    let clone = anchor.clone();
    let barrier = anchor.release_barrier();
    drop(anchor);
    assert!(!barrier.is_released());
    drop(clone);
    barrier.wait();
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
            Some(CaptureInput::Exchange {
                initial: b"",
                maximum: 1,
            }),
            || Ok(()),
            |_| {
                meter
                    .cancelled
                    .store(true, std::sync::atomic::Ordering::Release);
                Err(io::ErrorKind::Interrupted.into())
            },
            Some(&meter),
            Some([
                Vec::with_capacity(16),
                Vec::with_capacity(16),
                Vec::with_capacity(1),
            ]),
        )
        .unwrap_err();
    assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(failure.stdout, b"out");
    assert!(b"err".starts_with(&failure.stderr));
    assert_eq!(
        meter.read.load(Ordering::Acquire),
        (failure.stdout.len() + failure.stderr.len()) as u64
    );
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
            Some(CaptureInput::Exchange {
                initial: b"",
                maximum: 1,
            }),
            || Ok(()),
            |_| panic!("real protocol observer panic"),
            Some(&meter),
            Some([
                Vec::with_capacity(16),
                Vec::with_capacity(16),
                Vec::with_capacity(1),
            ]),
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
    assert_eq!(
        other.absorb(&mut split).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(split.bytes(), 7);
    assert_eq!(other.bytes(), 0);
    allocation.absorb(&mut split)?;
    assert_eq!(split.bytes(), 0);
    assert_eq!(allocation.bytes(), actual as u64);
    assert_eq!(
        allocation.split(actual + 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
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

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn prepared_read_returns_same_owner_short_prefix_and_eof_without_header() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("prepared-prefix");
    std::fs::write(&path, b"abc")?;
    let file = bounded_write_file(&path, false)?;
    for (offset, expected) in [(0, 3), (3, 0)] {
        let meter = Arc::new(ReadBudgetMeter::new(8, 8));
        let storage = vec![0x55; 8];
        let identity = storage.as_ptr();
        let read = file.read_prepared_bounded_async(offset, storage, 8, false, meter.clone())?;
        let (storage, filled) = {
            let _blocking = InlineBlocking::enter();
            complete(read)
        }?;
        assert_eq!(filled, expected);
        assert_eq!(storage.as_ptr(), identity);
        assert_eq!(&storage[..filled], &b"abc"[..expected]);
        assert!(storage[filled..].iter().all(|byte| *byte == 0x55));
        assert_eq!(meter.read.load(Ordering::Acquire), expected as u64);
        assert!(file.health.tail.lock().unwrap().is_none());
        let weak = Arc::downgrade(&meter);
        drop(meter);
        let deadline = Instant::now() + Duration::from_secs(10);
        while weak.upgrade().is_some() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(weak.upgrade().is_none());
        drop(storage);
    }
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn prepared_read_exact_eof_and_late_cancel_preserve_physical_work() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("prepared-refusal");
    std::fs::write(&path, b"abc")?;
    let file = bounded_write_file(&path, false)?;
    for (prepared, late_cancel) in [(true, false), (true, true), (false, true)] {
        let mut limits = ReadBudgetMeter::new(8, 8);
        limits.cancel_after_read = late_cancel;
        let meter = Arc::new(limits);
        let error = {
            let _blocking = InlineBlocking::enter();
            if prepared {
                complete(file.read_prepared_bounded_async(0, [0x55; 8], 8, true, meter.clone())?)
                    .unwrap_err()
            } else {
                complete(file.read_bounded_async(0, 8, meter.clone(), false)?)
                    .err()
                    .expect("cancelled read")
            }
        };
        assert_eq!(
            error.kind(),
            if late_cancel {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::UnexpectedEof
            }
        );
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        let deadline = Instant::now() + Duration::from_secs(10);
        while meter.memory.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn prepared_read_panic_preserves_original_payload_and_actual_work() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("prepared-panic");
    std::fs::write(&path, b"abc")?;
    let file = bounded_write_file(&path, false)?;
    let mut limits = ReadBudgetMeter::new(8, 3);
    limits.panic_after_read = true;
    let meter = Arc::new(limits);
    let mut read = file.read_prepared_bounded_async(0, [0; 3], 3, true, meter.clone())?;
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _blocking = InlineBlocking::enter();
        complete(&mut read)
    }))
    .unwrap_err();
    assert_eq!(
        payload.downcast_ref::<u64>(),
        Some(&0x7072_6570_6172_6564_u64)
    );
    assert_eq!(meter.read.load(Ordering::Acquire), 3);
    drop(payload);
    drop(read);
    let deadline = Instant::now() + Duration::from_secs(10);
    while meter.memory.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    assert!(file.health.tail.lock().unwrap().is_none());
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn bounded_read_byte_and_memory_refusals_precede_io() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("admission");
    std::fs::write(&path, b"abc")?;
    let file = bounded_write_file(&path, false)?;
    let byte_meter = Arc::new(ReadBudgetMeter::new(8, 2));
    assert!(
        file.read_bounded_async(0, 3, byte_meter.clone(), false)
            .is_err()
    );
    let mut limits = ReadBudgetMeter::new(8, 3);
    limits.memory_limit = 0;
    let memory_meter = Arc::new(limits);
    assert!(
        file.read_bounded_async(0, 3, memory_meter.clone(), false)
            .is_err()
    );
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
        payload
            .try_reserve_exact(if cancelled { 17 } else { 64 })
            .map_err(io::Error::other)?;
        observer.cancelled.store(cancelled, Ordering::Release);
        let actual = payload.capacity() as u64;
        assert_eq!(
            allocation
                .reconcile_capacity(payload.capacity())
                .unwrap_err()
                .kind(),
            if cancelled {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::Other
            }
        );
        assert_eq!(allocation.bytes(), actual);
        assert_eq!(observer.memory.load(Ordering::Acquire), actual);
        drop(payload);
        drop(allocation);
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn zero_bounded_read_needs_no_quota() -> io::Result<()> {
    let file = NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    let mut limits = ReadBudgetMeter::new(0, 0);
    limits.memory_limit = 0;
    limits.allocation_limit = 0;
    let meter = Arc::new(limits);
    let output = complete(file.read_bounded_async(u64::MAX, 0, meter.clone(), false)?)?;
    assert!(output.0.is_empty());
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn bounded_write_retains_custody_through_denial_drop_and_cancellation() -> io::Result<()> {
    for (payload, denied, cancelled) in [
        (b"persisted".to_vec(), true, false),
        (b"persisted".to_vec(), false, false),
        (b"persisted".to_vec(), false, true),
        (vec![0x5a; 1024 * 1024 + 7], false, true),
    ] {
        let temporary = tempfile::NamedTempFile::new()?;
        let file = bounded_write_file(temporary.path(), true)?;
        let expected = if denied {
            Vec::new()
        } else {
            payload[..payload.len().min(1024 * 1024)].to_vec()
        };
        let mut limits = ReadBudgetMeter::new(
            if cancelled {
                u64::MAX
            } else if cfg!(windows) {
                2
            } else {
                1
            },
            payload.len() as u64,
        );
        limits.memory_limit = if denied { 0 } else { u64::MAX };
        limits.cancel_after_read = cancelled;
        let meter = Arc::new(limits);
        let bytes_dropped = Arc::new(AtomicBool::new(false));
        let retired = Arc::new(AtomicBool::new(false));
        let bytes = Bytes::from_owner(ObservedWriteBytes {
            bytes: payload,
            dropped: bytes_dropped.clone(),
        });
        let custody = ObservedWriteCustody {
            bytes_dropped: bytes_dropped.clone(),
            retired: retired.clone(),
            meter: meter.clone(),
            actual_prefix: expected.len() as u64,
        };
        let completion = file.write_async(0, bytes, meter.clone(), custody);
        if denied {
            assert!(completion.is_err());
        } else if cancelled {
            assert_eq!(
                complete(completion?).unwrap_err().kind(),
                io::ErrorKind::Interrupted
            );
        } else {
            let mut write = completion?;
            let state = write.state.as_ref().unwrap().clone();
            let job = write.pending.take().unwrap();
            drop(write);
            assert!(!retired.load(Ordering::Acquire));
            assert!(!bytes_dropped.load(Ordering::Acquire));
            let _ = job.run();
            state
                .lock()
                .unwrap()
                .result
                .take()
                .expect("terminal write")?;
            drop(state);
        }
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(temporary.path())?, expected);
    }
    Ok(())
}
