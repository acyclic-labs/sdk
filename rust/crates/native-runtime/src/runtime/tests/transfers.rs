use super::*;

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
        assert!(
            file.health
                .tail
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        );
        let weak = Arc::downgrade(&meter);
        drop(meter);
        let deadline = Instant::now() + Duration::from_secs(10);
        while weak.upgrade().is_some() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(
            weak.upgrade().is_none(),
            "returned owner must not retain invocation custody"
        );
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
    .expect_err("prepared observer resumes original terminal panic");
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
    assert!(
        file.health
            .tail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    );
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

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn bounded_write_custody_remains_with_admitted_job_after_observer_drop() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("custodied-detached");
    File::create(&path)?;
    let file = bounded_write_file(&path, true)?;
    let per_request = if cfg!(windows) { 2 } else { 1 };
    let meter = Arc::new(ReadBudgetMeter {
        memory_limit: u64::MAX,
        ..ReadBudgetMeter::new(per_request, 9)
    });
    let bytes_dropped = Arc::new(AtomicBool::new(false));
    let retired = Arc::new(AtomicBool::new(false));
    let bytes = Bytes::from_owner(ObservedWriteBytes {
        bytes: b"persisted".to_vec(),
        dropped: bytes_dropped.clone(),
    });
    let custody = ObservedWriteCustody {
        bytes_dropped: bytes_dropped.clone(),
        retired: retired.clone(),
        meter: meter.clone(),
        actual_prefix: 9,
    };
    let mut write = file.write_async(0, bytes, meter.clone(), custody)?;
    let completed = write.state.as_ref().expect("pending write").clone();
    let (sender, receiver) = mpsc::sync_channel(1);
    sender
        .try_send(write.pending.take().expect("the real write is pending"))
        .map_err(|_| io::Error::other("write admission failed"))?;
    drop(write);
    assert!(!retired.load(Ordering::Acquire));
    assert!(!bytes_dropped.load(Ordering::Acquire));
    let _ = receiver
        .recv()
        .map_err(|_| io::Error::from(io::ErrorKind::Other))?
        .run();
    completed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .result
        .take()
        .expect("real write completes")?;
    assert!(retired.load(Ordering::Acquire));
    assert_eq!(std::fs::read(&path)?, b"persisted");
    drop(completed);
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn bounded_write_cold_admission_drops_payload_before_custody() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("custodied-denied");
    File::create(&path)?;
    let file = bounded_write_file(&path, true)?;
    let mut budget = ReadBudgetMeter {
        memory_limit: u64::MAX,
        ..ReadBudgetMeter::new(2, 9)
    };
    budget.memory_limit = 0;
    let meter = Arc::new(budget);
    let bytes_dropped = Arc::new(AtomicBool::new(false));
    let retired = Arc::new(AtomicBool::new(false));
    let bytes = Bytes::from_owner(ObservedWriteBytes {
        bytes: b"persisted".to_vec(),
        dropped: bytes_dropped.clone(),
    });
    let custody = ObservedWriteCustody {
        bytes_dropped: bytes_dropped.clone(),
        retired: retired.clone(),
        meter: meter.clone(),
        actual_prefix: 0,
    };
    assert!(file.write_async(0, bytes, meter.clone(), custody).is_err());
    assert!(retired.load(Ordering::Acquire));
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    assert_eq!(std::fs::read(&path)?, b"");
    Ok(())
}

#[test]
fn positional_io_preserves_cursor_and_bounds_ranges() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("positional");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
    write_all_at(&file, 2, &[0x22; 3], None)?;
    write_all_at(&file, 16, &[0x33; 5], None)?;
    sync_file(&file, Durability::Full, None)?;

    let mut bytes = [0_u8; 24];
    assert_eq!(read_at(&file, 0, &mut bytes)?, 21);
    assert_eq!(&bytes[..8], &[0, 0, 0x22, 0x22, 0x22, 0, 0, 0]);
    assert_eq!(&bytes[8..16], &[0; 8]);
    assert_eq!(&bytes[16..21], &[0x33; 5]);
    assert_eq!(read_at(&file, 21, &mut bytes)?, 0);

    complete(
        NativeFile::from_file(file.try_clone()?, None)
            .map_err(|(error, _)| error)?
            .write_all_batch_async(vec![
                OwnedWrite {
                    offset: 32,
                    bytes: Bytes::from_static(&[0x44; 5]),
                },
                OwnedWrite {
                    offset: 48,
                    bytes: Bytes::from_static(&[0x55; 7]),
                },
            ]),
    )?;
    let mut batch = [0_u8; 23];
    assert_eq!(read_at(&file, 32, &mut batch)?, 23);
    assert_eq!(&batch[..5], &[0x44; 5]);
    assert_eq!(&batch[5..16], &[0; 11]);
    assert_eq!(&batch[16..], &[0x55; 7]);

    let mut cursor = file.try_clone()?;
    cursor.seek(SeekFrom::End(0))?;
    let selected = complete(read_batch_async(
        file.try_clone()?,
        vec![OwnedRead {
            offset: 16,
            length: 5,
        }],
    ))?;
    assert_eq!(selected, [Bytes::from_static(&[0x33; 5])]);
    assert_eq!(cursor.stream_position()?, 55);

    complete(
        NativeFile::from_file(file.try_clone()?, None)
            .map_err(|(error, _)| error)?
            .write_all_batch_async(vec![OwnedWrite {
                offset: 4,
                bytes: Bytes::from_static(b"cursor-independent"),
            }]),
    )?;
    let mut written = [0_u8; 18];
    assert_eq!(read_at(&file, 4, &mut written)?, written.len());
    assert_eq!(&written, b"cursor-independent");
    assert_eq!(cursor.stream_position()?, 55);
    Ok(())
}

#[test]
fn batch_larger_than_native_queue_preserves_every_write() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    let writes: Vec<_> = (0_u8..17)
        .map(|value| OwnedWrite {
            offset: u64::from(value) * 4,
            bytes: Bytes::from(vec![value; 3]),
        })
        .collect();
    complete(
        NativeFile::from_file(file.try_clone()?, None)
            .map_err(|(error, _)| error)?
            .write_all_batch_async(writes),
    )?;

    let mut actual = [0_u8; 67];
    assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
    for value in 0_u8..17 {
        let start = usize::from(value) * 4;
        assert_eq!(actual.get(start..start + 3), Some([value; 3].as_slice()));
        if value != 16 {
            assert_eq!(actual.get(start + 3), Some(&0));
        }
    }
    Ok(())
}

#[test]
fn overlapping_batch_writes_fail_before_any_write() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("overlapping-batch");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    std::fs::write(&path, b"unchanged")?;
    let result = complete(
        NativeFile::from_file(file, None)
            .map_err(|(error, _)| error)?
            .write_all_batch_async(vec![
                OwnedWrite {
                    offset: 0,
                    bytes: Bytes::from_static(b"first"),
                },
                OwnedWrite {
                    offset: 3,
                    bytes: Bytes::from_static(b"second"),
                },
            ]),
    );
    assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    assert_eq!(std::fs::read(&path)?, b"unchanged");
    Ok(())
}

#[cfg(windows)]
#[test]
fn native_write_batch_preserves_empty_writes_and_port_reuse() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    for value in 0_u8..8 {
        complete(
            NativeFile::from_file(file.try_clone()?, None)
                .map_err(|(error, _)| error)?
                .write_all_batch_async(vec![
                    OwnedWrite {
                        offset: u64::from(value) * 2,
                        bytes: Bytes::copy_from_slice(&[value]),
                    },
                    OwnedWrite {
                        offset: u64::from(value) * 2 + 1,
                        bytes: Bytes::new(),
                    },
                ]),
        )?;
    }
    let mut actual = [0_u8; 15];
    assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
    for value in 0_u8..8 {
        assert_eq!(actual.get(usize::from(value) * 2), Some(&value));
    }
    Ok(())
}

/// An inline thread serves an overlapped handle's positional I/O in place with exactly the driver's results, before and after the driver has attached that handle, and neither moves the other.
#[cfg(windows)]
#[test]
#[allow(
    unsafe_code,
    reason = "the test drives a raw overlapped handle through the unsafe constructor"
)]
fn inline_overlapped_io_matches_the_driver() -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

    let temporary = tempfile::tempdir()?;
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(temporary.path().join("in-place"))?;
    // SAFETY: the handle was just opened overlapped, is attached to no
    // port, and is transferred once.
    let native = unsafe {
        NativeFile::from_overlapped_file_unchecked(file, None).map_err(|(error, _)| error)?
    };
    let reads = || {
        [(5, 8), (1, 3), (0, 0), (12, 4), (30, 4)]
            .map(|(offset, length)| OwnedRead { offset, length })
            .to_vec()
    };
    let expected = [
        Bytes::from_static(b"fgh\0\0xy"),
        Bytes::from_static(b"bcd"),
        Bytes::new(),
        Bytes::new(),
        Bytes::new(),
    ];
    let write =
        |offset, bytes| complete(native.write_all_batch_async(vec![OwnedWrite { offset, bytes }]));
    {
        let _inline = InlineBlocking::enter();
        write(0, Bytes::from_static(b"abcdefgh"))?;
        write(10, Bytes::from_static(b"xy"))?;
        assert_eq!(complete(native.read_batch_async(reads()))?, expected);
    }
    assert_eq!(complete(native.read_batch_async(reads()))?, expected);
    write(0, Bytes::from_static(b"A"))?;
    {
        let _inline = InlineBlocking::enter();
        write(1, Bytes::from_static(b"B"))?;
        let read = complete(native.read_batch_async(vec![OwnedRead {
            offset: 0,
            length: 3,
        }]))?;
        assert_eq!(read, [Bytes::from_static(b"ABc")]);
    }
    assert_eq!(
        complete(native.read_batch_async(vec![OwnedRead {
            offset: 0,
            length: 3
        }]))?,
        [Bytes::from_static(b"ABc")]
    );
    Ok(())
}

#[test]
fn native_read_batch_preserves_order_and_short_reads() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    write_all_at(&file, 0, b"abcdefgh", None)?;
    let reads = complete(read_batch_async(
        file,
        vec![
            OwnedRead {
                offset: 5,
                length: 8,
            },
            OwnedRead {
                offset: 1,
                length: 3,
            },
        ],
    ))?;
    assert_eq!(
        reads,
        [Bytes::from_static(b"fgh"), Bytes::from_static(b"bcd")]
    );
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[test]
fn apple_batches_preserve_unlinked_open_files() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("unlinked-open-file");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    std::fs::remove_file(path)?;
    complete(
        NativeFile::from_file(file.try_clone()?, None)
            .map_err(|(error, _)| error)?
            .write_all_batch_async(vec![OwnedWrite {
                offset: 3,
                bytes: Bytes::from_static(b"kept"),
            }]),
    )?;
    assert_eq!(
        complete(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 3,
                length: 4
            }],
        ))?,
        [Bytes::from_static(b"kept")]
    );
    Ok(())
}

#[test]
fn invalid_positional_batches_leave_contents_and_cursor_unchanged() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("invalid-io-uring-offset");
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    write_all_at(&file, 0, b"stable", None)?;
    file.seek(SeekFrom::Start(3))?;

    let mut byte = [0_u8; 1];
    assert!(matches!(
        read_at(&file, u64::MAX, &mut byte),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        write_all_at(&file, u64::MAX, b"x", None),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        complete(read_batch_async(
            file.try_clone()?,
            vec![
                OwnedRead {
                    offset: 0,
                    length: 1,
                },
                OwnedRead {
                    offset: u64::MAX,
                    length: 1,
                },
            ],
        )),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(matches!(
        complete(NativeFile::from_file(file.try_clone()?, None).map_err(|(error, _)| error)?.write_all_batch_async(vec![
                OwnedWrite {
                    offset: 0,
                    bytes: Bytes::from_static(b"changed"),
                },
                OwnedWrite {
                    offset: u64::MAX,
                    bytes: Bytes::from_static(b"x"),
                },
            ],
        )),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert_eq!(file.stream_position()?, 3);
    assert_eq!(std::fs::read(path)?, b"stable");
    Ok(())
}

#[test]
fn admitted_write_outlives_dropped_observer() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("detached-write");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let mut write = NativeFile::from_file(file, None)
        .map_err(|(error, _)| error)?
        .write_all_batch_async(vec![OwnedWrite {
            offset: 0,
            bytes: Bytes::from_static(b"persisted"),
        }]);
    let (sender, receiver) = mpsc::sync_channel(1);
    let admitted = write
        .pending
        .take()
        .ok_or_else(|| io::Error::other("write was not pending admission"))?;
    let completed = Arc::clone(write.state.as_ref().expect("pending write"));
    sender
        .try_send(admitted)
        .map_err(|_| io::Error::other("write admission failed"))?;

    drop(write);
    let job = receiver
        .recv()
        .map_err(|_| io::Error::other("admitted write disappeared"))?;
    let _ = job.run();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(result) = completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .result
            .take()
        {
            result?;
            break;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "detached admitted write did not complete",
            ));
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }

    assert_eq!(std::fs::read(path)?, b"persisted");
    Ok(())
}

#[test]
fn dropped_admission_waiters_do_not_block_later_reads() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("admission-drop");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
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

#[cfg(windows)]
#[test]
fn native_read_batch_preserves_empty_and_eof_ranges() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    write_all_at(&file, 0, b"abc", None)?;
    let reads = complete(read_batch_async(
        file.try_clone()?,
        vec![
            OwnedRead {
                offset: 0,
                length: 0,
            },
            OwnedRead {
                offset: 3,
                length: 4,
            },
            OwnedRead {
                offset: 30,
                length: 4,
            },
        ],
    ))?;
    assert_eq!(reads, [Bytes::new(), Bytes::new(), Bytes::new()]);
    for (offset, expected) in b"abc".iter().copied().enumerate() {
        let repeated = complete(read_batch_async(
            file.try_clone()?,
            vec![OwnedRead {
                offset: offset as u64,
                length: 1,
            }],
        ))?;
        assert_eq!(repeated, [Bytes::copy_from_slice(&[expected])]);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn native_read_batch_windows_requests_beyond_the_ring() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    let expected: Vec<u8> = (0..17).collect();
    write_all_at(&file, 0, &expected, None)?;
    let requests = (0_u64..17)
        .map(|offset| OwnedRead { offset, length: 1 })
        .collect();
    let actual = complete(read_batch_async(file, requests))?;
    assert_eq!(
        actual,
        expected
            .into_iter()
            .map(|value| Bytes::copy_from_slice(&[value]))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn native_read_batch_windows_one_range_beyond_the_ring() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    let expected = Bytes::from(vec![0x69; 16 * 1024 * 1024 + 1]);
    write_all_at(&file, 3, &expected, None)?;
    let actual = complete(read_batch_async(
        file,
        vec![OwnedRead {
            offset: 3,
            length: expected.len(),
        }],
    ))?;
    assert_eq!(actual, [expected]);
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn file_scope_nested_unwind_and_thread_isolation() {
    let a = Arc::new(Mutex::new(None));
    let b = Arc::new(Mutex::new(None));
    with_file_scope(Some(&a), || {
        with_file_scope(Some(&b), || {
            with_file_scope(Some(&a), || {
                assert!(in_file_scope(&a) && in_file_scope(&b));
            })
        });
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_file_scope(Some(&b), || panic!("unwind"));
            }))
            .is_err()
        );
        assert!(in_file_scope(&a) && !in_file_scope(&b));
        let other = a.clone();
        assert!(
            !std::thread::spawn(move || in_file_scope(&other))
                .join()
                .unwrap()
        );
    });
    assert!(!in_file_scope(&a) && !in_file_scope(&b));
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
    assert!(output.bytes.is_empty());
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn bounded_write_cancellation_retires_custody_after_actual_prefix() -> io::Result<()> {
    for payload in [b"persisted".to_vec(), vec![0x5a; 1024 * 1024 + 7]] {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-cancel");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let expected = payload[..payload.len().min(1024 * 1024)].to_vec();
        let mut limits = ReadBudgetMeter::new(u64::MAX, payload.len() as u64);
        limits.cancel_after_read = true;
        let meter = Arc::new(limits);
        let bytes_dropped = Arc::new(AtomicBool::new(false));
        let retired = Arc::new(AtomicBool::new(false));
        let bytes = Bytes::from_owner(ObservedWriteBytes {
            bytes: payload,
            dropped: bytes_dropped.clone(),
        });
        let custody = ObservedWriteCustody {
            bytes_dropped,
            retired: retired.clone(),
            meter: meter.clone(),
            actual_prefix: expected.len() as u64,
        };
        let error = complete(file.write_async(0, bytes, meter.clone(), custody)?).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(&path)?, expected);
    }
    Ok(())
}
