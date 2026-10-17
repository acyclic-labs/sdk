use super::*;
use proptest::prelude::*;

type Read<'a> = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'a>>;

fn completed(read: Read<'_>) -> io::Result<Vec<u8>> {
    complete(read)
}

/// One range read: where the cursor starts, what it covers, the caller's
/// buffer bound, which read API drives it, and whether the caller rejects it.
#[derive(Clone, Copy)]
struct Shape {
    offset: usize,
    length: usize,
    maximum: usize,
    api: u8,
    reject: bool,
}

fn read_range<F: std::borrow::Borrow<NativeFile>>(
    file: F,
    shape: Shape,
    meter: Arc<ReadBudgetMeter>,
    drive: impl FnOnce(Read<'_>) -> io::Result<Vec<u8>>,
) -> (Result<Vec<u8>, io::ErrorKind>, u64) {
    let Shape {
        offset,
        length,
        maximum,
        api,
        reject,
    } = shape;
    let mut range = AsyncRangeReader::new(file, offset as u64, length as u64);
    let accept = move |result: io::Result<(Vec<u8>, usize)>| {
        let (mut bytes, filled) = result?;
        if reject {
            return Err(io::ErrorKind::Interrupted.into());
        }
        bytes.truncate(filled);
        Ok(bytes)
    };
    let cursor = &mut range;
    let read: Read<'_> = match api {
        0 => Box::pin(cursor.read_prepared(vec![0x55; maximum], maximum, meter, true, accept)),
        1 => Box::pin(cursor.read_spare(
            (Vec::with_capacity(maximum), ()),
            maximum,
            meter,
            true,
            move |result| accept(result.map(|((bytes, ()), filled)| (bytes, filled))),
        )),
        _ => Box::pin(async move {
            cursor
                .read(maximum, meter, true)
                .await
                .map(|chunk| chunk.0.to_vec())
        }),
    };
    let result = drive(read);
    (result.map_err(|error| error.kind()), range.remaining())
}

fn cursor_matches(
    result: &Result<Vec<u8>, io::ErrorKind>,
    remaining: u64,
    length: usize,
) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(
        remaining.checked_add(result.as_ref().map_or(0, |bytes| bytes.len() as u64)),
        Some(length as u64)
    );
    Ok(())
}

#[test]
fn borrowed_ranges_match_owned_bytes_cursor_charges_and_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    // Exact charges hold only without other tests' load on the shared pools:
    // a contended submission also charges its admission waiter (see below).
    if passed_in_exclusive_process(
        "tests::range::borrowed_ranges_match_owned_bytes_cursor_charges_and_retirement",
    )? {
        return Ok(());
    }
    initialize_native_io_services()?;
    let _inline = InlineBlocking::enter();
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("borrowed-range");
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    });
    // All three read APIs have identical outcomes, charges and retirement with
    // borrowed and owned files, including EOF, cancellation and consumer refusal.
    runner.run(
        &(
            prop::collection::vec(any::<u8>(), 0..96),
            0_usize..128,
            0_usize..128,
            0_usize..128,
            any::<bool>(),
            any::<bool>(),
        ),
        |(body, offset, length, maximum, cancelled, reject)| {
            std::fs::write(&path, &body).unwrap();
            let file = Arc::new(bounded_write_file(&path, false).unwrap());
            for api in 0..3 {
                let make_meter = || {
                    let meter = Arc::new(ReadBudgetMeter::new(16, 256));
                    meter.cancelled.store(cancelled, Ordering::Release);
                    meter
                };
                let owned_meter = make_meter();
                let borrowed_meter = make_meter();
                let owned = read_range(
                    file.clone(),
                    Shape {
                        offset,
                        length,
                        maximum,
                        api,
                        reject,
                    },
                    owned_meter.clone(),
                    completed,
                );
                let borrowed = read_range(
                    file.as_ref(),
                    Shape {
                        offset,
                        length,
                        maximum,
                        api,
                        reject,
                    },
                    borrowed_meter.clone(),
                    completed,
                );
                prop_assert_eq!(&borrowed, &owned);
                cursor_matches(&borrowed.0, borrowed.1, length)?;
                // Cursor-only control: retaining an extra byte must fail the same
                // cursor invariant, even when the read itself fails.
                prop_assert!(
                    cursor_matches(&borrowed.0, borrowed.1.saturating_add(1), length).is_err()
                );
                let requested = length.min(maximum);
                let available = body.len().saturating_sub(offset).min(requested);
                if !cancelled && (api == 2 || !reject) && available == requested {
                    prop_assert_eq!(
                        borrowed.0.unwrap(),
                        body.get(offset..offset + available).unwrap_or_default()
                    );
                } else {
                    prop_assert!(borrowed.0.is_err());
                }
                for counter in [
                    |meter: &ReadBudgetMeter| meter.read.load(Ordering::Acquire),
                    |meter: &ReadBudgetMeter| meter.used_operations.load(Ordering::Acquire),
                    |meter: &ReadBudgetMeter| meter.used_bytes.load(Ordering::Acquire),
                    |meter: &ReadBudgetMeter| meter.used_allocations.load(Ordering::Acquire),
                ] {
                    prop_assert_eq!(counter(&owned_meter), counter(&borrowed_meter));
                }
                prop_assert_eq!(
                    borrowed_meter.read.load(Ordering::Acquire),
                    if cancelled { 0 } else { available as u64 }
                );
                prop_assert_eq!(borrowed_meter.memory.load(Ordering::Acquire), 0);
                prop_assert_eq!(owned_meter.memory.load(Ordering::Acquire), 0);
                prop_assert_eq!(Arc::strong_count(&file), 1);
            }
            Ok(())
        },
    )?;
    Ok(())
}

fn counters(meter: &ReadBudgetMeter) -> [u64; 4] {
    [
        meter.read.load(Ordering::Acquire),
        meter.used_operations.load(Ordering::Acquire),
        meter.used_bytes.load(Ordering::Acquire),
        meter.used_allocations.load(Ordering::Acquire),
    ]
}

/// Wakes the reader, but holds the first waking thread inside `wake` until
/// released, after reporting that it is there.
struct HeldWake {
    reader: std::thread::Thread,
    entered: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl Wake for HeldWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.reader.unpark();
        let entered = self.entered.lock().unwrap().take();
        if let Some(entered) = entered {
            let _ = entered.send(());
            let (open, changed) = &*self.release;
            let mut open = open.lock().unwrap();
            while !*open {
                open = changed.wait(open).unwrap();
            }
        }
    }
}

#[test]
fn a_range_read_that_waits_for_admission_charges_exactly_its_waiter() -> io::Result<()> {
    if passed_in_exclusive_process(
        "tests::range::a_range_read_that_waits_for_admission_charges_exactly_its_waiter",
    )? {
        return Ok(());
    }
    initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("contended-range");
    std::fs::write(&path, [7; 29])?;
    let file = Arc::new(bounded_write_file(&path, false)?);
    for api in 0..3 {
        for (offset, reject) in [(0, false), (56, true)] {
            let free = Arc::new(ReadBudgetMeter::new(16, 256));
            let contended = Arc::new(ReadBudgetMeter::new(16, 256));
            let uncontended = read_range(
                file.as_ref(),
                Shape {
                    offset,
                    length: 87,
                    maximum: 29,
                    api,
                    reject,
                },
                free.clone(),
                completed,
            );
            // Forced order: the pool is full at the first poll, so the read
            // registers an admission waiter. Freed capacity then makes a
            // worker wake it, and that worker is held inside the wake, still
            // holding the waiter node, while the reader is admitted, runs to
            // completion and has its charges inspected.
            let saturation = Saturation::new(&NATIVE_WORKERS)?;
            let mut retained_while_held = None;
            let waited = read_range(
                file.as_ref(),
                Shape {
                    offset,
                    length: 87,
                    maximum: 29,
                    api,
                    reject,
                },
                contended.clone(),
                |mut read: Read<'_>| {
                    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
                    let release =
                        Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
                    let waker = Waker::from(Arc::new(HeldWake {
                        reader: std::thread::current(),
                        entered: std::sync::Mutex::new(Some(entered_tx)),
                        release: Arc::clone(&release),
                    }));
                    assert!(
                        read.as_mut()
                            .poll(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                    drop(saturation);
                    entered_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(io::Error::other)?;
                    let result = complete(read);
                    retained_while_held = Some(contended.memory.load(Ordering::Acquire));
                    *release.0.lock().unwrap() = true;
                    release.1.notify_all();
                    result
                },
            );
            assert_eq!(waited, uncontended);
            assert_eq!(retained_while_held, Some(0), "api {api} offset {offset}");
            let [free_read, free_operations, free_bytes, free_allocations] = counters(&free);
            assert_eq!(
                counters(&contended),
                [free_read, free_operations, free_bytes, free_allocations + 1],
                "api {api} offset {offset}"
            );
        }
    }
    Ok(())
}
