use super::cursor_work::BaselineCursor;
use super::*;
use crate::test_meter::{Meter, trace as meter_trace};
use proptest::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

// Original native read, including its concrete old cursor frame and admission order.
fn baseline_native_read<'a>(
    provider: &'a MemoryStream,
    request: ReadRequest,
    meter: Arc<dyn NativeReadMeter>,
) -> crate::NativeProviderFuture<'a, crate::NativeRecordStream> {
    let original = Arc::clone(&meter);
    crate::NativeProviderFuture::new(&original, async move {
        if let Err(error) = validate_limit(request.limit) {
            return Ok(Err(error));
        }
        let state = provider.state.read().await;
        let path = inspect(&meter, || state.paths.get(&request.path))?;
        let Some(path) = path else {
            return Ok(Err(StreamError::NotFound));
        };
        if request.from > path.tail {
            return Ok(Err(StreamError::OutOfRange));
        }
        let count = usize::try_from((path.tail - request.from).min(u64::from(request.limit)))
            .map_err(|_| std::io::ErrorKind::InvalidInput)?;
        let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&meter);
        let mut windows = crate::native::vector(count, &meter, &mut allocation)?;
        let end = path
            .tail
            .min(request.from.saturating_add(u64::from(request.limit)));
        if let Some(window) = path.prefix_window(request.from, end) {
            meter.admit_examination(1, 0)?;
            windows.push(window);
            meter.record_examination(1, 0)?;
        } else {
            let mut history = HistoryWalk::new(path.history.as_ref(), end, request.from);
            while history.next.is_some() && history.ceiling > request.from {
                meter.admit_examination(1, 0)?;
                if let Some((History::Batch { records, .. }, ceiling)) = history.next()
                    && let Some(window) = history_window(Arc::clone(records), ceiling, request.from)
                {
                    windows.push(window);
                }
                meter.record_examination(1, 0)?;
            }
        }
        windows.reverse();
        let cursor_meter = Arc::clone(&meter);
        let cursor = BaselineCursor {
            root: None,
            windows: windows.into(),
            next: request.from,
        };
        let cursor = stream::try_unfold(cursor, move |mut cursor| {
            let meter = Arc::clone(&cursor_meter);
            async move {
                let Some(window) = cursor.windows.front() else {
                    return Ok(None);
                };
                let length = window
                    .records
                    .records
                    .get(window.start)
                    .ok_or(std::io::ErrorKind::InvalidData)?
                    .value
                    .len();
                meter.check()?;
                let mut bytes = acyclic_native_runtime::NativeByteReservation::new(
                    Arc::clone(&meter),
                    NativeOperationKind::Read,
                    length,
                )?;
                let copied = std::mem::size_of::<Record>() as u64;
                meter.admit_examination(1, copied)?;
                let record = cursor
                    .next_record()
                    .ok_or(std::io::ErrorKind::InvalidData)?;
                let examined = meter.record_examination(1, copied);
                let read = bytes.record(length);
                examined?;
                read?;
                Ok(Some((
                    Ok(crate::NativeStreamOutput::new(
                        record,
                        ReadAllocation::empty(&meter),
                    )),
                    cursor,
                )))
            }
        });
        let mut frame =
            acyclic_native_runtime::ReadAllocation::new(&meter, std::mem::size_of_val(&cursor))?;
        allocation.absorb(&mut frame)?;
        let cursor: crate::NativeRecordStream = Box::pin(cursor);
        drop(state);
        Ok(Ok(crate::NativeStreamOutput::new(cursor, allocation)))
    })
}

type Trace = Vec<Result<(u64, Vec<u8>, CommitId, u64), String>>;
async fn seed(batches: usize) -> MemoryStream {
    let provider =
        MemoryStream::new_with_clock(MemoryLimits::default(), Arc::new(AtomicU64::new(7)));
    let path = StreamPath::new("native/differential").unwrap();
    for i in 0..batches {
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![(Bytes::from(vec![i.to_le_bytes()[0], 0, 255])).into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await
            .unwrap();
    }
    provider
}
async fn observe(
    provider: &MemoryStream,
    old: bool,
    from: u64,
    limit: u32,
    read_limit: u64,
    reorder: bool,
) -> (Trace, [u64; 8], u64, Vec<meter_trace::Event>, u64) {
    let path = StreamPath::new("native/differential").unwrap();
    let meter = Arc::new(Meter {
        read_limit,
        ..Meter::default()
    });
    let request = ReadRequest {
        path,
        from,
        limit,
        at_least: Default::default(),
    };
    meter_trace::begin();
    let _order = reorder.then(meter_trace::reorder_recordings);
    let future = if old {
        baseline_native_read(provider, request, meter.original())
    } else {
        provider.native_read(request, meter.original())
    };
    let opening_frame = match &future {
        crate::NativeProviderFuture::Ready(_) => 0,
        crate::NativeProviderFuture::Funded { future, .. } => {
            std::mem::size_of_val(future.as_ref().get_ref()) as u64
        }
    };
    let result = future.await;
    let mut trace = Vec::new();
    let mut opened_bytes = 0;
    match result {
        Err(error) => trace.push(Err(format!("io:{:?}:{error}", error.kind()))),
        Ok(Err(error)) => trace.push(Err(format!("provider:{error:?}"))),
        Ok(Ok(mut output)) => {
            opened_bytes = meter.held.load(Ordering::SeqCst);
            while let Some(row) = output.value_mut().next().await {
                trace.push(match row {
                    Err(error) => Err(format!("io:{:?}:{error}", error.kind())),
                    Ok(Err(error)) => Err(format!("provider:{error:?}")),
                    Ok(Ok(row)) => {
                        let row = row.value();
                        Ok((
                            row.sequence,
                            row.value.to_vec(),
                            row.commit_id,
                            row.committed_at_micros,
                        ))
                    }
                });
            }
        }
    }
    let work = [
        &meter.read,
        &meter.copied,
        &meter.examined,
        &meter.pending_read,
        &meter.pending_examination,
        &meter.held,
        &meter.allocations,
        &meter.read_operations,
    ]
    .map(|counter| counter.load(Ordering::SeqCst));
    (
        trace,
        work,
        opened_bytes,
        meter_trace::finish(),
        opening_frame,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn bounded_native_read_keeps_full_bytes_errors_and_work_trace(batches in 1usize..9, from in 0u64..10, limit in 0u32..10, read_limit in 0u64..28) {
        // Initialize two independent backends, and the test executor, outside
        // measurement. The executor's first ThreadNotify allocation is not read work.
        let baseline_provider = futures::executor::block_on(seed(batches));
        let candidate_provider = futures::executor::block_on(seed(batches));
        let before_bytes = std::cell::Cell::new((0, 0));
        let after_bytes = std::cell::Cell::new((0, 0));
        let (_, [before, after]) = acyclic_test_support::differential(
            || { let (trace, work, held, events, opening) = futures::executor::block_on(observe(&baseline_provider, true, from, limit, read_limit, false)); before_bytes.set((held, opening)); (trace, work, normalize_baseline_events(events, held, opening)) },
            || { let (trace, work, held, events, opening) = futures::executor::block_on(observe(&candidate_provider, false, from, limit, read_limit, false)); after_bytes.set((held, opening)); (trace, work, normalize_opening_frame(events, opening)) },
        );
        funding_bound(before_bytes.get(), after_bytes.get(), before, after)?;
    }
}

fn funding_bound(
    (before_held, before_opening): (u64, u64),
    (after_held, after_opening): (u64, u64),
    before: acyclic_test_support::AllocationInfo,
    after: acyclic_test_support::AllocationInfo,
) -> proptest::test_runner::TestCaseResult {
    // The paged cursor additionally retains its immutable snapshot and page cut.
    // TryUnfold holds both the idle state and the in-flight step future.
    let added = if before_held == 0 {
        0
    } else {
        native_cursor_frame_delta()
    };
    prop_assert_eq!(before.count_total, after.count_total);
    prop_assert_eq!(i128::from(before_held) + added, i128::from(after_held));
    prop_assert_eq!(
        i128::from(before.bytes_total) - i128::from(before_opening) + added,
        i128::from(after.bytes_total) - i128::from(after_opening)
    );
    Ok(())
}

#[test]
fn unfunded_native_cursor_storage_fails_the_same_funding_bound() {
    for old_candidate in [false, true] {
        let baseline_provider = futures::executor::block_on(seed(1));
        let candidate_provider = futures::executor::block_on(seed(1));
        let before_held = std::cell::Cell::new((0, 0));
        let after_held = std::cell::Cell::new((0, 0));
        let (_, [before, after]) =
            acyclic_test_support::differential(
                || {
                    let (trace, work, held, events, opening) = futures::executor::block_on(
                        observe(&baseline_provider, true, 0, 1, 3, false),
                    );
                    before_held.set((held, opening));
                    (
                        trace,
                        work,
                        normalize_baseline_events(events, held, opening),
                    )
                },
                || {
                    let (trace, work, held, events, opening) = futures::executor::block_on(
                        observe(&candidate_provider, old_candidate, 0, 1, 3, false),
                    );
                    after_held.set((held, opening));
                    // This control changes only frame size, not the observer protocol.
                    let events = if old_candidate {
                        normalize_baseline_events(events, held, opening)
                    } else {
                        normalize_opening_frame(events, opening)
                    };
                    (trace, work, events)
                },
            );
        assert_eq!(
            funding_bound(before_held.get(), after_held.get(), before, after).is_ok(),
            !old_candidate
        );
    }
}

fn native_cursor_frame_delta() -> i128 {
    2 * (std::mem::size_of::<NativeReadCursor>() as i128
        - std::mem::size_of::<BaselineCursor>() as i128)
}

// For reads fitting one page, only the funded cursor frame changes.
// Retain every event and argument, adjusting exactly its allocation and the
// final aggregate refund. The funding property independently checks this delta.
fn normalize_baseline_events(
    mut events: Vec<meter_trace::Event>,
    held: u64,
    opening: u64,
) -> Vec<meter_trace::Event> {
    if held != 0 {
        let added = native_cursor_frame_delta();
        let allocation = events
            .iter_mut()
            .rfind(|event| event.0 == "allocate")
            .unwrap();
        allocation.1 = u64::try_from(i128::from(allocation.1) + added).unwrap();
        let refund = events
            .iter_mut()
            .rfind(|event| event.0 == "release")
            .unwrap();
        assert_eq!(refund.1, held);
        refund.1 = u64::try_from(i128::from(refund.1) + added).unwrap();
    }
    normalize_opening_frame(events, opening)
}

// Opening futures have different concrete compiler-generated frames. Verify
// their real allocation/refund against that exact physical size before removing
// only those sizes from the observer comparison; allocation events remain.
fn normalize_opening_frame(
    mut events: Vec<meter_trace::Event>,
    opening: u64,
) -> Vec<meter_trace::Event> {
    if opening != 0 {
        assert_eq!(
            events.first().map(|event| (event.0, event.1)),
            Some(("allocate", opening))
        );
        events[0].1 = 0;
        let release = events
            .iter_mut()
            .find(|event| event.0 == "release" && event.1 == opening)
            .unwrap();
        release.1 = 0;
    }
    events
}

#[test]
fn reordered_recording_fails_observer_equality_with_unchanged_bytes_and_totals() {
    let baseline = futures::executor::block_on(seed(2));
    let broken = futures::executor::block_on(seed(2));
    let before = futures::executor::block_on(observe(&baseline, true, 0, 2, 6, false));
    let after = futures::executor::block_on(observe(&broken, false, 0, 2, 6, true));
    assert_eq!(before.0, after.0);
    assert_eq!(before.1, after.1);
    let expected_events = normalize_baseline_events(before.3, before.2, before.4);
    let after_events = normalize_opening_frame(after.3, after.4);
    assert_eq!(expected_events.len(), after_events.len());
    assert!(std::panic::catch_unwind(|| assert_eq!(expected_events, after_events)).is_err());
    let expected = expected_events
        .iter()
        .position(|event| event.0 == "record_examination" && event.2 != 0)
        .unwrap();
    assert_eq!(
        after_events.get(expected).map(|event| event.0),
        Some("record_reserved_bytes")
    );
}
