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
                let mut bytes = NativeByteReservation::new(
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
                records: vec![Bytes::from(vec![i as u8, 0, 255])],
                if_tail: None,
                idempotency_key: None,
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
) -> (Trace, [u64; 8], u64, Vec<meter_trace::Event>) {
    let path = StreamPath::new("native/differential").unwrap();
    let meter = Arc::new(Meter {
        read_limit,
        ..Meter::default()
    });
    let request = ReadRequest { path, from, limit };
    meter_trace::begin();
    let _order = reorder.then(meter_trace::reorder_recordings);
    let result = if old {
        baseline_native_read(provider, request, meter.original()).await
    } else {
        provider.native_read(request, meter.original()).await
    };
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
    (trace, work, opened_bytes, meter_trace::finish())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn bounded_native_read_keeps_full_bytes_errors_and_work_trace(batches in 1usize..9, from in 0u64..10, limit in 0u32..10, read_limit in 0u64..28) {
        // Initialize two independent backends, and the test executor, outside
        // measurement. The executor's first ThreadNotify allocation is not read work.
        let baseline_provider = futures::executor::block_on(seed(batches));
        let candidate_provider = futures::executor::block_on(seed(batches));
        let before_bytes = std::cell::Cell::new(0);
        let after_bytes = std::cell::Cell::new(0);
        let (_, [before, after]) = acyclic_test_support::differential(
            || { let (trace, work, held, events) = futures::executor::block_on(observe(&baseline_provider, true, from, limit, read_limit, false)); before_bytes.set(held); (trace, work, normalize_baseline_events(events, held)) },
            || { let (trace, work, held, events) = futures::executor::block_on(observe(&candidate_provider, false, from, limit, read_limit, false)); after_bytes.set(held); (trace, work, events) },
        );
        funding_bound(before_bytes.get(), after_bytes.get(), before, after)?;
    }
}

fn funding_bound(
    before_held: u64,
    after_held: u64,
    before: acyclic_test_support::AllocationInfo,
    after: acyclic_test_support::AllocationInfo,
) -> proptest::test_runner::TestCaseResult {
    // TryUnfold stores both its cursor state and the in-flight step future.
    let saved = if before_held == 0 {
        0
    } else {
        2 * (std::mem::size_of::<BaselineCursor>() - std::mem::size_of::<VecDeque<HistoryWindow>>())
    } as u64;
    prop_assert_eq!(before.count_total, after.count_total);
    prop_assert_eq!(before_held, after_held + saved);
    prop_assert_eq!(before.bytes_total, after.bytes_total + saved);
    Ok(())
}

#[test]
fn restoring_unused_native_cursor_fields_fails_the_same_funding_bound() {
    for old_candidate in [false, true] {
        let baseline_provider = futures::executor::block_on(seed(1));
        let candidate_provider = futures::executor::block_on(seed(1));
        let before_held = std::cell::Cell::new(0);
        let after_held = std::cell::Cell::new(0);
        let (_, [before, after]) = acyclic_test_support::differential(
            || {
                let (trace, work, held, events) =
                    futures::executor::block_on(observe(&baseline_provider, true, 0, 1, 3, false));
                before_held.set(held);
                (trace, work, normalize_baseline_events(events, held))
            },
            || {
                let (trace, work, held, events) = futures::executor::block_on(observe(
                    &candidate_provider,
                    old_candidate,
                    0,
                    1,
                    3,
                    false,
                ));
                after_held.set(held);
                // This control changes only frame size, not the observer protocol.
                let events = if old_candidate {
                    normalize_baseline_events(events, held)
                } else {
                    events
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

// The sole intentional observer difference is the smaller native cursor frame.
// Retain every event and argument, adjusting exactly its allocation and the
// final aggregate refund. The funding property independently checks this delta.
fn normalize_baseline_events(
    mut events: Vec<meter_trace::Event>,
    held: u64,
) -> Vec<meter_trace::Event> {
    if held != 0 {
        let saved = 2
            * (std::mem::size_of::<BaselineCursor>()
                - std::mem::size_of::<VecDeque<HistoryWindow>>()) as u64;
        let allocation = events
            .iter_mut()
            .rfind(|event| event.0 == "allocate")
            .unwrap();
        allocation.1 = allocation.1.checked_sub(saved).unwrap();
        let refund = events
            .iter_mut()
            .rfind(|event| event.0 == "release")
            .unwrap();
        assert_eq!(refund.1, held);
        refund.1 -= saved;
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
    let expected_events = normalize_baseline_events(before.3, before.2);
    assert_eq!(expected_events.len(), after.3.len());
    assert!(std::panic::catch_unwind(|| assert_eq!(expected_events, after.3)).is_err());
    let expected = expected_events
        .iter()
        .position(|event| event.0 == "record_examination" && event.2 != 0)
        .unwrap();
    assert_eq!(after.3[expected].0, "record_reserved_bytes");
}
