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
) -> (Trace, [u64; 8], u64, Vec<meter_trace::Event>) {
    let path = StreamPath::new("native/differential").unwrap();
    let meter = Arc::new(Meter {
        read_limit,
        ..Meter::default()
    });
    let request = ReadRequest {
        key: None,
        path,
        from,
        limit,
        at_least: Default::default(),
    };
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

// Payload copying and byte custody keep their original protocol. History
// examinations and frame storage deliberately move from opening to page pulls.
fn payload_events(events: Vec<meter_trace::Event>) -> Vec<meter_trace::Event> {
    events
        .into_iter()
        .filter(|event| match event.0 {
            "admit_examination" | "record_examination" => event.2 != 0,
            "reserve_bytes" | "record_reserved_bytes" | "cancel_reserved_bytes" => true,
            _ => false,
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn lazy_native_read_keeps_records_refusals_and_payload_admission(batches in 1usize..9, from in 0u64..10, limit in 0u32..10, read_limit in 0u64..28) {
        let provider = futures::executor::block_on(seed(batches));
        let before = futures::executor::block_on(observe(&provider, true, from, limit, read_limit, false));
        let after = futures::executor::block_on(observe(&provider, false, from, limit, read_limit, false));
        prop_assert_eq!(before.0, after.0);
        for index in [0, 1, 3, 4, 5] {
            prop_assert_eq!(before.1[index], after.1[index]);
        }
        prop_assert_eq!(payload_events(before.3), payload_events(after.3));
    }
}

#[test]
fn native_read_open_has_constant_funding_and_eager_control_exposes_backlog() {
    let mut lazy = Vec::new();
    let mut eager = Vec::new();
    for batches in [8, 128, 1024] {
        let provider = futures::executor::block_on(seed(batches));
        for old in [true, false] {
            let meter = Arc::new(Meter::default());
            let request = ReadRequest {
                path: StreamPath::new("native/differential").unwrap(),
                from: 0,
                limit: batches as u32,
                key: None,
                at_least: Default::default(),
            };
            let mut output = futures::executor::block_on(async {
                if old {
                    baseline_native_read(&provider, request, meter.original()).await
                } else {
                    provider.native_read(request, meter.original()).await
                }
            })
            .unwrap()
            .unwrap();
            let opened = (
                meter.held.load(Ordering::SeqCst),
                meter.examined.load(Ordering::SeqCst),
                meter.allocations.load(Ordering::SeqCst),
            );
            let first = futures::executor::block_on(output.value_mut().next())
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(first.value().sequence, 0);
            assert_eq!(first.value().value.as_ref(), &[0, 0, 255]);
            let first_work = meter.examined.load(Ordering::SeqCst);
            drop(first);
            drop(output);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
            if old {
                eager.push((opened, first_work));
            } else {
                lazy.push((opened, first_work));
            }
            println!(
                "native read batches={batches} eager={old} open={opened:?} first_examinations={first_work}"
            );
        }
    }
    assert!(lazy.windows(2).all(|pair| pair[0].0 == pair[1].0));
    assert_eq!(lazy[0].0.1, 2, "opening traversed pinned history");
    assert_eq!(lazy[0].0.2, 2, "only the future and cursor frames allocate");
    assert!(
        eager
            .windows(2)
            .all(|pair| pair[0].0.0 < pair[1].0.0 && pair[0].0.1 < pair[1].0.1)
    );
    assert!(
        eager[2].1 > lazy[2].1,
        "eager control hid its first-output work"
    );
}

#[test]
fn reordered_recording_fails_payload_protocol_with_unchanged_bytes_and_totals() {
    let provider = futures::executor::block_on(seed(2));
    let before = futures::executor::block_on(observe(&provider, false, 0, 2, 6, false));
    let after = futures::executor::block_on(observe(&provider, false, 0, 2, 6, true));
    assert_eq!(before.0, after.0);
    assert_eq!(before.1, after.1);
    let expected = payload_events(before.3);
    let changed = payload_events(after.3);
    assert_ne!(expected, changed);
    let recorded = expected
        .iter()
        .position(|event| event.0 == "record_examination")
        .unwrap();
    assert_eq!(changed[recorded].0, "record_reserved_bytes");
}
