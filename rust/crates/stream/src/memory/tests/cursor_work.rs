use super::*;
use proptest::prelude::*;
use std::sync::atomic::AtomicU64;

pub(super) struct BaselineCursor {
    pub(super) root: Option<Arc<History>>,
    pub(super) windows: VecDeque<HistoryWindow>,
    pub(super) next: u64,
}
impl BaselineCursor {
    fn at(next: u64) -> Self {
        Self {
            root: None,
            windows: VecDeque::new(),
            next,
        }
    }
    fn new(path: &PathState, tail: u64, next: u64) -> Self {
        if let Some(window) = path.prefix_window(next, tail) {
            return Self {
                root: path.history.clone(),
                windows: VecDeque::from([window]),
                next,
            };
        }
        let mut cursor = Self::at(next);
        baseline(&mut cursor, path.history.as_ref(), tail);
        cursor
    }
    pub(super) fn next_record(&mut self) -> Option<Record> {
        let window = self.windows.front_mut()?;
        let record = window.records.records.get(window.start)?.clone();
        window.start += 1;
        self.next = record.sequence.checked_add(1)?;
        if window.start == window.end {
            self.windows.pop_front();
        }
        Some(record)
    }
}
fn read(old: bool, baseline: &mut BaselineCursor, candidate: &mut HistoryCursor) -> Option<Record> {
    if old {
        baseline.next_record()
    } else {
        candidate.next_record()
    }
}

fn baseline(cursor: &mut BaselineCursor, root: Option<&Arc<History>>, tail: u64) {
    if match (&cursor.root, root) {
        (Some(old), Some(root)) => Arc::ptr_eq(old, root),
        (None, None) => true,
        _ => false,
    } {
        return;
    }
    let mut added = Vec::new();
    let mut node = root;
    while let Some(current) = node {
        HISTORY_VISITS.with(|count| count.set(count.get() + 1));
        if cursor
            .root
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, current))
        {
            added.reverse();
            cursor.windows.extend(added);
            cursor.root = root.cloned();
            return;
        }
        match current.as_ref() {
            History::Batch {
                parent, records, ..
            } => {
                if let Some(window) = history_window(Arc::clone(records), tail, cursor.next) {
                    added.push(window);
                }
                if records
                    .records
                    .first()
                    .is_some_and(|first| first.sequence <= cursor.next)
                {
                    break;
                }
                node = parent.as_ref();
            }
            History::Prefix { .. } => break,
        }
    }
    added.clear();
    for (node, ceiling) in HistoryWalk::new(root, tail, cursor.next) {
        if let History::Batch { records, .. } = node
            && let Some(window) = history_window(Arc::clone(records), ceiling, cursor.next)
        {
            added.push(window);
        }
    }
    added.reverse();
    cursor.windows = added.into();
    cursor.root = root.cloned();
}

async fn seed(
    batches: usize,
    fork: bool,
) -> (
    MemoryStream,
    StreamPath,
    Vec<Result<AppendOutcome, StreamError>>,
    Vec<Result<ForkReceipt, StreamError>>,
) {
    let provider =
        MemoryStream::new_with_clock(MemoryLimits::default(), Arc::new(AtomicU64::new(7)));
    let source = StreamPath::new("history/source").unwrap();
    let mut appends = Vec::new();
    for i in 0..batches {
        appends.push(
            provider
                .append(AppendRequest {
                    path: source.clone(),
                    records: vec![(Bytes::from(vec![i.to_le_bytes()[0], 0, 255])).into()],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await,
        );
    }
    let mut forks = Vec::new();
    let path = if fork {
        let target = StreamPath::new("history/fork").unwrap();
        forks.push(
            provider
                .fork(ForkRequest {
                    source,
                    destination: target.clone(),
                    at_tail: Some((batches / 2) as u64),
                    idempotency_key: None,
                })
                .await,
        );
        forks.push(
            provider
                .fork(ForkRequest {
                    source: StreamPath::new("history/source").unwrap(),
                    destination: target.clone(),
                    at_tail: None,
                    idempotency_key: None,
                })
                .await,
        );
        target
    } else {
        source
    };
    (provider, path, appends, forks)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn cursor_windows_preserve_full_records_errors_and_observation_cuts(batches in 1usize..25, fork in any::<bool>(), cross_prefix in any::<bool>(), from in 0usize..25, cut in 0usize..25) {
        let run = |old| futures::executor::block_on(async {
                let (provider, path, appends, forks) = seed(batches, fork).await;
                let state = provider.state.read().await;
                let initial_path = if cross_prefix { StreamPath::new("history/source").unwrap() } else { path.clone() };
                let stream = state.paths.get(&initial_path).unwrap();
                let start = (from as u64).min(stream.tail);
                let mut cursor = HistoryCursor::at(start);
                let mut old_cursor = BaselineCursor::at(start);
                if old { baseline(&mut old_cursor, stream.history.as_ref(), stream.tail); }
                else { cursor.sync(stream.history.as_ref(), stream.tail); }
                let first: Vec<_> = std::iter::from_fn(|| read(old, &mut old_cursor, &mut cursor)).map(record_bytes).take(cut).collect();
                drop(state);
                let appended = provider.append(AppendRequest {
                    path: path.clone(), records: vec![(Bytes::from_static(b"new\0bytes")).into()],
                    if_tail: None, idempotency_key: None,
                }).await;
                let state = provider.state.read().await;
                let stream = state.paths.get(&path).unwrap();
                if old { baseline(&mut old_cursor, stream.history.as_ref(), stream.tail); }
                else { cursor.sync(stream.history.as_ref(), stream.tail); }
                let rest: Vec<_> = std::iter::from_fn(|| read(old, &mut old_cursor, &mut cursor)).map(record_bytes).collect();
                (appends, forks, appended, first, rest, if old { old_cursor.next } else { cursor.next })
        });
        let _ = acyclic_test_support::differential(|| run(true), || run(false));
    }
}

#[test]
fn initial_cursor_visits_each_batch_once_and_rescan_control_fails() {
    for batches in [4, 32] {
        let mut counts = Vec::new();
        let mut allocations = Vec::new();
        let mut observations = Vec::new();
        for old in [true, false] {
            let (provider, path, _, _) = futures::executor::block_on(seed(batches, false));
            let state = futures::executor::block_on(provider.state.read());
            let stream = state.paths.get(&path).unwrap();
            let mut cursor = HistoryCursor::at(0);
            let mut old_cursor = BaselineCursor::at(0);
            HISTORY_VISITS.with(|count| count.set(0));
            allocations.push(acyclic_test_support::measure(|| {
                if old {
                    baseline(&mut old_cursor, stream.history.as_ref(), stream.tail);
                } else {
                    cursor.sync(stream.history.as_ref(), stream.tail);
                }
            }));
            counts.push(HISTORY_VISITS.with(std::cell::Cell::get));
            observations.push(
                std::iter::from_fn(|| read(old, &mut old_cursor, &mut cursor))
                    .map(record_bytes)
                    .collect::<Vec<_>>(),
            );
        }
        let ([old_records, new_records], [old_count, _], [old_heap, new_heap]) = (
            observations.as_slice(),
            counts.as_slice(),
            allocations.as_slice(),
        ) else {
            panic!("each cursor runs once");
        };
        assert_eq!(old_records, new_records);
        assert_eq!(counts, [2 * batches, batches]);
        assert_ne!(
            *old_count, batches,
            "rescan control escaped the exact bound"
        );
        assert_eq!(old_heap.count_total, new_heap.count_total);
        assert_eq!(new_heap.count_total, if batches == 4 { 1 } else { 4 });
        println!("cursor batches={batches} visits={counts:?} allocations={allocations:?}");
    }
}

#[test]
fn appended_follow_reuses_the_cursor_queue_without_scratch_allocations() {
    let mut observations = Vec::new();
    let mut measured = Vec::new();
    for old in [true, false] {
        let (provider, path, _, _) = futures::executor::block_on(seed(4, false));
        let mut cursor = HistoryCursor::at(0);
        let mut old_cursor = BaselineCursor::at(0);
        {
            let state = futures::executor::block_on(provider.state.read());
            let stream = state.paths.get(&path).unwrap();
            if old {
                baseline(&mut old_cursor, stream.history.as_ref(), stream.tail);
            } else {
                cursor.sync(stream.history.as_ref(), stream.tail);
            }
            while read(old, &mut old_cursor, &mut cursor).is_some() {}
        }
        futures::executor::block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![(Bytes::from_static(b"next")).into()],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let state = futures::executor::block_on(provider.state.read());
        let stream = state.paths.get(&path).unwrap();
        HISTORY_VISITS.with(|count| count.set(0));
        let allocations = acyclic_test_support::measure(|| {
            if old {
                baseline(&mut old_cursor, stream.history.as_ref(), stream.tail);
            } else {
                cursor.sync(stream.history.as_ref(), stream.tail);
            }
        });
        measured.push((
            HISTORY_VISITS.with(std::cell::Cell::get),
            allocations.count_total,
            allocations.bytes_total,
        ));
        observations.push(
            std::iter::from_fn(|| read(old, &mut old_cursor, &mut cursor))
                .map(record_bytes)
                .collect::<Vec<_>>(),
        );
    }
    let ([old_records, new_records], [before, after]) =
        (observations.as_slice(), measured.as_slice())
    else {
        panic!("each cursor runs once");
    };
    assert_eq!(old_records, new_records);
    assert_eq!(
        *before,
        (2, 1, (4 * std::mem::size_of::<HistoryWindow>()) as u64)
    );
    assert_eq!(*after, (1, 0, 0));
    assert_ne!(
        before, after,
        "old scratch allocation escaped exact work assertions"
    );
    println!("appended_follow before={before:?} after={after:?}");
}

fn record_bytes(record: Record) -> (u64, Vec<u8>, CommitId, u64) {
    let Record {
        sequence,
        value,
        commit_id,
        committed_at_micros,
        headers: _,
    } = record;
    (sequence, value.to_vec(), commit_id, committed_at_micros)
}

#[test]
fn singleton_reads_retain_the_window_inline_without_allocating() {
    let mut measurements = Vec::new();
    let mut observations = Vec::new();
    for old in [true, false] {
        let (provider, path, _, _) = futures::executor::block_on(seed(1, false));
        let state = futures::executor::block_on(provider.state.read());
        let stream = state.paths.get(&path).unwrap();
        let (mut cursor, mut old_cursor) = (HistoryCursor::at(0), BaselineCursor::at(0));
        let allocation = acyclic_test_support::measure(|| {
            if old {
                old_cursor = BaselineCursor::new(stream, stream.tail, 0);
            } else {
                cursor = HistoryCursor::new(stream, stream.tail, 0);
            }
        });
        measurements.push((allocation.count_total, allocation.bytes_total));
        observations.push(
            std::iter::from_fn(|| read(old, &mut old_cursor, &mut cursor))
                .map(record_bytes)
                .collect::<Vec<_>>(),
        );
    }
    let ([old_records, new_records], [before, after]) =
        (observations.as_slice(), measurements.as_slice())
    else {
        panic!("each cursor runs once");
    };
    assert_eq!(old_records, new_records);
    assert_eq!(*before, (1, std::mem::size_of::<HistoryWindow>() as u64));
    assert_eq!(*after, (0, 0));
    assert_ne!(
        before, after,
        "old singleton heap queue escaped the exact allocation bound"
    );
    println!("singleton cursor before={before:?} after={after:?}");
}
