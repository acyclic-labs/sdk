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
        futures::FutureExt::now_or_never(candidate.next_record())
            .expect("memory pages are immediately ready")
            .unwrap()
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
    let mut provider =
        MemoryStream::new_with_clock(MemoryLimits::default(), Arc::new(AtomicU64::new(7)));
    // Both implementations observe one logical authority and trusted clock.
    provider.revisions = Arc::new(crate::AppliedRevisions::new(
        crate::RevisionScope::new(Bytes::from_static(b"cursor-oracle")),
        1,
        0,
    ));
    let source = StreamPath::new("history/source").unwrap();
    let mut appends = Vec::new();
    for i in 0..batches {
        appends.push(
            provider
                .append(AppendRequest {
                    path: source.clone(),
                    records: vec![(Bytes::from(vec![i.to_le_bytes()[0], 0, 255])).into()],
                    if_tail: None,
                    idempotency_key: crate::IdempotencyKey::unique(),
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
                    exclude: Vec::new(),
                    source,
                    destination: target.clone(),
                    at_tail: Some((batches / 2) as u64),
                    idempotency_key: crate::IdempotencyKey::unique(),
                })
                .await,
        );
        forks.push(
            provider
                .fork(ForkRequest {
                    exclude: Vec::new(),
                    source: StreamPath::new("history/source").unwrap(),
                    destination: target.clone(),
                    at_tail: None,
                    idempotency_key: crate::IdempotencyKey::unique(),
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
                    if_tail: None, idempotency_key: crate::IdempotencyKey::unique(),
                }).await;
                let state = provider.state.read().await;
                let stream = state.paths.get(&path).unwrap();
                if old { baseline(&mut old_cursor, stream.history.as_ref(), stream.tail); }
                else { cursor.sync(stream.history.as_ref(), stream.tail); }
                let rest: Vec<_> = std::iter::from_fn(|| read(old, &mut old_cursor, &mut cursor)).map(record_bytes).collect();
                (appends, forks, appended, first, rest, if old { old_cursor.next } else { cursor.position() })
        });
        let _ = acyclic_test_support::differential(|| run(true), || run(false));
    }
}

#[test]
fn pinned_cursor_open_is_constant_space_and_eager_control_grows() {
    for batches in [4, 32, 1024] {
        let mut counts = Vec::new();
        let mut allocations = Vec::new();
        let mut first_output = Vec::new();
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
            let first = read(old, &mut old_cursor, &mut cursor).map(record_bytes);
            first_output.push(HISTORY_VISITS.with(std::cell::Cell::get));
            observations.push(
                first
                    .into_iter()
                    .chain(
                        std::iter::from_fn(|| read(old, &mut old_cursor, &mut cursor))
                            .map(record_bytes),
                    )
                    .collect::<Vec<_>>(),
            );
        }
        let ([old_records, new_records], [old_count, new_count], [old_heap, new_heap]) = (
            observations.as_slice(),
            counts.as_slice(),
            allocations.as_slice(),
        ) else {
            panic!("each cursor runs once");
        };
        assert_eq!(old_records, new_records);
        assert_eq!(*new_count, 0, "opening walked the pinned backlog");
        assert_eq!(new_heap.count_total, 0, "opening allocated backlog windows");
        assert!(*old_count >= batches, "eager control hid its history walk");
        assert!(old_heap.count_total > 0 && old_heap.bytes_total > new_heap.bytes_total);
        let [eager_first, lazy_first] = first_output.as_slice() else {
            panic!("two controls");
        };
        assert!(
            *lazy_first <= 64,
            "first output visited {lazy_first} history nodes"
        );
        if batches == 1024 {
            assert!(
                eager_first > lazy_first,
                "eager control hid its first-output slope"
            );
        }
        println!(
            "cursor batches={batches} open_visits={counts:?} first_output={first_output:?} allocations={allocations:?}"
        );
    }
}

#[test]
fn appended_follow_updates_its_pin_without_history_work_or_allocations() {
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
            idempotency_key: crate::IdempotencyKey::unique(),
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
    assert_eq!(*after, (0, 0, 0));
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
        origin: _,
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn pinned_page_budgets_preserve_fork_cuts(batches in 1usize..25, fork in any::<bool>(), from in 0usize..25, budget in 1u32..9, width in 1usize..10) {
        futures::executor::block_on(async {
            let (provider, mut path, _, _) = seed(batches, false).await;
            provider.append(AppendRequest {
                path: path.clone(), records: vec![Bytes::from_static(b"batch").into(); width],
                if_tail: None, idempotency_key: IdempotencyKey::unique(),
            }).await.unwrap();
            if fork {
                let destination = StreamPath::new("pages/fork").unwrap();
                provider.fork(ForkRequest {
                    source: path, destination: destination.clone(), at_tail: Some((batches + width / 2) as u64),
                    exclude: Vec::new(), idempotency_key: IdempotencyKey::unique(),
                }).await.unwrap();
                path = destination;
            }
            let snapshot = provider.state.read().await.paths.get(&path).cloned().unwrap();
            let start = (from as u64).min(snapshot.tail);
            let mut baseline_cursor = BaselineCursor::at(start);
            baseline(&mut baseline_cursor, snapshot.history.as_ref(), snapshot.tail);
            let expected: Vec<_> = std::iter::from_fn(|| baseline_cursor.next_record()).map(record_bytes).collect();
            let mut pinned = PinnedHistory::new(snapshot.history, snapshot.tail);
            let mut after = start;
            let mut actual = Vec::new();
            while let Some(window) = crate::pages::PageSource::next_page(&mut pinned, after, budget).await.unwrap() {
                prop_assert!(window.end - window.start <= budget as usize);
                for record in &window.records.records[window.start..window.end] {
                    prop_assert_eq!(record.sequence, after);
                    after += 1;
                    actual.push(record_bytes(record.clone()));
                }
            }
            prop_assert_eq!(actual, expected);
            prop_assert_eq!(after, pinned.tail);
            Ok(())
        })?;
    }
}

/// What following a chain cost: history nodes visited, pages that sought from
/// the root, the longest retained path and the peak live allocation.
#[derive(Debug)]
struct FollowCost {
    visits: usize,
    seeks: usize,
    path: usize,
    bytes: u64,
}

/// Follows the tip of a fork chain from zero, where every hop appended one
/// record. Returns the records' (value, origin) pairs and the follow's cost.
fn follow_chain(depth: usize) -> (Vec<(u64, String)>, FollowCost) {
    let provider = MemoryStream::new(MemoryLimits::default());
    let hop = |index: usize| StreamPath::new(format!("chain/{index}")).unwrap();
    futures::executor::block_on(async {
        for index in 0..depth {
            if index > 0 {
                provider
                    .fork(ForkRequest {
                        exclude: Vec::new(),
                        source: hop(index - 1),
                        destination: hop(index),
                        at_tail: None,
                        idempotency_key: crate::IdempotencyKey::unique(),
                    })
                    .await
                    .unwrap();
            }
            provider
                .append(AppendRequest {
                    path: hop(index),
                    records: vec![Bytes::from((index as u64).to_le_bytes().to_vec()).into()],
                    if_tail: Some(index as u64),
                    idempotency_key: crate::IdempotencyKey::unique(),
                })
                .await
                .unwrap();
        }
    });
    let state = futures::executor::block_on(provider.state.read());
    let tip = state.paths.get(&hop(depth - 1)).unwrap();
    let mut cursor = HistoryCursor::at(0);
    let mut records = Vec::with_capacity(depth);
    for counter in [&HISTORY_VISITS, &HISTORY_SEEKS, &HISTORY_PATH_PEAK] {
        counter.with(|count| count.set(0));
    }
    let allocations = acyclic_test_support::measure(|| {
        cursor.sync(tip.history.as_ref(), tip.tail);
        while let Some(record) = futures::FutureExt::now_or_never(cursor.next_record())
            .expect("memory pages are immediately ready")
            .unwrap()
        {
            // The oracle shares the origin and was reserved before measuring.
            records.push((
                u64::from_le_bytes(record.value.as_ref().try_into().unwrap()),
                record.origin,
            ));
        }
    });
    let records = records
        .into_iter()
        .map(|(value, origin)| (value, origin.to_string()))
        .collect();
    let cost = FollowCost {
        visits: HISTORY_VISITS.with(std::cell::Cell::get),
        seeks: HISTORY_SEEKS.with(std::cell::Cell::get),
        path: HISTORY_PATH_PEAK.with(std::cell::Cell::get),
        bytes: allocations.bytes_max,
    };
    (records, cost)
}

#[test]
fn a_deep_fork_chain_follows_each_origin_with_flat_work_per_record() {
    let mut per_depth = Vec::new();
    for depth in [2_000, 20_000] {
        let (records, cost) = follow_chain(depth);
        assert_eq!(records.len(), depth);
        for (sequence, (value, origin)) in records.iter().enumerate() {
            assert_eq!(*value, sequence as u64);
            assert_eq!(*origin, format!("chain/{sequence}"));
        }
        println!("fork chain depth={depth} {cost:?}");
        // One seek from the tip; the chain holds a batch and a fork cut per
        // record, each visited once more or less, plus that seek's path.
        let nodes = 2 * depth;
        let levels = nodes.ilog2() as usize + 1;
        assert!(
            cost.visits <= 2 * nodes + 8 * levels,
            "{} visits for {depth} records",
            cost.visits
        );
        assert_eq!(cost.seeks, 1, "a page restarted from the root");
        // Skew-binary links reach any ancestor in about 2 log2 n steps.
        assert!(
            cost.path <= 2 * levels + 8,
            "path {} at depth {depth}",
            cost.path
        );
        per_depth.push(cost);
    }
    // Memory grows only by the retained path, logarithmic in depth.
    let entry = std::mem::size_of::<(Arc<History>, u64)>() as u64;
    assert!(
        per_depth[1].bytes <= per_depth[0].bytes + 2 * entry * per_depth[1].path as u64,
        "follow memory grew with depth: {per_depth:?}"
    );
}
