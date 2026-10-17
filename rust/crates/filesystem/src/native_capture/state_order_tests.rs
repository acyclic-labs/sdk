use super::*;
use proptest::prelude::*;

thread_local! {
    static PREVIOUS_SORT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) struct PreviousSort(bool);

pub(super) fn previous_sort(enabled: bool) -> PreviousSort {
    PreviousSort(PREVIOUS_SORT.replace(enabled))
}

pub(super) fn use_previous_sort() -> bool {
    PREVIOUS_SORT.get()
}

impl Drop for PreviousSort {
    fn drop(&mut self) {
        PREVIOUS_SORT.set(self.0);
    }
}

/// Host observations of a directory and a regular file, to stand for every
/// observed state of those kinds.
struct Kinds {
    _source: tempfile::TempDir,
    directory: HostStat,
    file: HostStat,
    meter: NativeMaterializationWork,
}

fn kinds() -> Kinds {
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("f"), b"body").unwrap();
    let meter =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let root = HostRoot::open(source.path(), &mut meter.clone()).unwrap();
    let limits = crate::model::VolumeLimits::default();
    let stat = |text: &str| {
        let path = NamespacePath::from_portable(
            &crate::path::PortablePath::parse(text, limits).unwrap(),
            limits,
        )
        .unwrap();
        let host = namespace_to_host_path_measured(&path, &meter).unwrap();
        let stat = observe_host_path(&root, &host, &meter).unwrap().unwrap();
        capture_drop_host_path(host, &meter).unwrap();
        capture_drop_path(path, &meter).unwrap();
        stat
    };
    let (directory, file) = (stat("/"), stat("/f"));
    Kinds {
        _source: source,
        directory,
        file,
        meter,
    }
}

fn record() -> FileRecord {
    let object = |kind| crate::ObjectId {
        kind,
        digest: crate::Digest::from_bytes([3; 32]),
    };
    FileRecord {
        file_id: crate::FileId::from_bytes([2; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: object(crate::ObjectKind::Metadata),
        payload: crate::kernel::FilePayload::InlineRegular(
            crate::kernel::InlineFileData::new(b"x").unwrap(),
        ),
    }
}

fn states(kinds: &Kinds, entries: &[(String, u8)]) -> Vec<CapturePathState> {
    let limits = crate::model::VolumeLimits::default();
    entries
        .iter()
        .map(|(text, rank)| {
            let path = NamespacePath::from_portable(
                &crate::path::PortablePath::parse(text, limits).unwrap(),
                limits,
            )
            .unwrap();
            match rank % 4 {
                0 => (None, path, None),
                1 => (Some(kinds.directory.clone()), path, None),
                2 => (Some(kinds.file.clone()), path, Some(record())),
                _ => (Some(kinds.file.clone()), path, None),
            }
        })
        .collect()
}

type Order = Vec<(String, bool, bool, bool)>;

fn order(states: &[CapturePathState]) -> Order {
    states
        .iter()
        .map(|(observation, path, current)| {
            (
                format!("{path:?}"),
                observation.is_some(),
                observation.as_ref().is_some_and(HostStat::is_dir),
                current.is_some(),
            )
        })
        .collect()
}

/// Sorts with `sort`, returning the order and the items it charged.
fn charged(
    kinds: &Kinds,
    mut states: Vec<CapturePathState>,
    sort: fn(&mut [CapturePathState], &NativeMaterializationWork) -> Result<(), CaptureError>,
) -> (Order, u64, u64) {
    let before = kinds.meter.receipt().items_examined;
    let heap = acyclic_test_support::measure(|| sort(&mut states, &kinds.meter).unwrap());
    (
        order(&states),
        kinds.meter.receipt().items_examined - before,
        heap.count_total,
    )
}

pub(super) fn comparison_sort(
    states: &mut [CapturePathState],
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    capture_sort_by(states, previous_state_order, meter)
}

// Frozen comparator from before the counting pass: neither its ranks nor
// its depth direction share the candidate's key construction.
fn previous_state_order(
    (left, left_path, left_current): &CapturePathState,
    (right, right_path, right_current): &CapturePathState,
) -> std::cmp::Ordering {
    fn rank(observation: &Option<HostStat>, current: &Option<FileRecord>) -> u8 {
        match observation {
            None => 0,
            Some(observation) if observation.is_dir() => 1,
            Some(_) if current.is_some() => 2,
            Some(_) => 3,
        }
    }
    let left_rank = rank(left, left_current);
    let right_rank = rank(right, right_current);
    left_rank.cmp(&right_rank).then_with(|| {
        if left_rank == 0 {
            right_path
                .depth()
                .cmp(&left_path.depth())
                .then_with(|| left_path.cmp(right_path))
        } else {
            left_path
                .depth()
                .cmp(&right_path.depth())
                .then_with(|| left_path.cmp(right_path))
        }
    })
}

fn paths() -> impl Strategy<Value = Vec<(String, u8)>> {
    proptest::collection::btree_map(
        proptest::collection::vec(prop_oneof![Just("a"), Just("b"), Just("c")], 1..6)
            .prop_map(|parts| format!("/{}", parts.join("/"))),
        0u8..4,
        0..200,
    )
    .prop_map(|map| map.into_iter().collect())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    /// The counting pass over path-ordered states is the comparison order, at
    /// a charge linear in the states and their depth classes.
    #[test]
    fn path_ordered_states_bucket_into_capture_order(entries in paths()) {
        let kinds = kinds();
        let mut ordered = entries.clone();
        ordered.sort_by(|left, right| {
            states(&kinds, std::slice::from_ref(left))[0]
                .1
                .cmp(&states(&kinds, std::slice::from_ref(right))[0].1)
        });
        let input = states(&kinds, &ordered);
        let count = input.len() as u64;
        let (expected, old_items, old_heap) = charged(&kinds, states(&kinds, &ordered), comparison_sort);
        let (actual, items, heap) = charged(&kinds, input, sort_capture_states);
        prop_assert_eq!(&actual, &expected);
        prop_assert_eq!(old_heap, 0);
        prop_assert_eq!(heap, u64::from(count > 128));
        let expected_items = if count < 4 { old_items } else { 3 * count - 2 };
        prop_assert_eq!(items, expected_items);
        prop_assert!(items <= old_items);
        // Reversed input selects the comparison sort before spending the
        // counting strategy's allowance.
        if count > 1 {
            let mut reversed = ordered.clone();
            reversed.reverse();
            let (fallback, fallback_items, fallback_heap) =
                charged(&kinds, states(&kinds, &reversed), sort_capture_states);
            let (_, comparison_items, comparison_heap) =
                charged(&kinds, states(&kinds, &reversed), comparison_sort);
            prop_assert_eq!(&fallback, &expected);
            prop_assert_eq!(fallback_items, comparison_items);
            prop_assert_eq!((fallback_heap, comparison_heap), (0, 0));
        }
    }
}

/// The path check is what makes the counting pass sound: without it, one
/// class out of path order stays out of order.
#[test]
fn unordered_states_need_the_comparison_sort() {
    let kinds = kinds();
    let entries = [("/b".to_owned(), 3), ("/a".to_owned(), 3)];
    let (expected, _, old_heap) = charged(&kinds, states(&kinds, &entries), comparison_sort);
    let (checked, _, heap) = charged(&kinds, states(&kinds, &entries), sort_capture_states);
    let (unchecked, _, unchecked_heap) = charged(
        &kinds,
        states(&kinds, &entries),
        unchecked_bucket_path_ordered_states,
    );
    assert_eq!((old_heap, heap, unchecked_heap), (0, 0, 0));
    assert_eq!(checked, expected);
    assert_ne!(unchecked, expected);
}

/// Deep chains need more depth classes than the stack holds; the measured
/// buffer gives the same order and is released.
#[test]
fn deep_paths_take_a_measured_class_buffer() {
    let kinds = kinds();
    let mut entries = Vec::new();
    for depth in 1..=24 {
        let chain = vec!["d"; depth].join("/");
        for (leaf, rank) in [("a", depth as u8), ("z", depth as u8 + 1)] {
            entries.push((format!("/{chain}/{leaf}"), rank));
        }
        entries.push((format!("/{chain}"), 1));
    }
    entries.sort_by(|left, right| {
        states(&kinds, std::slice::from_ref(left))[0]
            .1
            .cmp(&states(&kinds, std::slice::from_ref(right))[0].1)
    });
    let live = kinds.meter.receipt().peak_allocation_bytes;
    let (expected, _, old_heap) = charged(&kinds, states(&kinds, &entries), comparison_sort);
    let (actual, items, heap) = charged(&kinds, states(&kinds, &entries), sort_capture_states);
    assert_eq!((old_heap, heap), (0, 1));
    let count = entries.len() as u64;
    assert_eq!(actual, expected);
    assert_eq!(items, 2 * (count - 1) + count);
    assert!(kinds.meter.receipt().peak_allocation_bytes > live);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    /// Any batch funded for the old exact item receipt still succeeds. When
    /// either scratch allowance is zero, sorting needs no new allocation.
    #[test]
    fn old_budget_boundaries_admit_the_selected_strategy(
        count in 0usize..150,
        depth in 1usize..25,
        ranks in proptest::collection::vec(0u8..4, 150),
        spare in 0u64..2,
        no_bytes in any::<bool>(),
        no_operations in any::<bool>(),
    ) {
        let kinds = kinds();
        let prefix = vec!["d"; depth].join("/");
        let entries: Vec<_> = (0..count)
            .map(|index| (format!("/{prefix}/{index:03}"), ranks[index]))
            .collect();
        let (expected, old_items, _) =
            charged(&kinds, states(&kinds, &entries), comparison_sort);
        let budget = WorkBudget {
            items_examined: old_items + spare,
            // The original ledger remains live throughout the sort. Fund only
            // that required prefix, leaving zero spare scratch allowance.
            peak_allocation_bytes: if no_bytes {
                NativeMaterializationWork::accounting_state_bytes()
            } else { u64::MAX },
            allocation_operations: if no_operations { 1 } else { u64::MAX },
            ..WorkBudget::UNBOUNDED
        };
        let meter = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let initial = meter.receipt();
        let mut candidate = states(&kinds, &entries);
        let heap = acyclic_test_support::measure(|| {
            sort_capture_states(&mut candidate, &meter).unwrap();
        });
        prop_assert_eq!(order(&candidate), expected);
        let work = meter.receipt();
        prop_assert!(work.items_examined <= old_items);
        let needs_heap = count > 128 || depth + 1 >= 16;
        if count < 4 || (needs_heap && (no_bytes || no_operations)) {
            prop_assert_eq!(work.items_examined, old_items);
            prop_assert_eq!(work.allocation_operations, initial.allocation_operations);
            prop_assert_eq!(work.peak_allocation_bytes, initial.peak_allocation_bytes);
            prop_assert_eq!(heap.count_total, 0);
        } else {
            let requests = u64::from(count > 128) + u64::from(depth + 1 >= 16);
            prop_assert_eq!(heap.count_total, requests);
            prop_assert_eq!(work.allocation_operations, initial.allocation_operations + requests);
        }
    }
}

#[test]
fn rejected_counting_strategies_fail_the_same_budget_boundaries() {
    let kinds = kinds();
    // Negative controls restore the rejected strategy: two states exceed the
    // old item receipt; 129 states or deep paths need unfunded scratch.
    for (count, depth) in [(2, 1), (129, 1), (4, 16)] {
        let prefix = vec!["d"; depth].join("/");
        let entries: Vec<_> = (0..count)
            .map(|index| (format!("/{prefix}/{index:03}"), (index % 4) as u8))
            .collect();
        let (_, items, _) = charged(&kinds, states(&kinds, &entries), comparison_sort);
        let budget = WorkBudget {
            items_examined: items,
            allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            ..WorkBudget::UNBOUNDED
        };
        let rejected = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let mut input = states(&kinds, &entries);
        let result = capture_charge(
            &rejected,
            crate::materializer::NativeResourceCredit::Items.delta(2 * (count - 1) as u64),
        )
        .and_then(|()| unchecked_bucket_path_ordered_states(&mut input, &rejected));
        assert!(result.is_err());
        let funded = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        sort_capture_states(&mut states(&kinds, &entries), &funded).unwrap();
        assert_eq!(funded.receipt().allocation_operations, 1);
        assert!(funded.receipt().items_examined <= items);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn small_batches_preserve_both_sides_of_the_old_boundary(
        ranks in proptest::collection::vec(0u8..4, 2..4),
    ) {
        let kinds = kinds();
        let entries: Vec<_> = ranks.iter().enumerate()
            .map(|(index, rank)| (format!("/p{index}"), *rank)).collect();
        let (_, exact, _) = charged(&kinds, states(&kinds, &entries), comparison_sort);
        for limit in [exact - 1, exact, exact + 1] {
            let budget = WorkBudget { items_examined: limit, ..WorkBudget::UNBOUNDED };
            let old = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
            let new = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
            let mut baseline = states(&kinds, &entries);
            let mut candidate = states(&kinds, &entries);
            let expected = comparison_sort(&mut baseline, &old).map_err(|error| format!("{error:?}"));
            let actual = sort_capture_states(&mut candidate, &new).map_err(|error| format!("{error:?}"));
            prop_assert_eq!(actual.is_ok(), limit >= exact);
            prop_assert_eq!(actual, expected);
            prop_assert_eq!(order(&candidate), order(&baseline));
            prop_assert_eq!(new.receipt(), old.receipt());
        }
    }
}
