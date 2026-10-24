use super::*;
use crate::async_storage::{DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue};
use acyclic_test_support::{AllocationInfo, differential};
use proptest::prelude::*;
use std::cell::Cell;

thread_local! {
    static RESTORE_CLONES: Cell<bool> = const { Cell::new(false) };
    static RESTORE_UNBOUNDED_PAIR: Cell<bool> = const { Cell::new(false) };
}
pub(crate) fn restore_clones() -> bool {
    RESTORE_CLONES.get()
}
pub(crate) fn restore_unbounded_pair() -> bool {
    RESTORE_UNBOUNDED_PAIR.get()
}
struct Restore;
impl Drop for Restore {
    fn drop(&mut self) {
        RESTORE_CLONES.set(false);
        RESTORE_UNBOUNDED_PAIR.set(false);
    }
}

type Event = (u8, ObjectId, u64);
struct Store(CachedObjectStore<MemoryObjectStore>);
impl AsyncObjectStore for Store {
    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        differential::record::<Event>((0, key.object_id, 0));
        self.0.decoded_cache_get(key)
    }
    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        differential::record::<Event>((1, key.object_id, 0));
        self.0.decoded_cache_admit(key, value)
    }
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        differential::record::<Event>((2, id, bytes.len() as u64));
        self.0.put(id, bytes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, ObjectStoreError> {
        differential::record::<Event>((3, id, maximum));
        self.0.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        for request in requests {
            differential::record::<Event>((4, request.object_id, request.maximum_bytes));
        }
        self.0.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        differential::record::<Event>((5, id, 0));
        self.0.contains(id, work).await
    }
}

fn canonical<F: Format>(values: &[F::Value]) -> Vec<u8> {
    canonical_page::<F>(PageRef::Leaf(values.into()))
}

fn canonical_page<F: Format>(page: PageRef<'_, F>) -> Vec<u8> {
    let token = CancellationToken::new();
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token);
    let (bytes, guard) = F::encode_with_accounting(&page, 128, &mut work).unwrap();
    let capacity = bytes.capacity() as u64;
    let output = bytes.clone();
    drop(bytes);
    retire_allocation(guard, capacity, &mut work).unwrap();
    output
}

async fn initialize<F: Format>(before: &[F::Value], after: &[F::Value]) -> (Store, [ObjectId; 2]) {
    initialize_layout::<F>(before, after, (usize::MAX, usize::MAX)).await
}

async fn initialize_layout<F: Format>(
    before: &[F::Value],
    after: &[F::Value],
    widths: (usize, usize),
) -> (Store, [ObjectId; 2]) {
    let store = Store(
        CachedObjectStore::new(MemoryObjectStore::default(), ObjectCacheOptions::default())
            .unwrap(),
    );
    let mut roots = Vec::new();
    for (values, width) in [(before, widths.0), (after, widths.1)] {
        if values.len() <= width {
            roots.push(install::<F>(&store, PageRef::Leaf(values.into())).await);
        } else {
            let mut children = Vec::new();
            for chunk in values.chunks(width) {
                children.push(Child {
                    first: F::key(&chunk[0]).clone(),
                    page: install::<F>(&store, PageRef::Leaf(chunk.into())).await,
                });
            }
            roots.push(install::<F>(&store, PageRef::Internal(children.as_slice().into())).await);
        }
    }
    (store, roots.try_into().unwrap())
}

async fn install<F: Format>(store: &Store, page: PageRef<'_, F>) -> ObjectId {
    let token = CancellationToken::new();
    let object = crate::HashedObject::new(F::kind(), canonical_page::<F>(page).into());
    store
        .put(
            object.object_id(),
            object.bytes().clone(),
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &token),
        )
        .await
        .unwrap();
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token);
    let (page, guard) = persistent_io::read_page_with_accounting::<_, F>(
        store,
        object.object_id(),
        DecodeLimits::default(),
        &mut work,
        &token,
        None,
    )
    .await
    .unwrap();
    let bytes = page.logical_bytes;
    drop(page);
    retire_allocation(guard, bytes, &mut work).unwrap();
    object.object_id()
}

type Output<F> = Result<
    (
        Vec<ValueChange<<F as Format>::Key, <F as Format>::Value>>,
        bool,
        Vec<Vec<u8>>,
    ),
    String,
>;
fn run<F: Format, const EARLY_EQUAL: bool>(
    runtime: &tokio::runtime::Runtime,
    fixture: (Store, [ObjectId; 2]),
    baseline: bool,
    maximum: u32,
    native: bool,
) -> (Output<F>, AllocationInfo, WorkCounters) {
    let (store, [before, after]) = fixture;
    RESTORE_CLONES.set(baseline);
    let _restore = Restore;
    let token = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &token).unwrap();
    let retained_before = original.live_allocation_bytes();
    let mut accounting = if native {
        KernelAccounting::native(&original, &token)
    } else {
        KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token)
    };
    let mut result = None;
    let started = std::time::Instant::now();
    let allocations = acyclic_test_support::measure(|| {
        result = Some(
            runtime.block_on(diff_tree_entries_or_records::<F, EARLY_EQUAL>(
                &store,
                before,
                after,
                maximum,
                &mut accounting,
                &token,
            )),
        );
    });
    let elapsed = started.elapsed();
    let work = accounting.spent();
    let output = result
        .unwrap()
        .map(|(diff, guard)| {
            let output_bytes = diff
                .changes
                .iter()
                .flat_map(|change| [change.before.as_ref(), change.after.as_ref()])
                .map(|value| canonical::<F>(value.map(std::slice::from_ref).unwrap_or(&[])))
                .collect();
            let output = (diff.changes.clone(), diff.truncated, output_bytes);
            let bytes = diff.retained_allocation_bytes;
            drop(diff);
            retire_allocation(guard, bytes, &mut accounting).unwrap();
            output
        })
        .map_err(|error| format!("{error:?}"));
    eprintln!(
        "borrowed leaf diff baseline={baseline} native={native}: {allocations:?}, {elapsed:?}"
    );
    assert_eq!(original.live_allocation_bytes(), retained_before);
    (output, allocations, work)
}

async fn diff_tree_entries_or_records<F: Format, const EARLY_EQUAL: bool>(
    store: &Store,
    before: ObjectId,
    after: ObjectId,
    maximum: u32,
    work: &mut KernelAccounting<'_>,
    token: &CancellationToken,
) -> Result<(Diff<F::Key, F::Value>, KernelOwnedAllocation), FsError> {
    diff_with_accounting::<_, F, true, true, EARLY_EQUAL>(
        store,
        Some(before),
        Some(after),
        maximum,
        DecodeLimits::default(),
        work,
        token,
    )
    .await
}

fn compare<F: Format, const EARLY_EQUAL: bool>(
    before: &[F::Value],
    after: &[F::Value],
    maximum: u32,
    widths: (usize, usize),
) where
    F::Key: std::fmt::Debug,
    F::Value: std::fmt::Debug,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        let [((old, _, _), old_trace), ((new, _, _), new_trace)] = differential::pair::<_, _, Event>(
            || runtime.block_on(initialize_layout::<F>(before, after, widths)),
            |fixture, baseline| run::<F, EARLY_EQUAL>(&runtime, fixture, baseline, maximum, native),
        );
        assert_eq!(new, old);
        assert_eq!(new_trace, old_trace);
    }
}

fn bounded_pair<F: Format>(before: &[F::Value], after: &[F::Value], maximum: u32, broken: bool)
where
    F::Key: std::fmt::Debug,
    F::Value: std::fmt::Debug,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        let [((old, _, _), old_trace), ((new, _, _), new_trace)] = differential::pair::<_, _, Event>(
            || runtime.block_on(initialize::<F>(before, after)),
            |fixture, baseline| {
                RESTORE_UNBOUNDED_PAIR.set(broken && !baseline);
                run::<F, true>(
                    &runtime,
                    fixture,
                    baseline,
                    if baseline { 128 } else { maximum },
                    native,
                )
            },
        );
        let mut expected = old.unwrap();
        let maximum = maximum as usize;
        expected.1 = expected.0.len() > maximum;
        expected.0.truncate(maximum);
        expected.2.truncate(2 * maximum);
        let actual = new.expect("bounded paired diff must truncate rather than fail allocation");
        assert_eq!(actual, expected);
        assert_eq!(new_trace, old_trace);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn paired_shared_leaves_preserve_full_bytes_errors_and_store_trace(
        before in prop::collection::btree_map(0_u8..32, 1_u8..4, 0..25),
        after in prop::collection::btree_map(0_u8..32, 1_u8..4, 0..25),
        maximum in 1_u32..65,
        left_width in 1_usize..9, right_width in 1_usize..9,
    ) {
        let trees = |values: &std::collections::BTreeMap<u8,u8>| values.iter().map(|(&key, &value)| entry(&format!("key-{key:02}"), value).unwrap()).collect::<Vec<_>>();
        for widths in [(usize::MAX, usize::MAX), (left_width, right_width)] {
        compare::<TreeFormat, false>(&trees(&before), &trees(&after), maximum, widths);
        let records = |values: &std::collections::BTreeMap<u8,u8>| values.iter().map(|(&key, &value)| { let mut record=file_record(key).unwrap(); record.link_count=u64::from(value); record }).collect::<Vec<_>>();
        compare::<FileTableFormat, false>(&records(&before), &records(&after), maximum, widths);
        // A separately initialized old collector sees the same complete diff,
        // canonical bytes, errors and observer trace across several frontiers.
        compare::<TreeFormat, true>(&trees(&before), &trees(&after), 128, widths);
        compare::<FileTableFormat, true>(&records(&before), &records(&after), 128, widths);
        }
    }

    #[test]
    fn bounded_proof_leaf_matches_the_complete_old_diff_prefix(
        before in prop::collection::btree_map(0_u8..32, 1_u8..4, 0..25),
        after in prop::collection::btree_map(0_u8..32, 1_u8..4, 0..25),
        maximum in 1_u32..33,
    ) {
        let trees = |values: &std::collections::BTreeMap<u8,u8>| values.iter().map(|(&key, &value)| entry(&format!("key-{key:02}"), value).unwrap()).collect::<Vec<_>>();
        bounded_pair::<TreeFormat>(&trees(&before), &trees(&after), maximum, false);
        let records = |values: &std::collections::BTreeMap<u8,u8>| values.iter().map(|(&key, &value)| { let mut record=file_record(key).unwrap(); record.link_count=u64::from(value); record }).collect::<Vec<_>>();
        bounded_pair::<FileTableFormat>(&records(&before), &records(&after), maximum, false);
    }
}

#[test]
#[should_panic(expected = "bounded paired diff must truncate rather than fail allocation")]
fn removed_pair_bound_breaks_the_same_complete_diff_prefix_property() {
    let before = (0..8)
        .map(|key| file_record(key).unwrap())
        .collect::<Vec<_>>();
    let mut after = before.clone();
    for record in &mut after {
        record.link_count = 2;
    }
    bounded_pair::<FileTableFormat>(&before, &after, 1, true);
}

fn allocation_pin(baseline: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        let mut counts = Vec::new();
        for count in [8, 24] {
            let before = (0..count)
                .map(|key| entry(&format!("key-{key:02}"), key).unwrap())
                .collect::<Vec<_>>();
            let mut after = before.clone();
            after[3].file_id = FileId::from_bytes([99; 16]);
            let fixture = runtime.block_on(initialize::<TreeFormat>(&before, &after));
            let (output, allocations, _) =
                run::<TreeFormat, false>(&runtime, fixture, baseline, 128, native);
            let (changes, truncated, _) = output.unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].before.as_ref(), Some(&before[3]));
            assert_eq!(changes[0].after.as_ref(), Some(&after[3]));
            assert!(!truncated);
            counts.push(allocations);
        }
        assert_eq!(
            counts[0], counts[1],
            "shared leaf diff cloned unchanged entries"
        );
    }
}
#[test]
fn only_changed_shared_values_are_allocated_at_both_history_sizes() {
    allocation_pin(false);
}
#[test]
#[should_panic(expected = "shared leaf diff cloned unchanged entries")]
fn restored_collector_fails_the_same_allocation_pin() {
    allocation_pin(true);
}

fn proof_allocation_pin(baseline: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        let mut counts = Vec::new();
        for count in [8_u8, 24] {
            let before = (0..count)
                .map(|key| file_record(key).unwrap())
                .collect::<Vec<_>>();
            let mut after = before.clone();
            after[0].link_count = 2;
            after[usize::from(count) - 1].link_count = 2;
            let width = usize::from(count) / 2;
            let fixture = runtime.block_on(initialize_layout::<FileTableFormat>(
                &before,
                &after,
                (width, width),
            ));
            let (output, allocations, work) =
                run::<FileTableFormat, true>(&runtime, fixture, baseline, 128, native);
            let (changes, truncated, _) = output.unwrap();
            assert!(!truncated);
            assert_eq!(changes.len(), 2);
            for (change, index) in changes.iter().zip([0, usize::from(count) - 1]) {
                assert_eq!(change.key, before[index].file_id);
                assert_eq!(change.before, Some(before[index]));
                assert_eq!(change.after, Some(after[index]));
            }
            eprintln!(
                "PROOF_DIFF count={count} native={native} baseline={baseline}: {allocations:?}, {work:?}"
            );
            counts.push((allocations, work.bytes_copied));
        }
        assert_eq!(
            counts[0], counts[1],
            "proof diff retained unchanged records across leaf frontiers"
        );
    }
}

#[test]
fn proof_diff_retains_only_changed_records_across_leaf_frontiers() {
    proof_allocation_pin(false);
}

#[test]
#[should_panic(expected = "proof diff retained unchanged records across leaf frontiers")]
fn restored_union_collector_fails_the_same_proof_allocation_pin() {
    proof_allocation_pin(true);
}

#[test]
fn rejected_roots_and_missing_or_malformed_pages_preserve_exact_errors_and_trace() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        for case in 0..4 {
            let [((old, _, _), old_trace), ((new, _, _), new_trace)] =
                differential::pair::<_, _, Event>(
                    || {
                        runtime.block_on(async {
                            let (store, mut roots) = initialize::<TreeFormat>(
                                &[entry("a", 1).unwrap()],
                                &[entry("a", 2).unwrap()],
                            )
                            .await;
                            match case {
                                1 => roots[1].kind = ObjectKind::Blob,
                                2 => roots[1].digest = Digest::from_bytes([99; 32]),
                                3 => {
                                    let object = crate::HashedObject::new(
                                        ObjectKind::TreePage,
                                        OwnedBytes::from_static(b"not a canonical page"),
                                    );
                                    roots[1] = object.object_id();
                                    store
                                        .put(
                                            roots[1],
                                            object.bytes().clone(),
                                            &mut crate::AuthorityWork::new(
                                                WorkBudget::UNBOUNDED,
                                                &CancellationToken::new(),
                                            ),
                                        )
                                        .await
                                        .unwrap();
                                }
                                _ => {}
                            }
                            (store, roots)
                        })
                    },
                    |fixture, baseline| {
                        run::<TreeFormat, true>(
                            &runtime,
                            fixture,
                            baseline,
                            if case == 0 { 0 } else { 128 },
                            native,
                        )
                    },
                );
            assert!(old.is_err());
            assert_eq!(new, old);
            assert_eq!(new_trace, old_trace);
        }
    }
}
