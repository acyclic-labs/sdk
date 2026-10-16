use super::*;
use proptest::prelude::*;

type Changes = Vec<ValueChange<FileId, FileRecord>>;
type Outcome = Result<(Changes, bool), String>;

#[path = "ordered/prefixes.rs"]
mod prefixes;

std::thread_local! {
    static COMPARISONS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn record_comparison() {
    COMPARISONS.with(|count| {
        if let Some(previous) = count.get() {
            count.set(Some(previous + 1));
        }
    });
}

async fn put_page(store: &MemoryObjectStore, page: FileTablePage) -> ObjectId {
    let id = file_table_page_id(&page, 128).unwrap();
    let bytes = encode_file_table_page(
        &page,
        128,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )
    .unwrap();
    store
        .put(
            id,
            OwnedBytes::from(bytes),
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await
        .unwrap();
    id
}

async fn root(store: &MemoryObjectStore, keys: &[u8], width: usize, after: bool) -> ObjectId {
    if keys.is_empty() {
        return put_page(store, FileTablePage::Leaf(Vec::new())).await;
    }
    let mut children = Vec::new();
    for chunk in keys.chunks(width) {
        let records = chunk
            .iter()
            .map(|key| {
                let mut record = file_record(*key).unwrap();
                record.link_count = if after { 2 } else { 1 };
                record
            })
            .collect();
        children.push(Child {
            first: FileId::from_bytes([chunk[0]; 16]),
            page: put_page(store, FileTablePage::Leaf(records)).await,
        });
    }
    put_page(store, FileTablePage::Internal(children)).await
}

async fn run<const PREFIX: bool, const RESET: bool>(
    store: &MemoryObjectStore,
    roots: (ObjectId, ObjectId),
    maximum: u32,
    items: u64,
) -> (Outcome, WorkCounters) {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            items_examined: items,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let result = diff_with_accounting::<_, FileTableFormat, PREFIX, RESET>(
        store,
        Some(roots.0),
        Some(roots.1),
        maximum,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    )
    .await;
    let work = accounting.spent();
    (
        result
            .map(|(diff, _)| (diff.changes, diff.truncated))
            .map_err(|error| error.to_string()),
        work,
    )
}

async fn compare(
    before: &[u8],
    after: &[u8],
    widths: (usize, usize),
    maximum: u32,
    prefix_budget: usize,
) -> proptest::test_runner::TestCaseResult {
    let store = MemoryObjectStore::default();
    let roots = (
        root(&store, before, widths.0, false).await,
        root(&store, after, widths.1, true).await,
    );
    // The old-search mode uses the same real read/frontier/insertion/retirement
    // path. Only the known-prefix optimization is disabled.
    let (old, old_work) = run::<false, true>(&store, roots, maximum, u64::MAX).await;
    let (new, new_work) = run::<true, true>(&store, roots, maximum, u64::MAX).await;
    prop_assert_eq!(&new, &old);
    prop_assert!(new.is_ok());
    prop_assert!(new_work.items_examined <= old_work.items_examined);
    let mut old_other = old_work;
    let mut new_other = new_work;
    old_other.items_examined = 0;
    new_other.items_examined = 0;
    prop_assert_eq!(
        new_other,
        old_other,
        "all non-comparison receipts are unchanged"
    );
    // This exact budget admitted the baseline's complete traversal, including
    // reads and every old comparator. The candidate must still complete.
    let (limited, work) = run::<true, true>(&store, roots, maximum, old_work.items_examined).await;
    prop_assert_eq!(&limited, &old);
    prop_assert_eq!(work, new_work);
    let budget = prefix_budget as u64 % (old_work.items_examined + 1);
    let (old_limited, old_prefix) = run::<false, true>(&store, roots, maximum, budget).await;
    let (new_limited, new_prefix) = run::<true, true>(&store, roots, maximum, budget).await;
    prop_assert!(old_prefix.items_examined <= budget);
    prop_assert!(new_prefix.items_examined <= budget);
    if old_limited.is_ok() {
        prop_assert_eq!(&new_limited, &old_limited);
    }
    if new_limited.is_ok() {
        prop_assert_eq!(&new_limited, &old);
    }
    for (outcome, receipt) in [(old_limited, old_prefix), (new_limited, new_prefix)] {
        if let Err(error) = outcome {
            prop_assert!(error.contains("items_examined"), "{error}");
            prop_assert!(receipt.items_examined <= budget);
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn traversal_matches_old_receipts_and_budget_prefixes_across_pages_and_sides(
        before in prop::collection::btree_set(1_u8..=255, 0..65),
        after in prop::collection::btree_set(1_u8..=255, 0..65),
        left_width in 1_usize..9, right_width in 1_usize..9,
        maximum in 1_u32..129, budget in any::<usize>(),
    ) {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(compare(
            &before.into_iter().collect::<Vec<_>>(), &after.into_iter().collect::<Vec<_>>(),
            (left_width,right_width), maximum, budget))?;
    }
}

#[test]
fn sparse_overlap_keeps_the_baseline_budget_and_actual_reset_is_required() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let keys: Vec<_> = (1..=64).collect();
            compare(&keys, &[1, 64], (1, 2), 128, 277).await.unwrap();
            let store = MemoryObjectStore::default();
            let roots = (
                root(&store, &keys, 1, false).await,
                root(&store, &[1, 64], 2, true).await,
            );
            let (old, old_work) = run::<false, true>(&store, roots, 128, u64::MAX).await;
            let (valid, _) = run::<true, true>(&store, roots, 128, old_work.items_examined).await;
            let (broken, _) = run::<true, false>(&store, roots, 128, u64::MAX).await;
            assert_eq!(valid, old);
            assert_ne!(
                broken, old,
                "breaking push_page's reset must change the real traversal"
            );
        });
}

fn replay_pages<const PREFIX: bool, const RESET: bool>(
    pages: &[(bool, Vec<u8>)],
    budget: u64,
) -> (Outcome, WorkCounters, u64, Changes) {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            items_examined: budget,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let mut tasks = Owned::<Vec<Task>>::allocate(0, &mut accounting).unwrap();
    let mut values = Owned::<Changes>::allocate(0, &mut accounting).unwrap();
    let mut truncated = false;
    COMPARISONS.with(|count| count.set(Some(0)));
    let result = (|| {
        for (before, keys) in pages {
            let mut records = Owned::<Vec<FileRecord>>::allocate(keys.len(), &mut accounting)?;
            assert_eq!(records.value.capacity(), keys.len());
            records.value.extend(keys.iter().map(|key| {
                let mut record = file_record(*key).unwrap();
                record.link_count = if *before { 1 } else { 2 };
                record
            }));
            let bytes = records.bytes;
            let page = Owned::new(
                OwnedPage {
                    page: PageLease::Owned(Page::Leaf(records.value)),
                    logical_bytes: bytes,
                    #[cfg(not(target_arch = "wasm32"))]
                    native_custody: None,
                },
                records.allocation,
                bytes,
            );
            // This is the real page mover, including its reset and both owned
            // value transfers; the test never manages an insertion cursor.
            push_page::<FileTableFormat, PREFIX, RESET>(
                page,
                *before,
                &mut tasks,
                &mut values,
                128,
                &mut truncated,
                DecodeLimits::default(),
                &mut accounting,
            )?;
        }
        Ok::<_, FsError>(())
    })();
    let probes = COMPARISONS.with(|count| count.replace(None).unwrap());
    let work = accounting.spent();
    tasks.retire(&mut accounting).unwrap();
    let partial = values.value.clone();
    let outcome = match result {
        Ok(()) => Ok((values.value.clone(), truncated)),
        Err(error) => Err(error.to_string()),
    };
    values.retire(&mut accounting).unwrap();
    (outcome, work, probes, partial)
}

#[test]
fn actual_page_mover_preserves_all_277_comparison_budget_prefixes() {
    let pages: Vec<_> = (1..=64)
        .map(|key| (true, vec![key]))
        .chain([(false, vec![1, 64])])
        .collect();
    let (baseline, work, probes, _) = replay_pages::<false, true>(&pages, 277);
    assert!(baseline.is_ok());
    assert_eq!(work.items_examined, 277);
    assert_eq!(probes, 277);
    for budget in 0..=277 {
        let (old, old_work, old_calls, _) = replay_pages::<false, true>(&pages, budget);
        let (new, new_work, new_calls, _) = replay_pages::<true, true>(&pages, budget);
        assert_eq!(old_work.items_examined, old_calls);
        assert_eq!(new_work.items_examined, new_calls);
        assert!(old_calls <= budget && new_calls <= budget);
        if old.is_ok() {
            assert_eq!(new, baseline);
        }
        if new.is_err() {
            assert_eq!(new_calls, budget);
        }
    }
    let (broken, _, _, _) = replay_pages::<true, false>(&pages, u64::MAX);
    assert_ne!(
        broken, baseline,
        "the production reset is essential across sides"
    );
}
