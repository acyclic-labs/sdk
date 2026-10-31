use super::*;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

async fn root(
    store: &MemoryObjectStore,
    records: &BTreeMap<u8, u64>,
    width: usize,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let values = records
        .iter()
        .map(|(key, links)| {
            let mut record = file_record(*key)?;
            record.link_count = *links;
            Ok(record)
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    if values.len() <= width {
        return put_file_table(store, &FileTablePage::Leaf(values), 128).await;
    }
    let mut children = Vec::new();
    for values in values.chunks(width) {
        children.push(Child {
            first: values[0].file_id,
            page: put_file_table(store, &FileTablePage::Leaf(values.to_vec()), 128).await?,
        });
    }
    put_file_table(store, &FileTablePage::Internal(children), 128).await
}

fn expected(
    before: &BTreeMap<u8, u64>,
    after: &BTreeMap<u8, u64>,
) -> Vec<ValueChange<FileId, FileRecord>> {
    let record = |key, links| {
        let mut record = file_record(key).unwrap();
        record.link_count = links;
        record
    };
    before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| before.get(key) != after.get(key))
        .map(|key| ValueChange {
            key: FileId::from_bytes([key; 16]),
            before: before.get(&key).map(|links| record(key, *links)),
            after: after.get(&key).map(|links| record(key, *links)),
        })
        .collect()
}

async fn compare(
    before: &BTreeMap<u8, u64>,
    after: &BTreeMap<u8, u64>,
    widths: (usize, usize),
    maximum: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let roots = (
        root(&store, before, widths.0).await?,
        root(&store, after, widths.1).await?,
    );
    let cancellation = CancellationToken::new();
    let expected = expected(before, after);
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut cursor = Cursor::<FileTableFormat>::new(
        Some(roots.0),
        Some(roots.1),
        DecodeLimits::default(),
        &mut accounting,
    )?;
    // One row per yield exercises every retained leaf/parent position. An
    // independent ordered map decides both value sides, including equal rows.
    let mut observed = Vec::new();
    while let Some(change) = cursor.next(&store, &mut accounting, &cancellation).await? {
        observed.push(change.value.clone());
        change.retire(&mut accounting)?;
    }
    cursor.retire(&mut accounting)?;
    assert_eq!(observed, expected);
    let paged = accounting.spent();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let (diff, allocation) = diff_with_accounting::<_, FileTableFormat>(
        &store,
        Some(roots.0),
        Some(roots.1),
        maximum,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    )
    .await?;
    let count = (maximum as usize).min(expected.len());
    assert_eq!(diff.changes, expected[..count]);
    assert_eq!(diff.truncated, expected.len() > count);
    if expected.len() <= count {
        assert_eq!(
            diff.work.page_reads, paged.page_reads,
            "collecting must use the same retained frontier"
        );
        assert_eq!(diff.work.items_examined, paged.items_examined);
    }
    let bytes = diff.retained_allocation_bytes;
    drop(diff);
    retire_allocation(allocation, bytes, &mut accounting)?;
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn retained_pair_frontier_matches_ordered_maps_across_independent_page_shapes(
        before in prop::collection::btree_map(1_u8..=127, 1_u64..4, 0..65),
        after in prop::collection::btree_map(1_u8..=127, 1_u64..4, 0..65),
        left_width in 1_usize..9, right_width in 1_usize..9,
        maximum in 1_u32..129,
    ) {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(compare(
            &before, &after, (left_width, right_width), maximum,
        )).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
async fn retained_pair_frontier_keeps_early_output_and_cancellation_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let before: BTreeMap<_, _> = (1..=64).map(|key| (key, 1)).collect();
    let after: BTreeMap<_, _> = [(1, 2), (64, 2)].into_iter().collect();
    compare(&before, &after, (1, 2), 128).await?;
    let store = MemoryObjectStore::default();
    let roots = (
        root(&store, &before, 1).await?,
        root(&store, &after, 2).await?,
    );
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut cursor = Cursor::<FileTableFormat>::new(
        Some(roots.0),
        Some(roots.1),
        DecodeLimits::default(),
        &mut accounting,
    )?;
    let first = cursor
        .next(&store, &mut accounting, &cancellation)
        .await?
        .ok_or("missing early change")?;
    assert_eq!(first.value, expected(&before, &after)[0]);
    assert!(
        accounting.spent().page_reads < 10,
        "one row exported the remaining tree"
    );
    let reads = accounting.spent().page_reads;
    cancellation.cancel();
    assert!(matches!(
        cursor.next(&store, &mut accounting, &cancellation).await,
        Err(FsError::Cancelled(_))
    ));
    assert_eq!(accounting.spent().page_reads, reads);
    // Refusal retains both the delivered value and the held frontier until
    // their original physical retirement, even after caller cancellation.
    first.retire(&mut accounting)?;
    cursor.retire(&mut accounting)?;
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);

    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            items_examined: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let mut cursor = Cursor::<FileTableFormat>::new(
        Some(roots.0),
        Some(roots.1),
        DecodeLimits::default(),
        &mut accounting,
    )?;
    let failure = cursor.next(&store, &mut accounting, &cancellation).await;
    assert!(matches!(
        failure,
        Err(FsError::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            ..
        }))
    ));
    cursor.retire(&mut accounting)?;
    assert!(accounting.spent().items_examined <= 1);
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
    Ok(())
}
