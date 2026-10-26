use super::*;
use proptest::prelude::*;

proptest! {
    // Property: every path buffer is funded in all enclosing volume scopes;
    // destroying it refunds that same custody without poisoning any scope.
    #[test]
    fn namespace_copies_fund_nested_volume_custody(
        names in prop::collection::vec("[a-z]{1,24}", 1..8),
        depth in 1..4_usize,
        append in any::<bool>(),
    ) {
        let budget = crate::performance::work_budget(1_000_000, 1_000_000);
        let original = crate::NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let mut volume = original.clone();
        for _ in 0..depth { volume = volume.native_volume_work(budget).unwrap(); }
        let limits = crate::model::VolumeLimits::default();
        let names = names.into_iter().map(|name| LogicalName::new(
            NameEncoding::Utf8, name.into_bytes(), limits.maximum_component_bytes,
        ).unwrap()).collect();
        let path = NamespacePath::new(names, limits).unwrap();
        let baseline = original.live_allocation_bytes();
        let available = volume.remaining().unwrap().peak_allocation_bytes;
        let mut receipt = GenerationReceipt::from_work(&volume).unwrap();
        let copied = clone_namespace(&path, append.then(|| path.components().first().unwrap()), limits, &mut receipt).unwrap();
        let bytes = receipt.custody.bytes();
        prop_assert!(bytes > 0);
        prop_assert_eq!(original.live_allocation_bytes(), baseline + bytes);
        prop_assert_eq!(volume.remaining().unwrap().peak_allocation_bytes, available - bytes);
        drop((copied, receipt));
        prop_assert_eq!(original.live_allocation_bytes(), baseline);
        prop_assert_eq!(volume.remaining().unwrap().peak_allocation_bytes, available);
        // Admission must fail before creating an unfunded path in a child.
        let denied = volume.native_volume_work(crate::WorkBudget { peak_allocation_bytes: 0, ..budget }).unwrap();
        let mut receipt = GenerationReceipt::from_work(&denied).unwrap();
        prop_assert!(clone_namespace(&path, None, limits, &mut receipt).is_err());
        prop_assert_eq!(receipt.custody.bytes(), 0);
        prop_assert!(original.remaining().is_ok());
    }
}

#[test]
fn generation_index_finds_last_key_with_logarithmic_item_credit() {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .unwrap();
    let mut receipt = GenerationReceipt::from_work(&work).unwrap();
    let mut index = GenerationIndex::<u16, u16>::default();
    for key in (0..128).rev() {
        assert!(index.insert(key, key * 3, &mut receipt).unwrap());
    }
    for key in [127, 128] {
        let bounded = work
            .native_volume_work(WorkBudget {
                items_examined: 8,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
        let bounded_receipt = GenerationReceipt::from_work(&bounded).unwrap();
        let before = work.receipt().items_examined;
        assert_eq!(
            index.get(&key, &bounded_receipt).unwrap().copied(),
            (key < 128).then_some(key * 3)
        );
        assert!(work.receipt().items_examined - before <= 8);
    }
}

#[test]
fn generation_index_move_denial_preserves_entries_and_live_custody() {
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .unwrap();
    let baseline = work.live_allocation_bytes();
    let mut receipt = GenerationReceipt::from_work(&work).unwrap();
    let mut index = GenerationIndex::<u16, u16>::default();
    assert!(index.insert(2, 6, &mut receipt).unwrap());
    {
        let bounded = work
            .native_volume_work(WorkBudget {
                bytes_copied: 0,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
        let mut bounded_receipt = GenerationReceipt::from_work(&bounded).unwrap();
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        assert!(index.insert(1, 3, &mut bounded_receipt).is_err());
        assert_eq!(
            index
                .entries()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            [(2, 6)]
        );
        assert!(index.valid());
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations
        );
    }
    drop(index);
    drop(receipt);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

proptest! {
    #[test]
    fn generation_index_preserves_first_representatives_and_balanced_unique_keys(
        entries in prop::collection::vec((0..64_u16, any::<u16>()), 0..128),
    ) {
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
        let mut receipt = GenerationReceipt::from_work(&work).unwrap();
        let mut index = GenerationIndex::<u16, u16>::default();
        let mut expected = std::collections::BTreeMap::new();
        for (key, value) in entries {
            let vacant = !expected.contains_key(&key);
            expected.entry(key).or_insert(value);
            prop_assert_eq!(index.insert(key, value, &mut receipt).unwrap(), vacant);
            prop_assert_eq!(index.get(&key, &receipt).unwrap().copied(), expected.get(&key).copied());
            prop_assert!(index.valid());
        }
        let mut actual=index.entries().map(|(key,value)| (*key,*value)).collect::<Vec<_>>();
        actual.sort_unstable_by_key(|entry| entry.0);
        prop_assert_eq!(actual, expected.into_iter().collect::<Vec<_>>());
    }
}

#[test]
fn generation_index_insertion_copy_cost_is_bounded_in_both_orders() {
    for reverse in [false, true] {
        // At 2,048 entries a sorted-vector index needs more than8MiB just to
        // shift u16 pairs. A balanced append-only arena stays below4MiB,
        // including every arena growth and every link/height update.
        let work = crate::NativeMaterializationWork::new(
            WorkBudget {
                bytes_copied: 4 * 1024 * 1024,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .unwrap();
        let baseline = work.live_allocation_bytes();
        let mut receipt = GenerationReceipt::from_work(&work).unwrap();
        let mut index = GenerationIndex::<u16, u16>::default();
        for step in 0..2048_u16 {
            let key = if reverse { 2047 - step } else { step };
            assert!(index.insert(key, key, &mut receipt).unwrap());
            assert!(index.valid());
        }
        let copied = work.receipt().bytes_copied;
        assert!(copied < 4 * 1024 * 1024);
        for key in 0..2048_u16 {
            assert_eq!(index.get(&key, &receipt).unwrap(), Some(&key));
        }
        // Duplicate admission preserves the first representative and performs
        // no insertion copies or capacity change.
        assert!(!index.insert(1024, 0, &mut receipt).unwrap());
        assert_eq!(work.receipt().bytes_copied, copied);
        drop(index);
        drop(receipt);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
}
