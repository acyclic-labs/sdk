use super::*;
use crate::{CancellationToken, WorkBudget};
use proptest::prelude::*;
use std::collections::BTreeMap;

fn audit<V, const N: usize>(table: &FixedIndex<V, N>) {
    let mut pending = vec![(table.root.as_deref(), None)];
    let mut leaves = 0_usize;
    let mut branches = 0_usize;
    while let Some((node, preceding)) = pending.pop() {
        match node.map(|node| &node.kind) {
            Some(Kind::Branch { bit, low, high }) => {
                assert!(
                    bit.byte < N && bit.offset < 8 && preceding.is_none_or(|prior| prior < *bit)
                );
                assert!(low.is_some() && high.is_some());
                pending.push((low.as_deref(), Some(*bit)));
                pending.push((high.as_deref(), Some(*bit)));
                branches += 1;
            }
            Some(Kind::Leaf { .. }) => leaves += 1,
            None => {}
        }
    }
    assert_eq!(branches, leaves.saturating_sub(1));
}

#[test]
fn exact_retirement_preserves_every_sibling_and_can_finish_after_cancellation()
-> Result<(), FsError> {
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let mut table = FixedIndex::default();
    for key in 0..=u8::MAX {
        table.get_or_try_insert_with([key], &mut accounting, |_| Ok(key))?;
    }
    let mut remaining: BTreeMap<_, _> = (0..=u8::MAX).map(|key| (key, key)).collect();
    for ordinal in 0..=u8::MAX {
        let key = ordinal.reverse_bits();
        assert!(!table.retire_if([key], |_| false));
        assert!(table.retire_if([key], |value| *value == key));
        assert!(!table.retire_if([key], |_| true));
        remaining.remove(&key);
        audit(&table);
        for remaining in remaining.keys() {
            assert_eq!(table.get([*remaining], &mut accounting)?, Some(remaining));
        }
    }
    assert!(table.root.is_none());
    table.get_or_try_insert_with([7], &mut accounting, |_| Ok(9))?;
    cancel.cancel();
    assert!(table.retire_if([7], |value| *value == 9));
    assert!(table.root.is_none());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]
    #[test]
    fn fixed_index_matches_the_map_for_every_generated_history(keys in prop::collection::vec(any::<u128>(), 0..96)) {
        let cancel = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let mut table = FixedIndex::default();
        let mut expected = BTreeMap::new();
        for (index, key) in keys.into_iter().enumerate() {
            let observed = table.get_or_try_insert_with(key.to_be_bytes(), &mut accounting, |_| Ok(index))?;
            prop_assert_eq!(*observed, *expected.entry(key).or_insert(index));
            audit(&table);
            for (key, value) in &expected {
                let before = accounting.spent();
                let observed = table.get_or_try_insert_with(key.to_be_bytes(), &mut accounting, |_| {
                    unreachable!("an existing key never runs its constructor")
                })?;
                prop_assert_eq!(*observed, *value);
                prop_assert!(accounting.spent().items_examined - before.items_examined <= 290);
                prop_assert_eq!(accounting.spent().allocation_operations, before.allocation_operations);
            }
        }
    }
}

#[test]
fn every_key_bit_can_be_a_branch_without_unbounded_lookup_or_drop() -> Result<(), FsError> {
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let mut table = FixedIndex::default();
    table.get_or_try_insert_with(0_u128.to_be_bytes(), &mut accounting, |_| Ok(0))?;
    for bit in 0..128 {
        table.get_or_try_insert_with((1_u128 << bit).to_be_bytes(), &mut accounting, |_| {
            Ok(bit + 1)
        })?;
        audit(&table);
    }
    let before = accounting.spent().items_examined;
    assert_eq!(
        *table.get_or_try_insert_with(0_u128.to_be_bytes(), &mut accounting, |_| Ok(999))?,
        0
    );
    assert_eq!(accounting.spent().items_examined - before, 290);
    drop(table);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn failed_node_admission_and_construction_leave_the_index_and_funding_intact()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let original_control = original.live_allocation_bytes();
    let mut table = FixedIndex::default();
    table.get_or_try_insert_with(
        [0; 16],
        &mut KernelAccounting::native(&original, &cancel),
        |_| Ok(7),
    )?;
    let baseline = original.live_allocation_bytes();
    for admitted in 0..=2 {
        let budget = WorkBudget {
            allocation_operations: admitted + 1,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let attempt =
            crate::NativeMaterializationWork::new(budget, &cancel).map_err(|(error, _)| error)?;
        let mut called = false;
        let result = table.get_or_try_insert_with(
            [255; 16],
            &mut KernelAccounting::native(&attempt, &cancel),
            |_| {
                called = true;
                Err(FsError::InvalidDiff)
            },
        );
        assert!(result.is_err());
        assert_eq!(
            called,
            admitted == 2,
            "constructor ran before both nodes were funded"
        );
        assert_eq!(
            attempt.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
        audit(&table);
        assert_eq!(
            *table.get_or_try_insert_with(
                [0; 16],
                &mut KernelAccounting::native(&original, &cancel),
                |_| Ok(999)
            )?,
            7
        );
    }
    drop(table);
    assert_eq!(original.live_allocation_bytes(), original_control);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn fixed_index_value_retires_before_its_original_node_funding()
-> Result<(), Box<dyn std::error::Error>> {
    struct Observe<'a> {
        original: &'a crate::NativeMaterializationWork,
        observed: &'a std::cell::Cell<bool>,
        minimum: u64,
    }
    impl Drop for Observe<'_> {
        fn drop(&mut self) {
            self.observed
                .set(self.original.live_allocation_bytes() >= self.minimum);
        }
    }
    let cancel = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
            .map_err(|(error, _)| error)?;
    let observed = std::cell::Cell::new(false);
    let original_control = original.live_allocation_bytes();
    let minimum = original_control + std::mem::size_of::<Node<Observe, 16>>() as u64;
    let mut table = FixedIndex::default();
    table.get_or_try_insert_with(
        [0; 16],
        &mut KernelAccounting::native(&original, &cancel),
        |_| {
            Ok(Observe {
                original: &original,
                observed: &observed,
                minimum,
            })
        },
    )?;
    drop(table);
    assert!(observed.get(), "node funding retired before its value");
    assert_eq!(original.live_allocation_bytes(), original_control);
    Ok(())
}

fn every_bit<const N: usize>() -> Result<(), FsError> {
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let mut index = FixedIndex::default();
    index.get_or_try_insert_with([0; N], &mut accounting, |_| Ok(0))?;
    let node_bytes = std::mem::size_of::<Node<usize, N>>() as u64;
    let baseline = accounting
        .standalone_parts()
        .ok_or(FsError::InvalidDiff)?
        .0
        .live_bytes()
        - node_bytes;
    for byte in 0..N {
        for offset in 0..8 {
            let mut key = [0; N];
            *key.get_mut(byte).ok_or(FsError::InvalidDiff)? = 0x80 >> offset;
            let expected = byte * 8 + offset + 1;
            index.get_or_try_insert_with(key, &mut accounting, |_| Ok(expected))?;
            assert_eq!(index.get(key, &mut accounting)?, Some(&expected));
            audit(&index);
        }
    }
    let before = accounting.spent();
    assert_eq!(index.get([0; N], &mut accounting)?, Some(&0));
    let after = accounting.spent();
    assert_eq!(
        after.items_examined - before.items_examined,
        (N * 9 + 1) as u64
    );
    assert_eq!(before.allocation_operations, after.allocation_operations);
    drop(index);
    let (allocations, _, _, _) = accounting.standalone_parts().ok_or(FsError::InvalidDiff)?;
    assert_eq!(allocations.live_bytes(), baseline);
    Ok(())
}

#[test]
fn fixed_index_generation_and_exact_grant_keys_keep_every_identity_bit() -> Result<(), FsError> {
    every_bit::<32>()?;
    every_bit::<40>()
}

#[test]
fn fixed_index_empty_and_missing_lookup_never_allocates_or_constructs() -> Result<(), FsError> {
    let cancel = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let mut index = FixedIndex::default();
    let before = accounting.spent();
    assert_eq!(index.get([0; 40], &mut accounting)?, None);
    assert_eq!(accounting.spent(), before);
    index.get_or_try_insert_with([255; 40], &mut accounting, |_| Ok(7))?;
    let before = accounting.spent();
    assert_eq!(index.get([0; 40], &mut accounting)?, None);
    assert_eq!(index.get([255; 40], &mut accounting)?, Some(&7));
    assert_eq!(
        accounting.spent().allocation_operations,
        before.allocation_operations
    );
    audit(&index);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]
    #[test]
    fn fixed_index_wide_histories_preserve_all_values_and_retire_original_credit(
        keys in prop::collection::vec(prop::array::uniform::<_, 40>(any::<u8>()), 0..96)
    ) {
        struct Observed<'a> { value: usize, drops: &'a std::cell::Cell<usize> }
        impl Drop for Observed<'_> {
            fn drop(&mut self) { self.drops.set(self.drops.get() + 1); }
        }
        let cancel = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let drops = std::cell::Cell::new(0);
        let mut index = FixedIndex::default();
        let mut expected = BTreeMap::new();
        for (value, key) in keys.into_iter().enumerate() {
            let entry = index.get_or_try_insert_with(key, &mut accounting, |_| Ok(Observed { value, drops: &drops }))?;
            prop_assert_eq!(entry.value, *expected.entry(key).or_insert(value));
            audit(&index);
            for (key, value) in &expected {
                let before = accounting.spent();
                prop_assert_eq!(index.get(*key, &mut accounting)?.map(|entry| entry.value), Some(*value));
                prop_assert!(accounting.spent().items_examined - before.items_examined <= 361);
                prop_assert_eq!(accounting.spent().allocation_operations, before.allocation_operations);
            }
        }
        prop_assert_eq!(drops.get(), 0);
        let node_bytes = std::mem::size_of::<Node<Observed, 40>>() as u64;
        let nodes = expected.len().saturating_mul(2).saturating_sub(1) as u64;
        let before_drop = accounting.standalone_parts().ok_or(FsError::InvalidDiff)?.0.live_bytes();
        drop(index);
        prop_assert_eq!(drops.get(), expected.len());
        let (allocations, _, _, _) = accounting.standalone_parts().ok_or(FsError::InvalidDiff)?;
        prop_assert_eq!(allocations.live_bytes(), before_drop - nodes * node_bytes);
    }
}

#[test]
fn fixed_index_lookup_admits_comparison_before_returning_a_value() -> Result<(), FsError> {
    let cancel = CancellationToken::new();
    let mut owner = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
    let mut index = FixedIndex::default();
    index.get_or_try_insert_with([255; 40], &mut owner, |_| Ok(7))?;
    for items in 0..=41 {
        let mut reader = KernelAccounting::standalone(
            WorkBudget {
                items_examined: items,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &cancel,
        );
        let observed = index.get([255; 40], &mut reader);
        if items < 41 {
            assert!(observed.is_err(), "comparison escaped admission at {items}");
        } else {
            assert_eq!(observed?, Some(&7));
        }
        assert_eq!(reader.spent().allocation_operations, 0);
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut reader = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancelled);
    let empty = FixedIndex::<u64, 40>::default();
    assert!(empty.get([0; 40], &mut reader).is_err());
    assert_eq!(reader.spent(), crate::WorkCounters::UNCHARGED);
    Ok(())
}
