use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

fn audit<V, A, const N: usize>(table: &FixedIndex<V, A, N>) {
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
use std::{cell::Cell, rc::Rc};

#[derive(Default)]
struct Work {
    items: u64,
    claims: u64,
    maximum_items: Option<u64>,
    maximum_claims: Option<u64>,
    cancelled: bool,
    live: Rc<Cell<u64>>,
}

struct Allocation {
    bytes: u64,
    live: Rc<Cell<u64>>,
}

impl Drop for Allocation {
    fn drop(&mut self) {
        self.live
            .set(self.live.get().checked_sub(self.bytes).unwrap());
    }
}

impl IndexWork for Work {
    type Error = &'static str;
    type Allocation = Allocation;
    fn check(&mut self) -> Result<(), Self::Error> {
        if self.cancelled {
            Err("cancelled")
        } else {
            Ok(())
        }
    }
    fn charge_items(&mut self, items: u64) -> Result<(), Self::Error> {
        self.check()?;
        let next = self.items.checked_add(items).ok_or("items overflow")?;
        if self.maximum_items.is_some_and(|maximum| next > maximum) {
            return Err("items refused");
        }
        self.items = next;
        Ok(())
    }
    fn claim(&mut self, bytes: u64) -> Result<Allocation, Self::Error> {
        self.check()?;
        let next = self.claims.checked_add(1).ok_or("claims overflow")?;
        if self.maximum_claims.is_some_and(|maximum| next > maximum) {
            return Err("allocation refused");
        }
        let live = self.live.get().checked_add(bytes).ok_or("live overflow")?;
        self.claims = next;
        self.live.set(live);
        Ok(Allocation {
            bytes,
            live: Rc::clone(&self.live),
        })
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]
    #[test]
    fn ordered_histories_preserve_values_branch_bounds_and_original_custody(
        keys in prop::collection::vec(prop::array::uniform::<_, 40>(any::<u8>()), 0..96)
    ) {
        let mut work = Work::default();
        let mut table = FixedIndex::default();
        let mut expected = BTreeMap::new();
        for (value, key) in keys.into_iter().enumerate() {
            let observed = table.get_or_try_insert_with(key, &mut work, |_| Ok(value)).map_err(TestCaseError::fail)?;
            prop_assert_eq!(*observed, *expected.entry(key).or_insert(value));
            audit(&table);
            prop_assert_eq!(work.claims, (expected.len() * 2 - 1) as u64);
            prop_assert_eq!(work.live.get(), work.claims * std::mem::size_of::<Node<usize, Allocation, 40>>() as u64);
            let claims = work.claims;
            for (key, value) in &expected {
                let before = work.items;
                prop_assert_eq!(table.get(*key, &mut work).map_err(TestCaseError::fail)?, Some(value));
                prop_assert!(work.items - before <= 361);
            }
            prop_assert_eq!(work.claims, claims);
            prop_assert_eq!(table.first(&mut work).map_err(TestCaseError::fail)?, expected.first_key_value());
        }
        for key in expected.keys().rev() {
            prop_assert!(!table.retire_if(*key, |_| false));
            prop_assert!(table.retire_if(*key, |_| true));
            prop_assert!(!table.retire_if(*key, |_| true));
            audit(&table);
        }
        prop_assert_eq!(work.live.get(), 0);
    }
}

fn every_bit<const N: usize>() -> Result<(), &'static str> {
    let mut work = Work::default();
    let mut table = FixedIndex::default();
    table.get_or_try_insert_with([0; N], &mut work, |_| Ok(0))?;
    for byte in 0..N {
        for offset in 0..8 {
            let mut key = [0; N];
            key[byte] = 0x80 >> offset;
            table.get_or_try_insert_with(key, &mut work, |_| Ok(byte * 8 + offset + 1))?;
            audit(&table);
        }
    }
    let before = work.items;
    assert_eq!(table.get([0; N], &mut work)?, Some(&0));
    assert_eq!(work.items - before, (N * 9 + 1) as u64);
    let before = work.items;
    assert_eq!(table.first(&mut work)?, Some((&[0; N], &0)));
    assert_eq!(work.items - before, (N * 8 + 1) as u64);
    work.cancelled = true;
    assert!(table.get([0; N], &mut work).is_err());
    assert!(table.first(&mut work).is_err());
    assert!(table.retire_if([0; N], |value| *value == 0));
    audit(&table);
    drop(table);
    assert_eq!(work.live.get(), 0);
    Ok(())
}

#[test]
fn adversarial_branches_have_a_fixed_key_width_bound_and_cancel_safe_retirement()
-> Result<(), &'static str> {
    every_bit::<0>()?;
    every_bit::<8>()?;
    every_bit::<24>()?;
    every_bit::<40>()
}

#[test]
fn refused_admission_never_constructs_or_publishes_and_preserves_prior_funding()
-> Result<(), &'static str> {
    let mut work = Work::default();
    let mut table = FixedIndex::default();
    table.get_or_try_insert_with([0; 8], &mut work, |_| Ok(7))?;
    let baseline = work.live.get();
    for claims in 0..=2 {
        work.maximum_claims = Some(work.claims + claims);
        let called = Cell::new(false);
        let result = table.get_or_try_insert_with([255; 8], &mut work, |_| {
            called.set(true);
            Err("construction refused")
        });
        assert!(result.is_err());
        assert_eq!(called.get(), claims == 2);
        assert_eq!(work.live.get(), baseline);
        assert_eq!(table.get([0; 8], &mut work)?, Some(&7));
        assert_eq!(table.get([255; 8], &mut work)?, None);
        audit(&table);
    }
    for credit in 0..9 {
        work.maximum_items = Some(work.items + credit);
        assert!(table.get([0; 8], &mut work).is_err());
    }
    drop(table);
    assert_eq!(work.live.get(), 0);
    Ok(())
}

#[test]
fn a_full_scan_negative_exposes_population_work_hidden_by_the_fixed_key_index()
-> Result<(), &'static str> {
    let mut work = Work::default();
    let mut table = FixedIndex::default();
    let mut scan = BTreeMap::new();
    for value in 0_u64..4096 {
        table.get_or_try_insert_with(value.to_be_bytes(), &mut work, |_| Ok(value))?;
        scan.insert(value.to_be_bytes(), value);
    }
    let key = 4095_u64.to_be_bytes();
    let before = work.items;
    let indexed = table.get(key, &mut work)?;
    let index_work = work.items - before;
    let mut scan_work = 0;
    let scanned = scan
        .iter()
        .inspect(|_| scan_work += 1)
        .find(|(observed, _)| **observed == key)
        .map(|(_, value)| value);
    assert_eq!(indexed, scanned);
    assert!(index_work <= 73);
    assert_eq!(scan_work, 4096);
    assert!(scan_work > index_work);
    let before = work.items;
    let after = 4094_u64.to_be_bytes();
    let indexed = table.seek_after(after, &mut work)?;
    let seek_work = work.items - before;
    let mut scan_work = 0;
    let scanned = scan
        .iter()
        .inspect(|_| scan_work += 1)
        .find(|(key, _)| **key > after);
    assert_eq!(indexed, scanned);
    assert!(seek_work <= 25 * 8 + 3);
    assert_eq!(scan_work, 4096);
    assert!(scan_work > seek_work);
    Ok(())
}

#[test]
fn extracted_values_keep_their_original_leaf_funding_after_cancelled_removal()
-> Result<(), &'static str> {
    struct Value {
        live: Rc<Cell<u64>>,
        minimum: Rc<Cell<u64>>,
        dropped: Rc<Cell<bool>>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            assert!(self.live.get() >= self.minimum.get());
            self.dropped.set(true);
        }
    }
    let mut work = Work::default();
    let minimum = Rc::new(Cell::new(1));
    let dropped = Rc::new(Cell::new(false));
    let mut table = FixedIndex::default();
    let live = Rc::clone(&work.live);
    table.get_or_try_insert_with([7; 8], &mut work, |_| {
        Ok(Value {
            live,
            minimum: Rc::clone(&minimum),
            dropped: Rc::clone(&dropped),
        })
    })?;
    minimum.set(work.live.get());
    let items = work.items;
    let claims = work.claims;
    work.cancelled = true;
    work.maximum_claims = Some(0);
    assert!(table.remove_if([7; 8], |_| false).is_none());
    let value = table
        .remove_if([7; 8], |_| true)
        .ok_or("original owner missing")?;
    assert_eq!(value.key(), &[7; 8]);
    assert!(!value.value().dropped.get());
    assert_eq!(work.live.get(), minimum.get());
    assert_eq!(work.items, items);
    assert_eq!(work.claims, claims);
    drop(table);
    assert_eq!(work.live.get(), minimum.get());
    drop(value);
    assert!(dropped.get());
    assert_eq!(work.live.get(), 0);
    Ok(())
}

#[test]
fn ordered_seek_and_mutation_preserve_original_credit_and_sparse_cursors()
-> Result<(), &'static str> {
    use proptest::test_runner::{Config, TestRunner};
    use std::ops::Bound::{Excluded, Unbounded};

    let mut runner = TestRunner::new(Config {
        cases: 128,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(
            &(
                prop::collection::vec(prop::array::uniform::<_, 40>(any::<u8>()), 0..96),
                prop::collection::vec(prop::array::uniform::<_, 40>(any::<u8>()), 0..32),
            ),
            |(keys, queries)| {
                let mut work = Work::default();
                let mut table = FixedIndex::default();
                let mut expected = BTreeMap::new();
                for (value, key) in keys.into_iter().enumerate() {
                    table
                        .get_or_try_insert_with(key, &mut work, |_| Ok(value))
                        .map_err(TestCaseError::fail)?;
                    expected.entry(key).or_insert(value);
                }
                let claims = work.claims;
                let live = work.live.get();
                let existing: Vec<_> = expected.keys().copied().collect();
                for key in existing.into_iter().chain(queries) {
                    let before = work.items;
                    let observed = table
                        .seek_after(key, &mut work)
                        .map_err(TestCaseError::fail)?;
                    prop_assert_eq!(observed, expected.range((Excluded(key), Unbounded)).next());
                    prop_assert!(work.items - before <= 25 * 40 + 3);
                    let before = work.items;
                    let observed = table.get_mut(key, &mut work).map_err(TestCaseError::fail)?;
                    prop_assert_eq!(observed.as_deref(), expected.get(&key));
                    if let Some(value) = observed {
                        *value += 1;
                        *expected.get_mut(&key).unwrap() += 1;
                    }
                    prop_assert!(work.items - before <= 9 * 40 + 1);
                }
                let mut row = table.first(&mut work).map_err(TestCaseError::fail)?;
                for expected in &expected {
                    prop_assert_eq!(row, Some(expected));
                    row = table
                        .seek_after(*expected.0, &mut work)
                        .map_err(TestCaseError::fail)?;
                }
                prop_assert_eq!(row, None);
                prop_assert_eq!(work.claims, claims);
                prop_assert_eq!(work.live.get(), live);
                drop(table);
                prop_assert_eq!(work.live.get(), 0);
                Ok(())
            },
        )
        .map_err(|_| "ordered seek or existing mutation disagreed with independent map")?;

    // Every possible branch coordinate, including beyond 256 bits, and the
    // empty key exercise the structural bound independently of random shape.
    fn boundary<const N: usize>() -> Result<(), &'static str> {
        let mut work = Work::default();
        let mut table = FixedIndex::default();
        let mut expected = BTreeMap::new();
        table.get_or_try_insert_with([0; N], &mut work, |_| Ok(0))?;
        expected.insert([0; N], 0);
        for byte in 0..N {
            for offset in 0..8 {
                let mut key = [0; N];
                key[byte] = 0x80 >> offset;
                table.get_or_try_insert_with(key, &mut work, |_| Ok(1))?;
                expected.insert(key, 1);
            }
        }
        let claims = work.claims;
        for key in expected.keys().copied().chain([[255; N]]) {
            let before = work.items;
            assert_eq!(
                table.seek_after(key, &mut work)?,
                expected.range((Excluded(key), Unbounded)).next()
            );
            assert!(work.items - before <= (25 * N + 3) as u64);
        }
        let before = work.items;
        table
            .get_mut([0; N], &mut work)?
            .ok_or("missing original key")?;
        let required = work.items - before;
        for credit in 0..required {
            work.maximum_items = Some(work.items + credit);
            assert_eq!(
                table.get_mut([0; N], &mut work).err(),
                Some("items refused")
            );
        }
        work.maximum_items = None;
        assert_eq!(table.get([0; N], &mut work)?, Some(&0));
        let before = work.items;
        table.seek_after([0; N], &mut work)?;
        let required = work.items - before;
        for credit in 0..required {
            work.maximum_items = Some(work.items + credit);
            assert_eq!(
                table.seek_after([0; N], &mut work).err(),
                Some("items refused")
            );
        }
        work.maximum_items = None;
        work.cancelled = true;
        assert_eq!(table.get_mut([255; N], &mut work).err(), Some("cancelled"));
        assert_eq!(
            table.seek_after([255; N], &mut work).err(),
            Some("cancelled")
        );
        let mut empty = FixedIndex::<usize, Allocation, N>::default();
        assert_eq!(empty.get_mut([0; N], &mut work).err(), Some("cancelled"));
        assert_eq!(empty.seek_after([0; N], &mut work).err(), Some("cancelled"));
        assert!(
            empty
                .get_or_try_insert_with([0; N], &mut work, |_| panic!("cancelled construction"))
                .is_err()
        );
        assert_eq!(work.claims, claims);
        drop(table);
        assert_eq!(work.live.get(), 0);
        Ok(())
    }
    boundary::<0>()?;
    boundary::<8>()?;
    boundary::<24>()?;
    boundary::<40>()?;
    a_full_scan_negative_exposes_population_work_hidden_by_the_fixed_key_index()
}
