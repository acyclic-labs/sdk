use super::*;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;
use std::collections::BTreeMap;

mod compiled_prefix;
mod keyed;
mod ordered;
mod persistent;
mod prefix;
mod preparation;

fn audit<V, A, const N: usize>(table: &FixedIndex<V, A, N>) {
    let mut pending = vec![(table.root.as_ref(), None)];
    let mut leaves = 0_usize;
    let mut branches = 0_usize;
    while let Some((node, preceding)) = pending.pop() {
        match node.map(|node| node.kind.as_ref()) {
            Some(Kind::Branch { bit, low, high }) => {
                assert!(
                    bit.byte < N && bit.offset < 8 && preceding.is_none_or(|prior| prior < *bit)
                );
                assert!(low.is_some() && high.is_some());
                pending.push((low.as_ref(), Some(*bit)));
                pending.push((high.as_ref(), Some(*bit)));
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
    claimed_bytes: u64,
    maximum_items: Option<u64>,
    maximum_claims: Option<u64>,
    cancelled: bool,
    live: Rc<Cell<u64>>,
    observe_retirement: bool,
    early_refund: Rc<Cell<bool>>,
}

impl Work {
    fn live_bytes(&self) -> i64 {
        i64::try_from(self.live.get()).expect("live backing fits the physical counter")
    }
}

thread_local! {
    static ORDERED_COMPARISONS: Cell<u64> = const { Cell::new(0) };
}

// The observer belongs to this test thread, never to an ordered map's key.
#[derive(Clone, Copy, PartialEq, Eq)]
struct CountedKey(u64);

impl PartialOrd for CountedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CountedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        ORDERED_COMPARISONS.set(ORDERED_COMPARISONS.get() + 1);
        self.0.cmp(&other.0)
    }
}

fn ordered_comparisons(operation: impl FnOnce()) -> u64 {
    ORDERED_COMPARISONS.set(0);
    operation();
    ORDERED_COMPARISONS.get()
}

struct Allocation {
    bytes: u64,
    live: Rc<Cell<u64>>,
    observe_retirement: bool,
    early_refund: Rc<Cell<bool>>,
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let remaining = self.live.get().checked_sub(self.bytes).unwrap();
        if self.observe_retirement {
            let physical = acyclic_test_support::current_allocations()
                .expect("retirement is inside its original measurement")
                .bytes_current;
            if physical
                > i64::try_from(remaining).expect("remaining backing fits the physical counter")
            {
                self.early_refund.set(true);
            }
        }
        self.live.set(remaining);
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
        self.claimed_bytes += bytes;
        self.live.set(live);
        Ok(Allocation {
            bytes,
            live: Rc::clone(&self.live),
            observe_retirement: self.observe_retirement,
            early_refund: Rc::clone(&self.early_refund),
        })
    }
}

// Reads borrow the original admission without an allocation capability.
struct ReadOnly<'a>(&'a mut Work);

impl IndexReadWork for ReadOnly<'_> {
    type Error = &'static str;

    fn validate(&mut self) -> Result<(), Self::Error> {
        self.0.check()
    }

    fn examine(&mut self, items: u64) -> Result<(), Self::Error> {
        self.0.charge_items(items)
    }
}

#[test]
fn original_credit_covers_physical_backing_through_extraction_and_retirement() {
    for population in [1_u64, 4, 32, 256] {
        let mut work = Work {
            observe_retirement: true,
            ..Work::default()
        };
        let measured = acyclic_test_support::measure(|| {
            let mut table = FixedIndex::default();
            for value in 0..population {
                table
                    .get_or_try_insert_with(value.to_be_bytes(), &mut work, |_| Ok(value))
                    .expect("original node admission succeeds");
            }
            assert_eq!(
                acyclic_test_support::current_allocations()
                    .expect("original measurement")
                    .bytes_current,
                work.live_bytes()
            );
            work.cancelled = true;
            for value in (0..population).step_by(2) {
                let entry = table
                    .remove_if(value.to_be_bytes(), |observed| *observed == value)
                    .expect("original leaf remains removable after cancellation");
                assert_eq!(*entry.value(), value);
                drop(entry);
            }
            drop(table);
        });
        assert_eq!(measured.count_total, population * 2 - 1);
        assert_eq!(measured.count_current, 0);
        assert_eq!(measured.bytes_current, 0);
        assert_eq!(work.live.get(), 0);
        assert!(
            !work.early_refund.get(),
            "physical node backing outlived its original credit"
        );
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
            prop_assert_eq!(work.live.get(), work.claims * std::mem::size_of::<Kind<usize, Allocation, 40>>() as u64);
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
            *key.get_mut(byte)
                .ok_or("branch byte exceeds original key")? = 0x80 >> offset;
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
