use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

#[test]
fn persistent_histories_preserve_prefixes_bounded_paths_and_original_custody()
-> Result<(), &'static str> {
    use proptest::test_runner::{Config, RngSeed, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 128,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(0x7261_6e67_652d_7631),
        ..Config::default()
    });
    runner
        .run(
            &prop::collection::vec(
                (any::<usize>(), any::<u64>(), prop::option::of(any::<u64>())),
                1..96,
            ),
            |operations| {
                let mut work = Work::default();
                let mut histories = vec![(PersistentIndex::default(), BTreeMap::new())];
                for (value, (parent, key, cut)) in operations.into_iter().enumerate() {
                    let (original, expected) = &histories[parent % histories.len()];
                    let mut expected = expected.clone();
                    let view = if let Some(cut) = cut {
                        expected.retain(|key, _| *key < cut);
                        original.prefix(cut.to_be_bytes())
                    } else {
                        original.clone()
                    };
                    let items = work.items;
                    let claims = work.claims;
                    let next = view
                        .insert(key.to_be_bytes(), value, &mut work)
                        .map_err(TestCaseError::fail)?;
                    // Two search paths plus two admitted child minima at each
                    // copied branch; coordinates strictly increase within u64.
                    prop_assert!(work.items - items <= 65 * 80);
                    prop_assert!(work.claims - claims <= 65);
                    expected.insert(key, value);
                    histories.push((next, expected));
                    for (history, expected) in &histories {
                        let claims = work.claims;
                        for query in [0, key, key.saturating_sub(1), u64::MAX] {
                            let items = work.items;
                            let actual = history
                                .predecessor(query.to_be_bytes(), &mut work)
                                .map_err(TestCaseError::fail)?
                                .map(|entry| (u64::from_be_bytes(*entry.key), *entry.value));
                            prop_assert_eq!(
                                actual,
                                expected.range(..=query).next_back().map(|(k, v)| (*k, *v))
                            );
                            prop_assert!(work.items - items <= 65 * 58);
                        }
                        prop_assert_eq!(work.claims, claims);
                    }
                }
                drop(histories);
                prop_assert_eq!(work.live.get(), 0);
                Ok(())
            },
        )
        .map_err(|error| {
            eprintln!("{error}");
            "immutable prefix histories differ from original ordered facts"
        })?;

    let mut work = Work {
        observe_retirement: true,
        ..Work::default()
    };
    let mut result: Result<(), &'static str> = Ok(());
    let measured = acyclic_test_support::measure(|| {
        result = (|| {
            let mut history = PersistentIndex::default();
            for key in 0_u64..256 {
                history = history.insert(key.to_be_bytes(), key, &mut work)?;
                assert_eq!(
                    work.live.get() as i64,
                    acyclic_test_support::current_allocations()
                        .unwrap()
                        .bytes_current
                );
            }
            let original = history.clone();
            history = history.prefix(128_u64.to_be_bytes());
            let live = work.live.get();
            for claims in 0..=8 {
                work.maximum_claims = Some(work.claims + claims);
                let attempt = history.insert(200_u64.to_be_bytes(), 999, &mut work);
                if claims == 0 {
                    assert!(matches!(attempt, Err("allocation refused")));
                }
                if let Ok(next) = attempt {
                    drop(next);
                }
                assert_eq!(work.live.get(), live);
                work.maximum_claims = None;
                assert_eq!(
                    history
                        .predecessor(u64::MAX.to_be_bytes(), &mut work)?
                        .map(|entry| *entry.value),
                    Some(127)
                );
                assert_eq!(
                    original
                        .predecessor(u64::MAX.to_be_bytes(), &mut work)?
                        .map(|entry| *entry.value),
                    Some(255)
                );
            }
            for items in [0, 1, 32, 128] {
                work.maximum_items = Some(work.items + items);
                drop(history.insert(200_u64.to_be_bytes(), 999, &mut work));
                assert_eq!(work.live.get(), live);
                work.maximum_items = None;
            }
            work.cancelled = true;
            assert!(history.predecessor(0_u64.to_be_bytes(), &mut work).is_err());
            assert!(history.insert(0_u64.to_be_bytes(), 1, &mut work).is_err());
            drop(original);
            drop(history);
            Ok(())
        })();
    });
    result?;
    assert_eq!(work.live.get(), 0);
    assert_eq!(measured.bytes_current, 0);
    assert!(
        !work.early_refund.get(),
        "immutable node backing outlived its original credit"
    );
    Ok(())
}

#[test]
fn keyed_histories_preserve_global_cuts_bounded_work_and_physical_custody()
-> Result<(), &'static str> {
    use proptest::test_runner::{Config, RngSeed, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(0x6b65_7965_642d_7631),
        ..Config::default()
    });
    runner
        .run(
            &prop::collection::vec(
                (
                    any::<usize>(),
                    0_u64..8,
                    0_u64..32,
                    prop::option::of(0_u64..32),
                ),
                1..64,
            ),
            |operations| {
                let mut work = Work::default();
                let mut histories = vec![(KeyedIndex::<_, _, 8>::default(), BTreeMap::new())];
                for (value, (parent, key, sequence, cut)) in operations.into_iter().enumerate() {
                    let (original, expected) = &histories[parent % histories.len()];
                    let mut expected = expected.clone();
                    let claims = work.claims;
                    let view = if let Some(cut) = cut {
                        expected.retain(|(_, sequence), _| *sequence < cut);
                        original.cap(cut)
                    } else {
                        original.clone()
                    };
                    prop_assert_eq!(work.claims, claims);
                    let next = view
                        .insert(key.to_be_bytes(), sequence, value, &mut work)
                        .map_err(TestCaseError::fail)?;
                    prop_assert!(work.claims - claims <= 130);
                    expected.insert((key, sequence), value);
                    histories.push((next, expected));
                    for (history, expected) in &histories {
                        for query in [0, key, 7, 8] {
                            for (from, before) in
                                [(0, 32), (sequence, 32), (0, sequence), (sequence, sequence)]
                            {
                                let items = work.items;
                                let claims = work.claims;
                                let actual = history
                                    .seek(query.to_be_bytes(), from, before, &mut work)
                                    .map_err(TestCaseError::fail)?
                                    .map(|(s, v)| (s, *v));
                                let expected = expected
                                    .iter()
                                    .find(|((k, s), _)| *k == query && from <= *s && *s < before)
                                    .map(|((_, s), v)| (*s, *v));
                                prop_assert_eq!(actual, expected);
                                prop_assert!(work.items - items <= 3 * 65 * 60);
                                prop_assert_eq!(work.claims, claims);
                            }
                        }
                    }
                }
                drop(histories);
                prop_assert_eq!(work.live.get(), 0);
                Ok(())
            },
        )
        .map_err(|error| {
            eprintln!("{error}");
            "keyed histories differ from original range facts"
        })?;

    // A lexical composite prefix is the actual existing operation, and cannot
    // implement a global sequence cut: it keeps a future row under a lower key.
    let mut work = Work::default();
    let composite = |key: u64, sequence: u64| {
        let mut result = [0; 16];
        result[..8].copy_from_slice(&key.to_be_bytes());
        result[8..].copy_from_slice(&sequence.to_be_bytes());
        result
    };
    let old = PersistentIndex::default().insert(composite(0, 9), 9, &mut work)?;
    assert!(
        old.prefix(composite(1, 5))
            .predecessor(composite(0, 9), &mut work)?
            .is_some()
    );
    let corrected =
        KeyedIndex::<_, _, 8>::default().insert(0_u64.to_be_bytes(), 9, 9, &mut work)?;
    let corrected = corrected
        .cap(5)
        .insert(1_u64.to_be_bytes(), 5, 5, &mut work)?;
    assert_eq!(corrected.seek(0_u64.to_be_bytes(), 0, 20, &mut work)?, None);
    assert_eq!(
        corrected.seek(1_u64.to_be_bytes(), 0, 20, &mut work)?,
        Some((5, &5))
    );
    drop((old, corrected));
    assert_eq!(work.live.get(), 0);

    for population in [1_u64, 100_000] {
        let mut work = Work {
            observe_retirement: true,
            ..Work::default()
        };
        let mut result = Ok(());
        let measured = acyclic_test_support::measure(|| {
            result = (|| {
                let mut index = KeyedIndex::<_, _, 8>::default();
                for key in 0..population {
                    index = index.insert(key.to_be_bytes(), 7, key, &mut work)?;
                }
                assert_eq!(
                    work.live.get() as i64,
                    acyclic_test_support::current_allocations()
                        .unwrap()
                        .bytes_current
                );
                let claims = work.claims;
                let capped = index.cap(7);
                let fork = capped.clone();
                assert_eq!(work.claims, claims);
                for key in [0, population - 1, u64::MAX] {
                    let items = work.items;
                    let actual = index.seek(key.to_be_bytes(), 0, 8, &mut work)?;
                    assert_eq!(
                        actual.map(|(s, v)| (s, *v)),
                        (key < population).then_some((7, key))
                    );
                    assert!(work.items - items <= 3 * 65 * 60);
                    assert_eq!(work.claims, claims);
                    assert_eq!(fork.seek(key.to_be_bytes(), 0, 8, &mut work)?, None);
                }
                let live = work.live.get();
                for additional in 0..=16 {
                    work.maximum_claims = Some(work.claims + additional);
                    drop(capped.insert(0_u64.to_be_bytes(), 8, 999, &mut work));
                    work.maximum_claims = None;
                    assert_eq!(work.live.get(), live);
                }
                for additional in [0, 1, 32, 128, 1024] {
                    work.maximum_items = Some(work.items + additional);
                    drop(capped.insert(0_u64.to_be_bytes(), 8, 999, &mut work));
                    work.maximum_items = None;
                    assert_eq!(work.live.get(), live);
                }
                work.cancelled = true;
                assert!(index.seek(0_u64.to_be_bytes(), 0, 8, &mut work).is_err());
                assert!(index.seek(0_u64.to_be_bytes(), 8, 8, &mut work).is_err());
                assert!(
                    index
                        .insert(0_u64.to_be_bytes(), 8, 999, &mut work)
                        .is_err()
                );
                drop((index, capped, fork));
                Ok(())
            })();
        });
        result?;
        assert_eq!(work.live.get(), 0);
        assert_eq!(measured.bytes_current, 0);
        assert!(
            !work.early_refund.get(),
            "keyed physical backing outlived original funding"
        );
    }

    let mut work = Work::default();
    let inputs: &[&[u8]] = &[
        b"",
        b"\0",
        b"\0\0",
        b"\0\x01",
        b"a",
        b"a\0",
        b"a\0\0",
        b"b",
        &[255],
    ];
    for left in inputs {
        for right in inputs {
            assert_eq!(
                binary_key::<16, _>(left, &mut work)
                    .unwrap()
                    .cmp(&binary_key::<16, _>(right, &mut work).unwrap()),
                left.cmp(right)
            );
        }
    }
    assert_eq!(
        binary_key::<7, _>(b"", &mut work),
        Err(BinaryKeyError::Width(7))
    );
    assert_eq!(
        binary_key::<8, _>(b"x", &mut work),
        Err(BinaryKeyError::TooLong {
            observed: 1,
            maximum: 0
        })
    );
    work.cancelled = true;
    assert_eq!(
        binary_key::<16, _>(b"a", &mut work),
        Err(BinaryKeyError::Work("cancelled"))
    );

    // Every branch coordinate is present. Reverse construction adds roots;
    // replacing the all-zero key copies the full original wide path. Retirement
    // and successor must use bounded explicit storage rather than recursion.
    let mut work = Work {
        observe_retirement: true,
        ..Work::default()
    };
    let mut result = Ok(());
    let measured = acyclic_test_support::measure(|| {
        result = (|| {
            let zero = binary_key::<1032, _>(&[0; 1024], &mut work).unwrap();
            let mut index = KeyedIndex::default().insert(zero, 0, 0, &mut work)?;
            for bit in (0..8192).rev() {
                let mut bytes = [0; 1024];
                bytes[bit / 8] = 0x80 >> (bit % 8);
                let key = binary_key::<1032, _>(&bytes, &mut work).unwrap();
                index = index.insert(key, 9, bit + 1, &mut work)?;
            }
            let original = index.clone();
            index = index.cap(1).insert(zero, 1, 9999, &mut work)?;
            let items = work.items;
            assert_eq!(index.seek(zero, 1, 2, &mut work)?, Some((1, &9999)));
            assert!(work.items - items <= (8 * 1032 + 1) * (7 * 1032 + 2) + 3 * 65 * 60);
            let mut first = [0; 1024];
            first[0] = 128;
            let first = binary_key::<1032, _>(&first, &mut work).unwrap();
            assert_eq!(index.seek(first, 0, 10, &mut work)?, None);
            assert_eq!(original.seek(first, 0, 10, &mut work)?, Some((9, &1)));
            drop((index, original));
            Ok(())
        })();
    });
    result?;
    assert_eq!(work.live.get(), 0);
    assert_eq!(measured.bytes_current, 0);
    assert!(!work.early_refund.get());
    Ok(())
}

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
    maximum_items: Option<u64>,
    maximum_claims: Option<u64>,
    cancelled: bool,
    live: Rc<Cell<u64>>,
    observe_retirement: bool,
    early_refund: Rc<Cell<bool>>,
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
            if physical > remaining as i64 {
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
        self.live.set(live);
        Ok(Allocation {
            bytes,
            live: Rc::clone(&self.live),
            observe_retirement: self.observe_retirement,
            early_refund: Rc::clone(&self.early_refund),
        })
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
                work.live.get() as i64
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
                    let before = work.items;
                    let mut expected_owner = expected.get(&key).copied();
                    let observed = table.get_retained_mut(
                        key,
                        |value| Some(*value) == expected_owner,
                        |items| work.items += items,
                    );
                    prop_assert_eq!(observed.as_deref(), expected.get(&key));
                    if let Some(value) = observed {
                        *value += 1;
                        *expected.get_mut(&key).unwrap() += 1;
                    }
                    prop_assert!(work.items - before <= 9 * 40 + 1);
                    expected_owner = expected.get(&key).map(|value| value + 1);
                    prop_assert!(
                        table
                            .get_retained_mut(
                                key,
                                |value| Some(*value) == expected_owner,
                                |items| work.items += items,
                            )
                            .is_none()
                    );
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
        let before = work.items;
        work.maximum_items = Some(before);
        assert!(
            table
                .get_retained_mut([0; N], |value| *value == 1, |items| work.items += items)
                .is_none()
        );
        *table
            .get_retained_mut([0; N], |value| *value == 0, |items| work.items += items)
            .ok_or("original retained owner was refused during cancellation")? = 2;
        assert!(work.items - before <= 2 * (9 * N + 1) as u64);
        assert!(
            empty
                .get_retained_mut(
                    [0; N],
                    |_| panic!("absent owner predicate"),
                    |_| panic!("absent node observation"),
                )
                .is_none()
        );
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
    // An old retained registration must not mutate a successor at the same key.
    let mut work = Work::default();
    let mut table = FixedIndex::default();
    table.get_or_try_insert_with([7; 8], &mut work, |_| Ok(0))?;
    let original = table
        .remove_if([7; 8], |generation| *generation == 0)
        .ok_or("missing original registration")?;
    table.get_or_try_insert_with([7; 8], &mut work, |_| Ok(1))?;
    let claims = work.claims;
    let live = work.live.get();
    work.cancelled = true;
    assert!(
        table
            .get_retained_mut(
                [7; 8],
                |generation| *generation == 0,
                |items| work.items += items
            )
            .is_none()
    );
    *table
        .get_retained_mut(
            [7; 8],
            |generation| *generation == 1,
            |items| work.items += items,
        )
        .ok_or("successor retirement was refused")? = 2;
    assert_eq!(*original.value(), 0);
    assert_eq!(work.claims, claims);
    assert_eq!(work.live.get(), live);
    drop(table);
    drop(original);
    assert_eq!(work.live.get(), 0);
    a_full_scan_negative_exposes_population_work_hidden_by_the_fixed_key_index()
}
