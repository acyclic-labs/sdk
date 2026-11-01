use super::*;

fn contents<const N: usize>(
    index: &FixedIndex<usize, Allocation, N>,
    work: &mut Work,
) -> Result<BTreeMap<[u8; N], usize>, &'static str> {
    audit(index);
    let mut result = BTreeMap::new();
    let mut entry = index.first(work)?;
    while let Some((key, value)) = entry {
        assert!(result.insert(*key, *value).is_none());
        entry = index.seek_after(*key, work)?;
    }
    Ok(result)
}

#[test]
fn prefix_extraction_preserves_exact_keys_original_custody_and_bounded_work()
-> Result<(), &'static str> {
    use proptest::test_runner::{Config, RngSeed, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 128,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(0x7072_6566_6978_7631),
        ..Config::default()
    });
    runner
        .run(
            &prop::collection::vec((any::<u32>(), 0_usize..7, any::<bool>(), any::<bool>()), 1..96),
            |operations| {
                let mut work = Work::default();
                let mut index = FixedIndex::<usize, _, 4>::default();
                let mut expected = BTreeMap::new();
                for (value, (key, length, extract, miss)) in operations.into_iter().enumerate() {
                    let key = key.to_be_bytes();
                    index
                        .get_or_try_insert_with(key, &mut work, |_| Ok(value))
                        .map_err(TestCaseError::fail)?;
                    expected.entry(key).or_insert(value);
                    if !extract {
                        if value % 7 == 0 {
                            let removed = index.remove_if(key, |_| !miss);
                            if miss {
                                prop_assert!(removed.is_none());
                            } else {
                                prop_assert_eq!(removed.as_ref().map(|entry| *entry.value()), expected.remove(&key));
                            }
                            drop(removed);
                            prop_assert_eq!(contents(&index, &mut work).map_err(TestCaseError::fail)?, expected.clone());
                        }
                        continue;
                    }
                    let live = work.live.get();
                    let claims = work.claims;
                    let items = work.items;
                    // Retirement cannot depend on a cancelled issuing call or
                    // a fresh work/allocation allowance being available.
                    work.cancelled = true;
                    work.maximum_items = Some(0);
                    work.maximum_claims = Some(0);
                    let mut visits = 0;
                    let mut bytes = [key[0], key[1], key[2], key[3], 0, 0];
                    if miss {
                        bytes[0] ^= 255;
                    }
                    let prefix = &bytes[..length];
                    let result = index.extract_prefix(prefix, |n| visits += n);
                    prop_assert_eq!(work.live.get(), live, "extraction retired original backing");
                    prop_assert_eq!(work.claims, claims);
                    prop_assert_eq!(work.items, items);
                    work.cancelled = false;
                    work.maximum_items = None;
                    work.maximum_claims = None;
                    if length > key.len() {
                        prop_assert!(matches!(result, Err(PrefixTooLong { observed, maximum: 4 }) if observed == length), "oversized prefix was not refused");
                        prop_assert_eq!(visits, 0);
                    } else {
                        let extracted = result.map_err(|error| TestCaseError::fail(error.to_string()))?;
                        let selected: BTreeMap<_, _> = expected
                            .iter()
                            .filter(|(candidate, _)| candidate.starts_with(prefix))
                            .map(|(key, value)| (*key, *value))
                            .collect();
                        expected.retain(|key, _| !selected.contains_key(key));
                        prop_assert!(visits <= 17 * 4 + 3);
                        prop_assert_eq!(&contents(extracted.index(), &mut work).map_err(TestCaseError::fail)?, &selected);
                        let items = work.items;
                        let claims = work.claims;
                        let mut drain_items = 0;
                        work.cancelled = true;
                        work.maximum_items = Some(0);
                        work.maximum_claims = Some(0);
                        prop_assert!(extracted.index().first(&mut work).is_err());
                        let mut entries = extracted.into_entries(|n| drain_items += n);
                        if value % 2 == 0 {
                            // Withdrawing a partial consumer must retire only
                            // the unconsumed subtree, never its delivered leaf.
                            let entry = entries.next();
                            prop_assert_eq!(entry.as_ref().map(|entry| (*entry.key(), *entry.value())), selected.first_key_value().map(|(key, value)| (*key, *value)));
                            drop(entries);
                            let retained = entry.as_ref().map_or(0, |entry| entry._allocation.bytes);
                            let live = work.live.get();
                            prop_assert!(live >= retained);
                            drop(entry);
                            prop_assert_eq!(work.live.get(), live - retained);
                        } else {
                            let mut delivered = Vec::new();
                            for entry in &mut entries {
                                delivered.push(entry.into_parts());
                            }
                            prop_assert!(entries.next().is_none());
                            prop_assert!(entries.next().is_none());
                            drop(entries);
                            prop_assert_eq!(delivered.iter().map(|(key, value, _)| (*key, *value)).collect::<Vec<_>>(), selected.iter().map(|(key, value)| (*key, *value)).collect::<Vec<_>>());
                            drop(delivered);
                        }
                        prop_assert!(drain_items <= 6 * selected.len().saturating_mul(2) as u64);
                        prop_assert_eq!(work.claims, claims);
                        prop_assert_eq!(work.items, items);
                        work.cancelled = false;
                        work.maximum_items = None;
                        work.maximum_claims = None;
                        prop_assert_eq!(contents(&index, &mut work).map_err(TestCaseError::fail)?, expected.clone());
                    }
                    prop_assert_eq!(contents(&index, &mut work).map_err(TestCaseError::fail)?, expected.clone());
                }
                drop(index);
                prop_assert_eq!(work.live.get(), 0);
                Ok(())
            },
        )
        .map_err(|error| {
            eprintln!("{error}");
            "prefix extraction differs from complete original keys"
        })?;

    let mut indexed = [0; 2];
    let mut census = [0; 2];
    for (run, population) in [1_u64, 100_000].into_iter().enumerate() {
        let mut work = Work::default();
        let mut index = FixedIndex::<usize, _, 9>::default();
        let mut ordered = BTreeMap::new();
        index.get_or_try_insert_with([0; 9], &mut work, |_| Ok(0))?;
        ordered.insert([0; 9], 0);
        for key in (0..population).rev() {
            let mut bytes = [1; 9];
            bytes[1..].copy_from_slice(&key.to_be_bytes());
            index.get_or_try_insert_with(bytes, &mut work, |_| Ok(1))?;
            ordered.insert(bytes, 1);
        }
        let claims = work.claims;
        let live = work.live.get();
        let selected = index
            .extract_prefix(&[0], |items| indexed[run] += items)
            .unwrap();
        assert_eq!(work.claims, claims);
        assert_eq!(work.live.get(), live);
        assert_eq!(contents(selected.index(), &mut work)?.len(), 1);
        assert_eq!(contents(&index, &mut work)?.len(), population as usize);
        // The original whole-partition census must fail the same fixed-output
        // cost requirement even when it produces the correct selected entry.
        assert_eq!(
            ordered
                .keys()
                .filter(|key| {
                    census[run] += 1;
                    key.starts_with(&[0])
                })
                .count(),
            1
        );
        work.cancelled = true;
        let mut drain_items = 0;
        let mut entries = selected.into_entries(|n| drain_items += n);
        assert_eq!(
            entries.next().map(|entry| (*entry.key(), *entry.value())),
            Some(([0; 9], 0))
        );
        assert!(entries.next().is_none());
        drop(entries);
        assert_eq!(drain_items, 1);
        drop(index);
        assert_eq!(work.live.get(), 0);
    }
    assert_eq!(indexed[0], indexed[1]);
    assert!(census[1] > census[0]);
    println!("foreign=[1,100000] prefix_items={indexed:?} census_items={census:?}; selected=1");

    let mut work = Work {
        observe_retirement: true,
        ..Work::default()
    };
    let mut result: Result<(), &'static str> = Ok(());
    let measured = acyclic_test_support::measure(|| {
        result = (|| {
            let mut index = FixedIndex::<usize, _, 4>::default();
            for first in 0..=255_u8 {
                for second in [0, 128] {
                    index.get_or_try_insert_with([first, second, 0, 0], &mut work, |_| Ok(7))?;
                }
            }
            let before = acyclic_test_support::current_allocations().unwrap();
            let live = work.live.get();
            assert_eq!(live as i64, before.bytes_current);
            let detached = index.extract_prefix(&[127], |_| {}).unwrap();
            let after = acyclic_test_support::current_allocations().unwrap();
            assert_eq!(after.bytes_current, before.bytes_current);
            assert_eq!(after.count_total, before.count_total);
            assert_eq!(after.count_current, before.count_current);
            assert_eq!(work.live.get(), live);
            work.cancelled = true;
            drop(index);
            assert!(
                work.live.get() > 0,
                "extracted prefix lost its original credit"
            );
            let before = acyclic_test_support::current_allocations().unwrap();
            let mut drain_items = 0;
            let mut entries = detached.into_entries(|n| drain_items += n);
            let first = entries.next().ok_or("missing detached leaf")?;
            assert_eq!(first.key(), &[127, 0, 0, 0]);
            assert_eq!(*first.value(), 7);
            drop(entries);
            assert_eq!(work.live.get(), first._allocation.bytes);
            let after = acyclic_test_support::current_allocations().unwrap();
            assert_eq!(before.count_total, after.count_total);
            assert_eq!(after.count_current, 0);
            drop(first);
            assert_eq!(work.live.get(), 0);
            assert!(drain_items <= 6 * 3);

            // Include every possible branch coordinate, constructed in reverse
            // order. The complete-key prefix reaches the deepest original leaf.
            work.cancelled = false;
            let mut index = FixedIndex::<usize, _, 4>::default();
            index.get_or_try_insert_with([0; 4], &mut work, |_| Ok(7))?;
            for bit in (0..32).rev() {
                let key = (1_u32 << bit).to_be_bytes();
                index.get_or_try_insert_with(key, &mut work, |_| Ok(8))?;
            }
            work.cancelled = true;
            let mut visits = 0;
            let detached = index.extract_prefix(&[0; 4], |n| visits += n).unwrap();
            assert!(visits <= 17 * 4 + 3);
            drop((index, detached));
            assert_eq!(work.live.get(), 0);

            verify_drain_shape::<0>(&mut work)?;
            verify_drain_shape::<4>(&mut work)?;
            verify_drain_shape::<40>(&mut work)?;
            Ok(())
        })();
    });
    result?;
    assert_eq!(measured.bytes_current, 0);
    assert!(!work.early_refund.get());
    Ok(())
}

fn verify_drain_shape<const N: usize>(work: &mut Work) -> Result<(), &'static str> {
    let mut index = FixedIndex::<usize, _, N>::default();
    work.cancelled = false;
    index.get_or_try_insert_with([0; N], work, |_| Ok(0))?;
    // A deep adversarial tree includes every original coordinate. Only actual
    // input construction visits a key-width path per inserted key.
    for bit in (0..8 * N).rev() {
        let mut key = [0; N];
        key[bit / 8] = 0x80 >> (bit % 8);
        index.get_or_try_insert_with(key, work, |_| Ok(bit + 1))?;
    }
    work.cancelled = true;
    let original_items = work.items;
    let original_claims = work.claims;
    let before = acyclic_test_support::current_allocations().unwrap();
    let detached = index.extract_prefix(&[], |_| {}).unwrap();
    let mut visits = 0;
    let mut entries = detached.into_entries(|n| visits += n);
    let mut prior = None;
    let mut count = 0;
    for entry in &mut entries {
        if let Some(prior) = prior {
            assert!(prior < *entry.key(), "owned drain lost canonical order");
        }
        prior = Some(*entry.key());
        count += 1;
    }
    assert!(entries.next().is_none());
    drop(entries);
    drop(index);
    assert_eq!(count, 8 * N + 1);
    assert!(visits <= 6 * (2 * count - 1) as u64);
    let after = acyclic_test_support::current_allocations().unwrap();
    assert_eq!(before.count_total, after.count_total);
    assert_eq!(after.count_current, 0);
    assert_eq!(work.items, original_items);
    assert_eq!(work.claims, original_claims);
    assert_eq!(work.live.get(), 0);
    Ok(())
}
