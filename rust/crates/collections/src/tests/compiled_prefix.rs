use super::*;

#[test]
fn compiled_prefix_matches_original_keys_and_refuses_original_read_credit() {
    // Deliberately neither Clone nor AsRef: the projection borrows each owned
    // key, while entry identity and the caller's vector order remain intact.
    struct Record {
        key: Vec<u8>,
        original: usize,
    }
    use proptest::test_runner::{Config, RngSeed, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 128,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(0x7072_6566_6978_7631),
        ..Config::default()
    });
    runner
        .run(
            &(
                prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..64),
                prop::collection::vec(any::<u8>(), 0..80),
            ),
            |(keys, query)| {
                for repeated in [false, true] {
                    let mut keys = keys.clone();
                    if repeated {
                        keys.extend(keys.clone());
                        keys.extend([Vec::new(), Vec::new()]);
                    }
                    keys.sort_unstable();
                    if !repeated {
                        keys.dedup();
                    }
                    let mut work = Work::default();
                    let entries = keys
                        .iter()
                        .rev()
                        .enumerate()
                        .map(|(original, key)| Record {
                            key: key.clone(),
                            original,
                        })
                        .collect::<Vec<_>>();
                    let original_vector = entries.as_ptr();
                    let key_backing = entries
                        .iter()
                        .map(|entry| entry.key.as_ptr())
                        .collect::<Vec<_>>();
                    let key: fn(&Record) -> &[u8] = |entry| entry.key.as_slice();
                    let index = if repeated {
                        CompiledPrefixIndex::from_repeated_entries(entries, key, &mut work)
                    } else {
                        CompiledPrefixIndex::from_entries(entries, key, &mut work)
                    }
                    .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
                    prop_assert_eq!(index.entries().as_ptr(), original_vector);
                    for (position, entry) in index.entries().iter().enumerate() {
                        prop_assert_eq!(entry.original, position);
                        prop_assert_eq!(entry.key.as_ptr(), key_backing[position]);
                    }
                    let queries = std::iter::once(query.as_slice()).chain(
                        keys.iter()
                            .flat_map(|key| (0..=key.len()).map(|end| &key[..end])),
                    );
                    for query in queries {
                        for mode in 0..3 {
                            let accept = |key: &Vec<u8>| match mode {
                                0 => true,
                                1 => !key.is_empty(),
                                _ => key.len().is_multiple_of(2),
                            };
                            let before = work.items;
                            let matches = index
                                .matches(query, |entry| accept(&entry.key), &mut work)
                                .unwrap();
                            let expected = keys
                                .iter()
                                .filter(|key| key.len() < query.len() && query.starts_with(key))
                                .filter(|key| accept(key))
                                .min_by_key(|key| key.len());
                            prop_assert_eq!(
                                matches.exact.as_ref().map(|matched| &matched.entry.key),
                                keys.iter().find(|key| key.as_slice() == query)
                            );
                            prop_assert_eq!(
                                matches.shortest.as_ref().map(|matched| &matched.entry.key),
                                expected
                            );
                            for matched in [matches.exact, matches.shortest].into_iter().flatten() {
                                let entry = matched.entry;
                                prop_assert_eq!(
                                    matched.position,
                                    entry.original,
                                    "matched original position"
                                );
                                prop_assert!(std::ptr::eq(entry, &index.entries()[entry.original]));
                                prop_assert_eq!(
                                    entry.original,
                                    index
                                        .entries()
                                        .iter()
                                        .position(|original| original.key == entry.key)
                                        .unwrap()
                                );
                            }
                            prop_assert!(work.items - before <= 12 * query.len() as u64 + 3);
                            let used = work.items - before;
                            work.maximum_items = Some(work.items);
                            if used != 0 {
                                prop_assert_eq!(
                                    index
                                        .matches(query, |entry| accept(&entry.key), &mut work)
                                        .err(),
                                    Some("items refused")
                                );
                            }
                            work.maximum_items = None;
                        }
                    }
                    work.cancelled = true;
                    prop_assert_eq!(
                        index.matches(&[], |_| true, &mut work).err(),
                        Some("cancelled")
                    );
                    drop(index);
                    prop_assert_eq!(work.live.get(), 0);
                    for maximum_claims in [Some(0), Some(1)] {
                        let mut refused = Work {
                            maximum_claims,
                            ..Work::default()
                        };
                        let key: fn(&Vec<u8>) -> &[u8] = Vec::as_slice;
                        let attempt = if repeated {
                            CompiledPrefixIndex::from_repeated_entries(
                                keys.clone(),
                                key,
                                &mut refused,
                            )
                        } else {
                            CompiledPrefixIndex::from_entries(keys.clone(), key, &mut refused)
                        };
                        prop_assert_eq!(attempt.is_ok(), keys.is_empty());
                        drop(attempt);
                        prop_assert_eq!(refused.live.get(), 0);
                    }
                    if keys.len() > 1 {
                        let mut refused = Work {
                            maximum_items: Some(0),
                            ..Work::default()
                        };
                        let key: fn(&Vec<u8>) -> &[u8] = Vec::as_slice;
                        let attempt = if repeated {
                            CompiledPrefixIndex::from_repeated_entries(
                                keys.clone(),
                                key,
                                &mut refused,
                            )
                        } else {
                            CompiledPrefixIndex::from_entries(keys.clone(), key, &mut refused)
                        };
                        prop_assert!(matches!(
                            attempt,
                            Err(CompiledPrefixError::Work("items refused"))
                        ));
                        prop_assert_eq!(refused.live.get(), 0);
                    }
                    if let Some(key) = keys.first() {
                        let mut refused = Work::default();
                        let attempt = CompiledPrefixIndex::from_entries(
                            vec![key.clone(), key.clone()],
                            |key: &Vec<u8>| key.as_slice(),
                            &mut refused,
                        );
                        prop_assert!(matches!(attempt, Err(CompiledPrefixError::Order(_))));
                        prop_assert_eq!(refused.live.get(), 0);
                    }
                }
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn compiled_prefix_cost_and_backing_do_not_grow_with_unrelated_keys() {
    for repeated in [false, true] {
        for population in [1_u32, 100_000] {
            let mut keys = (0..population)
                .map(|value| value.to_be_bytes().to_vec())
                .collect::<Vec<_>>();
            keys.extend([b"root".to_vec(), b"root/leaf".to_vec()]);
            let mut work = Work::default();
            let key: fn(&Vec<u8>) -> &[u8] = Vec::as_slice;
            let index = if repeated {
                keys.extend(keys.clone());
                CompiledPrefixIndex::from_repeated_entries(keys, key, &mut work)
            } else {
                CompiledPrefixIndex::from_entries(keys, key, &mut work)
            }
            .unwrap();
            let query = b"root/leaf/child";
            let before = work.items;
            let claims = work.claims;
            let physical = acyclic_test_support::measure(|| {
                let matches = index.matches(query, |_| true, &mut work).unwrap();
                assert_eq!(
                    matches.shortest.map(|matched| matched.entry.as_slice()),
                    Some(b"root".as_slice())
                );
                assert!(matches.exact.is_none());
            });
            let examined = work.items - before;
            assert!(examined <= 12 * query.len() as u64 + 3);
            assert_eq!(work.claims, claims);
            assert_eq!(physical.count_total, 0);
            // The old all-key scan is an independent negative cost control.
            // It cannot satisfy the identical fixed-query bound at large population.
            if population == 100_000 {
                assert!(index.entries().len() as u64 > 12 * query.len() as u64 + 3);
            }
            eprintln!(
                "prefix population={population} repeated={repeated} examined={examined} lookup_allocations=0"
            );
            drop(index);
            assert_eq!(work.live.get(), 0);
        }
    }
    // Width is supplied by the real key, with no fixed-size encoding or hash.
    let long = vec![0; 70_000];
    let mut longer = long.clone();
    longer.push(0);
    let mut work = Work::default();
    let index = CompiledPrefixIndex::from_sorted(vec![long, longer.clone()], &mut work).unwrap();
    let matches = index.matches(&longer, |_| true, &mut work).unwrap();
    assert_eq!(matches.exact.map(|matched| matched.entry), Some(&longer));
    assert_eq!(matches.shortest.unwrap().entry.len(), 70_000);
    drop(index);
    assert_eq!(work.live.get(), 0);

    for maximum in [Some(0), Some(1), None] {
        let mut work = Work {
            maximum_items: maximum,
            ..Work::default()
        };
        let outcome =
            CompiledPrefixIndex::from_sorted(vec![b"a".to_vec(), b"ab".to_vec()], &mut work);
        assert_eq!(outcome.is_ok(), maximum.is_none());
        drop(outcome);
        assert_eq!(work.live.get(), 0);
    }
    let mut work = Work {
        maximum_claims: Some(0),
        ..Work::default()
    };
    assert!(matches!(
        CompiledPrefixIndex::from_sorted(vec![b"a".to_vec()], &mut work),
        Err(CompiledPrefixError::Work("allocation refused"))
    ));
    assert_eq!(work.live.get(), 0);
    assert!(matches!(
        CompiledPrefixIndex::from_sorted(vec![b"a".to_vec(), b"a".to_vec()], &mut work),
        Err(CompiledPrefixError::Order(1))
    ));

    let mut work = Work {
        observe_retirement: true,
        ..Work::default()
    };
    let measured = acyclic_test_support::measure(|| {
        // The original vector has its own caller-retained funding. The table
        // adds exactly one physical allocation, not a box or copy per key.
        let original = work
            .claim(2 * std::mem::size_of::<[u8; 4]>() as u64)
            .unwrap();
        let entries = vec![[0; 4], [1; 4]];
        let index = CompiledPrefixIndex::from_sorted(entries, &mut work).unwrap();
        assert_eq!(
            acyclic_test_support::current_allocations()
                .unwrap()
                .bytes_current,
            work.live.get() as i64
        );
        drop(index);
        drop(original);
    });
    assert_eq!(measured.count_total, 2);
    assert_eq!(measured.bytes_current, 0);
    assert_eq!(work.live.get(), 0);
    assert!(!work.early_refund.get());

    let mut work = Work {
        observe_retirement: true,
        ..Work::default()
    };
    let measured = acyclic_test_support::measure(|| {
        let original = work
            .claim(2 * std::mem::size_of::<[u8; 4]>() as u64)
            .unwrap();
        let entries = vec![[1; 4], [0; 4]];
        let index =
            CompiledPrefixIndex::from_entries(entries, |key: &[u8; 4]| key.as_slice(), &mut work)
                .unwrap();
        assert_eq!(
            acyclic_test_support::current_allocations()
                .unwrap()
                .bytes_current,
            work.live.get() as i64
        );
        drop(index);
        drop(original);
    });
    // Original vector, temporary permutation, retained radix nodes. All three
    // remain funded until their actual backing retires, including on refusal.
    assert_eq!(measured.count_total, 3);
    assert_eq!(measured.bytes_current, 0);
    assert_eq!(work.live.get(), 0);
    assert!(!work.early_refund.get());
}
