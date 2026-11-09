use super::*;

#[test]
fn keyed_histories_preserve_global_cuts_bounded_work_and_physical_custody()
-> Result<(), &'static str> {
    keyed_generated_histories()?;
    keyed_global_cut_negative()?;
    keyed_population_work()?;
    binary_key_order_and_refusal()?;
    keyed_wide_path_retirement()?;
    Ok(())
}

fn keyed_generated_histories() -> Result<(), &'static str> {
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
                    let (original, expected) = histories
                        .get(parent % histories.len())
                        .ok_or_else(|| TestCaseError::fail("original history is missing"))?;
                    let mut expected = expected.clone();
                    let claims = work.claims;
                    let claimed_bytes = work.claimed_bytes;
                    let items = work.items;
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
                    let bound = view.insertion_bound(1).unwrap();
                    prop_assert!(work.claimed_bytes - claimed_bytes <= bound.allocation_bytes);
                    prop_assert!(work.items - items <= bound.examined_items);
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
    Ok(())
}

fn keyed_global_cut_negative() -> Result<(), &'static str> {
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
    let original = KeyedIndex::<_, _, 8>::default().insert(0_u64.to_be_bytes(), 9, 9, &mut work)?;
    let corrected = original
        .cap(5)
        .insert(1_u64.to_be_bytes(), 5, 5, &mut work)?;
    assert_eq!(corrected.seek(0_u64.to_be_bytes(), 0, 20, &mut work)?, None);
    assert_eq!(
        corrected.seek(1_u64.to_be_bytes(), 0, 20, &mut work)?,
        Some((5, &5))
    );
    drop((old, original, corrected));
    assert_eq!(work.live.get(), 0);
    Ok(())
}

fn keyed_population_work() -> Result<(), &'static str> {
    let mut indexed_work = [[0; 3]; 2];
    for (observations, population) in indexed_work.iter_mut().zip([1_u64, 100_000]) {
        let mut work = Work {
            observe_retirement: true,
            ..Work::default()
        };
        let mut result: Result<(), &'static str> = Ok(());
        let measured = acyclic_test_support::measure(|| {
            result = (|| {
                let mut index = KeyedIndex::<_, _, 8>::default();
                for key in 0..population {
                    index = index.insert(key.to_be_bytes(), 7, key, &mut work)?;
                }
                assert_eq!(
                    work.live_bytes(),
                    acyclic_test_support::current_allocations()
                        .unwrap()
                        .bytes_current
                );
                let claims = work.claims;
                let capped = index.cap(7);
                let fork = capped.clone();
                assert_eq!(work.claims, claims);
                for (observation, key) in observations.iter_mut().zip([0, population - 1, u64::MAX])
                {
                    let items = work.items;
                    let actual = index.seek(key.to_be_bytes(), 0, 8, &mut work)?;
                    assert_eq!(
                        actual.map(|(s, v)| (s, *v)),
                        (key < population).then_some((7, key))
                    );
                    *observation = work.items - items;
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

    let old_work = [1_u64, 100_000].map(|population| {
        ordered_comparisons(|| {
            let mut old = BTreeMap::new();
            for key in 0..population {
                old.insert(CountedKey(key), key);
            }
            ORDERED_COMPARISONS.set(0);
            assert_eq!(old.get(&CountedKey(0)), Some(&0));
        })
    });
    assert!(
        old_work[1] > old_work[0],
        "the actual ordered-map negative must expose population growth"
    );
    println!(
        "population=[1,100000] indexed_items={indexed_work:?} ordered_comparisons={old_work:?}; seek/cap allocations=0"
    );
    Ok(())
}

fn binary_key_order_and_refusal() -> Result<(), &'static str> {
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
                    .map_err(|_| "left binary key refused")?
                    .cmp(
                        &binary_key::<16, _>(right, &mut work)
                            .map_err(|_| "right binary key refused")?
                    ),
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
    Ok(())
}

fn keyed_wide_path_retirement() -> Result<(), &'static str> {
    // Every branch coordinate is present. Reverse construction adds roots;
    // replacing the all-zero key copies the full original wide path. Retirement
    // and successor must use bounded explicit storage rather than recursion.
    let mut work = Work {
        observe_retirement: true,
        ..Work::default()
    };
    let mut result: Result<(), &'static str> = Ok(());
    let measured = acyclic_test_support::measure(|| {
        result = (|| {
            let zero = binary_key::<1032, _>(&[0; 1024], &mut work).unwrap();
            let mut index = KeyedIndex::default().insert(zero, 0, 0, &mut work)?;
            let items = work.items;
            drop(index.insert(zero, 0, 0, &mut work)?);
            assert!(work.items - items <= 2 * 1032 + 8 + 5);
            let items = work.items;
            assert_eq!(index.seek(zero, 0, 1, &mut work)?, Some((0, &0)));
            assert!(work.items - items <= 1032 + 1 + 3 * 8 + 1);
            let required = work.items - items;
            work.maximum_items = Some(work.items + required - 1);
            assert_eq!(
                index.seek(zero, 0, 1, &mut work).err(),
                Some("items refused")
            );
            work.maximum_items = None;
            for bit in (0..8192).rev() {
                let mut bytes = [0; 1024];
                *bytes
                    .get_mut(bit / 8)
                    .ok_or("branch bit exceeds original key")? = 0x80 >> (bit % 8);
                let key = binary_key::<1032, _>(&bytes, &mut work).unwrap();
                let bound = index.insertion_bound(1).unwrap();
                let claimed = work.claimed_bytes;
                let items = work.items;
                index = index.insert(key, 9, bit + 1, &mut work)?;
                assert!(work.claimed_bytes - claimed <= bound.allocation_bytes);
                assert!(work.items - items <= bound.examined_items);
            }
            let original = index.clone();
            let bound = index.insertion_bound(1).unwrap();
            let claimed = work.claimed_bytes;
            let items = work.items;
            work.maximum_items = Some(items + bound.examined_items);
            index = index.insert(zero, 0, 8888, &mut work)?;
            assert!(work.claimed_bytes - claimed <= bound.allocation_bytes);
            assert!(work.items - items <= bound.examined_items);
            work.maximum_items = None;
            assert_eq!(index.seek(zero, 0, 1, &mut work)?, Some((0, &8888)));
            assert_eq!(original.seek(zero, 0, 1, &mut work)?, Some((0, &0)));
            index = index.cap(1).insert(zero, 1, 9999, &mut work)?;
            let items = work.items;
            assert_eq!(index.seek(zero, 1, 2, &mut work)?, Some((1, &9999)));
            assert!(work.items - items <= 9 * 1032 + 1 + 3 * 65 * 60);
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
