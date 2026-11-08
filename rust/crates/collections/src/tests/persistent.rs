use super::*;

#[test]
fn persistent_histories_preserve_prefixes_bounded_paths_and_original_custody()
-> Result<(), &'static str> {
    persistent_generated_histories()?;
    persistent_physical_retirement()?;
    Ok(())
}

fn persistent_generated_histories() -> Result<(), &'static str> {
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
                    let (original, expected) = histories
                        .get(parent % histories.len())
                        .ok_or_else(|| TestCaseError::fail("original history is missing"))?;
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
                                .map(
                                    |PersistentPosition {
                                         key,
                                         value,
                                         before: _,
                                     }| {
                                        (u64::from_be_bytes(*key), *value)
                                    },
                                );
                            prop_assert_eq!(
                                actual,
                                expected.range(..=query).next_back().map(|(k, v)| (*k, *v))
                            );
                            prop_assert!(work.items - items <= 65 * 58);
                            let actual = history
                                .exact_position(query.to_be_bytes(), &mut work)
                                .map_err(TestCaseError::fail)?
                                .map(|position| *position.value);
                            prop_assert_eq!(actual, expected.get(&query).copied());
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
    Ok(())
}

fn persistent_physical_retirement() -> Result<(), &'static str> {
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
                    work.live_bytes(),
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
