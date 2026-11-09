use super::*;

#[test]
fn ordered_seek_and_mutation_preserve_original_credit_and_sparse_cursors()
-> Result<(), &'static str> {
    ordered_generated_histories()?;
    ordered_boundary_shapes()?;
    let mut work = ordered_retained_generations()?;
    recent_read_only_refusals(&mut work)?;
    Ok(())
}

fn ordered_generated_histories() -> Result<(), &'static str> {
    use proptest::test_runner::{Config, RngSeed, TestRunner};

    let mut runner = TestRunner::new(Config {
        cases: 128,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(0x7265_6164_2d69_6e64),
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
                    verify_ordered_query(&mut table, &mut expected, key, &mut work)?;
                }
                let mut row = table
                    .first(&mut ReadOnly(&mut work))
                    .map_err(TestCaseError::fail)?;
                for expected in &expected {
                    prop_assert_eq!(row, Some(expected));
                    row = table
                        .seek_after(*expected.0, &mut ReadOnly(&mut work))
                        .map_err(TestCaseError::fail)?;
                }
                prop_assert_eq!(row, None);
                prop_assert_eq!(work.claims, claims);
                prop_assert_eq!(work.live.get(), live);
                for key in expected.keys().copied().collect::<Vec<_>>() {
                    let before = work.items;
                    let entry = table
                        .remove(key, &mut ReadOnly(&mut work))
                        .map_err(TestCaseError::fail)?;
                    prop_assert_eq!(
                        entry.as_ref().map(|entry| (*entry.key(), *entry.value())),
                        expected.remove(&key).map(|value| (key, value))
                    );
                    prop_assert!(work.items - before <= 17 * 40 + 1);
                    drop(entry);
                    audit(&table);
                }
                prop_assert_eq!(work.claims, claims);
                drop(table);
                prop_assert_eq!(work.live.get(), 0);
                Ok(())
            },
        )
        .map_err(|_| "ordered seek or existing mutation disagreed with independent map")?;
    Ok(())
}

fn verify_ordered_query(
    table: &mut FixedIndex<usize, Allocation, 40>,
    expected: &mut BTreeMap<[u8; 40], usize>,
    key: [u8; 40],
    work: &mut Work,
) -> TestCaseResult {
    use std::ops::Bound::{Excluded, Unbounded};
    let before = work.items;
    let observed = table
        .seek_after(key, &mut ReadOnly(work))
        .map_err(TestCaseError::fail)?;
    prop_assert_eq!(observed, expected.range((Excluded(key), Unbounded)).next());
    prop_assert!(work.items - before <= 25 * 40 + 3);
    let before = work.items;
    let observed = table
        .get_mut(key, &mut ReadOnly(work))
        .map_err(TestCaseError::fail)?;
    prop_assert_eq!(observed.as_deref(), expected.get(&key));
    if let Some(value) = observed {
        *value += 1;
        *expected
            .get_mut(&key)
            .ok_or_else(|| TestCaseError::fail("mutable original key is missing"))? += 1;
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
        *expected
            .get_mut(&key)
            .ok_or_else(|| TestCaseError::fail("retained original key is missing"))? += 1;
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
    Ok(())
}

fn ordered_boundary_shapes() -> Result<(), &'static str> {
    // Every possible branch coordinate, including beyond 256 bits, and the
    // empty key exercise the structural bound independently of random shape.

    boundary::<0>()?;
    boundary::<8>()?;
    boundary::<24>()?;
    boundary::<40>()?;
    Ok(())
}

fn ordered_retained_generations() -> Result<Work, &'static str> {
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
    Ok(work)
}

fn recent_read_only_refusals(work: &mut Work) -> Result<(), &'static str> {
    // The shared recent-fact composition must preserve the same read-only
    // admission when a miss in its current window searches the previous one.
    work.cancelled = false;
    let mut recent = RecentIndex::<_, _, 8, 2>::default();
    for key in 0_u64..5 {
        recent.get_or_try_insert_with(key.to_be_bytes(), work, |_| Ok(key))?;
    }
    let claims = work.claims;
    let live = work.live.get();
    for (key, expected) in [(0, None), (2, Some(2)), (4, Some(4)), (u64::MAX, None)] {
        let before = work.items;
        assert_eq!(
            recent.get(key.to_be_bytes(), &mut ReadOnly(work))?,
            expected.as_ref()
        );
        let required = work.items - before;
        assert!(required <= 2 * (9 * 8 + 1));
        for credit in 0..required {
            work.maximum_items = Some(work.items + credit);
            assert_eq!(
                recent.get(key.to_be_bytes(), &mut ReadOnly(work)).err(),
                Some("items refused"),
                "recent window refusal must not become a cache miss"
            );
        }
        work.maximum_items = None;
        work.cancelled = true;
        assert_eq!(
            recent.get(key.to_be_bytes(), &mut ReadOnly(work)).err(),
            Some("cancelled")
        );
        work.cancelled = false;
    }
    assert_eq!(work.claims, claims);
    assert_eq!(work.live.get(), live);
    drop(recent);
    assert_eq!(work.live.get(), 0);
    Ok(())
}

fn boundary<const N: usize>() -> Result<(), &'static str> {
    use std::ops::Bound::{Excluded, Unbounded};
    let mut work = Work::default();
    let mut table = FixedIndex::default();
    let mut expected = BTreeMap::new();
    table.get_or_try_insert_with([0; N], &mut work, |_| Ok(0))?;
    expected.insert([0; N], 0);
    for byte in 0..N {
        for offset in 0..8 {
            let mut key = [0; N];
            *key.get_mut(byte)
                .ok_or("branch byte exceeds original key")? = 0x80 >> offset;
            table.get_or_try_insert_with(key, &mut work, |_| Ok(1))?;
            expected.insert(key, 1);
        }
    }
    let claims = work.claims;
    for key in expected.keys().copied().chain([[255; N]]) {
        let before = work.items;
        assert_eq!(
            table.seek_after(key, &mut ReadOnly(&mut work))?,
            expected.range((Excluded(key), Unbounded)).next()
        );
        assert!(work.items - before <= (25 * N + 3) as u64);
    }

    boundary_read_refusals(&mut table, &mut work)?;
    boundary_removal_and_cancelled_custody(table, &mut work, claims)
}

fn boundary_read_refusals<const N: usize>(
    table: &mut FixedIndex<usize, Allocation, N>,
    work: &mut Work,
) -> Result<(), &'static str> {
    let before = work.items;
    table
        .get_mut([0; N], &mut ReadOnly(work))?
        .ok_or("missing original key")?;
    let required = work.items - before;
    for credit in 0..required {
        work.maximum_items = Some(work.items + credit);
        assert_eq!(
            table.get_mut([0; N], &mut ReadOnly(work)).err(),
            Some("items refused")
        );
    }
    work.maximum_items = None;
    assert_eq!(table.get([0; N], &mut ReadOnly(work))?, Some(&0));
    let before = work.items;
    table.seek_after([0; N], &mut ReadOnly(work))?;
    let required = work.items - before;
    for credit in 0..required {
        work.maximum_items = Some(work.items + credit);
        assert_eq!(
            table.seek_after([0; N], &mut ReadOnly(work)).err(),
            Some("items refused")
        );
    }
    work.maximum_items = None;
    Ok(())
}

fn boundary_removal_and_cancelled_custody<const N: usize>(
    mut table: FixedIndex<usize, Allocation, N>,
    work: &mut Work,
    claims: u64,
) -> Result<(), &'static str> {
    let held = work.live.get();
    // The zero key traverses every actual branch coordinate in this tree.
    // Refusal at any point, including the second walk, must preserve it.
    for credit in 0..(17 * N + 1) as u64 {
        work.maximum_items = Some(work.items + credit);
        assert_eq!(
            table.remove([0; N], &mut ReadOnly(work)).err(),
            Some("items refused")
        );
        work.maximum_items = None;
        assert_eq!(
            table.get([0; N], &mut ReadOnly(work))?,
            Some(&0),
            "refused removal must preserve original owner"
        );
        assert_eq!(work.live.get(), held);
    }
    work.cancelled = true;
    assert_eq!(
        table.remove([0; N], &mut ReadOnly(work)).err(),
        Some("cancelled")
    );
    assert_eq!(
        table.get_mut([255; N], &mut ReadOnly(work)).err(),
        Some("cancelled")
    );
    assert_eq!(
        table.seek_after([255; N], &mut ReadOnly(work)).err(),
        Some("cancelled")
    );
    let mut empty = FixedIndex::<usize, Allocation, N>::default();
    assert_eq!(
        empty.remove([0; N], &mut ReadOnly(work)).err(),
        Some("cancelled")
    );
    assert_eq!(
        empty.get_mut([0; N], &mut ReadOnly(work)).err(),
        Some("cancelled")
    );
    assert_eq!(
        empty.get([0; N], &mut ReadOnly(work)).err(),
        Some("cancelled"),
        "cancelled empty exact read must be refused"
    );
    assert_eq!(empty.first(&mut ReadOnly(work)).err(), Some("cancelled"));
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
    assert_eq!(
        empty.seek_after([0; N], &mut ReadOnly(work)).err(),
        Some("cancelled")
    );
    assert!(
        empty
            .get_or_try_insert_with([0; N], work, |_| panic!("cancelled construction"))
            .is_err()
    );
    assert_eq!(work.claims, claims);
    work.cancelled = false;
    work.maximum_items = None;
    let before = work.items;
    let entry = table
        .remove([0; N], &mut ReadOnly(work))?
        .ok_or("original owner missing after refused removals")?;
    assert_eq!((*entry.key(), *entry.value()), ([0; N], 2));
    assert_eq!(work.items - before, (17 * N + 1) as u64);
    drop(entry);
    assert_eq!(table.get([0; N], &mut ReadOnly(work))?, None);
    assert_eq!(work.claims, claims);
    drop(table);
    assert_eq!(work.live.get(), 0);
    Ok(())
}
