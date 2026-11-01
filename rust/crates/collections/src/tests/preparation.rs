use super::*;

// Retains the original issuing work; only the index-owned bound is consumed.
struct Bounded<'a> {
    original: &'a mut Work,
    remaining: IndexInsertionBound,
}

impl IndexWork for Bounded<'_> {
    type Error = &'static str;
    type Allocation = Allocation;

    fn check(&mut self) -> Result<(), Self::Error> {
        self.original.check()
    }

    fn charge_items(&mut self, items: u64) -> Result<(), Self::Error> {
        let remaining = self
            .remaining
            .examined_items
            .checked_sub(items)
            .ok_or("bound items refused")?;
        self.original.charge_items(items)?;
        self.remaining.examined_items = remaining;
        Ok(())
    }

    fn claim(&mut self, bytes: u64) -> Result<Allocation, Self::Error> {
        let remaining = self
            .remaining
            .allocation_bytes
            .checked_sub(bytes)
            .ok_or("bound bytes refused")?;
        let allocation = self.original.claim(bytes)?;
        self.remaining.allocation_bytes = remaining;
        Ok(allocation)
    }
}

#[test]
fn mutable_insertion_bounds_cover_original_backing_refusal_and_unrelated_population()
-> Result<(), &'static str> {
    use proptest::test_runner::{Config, RngSeed, TestRunner};
    let mut runner = TestRunner::new(Config {
        cases: 128,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(0x7072_6570_6172_6564),
        ..Config::default()
    });
    runner
        .run(&prop::collection::vec(any::<u64>(), 1..96), |keys| {
            let mut original = Work::default();
            let mut index = FixedIndex::<usize, _, 8>::default();
            let mut expected = BTreeMap::new();
            let mut work = Bounded {
                original: &mut original,
                remaining: FixedIndex::<usize, Allocation, 8>::insertion_bound(keys.len() as u64)
                    .unwrap(),
            };
            for (value, key) in keys.into_iter().enumerate() {
                let observed = index
                    .get_or_try_insert_with(key.to_be_bytes(), &mut work, |_| Ok(value))
                    .map_err(TestCaseError::fail)?;
                prop_assert_eq!(*observed, *expected.entry(key).or_insert(value));
            }
            audit(&index);
            for (key, value) in expected {
                prop_assert_eq!(
                    index
                        .get(key.to_be_bytes(), &mut original)
                        .map_err(TestCaseError::fail)?,
                    Some(&value)
                );
            }
            drop(index);
            prop_assert_eq!(original.live.get(), 0);
            Ok(())
        })
        .map_err(|error| {
            eprintln!("{error}");
            "mutable insertion exceeded its owned bound or changed exact contents"
        })?;

    fn coordinates<const N: usize>() -> Result<(), &'static str> {
        let mut original = Work {
            observe_retirement: true,
            ..Work::default()
        };
        let mut result: Result<(), &'static str> = Ok(());
        let physical = acyclic_test_support::measure(|| {
            result = (|| {
                let mut index = FixedIndex::<usize, _, N>::default();
                let bound = FixedIndex::<usize, Allocation, N>::insertion_bound(1).unwrap();
                index.get_or_try_insert_with(
                    [0; N],
                    &mut Bounded {
                        original: &mut original,
                        remaining: bound,
                    },
                    |_| Ok(0),
                )?;
                for byte in (0..N).rev() {
                    for bit in (0..8).rev() {
                        let mut key = [0; N];
                        key[byte] = 0x80 >> bit;
                        index.get_or_try_insert_with(
                            key,
                            &mut Bounded {
                                original: &mut original,
                                remaining: bound,
                            },
                            |_| Ok(1),
                        )?;
                    }
                }
                // Existing-key traversal also consumes the same public bound,
                // but never funds another node or runs a value constructor.
                index.get_or_try_insert_with(
                    [0; N],
                    &mut Bounded {
                        original: &mut original,
                        remaining: bound,
                    },
                    |_| panic!("existing constructor"),
                )?;
                assert_eq!(
                    original.live.get() as i64,
                    acyclic_test_support::current_allocations()
                        .unwrap()
                        .bytes_current
                );
                original.cancelled = true;
                assert_eq!(
                    index
                        .get_or_try_insert_with(
                            [255; N],
                            &mut Bounded {
                                original: &mut original,
                                remaining: bound
                            },
                            |_| panic!("cancelled constructor")
                        )
                        .err(),
                    Some("cancelled")
                );
                drop(index);
                Ok(())
            })();
        });
        result?;
        assert_eq!(physical.bytes_current, 0);
        assert_eq!(original.live.get(), 0);
        assert!(!original.early_refund.get());
        Ok(())
    }
    coordinates::<0>()?;
    coordinates::<8>()?;
    coordinates::<40>()?;

    let mut observations = [0; 2];
    let mut ordered_comparisons = [0; 2];
    for (row, population) in [1_u64, 100_000].into_iter().enumerate() {
        let mut original = Work::default();
        let mut index = FixedIndex::<usize, _, 9>::default();
        index.get_or_try_insert_with([0; 9], &mut original, |_| Ok(7))?;
        for foreign in (0..population).rev() {
            let mut key = [255; 9];
            key[1..].copy_from_slice(&foreign.to_be_bytes());
            index.get_or_try_insert_with(key, &mut original, |_| Ok(9))?;
        }
        let mut key = [0; 9];
        key[8] = 1;
        let bound = FixedIndex::<usize, Allocation, 9>::insertion_bound(1).unwrap();
        let live = original.live.get();
        let claims = original.claims;
        // One byte short of the two actual node claims refuses before the
        // constructor or publication, refunding the first original claim.
        let mut short = bound;
        short.allocation_bytes -= 1;
        assert_eq!(
            index
                .get_or_try_insert_with(
                    key,
                    &mut Bounded {
                        original: &mut original,
                        remaining: short
                    },
                    |_| panic!("unfunded constructor")
                )
                .err(),
            Some("bound bytes refused")
        );
        assert_eq!(original.live.get(), live);
        assert_eq!(index.get(key, &mut original)?, None);
        assert_eq!(original.claims, claims + 1);
        let before = original.items;
        index.get_or_try_insert_with(
            key,
            &mut Bounded {
                original: &mut original,
                remaining: bound,
            },
            |_| Ok(11),
        )?;
        observations[row] = original.items - before;
        assert_eq!(original.live.get() - live, bound.allocation_bytes);
        // Restoring an ordered insertion pays comparisons against unrelated
        // keys. The independent comparison sink exposes that forbidden slope.
        #[derive(Clone)]
        struct Ordered(u64, Rc<Cell<u64>>);
        impl PartialEq for Ordered {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl Eq for Ordered {}
        impl PartialOrd for Ordered {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for Ordered {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.1.set(self.1.get() + 1);
                self.0.cmp(&other.0)
            }
        }
        let comparisons = Rc::new(Cell::new(0));
        let mut ordered = BTreeMap::new();
        for foreign in (1..=population).rev() {
            ordered.insert(Ordered(foreign, comparisons.clone()), ());
        }
        comparisons.set(0);
        ordered.insert(Ordered(0, comparisons.clone()), ());
        ordered_comparisons[row] = comparisons.get();
        drop(index);
        assert_eq!(original.live.get(), 0);
    }
    assert_eq!(observations[0], observations[1]);
    assert!(ordered_comparisons[1] > ordered_comparisons[0]);
    eprintln!(
        "mutable insertion items={observations:?}; ordered negative comparisons={ordered_comparisons:?}"
    );
    assert!(FixedIndex::<usize, Allocation, 8>::insertion_bound(u64::MAX).is_none());
    Ok(())
}
