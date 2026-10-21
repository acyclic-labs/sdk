use super::*;

// The bounds include only this child's result keys; siblings are unreachable.
pub(super) fn evict(values: &mut BTreeMap<FileKey, FileValue>, reference: &Component, turn: u64) {
    let key = |turn| FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Result(turn),
    };
    while let Some(previous) = values
        .range(key(0)..key(turn))
        .next()
        .map(|(key, _)| key.clone())
    {
        #[cfg(test)]
        acyclic_test_support::differential::record(("result-fold-visit", 1u64));
        values.remove(&previous);
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use acyclic_test_support::differential;
    use proptest::prelude::*;

    fn run(count: usize) -> [(BTreeMap<FileKey, FileValue>, Vec<(&'static str, u64)>); 2] {
        differential::pair_of(
            &["result-fold-visit"],
            || {
                (0..count)
                    .map(|index| {
                        let reference = Component::new(&format!("child-{index:04}")).unwrap();
                        let key = FileKey::Child {
                            reference,
                            file: ChildFile::Result(1),
                        };
                        (key.clone(), FileValue::new(key, b"first".to_vec()))
                    })
                    .collect::<BTreeMap<_, _>>()
            },
            |mut values, old| {
                for index in 0..count {
                    let reference = Component::new(&format!("child-{index:04}")).unwrap();
                    if old {
                        values.retain(|key, _| {
                            differential::record(("result-fold-visit", 1u64));
                            !matches!(key, FileKey::Child { reference: r, file: ChildFile::Result(n) } if r == &reference && *n < 2)
                        });
                    }
                    let key = FileKey::Child {
                        reference,
                        file: ChildFile::Result(2),
                    };
                    let file = FileValue::new(key.clone(), b"second".to_vec());
                    if old {
                        values.insert(key, file);
                    } else {
                        super::super::fold(
                            &mut values,
                            Change {
                                file: Some(file),
                                files: Vec::new(),
                                removed: Vec::new(),
                                cursor: crate::task_volume::Cursor {
                                    volume: [0; 32],
                                    epoch: uuid::Uuid::nil(),
                                    schema: 0,
                                    query: [0; 32],
                                    position: 0,
                                    accepted: 0,
                                    durable: 0,
                                    commit: [0; 32],
                                },
                            },
                        );
                    }
                }
                values
            },
        )
    }

    fn visits(trace: &[(&str, u64)]) -> u64 {
        trace.iter().map(|(_, amount)| amount).sum()
    }
    fn linear(work: u64, count: u64, check: bool) {
        if check {
            assert_eq!(work, count, "result folding revisited siblings");
        }
    }

    #[test]
    fn completing_twice_as_many_siblings_doubles_actual_fold_visits() {
        let small = run(16);
        let large = run(32);
        assert_eq!(small[0].0, small[1].0);
        assert_eq!(large[0].0, large[1].0);
        linear(visits(&small[1].1), 16, true);
        linear(visits(&large[1].1), 32, true);
        assert_eq!(visits(&large[1].1), 2 * visits(&small[1].1));
        assert_eq!(visits(&small[0].1), 16 * 16);
        assert_eq!(visits(&large[0].1), 32 * 32);
        for (pair, count) in [(&small, 16), (&large, 32)] {
            let work = visits(&pair[0].1);
            std::panic::catch_unwind(|| linear(work, count, true))
                .expect_err("quadratic result folding escaped the work assertion");
            linear(work, count, false);
        }
    }
    proptest! {
        #[test]
        fn result_range_preserves_every_sibling_and_newer_turn(
            children in 1usize..12, chosen in 0usize..12, turn in 0u64..8
        ) {
            let mut values = BTreeMap::new();
            for child in 0..children {
                for revision in 0..8 {
                    let key = FileKey::Child {
                        reference: Component::new(&format!("child-{child}")).unwrap(),
                        file: ChildFile::Result(revision),
                    };
                    values.insert(key.clone(), FileValue::new(key, vec![revision as u8]));
                }
            }
            let reference = Component::new(&format!("child-{}", chosen % children)).unwrap();
            let mut expected = values.clone();
            expected.retain(|key, _| !matches!(key, FileKey::Child { reference: r, file: ChildFile::Result(n) } if r == &reference && *n < turn));
            evict(&mut values, &reference, turn);
            prop_assert_eq!(values, expected);
        }
    }
}
