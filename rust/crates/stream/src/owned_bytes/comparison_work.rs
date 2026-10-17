use super::*;
use proptest::prelude::*;

struct OriginalComparison(bool);
impl OriginalComparison {
    fn enter(old: bool) -> Self {
        Self(OLD_BYTE_COMPARE.replace(old))
    }
}
impl Drop for OriginalComparison {
    fn drop(&mut self) {
        OLD_BYTE_COMPARE.set(self.0);
    }
}

#[test]
fn ordered_prefixes_avoid_general_comparisons_at_n_and_two_n() {
    for length in [12, 24, 64, 128] {
        for late in [false, true] {
            if late && length < 24 {
                continue;
            }
            let a = vec![b'a'; length];
            let mut b = a.clone();
            b[if late { length - 1 } else { 8 }] = b'b';
            let expected = a.cmp(&b);
            let left = OwnedBytes::copy_from_slice(&a);
            let right = OwnedBytes::copy_from_slice(&b);
            for count in [1024, 2048] {
                for old in [false, true] {
                    let _restore = OriginalComparison::enter(old);
                    FALLBACK_COMPARISONS.set(0);
                    FALLBACK_BYTES.set(0);
                    let allocation = acyclic_test_support::measure(|| {
                        for _ in 0..count {
                            assert_eq!(left.cmp(&right), expected);
                        }
                    });
                    let calls = FALLBACK_COMPARISONS.get();
                    let bytes = FALLBACK_BYTES.get();
                    let expected_calls = if late { count } else { 0 };
                    let expected_bytes = if late { (length - 16) * count } else { 0 };
                    assert_eq!(calls, if old { count } else { expected_calls });
                    assert_eq!(bytes, if old { length * count } else { expected_bytes });
                    let bounded = calls == expected_calls && bytes == expected_bytes;
                    assert_eq!(std::panic::catch_unwind(|| assert!(bounded)).is_ok(), !old);
                    assert_eq!(allocation.count_total, 0);
                    assert_eq!(allocation.bytes_total, 0);
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Integer prefixes must preserve byte ordering, including common prefixes,
    // embedded zeroes, partial words, unequal lengths, and retained slices.
    #[test]
    fn word_comparisons_equal_independent_byte_order(
        prefix in prop::collection::vec(any::<u8>(), 0..65),
        a in prop::collection::vec(any::<u8>(), 0..65),
        b in prop::collection::vec(any::<u8>(), 0..65),
        cut in 0usize..129,
    ) {
        let a = [prefix.as_slice(), a.as_slice()].concat();
        let b = [prefix.as_slice(), b.as_slice()].concat();
        let left = OwnedBytes::from(a.clone());
        let right = OwnedBytes::copy_from_slice(&b);
        let end = cut.min(a.len());
        let short = left.slice(..end);
        acyclic_test_support::differential(
            || (a.clone(), b.clone(), a.cmp(&b), a[..end].cmp(&b)),
            || (left.to_vec(), right.to_vec(), left.cmp(&right), short.cmp(&right)),
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn shared_bytes_keep_complete_order_equality_and_content(a in prop::collection::vec(any::<u8>(), 0..257), b in prop::collection::vec(any::<u8>(), 0..257), from in 0usize..257, to in 0usize..257) {
        let left = OwnedBytes::copy_from_slice(&a);
        let right = OwnedBytes::copy_from_slice(&b);
        let start = from.min(a.len());
        let end = start.max(to.min(a.len()));
        let alias = left.slice(start..end);
        let expected = a[start..end].to_vec();
        let _ = acyclic_test_support::differential(
            || (a.clone(), b.clone(), a.cmp(&b), expected.cmp(&a), expected == a),
            || (left.to_vec(), right.to_vec(), left.cmp(&right), alias.cmp(&left), alias == left),
        );
        let clone = alias.clone();
        BYTE_COMPARISONS.with(|count| count.set(0));
        let allocations = acyclic_test_support::measure(|| {
            assert_eq!(alias.cmp(&clone), Ordering::Equal);
            assert!(alias == clone);
        });
        prop_assert_eq!(BYTE_COMPARISONS.with(Cell::get), 0);
        prop_assert_eq!(allocations.count_total, 0);
        prop_assert_eq!(allocations.bytes_total, 0);
    }
}

#[test]
fn shared_path_comparison_control_reintroduces_the_physical_comparison() {
    let path = OwnedBytes::copy_from_slice(b"swarm/agent-0000000000000000/files/status.json");
    let clone = path.clone();
    for redundant in [false, true] {
        BYTE_COMPARISONS.with(|count| count.set(0));
        let allocations = acyclic_test_support::measure(|| {
            assert_eq!(path.cmp(&clone), Ordering::Equal);
            if redundant {
                // Same production comparator, with the old redundant byte work.
                assert_eq!(
                    compare_bytes(path.as_slice(), clone.as_slice()),
                    Ordering::Equal
                );
            }
        });
        let comparisons = BYTE_COMPARISONS.with(Cell::get);
        assert_eq!(comparisons, usize::from(redundant));
        assert_eq!(
            comparisons == 0,
            !redundant,
            "physical comparison control escaped"
        );
        assert_eq!(allocations.count_total, 0);
        assert_eq!(allocations.bytes_total, 0);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn ordinary_views_do_not_recheck_the_bytes_range(data in prop::collection::vec(any::<u8>(), 0..257), from in 0usize..257, to in 0usize..257) {
        let start = from.min(data.len());
        let end = start.max(to.min(data.len()));
        let ordinary = OwnedBytes::copy_from_slice(&data).slice(start..end);
        let retained = OwnedBytes::from(data.clone()).slice(start..end);
        for (bytes, expected_checks) in [(&ordinary, 0), (&retained, 1)] {
            RANGE_CHECKS.with(|count| count.set(0));
            let allocation = acyclic_test_support::measure(|| {
                assert_eq!(bytes.as_slice(), &data[start..end]);
            });
            prop_assert_eq!(RANGE_CHECKS.with(Cell::get), expected_checks);
            prop_assert_eq!(allocation.count_total, 0);
            prop_assert_eq!(allocation.bytes_total, 0);
        }
        // Reintroduce the old projection with unchanged ordinary view bytes.
        let Data::Plain(backing) = &ordinary.data else { unreachable!("ordinary fixture") };
        RANGE_CHECKS.with(|count| count.set(0));
        prop_assert_eq!(checked_range(backing.as_ref(), ordinary.range.clone()), ordinary.as_slice());
        prop_assert_eq!(RANGE_CHECKS.with(Cell::get), 1);
    }
}
