use super::*;
use proptest::prelude::*;

/// The index-based loop every kernel search used before sharing one helper.
fn reference(
    items: &[u16],
    mut compare: impl FnMut(&u16) -> Ordering,
) -> (Result<usize, usize>, u64) {
    let (mut left, mut right, mut probes) = (0, items.len(), 0_u64);
    while left < right {
        probes += 1;
        let middle = left + (right - left) / 2;
        match items.get(middle).map(&mut compare) {
            Some(Ordering::Less) => left = middle + 1,
            Some(Ordering::Greater) => right = middle,
            Some(Ordering::Equal) => return (Ok(middle), probes),
            None => return (Err(usize::MAX), probes),
        }
    }
    (Err(left), probes)
}

fn sorted() -> impl Strategy<Value = Vec<u16>> {
    prop::collection::vec(0_u16..64, 0..80).prop_map(|mut items| {
        items.sort_unstable();
        items
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn binary_search_matches_reference_and_std(items in sorted(), key in 0_u16..70) {
        let counted = counted_binary_search(&items, |item| item.cmp(&key));
        prop_assert_eq!(counted, reference(&items, |item| item.cmp(&key)));
        match (counted.0, items.binary_search(&key)) {
            (Ok(found), Ok(_)) => prop_assert_eq!(items.get(found), Some(&key)),
            (Err(counted), Err(expected)) => prop_assert_eq!(counted, expected),
            (counted, expected) => prop_assert!(false, "{counted:?} != {expected:?}"),
        }
    }

    #[test]
    fn partition_point_matches_reference_and_std(
        items in sorted(),
        key in 0_u16..70,
        inclusive in any::<bool>(),
    ) {
        let before = |item: &u16| if inclusive { *item <= key } else { *item < key };
        let (index, probes) = counted_partition_point(&items, before);
        prop_assert_eq!(index, items.partition_point(before));
        let expected = reference(&items, |item| {
            if before(item) { Ordering::Less } else { Ordering::Greater }
        });
        prop_assert_eq!((Err(index), probes), expected);
    }
}
