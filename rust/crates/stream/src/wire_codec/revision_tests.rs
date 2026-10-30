use super::*;
use proptest::prelude::*;

fn revision(scope: u8, incarnation: u128, index: u64) -> Revision {
    Revision {
        scope: RevisionScope::new(vec![scope]),
        incarnation,
        index,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn borrowed_minimum_proof_matches_canonical_decoding(
        entries in prop::collection::vec((0_u8..4, 0_u128..4, any::<u64>()), 0..24),
        applied in prop::option::of((0_u8..4, 0_u128..4, any::<u64>())),
        malformed in any::<bool>(),
    ) {
        let mut minimum: Vec<_> = entries.into_iter()
            .map(|(scope, incarnation, index)| revision_to_wire(&revision(scope, incarnation, index)))
            .collect();
        if malformed && let Some(first) = minimum.first_mut() {
            first.incarnation = first.incarnation.slice(..15);
        }
        let applied = applied.map(|(scope, incarnation, index)| revision(scope, incarnation, index));
        let canonical = cut_from_wire(minimum.clone())
            .is_ok_and(|minimum| minimum.proven_by(applied.as_ref()));
        prop_assert_eq!(cut_proven_by(&minimum, applied.as_ref()), canonical);
    }
}

#[test]
fn borrowed_minimum_proof_avoids_decoding_work() {
    let applied = revision(1, 7, 64);
    for count in [0, 1, 8, 64] {
        let minimum: Vec<_> = (0..count)
            .rev()
            .map(|index| {
                revision_to_wire(&Revision {
                    index,
                    ..applied.clone()
                })
            })
            .collect();
        for applied in [None, Some(&applied)] {
            let mut canonical = false;
            // Executable negative control: restoring decode-and-canonicalize
            // must spend allocations on the very same retained wire input.
            let before = acyclic_test_support::measure(|| {
                canonical =
                    cut_from_wire(minimum.clone()).is_ok_and(|minimum| minimum.proven_by(applied));
            });
            let mut borrowed = false;
            let after = acyclic_test_support::measure(|| {
                borrowed = cut_proven_by(&minimum, applied);
            });
            assert_eq!(borrowed, canonical);
            assert_eq!(after.count_total, 0);
            assert_eq!(after.bytes_total, 0);
            if count > 0 {
                assert!(before.count_total >= count + 2);
            }
            eprintln!(
                "minimum_entries={count} applied={} allocations={}->{} bytes={}->{}",
                applied.is_some(),
                before.count_total,
                after.count_total,
                before.bytes_total,
                after.bytes_total,
            );
        }
    }
}
