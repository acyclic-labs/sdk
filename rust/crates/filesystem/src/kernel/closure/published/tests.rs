use super::{test_support::outcome, *};
use proptest::prelude::*;

fn check<const ORDERED: bool>(deliveries: &[(u16, bool)]) {
    let slot = Arc::new(ProofSlot::default());
    // All proofs finish before delivery. Their acknowledgements may then be
    // delayed or retried in any order, independently of durable sequence.
    let pending: Vec<_> = deliveries
        .iter()
        .map(|(sequence, retry)| {
            (
                Unpublished::new(slot.clone(), CollectionEpoch::read(None), *sequence),
                *sequence,
                *retry,
            )
        })
        .collect();
    let mut maximum = None;
    for (ticket, sequence, retry) in pending {
        let previous = slot.current(None);
        if ORDERED {
            ticket.publish(&outcome(u64::from(sequence), retry));
        } else {
            ticket.install::<false>(Sequence::new(u64::from(sequence)));
        }
        maximum = Some(maximum.map_or(sequence, |prior: u16| prior.max(sequence)));
        let current = slot.current(None);
        assert_eq!(
            current.as_deref().copied(),
            maximum,
            "proof slot regressed durable sequence"
        );
        if let Some(previous) = previous
            && sequence <= *previous
        {
            assert!(
                current
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(&previous, current)),
                "retry replaced the retained proof"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn delayed_and_retried_publications_preserve_the_newest_proof(
        deliveries in prop::collection::vec((any::<u16>(), any::<bool>()), 1..24)
    ) {
        check::<true>(&deliveries);
    }
}

#[test]
fn retrying_g1_after_g2_preserves_g2() {
    check::<true>(&[(1, false), (2, false), (1, true)]);
    check::<true>(&[(2, false), (1, false), (2, true)]);
}

#[test]
#[should_panic(expected = "proof slot regressed durable sequence")]
fn negative_control_detects_unconditional_install() {
    check::<false>(&[(1, false), (2, false), (1, true)]);
}
