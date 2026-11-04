//! Count caveat visits on the production iterator, independently of timing.
use super::*;
use crate::testing as signed;
use proptest::prelude::*;

fn holder(count: usize) -> VerifiedCapability {
    let mut cap = verify(
        &signed::root(|_| {}),
        &signed::Snapshot::default(),
        signed::NOW,
    )
    .unwrap();
    for _ in 0..count {
        cap.blocks.push(Block {
            certificates: None,
            next_public_key: vec![0; 32],
            caveats: vec![Caveat {
                kind: CaveatKind::Filter(Filter {
                    keys: vec![],
                    attributes: vec![],
                }),
            }],
        });
    }
    cap
}

fn work(cap: &VerifiedCapability) -> usize {
    FILTER_WORK.with(|work| work.set(0));
    for clause in cap.effective().filters() {
        assert_eq!(clause.subject_id(), "issuer");
        assert_eq!(clause.lineage().collect::<Vec<_>>(), vec!["issuer"]);
    }
    FILTER_WORK.with(|work| work.get())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn filter_projection_work_is_linear(count in 8..128usize) {
        let small = holder(count);
        let large = holder(count * 2);
        prop_assert_eq!(work(&large), work(&small) * 2);
    }
}
