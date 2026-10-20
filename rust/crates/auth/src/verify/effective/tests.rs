//! Count caveat visits on the production iterator, independently of timing.
use super::*;
use crate::fixtures::signed;
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

fn work(cap: &VerifiedCapability, broken: bool) -> usize {
    FILTER_WORK.with(|work| work.set(0));
    for clause in cap.effective().filters() {
        assert_eq!(clause.subject_id(), "issuer");
        assert_eq!(clause.lineage().collect::<Vec<_>>(), vec!["issuer"]);
        if broken {
            // Restore the former two descendant scans per filter. Count the
            // same caveat visits as the production traversal above.
            for _ in 0..2 {
                for block in cap.blocks.iter().skip(clause.declaring_block() + 1) {
                    for _ in &block.caveats {
                        filter_work();
                    }
                }
            }
        }
    }
    FILTER_WORK.with(|work| work.get())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn filter_projection_work_is_linear(count in 8..128usize) {
        let small = holder(count);
        let large = holder(count * 2);
        prop_assert_eq!(work(&large, false), work(&small, false) * 2);
        prop_assert!(work(&large, true) > work(&small, true) * 3);
    }
}

#[test]
fn thousands_of_filters_double_the_work() {
    assert_eq!(work(&holder(4096), false), work(&holder(2048), false) * 2);
}
