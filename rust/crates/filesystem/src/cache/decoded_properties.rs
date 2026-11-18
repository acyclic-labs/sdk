use super::decoded_fuzz::{check, check_inner};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn decoded_histories_preserve_original_misses(input in prop::collection::vec(any::<u8>(), 0..128)) {
        check(&input).expect("decoded cache oracle");
    }
}

#[test]
fn decoded_corpus_and_negative_controls_cover_every_transition() {
    let corpus: &[&[u8]] = &[
        include_bytes!("../../fuzz/corpus/decoded_cache/identity"),
        include_bytes!("../../fuzz/corpus/decoded_cache/eviction"),
        include_bytes!("../../fuzz/corpus/decoded_cache/stale-fill"),
        include_bytes!("../../fuzz/corpus/decoded_cache/overlapping-fill"),
        include_bytes!("../../fuzz/corpus/decoded_cache/oversized"),
    ];
    let coverage = corpus.iter().fold(0, |coverage, input| {
        coverage | check(input).expect("decoded cache oracle")
    });
    assert_eq!(
        coverage, 0xff,
        "retained corpus must reach every oracle branch"
    );
    for (fault, input) in [(1, corpus[0]), (2, corpus[1]), (3, corpus[2])] {
        let failure = match fault {
            1 => check_inner::<1>(input),
            2 => check_inner::<2>(input),
            _ => check_inner::<3>(input),
        }
        .expect_err("planted cache defect survived the unchanged oracle");
        let message = failure.to_string();
        assert!(message.contains("decoded cache"), "{message}");
    }
}
