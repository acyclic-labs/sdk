use crate::Adapter;
use crate::accounting::*;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn calibration_only_tightens_and_matches_the_minimum_observed_ratio(
        observations in prop::collection::vec((1usize..65536, 1u32..65536), 0..32),
        bytes in 0usize..4096,
    ) {
        let estimator = TokenEstimator::new(Adapter::Responses);
        let mut ratio = 2.0f64;
        let mut previous = estimator.tokens(bytes).unwrap();
        for (size, tokens) in observations {
            estimator.observe(size, &json!({"usage":{"input_tokens":tokens}}));
            ratio = ratio.min(size as f64 / f64::from(tokens) * 0.9);
            let counted = estimator.tokens(bytes).unwrap();
            prop_assert_eq!(counted, (bytes as f64 / ratio).ceil() as u32);
            prop_assert!(counted >= previous);
            previous = counted;
        }
    }
}
