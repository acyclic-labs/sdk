use crate::Adapter;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn calibration_only_tightens_and_matches_the_minimum_observed_ratio(
        observations in prop::collection::vec((1u32..65536, 1u32..65536), 0..32),
        bytes in 0u32..4096,
    ) {
        // The selection's calibration owner, as Registry::resolve builds it. It is
        // inline in the owner that holds it, so it allocates no separate owner.
        let cost = acyclic_test_support::measure(|| {
            crate::accounting::TokenEstimator::new(Adapter::Responses)
        });
        prop_assert_eq!(cost.count_total, 0, "calibration allocated a separate owner");
        let estimator = &cost.output;
        let mut ratio = 2.0f64;
        let mut previous = estimator.tokens(bytes as usize).unwrap();
        for (size, tokens) in observations {
            estimator.observe(size as usize, &json!({"usage":{"input_tokens":tokens}}));
            ratio = ratio.min(f64::from(size) / f64::from(tokens) * 0.9);
            let counted = estimator.tokens(bytes as usize).unwrap();
            prop_assert_eq!(f64::from(counted), (f64::from(bytes) / ratio).ceil());
            prop_assert!(counted >= previous);
            previous = counted;
        }
    }
}
