//! Independent order-statistic admission checks for the original report constructor.
#[path = "../../crates/conformance/benches/local_hotpath/quantiles.rs"]
mod quantiles;

use proptest::prelude::*;
use quantiles::Quantiles;

proptest! {
    #[test]
    fn original_quantiles_admit_exactly_valid_cohorts_and_keep_nearest_ranks(
        bits in prop::collection::vec(0_u64..0x7ff0_0000_0000_0000, 0..128),
        zero_cohort in any::<bool>(),
        negative_zero in any::<bool>(),
        invalid in prop::option::of(prop_oneof![
            Just(f64::NAN), Just(f64::INFINITY), Just(f64::NEG_INFINITY), Just(-f64::MIN_POSITIVE)
        ]),
        insert_at in any::<usize>(),
    ) {
        let mut values: Vec<_> = bits.into_iter().map(f64::from_bits).collect();
        if zero_cohort {
            values.fill(if negative_zero { -0.0 } else { 0.0 });
        }
        if let Some(invalid) = invalid {
            values.insert(insert_at % (values.len() + 1), invalid);
        }
        let valid = !values.is_empty() && invalid.is_none();
        let original = values.clone();
        let result = Quantiles::new(&mut values);
        prop_assert_eq!(result.is_ok(), valid);
        if let Ok(result) = result {
            prop_assert_eq!(result.samples, original.len());
            for (percent, observed) in [(50, result.p50_ns), (99, result.p99_ns)] {
                // Definition of nearest rank, independently of the production
                // sort, integer division and selected array index. Ties and
                // signed zero are legitimate equal observations.
                let below = original.iter().filter(|value| **value < observed).count();
                let through = original.iter().filter(|value| **value <= observed).count();
                prop_assert!(below * 100 < percent * original.len());
                prop_assert!(through * 100 >= percent * original.len());
                prop_assert!(original.contains(&observed));
            }
        }
    }
}
