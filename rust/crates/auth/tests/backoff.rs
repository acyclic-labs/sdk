#[path = "../src/operation/backoff.rs"]
mod backoff;

use backoff::Backoff;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn indexed_ceiling_preserves_wide_configured_bounds(
        base in any::<u64>(),
        maximum in any::<u64>(),
        doublings in 0..=65_u32,
    ) {
        let expected = (u128::from(base) << doublings.min(64)).min(u128::from(maximum));
        prop_assert_eq!(
            u128::from(Backoff::ceiling_ms_at(base, maximum, doublings)),
            expected,
        );
    }

    #[test]
    fn fractional_projection_preserves_javascript_spacing(
        base in 0.0_f64..1.0e12,
        maximum in 0.0_f64..1.0e16,
        doublings in 0..=65_i32,
        draw in 0.0_f64..=1.0,
    ) {
        let upper = (base * 2.0_f64.powi(doublings.min(52))).min(maximum);
        let expected = (upper * (0.5 + draw / 2.0)).ceil();
        prop_assert_eq!(
            Backoff::fractional_delay_ms_at(base, maximum, doublings.unsigned_abs(), draw),
            expected,
        );
    }

    #[test]
    fn owner_draw_uses_the_full_equal_jitter_interval(
        base in any::<u32>(),
        maximum in any::<u32>(),
        sample in any::<u32>(),
    ) {
        let mut backoff = Backoff::new(base, maximum, 0);
        let mut ceilings = Backoff::new(base, maximum, 0);
        let initial = u128::from(base.min(maximum));
        for attempt in 0..=33 {
            let ceiling = (initial << attempt).min(u128::from(maximum));
            let lower = ceiling.div_ceil(2);
            let delay = backoff.delay_with(|choices| {
                assert_eq!(u128::from(choices), ceiling - lower + 1);
                sample
            });
            let expected = lower + u128::from(sample) % (ceiling - lower + 1);
            prop_assert_eq!(delay.as_millis(), expected);
            prop_assert_eq!(
                ceilings.delay_with(|choices| choices - 1).as_millis(),
                ceiling,
            );
        }
        backoff.reset();
        prop_assert_eq!(
            backoff.delay_with(|_| 0).as_millis(),
            initial.div_ceil(2),
        );
    }

    #[test]
    fn seeded_recovery_replays_and_reset_retains_entropy(
        base in 1_u32..=1_000_000,
        maximum in 1_u32..=u32::MAX,
        seed in any::<u32>(),
    ) {
        let mut first = Backoff::new(base, maximum, seed);
        let mut replay = Backoff::new(base, maximum, seed);
        let mut spread = Backoff::new(base, maximum, seed.wrapping_add(1));
        // A constant ceiling advances the same entropy without any resets.
        let mut entropy = Backoff::new(base.min(maximum), base.min(maximum), seed);
        let mut differs = false;
        for _ in 0..64 {
            let wait = first.delay_ms();
            entropy.delay_ms();
            prop_assert_eq!(wait, replay.delay_ms());
            let other = spread.delay_ms();
            differs |= other != wait;
        }
        if maximum > 1 {
            prop_assert!(differs);
        }
        first.reset();
        replay.reset();
        let reset = first.delay();
        prop_assert_eq!(reset, replay.delay());
        prop_assert_eq!(reset.as_millis(), u128::from(entropy.delay_ms()));
        for _ in 0..64 {
            first.reset();
            prop_assert_eq!(first.delay_ms(), entropy.delay_ms());
        }
    }
}
