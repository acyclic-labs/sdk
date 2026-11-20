//! Properties of transport backoff bounds, jitter, and reset behavior.

use acyclic_auth::operation::Backoff;
use proptest::prelude::*;

fn finite_milliseconds() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.0),
        Just(f64::from_bits(1)),
        Just(f64::MAX),
        (0..=f64::MAX.to_bits()).prop_map(f64::from_bits),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn indexed_ceiling_preserves_wide_configured_bounds(
        base in prop_oneof![Just(0_u64), 0..64_u64, any::<u64>()],
        maximum in any::<u64>(),
    ) {
        for doublings in 0..=65 {
            let expected = (u128::from(base) << doublings.min(64)).min(u128::from(maximum));
            prop_assert_eq!(
                u128::from(Backoff::ceiling_ms_at(base, maximum, doublings)),
                expected,
            );
        }
        prop_assert_eq!(
            Backoff::ceiling_ms_at(base, maximum, u32::MAX),
            if base == 0 { 0 } else { maximum },
        );
    }

    #[test]
    fn reconnect_projection_preserves_additive_jitter(
        ceiling in prop_oneof![Just(0_u64), Just(u64::MAX), any::<u64>()],
        sample in any::<u64>(),
    ) {
        let upper = u128::from(ceiling);
        let expected = (upper + u128::from(sample) % (upper / 4 + 1))
            .min(u128::from(u64::MAX));
        prop_assert_eq!(
            u128::from(Backoff::reconnect_delay_ms(ceiling, sample)),
            expected,
        );
    }

    #[test]
    fn fractional_projection_preserves_javascript_spacing(
        base in finite_milliseconds(),
        maximum in finite_milliseconds(),
        doublings in prop_oneof![0..=65_u32, any::<u32>()],
        draw in 0.0_f64..=1.0,
    ) {
        let exponent = i32::try_from(doublings).map_or(52, |count| count.min(52));
        let upper = (base * 2.0_f64.powi(exponent)).min(maximum);
        let actual = Backoff::fractional_delay_ms_at(base, maximum, doublings, draw);
        prop_assert_eq!(actual, (upper * (0.5 + draw / 2.0)).ceil());
        prop_assert!((upper * 0.5).ceil() <= actual && actual <= upper.ceil());
    }

    #[test]
    fn both_draw_sources_stay_in_the_equal_jitter_interval(
        base in prop_oneof![Just(0_u32), Just(1_u32), Just(u32::MAX), any::<u32>()],
        maximum in prop_oneof![Just(0_u32), Just(1_u32), Just(u32::MAX), any::<u32>()],
        sample in any::<u32>(),
    ) {
        let mut backoff = Backoff::new(base, maximum, 0);
        let mut ceilings = Backoff::new(base, maximum, 0);
        let mut seeded = Backoff::new(base, maximum, sample);
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
            prop_assert!((lower..=ceiling).contains(&u128::from(seeded.delay_ms())));
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
