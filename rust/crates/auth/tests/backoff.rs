use acyclic_auth::operation::Backoff;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
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
    fn recovery_spacing_stays_bounded_uncapped_and_reproducible(
        base in 1_u32..=1_000_000,
        maximum in 1_u32..=u32::MAX,
        seed in any::<u32>(),
    ) {
        let mut first = Backoff::new(base, maximum, seed);
        let mut replay = Backoff::new(base, maximum, seed);
        let mut spread = Backoff::new(base, maximum, seed.wrapping_add(1));
        let mut scenario = Backoff::new(base, maximum, seed);
        // A constant ceiling advances the same entropy without any resets.
        let mut entropy = Backoff::new(base.min(maximum), base.min(maximum), seed);
        let mut differs = false;
        for attempt in 0..512 {
            let wait = first.delay_ms();
            entropy.delay_ms();
            prop_assert_eq!(wait, replay.delay_ms());
            let other = spread.delay_ms();
            differs |= other != wait;
            prop_assert!(wait > 0);
            prop_assert!(wait <= maximum);
            // An owner selecting the final jitter choice gets the public
            // exponential ceiling, including saturation over long outages.
            let ceiling = u64::from(base.min(maximum))
                .saturating_mul(2_u64.saturating_pow(attempt))
                .min(u64::from(maximum));
            prop_assert_eq!(scenario.delay_with(|choices| choices - 1).as_millis(), u128::from(ceiling));
            if attempt > 32 {
                prop_assert!(wait >= maximum.div_ceil(2));
            }
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
        prop_assert!(reset.as_millis() <= u128::from(base.min(maximum)));
        prop_assert!(reset.as_millis() >= u128::from(base.min(maximum).div_ceil(2)));
    }
}
