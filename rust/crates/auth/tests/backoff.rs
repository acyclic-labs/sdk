use acyclic_auth::operation::Backoff;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn recovery_spacing_stays_bounded_uncapped_and_reproducible(
        base in 1_u32..=1_000_000,
        maximum in 1_u32..=u32::MAX,
        seed in any::<u32>(),
    ) {
        let mut first = Backoff::new(base, maximum, seed);
        let mut replay = Backoff::new(base, maximum, seed);
        let mut spread = Backoff::new(base, maximum, seed.wrapping_add(1));
        let mut differs = false;
        for attempt in 0..512 {
            let wait = first.delay_ms();
            prop_assert_eq!(wait, replay.delay_ms());
            let other = spread.delay_ms();
            differs |= other != wait;
            prop_assert!(wait > 0);
            prop_assert!(wait <= maximum);
            if attempt > 32 {
                prop_assert!(wait >= maximum.div_ceil(2));
            }
        }
        if maximum > 1 {
            prop_assert!(differs);
        }
    }
}
