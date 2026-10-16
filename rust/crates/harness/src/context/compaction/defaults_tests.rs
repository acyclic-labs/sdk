use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Every accepted automatic preset fits fixed overhead plus its output and
    // recent context, leaving at least as much input space for compaction.
    #[test]
    fn automatic_budgets_fit_the_complete_request(
        context in 3u32..=u32::MAX, output in 1u32..=u32::MAX,
        fixed in any::<u32>(),
    ) {
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: output.min(context) };
        let result = ThresholdCompaction::for_capacity(capacity, fixed);
        let reserve = 16_384.min(capacity.output_tokens).min(context - 2);
        let usable = context.saturating_sub(reserve).saturating_sub(fixed);
        prop_assert_eq!(result.is_ok(), usable >= 2);
        if let Ok(policy) = result {
            prop_assert!(policy.recent_tokens > 0);
            prop_assert!(u64::from(fixed) + u64::from(policy.response_reserve_tokens)
                + 2 * u64::from(policy.recent_tokens) <= u64::from(context));
            prop_assert!(policy.recent_tokens <= ThresholdCompaction::default().recent_tokens);
            prop_assert_eq!(policy.validate(capacity, None).unwrap(), reserve);
        }
    }
}
