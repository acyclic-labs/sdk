use super::*;
use proptest::prelude::*;

proptest! {
    // Every accepted automatic preset fits fixed overhead plus its output and
    // recent context, leaving at least as much input space for compaction.
    #[test]
    fn automatic_budgets_fit_the_complete_request(
        context in 3u32..=u32::MAX, output in 1u32..=u32::MAX,
        fixed in any::<u32>(),
    ) {
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: Some(output.min(context))};
        let result = ThresholdCompaction::for_capacity(capacity, fixed);
        let available = context.checked_sub(fixed);
        prop_assert_eq!(result.is_ok(), available.is_some_and(|tokens| tokens >= 3));
        if let Ok(policy) = result {
            prop_assert!(policy.recent_tokens > 0);
            prop_assert!(u64::from(fixed) + u64::from(policy.response_reserve_tokens)
                + 2 * u64::from(policy.recent_tokens) <= u64::from(context));
            prop_assert!(policy.recent_tokens <= ThresholdCompaction::default().recent_tokens);
            prop_assert!(u64::from(fixed) + 2 * u64::from(policy.response_reserve_tokens) <= u64::from(context));
            prop_assert!(policy.response_reserve_tokens <= capacity.output_tokens.unwrap());
            prop_assert!(policy.response_reserve_tokens <= ThresholdCompaction::default().response_reserve_tokens);
            prop_assert_eq!(policy.validate(capacity, None).unwrap(), policy.response_reserve_tokens);
        }
    }
}

proptest! {
    // The former output-first choice starves every generated small model once
    // even a little framing is present. Both choices face the same fit detector.
    #[test]
    fn equal_input_and_output_capacities_preserve_fixed_headroom(
        context in 8u32..=16_384, fixed in 3u32..=4,
    ) {
        let capacity = ModelContextCapacity {context_tokens:context,output_tokens: Some(context)};
        let policy = ThresholdCompaction::for_capacity(capacity,fixed).unwrap();
        let fits = |reserve:u32,recent:u32| recent > 0 && reserve > 0
            && u64::from(fixed) + u64::from(reserve) + 2 * u64::from(recent) <= u64::from(context);
        prop_assert!(fits(policy.response_reserve_tokens,policy.recent_tokens));
        let legacy_reserve = 16_384.min(capacity.output_tokens.unwrap()).min(context - 2);
        let legacy_recent = context.saturating_sub(legacy_reserve).saturating_sub(fixed) / 2;
        prop_assert!(!fits(legacy_reserve,legacy_recent));
    }
}
