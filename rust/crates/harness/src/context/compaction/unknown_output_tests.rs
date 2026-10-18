use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Unknown output capacity never disables context pressure or permits a
    // request to exceed its context. The reserve is a policy budget only.
    #[test]
    fn unknown_output_still_compacts_against_context(
        context in 3u32..=u32::MAX, fixed in any::<u32>(),
    ) {
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: None};
        let result = ThresholdCompaction::for_capacity(capacity, fixed);
        prop_assert_eq!(result.is_ok(), context.checked_sub(fixed).is_some_and(|n| n >= 3));
        if let Ok(policy) = result {
            let reserve = policy.validate(capacity, None).unwrap();
            prop_assert!(reserve > 0);
            prop_assert!(u64::from(fixed) + u64::from(reserve)
                + 2 * u64::from(policy.recent_tokens) <= u64::from(context));
            prop_assert_eq!(capacity.output_tokens, None);
            prop_assert!(policy.needs_compaction(capacity, u64::from(context)).unwrap());
            prop_assert!(!policy.needs_compaction(capacity, 0).unwrap());
        }
    }
}
