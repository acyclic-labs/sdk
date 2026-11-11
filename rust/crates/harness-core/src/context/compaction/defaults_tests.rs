use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn automatic_budgets_preserve_the_selected_output_reservation(
        context in 3u32..=u32::MAX, output in 1u32..=u32::MAX,
        fixed in any::<u32>(),
    ) {
        let output = output.min(context);
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: Some(output) };
        let result = CapacityCompaction::for_capacity(capacity, fixed);
        let remaining = i64::from(context) - i64::from(fixed) - i64::from(output);
        prop_assert_eq!(result.is_ok(), remaining >= 2);
        if let Ok(policy) = result {
            prop_assert_eq!(policy.validate(capacity, None).unwrap(), output);
            let boundary = u64::from(context - output);
            prop_assert!(!policy.needs_compaction(capacity, boundary, None).unwrap());
            prop_assert!(policy.needs_compaction(capacity, boundary + 1, None).unwrap());
        }
        // The former clamp admits an input even when the selected output
        // occupies the whole context. The actual reservation refuses that cut.
        let full = ModelContextCapacity { context_tokens: context, output_tokens: Some(context) };
        prop_assert!(CapacityCompaction::for_capacity(full, 0).is_err());
    }
}
