use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn automatic_budgets_preserve_the_selected_output_reservation(
        context in 3u32..=u32::MAX, output in 1u32..=u32::MAX,
        fixed in any::<u32>(), selected_watermark in any::<u32>(),
    ) {
        let output = output.min(context);
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: Some(output) };
        let result = ThresholdCompaction::for_capacity(capacity, fixed);
        let remaining = i64::from(context) - i64::from(fixed) - i64::from(output);
        prop_assert_eq!(result.is_ok(), remaining >= 2);
        if let Ok(policy) = result {
            prop_assert!(policy.recent_tokens > 0);
            prop_assert_eq!(policy.validate(capacity, None).unwrap(), output);
            prop_assert!(u64::from(fixed) + u64::from(output)
                + 2 * u64::from(policy.recent_tokens) <= u64::from(context));
            let boundary = u64::from(context - output);
            prop_assert!(!policy.needs_compaction(capacity, boundary, None).unwrap());
            prop_assert!(policy.needs_compaction(capacity, boundary + 1, None).unwrap());

            let selected = selected_watermark.max(2);
            let watermark = selected.min(context - output);
            let policy = ThresholdCompaction::for_watermark(capacity, selected, output).unwrap();
            prop_assert_eq!(policy.validate(capacity, None).unwrap(), output);
            prop_assert!(!policy.needs_compaction(capacity, u64::from(watermark), None).unwrap());
            prop_assert!(policy.needs_compaction(capacity, u64::from(watermark) + 1, None).unwrap());
            prop_assert!(policy.recent_tokens <= watermark / 2);
            prop_assert!(ThresholdCompaction::for_watermark(capacity, 0, output).is_err());
            prop_assert!(ThresholdCompaction::for_watermark(capacity, 1, output).is_err());
        }
        // The former clamp admits an input even when the selected output
        // occupies the whole context. The actual reservation refuses that cut.
        let full = ModelContextCapacity { context_tokens: context, output_tokens: Some(context) };
        prop_assert!(ThresholdCompaction::for_capacity(full, 0).is_err());
    }
}
