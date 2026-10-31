use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn unknown_output_requires_an_explicit_reservation(
        context in 8u32..=u32::MAX,
        output in 1u32..=u32::MAX,
    ) {
        let output = output.min(context - 2);
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: None };
        let policy = ThresholdCompaction {
            response_reserve_tokens: output,
            recent_tokens: 1,
        };
        prop_assert!(ThresholdCompaction::for_capacity(capacity, 0).is_err());
        prop_assert!(policy.validate(capacity, None).is_err());
        prop_assert!(policy.needs_compaction(capacity, 0, None).is_err());
        prop_assert_eq!(policy.validate(capacity, Some(output)).unwrap(), output);
        let boundary = u64::from(context - output);
        prop_assert!(!policy.needs_compaction(capacity, boundary, Some(output)).unwrap());
        prop_assert!(policy.needs_compaction(capacity, boundary + 1, Some(output)).unwrap());
        // Restoring the former guessed reserve admits an unbounded selection.
        prop_assert!(capacity.output_reservation(Some(policy.response_reserve_tokens)).is_ok());
        prop_assert!(capacity.output_reservation(None).is_err());
    }
}
