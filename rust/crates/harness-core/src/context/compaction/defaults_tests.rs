use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn context_admission_is_independent_of_published_output_capacity(
        context in 1u32..=u32::MAX, output in prop::option::of(any::<u32>()),
    ) {
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: output };
        let policy = CapacityCompaction::default();
        let boundary = u64::from(context);
        prop_assert!(!policy.needs_compaction(capacity, boundary).unwrap());
        prop_assert!(policy.needs_compaction(capacity, boundary + 1).unwrap());
    }
}
