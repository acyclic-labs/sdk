use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn context_admission_preserves_verified_response_room(
        context in 1u32..=u32::MAX, output in prop::option::of(any::<u32>()),
    ) {
        let capacity = ModelContextCapacity { context_tokens: context, output_tokens: output };
        let policy = CapacityCompaction::default();
        if let Some(output) = output.filter(|output| *output < context) {
            let boundary = u64::from(context - output);
            prop_assert!(!policy.needs_compaction(capacity, boundary).unwrap());
            prop_assert!(policy.needs_compaction(capacity, boundary + 1).unwrap());
        } else {
            prop_assert!(policy.input_ceiling(capacity).is_err());
        }
    }
}
