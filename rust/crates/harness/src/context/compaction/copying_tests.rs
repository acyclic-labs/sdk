use super::*;
use crate::model::{ModelContent, ModelMessage, ModelRole};
use proptest::prelude::*;

// The original clone-and-grow branch is independent of the reservation.
fn original(
    first: &[ModelMessage],
    user: Option<&ModelContent>,
    second: &[ModelMessage],
) -> Vec<ModelMessage> {
    let mut result = first.to_vec();
    if let Some(content) = user {
        result.push(ModelMessage {
            role: ModelRole::User,
            content: content.clone(),
        });
    }
    result.extend_from_slice(second);
    result
}

fn allocation_bound(count: u64, len: usize) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(count, u64::from(len != 0));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn concatenation_allocates_only_the_final_vector(
        first in 0usize..1025,
        second in 0usize..1025,
        insert in any::<bool>(),
    ) {
        let content = ModelContent::Text(String::new());
        let message = ModelMessage { role: ModelRole::Assistant, content: content.clone() };
        // Empty payloads isolate vector allocations. Always include the empty
        // result and a growing baseline, even after shrinking the random sizes.
        for (first, second, insert) in [(0, 0, false), (first, second, insert), (1, 1, true)] {
            let first = vec![message.clone(); first];
            let second = vec![message.clone(); second];
            let user = insert.then_some(&content);
            let len = first.len() + second.len() + usize::from(insert);
            let mut actual = None;
            let measured = allocation_counter::measure(|| {
                actual = Some(clone_messages(&first, user, &second, &mut None));
            });
            allocation_bound(measured.count_total, len)?;
            let (actual, claim) = actual.expect("copy was invoked")?;
            prop_assert_eq!(&actual, &original(&first, user, &second));
            prop_assert_eq!(claim, 0);
            if !first.is_empty() && (insert || !second.is_empty()) {
                let old = allocation_counter::measure(|| {
                    std::hint::black_box(original(&first, user, &second));
                });
                prop_assert!(allocation_bound(old.count_total, len).is_err());
            }
        }
    }
}
