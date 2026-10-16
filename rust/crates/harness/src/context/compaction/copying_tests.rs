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
    // Each distinct, nonempty text payload necessarily allocates once.
    prop_assert_eq!(count, len as u64 + u64::from(len != 0));
    Ok(())
}

fn capacity_bound(capacity: usize, len: usize) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(capacity, len);
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
        let content = ModelContent::Text("inserted user".into());
        // Keep empty and order-sensitive growing cases even after shrinking.
        for (first, second, insert) in [(0, 0, false), (first, second, insert), (2, 2, true)] {
            let messages = |prefix, count| (0..count).map(|i| ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text(format!("{prefix}-{i}")),
            }).collect::<Vec<_>>();
            let first = messages("first", first);
            let second = messages("second", second);
            let user = insert.then_some(&content);
            let len = first.len() + second.len() + usize::from(insert);
            let mut actual = None;
            let measured = acyclic_test_support::measure(|| {
                actual = Some(clone_messages(&first, user, &second, &mut None));
            });
            allocation_bound(measured.count_total, len)?;
            let (actual, claim) = actual.expect("copy was invoked")?;
            capacity_bound(actual.capacity(), len)?;
            prop_assert_eq!(&actual, &original(&first, user, &second));
            prop_assert_eq!(claim, 0);
            if len != 0 {
                let oversized = Vec::<ModelMessage>::with_capacity(2 * len);
                prop_assert!(capacity_bound(oversized.capacity(), len).is_err());
            }
            if !first.is_empty() && (insert || !second.is_empty()) {
                let old = acyclic_test_support::measure(|| {
                    std::hint::black_box(original(&first, user, &second));
                });
                prop_assert!(allocation_bound(old.count_total, len).is_err());
            }
        }
    }
}
