use super::*;
use crate::model::{ModelContent, ModelMessage, ModelRole};
use proptest::prelude::*;

// The complete original unmetered production branch, independent of the new reservation.
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

fn message((role, parts, text): (u8, bool, String)) -> ModelMessage {
    ModelMessage {
        role: match role % 4 {
            0 => ModelRole::System,
            1 => ModelRole::User,
            2 => ModelRole::Assistant,
            _ => ModelRole::Tool,
        },
        content: if parts {
            ModelContent::Parts(vec![ModelContentPart::Text { text }])
        } else {
            ModelContent::Text(text)
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Concatenation preserves every role/content, inserts exactly one user when
    // requested, and reserves exactly the final number of message slots.
    #[test]
    fn concatenation_matches_original_with_exact_storage(
        first in prop::collection::vec((any::<u8>(), any::<bool>(), ".{0,32}"), 0..24),
        second in prop::collection::vec((any::<u8>(), any::<bool>(), ".{0,32}"), 0..24),
        user in prop::option::of(".{0,32}"),
    ) {
        let first: Vec<_> = first.into_iter().map(message).collect();
        let second: Vec<_> = second.into_iter().map(message).collect();
        let user = user.map(ModelContent::Text);
        let expected = original(&first, user.as_ref(), &second);
        let (actual, claim) = clone_messages(&first, user.as_ref(), &second, &mut None)?;
        prop_assert_eq!(actual.capacity(), expected.len());
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(claim, 0);
    }
}

fn empty_messages(count: usize) -> Vec<ModelMessage> {
    vec![
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text(String::new())
        };
        count
    ]
}

#[test]
fn concatenation_allocates_only_the_final_vector_and_rejects_the_old_growth() -> Result<()> {
    let user = ModelContent::Text(String::new());
    for (first, second, insert) in [
        (0, 0, false),
        (0, 0, true),
        (1, 32, true),
        (32, 32, false),
        (32, 32, true),
        (64, 0, false),
    ] {
        let first = empty_messages(first);
        let second = empty_messages(second);
        let user = insert.then_some(&user);
        let count = first.len() + second.len() + usize::from(insert);
        let mut result = None;
        let measured = allocation_counter::measure(|| {
            result = Some(clone_messages(&first, user, &second, &mut None));
        });
        let (result, claim) = result.expect("copy was invoked")?;
        assert_eq!(result, original(&first, user, &second));
        assert_eq!(claim, 0);
        assert_eq!(measured.count_total, u64::from(count != 0));
        assert_eq!(
            measured.bytes_total,
            (count * std::mem::size_of::<ModelMessage>()) as u64
        );
        assert_eq!(measured.bytes_max, measured.bytes_total);
        let before = allocation_counter::measure(|| {
            std::hint::black_box(original(&first, user, &second));
        });
        if !first.is_empty() && (!second.is_empty() || user.is_some()) {
            assert!(
                before.count_total > 1,
                "the original path must violate the same one-vector bound"
            );
            assert!(before.bytes_total > measured.bytes_total);
        }
    }
    Ok(())
}

#[path = "copying_profile.rs"]
mod profile;
