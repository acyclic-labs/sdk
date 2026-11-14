use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn reserved_output_leaves_its_published_maximum(
        context in 1u32..=u32::MAX, output in prop::option::of(any::<u32>()),
        observed in any::<u32>(),
    ) {
        let capacity = ModelContextCapacity {
            context_tokens: context,
            output_tokens: output,
            output_shares_context: false,
        };
        let policy = CapacityCompaction::default();
        let observed = u64::from(observed);
        if let Some(output) = output.filter(|output| *output < context) {
            let boundary = u64::from(context - output);
            prop_assert!(!policy.needs_compaction(capacity, boundary, observed).unwrap());
            prop_assert!(policy.needs_compaction(capacity, boundary + 1, observed).unwrap());
        } else {
            prop_assert!(policy.input_ceiling(capacity, observed).is_err());
        }
    }

    #[test]
    fn shared_output_leaves_the_largest_observed_response(
        context in 1u32..=u32::MAX, output in prop::option::of(any::<u32>()),
        observed in any::<u32>(),
    ) {
        let capacity = ModelContextCapacity {
            context_tokens: context,
            output_tokens: output,
            output_shares_context: true,
        };
        let policy = CapacityCompaction::default();
        if observed < context {
            let boundary = u64::from(context - observed);
            let observed = u64::from(observed);
            prop_assert!(!policy.needs_compaction(capacity, boundary, observed).unwrap());
            prop_assert!(policy.needs_compaction(capacity, boundary + 1, observed).unwrap());
        } else {
            prop_assert!(policy.input_ceiling(capacity, u64::from(observed)).is_err());
        }
    }
}

/// Morph's published capacity: output = context = 1,048,576 tokens.
#[test]
fn morph_shared_window_admits_input_beside_observed_responses() -> Result<()> {
    let morph = |shared| ModelContextCapacity {
        context_tokens: 1_048_576,
        output_tokens: Some(1_048_576),
        output_shares_context: shared,
    };
    let policy = CapacityCompaction::default();
    assert_eq!(policy.input_ceiling(morph(true), 0)?, 1_048_576);
    assert_eq!(policy.input_ceiling(morph(true), 48_576)?, 1_000_000);
    assert!(!policy.needs_compaction(morph(true), 1_000_000, 48_576)?);
    assert!(policy.needs_compaction(morph(true), 1_000_001, 48_576)?);
    // Negative control: reserving the published maximum leaves no input.
    assert!(policy.input_ceiling(morph(false), 0).is_err());
    Ok(())
}

#[test]
fn latest_document_revision_survives_compaction_without_retaining_superseded_notices() -> Result<()>
{
    use crate::model::{ModelContent, ModelMessage, ModelRole};
    for removed in [false, true] {
        let notice = |revision: &str, removed: bool| ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Part(ModelContentPart::Notices {
                notices: vec![crate::conversation::wire::Notice {
                    source: "harness".into(),
                    kind: crate::context::CONTEXT_CHANGED.into(),
                    body: serde_json::json!({"version":1,"document":{"path":"context/pins/evidence.md"},"text":revision,"removed":removed}),
                    wake: false,
                    coalesce_key: Some("context.changed:document:evidence".into()),
                }],
            }),
        };
        let latest = notice("current evidence", removed);
        let current = ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("continue".into()),
        };
        let context = Context {
            messages: vec![
                notice("superseded evidence", false),
                latest.clone(),
                current.clone(),
            ],
            current_input_index: Some(2),
            ..Default::default()
        };
        let retained = super::selection::mandatory_messages(&context, 3, &mut None)?;
        assert_eq!(retained, vec![latest, current]);
    }
    Ok(())
}
