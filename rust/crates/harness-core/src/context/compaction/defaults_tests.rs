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
