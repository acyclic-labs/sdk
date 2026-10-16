use super::*;
use crate::model::{
    ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome, ToolCall, ToolResultContent,
};
use proptest::prelude::*;

type Batch = (u8, u8, bool, bool);

fn transcript(batches: &[Batch], text: &str) -> (Context, Vec<(usize, usize)>) {
    let mut messages = vec![super::text(ModelRole::User, "old")];
    let mut units = Vec::new();
    for (turn, &(provider, calls, packed, reverse)) in batches.iter().enumerate() {
        messages.push(super::text(ModelRole::User, &format!("input{turn}")));
        let first = messages.len();
        let mut parts = Vec::new();
        for index in 0..usize::from(calls) {
            let id = format!("turn{turn}-call{index}");
            let signature = format!("{id}:{text}");
            let (family, kind, payload) = match provider % 3 {
                0 => (
                    "gemini",
                    ReasoningKind::Signature,
                    serde_json::json!({"functionCall":{"name":"tool","args":{}},"thoughtSignature":signature}),
                ),
                1 => (
                    "anthropic",
                    ReasoningKind::Readable,
                    serde_json::json!({"type":"thinking","thinking":text,"signature":signature}),
                ),
                _ => (
                    "openai-responses",
                    ReasoningKind::Encrypted,
                    serde_json::json!({"type":"reasoning","id":id,"encrypted_content":signature,"summary":[]}),
                ),
            };
            parts.push(ModelContentPart::Reasoning {
                block: ReasoningBlock {
                    kind,
                    text: (provider % 3 == 1).then(|| text.to_owned()),
                    origin: ReasoningOrigin {
                        provider: family.into(),
                        model: format!("model{turn}"),
                        credential_scope: Some([provider; 32]),
                    },
                    continuity: serde_json::to_vec_pretty(&payload).unwrap(),
                    outcome: ReasoningOutcome::PreservedOpaque,
                },
            });
            parts.push(ModelContentPart::Text {
                text: format!("between:{id}:{text}"),
            });
            parts.push(ModelContentPart::ToolCall(ToolCall::new(
                id,
                "tool".into(),
                serde_json::json!({}),
            )));
        }
        if packed {
            messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Parts(parts),
            });
        } else {
            messages.extend(parts.into_iter().map(|part| ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(part),
            }));
        }
        for offset in 0..usize::from(calls) {
            let index = if reverse {
                usize::from(calls) - offset - 1
            } else {
                offset
            };
            messages.push(ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: format!("turn{turn}-call{index}"),
                    name: "tool".into(),
                    content: ToolResultContent::Json {
                        value: serde_json::json!({"index":index}),
                    },
                }),
            });
        }
        units.push((first, messages.len() - 1));
    }
    messages.push(super::text(ModelRole::User, "current"));
    let current = messages.len() as u32 - 1;
    (context(messages, Some(current)), units)
}

// Independent oracle: generation records each complete source batch. Equality
// includes the exact native continuity bytes, rather than reinterpreted JSON.
fn whole_units(source: &Context, retained: &Context, units: &[(usize, usize)]) -> bool {
    units.iter().all(|&(first, last)| {
        let required: Vec<_> = source.messages[first..=last]
            .iter()
            .filter(|message| {
                message.content.parts().iter().any(|part| {
                    matches!(
                        part,
                        ModelContentPart::Reasoning { .. }
                            | ModelContentPart::ToolCall(_)
                            | ModelContentPart::ToolResult { .. }
                    )
                })
            })
            .collect();
        let kept = required
            .iter()
            .filter(|message| retained.messages.contains(message))
            .count();
        kept == 0 || kept == required.len()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn random_cuts_never_separate_native_reasoning_from_its_exchange(
        batches in prop::collection::vec((0u8..3, 1u8..4, any::<bool>(), any::<bool>()), 1..6),
        text in ".{0,32}", raw_cut in any::<usize>(), raw_maximum in any::<usize>(),
    ) {
        let (source, units) = transcript(&batches, &text);
        let cut = 1 + raw_cut % (source.messages.len() - 1);
        let count = ModelTokenCount::from_upper_bounds([0; 32], 0, vec![1; source.messages.len()]);
        let policy = ThresholdCompaction { recent_tokens: (source.messages.len() - cut) as u32, ..ThresholdCompaction::default() };
        let (through, maximum) = policy.projection_budget(&source, &count, source.messages.len(), &mut None).unwrap();
        prop_assert!(units.iter().all(|&(first, last)| through <= first || through > last));
        let (retained, reference) = compact(&source, maximum, through).unwrap();
        prop_assert!(whole_units(&source, &retained, &units));
        validate_compaction(&reference, &source, &retained, None).unwrap();
        // Application also accepts arbitrary source cuts through the public path.
        let (retained, _) = compact(&source, source.messages.len(), cut).unwrap();
        prop_assert!(whole_units(&source, &retained, &units));
        // A fully summarized prefix exercises refill with too few spare slots.
        let maximum = 2 + raw_maximum % (source.messages.len() - 1);
        let (through, planned_maximum) = policy.projection_budget(&source, &count, maximum, &mut None).unwrap();
        prop_assert!(units.iter().all(|&(first, last)| through <= first || through > last));
        let (retained, reference) = compact(&source, planned_maximum, through).unwrap();
        prop_assert!(whole_units(&source, &retained, &units));
        validate_compaction(&reference, &source, &retained, None).unwrap();
        let (retained, reference) = compact(&source, maximum, source.messages.len()).unwrap();
        prop_assert!(retained.messages.len() <= maximum);
        prop_assert!(whole_units(&source, &retained, &units));
        validate_compaction(&reference, &source, &retained, None).unwrap();
    }
}

#[test]
fn call_boundaries_keep_signed_protocols_and_tight_budgets_drop_whole_units() -> Result<()> {
    for provider in 0..3 {
        let (source, units) = transcript(&[(provider, 2, false, true)], "signed\nquote\"\\\u{3bb}");
        let call = units[0].0 + 2;
        let (retained, _) = compact(&source, source.messages.len(), call)?;
        assert!(whole_units(&source, &retained, &units));
        assert!(retained.messages.contains(&source.messages[units[0].0]));
        let (dropped, _) = compact(&source, 2, source.messages.len())?;
        assert_eq!(dropped.messages.len(), 2);
        assert!(whole_units(&source, &dropped, &units));
        assert!(!dropped.messages.contains(&source.messages[call]));
    }
    Ok(())
}

#[test]
fn negative_control_detects_call_result_links_that_omit_required_reasoning() {
    for provider in 0..3 {
        let (source, units) = transcript(&[(provider, 1, false, false)], "signature");
        let cut = units[0].0 + 2;
        // Former selection: both call and result are already retained. Linking
        // only them never restores the reasoning immediately before the call.
        let mut selected: Vec<_> = (0..source.messages.len()).map(|i| i >= cut).collect();
        for (result_index, result) in source.messages.iter().enumerate() {
            for part in result.content.parts() {
                if let ModelContentPart::ToolResult { call_id, .. } = part {
                    for (call_index, call) in source.messages.iter().enumerate() {
                        if call.content.parts().iter().any(|part| {
                            matches!(part, ModelContentPart::ToolCall(call) if &call.call_id == call_id)
                        }) && (selected[call_index] || selected[result_index]) {
                            selected[call_index] = true;
                            selected[result_index] = true;
                        }
                    }
                }
            }
        }
        let broken = context(
            source
                .messages
                .iter()
                .zip(selected)
                .filter_map(|(message, keep)| keep.then(|| message.clone()))
                .collect(),
            None,
        );
        assert!(!whole_units(&source, &broken, &units));
        assert!(broken.messages.iter().any(|message| {
            message
                .content
                .parts()
                .iter()
                .any(|part| matches!(part, ModelContentPart::ToolCall(_)))
        }));
    }
}

#[cfg(feature = "filesystem")]
#[test]
fn rejected_exchange_selection_releases_every_temporary_allocation() -> Result<()> {
    let (source, _) = transcript(&[(0, 2, false, true), (2, 1, true, false)], "opaque");
    let count = ModelTokenCount::from_upper_bounds([0; 32], 0, vec![1; source.messages.len()]);
    let policy = ThresholdCompaction {
        recent_tokens: 3,
        ..ThresholdCompaction::default()
    };
    let mut rejected = 0;
    let mut admitted = 0;
    for items in 0..512 {
        let mut budget = owned_context_budget();
        budget.items_examined = items;
        let cancel = acyclic_fs::CancellationToken::new();
        let io = crate::executor::PreparationIo::from_budget(budget, &cancel)?;
        let result = crate::executor::with_history_io(Some(&io), |work| {
            policy.projection_budget(&source, &count, source.messages.len(), work)
        });
        if result.is_err() {
            rejected += 1;
        } else {
            admitted += 1;
        }
        assert_eq!(
            io.owned_bytes()?,
            0,
            "selection leaked custody at {items} items"
        );
    }
    assert!(rejected > 0 && admitted > 0);
    Ok(())
}
