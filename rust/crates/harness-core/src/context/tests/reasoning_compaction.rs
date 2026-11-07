use super::*;
use crate::model::{
    NativePart, ReasoningBlock, ReasoningKind, ReasoningOrigin, ReasoningOutcome, ToolCall,
    ToolResultContent,
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
            let origin = ReasoningOrigin {
                provider: family.into(),
                model: format!("model{turn}"),
                credential_scope: Some([provider; 32]),
            };
            let between = format!("between:{id}:{text}");
            if family == "gemini" {
                // Gemini signs the parts themselves: native text and a native
                // call, with no reasoning block before them.
                let native = |payload: serde_json::Value| NativePart {
                    origin: origin.clone(),
                    continuity: serde_json::to_vec_pretty(&payload).unwrap(),
                };
                parts.push(ModelContentPart::Text {
                    native: Some(native(
                        serde_json::json!({"text":between,"thoughtSignature":signature}),
                    )),
                    text: between,
                });
                parts.push(ModelContentPart::ToolCall(
                    ToolCall::new(id, "tool".into(), serde_json::json!({}))
                        .with_native(Some(native(payload))),
                ));
                continue;
            }
            parts.push(ModelContentPart::Reasoning {
                block: ReasoningBlock {
                    kind,
                    text: (provider % 3 == 1).then(|| text.to_owned()),
                    origin,
                    continuity: serde_json::to_vec_pretty(&payload).unwrap(),
                    outcome: ReasoningOutcome::PreservedOpaque,
                },
            });
            parts.push(ModelContentPart::text(between));
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
                            | ModelContentPart::Text {
                                native: Some(_),
                                ..
                            }
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
        let (through, maximum) = policy.projection_budget(&source, &count, source.messages.len(), policy.recent_tokens, &mut None).unwrap();
        prop_assert!(units.iter().all(|&(first, last)| through <= first || through > last));
        let (retained, reference) = compact(&source, maximum, through).unwrap();
        prop_assert!(whole_units(&source, &retained, &units));
        validate_compaction(&reference, &source, &retained, None).unwrap();
        // Application also accepts arbitrary source cuts through the public path.
        let (retained, _) = compact(&source, source.messages.len(), cut).unwrap();
        prop_assert!(whole_units(&source, &retained, &units));
        // A fully summarized prefix exercises refill with too few spare slots.
        let maximum = 2 + raw_maximum % (source.messages.len() - 1);
        let mut planned = None;
        let measured = acyclic_test_support::measure(|| {
            planned = Some(policy.projection_budget(&source, &count, maximum, policy.recent_tokens, &mut None));
        });
        // One call vector, sort permutation, radix table, component vector and selection vector,
        // regardless of how many candidate boundaries the budget rejects.
        prop_assert_eq!(measured.count_total, 5);
        let (through, planned_maximum) = planned.unwrap().unwrap();
        prop_assert_eq!(planned_maximum, super::super::compaction::mandatory_messages(&source, through, &mut None).unwrap().len() + 1);
        // Negative control: rebuilding dependencies for a candidate, as the old
        // planner did, violates the same scratch-allocation bound.
        let duplicated = acyclic_test_support::measure(|| {
            std::hint::black_box(policy.projection_budget(&source, &count, maximum, policy.recent_tokens, &mut None).unwrap());
            std::hint::black_box(super::super::compaction::mandatory_messages(&source, through, &mut None).unwrap());
        });
        prop_assert!(duplicated.count_total > 5);
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
    // All outstanding calls with the same id depend on its closing result.
    // Interior plain text remains independent of that protocol component.
    let call = |index| ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
            "reused".into(),
            "tool".into(),
            serde_json::json!({"index": index}),
        ))),
    };
    let source = context(
        vec![
            super::text(ModelRole::User, "old"),
            call(0),
            super::text(ModelRole::User, "interior"),
            call(1),
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "reused".into(),
                    name: "tool".into(),
                    content: ToolResultContent::Json {
                        value: serde_json::Value::Null,
                    },
                }),
            },
            super::text(ModelRole::User, "latest"),
        ],
        Some(1),
    );
    let (retained, proof) = compact(&source, 5, source.messages.len())?;
    assert!(whole_units(&source, &retained, &[(1, 4)]));
    assert!(!retained.messages.contains(&source.messages[2]));
    validate_compaction(&proof, &source, &retained, None)?;
    assert!(compact(&source, 4, source.messages.len()).is_err());
    // A closed id can start an independent exchange. The original input pins
    // the earlier component without pinning this later reuse of the same id.
    let mut reused = source.clone();
    reused.messages.insert(5, call(2));
    let mut result = source.messages[4].clone();
    if let ModelContent::Part(ModelContentPart::ToolResult { content, .. }) = &mut result.content {
        *content = ToolResultContent::Json {
            value: serde_json::json!(2),
        };
    }
    reused.messages.insert(6, result);
    let (retained, proof) = compact(&reused, 5, reused.messages.len())?;
    assert!(whole_units(&reused, &retained, &[(1, 4), (5, 6)]));
    assert!(!retained.messages.contains(&reused.messages[5]));
    validate_compaction(&proof, &reused, &retained, None)?;
    Ok(())
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
    let plan = |io: &crate::executor::PreparationIo| {
        crate::executor::with_history_io(Some(io), |work| {
            policy.projection_budget(
                &source,
                &count,
                source.messages.len(),
                policy.recent_tokens,
                work,
            )
        })
    };
    let budget = owned_context_budget();
    let cancel = acyclic_fs::CancellationToken::new();
    let measured = crate::executor::PreparationIo::from_budget(budget, &cancel)?;
    plan(&measured)?;
    let required = budget.items_examined - measured.remaining()?.items_examined;
    assert_eq!(measured.owned_bytes()?, 0);
    let mut rejected = 0;
    let mut admitted = 0;
    // Exercise every original admission boundary through the exact successful
    // cost, rather than stopping at a fixed allowance unrelated to the input.
    for items in 0..=required {
        let mut budget = owned_context_budget();
        budget.items_examined = items;
        let cancel = acyclic_fs::CancellationToken::new();
        let io = crate::executor::PreparationIo::from_budget(budget, &cancel)?;
        let result = plan(&io);
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

#[cfg(feature = "filesystem")]
#[test]
fn incomplete_exchange_refusal_precedes_output_copy_and_releases_graph_custody() -> Result<()> {
    for provider in 0..3 {
        for remove_call in [false, true] {
            let (mut source, _) =
                transcript(&[(provider, 2, false, true)], "retained native payload");
            let removed = source
                .messages
                .iter()
                .position(|message| {
                    message.content.parts().iter().any(|part| {
                        if remove_call {
                            matches!(part, ModelContentPart::ToolCall(_))
                        } else {
                            matches!(part, ModelContentPart::ToolResult { .. })
                        }
                    })
                })
                .unwrap();
            source.messages.remove(removed);
            source.current_input_index = Some(source.messages.len() as u32 - 1);
            let summary = super::summary(&source, source.messages.len())?;
            let mut budget = owned_context_budget();
            // The source is already resident. Matching and closure borrow it;
            // no retained output should be copied before protocol refusal.
            budget.bytes_copied = 0;
            let cancel = acyclic_fs::CancellationToken::new();
            let io = crate::executor::PreparationIo::from_budget(budget, &cancel)?;
            let result = crate::executor::with_history_io(Some(&io), |work| {
                super::super::compaction::compact_context(
                    &source,
                    source.messages.len(),
                    &summary,
                    work,
                )
            });
            assert!(matches!(result, Err(crate::Error::Invalid(reason))
                if reason == "compaction boundary splits a tool exchange"));
            assert_eq!(io.owned_bytes()?, 0);
        }
    }
    Ok(())
}
