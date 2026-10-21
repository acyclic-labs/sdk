use crate::{Adapter, input::validate_input};
use acyclic_harness_core::model::{
    ModelContent, ModelContentPart, ModelMessage, ModelRole, ReasoningBlock, ReasoningKind,
    ReasoningOutcome, ToolCall, ToolResultContent,
};
use proptest::prelude::*;
use serde_json::json;

// Complete neutral exchanges can be mixed with arbitrary role/text turns. Native
// payloads are fixture continuations; opaque bytes are never invented on output.
fn transcript(units: &[(u8, u8, String)]) -> Vec<ModelMessage> {
    let mut messages = Vec::new();
    for (index, (kind, source, text)) in units.iter().enumerate() {
        let role = match kind % 3 {
            0 => ModelRole::System,
            1 => ModelRole::User,
            _ => ModelRole::Assistant,
        };
        if *kind < 3 {
            messages.push(ModelMessage {
                role,
                content: ModelContent::Text(text.clone()),
            });
            continue;
        }
        let (source_provider, _) = crate::tests::provider(
            [
                Adapter::Anthropic,
                Adapter::Responses,
                Adapter::Gemini,
                Adapter::Chat,
            ][usize::from(*source) % 4],
            200,
        );
        let origin = source_provider.reasoning_origin(source_provider.key.as_deref(), None);
        let family = ["anthropic", "responses", "gemini", "chat"][usize::from(*source) % 4];
        let (reasoning_kind, payload) = match family {
            "anthropic" => (
                ReasoningKind::Readable,
                json!({"type":"thinking","thinking":text,"signature":"signed-fixture"}),
            ),
            "responses" => (
                ReasoningKind::Encrypted,
                json!({"type":"reasoning","id":format!("rs_{index}"),"encrypted_content":"encrypted-fixture","summary":[{"type":"summary_text","text":text}]}),
            ),
            "gemini" => (
                ReasoningKind::Signature,
                if *kind == 3 {
                    json!({"thoughtSignature":"signed-fixture","functionCall":{"id":format!("native_{index}"),"name":"echo","args":{"text":text}}})
                } else {
                    json!({"thoughtSignature":"signed-fixture","text":text})
                },
            ),
            _ => (ReasoningKind::Readable, json!({"text":text})),
        };
        let mut parts = vec![ModelContentPart::Reasoning {
            block: ReasoningBlock {
                kind: reasoning_kind,
                text: (family != "gemini" && !text.is_empty()).then(|| text.clone()),
                origin,
                continuity: serde_json::to_vec(&payload).unwrap(),
                outcome: if family == "gemini" && *kind != 3 {
                    ReasoningOutcome::SuppressedDisplay
                } else {
                    ReasoningOutcome::PreservedOpaque
                },
            },
        }];
        let batch = 1 + usize::from(*source / 4) % 4;
        if *kind == 3 {
            for ordinal in 0..batch {
                let call_id = format!("call:{index}:{ordinal}");
                let native = if ordinal == 0 {
                    format!("native_{index}")
                } else {
                    format!("native_{index}_{ordinal}")
                };
                parts.push(ModelContentPart::ToolCall(
                    ToolCall::new(call_id, "echo".into(), json!({"text":text}))
                        .with_metadata(json!({"provider_call":{"provider":family,"id":native}})),
                ));
            }
        } else {
            parts.push(ModelContentPart::Text { text: text.clone() });
        }
        messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Parts(parts),
        });
        if *kind == 3 {
            for ordinal in (0..batch).rev() {
                messages.push(ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: format!("call:{index}:{ordinal}"),
                        name: "echo".into(),
                        content: ToolResultContent::Json { value: json!(text) },
                    }),
                });
            }
        }
    }
    messages
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn mixed_origin_transcripts_project_to_each_targets_accepted_grammar(
        units in prop::collection::vec((0u8..5,0u8..32,prop::collection::vec(any::<char>(),0..32).prop_map(|chars|chars.into_iter().collect::<String>())),0..12)
    ) {
        let messages=transcript(&units);
        for adapter in [Adapter::Anthropic,Adapter::Responses,Adapter::Chat,Adapter::Gemini] {
            let (provider,_)=crate::tests::provider(adapter,200);
            let (prepared,_)=crate::tests::request(&provider).unwrap();
            let mut request=prepared.request().clone();
            request.messages=messages.clone();
            request.tools.push(acyclic_harness_core::tool::ToolDefinition {
                name:"echo".into(),revision:"1".into(),description:"fixture".into(),input_schema:json!({"type":"object","properties":{"text":{"type":"string"}}}),output_schema:json!({}),projection_schema:json!({}),
            });
            let body=crate::projection::body(&provider,&request).unwrap();
            let replay: acyclic_harness_core::model::ModelRequest = serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
            prop_assert_eq!(&body, &crate::projection::body(&provider, &replay).unwrap());
            prop_assert_eq!(validate_input(adapter,&body),Vec::<&str>::new(),"adapter {:?}, wire {}",adapter,body);
        }
    }
}
