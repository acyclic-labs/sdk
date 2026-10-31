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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn admitted_tool_schemas_survive_generation_counting_and_override_attempts(text in ".{0,64}") {
        let schema = json!({
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "title":"unused root title", "description":"repeated root description",
            "type":"object", "required":["title", "payload"],
            "properties":{
                "title":{"title":"unused field title", "type":"string", "description":"keep field guidance"},
                "payload":{"const":{"title":text,"$schema":"literal"}}
            }, "additionalProperties":false
        });
        let model_schema = serde_json::to_value(acyclic_harness_core::tool::schema::model_input(&schema)).unwrap();
        for adapter in [Adapter::Anthropic, Adapter::Responses, Adapter::Chat, Adapter::Gemini] {
            let (provider, _) = crate::tests::provider(adapter, 200);
            let (prepared, _) = crate::tests::request(&provider).unwrap();
            let mut request = prepared.request().clone();
            request.tools.push(acyclic_harness_core::tool::ToolDefinition {
                name:"echo".into(), revision:"1".into(), description:"original guidance".into(),
                input_schema:schema.clone(), output_schema:json!({}), projection_schema:json!({}),
            });
            request.model.options["tools"] = json!([{"type":"forged-native-tool"}]);
            let names = crate::tool_names::Names::new(&request);
            let body = crate::projection::body_named(&provider, &request, &names,
                &provider.reasoning_origin(provider.key.as_deref(), None),
                &mut crate::files::FileText::default()).unwrap();
            let generation = serde_json::to_value(&body).unwrap();
            let count = serde_json::to_value(crate::counting::wire::body(&body, &request.model.name, adapter)).unwrap();
            let count = if adapter == Adapter::Gemini { &count["generateContentRequest"] } else { &count };
            prop_assert_eq!(&count["tools"], &generation["tools"], "count/generation declarations diverged for {:?}", adapter);
            prop_assert_eq!(validate_input(adapter, &generation), Vec::<&str>::new(), "provider grammar {:?}", adapter);
            let parameters = match adapter {
                Adapter::Anthropic => &generation["tools"][0]["input_schema"],
                Adapter::Responses => &generation["tools"][0]["parameters"],
                Adapter::Chat => &generation["tools"][0]["function"]["parameters"],
                Adapter::Gemini => &generation["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"],
            };
            prop_assert_eq!(parameters, &model_schema);
            prop_assert_eq!(&request.tools[0].input_schema, &schema, "source schema was mutated");
            drop(body);
            drop(names);
            request.tools.clear();
            let generation = crate::projection::body(&provider, &request).unwrap();
            prop_assert!(generation.get("tools").is_none_or(|tools| tools.as_array().is_some_and(Vec::is_empty)),
                "options supplied an unadmitted tool for {:?}", adapter);
            prop_assert_eq!(validate_input(adapter, &generation), Vec::<&str>::new());
        }
    }
}
