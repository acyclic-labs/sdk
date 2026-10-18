use super::*;
use acyclic_harness::model::{ModelContentPart, ToolResultContent};
use proptest::prelude::*;
use serde_json::Value;

fn append(events: Vec<ModelEvent>, request: &mut ModelRequest, answer: &mut String) {
    let bytes = serde_json::to_vec(&events).unwrap();
    let replay: Vec<ModelEvent> = serde_json::from_slice(&bytes).unwrap();
    crate::projection::gemini::assert_visible_replay(&replay);
    let mut results = Vec::new();
    for event in replay {
        let part = match event {
            ModelEvent::Content { delta } => {
                answer.push_str(&delta);
                continue;
            }
            ModelEvent::ReasoningBlock { block } => ModelContentPart::Reasoning { block },
            ModelEvent::ToolCall(call) => {
                results.push(ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: call.call_id.clone(),
                        name: call.name.clone(),
                        content: ToolResultContent::Json {
                            value: json!({"ok":true}),
                        },
                    }),
                });
                ModelContentPart::ToolCall(call)
            }
            _ => continue,
        };
        request.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(part),
        });
    }
    request.messages.extend(results);
}

fn finish(request: &mut ModelRequest, answer: &str) {
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Text(answer.into()),
    });
    request.messages.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text("next".into()),
    });
}

fn visible(body: &Value, adapter: Adapter) -> String {
    let mut text = String::new();
    let field = match adapter {
        Adapter::Gemini => "contents",
        Adapter::Responses => "input",
        _ => "messages",
    };
    for message in body[field].as_array().unwrap() {
        if message["role"]
            != if adapter == Adapter::Gemini {
                "model"
            } else {
                "assistant"
            }
        {
            continue;
        }
        if let Some(content) = message["content"].as_str() {
            text.push_str(content);
        }
        let parts = if adapter == Adapter::Gemini {
            &message["parts"]
        } else {
            &message["content"]
        };
        if let Some(parts) = parts.as_array() {
            for part in parts {
                if part["thought"] != true {
                    if let Some(value) = part["text"].as_str() {
                        text.push_str(value);
                    }
                }
            }
        }
    }
    text
}

fn round_trip(provider: &ResolvedModel, request: ModelRequest, answer: &str, signed: &[String]) {
    let original = projection::body(provider, &request).unwrap();
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None).unwrap();
    let restored = PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None).unwrap();
    assert_eq!(
        original,
        projection::body(provider, restored.request()).unwrap()
    );
    for adapter in [
        Adapter::Gemini,
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Chat,
    ] {
        let (target, _) = super::provider(adapter, 200);
        let body = projection::body(&target, restored.request()).unwrap();
        assert_eq!(
            visible(&body, adapter),
            if adapter == Adapter::Anthropic && answer.trim().is_empty() {
                ""
            } else {
                answer
            },
            "{adapter:?} repeated or lost visible text"
        );
    }
    // GEM-THK-01/04 and O-9: each signed part stays byte-exact and separate.
    let encoded = serde_json::to_vec(
        &projection::body_named(
            provider,
            restored.request(),
            &crate::tool_names::Names::new(restored.request()),
            &provider.reasoning_origin(provider.key.as_deref(), None),
            &mut crate::files::FileText::default(),
        )
        .unwrap(),
    )
    .unwrap();
    for part in signed {
        assert_eq!(
            encoded
                .windows(part.len())
                .filter(|bytes| *bytes == part.as_bytes())
                .count(),
            1
        );
    }
}

#[test]
fn signed_answer_fixture_replays_once_with_exact_native_parts() {
    let (provider, _) = provider(Adapter::Gemini, 200);
    let (prepared, dispatch) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    let seed = include_bytes!("../fuzz/corpus/provider_stream/gemini-signed-answer.sse");
    let stream = std::str::from_utf8(&seed[1..]).unwrap();
    let events = futures::executor::block_on(
        stream::events_named(
            Adapter::Gemini,
            chunks(stream, 1),
            4096,
            dispatch,
            None,
            Some(provider.reasoning_origin(provider.key.as_deref(), None)),
        )
        .try_collect(),
    )
    .unwrap();
    let mut answer = String::new();
    append(events, &mut request, &mut answer);
    let signed: Vec<_> = request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::Reasoning { block } => {
                let value: Value = serde_json::from_slice(&block.continuity).unwrap();
                value
                    .get("thoughtSignature")
                    .map(|_| String::from_utf8(block.continuity.clone()).unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(signed.len(), 3, "including the empty signed part");
    assert_eq!(answer, "prefix\nhé\"llo\\ tail");
    finish(&mut request, &answer);
    round_trip(&provider, request, &answer, &signed);
    // Negative control: the old native-parts + final-answer path doubles text.
    assert_ne!(format!("{answer}{answer}"), answer);
    crate::fuzzing::assert_stream_invariants(seed);
}

#[test]
fn shrunk_split_answer_fixture_preserves_text_for_every_target() {
    let request: ModelRequest =
        serde_json::from_str(include_str!("fixtures/split-answer.json")).unwrap();
    let (provider, _) = provider(Adapter::Gemini, 200);
    round_trip(&provider, request, "¡¡", &[]);
}

#[test]
fn malformed_native_part_fields_fail_before_publication() {
    for part in [
        json!({"text":"answer","thought":"true"}),
        json!({"text":"answer","thoughtSignature":1}),
        json!({"text":"answer","functionCall":{"name":"echo","args":{}}}),
    ] {
        let input = format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"parts":[part]},"finishReason":"STOP"}]})
        );
        let result = futures::executor::block_on(
            stream::events(Adapter::Gemini, chunks(&input, 1), 4096, parser_dispatch())
                .try_collect::<Vec<_>>(),
        );
        assert!(matches!(result, Err(Error::Invalid(_))));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn signature_boundaries_survive_chunks_tool_steps_and_foreign_projection(
        fragments in prop::collection::vec((prop::collection::vec(any::<char>(),0..24),any::<bool>()),1..8),
        width in 1usize..64, tools in any::<bool>(), split_answer in any::<bool>(),
    ) {
        let (provider, _) = provider(Adapter::Gemini, 200);
        let (initial, dispatch) = request(&provider).unwrap();
        let mut request = initial.request().clone();
        request.tools.push(acyclic_harness::tool::ToolDefinition {
            name:"echo".into(),revision:"1".into(),description:"echo".into(),
            input_schema:json!({"type":"object"}),output_schema:json!({}),projection_schema:json!({}),
        });
        let mut answer = String::new();
        let mut signed = Vec::new();
        for step in 0..=u32::from(tools) {
            let mut parts = Vec::new();
            for (index,(text,sign)) in fragments.iter().enumerate() {
                let text:String=text.iter().collect();
                let part=if *sign {
                    format!("{{ \"text\" : {}, \"thoughtSignature\" : \"signature-{step}-{index}\" }}",json!(text))
                } else {json!({"text":text}).to_string()};
                if *sign {signed.push(part.clone());}
                parts.push(part);
            }
            if tools && step==0 {
                parts.push(json!({"thoughtSignature":"tool-signature","functionCall":{"id":"call_0","name":"echo","args":{}}}).to_string());
            }
            let input=format!("data: {{\"candidates\":[{{\"content\":{{\"parts\":[{}]}},\"finishReason\":\"STOP\"}}]}}\n\n",parts.join(","));
            let events=futures::executor::block_on(stream::events_named(
                Adapter::Gemini,chunks(&input,width),4096,ModelDispatch {step,..dispatch},None,
                Some(provider.reasoning_origin(provider.key.as_deref(),None)),
            ).try_collect()).unwrap();
            append(events,&mut request,&mut answer);
        }
        finish(&mut request,&answer);
        if split_answer {
            let index=request.messages.len()-2;
            request.messages[index].content=ModelContent::Parts(answer.chars().map(|ch| {
                ModelContentPart::Text {text:ch.to_string()}
            }).collect());
        }
        round_trip(&provider,request,&answer,&signed);
    }
}

#[tokio::test]
async fn signed_answer_native_stream_keeps_exact_custody() -> Result<()> {
    let (mut provider, _) = provider(Adapter::Gemini, 200);
    let seed = include_bytes!("../fuzz/corpus/provider_stream/gemini-signed-answer.sse");
    provider.transport = crate::deadline::Bounded::new(
        Arc::new(super::native_tests::Fixture::new(
            std::str::from_utf8(&seed[1..]).unwrap(),
        )),
        crate::TimeoutSpec::DEFAULT,
    );
    let (prepared, dispatch) = request(&provider)?;
    let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
    let baseline = work.live_allocation_bytes();
    let mut events = Vec::new();
    let mut holds = Vec::new();
    {
        let mut stream = provider
            .generate_with_native_work(&prepared, dispatch, &work)?
            .await?;
        while let Some(event) = stream.next().await {
            let (event, custody) = event?;
            assert_eq!(custody.bytes(), event.logical_owned_bytes()?);
            events.push(event);
            holds.push(custody);
        }
    }
    crate::projection::gemini::assert_visible_replay(&events);
    drop((events, holds));
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
