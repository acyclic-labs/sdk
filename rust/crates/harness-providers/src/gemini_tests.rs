use super::*;
use acyclic_harness_core::model::{
    ModelContentPart, ReasoningBlock, ReasoningKind, ReasoningOutcome, ToolCall, ToolResultContent,
};

#[tokio::test]
async fn absent_arguments_are_empty_but_invalid_supplied_arguments_are_rejected() -> Result<()> {
    for args in [
        None,
        Some(json!({})),
        Some(json!(null)),
        Some(json!([])),
        Some(json!("{}")),
    ] {
        let valid = args.as_ref().is_none_or(serde_json::Value::is_object);
        let mut call = json!({"name":"echo"});
        if let Some(args) = args {
            call["args"] = args;
        }
        let frame = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":call}]},"finishReason":"STOP"}]});
        let parsed = stream::events(
            Adapter::Gemini,
            chunks(&format!("data: {frame}\n\n"), 1),
            4096,
            parser_dispatch(),
        )
        .try_collect::<Vec<_>>()
        .await;
        if valid {
            let events = parsed?;
            let ModelEvent::ToolCall(call) = &events[0] else {
                panic!("missing call")
            };
            assert_eq!(call.arguments, json!({}));
        } else {
            assert!(matches!(
                parsed,
                Err(acyclic_harness_core::Error::Invalid(_))
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_or_malformed_native_tool_batches_never_execute() -> Result<()> {
    // Generated adversarial protocol data, not captured provider fixtures.
    let mut frames = Vec::new();
    for (reason, id) in [
        ("MAX_TOKENS", Some(json!("call"))),
        ("STOP", Some(json!(null))),
        ("STOP", Some(json!(42))),
        ("STOP", Some(json!(""))),
    ] {
        let mut function = json!({"name":"echo","args":{}});
        if let Some(id) = id {
            function["id"] = id;
        }
        frames.push(json!({"candidates":[{"content":{"parts":[{
            "functionCall":function,"thoughtSignature":"opaque"
        }]},"finishReason":reason}]}));
    }
    let valid = json!({"content":{"role":"model","parts":[{
        "functionCall":{"name":"echo","args":{}},"thoughtSignature":"opaque"
    }]},"finishReason":"STOP"});
    frames.push(json!({"candidates":true}));
    frames.push(json!({"candidates":[true]}));
    frames.push(json!({"candidates":[valid,valid]}));
    for index in [json!(1), json!(-1), json!(true), json!("0")] {
        let mut candidate = valid.clone();
        candidate["index"] = index;
        frames.push(json!({"candidates":[candidate]}));
    }
    for role in [json!("user"), json!(true)] {
        let mut candidate = valid.clone();
        candidate["content"]["role"] = role;
        frames.push(json!({"candidates":[candidate]}));
    }
    for parts in [json!(true), json!({}), json!([true]), json!([{"text":42}])] {
        let mut candidate = valid.clone();
        candidate["content"]["parts"] = parts;
        frames.push(json!({"candidates":[candidate]}));
    }
    let mut candidate = valid;
    candidate["finishReason"] = json!(true);
    frames.push(json!({"candidates":[candidate]}));
    let prefix = json!({"candidates":[{"content":{"parts":[{
        "functionCall":{"name":"prior","args":{}},"thoughtSignature":"prior-signature"
    }]}}]});
    for frame in frames {
        let events = stream::events(
            Adapter::Gemini,
            chunks(&format!("data: {prefix}\n\ndata: {frame}\n\n"), 1),
            4096,
            parser_dispatch(),
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            Err(acyclic_harness_core::Error::Invalid(_))
        ));
    }
    Ok(())
}

#[test]
fn parallel_batches_and_signatures_survive_prepared_request_replay() -> Result<()> {
    let (provider, _) = provider(Adapter::Gemini, 200);
    let (initial, _) = request(&provider)?;
    let mut request = initial.request().clone();
    request
        .tools
        .push(acyclic_harness_core::tool::ToolDefinition {
            name: "echo".into(),
            revision: "1".into(),
            description: "echo".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({}),
            projection_schema: json!({}),
        });
    for step in 0..2 {
        for index in 0..2 {
            let mut parts = Vec::new();
            if index == 0 {
                parts.push(ModelContentPart::Reasoning { block: ReasoningBlock {
                    kind: ReasoningKind::Signature,
                    text: None,
                    origin: provider.reasoning_origin(provider.key.as_deref(), None),
                    continuity: serde_json::to_vec(&json!({"functionCall":{"id":format!("{step}-{index}"),"name":"echo","args":{}},"thoughtSignature":format!("signature-{step}")})).expect("native Gemini part"),
                    outcome: ReasoningOutcome::PreservedOpaque,
                }});
            }
            parts.push(ModelContentPart::ToolCall(ToolCall::new(
                format!("{step}-{index}"),
                "echo".into(),
                json!({}),
            )));
            request.messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Parts(parts),
            });
        }
        for index in 0..2 {
            request.messages.push(ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: format!("{step}-{index}"),
                    name: "echo".into(),
                    content: ToolResultContent::Json {
                        value: json!({"ok":true}),
                    },
                }),
            });
        }
    }
    let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    let replayed = PreparedModelRequest::decode(prepared.bytes(), Limits::default(), None)?;
    for request in [prepared.request(), replayed.request()] {
        let body = projection::body(&provider, request)?;
        let contents = body["contents"].as_array().expect("contents");
        assert_eq!(contents.len(), 5);
        for step in 0..2 {
            let model = &contents[1 + 2 * step];
            let results = &contents[2 + 2 * step];
            assert_eq!(model["role"], "model");
            assert_eq!(results["role"], "user");
            assert_eq!(model["parts"].as_array().expect("calls").len(), 2);
            assert_eq!(results["parts"].as_array().expect("results").len(), 2);
            assert_eq!(
                model["parts"][0]["thoughtSignature"],
                format!("signature-{step}")
            );
            assert!(model["parts"][1].get("thoughtSignature").is_none());
            for index in 0..2 {
                assert_eq!(
                    model["parts"][index]["functionCall"]["id"],
                    format!("{step}-{index}")
                );
                assert_eq!(
                    results["parts"][index]["functionResponse"]["id"],
                    format!("{step}-{index}")
                );
            }
        }
    }
    Ok(())
}
