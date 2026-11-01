use super::*;
use acyclic_harness_core::model::{ModelContentPart, ToolResultContent};
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
            ModelEvent::TextPart { text, native } => ModelContentPart::Text { text, native },
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

/// Native bytes on every kind of part, in transcript order.
fn natives(events: &[ModelEvent]) -> Vec<&[u8]> {
    events
        .iter()
        .filter_map(|event| match event {
            ModelEvent::ReasoningBlock { block } => Some(block.continuity.as_slice()),
            ModelEvent::TextPart { native, .. } => native.as_ref().map(|n| n.continuity.as_slice()),
            ModelEvent::ToolCall(call) => call.native.as_ref().map(|n| n.continuity.as_slice()),
            _ => None,
        })
        .collect()
}

/// The answer already rides on its text parts; the turn ends with a user message.
fn finish(request: &mut ModelRequest) {
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
    let events: Vec<ModelEvent> = futures::executor::block_on(
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
    let signed: Vec<_> = natives(&events)
        .into_iter()
        .filter(|bytes| {
            serde_json::from_slice::<Value>(bytes).unwrap()["thoughtSignature"].is_string()
        })
        .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
        .collect();
    let mut answer = String::new();
    append(events, &mut request, &mut answer);
    assert_eq!(signed.len(), 3, "including the empty signed part");
    assert_eq!(answer, "prefix\nhé\"llo\\ tail");
    finish(&mut request);
    round_trip(&provider, request, &answer, &signed);
    crate::fuzzing::assert_stream_invariants(seed);
}

#[test]
fn parallel_signed_calls_preserve_the_entire_provider_part() {
    let (provider, _) = provider(Adapter::Gemini, 200);
    let (prepared, dispatch) = request(&provider).unwrap();
    let mut request = prepared.request().clone();
    request
        .tools
        .push(acyclic_harness_core::tool::ToolDefinition {
            name: "read".into(),
            revision: "1".into(),
            description: "read".into(),
            input_schema: json!({"type":"object","properties":{"path":{"type":"string"}}}),
            output_schema: json!({}),
            projection_schema: json!({}),
        });
    // Generated adversarial inputs, not a captured external transcript.
    // Missing provider IDs, unusual key order and escaped signature bytes
    // expose reconstruction even when the decoded JSON is equivalent.
    let signed = [
        r#"{ "thoughtSignature":"one\u003d", "functionCall": {"args":{"path":"a"},"name":"r\u0065ad","extension":{"kept":true}} }"#.to_owned(),
        r#"{"functionCall":{"id":"call\u003d","name":"read","args":{"path":"b"}},"thoughtSignature":"two=="}"#.to_owned(),
    ];
    let input = format!(
        "data: {{\"candidates\":[{{\"content\":{{\"parts\":[{},{}]}},\"finishReason\":\"STOP\"}}]}}\n\n",
        signed[0], signed[1]
    );
    let events = futures::executor::block_on(
        stream::events_named(
            Adapter::Gemini,
            chunks(&input, 1),
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
    finish(&mut request);
    // Native Part data is a union. A matching canonical call cannot authorize
    // replaying a second data field hidden in the same signed envelope.
    for extra in [
        "text",
        "inlineData",
        "functionResponse",
        "fileData",
        "executableCode",
        "codeExecutionResult",
        "toolCall",
        "toolResponse",
    ] {
        let mut changed = request.clone();
        let native = changed
            .messages
            .iter_mut()
            .find_map(|message| match &mut message.content {
                ModelContent::Part(ModelContentPart::ToolCall(call)) => call.native.as_mut(),
                _ => None,
            })
            .unwrap();
        let mut part: Value = serde_json::from_slice(&native.continuity).unwrap();
        part[extra] = if extra == "text" {
            json!("ambiguous")
        } else {
            json!({})
        };
        native.continuity = serde_json::to_vec(&part).unwrap();
        assert!(
            projection::body(&provider, &changed).is_err(),
            "ambiguous signed {extra} must refuse"
        );
        let mut body = projection::body(&provider, &request).unwrap();
        body["contents"][1]["parts"][0] = part;
        assert!(crate::validate_input(Adapter::Gemini, &body).contains(&"GEM-SEQ-03"));
        let (foreign, _) = super::provider(Adapter::Responses, 200);
        assert!(
            !projection::body(&foreign, &changed)
                .unwrap()
                .to_string()
                .contains("thoughtSignature")
        );
    }
    for field in ["name", "arguments", "binding"] {
        let mut changed = request.clone();
        let call = changed
            .messages
            .iter_mut()
            .find_map(|message| match &mut message.content {
                ModelContent::Part(ModelContentPart::ToolCall(call)) => Some(call),
                _ => None,
            })
            .unwrap();
        match field {
            "name" => call.name = "forged".into(),
            "arguments" => call.arguments = json!({"path":"forged"}),
            _ => call.metadata["provider_call"]["id"] = json!("forged"),
        }
        assert!(
            projection::body(&provider, &changed).is_err(),
            "signed {field} mismatch must refuse"
        );
        // Crossing providers drops the native part instead of manufacturing a
        // signature for this changed canonical call.
        let (foreign, _) = super::provider(Adapter::Responses, 200);
        let body = projection::body(&foreign, &changed).unwrap();
        assert!(!body.to_string().contains("thoughtSignature"));
    }
    assert!(
        crate::validate_input(
            Adapter::Gemini,
            &projection::body(&provider, &request).unwrap()
        )
        .is_empty()
    );
    round_trip(&provider, request, &answer, &signed);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn mock_wire_preserves_signed_parallel_calls_usage_and_prompt_prefix(
        signature in "[a-zA-Z0-9+/]{1,48}={0,2}", width in 1usize..48,
        cached in 0u64..=4096, thoughts in 0u64..4096,
        native_id in prop::option::of(prop_oneof![
            Just("harness_call_0".to_owned()),
            Just("harness_call_2".to_owned()),
            Just("opaque:/=界".repeat(8)),
        ]),
    ) {
        use harness_mock_model::{Gemini, Step, events_with_usage};
        // Generated protocol data, not a captured provider fixture. The shared
        // mock server emits the wire; production ingress and projection own the
        // round trip, including exact native-part bytes.
        let (provider, _) = provider(Adapter::Gemini, 200);
        let (prepared, dispatch) = request(&provider).unwrap();
        let mut request = prepared.request().clone();
        request.tools.push(acyclic_harness_core::tool::ToolDefinition {
            name: "read_file".into(), revision: "1".into(), description: "read".into(),
            input_schema: json!({"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}),
            output_schema: json!({}), projection_schema: json!({}),
        });
        let encode = |request: &ModelRequest| {
            serde_json::to_vec(&projection::body_named(
                &provider, request, &crate::tool_names::Names::new(request),
                &provider.reasoning_origin(provider.key.as_deref(), None),
                &mut crate::files::FileText::default(),
            ).unwrap()).unwrap()
        };
        let prefix = encode(&request);
        let mut first = json!({"thoughtSignature":signature,"functionCall":{"name":"read_file","args":{"file_path":"a"}}});
        if let Some(id) = &native_id { first["functionCall"]["id"] = json!(id); }
        let parts = vec![
            first,
            json!({"functionCall":{"id":"second","name":"read_file","args":{"file_path":"b"}}}),
        ];
        let signed = vec![parts[0].to_string()];
        let mut step = Step {
            gemini: Some(Gemini {parts, cached_tokens:cached, thought_tokens:thoughts}),
            ..Default::default()
        };
        let path = "models/tiny:streamGenerateContent";
        let wire = events_with_usage(path, &step, 4096).unwrap().concat();
        let parsed = futures::executor::block_on(stream::events_named(
            Adapter::Gemini, chunks(&wire, width), 32768, dispatch, None,
            Some(provider.reasoning_origin(provider.key.as_deref(), None)),
        ).try_collect::<Vec<_>>()).unwrap();
        let calls: Vec<_> = parsed.iter().filter_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call), _ => None,
        }).collect();
        prop_assert_eq!(calls.len(), 2);
        prop_assert_eq!(&calls[0].call_id, &dispatch.tool_call_id(0));
        prop_assert_eq!(&calls[1].call_id, &dispatch.tool_call_id(1));
        prop_assert_ne!(&calls[0].call_id, &calls[1].call_id);
        let metadata = parsed.iter().find_map(|event| match event {
            ModelEvent::Completed {metadata} => Some(metadata), _ => None,
        }).unwrap();
        prop_assert_eq!(&metadata["usage"], &json!({
            "promptTokenCount":4096,"cachedContentTokenCount":cached,
            "candidatesTokenCount":2,"thoughtsTokenCount":thoughts,
            "totalTokenCount":4098+thoughts,
        }));
        let mut answer = String::new();
        append(parsed, &mut request, &mut answer);
        // Foreign exchanges retain their canonical opaque IDs. Appending a
        // lexically earlier ID must not rename any prior native/foreign call.
        let mut exchange_prefix = Vec::new();
        for (index, (id, path)) in [("foreign_call", "c"), ("earlier_call", "d")].into_iter().enumerate() {
            request.messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(
                    acyclic_harness_core::model::ToolCall::new(
                        id.into(), "read_file".into(), json!({"file_path":path}),
                    ),
                )),
            });
            request.messages.push(ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: id.into(), name: "read_file".into(),
                    content: ToolResultContent::Json {value:json!({"ok":true})},
                }),
            });
            if index == 0 { exchange_prefix = encode(&request); }
        }
        finish(&mut request);
        let continued = encode(&request);
        let body: Value = serde_json::from_slice(&continued).unwrap();
        let returned_id = &body["contents"][1]["parts"][0]["functionCall"]["id"];
        let response_id = &body["contents"][2]["parts"][0]["functionResponse"]["id"];
        prop_assert_eq!(returned_id.as_str(), native_id.as_deref());
        prop_assert_eq!(response_id.as_str(), native_id.as_deref());
        let foreign_id = body["contents"][3]["parts"][0]["functionCall"]["id"].as_str().unwrap();
        prop_assert_ne!(Some(foreign_id), native_id.as_deref());
        // The closing contents array and object are the only bytes replaced by
        // an append. Static tools, system instruction and prior contents retain
        // their positions and bytes, so implicit caching sees the same prefix.
        prop_assert!(continued.starts_with(&prefix[..prefix.len()-2]));
        prop_assert!(continued.starts_with(&exchange_prefix[..exchange_prefix.len()-2]));
        let mut changed = request.clone();
        changed.tools[0].description.push('!');
        prop_assert!(!encode(&changed).starts_with(&prefix[..prefix.len()-2]));
        round_trip(&provider, request, &answer, &signed);

        // Removing the original terminal producer must end the stream with a
        // typed interruption, without admitting either tool in a partial batch.
        step.hold = true;
        let unfinished = events_with_usage(path, &step, 4096).unwrap().concat();
        let parsed = futures::executor::block_on(stream::events_named(
            Adapter::Gemini, chunks(&unfinished, width), 32768, dispatch, None,
            Some(provider.reasoning_origin(provider.key.as_deref(), None)),
        ).collect::<Vec<_>>());
        prop_assert_eq!(parsed.len(), 1);
        prop_assert!(matches!(&parsed[0], Err(Error::ModelUnavailable {
            failure: acyclic_harness_core::ModelTransportFailure::Interrupted, ..
        })), "producer loss must terminate the batch with its typed error");
        for wrong in ["responses", "messages", "chat/completions"] {
            prop_assert!(events_with_usage(wrong, &step, 4096).is_none());
        }
        step.gemini.as_mut().unwrap().cached_tokens = 4097;
        prop_assert!(events_with_usage(path, &step, 4096).is_none());
    }
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
    let mut parts = vec![
        json!([]),
        json!(["answer"]),
        json!([null,null,null,{"name":"echo","args":{}}]),
        json!({"text":"answer","thought":"true"}),
        json!({"text":"answer","thoughtSignature":1}),
        json!({"text":"answer","functionCall":{"name":"echo","args":{}}}),
    ];
    // Generated union/shape mutations, not captured provider fixtures. The
    // original parser's retained call must also be withheld on a later error.
    let fields = [
        "text",
        "inlineData",
        "functionCall",
        "functionResponse",
        "fileData",
        "executableCode",
        "codeExecutionResult",
        "toolCall",
        "toolResponse",
    ];
    // Shape admission must not reject every opaque union member. This checks
    // only the outer Part contract, not a server tool's nested protocol.
    for field in fields
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
    {
        let payload = match field {
            Some("text") => json!("answer"),
            Some("functionCall") => json!({"name":"echo"}),
            _ => json!({}),
        };
        let member = field.map_or_else(
            || r#", "text":null, "functionCall":null, "thought":null"#.into(),
            |field| format!(",{field:?}:{payload}"),
        );
        let raw = format!(
            r#"{{ "thoughtSignature":"original\u003d", "extension":{{"kept":true}}{member} }}"#
        );
        let item = if field == Some("functionCall") {
            crate::projection::gemini::call(raw.as_bytes(), None, "echo", &json!({})).unwrap()
        } else {
            crate::projection::gemini::thought(raw.as_bytes()).unwrap()
        };
        assert_eq!(item.get(), raw);
        let input = format!(
            "data: {{\"candidates\":[{{\"content\":{{\"parts\":[{raw}]}},\"finishReason\":\"STOP\"}}]}}\n\n"
        );
        let events = futures::executor::block_on(
            stream::events(Adapter::Gemini, chunks(&input, 1), 4096, parser_dispatch())
                .try_collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(natives(&events).contains(&raw.as_bytes()));
    }
    // Native continuity may carry only thought metadata. Local wire admission
    // must accept the same exact model part as ingress/replay, without adding
    // a text member merely to satisfy the local validator.
    for part in [
        json!({"thoughtSignature":"original==","extension":{"kept":true}}),
        json!({"thought":true}),
    ] {
        let raw = serde_json::to_vec(&part).unwrap();
        let item = crate::projection::gemini::thought(&raw).unwrap();
        assert_eq!(item.get().as_bytes(), raw);
        let (provider, _) = provider(Adapter::Gemini, 200);
        let (prepared, dispatch) = request(&provider).unwrap();
        let input = format!(
            "data: {{\"candidates\":[{{\"content\":{{\"parts\":[{}]}},\"finishReason\":\"STOP\"}}]}}\n\n",
            std::str::from_utf8(&raw).unwrap(),
        );
        let events = futures::executor::block_on(
            stream::events_named(
                Adapter::Gemini,
                chunks(&input, 1),
                4096,
                dispatch,
                None,
                Some(provider.reasoning_origin(provider.key.as_deref(), None)),
            )
            .try_collect::<Vec<_>>(),
        )
        .unwrap();
        let mut request = prepared.request().clone();
        let mut answer = String::new();
        append(events, &mut request, &mut answer);
        finish(&mut request);
        round_trip(
            &provider,
            request,
            &answer,
            &[std::str::from_utf8(&raw).unwrap().to_owned()],
        );
        let mut body = json!({"contents":[
            {"role":"user","parts":[{"text":"question"}]},
            {"role":"model","parts":[part]},
            {"role":"user","parts":[{"text":"continue"}]},
        ]});
        assert!(crate::input::validate_input(Adapter::Gemini, &body).is_empty());
        // Moving provider thought metadata onto a user turn must not create a
        // new valid native continuity position.
        body["contents"][1]["role"] = json!("user");
        assert!(crate::input::validate_input(Adapter::Gemini, &body).contains(&"GEM-SEQ-03"));
    }
    for (index, first) in fields.iter().enumerate() {
        for second in &fields[index + 1..] {
            let mut part = json!({"thoughtSignature":"original==","extension":{"kept":true}});
            for field in [*first, *second] {
                part[field] = if field == "text" {
                    json!("answer")
                } else if field == "functionCall" {
                    json!({"name":"echo","args":{}})
                } else {
                    json!({})
                };
            }
            parts.push(part);
        }
        if *first != "text" {
            for value in [json!("object"), json!(false), json!(1), json!([])] {
                let mut part = json!({"thoughtSignature":"original=="});
                part[*first] = value;
                parts.push(part);
            }
        }
    }
    for field in ["name", "id", "args"] {
        for value in [json!(null), json!(false), json!(1), json!([])] {
            let mut part =
                json!({"thoughtSignature":"original==","functionCall":{"name":"echo","args":{}}});
            part["functionCall"][field] = value;
            parts.push(part);
        }
    }
    parts.push(json!({"thoughtSignature":"original==","functionCall":{"args":{}}}));
    parts.push(json!({"thoughtSignature":"original==","functionCall":{"name":"echo","args":"{}"}}));
    let unsigned: Vec<_> = parts
        .iter()
        .filter(|part| part["thoughtSignature"].is_string())
        .map(|part| {
            let mut part = part.clone();
            part.as_object_mut().unwrap().remove("thoughtSignature");
            part
        })
        .collect();
    parts.extend(unsigned);
    let rejects = |raw: &str| {
        assert!(crate::projection::gemini::part(raw.as_bytes()).is_err());
        let input = format!(
            "data: {{\"candidates\":[{{\"content\":{{\"parts\":[{{\"functionCall\":{{\"name\":\"prior\",\"args\":{{}}}},\"thoughtSignature\":\"prior==\"}},{raw}]}},\"finishReason\":\"STOP\"}}]}}\n\n"
        );
        let events = futures::executor::block_on(
            stream::events(Adapter::Gemini, chunks(&input, 1), 4096, parser_dispatch())
                .collect::<Vec<_>>(),
        );
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Err(Error::Invalid(_))));
    };
    for part in parts {
        rejects(&part.to_string());
    }
    for field in fields {
        let payload = match field {
            "text" => json!("answer"),
            "functionCall" => json!({"name":"echo","args":{}}),
            _ => json!({}),
        };
        for signature in ["", r#""thoughtSignature":"original==","#] {
            rejects(&format!(
                r#"{{{signature}{field:?}:{payload},{field:?}:{payload}}}"#
            ));
            rejects(&format!(
                r#"{{{signature}{field:?}:null,{field:?}:{payload}}}"#
            ));
        }
    }
    for field in ["thought", "thoughtSignature"] {
        let payload = if field == "thought" {
            "true"
        } else {
            r#""original==""#
        };
        rejects(&format!(r#"{{{field:?}:null,{field:?}:{payload}}}"#));
    }
    for function in [
        r#"{"id":"call","id":"call","name":"echo","args":{}}"#,
        r#"{"name":"echo","name":"echo","args":{}}"#,
        r#"{"name":"echo","args":{},"args":{}}"#,
        r#"{"id":null,"id":"call","name":"echo","args":{}}"#,
        r#"{"name":"echo","args":null,"args":{}}"#,
    ] {
        for signature in ["", r#""thoughtSignature":"original==","#] {
            rejects(&format!(r#"{{{signature}"functionCall":{function}}}"#));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn signature_boundaries_survive_chunks_tool_steps_and_foreign_projection(
        fragments in prop::collection::vec((prop::collection::vec(any::<char>(),0..24),any::<bool>()),1..8),
        width in 1usize..64, tools in any::<bool>(),
    ) {
        let (provider, _) = provider(Adapter::Gemini, 200);
        let (initial, dispatch) = request(&provider).unwrap();
        let mut request = initial.request().clone();
        request.tools.push(acyclic_harness_core::tool::ToolDefinition {
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
        finish(&mut request);
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
