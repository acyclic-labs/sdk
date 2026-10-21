use super::*;
use acyclic_harness_core::{
    Error,
    model::{ModelContentPart, ToolResultContent},
};
use std::collections::VecDeque;

fn total(count: &acyclic_harness_core::context::ModelTokenCount) -> u32 {
    count.fixed_tokens + count.message_tokens.iter().sum::<u32>()
}

#[tokio::test]
async fn token_estimates_start_at_two_bytes_and_only_tighten_per_resolved_model() -> Result<()> {
    for (adapter, fixture) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        assert_eq!(provider.estimator.tokens(1000), Some(500));
        let dense = fixture
            .replace("\"input_tokens\":5", "\"input_tokens\":512")
            .replace("\"prompt_tokens\":5", "\"prompt_tokens\":512")
            .replace("\"promptTokenCount\":5", "\"promptTokenCount\":512");
        provider.transport = crate::deadline::Bounded::new(
            Arc::new(ToolLoop {
                replies: Mutex::new(VecDeque::from([dense, fixture.into()])),
                sent: Mutex::new(Vec::new()),
            }),
            crate::TimeoutSpec::DEFAULT,
        );
        let (prepared, dispatch) = request(&provider)?;
        let initial = total(&projection::plan(&provider, &prepared, 512)?);
        let bytes = serde_json::to_vec(&projection::body(&provider, prepared.request())?)
            .expect("encode fixture")
            .len();
        assert!(initial > 0);
        assert_eq!(
            provider.estimator.tokens(bytes),
            Some(bytes.div_ceil(2) as u32)
        );
        provider
            .generate(prepared.clone(), dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        let calibrated = total(&projection::plan(&provider, &prepared, 512)?);
        assert!(
            calibrated > initial,
            "{adapter:?}: {initial} -> {calibrated}"
        );
        // Usage calibrates provider wire bytes. Canonical transcript weights
        // differ with framing and are not verified admission token counts.
        assert!(provider.estimator.tokens(bytes).expect("wire estimate") >= 512);
        provider
            .generate(prepared.clone(), dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            total(&projection::plan(&provider, &prepared, 512)?),
            calibrated
        );
        let (other, _) = super::provider(adapter, 200);
        assert_eq!(total(&projection::plan(&other, &prepared, 512)?), initial);
    }
    Ok(())
}

#[test]
fn dense_first_request_after_resume_requires_compaction_before_dispatch() -> Result<()> {
    use acyclic_harness_core::context::ThresholdCompaction;
    for (adapter, _) in fixtures() {
        // A resumed process resolves a fresh model: no previous usage calibration.
        let (provider, script) = provider(adapter, 200);
        let (original, _) = request(&provider)?;
        let mut resumed = original.request().clone();
        resumed.messages[1].content = ModelContent::Text("漢".repeat(5000));
        let prepared = PreparedModelRequest::prepare(resumed, Limits::default(), None)?;
        let count = futures::executor::block_on(provider.count_tokens(&prepared))?.input_tokens;
        let capacity = provider.context_capacity(&provider.model)?;
        let policy = ThresholdCompaction {
            response_reserve_tokens: 1024,
            recent_tokens: 1024,
            ..ThresholdCompaction::default()
        };
        assert!(policy.needs_compaction(capacity, u64::from(count))?);
        // No generation has started. A fresh process uses the verified count;
        // previous usage and a sparse calibration cannot weaken admission.
        assert!(script.sent.lock().expect("fixture requests").is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn responses_content_filter_and_unknown_incomplete_reasons_are_invalid() {
    for reason in [
        json!("content_filter"),
        json!("unknown"),
        serde_json::Value::Null,
    ] {
        for (event_type, status) in [
            ("response.incomplete", "incomplete"),
            ("response.completed", "incomplete"),
        ] {
            let event = json!({"type":event_type,"response":{
                "status":status,"incomplete_details":{"reason":reason}
            }});
            let fixture = format!("data: {event}\n\n");
            let error = stream::events(
                Adapter::Responses,
                chunks(&fixture, 1),
                4096,
                parser_dispatch(),
            )
            .try_collect::<Vec<_>>()
            .await
            .expect_err("incomplete response");
            assert!(matches!(error, Error::Invalid(_)));
        }
    }
}

#[tokio::test]
async fn http_status_classes_are_distinct_and_refusals_carry_only_a_bounded_excerpt() -> Result<()>
{
    for (adapter, body) in fixtures() {
        for status in [401, 403, 408, 429, 500, 502, 503, 400, 404] {
            let (provider, script) = provider(adapter, status);
            let (prepared, dispatch) = request(&provider)?;
            let error = provider
                .generate(prepared, dispatch)
                .try_collect::<Vec<_>>()
                .await
                .expect_err("HTTP failure");
            assert!(error.to_string().contains(&status.to_string()));
            assert!(!error.to_string().contains("scripted-key"));
            let sent = script
                .sent
                .lock()
                .map_err(|_| invalid("fixture lock"))?
                .len();
            match status {
                408 | 429 | 500..=599 => {
                    // Redelivered within the default budget, then a typed refusal.
                    assert_eq!(sent, 1 + usize::from(crate::RetrySpec::DEFAULT.attempts));
                    let Error::ModelUnavailable {
                        failure: acyclic_harness_core::ModelTransportFailure::Status(found),
                        excerpt,
                    } = &error
                    else {
                        panic!("{adapter:?}: {error}");
                    };
                    assert_eq!(*found, status);
                    assert!(excerpt.len() <= crate::EXCERPT_BYTES && !excerpt.is_empty());
                    assert!(!excerpt.chars().any(char::is_control));
                    let flat: String = body
                        .chars()
                        .map(|c| if c.is_control() { ' ' } else { c })
                        .collect();
                    assert!(flat.trim().starts_with(excerpt.as_str()));
                }
                // Negative controls: other statuses are never redelivered nor excerpted.
                401 | 403 => {
                    assert!(matches!(error, Error::Unauthorized(_)));
                    assert_eq!(sent, 1);
                    assert!(!error.to_string().contains("usage"));
                }
                _ => {
                    assert!(matches!(error, Error::Invalid(_)));
                    assert_eq!(sent, 1);
                    assert!(!error.to_string().contains("usage"));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn interrupted_bodies_are_transient_and_protocol_violations_are_invalid() -> Result<()> {
    for (adapter, _) in fixtures() {
        for input in ["data: {}\n\n", "data: {", ""] {
            let error = stream::events(adapter, chunks(input, 1), 4096, parser_dispatch())
                .try_collect::<Vec<_>>()
                .await
                .expect_err("unfinished body");
            assert!(
                matches!(
                    error,
                    Error::ModelUnavailable {
                        failure: acyclic_harness_core::ModelTransportFailure::Interrupted,
                        ..
                    }
                ),
                "{adapter:?}: {error}"
            );
        }
        let error = stream::events(
            adapter,
            chunks("data: invalid\n\n", 1),
            4096,
            parser_dispatch(),
        )
        .try_collect::<Vec<_>>()
        .await
        .expect_err("bad JSON");
        assert!(matches!(error, Error::Invalid(_)));
        let interrupted =
            futures::stream::once(async { Err(Error::Storage("body interrupted".into())) });
        let error = stream::events(adapter, Box::pin(interrupted), 4096, parser_dispatch())
            .try_collect::<Vec<_>>()
            .await
            .expect_err("transport failure");
        assert!(matches!(error, Error::Storage(_)));
    }
    Ok(())
}

#[tokio::test]
async fn token_limits_complete_uniformly_without_publishing_partial_tool_batches() -> Result<()> {
    for (adapter, fixture) in fixtures() {
        let truncated = match adapter {
            Adapter::Chat => fixture.replace(
                "\"finish_reason\":\"tool_calls\"",
                "\"finish_reason\":\"length\"",
            ),
            Adapter::Anthropic => fixture.replace(
                "\"stop_reason\":\"tool_use\"",
                "\"stop_reason\":\"max_tokens\"",
            ),
            Adapter::Responses => fixture
                .replace("response.completed", "response.incomplete")
                .replace("\"status\":\"completed\"", "\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}"),
            Adapter::Gemini => fixture.replace(
                "\"finishReason\":\"STOP\"",
                "\"finishReason\":\"MAX_TOKENS\"",
            ),
        };
        let events = stream::events(adapter, chunks(&truncated, 1), 4096, parser_dispatch())
            .try_collect::<Vec<_>>()
            .await?;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ModelEvent::ToolCall(_)))
        );
        let Some(ModelEvent::Completed { metadata }) = events.last() else {
            return Err(invalid("no completion"));
        };
        assert_eq!(metadata["finish_reason"], "length");
        assert!(metadata["usage"].is_object());
    }
    let incomplete = concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"partial\",\"name\":\"echo\",\"arguments\":\"{\"}}\n\n",
        "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"
    );
    let events = stream::events(
        Adapter::Responses,
        chunks(incomplete, 1),
        4096,
        parser_dispatch(),
    )
    .try_collect::<Vec<_>>()
    .await?;
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ModelEvent::Completed { .. }));
    Ok(())
}

struct ToolLoop {
    replies: Mutex<VecDeque<String>>,
    sent: Mutex<Vec<HttpRequest>>,
}

impl HttpTransport for ToolLoop {
    fn delay(&self, wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            self.sent
                .lock()
                .map_err(|_| invalid("fixture lock"))?
                .push(request);
            let reply = self
                .replies
                .lock()
                .map_err(|_| invalid("fixture lock"))?
                .pop_front()
                .ok_or_else(|| invalid("unexpected dispatch"))?;
            Ok(HttpResponse {
                retry_after: None,
                status: 200,
                body: chunks(&reply, 1),
            })
        })
    }
}

#[tokio::test]
async fn signed_thinking_survives_a_serialized_tool_call_and_followup_on_reasoning_protocols()
-> Result<()> {
    for adapter in [Adapter::Anthropic, Adapter::Gemini, Adapter::Responses] {
        let first = match adapter {
            Adapter::Anthropic => concat!(
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan\"}}\n\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"signed\"}}\n\n",
                "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call\",\"name\":\"echo\",\"input\":{\"value\":1}}}\n\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            ),
            Adapter::Responses => concat!(
                "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[],\"encrypted_content\":\"encrypted\"}}\n\n",
                "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call\",\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":1}\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
            ),
            _ => {
                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"echo\",\"args\":{\"value\":1}},\"thoughtSignature\":\"signed\"}]},\"finishReason\":\"STOP\"}]}\n\n"
            }
        };
        let second = match adapter {
            Adapter::Responses => concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
            ),
            Adapter::Anthropic => {
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
            }
            _ => {
                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}]},\"finishReason\":\"STOP\"}]}\n\n"
            }
        };
        let script = Arc::new(ToolLoop {
            replies: Mutex::new(VecDeque::from([first.into(), second.into()])),
            sent: Mutex::new(Vec::new()),
        });
        let (mut provider, _) = provider(adapter, 200);
        provider.transport =
            crate::deadline::Bounded::new(script.clone(), crate::TimeoutSpec::DEFAULT);
        let (initial, dispatch) = request(&provider)?;
        let events = provider
            .generate(initial.clone(), dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        // Replay through the durable serde contract rather than an in-memory signature cache.
        let events: Vec<ModelEvent> =
            serde_json::from_slice(&serde_json::to_vec(&events).expect("encode events"))
                .expect("decode events");
        let reasoning: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ModelEvent::ReasoningBlock { block } => Some(ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::Reasoning {
                        block: block.clone(),
                    }),
                }),
                _ => None,
            })
            .collect();
        assert!(
            !reasoning.is_empty(),
            "signed native reasoning is a canonical event"
        );
        let call = events
            .into_iter()
            .find(|event| matches!(event, ModelEvent::ToolCall(_)))
            .ok_or_else(|| invalid("missing tool call"))?;
        let ModelEvent::ToolCall(acyclic_harness_core::model::ToolCall {
            call_id,
            name,
            arguments,
            metadata,
        }) = call
        else {
            unreachable!()
        };
        if adapter == Adapter::Gemini {
            assert_eq!(call_id, dispatch.tool_call_id(0));
        }
        let mut next = initial.request().clone();
        next.tools.push(acyclic_harness_core::tool::ToolDefinition {
            name: name.clone(),
            revision: "1".into(),
            description: "echo".into(),
            input_schema: json!({"type":"object"}),
            output_schema: json!({}),
            projection_schema: json!({}),
        });
        next.messages.extend(reasoning);
        next.messages.extend([
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall(
                    acyclic_harness_core::model::ToolCall::new(
                        call_id.clone(),
                        name.clone(),
                        arguments,
                    )
                    .with_metadata(metadata),
                )),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id,
                    name,
                    content: ToolResultContent::Json {
                        value: json!({"value":1}),
                    },
                }),
            },
        ]);
        let next = PreparedModelRequest::prepare(next, Limits::default(), None)?;
        let next = PreparedModelRequest::decode(next.bytes(), Limits::default(), None)?;
        let next_dispatch = ModelDispatch {
            step: 1,
            request_digest: next.request_digest(),
            ..dispatch
        };
        let events = provider
            .generate(next, next_dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            events[0],
            ModelEvent::Content {
                delta: "done".into()
            }
        );
        let sent = script.sent.lock().map_err(|_| invalid("fixture lock"))?;
        let body = json(&sent[1].body)?;
        if adapter == Adapter::Gemini {
            assert_eq!(
                body["contents"][1]["parts"][0]["thoughtSignature"],
                "signed"
            );
            assert_eq!(
                body["contents"][1]["parts"][0]["functionCall"]["id"],
                "harness_call_0"
            );
        } else if adapter == Adapter::Responses {
            for request in sent.iter() {
                assert_eq!(
                    json(&request.body)?["include"],
                    json!(["reasoning.encrypted_content"])
                );
            }
            assert_eq!(
                body["input"][2],
                json!({
                    "type":"reasoning", "id":"rs_1", "summary":[], "encrypted_content":"encrypted"
                })
            );
            assert_eq!(body["input"][3]["type"], "function_call");
            assert_eq!(body["input"][4]["type"], "function_call_output");
        } else {
            assert_eq!(
                body["messages"][1]["content"][0],
                json!({"type":"thinking","thinking":"plan","signature":"signed"})
            );
            assert_eq!(body["messages"][1]["content"][1]["type"], "tool_use");
            assert_eq!(body["messages"][2]["content"][0]["type"], "tool_result");
        }
    }
    Ok(())
}
