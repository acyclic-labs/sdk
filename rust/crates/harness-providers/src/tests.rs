use super::*;
use acyclic_harness::{
    context::ModelContextCapacity,
    conversation::Limits,
    model::{
        Model, ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelProvider, ModelRequest,
        ModelRole, PreparedModelRequest,
    },
};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
#[path = "counting/admission_tests.rs"]
mod admission_tests;
#[path = "alloy_tests.rs"]
mod alloy_tests;
#[cfg(not(target_arch = "wasm32"))]
#[path = "api_key_tests.rs"]
mod api_key_tests;
#[path = "projection/arguments_tests.rs"]
mod argument_projection_tests;
#[path = "call_id_tests.rs"]
mod call_id_tests;
#[cfg(not(target_arch = "wasm32"))]
#[path = "catalog_tests.rs"]
mod catalog_tests;
#[cfg(not(target_arch = "wasm32"))]
#[path = "chat_batch_tests.rs"]
mod chat_batch_tests;
#[path = "gemini_replay_tests.rs"]
mod gemini_replay_tests;
#[path = "gemini_tests.rs"]
mod gemini_tests;
#[cfg(not(target_arch = "wasm32"))]
#[path = "input/projection_tests.rs"]
mod input_projection_tests;
#[cfg(not(target_arch = "wasm32"))]
#[path = "native_tests.rs"]
pub(crate) mod native_tests;
#[path = "input/portable_projection_tests.rs"]
mod portable_projection_tests;
#[path = "tool_name_tests.rs"]
pub(crate) mod tool_name_tests;

#[path = "ordered_parts_tests.rs"]
mod ordered_parts_tests;

use crate::fixtures::{CHAT, fixtures};

fn chunks(
    input: impl AsRef<[u8]>,
    width: usize,
) -> acyclic_stream::BoxProviderStream<'static, Result<Bytes>> {
    let bytes = Bytes::copy_from_slice(input.as_ref());
    let chunks: Vec<_> = (0..bytes.len())
        .step_by(width)
        .map(|start| Ok(bytes.slice(start..(start + width).min(bytes.len()))))
        .collect();
    Box::pin(futures::stream::iter(chunks))
}

#[tokio::test]
async fn all_adapters_parse_fragmented_utf8_tools_reasoning_and_usage() -> Result<()> {
    for (adapter, fixture) in fixtures() {
        let dispatch = parser_dispatch();
        for width in [1, 7, 1024] {
            let events: Vec<_> = stream::events(adapter, chunks(fixture, width), 4096, dispatch)
                .try_collect()
                .await?;
            let visible: Vec<_> = events
                .iter()
                .filter(|event| !matches!(event, ModelEvent::ReasoningBlock { .. }))
                .collect();
            assert_eq!(visible.len(), 4, "{adapter:?}");
            assert_eq!(
                *visible[0],
                ModelEvent::Content {
                    delta: "hé".into()
                }
            );
            assert_eq!(
                *visible[1],
                ModelEvent::Reasoning {
                    delta: "think".into()
                }
            );
            assert_eq!(
                *visible[2],
                ModelEvent::ToolCall(
                    acyclic_harness::model::ToolCall::new(
                        dispatch.tool_call_id(0),
                        "echo".into(),
                        json!({"value":1})
                    )
                    .with_metadata(
                        json!({"provider_call":{"provider":stream::family(adapter),"id":"call"}})
                    )
                )
            );
            if matches!(
                adapter,
                Adapter::Anthropic | Adapter::Gemini | Adapter::Chat
            ) {
                let canonical = events
                    .iter()
                    .find_map(|event| match event {
                        ModelEvent::ReasoningBlock { block } if block.text.is_some() => Some(block),
                        _ => None,
                    })
                    .expect("readable reasoning is durably retained");
                assert_eq!(canonical.text.as_deref(), Some("think"));
                assert!(!canonical.continuity.is_empty());
            }
            let ModelEvent::Completed { metadata } = visible[3] else {
                return Err(invalid("missing completion"));
            };
            assert_eq!(metadata["id"], "run");
            assert!(metadata["usage"].is_object());
            if adapter == Adapter::Anthropic {
                assert_eq!(metadata["usage"]["input_tokens"], 5);
                assert_eq!(metadata["usage"]["output_tokens"], 3);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn errors_truncation_and_event_limits_fail_every_adapter() {
    for (adapter, _) in fixtures() {
        for input in [
            "data: {\"error\":{\"message\":\"secret detail\"}}\n\n",
            "data: {",
            "data: {}\n\n",
            "data: invalid\n\n",
        ] {
            let result: Result<Vec<_>> =
                stream::events(adapter, chunks(input, 1), 1024, parser_dispatch())
                    .try_collect()
                    .await;
            assert!(result.is_err(), "{adapter:?} {input}");
        }
        assert!(
            stream::events(
                adapter,
                chunks("data: {\"text\":\"oversized\"}\n\n", 1),
                8,
                parser_dispatch()
            )
            .try_collect::<Vec<_>>()
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn malformed_chat_batches_emit_no_tool_calls_or_completion() {
    for calls in [
        json!([{"index":-1}]),
        json!([{"index":0,"function":{"name":"echo","arguments":"{}"}}]),
        json!([{"index":0,"id":"call","function":{"name":"echo","arguments":"{"}}]),
        json!([{"index":0,"id":"call","function":{"name":"echo","arguments":"[]"}}]),
        json!([{"index":0,"id":"call","function":{"name":"echo","arguments":"{\"value\":9007199254740993}"}}]),
        // The recording rule: negative zero and private serde marker keys.
        json!([{"index":0,"id":"call","function":{"name":"echo","arguments":"{\"value\":-0.0}"}}]),
        json!([{"index":0,"id":"call","function":{"name":"echo","arguments":"{\"$serde_json::private::RawValue\":\"null\"}"}}]),
        json!([{"index":0,"id":"call","function":{"name":"echo","arguments":"{\"n\":[{\"$serde_json::private::Number\":\"1\"}]}"}}]),
        json!([{"index":0,"id":"same","function":{"name":"echo","arguments":"{}"}},
            {"index":1,"id":"same","function":{"name":"echo","arguments":"{}"}}]),
        json!([{"index":0,"id":"valid","function":{"name":"echo","arguments":"{}"}},
            {"index":1,"id":"invalid","function":{"name":"echo","arguments":"{"}}]),
    ] {
        let input = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":{"tool_calls":calls}}]})
        );
        let mut events = stream::events(Adapter::Chat, chunks(&input, 3), 4096, parser_dispatch());
        let mut failed = false;
        while let Some(event) = events.next().await {
            assert!(event.is_err());
            failed = true;
        }
        assert!(failed);
    }
}

#[tokio::test]
async fn portable_chat_arguments_are_emitted_unchanged() {
    // Healthy control for the rejected fixtures above: the boundary values of
    // the same rule, and marker-like keys that are not serde's, are admitted.
    let arguments = json!({"max":9007199254740991_u64, "min":-9007199254740991_i64, "half":-0.5, "zero":0.0, "$serde_json::private":1});
    let calls = json!([{"index":0,"id":"call","function":{"name":"echo","arguments":arguments.to_string()}}]);
    let input = format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":calls}}]})
    );
    let events: Vec<_> = stream::events(Adapter::Chat, chunks(&input, 3), 4096, parser_dispatch())
        .try_collect()
        .await
        .unwrap();
    let call = events
        .iter()
        .find_map(|event| match event {
            ModelEvent::ToolCall(call) => Some(call),
            _ => None,
        })
        .unwrap();
    assert_eq!(call.arguments, arguments);
}

struct Script {
    adapter: Adapter,
    status: u16,
    sent: Mutex<Vec<HttpRequest>>,
}

impl HttpTransport for Script {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            if let Some(response) = fixture_count(&request) {
                return Ok(response);
            }
            self.sent
                .lock()
                .map_err(|_| invalid("poisoned fixture"))?
                .push(request);
            let fixture = fixtures()
                .into_iter()
                .find(|(adapter, _)| *adapter == self.adapter)
                .ok_or_else(|| invalid("missing fixture"))?
                .1;
            Ok(HttpResponse {
                status: self.status,
                body: chunks(fixture, 1),
            })
        })
    }
}

fn fixture_count(request: &HttpRequest) -> Option<HttpResponse> {
    if !(request.url.ends_with("count_tokens")
        || request.url.ends_with("input_tokens")
        || request.url.ends_with(":countTokens"))
    {
        return None;
    }
    // The fixture tokenizer counts one token per encoded byte plus framing.
    let tokens = request.body.len() as u64 + 512;
    let body = serde_json::to_vec(
        &json!({"input_tokens": tokens, "totalTokens": tokens, "count": tokens}),
    )
    .unwrap();
    Some(HttpResponse {
        status: 200,
        body: Box::pin(futures::stream::iter([Ok(body.into())])),
    })
}

fn provider(adapter: Adapter, status: u16) -> (ResolvedModel, Arc<Script>) {
    let script = Arc::new(Script {
        adapter,
        status,
        sent: Mutex::new(Vec::new()),
    });
    let provider = ResolvedModel {
        files: crate::files::Files::default(),
        estimator: crate::accounting::TokenEstimator::new(adapter),
        model: Model {
            provider: "fixture".into(),
            name: "model".into(),
            revision: "1".into(),
            options: json!({}),
        },
        config: Arc::new(ProviderSpec {
            adapter,
            authentication: Authentication::ApiKey,
            base_url: "http://127.0.0.1:1234/v1".into(),
            headers: BTreeMap::new(),
            models: BTreeMap::new(),
            token_counter: Some(crate::TokenCounterSpec {
                path: "count_tokens".into(),
                adapter,
                response_field: crate::TokenCountField::InputTokens,
                framing_tokens: 0,
            }),
            max_event_bytes: 4096,
        }),
        capacity: ModelContextCapacity {
            context_tokens: 8192,
            output_tokens: 1024,
        },
        key: Some("scripted-key".into()),
        transport: script.clone(),
        #[cfg(not(target_arch = "wasm32"))]
        codex_count: crate::codex_count::Counter::default(),
        #[cfg(not(target_arch = "wasm32"))]
        codex_login: None,
    };
    (provider, script)
}

pub(super) fn request(provider: &ResolvedModel) -> Result<(PreparedModelRequest, ModelDispatch)> {
    let prepared = PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.model.clone(),
            messages: vec![
                ModelMessage {
                    role: ModelRole::System,
                    content: ModelContent::Text("system".into()),
                },
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("user".into()),
                },
            ],
            tools: Vec::new(),
            max_output_tokens: Some(64),
        },
        Limits::default(),
        None,
    )?;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness::OperationId::new(),
        step: 0,
        request_digest: prepared.request_digest(),
    };
    Ok((prepared, dispatch))
}

#[tokio::test]
async fn model_provider_dispatch_projects_protocol_and_binds_accounting() -> Result<()> {
    for (adapter, _) in fixtures() {
        let (provider, script) = provider(adapter, 200);
        let (request, dispatch) = request(&provider)?;
        let count = provider.count_tokens(&request).await?;
        assert_eq!(count.request_digest, request.request_digest());
        assert_eq!(count.message_tokens.len(), 2);
        assert!(count.fixed_tokens > 0 && count.message_tokens.iter().all(|count| *count > 0));
        assert_eq!(
            provider.context_capacity(&provider.model)?.context_tokens,
            8192
        );
        let events = provider
            .generate(request, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            events
                .iter()
                .filter(|event| !matches!(event, ModelEvent::ReasoningBlock { .. }))
                .count(),
            4
        );
        if adapter != Adapter::Responses {
            let blocks: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    ModelEvent::ReasoningBlock { block } => Some(block),
                    _ => None,
                })
                .collect();
            let thinking = if adapter == Adapter::Gemini {
                assert_eq!(blocks.len(), 2);
                assert_eq!(blocks[0].text, None);
                assert_eq!(
                    blocks[0].kind,
                    acyclic_harness::model::ReasoningKind::Opaque
                );
                assert_eq!(
                    blocks[0].outcome,
                    acyclic_harness::model::ReasoningOutcome::SuppressedDisplay
                );
                assert_eq!(json(&blocks[0].continuity)?, json!({"text":"hé"}));
                blocks[1]
            } else {
                assert_eq!(blocks.len(), 1);
                blocks[0]
            };
            assert_eq!(thinking.text.as_deref(), Some("think"));
            for block in blocks {
                assert_eq!(
                    block.origin,
                    provider.reasoning_origin(provider.key.as_deref(), None)
                );
            }
        }
        let sent = script
            .sent
            .lock()
            .map_err(|_| invalid("poisoned fixture"))?;
        assert_eq!(sent.len(), 1);
        let body = json(&sent[0].body)?;
        match adapter {
            Adapter::Anthropic => {
                assert!(sent[0].url.ends_with("/messages"));
                assert_eq!(body["system"], "system");
                assert_eq!(body["max_tokens"], 64);
                assert_eq!(sent[0].headers["x-api-key"], "scripted-key");
                assert!(!sent[0].headers.contains_key("authorization"));
            }
            Adapter::Responses => {
                assert!(sent[0].url.ends_with("/responses"));
                assert_eq!(body["input"][0]["role"], "system");
                assert_eq!(body["store"], false);
            }
            Adapter::Chat => {
                assert!(sent[0].url.ends_with("/chat/completions"));
                assert_eq!(body["stream_options"]["include_usage"], true);
            }
            Adapter::Gemini => {
                assert!(
                    sent[0]
                        .url
                        .ends_with("/models/model:streamGenerateContent?alt=sse")
                );
                assert_eq!(body["contents"][0]["role"], "user");
                assert_eq!(body["generationConfig"]["maxOutputTokens"], 64);
                assert_eq!(sent[0].headers["x-goog-api-key"], "scripted-key");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn http_errors_and_changed_dispatches_never_complete() -> Result<()> {
    for (adapter, _) in fixtures() {
        let (provider, _) = provider(adapter, 401);
        let (request, dispatch) = request(&provider)?;
        assert!(
            provider
                .generate(request, dispatch)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        let (request, mut dispatch) = self::request(&provider)?;
        dispatch.request_digest[0] ^= 1;
        assert!(
            provider
                .generate(request, dispatch)
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn registry_rejects_unsafe_urls_and_preserves_model_slashes() -> Result<()> {
    let (_, script) = provider(Adapter::Chat, 200);
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), script);
    let mut config = ProviderSpec {
        adapter: Adapter::Chat,
        authentication: Authentication::Optional,
        base_url: "https://example.test/v1".into(),
        headers: BTreeMap::new(),
        models: BTreeMap::from([(
            "org/model".into(),
            ModelSpec {
                capacity: ModelContextCapacity {
                    context_tokens: 8192,
                    output_tokens: 1024,
                },
                features: ModelFeatures::default(),
            },
        )]),
        token_counter: Some(crate::TokenCounterSpec {
            path: "count_tokens".into(),
            adapter: Adapter::Chat,
            response_field: crate::TokenCountField::InputTokens,
            framing_tokens: 0,
        }),
        max_event_bytes: 4096,
    };
    for base in [
        "http://example.test",
        "https://u:p@example.test",
        "https://example.test?x",
        "https://example.test#x",
        "file:///tmp",
    ] {
        config.base_url = base.into();
        assert!(registry.register("ollama".into(), config.clone()).is_err());
    }
    config.base_url = "http://127.0.0.1:1234/v1".into();
    registry.register("ollama".into(), config)?;
    assert_eq!(
        registry.resolve("ollama/org/model")?.model.name,
        "org/model"
    );
    assert!(registry.resolve("ollama/unknown").is_err());
    assert!(registry.resolve("unknown/model").is_err());
    Ok(())
}

#[tokio::test]
async fn tool_history_round_trips_call_identity_and_arguments_in_every_protocol() -> Result<()> {
    use acyclic_harness::model::{ModelContentPart, ToolResultContent};
    for (adapter, _) in fixtures() {
        let (provider, script) = provider(adapter, 200);
        let tool = acyclic_harness::tool::ToolDefinition {
            name: "echo".into(),
            revision: "1".into(),
            description: "Echo text".into(),
            input_schema: json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}),
            output_schema: json!({"type":"object"}),
            projection_schema: json!({"type":"object"}),
        };
        let request = ModelRequest {
            model: provider.model.clone(),
            max_output_tokens: Some(64),
            tools: vec![tool],
            messages: vec![
                ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("run echo".into()),
                },
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(
                        acyclic_harness::model::ToolCall::new(
                            "call".into(),
                            "echo".into(),
                            json!({"value":1}),
                        ),
                    )),
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "echo".into(),
                        content: ToolResultContent::Json {
                            value: json!({"value":1}),
                        },
                    }),
                },
            ],
        };
        let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
        let count = provider.count_tokens(&prepared).await?;
        let dispatch = ModelDispatch {
            operation_id: acyclic_harness::OperationId::new(),
            step: 1,
            request_digest: prepared.request_digest(),
        };
        provider
            .generate(prepared, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        let sent = script
            .sent
            .lock()
            .map_err(|_| invalid("poisoned fixture"))?;
        let body = json(&sent[0].body)?;
        assert!(
            (u64::from(count.fixed_tokens)
                + count
                    .message_tokens
                    .iter()
                    .map(|count| u64::from(*count))
                    .sum::<u64>())
                * 3
                >= sent[0].body.len() as u64
        );
        match adapter {
            Adapter::Chat => {
                assert_eq!(body["messages"][1]["tool_calls"][0]["id"], "call");
                assert_eq!(
                    body["messages"][1]["tool_calls"][0]["function"]["arguments"],
                    "{\"value\":1}"
                );
                assert_eq!(body["messages"][2]["tool_call_id"], "call");
                assert_eq!(body["tools"][0]["function"]["parameters"]["type"], "object");
            }
            Adapter::Anthropic => {
                assert_eq!(body["messages"][1]["content"][0]["id"], "call");
                assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "call");
                assert_eq!(body["messages"][2]["role"], "user");
                assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
            }
            Adapter::Responses => {
                assert_eq!(body["input"][1]["call_id"], "call");
                assert_eq!(body["input"][2]["type"], "function_call_output");
                assert_eq!(body["input"][2]["call_id"], "call");
                assert_eq!(body["tools"][0]["parameters"]["type"], "object");
            }
            Adapter::Gemini => {
                assert_eq!(
                    body["contents"][1]["parts"][0]["functionCall"]["id"],
                    "call"
                );
                assert_eq!(
                    body["contents"][2]["parts"][0]["functionResponse"]["id"],
                    "call"
                );
                assert_eq!(
                    body["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"]["type"],
                    "object"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn model_content_is_available_before_the_network_stream_completes() -> Result<()> {
    for (adapter, _) in fixtures() {
        let value = match adapter {
            Adapter::Chat => json!({"choices":[{"delta":{"content":"first"}}]}),
            Adapter::Anthropic => {
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"first"}})
            }
            Adapter::Responses => json!({"type":"response.output_text.delta","delta":"first"}),
            Adapter::Gemini => json!({"candidates":[{"content":{"parts":[{"text":"first"}]}}]}),
        };
        let chunk = Bytes::from(format!("data: {value}\n\n"));
        let body = futures::stream::once(async { Ok(chunk) }).chain(futures::stream::pending());
        let mut events = stream::events(adapter, Box::pin(body), 4096, parser_dispatch());
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.next())
            .await
            .map_err(|_| invalid("provider buffered content until completion"))?;
        assert_eq!(
            event.transpose()?,
            Some(ModelEvent::Content {
                delta: "first".into()
            })
        );
    }
    Ok(())
}

#[tokio::test]
async fn dropping_model_stream_cancels_the_owned_http_body() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct BodyGuard(Arc<AtomicBool>);
    impl Drop for BodyGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    for (adapter, fixture) in fixtures() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = BodyGuard(cancelled.clone());
        let body = async_stream::try_stream! {
            let _guard = guard;
            yield Bytes::copy_from_slice(fixture.as_bytes());
            futures::future::pending::<()>().await;
        };
        let mut stream = stream::events(adapter, Box::pin(body), 4096, parser_dispatch());
        assert!(stream.next().await.transpose()?.is_some());
        assert!(!cancelled.load(Ordering::SeqCst));
        drop(stream);
        assert!(cancelled.load(Ordering::SeqCst));
    }
    Ok(())
}

fn parser_dispatch() -> ModelDispatch {
    ModelDispatch {
        operation_id: acyclic_harness::OperationId::new(),
        step: 0,
        request_digest: [0; 32],
    }
}

#[path = "protocol_tests.rs"]
mod protocol;

#[path = "stream/reasoning_tests.rs"]
mod reasoning;

#[path = "files_tests.rs"]
mod files;
