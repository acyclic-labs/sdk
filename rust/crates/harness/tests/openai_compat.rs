//! OpenAI-compatible and `OpenRouter` provider behaviour against recorded SSE
//! transcripts and a local fixture HTTP server. No test reaches the network.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    missing_docs
)]

use acyclic_harness::models::{
    BudgetReason, CompletionMetadata, OpenAiCompatibleProvider, ProviderConfig, ProviderError,
    ProviderErrorCode, RetryPolicy, Usage,
    openai_compat::{MappedRequest, StreamDecoder, decode_sse, map_request, wire_tool_name},
};
use acyclic_harness::{
    Error,
    model::{Model, ModelEvent, ModelMessage, ModelProvider as _, ModelRequest},
    tool::ToolDefinition,
};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _, channel::mpsc, stream};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

const TOOL_CALLS: &str = include_str!("fixtures/openai_tool_calls.sse");
const TEXT: &str = include_str!("fixtures/openai_text.sse");
const TRUNCATED: &str = include_str!("fixtures/openai_truncated.sse");
const MISSING_ID: &str = include_str!("fixtures/openai_tool_call_missing_id.sse");
const OPENROUTER_COST: &str = include_str!("fixtures/openrouter_tool_call_with_cost.sse");
const OPENROUTER_MIDSTREAM_402: &str = include_str!("fixtures/openrouter_midstream_402.sse");

type Decoded = Vec<Result<ModelEvent, ProviderError>>;

fn tool_names() -> BTreeMap<String, String> {
    ["acyclic.filesystem", "acyclic.shell"]
        .into_iter()
        .map(|name| (wire_tool_name(name), name.to_owned()))
        .collect()
}

/// Feeds a transcript through the decoder in fixed-size byte chunks so event
/// boundaries never line up with network chunk boundaries.
async fn decode_with(transcript: &str, chunk: usize, decoder: StreamDecoder) -> Decoded {
    let chunks = transcript
        .as_bytes()
        .chunks(chunk)
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect::<Vec<_>>();
    decode_sse(stream::iter(chunks), decoder).collect().await
}

async fn decode(transcript: &str, chunk: usize) -> Decoded {
    decode_with(transcript, chunk, StreamDecoder::new(tool_names())).await
}

fn tool(name: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        revision: "1".into(),
        description: format!("{name} description"),
        input_schema: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
        output_schema: json!({"type": "object"}),
    }
}

fn request(options: Value) -> ModelRequest {
    ModelRequest {
        model: Model::new(
            "openrouter",
            "anthropic/claude-sonnet-4.5",
            "unpinned",
            options,
        )
        .unwrap(),
        messages: vec![
            ModelMessage {
                role: "system".into(),
                content: json!("be brief"),
            },
            ModelMessage {
                role: "user".into(),
                content: json!({"text": "list files"}),
            },
        ],
        tools: vec![tool("acyclic.shell")],
        max_output_tokens: Some(64),
    }
}

fn metadata(events: &[ModelEvent]) -> CompletionMetadata {
    CompletionMetadata::from_event(events.last().unwrap())
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn tool_call_transcript_decodes_to_content_then_complete_tool_calls() {
    for chunk in [1, 7, 64, 4096] {
        let events = decode(TOOL_CALLS, chunk)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>();
        let events = events.expect("decodes");
        assert_eq!(
            events,
            vec![
                ModelEvent::Content {
                    delta: "Let me ".into()
                },
                ModelEvent::Content {
                    delta: "look.".into()
                },
                ModelEvent::ToolCall {
                    call_id: "call_read".into(),
                    name: "acyclic.filesystem".into(),
                    arguments: json!({"operation": "read", "path": "README.md"}),
                },
                ModelEvent::ToolCall {
                    call_id: "call_shell".into(),
                    name: "acyclic.shell".into(),
                    arguments: json!({"command": "ls"}),
                },
                ModelEvent::Completed {
                    metadata: json!({
                        "finish_reason": "tool_calls",
                        "usage": {"prompt_tokens": 40, "completion_tokens": 12, "total_tokens": 52},
                        "model": "gpt-test",
                        "id": "chatcmpl-1",
                    }),
                },
            ],
            "chunk size {chunk}"
        );
    }
}

#[tokio::test]
async fn text_transcript_with_crlf_and_comments_decodes_reasoning_and_content() {
    let events = decode(TEXT, 5)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        events,
        vec![
            ModelEvent::Reasoning {
                delta: "thinking".into()
            },
            ModelEvent::Content {
                delta: "Done.".into()
            },
            ModelEvent::Completed {
                metadata: json!({
                    "finish_reason": "stop",
                    "usage": null,
                    "model": "gpt-test",
                    "id": "chatcmpl-2",
                }),
            },
        ]
    );
}

#[tokio::test]
async fn openrouter_final_chunk_usage_and_cost_land_in_completion_metadata() {
    for chunk in [3, 64, 4096] {
        let decoder = StreamDecoder::new(tool_names()).with_user(Some("agent-7".into()));
        let events = decode_with(OPENROUTER_COST, chunk, decoder).await;
        let events = events.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            events.first(),
            Some(&ModelEvent::Content {
                delta: "Checking.".into()
            })
        );
        assert_eq!(
            events.get(1),
            Some(&ModelEvent::ToolCall {
                call_id: "toolu_1".into(),
                name: "acyclic.shell".into(),
                arguments: json!({"command": "ls"}),
            })
        );
        assert_eq!(
            metadata(&events),
            CompletionMetadata {
                id: Some("gen-1".into()),
                model: Some("anthropic/claude-sonnet-4.5".into()),
                provider: Some("Anthropic".into()),
                finish_reason: Some("tool_calls".into()),
                user: Some("agent-7".into()),
                usage: Some(Usage {
                    prompt_tokens: 1200,
                    completion_tokens: 34,
                    total_tokens: 1234,
                    cached_tokens: Some(1024),
                    cache_write_tokens: Some(0),
                    reasoning_tokens: Some(0),
                    cost: Some(0.004_113),
                    upstream_inference_cost: None,
                    is_byok: Some(false),
                }),
            },
            "chunk size {chunk}"
        );
    }
}

#[tokio::test]
async fn openrouter_midstream_402_is_a_typed_budget_error_after_the_prefix() {
    let events = decode(OPENROUTER_MIDSTREAM_402, 11).await;
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(
        events[0].as_ref().ok(),
        Some(&ModelEvent::Content {
            delta: "Par".into()
        })
    );
    match &events[1] {
        Err(ProviderError::BudgetExhausted { reason, message }) => {
            assert_eq!(*reason, BudgetReason::InsufficientCredits);
            assert!(message.contains("Insufficient credits"), "{message}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn truncated_transcript_yields_content_then_protocol_error_without_completion() {
    let events = decode(TRUNCATED, 16).await;
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].as_ref().ok(),
        Some(&ModelEvent::Content {
            delta: "partial".into()
        })
    );
    assert!(
        matches!(events[1], Err(ProviderError::Protocol { .. })),
        "{:?}",
        events[1]
    );
}

#[tokio::test]
async fn tool_call_without_an_id_is_rejected_not_fabricated() {
    let events = decode(MISSING_ID, 32).await;
    assert_eq!(events.len(), 1, "{events:?}");
    match &events[0] {
        Err(ProviderError::Invalid { message }) => {
            assert!(
                message.contains("acyclic.shell") && message.contains("no id"),
                "{message}"
            );
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn done_sentinel_completes_without_waiting_for_the_body_to_close() {
    let (mut sender, receiver) = mpsc::channel::<Result<Bytes, ProviderError>>(8);
    sender
        .send(Ok(Bytes::from_static(TEXT.as_bytes())))
        .await
        .unwrap();
    let decoded = tokio::time::timeout(
        Duration::from_secs(5),
        decode_sse(receiver, StreamDecoder::new(BTreeMap::new())).collect::<Vec<_>>(),
    )
    .await
    .expect("stream completes after [DONE] while the body stays open");
    let events = decoded.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(
        matches!(events.last(), Some(ModelEvent::Completed { .. })),
        "{events:?}"
    );
    drop(sender);
}

#[tokio::test]
async fn error_chunk_without_a_status_is_unavailable() {
    let transcript =
        "data: {\"error\": {\"message\": \"upstream reset\", \"type\": \"server\"}}\n\n";
    let events = decode(transcript, 1024).await;
    match events.as_slice() {
        [Err(error @ ProviderError::Unavailable { .. })] => assert!(error.is_retryable()),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn openrouter_request_carries_user_usage_accounting_and_merged_options() {
    let config = ProviderConfig::openrouter("sk-or-secret").with_user("agent-7");
    let options = json!({"provider": {"order": ["anthropic"]}, "temperature": 0.2});
    let MappedRequest { body, tool_names } = map_request(&config, &request(options)).unwrap();
    assert_eq!(
        body,
        json!({
            "model": "anthropic/claude-sonnet-4.5",
            "provider": {"order": ["anthropic"]},
            "temperature": 0.2,
            "stream": true,
            "stream_options": {"include_usage": true},
            "usage": {"include": true},
            "user": "agent-7",
            "max_tokens": 64,
            "messages": [
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": "list files"},
            ],
            "tools": [{"type": "function", "function": {
                "name": "acyclic_shell",
                "description": "acyclic.shell description",
                "parameters": {"type": "object", "properties": {"path": {"type": "string"}}},
            }}],
        })
    );
    assert_eq!(
        tool_names.get("acyclic_shell").map(String::as_str),
        Some("acyclic.shell")
    );
    assert!(
        !body.to_string().contains("sk-or-secret"),
        "key must never enter the body"
    );
}

#[test]
fn plain_dialect_omits_usage_accounting_and_rejects_reserved_options() {
    let config = ProviderConfig::new("sk", "https://gw.test/v1");
    let body = map_request(&config, &request(Value::Null)).unwrap().body;
    assert!(
        body.get("usage").is_none() && body.get("user").is_none(),
        "{body}"
    );
    for options in [
        json!({"user": "spoofed"}),
        json!({"model": "other"}),
        json!("text"),
    ] {
        assert!(
            matches!(
                map_request(&config, &request(options.clone())),
                Err(ProviderError::Invalid { .. })
            ),
            "{options} must be rejected"
        );
    }
}

#[test]
fn request_maps_tool_turns_and_rejects_bad_messages() {
    let config = ProviderConfig::new("sk", "https://gw.test/v1");
    let mut turn = request(Value::Null);
    turn.messages.extend([
        ModelMessage {
            role: "assistant".into(),
            content: json!({"tool_calls": [
                {"call_id": "call_1", "name": "acyclic.shell", "arguments": {"command": "ls"}}
            ]}),
        },
        ModelMessage {
            role: "tool".into(),
            content: json!({"call_id": "call_1", "name": "acyclic.shell", "result": {"out": "a"}}),
        },
    ]);
    let body = map_request(&config, &turn).unwrap().body;
    assert_eq!(
        body["messages"][2],
        json!({"role": "assistant", "content": null, "tool_calls": [
            {"id": "call_1", "type": "function",
             "function": {"name": "acyclic_shell", "arguments": "{\"command\":\"ls\"}"}}
        ]})
    );
    assert_eq!(
        body["messages"][3],
        json!({"role": "tool", "tool_call_id": "call_1", "content": "{\"out\":\"a\"}"})
    );
    for message in [
        ModelMessage {
            role: "developer".into(),
            content: json!("x"),
        },
        ModelMessage {
            role: "tool".into(),
            content: json!({"result": 1}),
        },
    ] {
        let mut bad = request(Value::Null);
        bad.messages = vec![message];
        assert!(matches!(
            map_request(&config, &bad),
            Err(ProviderError::Invalid { .. })
        ));
    }
}

#[test]
fn status_classification_separates_budget_rate_limit_and_auth() {
    use acyclic_harness::models::Dialect::{OpenAi, OpenRouter};
    let classify = |dialect, status, body| ProviderError::from_status(dialect, status, None, body);
    assert!(matches!(
        classify(OpenRouter, 402, "Insufficient credits"),
        ProviderError::BudgetExhausted {
            reason: BudgetReason::InsufficientCredits,
            ..
        }
    ));
    assert!(matches!(
        classify(
            OpenRouter,
            403,
            r#"{"error":{"message":"Key limit exceeded (total limit)"}}"#
        ),
        ProviderError::BudgetExhausted {
            reason: BudgetReason::KeyLimitReached,
            ..
        }
    ));
    assert!(matches!(
        classify(OpenAi, 403, "Key limit exceeded"),
        ProviderError::Unauthorized { .. }
    ));
    assert!(matches!(
        classify(OpenRouter, 403, "flagged"),
        ProviderError::Unauthorized { .. }
    ));
    assert!(matches!(
        classify(OpenRouter, 401, "expired"),
        ProviderError::Unauthorized { .. }
    ));
    assert!(classify(OpenRouter, 429, "slow down").is_retryable());
    assert!(classify(OpenRouter, 503, "no provider").is_retryable());
    assert!(!classify(OpenRouter, 402, "credits").is_retryable());
    assert!(!classify(OpenRouter, 400, "bad").is_retryable());
}

#[test]
fn harness_errors_keep_a_parseable_typed_code() {
    let budget = ProviderError::from_status(Default::default(), 402, None, "credits");
    let budget = Error::from(budget);
    assert!(matches!(budget, Error::Unauthorized(_)), "{budget:?}");
    assert_eq!(
        ProviderErrorCode::from_harness(&budget),
        Some(ProviderErrorCode::BudgetExhausted)
    );
    let limited = Error::from(ProviderError::from_status(
        Default::default(),
        429,
        None,
        "x",
    ));
    assert!(matches!(limited, Error::Storage(_)), "{limited:?}");
    let code = ProviderErrorCode::from_harness(&limited).unwrap();
    assert_eq!(code, ProviderErrorCode::RateLimited);
    assert!(code.is_retryable());
    assert_eq!(
        ProviderErrorCode::from_harness(&Error::Storage("disk full".into())),
        None
    );
    assert_eq!(
        ProviderErrorCode::from_harness(&Error::NotFound("x".into())),
        None
    );
}

#[test]
fn usage_totals_sum_completed_events_only() {
    let completed = |cost: f64, tokens: u64| ModelEvent::Completed {
        metadata: CompletionMetadata {
            usage: Some(Usage {
                prompt_tokens: tokens,
                completion_tokens: 1,
                total_tokens: tokens + 1,
                cost: Some(cost),
                ..Usage::default()
            }),
            ..CompletionMetadata::default()
        }
        .to_value(),
    };
    let events = [
        completed(0.25, 10),
        ModelEvent::Content { delta: "x".into() },
        completed(0.5, 20),
        ModelEvent::Completed {
            metadata: Value::Null,
        },
    ];
    let total = Usage::total(&events);
    assert_eq!((total.prompt_tokens, total.total_tokens), (30, 32));
    assert_eq!(total.cost, Some(0.75));
}

#[test]
fn debug_output_keeps_the_key_and_url_secrets_out() {
    let config = ProviderConfig::openrouter("sk-or-secret")
        .with_base_url("https://user:hunter2@gw.test:8443/v1/private?token=xyz");
    let provider = OpenAiCompatibleProvider::new(config.clone()).unwrap();
    for rendered in [format!("{config:?}"), format!("{provider:?}")] {
        assert!(rendered.contains("https://gw.test:8443") && rendered.contains("<redacted>"));
        for leaked in ["sk-or-secret", "hunter2", "private", "token=xyz"] {
            assert!(
                !rendered.contains(leaked),
                "{leaked} leaked into {rendered}"
            );
        }
    }
    assert_eq!(
        ProviderConfig::openrouter("k").completions_url(),
        "https://openrouter.ai/api/v1/chat/completions"
    );
}

/// One scripted fixture response.
struct Scripted {
    status: u16,
    headers: Vec<(&'static str, &'static str)>,
    body: &'static str,
}

/// One request the fixture server observed.
struct Recorded {
    path: String,
    headers: HeaderMap,
    body: Value,
}

#[derive(Clone, Default)]
struct Fixture {
    script: Arc<Mutex<VecDeque<Scripted>>>,
    seen: Arc<Mutex<Vec<Recorded>>>,
}

async fn replay(State(fixture): State<Fixture>, request: axum::extract::Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    fixture.seen.lock().unwrap().push(Recorded {
        path: parts.uri.path().to_owned(),
        headers: parts.headers,
        body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    });
    let scripted = fixture
        .script
        .lock()
        .unwrap()
        .pop_front()
        .expect("unscripted request");
    let mut response = Response::builder().status(StatusCode::from_u16(scripted.status).unwrap());
    for (name, value) in scripted.headers {
        response = response.header(name, value);
    }
    response.body(Body::from(scripted.body)).unwrap()
}

/// Serves `script` in order on an ephemeral port and returns the base URL.
async fn serve(script: Vec<Scripted>) -> (String, Fixture) {
    let fixture = Fixture {
        script: Arc::new(Mutex::new(script.into())),
        ..Fixture::default()
    };
    let app = Router::new().fallback(replay).with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/api/v1"), fixture)
}

fn sse(body: &'static str) -> Scripted {
    Scripted {
        status: 200,
        headers: vec![("content-type", "text/event-stream")],
        body,
    }
}

fn openrouter(base_url: String) -> OpenAiCompatibleProvider {
    let config = ProviderConfig::openrouter("sk-or-swarm")
        .with_base_url(base_url)
        .with_user("agent-7")
        .with_app("https://acyclic.dev", "Acyclic")
        .with_retry(RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        });
    OpenAiCompatibleProvider::new(config).unwrap()
}

#[tokio::test]
async fn openrouter_round_trip_sends_attribution_and_reports_cost() {
    let (base_url, fixture) = serve(vec![sse(OPENROUTER_COST)]).await;
    let provider = openrouter(base_url);
    let events = provider
        .generate(request(Value::Null))
        .collect::<Vec<_>>()
        .await;
    let events = events.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    let completion = metadata(&events);
    assert_eq!(completion.user.as_deref(), Some("agent-7"));
    assert_eq!(
        completion.usage.as_ref().and_then(|usage| usage.cost),
        Some(0.004_113)
    );
    assert_eq!(Usage::total(&events).total_tokens, 1234);

    let seen = fixture.seen.lock().unwrap();
    let [only] = seen.as_slice() else {
        panic!("expected one request")
    };
    assert_eq!(only.path, "/api/v1/chat/completions");
    let header = |name: &str| only.headers.get(name).and_then(|v| v.to_str().ok());
    assert_eq!(header("authorization"), Some("Bearer sk-or-swarm"));
    assert_eq!(header("http-referer"), Some("https://acyclic.dev"));
    assert_eq!(header("x-title"), Some("Acyclic"));
    assert_eq!(header("accept"), Some("text/event-stream"));
    assert_eq!(only.body["user"], "agent-7");
    assert_eq!(only.body["usage"], json!({"include": true}));
    assert_eq!(only.body["model"], "anthropic/claude-sonnet-4.5");
}

#[tokio::test]
async fn http_402_stops_without_retry_and_maps_to_a_budget_error() {
    let body = r#"{"error":{"code":402,"message":"Insufficient credits"}}"#;
    let (base_url, fixture) = serve(vec![Scripted {
        status: 402,
        headers: vec![],
        body,
    }])
    .await;
    let provider = openrouter(base_url);
    let events = provider
        .stream(&request(Value::Null))
        .collect::<Vec<_>>()
        .await;
    match events.as_slice() {
        [
            Err(ProviderError::BudgetExhausted {
                reason: BudgetReason::InsufficientCredits,
                ..
            }),
        ] => {}
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        fixture.seen.lock().unwrap().len(),
        1,
        "budget errors are never retried"
    );
}

#[tokio::test]
async fn http_403_key_limit_surfaces_through_the_harness_as_budget_exhausted() {
    let body = r#"{"error":{"code":403,"message":"Key limit exceeded (total limit)"}}"#;
    let (base_url, _fixture) = serve(vec![Scripted {
        status: 403,
        headers: vec![],
        body,
    }])
    .await;
    let provider = openrouter(base_url);
    let events = provider
        .generate(request(Value::Null))
        .collect::<Vec<_>>()
        .await;
    let [Err(error)] = events.as_slice() else {
        panic!("unexpected {events:?}")
    };
    assert!(matches!(error, Error::Unauthorized(_)), "{error:?}");
    assert_eq!(
        ProviderErrorCode::from_harness(error),
        Some(ProviderErrorCode::BudgetExhausted)
    );
}

#[tokio::test]
async fn http_429_is_retried_before_streaming_then_succeeds() {
    let limited = Scripted {
        status: 429,
        headers: vec![("retry-after", "0")],
        body: "slow down",
    };
    let (base_url, fixture) = serve(vec![limited, sse(TEXT)]).await;
    let provider = openrouter(base_url);
    let events = provider
        .stream(&request(Value::Null))
        .collect::<Vec<_>>()
        .await;
    let events = events.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(matches!(events.last(), Some(ModelEvent::Completed { .. })));
    assert_eq!(fixture.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn persistent_429_surfaces_a_retryable_rate_limit_with_retry_after() {
    let limited = || Scripted {
        status: 429,
        headers: vec![("retry-after", "0")],
        body: "busy",
    };
    let (base_url, fixture) = serve(vec![limited(), limited(), limited()]).await;
    let provider = openrouter(base_url);
    let events = provider
        .stream(&request(Value::Null))
        .collect::<Vec<_>>()
        .await;
    match events.as_slice() {
        [Err(error @ ProviderError::RateLimited { retry_after, .. })] => {
            assert_eq!(*retry_after, Some(Duration::ZERO));
            assert!(error.is_retryable());
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        fixture.seen.lock().unwrap().len(),
        3,
        "bounded by max_attempts"
    );
}

#[tokio::test]
async fn mixed_line_endings_split_events_at_every_chunk_size() {
    // `\n\r\n`, `\r\r`, and `\r\n\n` are all valid blank-line separators.
    let transcript = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\r\n",
        ": keep-alive\r\r",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"b\"},\"finish_reason\":\"stop\"}]}\r\n\n",
        "data: [DONE]\r\r",
    );
    for chunk in [1, 2, 3, 7, 1024] {
        let events = decode(transcript, chunk).await;
        let events = events
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|error| panic!("chunk {chunk}: {error:?}"));
        assert_eq!(events.len(), 3, "chunk {chunk}: {events:?}");
        assert_eq!(events[0], ModelEvent::Content { delta: "a".into() });
        assert_eq!(events[1], ModelEvent::Content { delta: "b".into() });
        assert!(matches!(events[2], ModelEvent::Completed { .. }));
    }
}

#[tokio::test]
async fn mixed_separator_yields_the_event_while_the_body_stays_open() {
    let (mut sender, receiver) = mpsc::channel::<Result<Bytes, ProviderError>>(8);
    sender
        .send(Ok(Bytes::from_static(
            b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\r\n",
        )))
        .await
        .unwrap();
    let mut events = decode_sse(receiver, StreamDecoder::new(BTreeMap::new()));
    let first = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("event separated by \\n\\r\\n is yielded without more bytes");
    assert_eq!(
        first.map(Result::ok),
        Some(Some(ModelEvent::Content { delta: "hi".into() }))
    );
    drop(sender);
}

#[tokio::test]
async fn finish_reason_without_done_is_a_protocol_error_not_a_completion() {
    // The connection closed after the finish chunk but before the trailing
    // usage chunk and `[DONE]`.
    let transcript = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\n";
    let events = decode(transcript, 8).await;
    match events.as_slice() {
        [
            Ok(ModelEvent::Content { .. }),
            Err(ProviderError::Protocol { message }),
        ] => {
            assert!(message.contains("[DONE]"), "{message}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn done_without_a_finish_reason_is_a_protocol_error_not_a_completion() {
    let transcript =
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n";
    let events = decode(transcript, 8).await;
    match events.as_slice() {
        [
            Ok(ModelEvent::Content { .. }),
            Err(ProviderError::Protocol { message }),
        ] => {
            assert!(message.contains("finish reason"), "{message}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn completions_url_keeps_the_suffix_in_the_path_before_any_query() {
    let url = |base: &str| ProviderConfig::new("k", base).completions_url();
    assert_eq!(
        url("https://host/v1?token=x"),
        "https://host/v1/chat/completions?token=x"
    );
    assert_eq!(
        url("https://host/v1/?token=x"),
        "https://host/v1/chat/completions?token=x"
    );
    assert_eq!(url("https://host/v1/"), "https://host/v1/chat/completions");
    assert_eq!(url("https://host"), "https://host/chat/completions");
}

fn plain(base_url: String, idle: Duration) -> OpenAiCompatibleProvider {
    let config = ProviderConfig::new("k", base_url)
        .with_idle_timeout(idle)
        .with_retry(RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        });
    OpenAiCompatibleProvider::new(config).unwrap()
}

/// Serves every request with `respond` on an ephemeral port and counts requests.
async fn serve_with<F, Fut>(respond: F) -> (String, Arc<Mutex<usize>>)
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Response> + Send + 'static,
{
    let count = Arc::new(Mutex::new(0_usize));
    let seen = count.clone();
    let app = Router::new().fallback(move || {
        *seen.lock().unwrap() += 1;
        respond()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/v1"), count)
}

#[tokio::test]
async fn stalled_body_fails_after_the_idle_timeout_instead_of_hanging() {
    let (base_url, _count) = serve_with(|| async {
        let first = stream::once(async {
            Ok::<_, std::io::Error>(Bytes::from_static(
                b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n",
            ))
        });
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(first.chain(stream::pending())))
            .unwrap()
    })
    .await;
    let provider = plain(base_url, Duration::from_millis(200));
    let events = tokio::time::timeout(
        Duration::from_secs(10),
        provider.stream(&request(Value::Null)).collect::<Vec<_>>(),
    )
    .await
    .expect("idle timeout ends the stream");
    match events.as_slice() {
        [
            Ok(ModelEvent::Content { .. }),
            Err(ProviderError::Unavailable { message, .. }),
        ] => {
            assert!(message.contains("idle"), "{message}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn missing_response_headers_time_out_and_are_not_resent() {
    let (base_url, count) = serve_with(|| async {
        futures::future::pending::<()>().await;
        Response::new(Body::empty())
    })
    .await;
    let provider = plain(base_url, Duration::from_millis(200));
    let events = tokio::time::timeout(
        Duration::from_secs(10),
        provider.stream(&request(Value::Null)).collect::<Vec<_>>(),
    )
    .await
    .expect("header timeout ends the stream");
    assert!(
        matches!(events.as_slice(), [Err(ProviderError::Unavailable { .. })]),
        "{events:?}"
    );
    assert_eq!(
        *count.lock().unwrap(),
        1,
        "the provider may already be generating, so the request is not resent"
    );
}

#[tokio::test]
async fn connection_dropped_after_the_request_was_sent_is_not_retried() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(Mutex::new(0_usize));
    let counter = accepted.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            *counter.lock().unwrap() += 1;
            let mut buffer = vec![0_u8; 64 * 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buffer).await;
            drop(socket);
        }
    });
    let provider = plain(format!("http://{address}/v1"), Duration::from_secs(5));
    let events = provider
        .stream(&request(Value::Null))
        .collect::<Vec<_>>()
        .await;
    assert!(
        matches!(events.as_slice(), [Err(ProviderError::Unavailable { .. })]),
        "{events:?}"
    );
    assert_eq!(*accepted.lock().unwrap(), 1, "no duplicate generation");
}

#[tokio::test]
async fn refused_connection_is_retried_because_nothing_was_sent() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let provider = plain(format!("http://{address}/v1"), Duration::from_secs(5));
    let events = provider
        .stream(&request(Value::Null))
        .collect::<Vec<_>>()
        .await;
    match events.as_slice() {
        [Err(error @ ProviderError::Unavailable { .. })] => assert!(error.is_retryable()),
        other => panic!("unexpected {other:?}"),
    }
}
