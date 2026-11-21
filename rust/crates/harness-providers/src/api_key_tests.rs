use super::*;
use acyclic_harness_core::model::{ModelContentPart, ToolResultContent};
use std::sync::atomic::{AtomicUsize, Ordering};

const FIRST: &str = concat!(
    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_first\",\"encrypted_content\":\"encrypted-exact==\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"Inspect the input\"}]}}\n\n",
    "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_echo\",\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":1}\"}}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":11,\"output_tokens\":4}}}\n\n",
);
const SECOND: &str = concat!(
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":13,\"output_tokens\":1}}}\n\n",
);

#[tokio::test]
async fn prepared_bodies_keep_the_credential_snapshot_that_selected_native_reasoning() -> Result<()>
{
    use acyclic_harness_core::model::{ReasoningBlock, ReasoningKind, ReasoningOutcome};

    let generation = Arc::new(AtomicUsize::new(0));
    let source = generation.clone();
    let mut credentials = Credentials::default();
    credentials.set_source(move |provider, origin| {
        Ok(
            (provider == "openai" && origin == crate::credentials::Source::Environment)
                .then(|| format!("fixture-{}", source.load(Ordering::SeqCst))),
        )
    });
    let (_, transport) = provider(Adapter::Responses, 200);
    let registry = Registry::with_transport(credentials, transport.clone());
    let provider = Arc::new(registry.resolve("openai/gpt-5")?);
    let (request, _) = request(&provider)?;
    let mut request = request.request().clone();
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Parts(vec![ModelContentPart::Reasoning {
            block: ReasoningBlock {
                kind: ReasoningKind::Encrypted,
                text: None,
                origin: provider.reasoning_origin(Some("fixture-1"), None),
                continuity: br#"{"type":"reasoning","id":"rs_prepared","encrypted_content":"scoped-prepared==","summary":[]}"#.to_vec(),
                outcome: ReasoningOutcome::PreservedOpaque,
            },
        }, ModelContentPart::text("retained answer".into())]),
    });
    let request = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    generation.store(1, Ordering::SeqCst);
    Credentials::changed();
    let prepared =
        Arc::new(crate::prepared::PreparedInput::new(provider.clone(), request.clone()).await?);
    generation.store(2, Ordering::SeqCst);
    Credentials::changed();
    crate::tests::count_result(crate::prepared::PreparedInput::count(prepared.clone())).await?;
    generation.store(3, Ordering::SeqCst);
    Credentials::changed();
    let dispatch = ModelDispatch {
        request_digest: request.request_digest(),
        ..parser_dispatch()
    };
    crate::prepared::PreparedInput::generate(prepared, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    let requests = transport.requests()?;
    assert_eq!(requests.len(), 3);
    for request in &requests[..2] {
        assert_eq!(request.headers["authorization"], "Bearer fixture-1");
        assert!(
            std::str::from_utf8(&request.body)
                .unwrap()
                .contains("scoped-prepared==")
        );
    }
    assert_eq!(requests[2].headers["authorization"], "Bearer fixture-3");
    assert!(
        !std::str::from_utf8(&requests[2].body)
            .unwrap()
            .contains("scoped-prepared==")
    );
    Ok(())
}

struct Responses {
    sent: AtomicUsize,
}
impl Responses {
    fn exchange(&self, request: &HttpRequest) -> Result<HttpResponse> {
        if let Some(response) = fixture_count(request) {
            assert_eq!(
                request.url,
                "https://api.openai.com/v1/responses/input_tokens"
            );
            assert_eq!(request.headers["authorization"], "Bearer fixture-api-key");
            let body = acyclic_harness_provider_response::json(&request.body)?;
            assert_eq!(body["model"], "gpt-6-luna");
            assert!(body.get("stream").is_none());
            assert!(body.get("store").is_none());
            return Ok(response);
        }
        assert_eq!(request.url, "https://api.openai.com/v1/responses");
        assert_eq!(request.headers["authorization"], "Bearer fixture-api-key");
        let body = acyclic_harness_provider_response::json(&request.body)?;
        assert_eq!(body["model"], "gpt-6-luna");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
        assert!(body.get("max_output_tokens").is_none());
        assert_eq!(body["tools"][0]["name"], "echo");
        let step = self.sent.fetch_add(1, Ordering::SeqCst);
        let fixture = match step {
            0 => {
                assert_eq!(body["input"][0]["role"], "user");
                FIRST
            }
            1 => {
                assert_eq!(
                    body["input"][1],
                    json!({"type":"reasoning","id":"rs_first","encrypted_content":"encrypted-exact==","summary":[{"type":"summary_text","text":"Inspect the input"}]})
                );
                assert_eq!(
                    body["input"][2],
                    json!({"type":"function_call","call_id":"call_echo","name":"echo","arguments":"{\"value\":1}"})
                );
                assert_eq!(
                    body["input"][3],
                    json!({"type":"function_call_output","call_id":"call_echo","output":"{\"value\":1}"})
                );
                SECOND
            }
            _ => panic!("unexpected inference retry"),
        };
        Ok(HttpResponse {
            request_id: None,
            retry_after: None,
            status: 200,
            body: chunks(fixture, 1),
        })
    }
}
#[tokio::test]
async fn configured_luna_streams_api_key_reasoning_and_tools_on_ordinary_and_native_paths()
-> Result<()> {
    for native in [false, true] {
        let responses = Arc::new(Responses {
            sent: AtomicUsize::new(0),
        });
        let ordinary = responses.clone();
        let funded = responses.clone();
        let transport = Arc::new(
            ScriptedHttp::respond(move |request| ordinary.exchange(request)).with_native(
                true,
                move |request, work| {
                    let funded = funded.clone();
                    acyclic_fs::NativeFutureBundle::new(work, move || async move {
                        let response = funded.exchange(&request)?;
                        HttpResponse::with_native_body(response.status, response.body, work)
                    })
                },
            ),
        );
        let mut registry = Registry::with_transport(
            Credentials::explicit(BTreeMap::from([(
                "openai".into(),
                "fixture-api-key".into(),
            )])),
            transport.clone(),
        );
        assert!(
            registry.resolve("openai/unregistered-model").is_err(),
            "unregistered context capacity is never guessed"
        );
        let file = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
        std::fs::write(file.path(), include_str!("fixtures/luna-catalog.json"))
            .map_err(|_| invalid("fixture catalog"))?;
        registry.load_catalog(file.path())?;
        let provider = registry.resolve("openai/gpt-6-luna")?;
        assert_eq!(
            provider.context_capacity(provider.model())?.context_tokens,
            8192
        );
        assert_eq!(
            provider.context_capacity(provider.model())?.output_tokens,
            Some(1024)
        );
        let mut request = ModelRequest {
            summary: false,
            compose_tools: false,
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("Run echo".into()),
            }],
            tools: vec![acyclic_harness_core::tool::ToolDefinition {
                name: "echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}),
                output_schema: json!({"type":"object"}),
                projection_schema: json!({"type":"object"}),
            }],
            sampling: Default::default(),
        };
        let operation_id = acyclic_harness_core::OperationId::new();
        let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
        let baseline = work.live_allocation_bytes();
        for step in 0..2 {
            let prepared = PreparedModelRequest::prepare(request.clone(), Limits::default(), None)?;
            let dispatch = ModelDispatch {
                operation_id,
                step,
                request_digest: prepared.request_digest(),
            };
            let count = provider.counted(&prepared, 0).await?;
            assert_eq!(count.request_digest, dispatch.request_digest);
            let mut custody = Vec::new();
            let events = if native {
                assert!(provider.supports_native_model(&work)?);
                let (native_count, hold) = provider
                    .counted_with_native_work(&prepared, 0, &work)?
                    .await?;
                assert_eq!(native_count, count);
                drop(hold);
                let mut stream = provider
                    .generate_with_native_work(&prepared, dispatch, &work)?
                    .await?;
                let mut events = Vec::new();
                while let Some(event) = stream.next().await {
                    let (event, hold) = event?;
                    events.push(event);
                    custody.push(hold);
                }
                events
            } else {
                provider
                    .generate(&prepared, dispatch)
                    .try_collect::<Vec<_>>()
                    .await?
            };
            if step == 0 {
                let parts: Vec<_> = events
                    .into_iter()
                    .filter_map(|event| match event {
                        ModelEvent::ToolCall(call) => Some(ModelContentPart::ToolCall(call)),
                        ModelEvent::ReasoningBlock { block } => {
                            Some(ModelContentPart::Reasoning { block })
                        }
                        _ => None,
                    })
                    .collect();
                assert!(matches!(
                    &parts[..],
                    [
                        ModelContentPart::Reasoning { .. },
                        ModelContentPart::ToolCall(_)
                    ]
                ));
                let saved = serde_json::from_slice(
                    &serde_json::to_vec(&parts).expect("serialized assistant parts"),
                )
                .expect("replayed assistant parts");
                request.messages.push(ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Parts(saved),
                });
                request.messages.push(ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: dispatch.tool_call_id(0),
                        name: "echo".into(),
                        content: ToolResultContent::Json {
                            value: json!({"value":1}),
                        },
                    }),
                });
            } else {
                let events: Vec<_> = events
                    .into_iter()
                    .filter(|event| {
                        !matches!(event, ModelEvent::Trace { .. } | ModelEvent::Wait(_))
                    })
                    .collect();
                assert_eq!(
                    events[0],
                    ModelEvent::Content {
                        delta: "done".into()
                    }
                );
                let ModelEvent::Completed { metadata } = &events[1] else {
                    panic!("completion")
                };
                assert_eq!(metadata["usage"]["input_tokens"], 13);
            }
            drop(custody);
        }
        assert_eq!(responses.sent.load(Ordering::SeqCst), 2);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
