use super::*;
use acyclic_harness::model::{ModelContentPart, ToolResultContent};
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

struct Responses {
    sent: AtomicUsize,
}
impl Responses {
    fn exchange(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(response) = fixture_count(&request) {
            assert_eq!(
                request.url,
                "https://api.openai.com/v1/responses/input_tokens"
            );
            assert_eq!(request.headers["authorization"], "Bearer fixture-api-key");
            let body = json(&request.body)?;
            assert_eq!(body["model"], "gpt-6-luna");
            assert!(body.get("stream").is_none());
            assert!(body.get("store").is_none());
            return Ok(response);
        }
        assert_eq!(request.url, "https://api.openai.com/v1/responses");
        assert_eq!(request.headers["authorization"], "Bearer fixture-api-key");
        let body = json(&request.body)?;
        assert_eq!(body["model"], "gpt-6-luna");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(body["max_output_tokens"], 64);
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
            retry_after: None,
            status: 200,
            body: chunks(fixture, 1),
        })
    }
}
impl HttpTransport for Responses {
    fn delay(&self, wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<acyclic_harness::NativeFutureBundle<'static, ()>> {
        crate::fixtures::native_delay(wait, work)
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move { self.exchange(request) })
    }
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let response = self.exchange(request)?;
            HttpResponse::with_native_body(response.status, response.body, work)
        })
    }
}

#[tokio::test]
async fn configured_luna_streams_api_key_reasoning_and_tools_on_ordinary_and_native_paths()
-> Result<()> {
    for native in [false, true] {
        let transport = Arc::new(Responses {
            sent: AtomicUsize::new(0),
        });
        let mut registry = Registry::with_transport(
            Credentials::explicit(BTreeMap::from([(
                "openai".into(),
                "fixture-api-key".into(),
            )])),
            transport.clone(),
        );
        assert!(
            registry.resolve("openai/gpt-6-luna").is_err(),
            "unknown capacity is never guessed"
        );
        let file = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
        std::fs::write(file.path(), include_str!("fixtures/luna-catalog.json"))
            .map_err(|_| invalid("fixture catalog"))?;
        registry.load_catalog(file.path())?;
        let provider = registry
            .resolve_with_options("openai/gpt-6-luna", json!({"reasoning":{"effort":"low"}}))?;
        assert_eq!(
            provider.context_capacity(provider.model())?.context_tokens,
            8192
        );
        assert_eq!(
            provider.context_capacity(provider.model())?.output_tokens,
            1024
        );
        let mut request = ModelRequest {
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("Run echo".into()),
            }],
            tools: vec![acyclic_harness::tool::ToolDefinition {
                name: "echo".into(),
                revision: "1".into(),
                description: "Echo".into(),
                input_schema: json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}),
                output_schema: json!({"type":"object"}),
                projection_schema: json!({"type":"object"}),
            }],
            max_output_tokens: Some(64),
        };
        let operation_id = acyclic_harness::OperationId::new();
        let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
        let baseline = work.live_allocation_bytes();
        for step in 0..2 {
            let prepared = PreparedModelRequest::prepare(request.clone(), Limits::default(), None)?;
            let dispatch = ModelDispatch {
                operation_id,
                step,
                request_digest: prepared.request_digest(),
            };
            let count = provider.count_tokens(&prepared).await?;
            assert_eq!(count.request_digest, dispatch.request_digest);
            let mut custody = Vec::new();
            let events = if native {
                assert!(provider.supports_native_model(&work)?);
                let (native_count, hold) = provider
                    .count_tokens_with_native_work(&prepared, &work)?
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
                    .generate(prepared, dispatch)
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
        assert_eq!(transport.sent.load(Ordering::SeqCst), 2);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
