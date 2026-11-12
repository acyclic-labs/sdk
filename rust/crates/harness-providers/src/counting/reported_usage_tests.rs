use crate::tests::{provider, request};
use crate::{Adapter, HttpRequest, HttpResponse, HttpTransport, Result};
use acyclic_harness_core::{conversation::Limits, model::*};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use serde_json::json;
use std::sync::{Arc, Mutex};

struct Reported {
    status: u16,
    body: &'static [u8],
    sent: Mutex<Vec<HttpRequest>>,
}

impl HttpTransport for Reported {
    fn delay(&self, wait: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn delay_with_native_work(
        &self,
        wait: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>> {
        crate::fixtures::native_delay(wait, work)
    }
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, HttpResponse, acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            self.sent.lock().unwrap().push(request);
            HttpResponse::with_native_body(
                self.status,
                futures::stream::iter([Ok(Bytes::from_static(self.body))]),
                work,
            )
        })
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            self.sent.lock().unwrap().push(request);
            Ok(HttpResponse {
                request_id: None,
                retry_after: None,
                status: self.status,
                body: Box::pin(futures::stream::iter([Ok(Bytes::from_static(self.body))])),
            })
        })
    }
}

#[tokio::test]
async fn reported_chat_usage_counts_original_input_on_every_preparation_path() -> Result<()> {
    for explicit in [false, true] {
        let (mut provider, _) = provider(Adapter::Chat, 200);
        provider.model.name = "qwen3-coder".into();
        Arc::make_mut(&mut provider.config).token_counter =
            explicit.then(|| crate::TokenCounterSpec {
                path: "chat/completions".into(),
                adapter: Adapter::Chat,
                response_field: crate::TokenCountField::PromptTokens,
                framing_tokens: 0,
            });
        let transport = Arc::new(Reported {
            status: 200,
            body: br#"{"choices":[],"usage":{"prompt_tokens":41,"completion_tokens":1,"total_tokens":42}}"#,
            sent: Mutex::new(Vec::new()),
        });
        provider.transport =
            crate::deadline::Bounded::new(transport.clone(), crate::TimeoutSpec::DEFAULT);
        let (original, _) = request(&provider)?;
        let mut source = original.request().clone();
        source.messages[1].content = ModelContent::Text("dense input ".repeat(256));
        source.messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Part(ModelContentPart::Reasoning {
                block: ReasoningBlock {
                    kind: ReasoningKind::Readable,
                    text: Some("retained reasoning ".repeat(4096)),
                    origin: provider.reasoning_origin(None, None),
                    continuity: serde_json::to_vec(&json!({
                        "reasoning_content": "retained reasoning ".repeat(4096)
                    }))
                    .expect("reasoning fixture JSON"),
                    outcome: ReasoningOutcome::PreservedReadable,
                },
            }),
        });
        source
            .tools
            .push(acyclic_harness_core::tool::ToolDefinition {
                name: "echo".into(),
                revision: "1".into(),
                description: "original guidance".into(),
                input_schema: json!({"type":"object","properties":{"text":{"type":"string"}}}),
                output_schema: json!({}),
                projection_schema: json!({}),
            });
        let request = PreparedModelRequest::prepare(source, Limits::default(), None)?;
        let generation = crate::projection::body(&provider, request.request())?;
        let count = provider.count_tokens(&request, 0).await?;
        assert_eq!(
            count.input_tokens, 41,
            "reported usage, never a size estimate"
        );
        count.validate(&request, &mut None)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 4_000_000),
            &cancellation,
        )
        .unwrap();
        let baseline = work.live_allocation_bytes();
        let (count, custody) = provider
            .count_tokens_with_native_work(&request, 0, &work)?
            .await?;
        assert_eq!(count.input_tokens, 41);
        drop(count);
        drop(custody);
        let shared = crate::prepared::PreparedInput::new(Arc::new(provider), request).await?;
        assert_eq!(shared.count().await?.input_tokens, 41);
        let mut expected = generation;
        expected["stream"] = json!(false);
        expected["max_completion_tokens"] = json!(1);
        expected.as_object_mut().unwrap().remove("stream_options");
        let mut sent = transport.sent.lock().unwrap();
        assert_eq!(sent.len(), 3);
        for exchange in sent.iter() {
            assert!(exchange.url.ends_with("/v1/chat/completions"));
            assert_eq!(
                crate::json(&exchange.body)?,
                expected,
                "input changed while counting"
            );
        }
        // Captured native request bytes retain their original custody.
        sent.clear();
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn absent_reported_chat_usage_refuses_instead_of_estimating() -> Result<()> {
    let (mut provider, _) = provider(Adapter::Chat, 200);
    Arc::make_mut(&mut provider.config).token_counter = None;
    let transport = Arc::new(Reported {
        status: 200,
        body: br#"{"choices":[],"usage":{"completion_tokens":1,"total_tokens":42}}"#,
        sent: Mutex::new(Vec::new()),
    });
    provider.transport =
        crate::deadline::Bounded::new(transport.clone(), crate::TimeoutSpec::DEFAULT);
    let (request, _) = request(&provider)?;
    let error = provider.count_tokens(&request, 0).await.unwrap_err();
    assert!(error.to_string().contains("invalid token count response"));
    assert_eq!(transport.sent.lock().unwrap().len(), 1);
    Ok(())
}

#[test]
fn reported_chat_usage_remains_a_strict_portable_integer() {
    let spec = crate::TokenCounterSpec {
        path: "chat/completions".into(),
        adapter: Adapter::Chat,
        response_field: crate::TokenCountField::PromptTokens,
        framing_tokens: 0,
    };
    for value in [
        json!(-1),
        json!(1.5),
        json!("secret"),
        json!(null),
        json!(u64::MAX),
        json!({"$serde_json::private::Number":"41"}),
    ] {
        let error = super::wire::response(
            &serde_json::to_vec(&json!({"usage":{"prompt_tokens":value}})).unwrap(),
            &spec,
        )
        .unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
}

#[test]
fn chat_catalog_defaults_accept_reported_usage_but_other_counter_contracts_stay_strict()
-> Result<()> {
    let mut registry = crate::Registry::new(crate::Credentials::explicit(Default::default()))?;
    let mut config = registry.catalog()["openai"].as_ref().clone();
    config.adapter = Adapter::Chat;
    config.authentication = crate::Authentication::Optional;
    config.token_counter = None;
    let name = config.models.keys().next().unwrap().clone();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        serde_json::to_vec(
            &json!({"fireworks":config,"default_model":format!("fireworks/{name}")}),
        )
        .unwrap(),
    )
    .unwrap();
    registry.load_catalog(file.path())?;
    let mut explicit = registry.catalog()["fireworks"].as_ref().clone();
    explicit.token_counter = Some(crate::TokenCounterSpec {
        path: "chat/completions".into(),
        adapter: Adapter::Chat,
        response_field: crate::TokenCountField::PromptTokens,
        framing_tokens: 0,
    });
    registry.register("fireworks".into(), explicit.clone())?;
    explicit.token_counter.as_mut().unwrap().response_field = crate::TokenCountField::InputTokens;
    assert!(
        registry
            .register("fireworks".into(), explicit.clone())
            .is_err()
    );
    let spec = explicit.token_counter.as_mut().unwrap();
    spec.response_field = crate::TokenCountField::PromptTokens;
    spec.path = "responses/input_tokens".into();
    assert!(registry.register("fireworks".into(), explicit).is_err());
    Ok(())
}

#[tokio::test]
async fn reported_chat_context_refusal_is_typed_and_never_retried() -> Result<()> {
    let (mut provider, _) = provider(Adapter::Chat, 200);
    Arc::make_mut(&mut provider.config).token_counter = None;
    let transport = Arc::new(Reported {
        status: 400,
        body: br#"{"error":{"code":"context_length_exceeded","message":"maximum context length exceeded"}}"#,
        sent: Mutex::new(Vec::new()),
    });
    provider.transport =
        crate::deadline::Bounded::new(transport.clone(), crate::TimeoutSpec::DEFAULT);
    let (request, _) = request(&provider)?;
    assert!(matches!(
        provider.count_tokens(&request, 0).await,
        Err(acyclic_harness_core::Error::ModelContextOverflow)
    ));
    assert_eq!(transport.sent.lock().unwrap().len(), 1);
    Ok(())
}
