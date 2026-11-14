//! A catalog proxy's delayed 502, permanent refusal and JSON/SSE completions
//! cross the real local host, durable journal, events and usage queries.
use super::*;
use acyclic_fs::NativeFutureBundle;
use acyclic_harness::{LocalHarness, local::agent_conversation::Conversation};
use acyclic_harness_core::{
    agent::{IntentId, Phase, Sender},
    context::CompactionPolicy,
    executor::{ModelPriceRecord, ModelUsageProjection, TokenPrice},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

struct Proxy {
    calls: AtomicUsize,
    denied: AtomicBool,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    waits: Mutex<Vec<Duration>>,
}

impl Proxy {
    async fn exchange(&self, request: HttpRequest) -> Result<HttpResponse> {
        if let Some(count) = fixture_count(&request) {
            assert!(request.url.ends_with("/responses/input_tokens"));
            return Ok(count);
        }
        assert!(request.url.ends_with("/responses"));
        assert!(!request.headers.contains_key("authorization"));
        let request: Value =
            serde_json::from_slice(&request.body).map_err(|error| invalid(error.to_string()))?;
        assert_eq!(
            request["stream"], true,
            "every generation requests streaming"
        );
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 1 {
            let entered = self.entered.lock().unwrap().take().unwrap();
            let release = self.release.lock().unwrap().take().unwrap();
            entered.send(()).unwrap();
            release.await.map_err(|_| invalid("proxy release lost"))?;
        }
        let (status, body) = if self.denied.load(Ordering::SeqCst) {
            (401, "authentication denied".to_owned())
        } else if call == 1 {
            (502, "upstream openai unreachable: TimeoutError".to_owned())
        } else {
            let (input, output) = match call {
                0 => (11, 3),
                2 => (13, 2),
                _ => (17, 4),
            };
            let response = json!({"object":"response", "id":format!("call-{call}"),
                "status":"completed", "output":[{"type":"message", "role":"assistant",
                "content":[{"type":"output_text", "text":"done"}]}],
                "usage":{"input_tokens":input, "output_tokens":output}});
            // One complete JSON response, then the same terminal through SSE.
            let body = if call == 0 {
                format!(" \n{response}\n")
            } else {
                format!(
                    "data: {}\n\n",
                    json!({"type":"response.completed", "response":response})
                )
            };
            (200, body)
        };
        Ok(HttpResponse {
            status,
            body: chunks(body, 1),
            request_id: None,
            retry_after: None,
        })
    }
}

impl HttpTransport for Proxy {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(self.exchange(request))
    }
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse, acyclic_harness_core::Error>> {
        NativeFutureBundle::new(work, || async move {
            let response = self.exchange(request).await?;
            HttpResponse::with_native_body(response.status, response.body, work)
        })
    }
    fn now(&self) -> Duration {
        Duration::ZERO
    }
    fn delay(&self, wait: Duration) -> BoxProviderFuture<'static, Result<()>> {
        self.waits.lock().unwrap().push(wait);
        Box::pin(async { Ok(()) })
    }
    fn delay_with_native_work(
        &self,
        wait: Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'static, (), acyclic_harness_core::Error>> {
        let timer = self.delay(wait);
        NativeFutureBundle::new(work, || timer)
    }
}

#[tokio::test]
async fn delayed_502_preserves_usage_and_later_turns_through_catalog_proxy() -> Result<()> {
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    let proxy = Arc::new(Proxy {
        calls: AtomicUsize::new(0),
        denied: AtomicBool::new(false),
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(Some(resume)),
        waits: Mutex::new(Vec::new()),
    });
    let mut registry =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), proxy.clone())?;
    let mut spec: BTreeMap<String, ProviderSpec> =
        serde_json::from_str(include_str!("fixtures/luna-catalog.json"))
            .map_err(|error| invalid(error.to_string()))?;
    let mut spec = spec.remove("openai").unwrap();
    spec.authentication = Authentication::Optional;
    spec.base_url = "https://key-injecting-proxy.invalid/v1".into();
    registry.register("catalog".into(), spec.clone())?;
    // Negative control: a count projection on /responses silently becomes
    // non-streaming inference. Refuse this configuration before any HTTP call.
    spec.token_counter.as_mut().unwrap().path = "responses".into();
    assert!(registry.register("misrouted".into(), spec).is_err());
    let provider = Arc::new(registry.resolve("catalog/gpt-6-luna")?);
    let directory = tempfile::tempdir().map_err(|error| invalid(error.to_string()))?;
    let config = acyclic_harness::test_support::local_harness_config(
        directory.path(),
        (provider.model().clone(), provider.clone()),
    )?
    .compaction(CompactionPolicy::Disabled)
    // The proxy scripts one generation per prompt; settlement notices would add more.
    .feedback_plugin(false);
    let harness = Arc::new(LocalHarness::open(config).await?);
    harness.agents().await?;
    let connection = harness.connect().await?;
    let conversation = Conversation::open(connection);
    let prompt =
        |id: &'static str| acyclic_harness::test_support::prompt_settled(&harness, id, "continue");
    assert!(matches!(
        prompt("first").await?,
        acyclic_harness::test_support::PromptOutcome::Settled(_, _)
    ));
    let second = prompt("delayed");
    let mut second = core::pin::pin!(second);
    tokio::select! {
        result = &mut second => panic!("turn ended before the proxy answered: {result:?}"),
        result = waiting => result.map_err(|_| invalid("proxy never entered"))?,
    }
    // A pending model call leaves its admission durable and the prior call intact.
    let mut pending = Vec::new();
    conversation.write_usage_jsonl(&mut pending).await?;
    let rows = usage_rows(&pending)?;
    assert_eq!(rows.len(), 2);
    assert!(rows[0].usage.completed);
    assert_eq!(rows[0].usage.tokens.input, Some(11));
    assert!(!rows[1].usage.completed);
    release.send(()).unwrap();
    assert!(matches!(
        second.await?,
        acyclic_harness::test_support::PromptOutcome::Settled(_, _)
    ));
    proxy.denied.store(true, Ordering::SeqCst);
    let failed = prompt("denied").await?;
    assert!(
        matches!(failed, acyclic_harness::test_support::PromptOutcome::Failed(ref message)
        if message.contains("401") && !message.contains("storage failure")),
        "{failed:?}"
    );
    let (agents, root) = harness.agents().await?;
    assert!(
        !agents
            .state(root)
            .await
            .map_err(|e| invalid(e.to_string()))?
            .closed
    );
    assert!(matches!(conversation.phase().await?, Phase::Idle));
    proxy.denied.store(false, Ordering::SeqCst);
    assert!(matches!(
        prompt("after-denied").await?,
        acyclic_harness::test_support::PromptOutcome::Settled(_, _)
    ));
    let mut exported = Vec::new();
    conversation.write_usage_jsonl(&mut exported).await?;
    let rows = usage_rows(&exported)?;
    assert_eq!(
        rows.len(),
        4,
        "one durable row per admitted call, retries never duplicate it"
    );
    assert_eq!(rows.iter().filter(|row| row.usage.completed).count(), 3);
    assert_eq!(
        rows.iter()
            .filter_map(|row| row.usage.tokens.input)
            .sum::<u64>(),
        41
    );
    assert_eq!(
        rows.iter()
            .filter_map(|row| row.usage.tokens.output)
            .sum::<u64>(),
        9
    );
    let price = ModelPriceRecord {
        version: 1,
        revision: "test-price".into(),
        provider: "catalog".into(),
        model: "gpt-6-luna".into(),
        effective_at: 0,
        currency: "USD".into(),
        input_nano_units_per_million: Some(2_000_000_000),
        output_nano_units_per_million: Some(8_000_000_000),
        cached: TokenPrice::Included,
        cache_write: TokenPrice::Included,
        reasoning: TokenPrice::Included,
    };
    let costs: Vec<_> = rows
        .iter()
        .map(|row| {
            row.usage
                .estimate(Some(&price))
                .unwrap()
                .estimated_cost
                .token_nano_units
        })
        .collect();
    assert_eq!(costs, [Some(46_000), Some(42_000), None, Some(66_000)]);
    let mut usage = conversation.usage_query();
    while !usage.synchronized() {
        usage.advance().await?;
    }
    assert_eq!(usage.totals().calls, 4);
    assert_eq!(usage.totals().input.known, 41);
    assert_eq!(
        usage.totals().input.unknown,
        1,
        "unreported failed-call usage stays unknown"
    );
    assert_eq!(proxy.waits.lock().unwrap().len(), 1);
    let mut position = acyclic_harness::client::ExecutionPosition::default();
    loop {
        let page = conversation
            .execution_page(position.input, position.after, false)
            .await?;
        position = page.continuation()?;
        if page.page.is_none() {
            break;
        }
    }
    agents
        .archive(
            root,
            &Sender::User {
                principal: "fixture".into(),
            },
            &IntentId::try_from("archive").map_err(|e| invalid(e.to_string()))?,
        )
        .await
        .map_err(|e| invalid(e.to_string()))?;
    agents
        .drain(root)
        .await
        .map_err(|e| invalid(e.to_string()))?;
    // The archived producer's journal remains readable and live queries reach
    // a terminal tail, rather than rejecting the agent or waiting forever.
    loop {
        let page = conversation
            .execution_page(position.input, position.after, true)
            .await?;
        position = page.continuation()?;
        if page.page.is_none() {
            assert!(page.settled);
            break;
        }
    }
    let mut after = Vec::new();
    conversation.write_usage_jsonl(&mut after).await?;
    assert_eq!(exported, after);
    Ok(())
}

fn usage_rows(bytes: &[u8]) -> Result<Vec<ModelUsageProjection<'_>>> {
    serde_json::Deserializer::from_slice(bytes)
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| invalid(error.to_string()))
}

#[test]
fn counters_cannot_alias_generation_routes_on_any_protocol() -> Result<()> {
    let (_, transport) = provider(Adapter::Responses, 200);
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport)?;
    for (adapter, route) in [
        (Adapter::Responses, "responses"),
        (Adapter::Chat, "chat/completions"),
        (Adapter::Anthropic, "messages"),
        (Adapter::Gemini, "models/{model}:streamGenerateContent"),
    ] {
        let (original, _) = provider(adapter, 200);
        let mut spec = original.config.as_ref().clone();
        spec.authentication = Authentication::Optional;
        for path in [route.to_owned(), format!("./{route}/")] {
            spec.token_counter.as_mut().unwrap().path = path;
            let error = registry
                .register("misrouted".into(), spec.clone())
                .err()
                .expect("generation route admitted as a counter");
            assert!(
                error
                    .to_string()
                    .contains("must not use the generation endpoint"),
                "{error}"
            );
        }
        spec.token_counter.as_mut().unwrap().path = "count_tokens".into();
        registry.register("counter".into(), spec)?;
    }
    Ok(())
}

#[tokio::test]
async fn truncated_responses_preserve_transport_cause_on_both_provider_paths() -> Result<()> {
    for wire in [
        r#"{"object":"response","status":"completed","output":["#,
        r#"{"object":"response","status":"completed","output":[],"usage":{"input_tokens":11,"output_tokens":"#,
    ] {
        for native in [false, true] {
            let transport = Arc::new(super::native_tests::Fixture::new(wire));
            let provider = provider_with(Adapter::Responses, "http://127.0.0.1:1234/v1", transport);
            let (request, dispatch) = request(&provider)?;
            let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
            let baseline = work.live_allocation_bytes();
            // A truncated exchange recovers indefinitely. Observe its cause
            // before the fixture's held timer, then cancel by dropping the stream.
            let event = if native {
                let mut stream = provider
                    .generate_with_native_work(&request, dispatch, &work)?
                    .await?;
                let (restarted, hold) = stream.next().await.expect("restart event")?;
                assert!(matches!(restarted, ModelEvent::Restarted), "{restarted:?}");
                drop(hold);
                let (event, hold) = stream.next().await.expect("recovery event")?;
                drop(hold);
                event
            } else {
                let mut stream = provider.generate(&request, dispatch);
                let restarted = stream.next().await.expect("restart event")?;
                assert!(matches!(restarted, ModelEvent::Restarted), "{restarted:?}");
                stream.next().await.expect("recovery event")?
            };
            assert!(
                matches!(
                    event,
                    ModelEvent::Wait(acyclic_harness_core::model::ModelWait::Started {
                        cause: Some(acyclic_harness_core::ModelTransportFailure::Interrupted),
                        ..
                    })
                ),
                "{event:?}"
            );
            drop(event);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[tokio::test]
async fn complete_responses_share_native_parts_tools_and_usage_with_streamed_responses()
-> Result<()> {
    let wire = concat!(
        r#"{"object":"response","status":"completed","output":["#,
        r#"{"type":"reasoning","id":"rs","encrypted_content":"opaque\u003d","summary":[]},"#,
        r#"{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]},"#,
        r#"{"type":"function_call","call_id":"echo-1","name":"echo","arguments":"{}"}],"#,
        r#""usage":{"input_tokens":11,"output_tokens":3}}"#
    );
    for native in [false, true] {
        let transport = Arc::new(super::native_tests::Fixture::new(wire));
        let provider = provider_with(Adapter::Responses, "http://127.0.0.1:1234/v1", transport);
        let (request, dispatch) = request(&provider)?;
        let work = super::native_tests::work(&acyclic_fs::CancellationToken::new())?;
        let baseline = work.live_allocation_bytes();
        let mut holds = Vec::new();
        let events = if native {
            let mut stream = provider
                .generate_with_native_work(&request, dispatch, &work)?
                .await?;
            let mut events = Vec::new();
            while let Some(event) = stream.next().await {
                let (event, hold) = event?;
                events.push(event);
                holds.push(hold);
            }
            events
        } else {
            provider
                .generate(&request, dispatch)
                .try_collect::<Vec<_>>()
                .await?
        };
        assert!(
            events.iter().any(
                |event| matches!(event, ModelEvent::MessageCompleted { text } if text == "done")
            )
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelEvent::ToolCall(call) if call.name == "echo"))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelEvent::ReasoningBlock { block }
            if String::from_utf8_lossy(&block.continuity).contains("opaque\\u003d")))
        );
        assert!(
            matches!(events.last(), Some(ModelEvent::Completed { metadata })
            if metadata["usage"]["input_tokens"] == 11 && metadata["usage"]["output_tokens"] == 3)
        );
        drop(holds);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    // The former SSE-only decoder loses this entire response, including usage.
    let input = crate::sse::frames(chunks(wire, 1), 4096, crate::sse::Decoder::default());
    assert!(input.try_collect::<Vec<_>>().await.is_err());
    Ok(())
}
