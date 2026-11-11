//! Explicit live acceptance, separate from deterministic provider conformance.
//! Only API keys supplied in the process environment are admitted. Nothing
//! prints credentials, request headers, prompts, response bodies or raw errors.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_harness_core::{
    AgentId, OperationId,
    context::{
        CompactionPolicy, Context, ContextPipeline, ContextPlacement, ContextStage, SourceStage,
    },
    executor::{Executor, StockExecutor, TurnInput, usage_page},
    filesystem::MemoryHarnessStorage,
    model::{ModelContent, ModelMessage, ModelRole},
    tool::ToolRegistry,
};
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, HttpRequest, HttpResponse, HttpTransport, Registry,
    ReqwestTransport,
};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use futures::StreamExt as _;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

type ProbeResult<T = ()> = std::result::Result<T, &'static str>;

#[derive(Clone, Copy, Default)]
struct WireUsage {
    observed: bool,
    input: Option<u64>,
    cached: Option<u64>,
    created: Option<u64>,
}

// Observe provider fields before the protocol adapter or durable usage mapping.
// The shared SSE primitive handles framing; the payload JSON remains unchanged.
struct ObservedTransport {
    inner: ReqwestTransport,
    adapter: Adapter,
    calls: Arc<Mutex<Vec<WireUsage>>>,
}

// Keep the synchronous observation lock outside the fallible async stream.
// try_stream turns an error into a yield, so no guard may live at a question mark.
fn retain_wire_usage(
    calls: &Mutex<Vec<WireUsage>>,
    slot: usize,
    adapter: Adapter,
    usage: &Value,
) -> acyclic_harness_core::Result<()> {
    let mut calls = calls.lock().unwrap();
    let row = &mut calls[slot];
    row.observed = true;
    let (input, cached, created) = match adapter {
        Adapter::Anthropic => (
            &usage["input_tokens"],
            &usage["cache_read_input_tokens"],
            &usage["cache_creation_input_tokens"],
        ),
        Adapter::Responses => (
            &usage["input_tokens"],
            &usage["input_tokens_details"]["cached_tokens"],
            &Value::Null,
        ),
        Adapter::Gemini => (
            &usage["promptTokenCount"],
            &usage["cachedContentTokenCount"],
            &Value::Null,
        ),
        Adapter::Chat => unreachable!(),
    };
    for (original, retained) in [
        (input, &mut row.input),
        (cached, &mut row.cached),
        (created, &mut row.created),
    ] {
        if !original.is_null() {
            *retained = Some(original.as_u64().ok_or_else(|| {
                acyclic_harness_core::Error::Invalid("live cache wire counter is malformed".into())
            })?);
        }
    }
    Ok(())
}

impl HttpTransport for ObservedTransport {
    fn now(&self) -> Duration {
        self.inner.now()
    }
    fn delay(
        &self,
        duration: Duration,
    ) -> BoxProviderFuture<'static, acyclic_harness_core::Result<()>> {
        self.inner.delay(duration)
    }
    fn send(
        &self,
        request: HttpRequest,
    ) -> BoxProviderFuture<'_, acyclic_harness_core::Result<HttpResponse>> {
        Box::pin(async move {
            let slot = {
                let mut calls = self.calls.lock().unwrap();
                let slot = calls.len();
                calls.push(WireUsage::default());
                slot
            };
            let response = self.inner.send(request).await?;
            if !(200..300).contains(&response.status) {
                return Ok(response);
            }
            let calls = self.calls.clone();
            let adapter = self.adapter;
            let body = Box::pin(async_stream::try_stream! {
                let frames = acyclic_harness_core::sse::frames(
                    response.body, None, acyclic_harness_core::sse::Decoder::default().with_json_document(),
                );
                let mut frames = std::pin::pin!(frames);
                while let Some(frame) = frames.next().await {
                    let frame = frame?;
                    if frame.data() != "[DONE]" {
                        let value: Value = serde_json::from_str(frame.data())
                            .map_err(|_| acyclic_harness_core::Error::Invalid("live cache wire JSON is malformed".into()))?;
                        let usage = match adapter {
                            Adapter::Anthropic => value.get("message").and_then(|message| message.get("usage")).or_else(|| value.get("usage")),
                            Adapter::Responses => value.get("response").and_then(|response| response.get("usage")),
                            Adapter::Gemini => value.get("usageMetadata"),
                            Adapter::Chat => None,
                        };
                        if let Some(usage) = usage {
                            retain_wire_usage(&calls, slot, adapter, usage)?;
                        }
                    }
                    let mut original_data = String::new();
                    for line in frame.data().split('\n') {
                        original_data.push_str("data: ");
                        original_data.push_str(line);
                        original_data.push('\n');
                    }
                    original_data.push('\n');
                    yield Bytes::from(original_data);
                }
            });
            Ok(HttpResponse {
                body,
                status: response.status,
                request_id: response.request_id,
                retry_after: response.retry_after,
            })
        })
    }
}

async fn run_call(
    registry: &Registry,
    provider: &str,
    model: &str,
    prefix: String,
    question: &str,
) -> ProbeResult<Option<u64>> {
    let resolved = registry
        .resolve(&format!("{provider}/{model}"))
        .map_err(|_| "live model selection failed")?;
    let model = resolved.model().clone();
    let context = Context {
        messages: vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text(prefix),
        }],
        ..Default::default()
    };
    let pipeline = ContextPipeline::new([Arc::new(SourceStage::new(
        "live-cache-prefix",
        "1",
        Arc::new(context),
        ContextPlacement::Prepend,
    )) as Arc<dyn ContextStage>]);
    let executor = StockExecutor::new(model, Arc::new(resolved), pipeline, ToolRegistry::default())
        .with_compaction_policy(CompactionPolicy::Disabled)
        .with_max_output_tokens(512)
        .map_err(|_| "live output reservation failed")?;
    let storage = MemoryHarnessStorage::new(AgentId::new(), Some(65_536))
        .await
        .map_err(|_| "live journal creation failed")?;
    let journal = storage.journal();
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text(question.into()),
        selected_context: None,
        max_steps: Some(1),
    };
    executor
        .execute(input.clone(), journal.as_ref())
        .await
        .map_err(|_| "live provider execution failed (raw diagnostics suppressed)")?;
    let mut after = 0;
    let mut records = Vec::new();
    loop {
        let page = usage_page(journal.as_ref(), input.operation_id, "live-cache", after)
            .await
            .map_err(|_| "live durable usage read failed")?;
        records.extend(page.items.into_iter().filter(|row| row.completed));
        match page.next {
            Some(next) => after = next,
            None => break,
        }
    }
    if records.len() != 1 {
        return Err("live probe did not retain exactly one completed model call");
    }
    Ok(records[0].tokens.cached)
}

/// Nine generation requests total, no retry or cache-readiness polling.
#[tokio::test]
#[ignore = "real provider acceptance: explicit API keys and user-approved spend required"]
async fn real_provider_cache_reads_match_recorded_usage() -> ProbeResult {
    // A loopback SSH relay can keep Infisical keys in its local process when
    // the native test host has no secret-store login. It changes only auth and
    // the upstream origin; provider request and response bodies pass unchanged.
    let relay = std::env::var("ACYCLIC_LIVE_CACHE_RELAY").ok();
    if let Some(relay) = &relay {
        let url = url::Url::parse(relay).map_err(|_| "live relay URL is invalid")?;
        if url.scheme() != "http"
            || url.host_str() != Some("127.0.0.1")
            || url.port().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("live relay must use an explicit loopback HTTP port");
        }
    }
    let models = [
        (
            "anthropic",
            "claude-haiku-5-5",
            "ANTHROPIC_API_KEY",
            Adapter::Anthropic,
        ),
        ("openai", "gpt-6-luna", "OPENAI_API_KEY", Adapter::Responses),
        (
            "gemini",
            "gemini-3.8-flash",
            "GOOGLE_API_KEY",
            Adapter::Gemini,
        ),
    ];
    let selected = std::env::var("ACYCLIC_LIVE_CACHE_PROVIDER").ok();
    if selected
        .as_deref()
        .is_some_and(|selected| !models.iter().any(|(provider, ..)| *provider == selected))
    {
        return Err("live provider selection is invalid");
    }
    let models = models.into_iter().filter(|(provider, ..)| {
        selected
            .as_deref()
            .is_none_or(|selected| selected == *provider)
    });
    // Admit every selected credential before the first paid request. Explicit
    // credentials disable environment/keychain/subscription source discovery.
    let keys = models
        .clone()
        .map(|(provider, _, variable, _)| {
            if relay.is_some() {
                return Ok((provider.to_string(), "live-cache-relay".into()));
            }
            std::env::var(variable)
                .ok()
                .filter(|value| !value.is_empty())
                .map(|key| (provider.to_string(), key))
                .ok_or("live API keys are missing")
        })
        .collect::<ProbeResult<BTreeMap<_, _>>>()?;
    let run = uuid::Uuid::new_v4();
    for (provider, model, _, adapter) in models {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = Arc::new(ObservedTransport {
            inner: ReqwestTransport::new().map_err(|_| "live HTTP initialization failed")?,
            adapter,
            calls: calls.clone(),
        });
        let mut registry = Registry::with_transport(Credentials::explicit(keys.clone()), transport);
        let mut spec = registry.catalog()[provider].as_ref().clone();
        if let Some(relay) = &relay {
            spec.base_url = format!("{}/{provider}", relay.trim_end_matches('/'));
        }
        spec.authentication = Authentication::ApiKey;
        spec.retry.attempts = 0;
        registry
            .register(provider.into(), spec)
            .map_err(|_| "live API-key selection failed")?;
        // A fresh first token prevents another run's cache from satisfying this
        // proof. 6,144 words exceed the largest cache minimum in this probe;
        // the first provider-reported input below verifies actual token volume.
        let prefix = format!(
            "cache-proof:{run}:{provider}\nIgnore the reference words. Reply only OK.\n{}",
            " pine".repeat(6144)
        );
        let changed = format!(
            "changed-cache-proof:{run}:{provider}\nIgnore the reference words. Reply only OK.\n{}",
            " pine".repeat(6144)
        );
        let mut retained = Vec::new();
        for (prefix, question) in [
            (prefix.clone(), "Reply OK."),
            (prefix, "Reply OK again."),
            (changed, "Reply OK."),
        ] {
            retained.push(run_call(&registry, provider, model, prefix, question).await?);
        }
        let calls = calls.lock().unwrap();
        if calls.len() != 3 || calls.iter().any(|row| !row.observed) {
            return Err("live probe did not observe exactly three original usage responses");
        }
        let first = calls[0];
        let input = if adapter == Adapter::Anthropic {
            first.input.and_then(|input| {
                input
                    .checked_add(first.cached?)
                    .and_then(|input| input.checked_add(first.created?))
            })
        } else {
            first.input
        };
        if input.is_none_or(|tokens| tokens <= 4096) {
            return Err("live prefix did not exceed the cache minimum");
        }
        if calls[1].cached.is_none_or(|tokens| tokens == 0) {
            return Err("second live request reported no cache read");
        }
        if calls[2].cached.is_some_and(|tokens| tokens != 0) {
            return Err("changed-prefix negative control reported a cache read");
        }
        for (wire, recorded) in calls.iter().zip(retained) {
            if wire.cached != recorded {
                return Err("recorded cache read differs from the original provider field");
            }
        }
        println!(
            "LIVE_CACHE_PASS provider={provider} model={model} calls=3 second_cached={} negative_cached={:?}",
            calls[1].cached.unwrap(),
            calls[2].cached
        );
    }
    Ok(())
}
