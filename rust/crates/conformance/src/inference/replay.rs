//! A replay uses the production registry, request projection, HTTP pipeline and decoder.
//! Only the HTTP boundary is replaced with hash-checked, unmodified captured bytes.
use super::*;
use acyclic_harness_core::{
    Error, Result,
    context::ModelContextCapacity,
    conversation::Limits,
    model::{
        ModelContent, ModelDispatch, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest,
    },
};
use acyclic_harness_providers::{
    Adapter, Credentials, HttpRequest, HttpResponse, HttpTransport, ModelFeatures, ModelSpec,
    ProviderSpec, Registry,
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use bytes::Bytes;
use futures::Stream;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

/// Immutable response body and its original HTTP status.
#[derive(Clone)]
struct Capture {
    status: u16,
    bytes: Bytes,
    assertions: BTreeMap<String, Value>,
    request_url: Option<String>,
}

#[derive(Deserialize)]
struct FileCapture {
    status: u16,
    file: String,
    sha256: String,
    #[serde(default)]
    assertions: BTreeMap<String, Value>,
    #[serde(default)]
    request_url: Option<String>,
}

/// Provenance and expectations accompany captured bytes; they never replace the response.
#[derive(Deserialize)]
pub struct Manifest {
    /// Capture origin, required for every fixture family.
    pub source: String,
    /// Original capture time in UTC.
    pub captured_at: String,
    /// Wire dialect of the actual captured response.
    pub adapter: Adapter,
    /// Registry identity used when the endpoint was captured.
    pub provider_id: String,
    /// Original public endpoint configuration, excluding credentials.
    pub endpoint: ProviderSpec,
    /// Selected model name.
    pub model: String,
    /// Exact model options used for the captures, excluding credentials.
    #[serde(default = "options")]
    pub options: Value,
    /// Independent expected token counters for the text step.
    pub usage: ExpectedUsage,
    /// Independent expected counters for tool and result completions.
    pub tool_usage: [ExpectedUsage; 2],
    /// Original input that actually exceeded this model's context capacity.
    pub context_input: String,
    captures: BTreeMap<String, FileCapture>,
}
fn options() -> Value {
    json!({})
}
struct Replay {
    responses: Mutex<VecDeque<Capture>>,
    active: Arc<AtomicUsize>,
}
struct Body {
    bytes: Bytes,
    active: Arc<AtomicUsize>,
}
struct TrackedBody {
    body: BoxProviderStream<'static, Result<Bytes>>,
    active: Arc<AtomicUsize>,
}
impl Stream for TrackedBody {
    type Item = Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.body.as_mut().poll_next(context)
    }
}
impl Drop for TrackedBody {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct TrackedTransport {
    inner: acyclic_harness_providers::ReqwestTransport,
    active: Arc<AtomicUsize>,
}
impl HttpTransport for TrackedTransport {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let mut response = self.inner.send(request).await?;
            self.active.fetch_add(1, Ordering::SeqCst);
            response.body = Box::pin(TrackedBody {
                body: response.body,
                active: self.active.clone(),
            });
            Ok(response)
        })
    }
    fn delay(&self, duration: Duration) -> BoxProviderFuture<'static, Result<()>> {
        self.inner.delay(duration)
    }
    fn now(&self) -> Duration {
        self.inner.now()
    }
}
impl Stream for Body {
    type Item = Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.bytes.is_empty() {
            Poll::Ready(None)
        } else {
            let width = self.bytes.len().min(7);
            Poll::Ready(Some(Ok(self.bytes.split_to(width))))
        }
    }
}
impl Drop for Body {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}
impl HttpTransport for Replay {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let body: Value = serde_json::from_slice(&request.body)
                .map_err(|_| Error::Invalid("replay request JSON".into()))?;
            let mut responses = self
                .responses
                .lock()
                .map_err(|_| Error::Storage("replay response lock".into()))?;
            let response = if responses.len() > 1 {
                responses.pop_front()
            } else {
                responses.front().cloned()
            }
            .ok_or_else(|| Error::Invalid("replay response absent".into()))?;
            if let Some(url) = &response.request_url {
                require(
                    request.url == *url,
                    "request route differs from real capture",
                )?;
            }
            for (pointer, expected) in &response.assertions {
                require(
                    body.pointer(pointer) == Some(expected),
                    "request differs from captured tool exchange",
                )?;
            }
            self.active.fetch_add(1, Ordering::SeqCst);
            let body: BoxProviderStream<'static, Result<Bytes>> = Box::pin(Body {
                bytes: response.bytes,
                active: self.active.clone(),
            });
            Ok(HttpResponse {
                status: response.status,
                retry_after: None,
                body,
            })
        })
    }
    fn delay(&self, _: Duration) -> BoxProviderFuture<'static, Result<()>> {
        Box::pin(futures::future::pending())
    }
    fn now(&self) -> Duration {
        Duration::ZERO
    }
}

fn read(root: &Path, capture: &FileCapture) -> Result<Capture> {
    // Fixture manifests cannot read outside their own checked-in capture directory.
    if Path::new(&capture.file)
        .components()
        .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(Error::Invalid(
            "capture path must stay under fixture root".into(),
        ));
    }
    let bytes = std::fs::read(root.join(&capture.file))
        .map_err(|cause| Error::Storage(cause.to_string()))?;
    if hex::encode(Sha256::digest(&bytes)) != capture.sha256 {
        return Err(Error::Invalid(format!(
            "captured inference body hash mismatch: {}",
            capture.file
        )));
    }
    Ok(Capture {
        status: capture.status,
        bytes: bytes.into(),
        assertions: capture.assertions.clone(),
        request_url: capture.request_url.clone(),
    })
}

fn prepare(
    provider: &acyclic_harness_providers::ResolvedModel,
    scenario: Scenario,
    context_input: &str,
) -> Result<(PreparedModelRequest, ModelDispatch)> {
    let tools = matches!(scenario, Scenario::Tools);
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            model: provider.model().clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(match scenario {
                    Scenario::Tools => "Call echo with value 1.".into(),
                    Scenario::ContextOverflow => context_input.into(),
                    _ => "Reply with exactly: hello".into(),
                }),
            }],
            tools: if tools {
                vec![acyclic_harness_core::tool::ToolDefinition {
                    name: "echo".into(),
                    revision: "1".into(),
                    description: "Echo a value".into(),
                    input_schema: json!({"type":"object", "properties":{"value":{"type":"integer"}}, "required":["value"], "additionalProperties":false}),
                    output_schema: json!({"type":"object"}),
                    projection_schema: json!({"type":"object"}),
                }]
            } else {
                Vec::new()
            },
            max_output_tokens: Some(128),
        },
        Limits::default(),
        None,
    )?;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness_core::OperationId::new(),
        step: 0,
        request_digest: request.request_digest(),
    };
    Ok((request, dispatch))
}

fn provider(
    manifest: &Manifest,
    captures: Vec<Capture>,
) -> Result<(acyclic_harness_providers::ResolvedModel, Arc<Replay>)> {
    let transport = Arc::new(Replay {
        responses: Mutex::new(captures.into()),
        active: Arc::new(AtomicUsize::new(0)),
    });
    let mut registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::from([(
            manifest.provider_id.clone(),
            "conformance-public-test-key".into(),
        )])),
        transport.clone(),
    );
    let mut config = manifest.endpoint.clone();
    require(
        config.adapter == manifest.adapter,
        "capture endpoint dialect mismatch",
    )?;
    config.retry.attempts = 0;
    registry.register(manifest.provider_id.clone(), config)?;
    let provider = registry.resolve_with_options(
        &format!("{}/{}", manifest.provider_id, manifest.model),
        manifest.options.clone(),
    )?;
    Ok((provider, transport))
}

struct Captured {
    manifest: Manifest,
    captures: BTreeMap<String, Capture>,
}

fn load(path: &Path) -> Result<Captured> {
    let bytes = std::fs::read(path).map_err(|cause| Error::Storage(cause.to_string()))?;
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|cause| Error::Invalid(cause.to_string()))?;
    require(
        !manifest.source.trim().is_empty() && !manifest.captured_at.is_empty(),
        "capture provenance absent",
    )?;
    let root = path
        .parent()
        .ok_or_else(|| Error::Invalid("manifest parent absent".into()))?;
    let captures: BTreeMap<_, _> = manifest
        .captures
        .iter()
        .map(|(name, capture)| Ok((name.clone(), read(root, capture)?)))
        .collect::<Result<_>>()?;
    // A fixture family is complete before a single assertion starts.
    for name in ["text", "tool", "result", "rate", "context", "auth"] {
        require(captures.contains_key(name), "missing required real capture")?;
    }
    require(
        captures
            .get("result")
            .is_some_and(|capture| !capture.assertions.is_empty()),
        "paired-result capture must assert its original request",
    )?;
    Ok(Captured { manifest, captures })
}

impl Captured {
    fn capture(&self, name: &str) -> Result<Capture> {
        self.captures
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Invalid(format!("missing real capture {name}")))
    }
}

impl Backend for Captured {
    fn session(&self, scenario: Scenario) -> Result<Session> {
        let names: &[&str] = match scenario {
            Scenario::Text | Scenario::Replay => &["text"],
            Scenario::Tools => &["tool", "result"],
            Scenario::RateLimit => &["rate"],
            Scenario::ContextOverflow => &["context"],
            Scenario::Authentication => &["auth"],
        };
        let captures = names
            .iter()
            .map(|name| self.capture(name))
            .collect::<Result<_>>()?;
        let (provider, transport) = provider(&self.manifest, captures)?;
        let (request, dispatch) = prepare(&provider, scenario, &self.manifest.context_input)?;
        let provider = counted(provider);
        let requests = provider.ledger();
        Ok(Session {
            provider: Arc::new(provider),
            requests,
            request,
            dispatch,
            released: Arc::new(move || transport.active.load(Ordering::SeqCst) == 0),
        })
    }
    fn usage(&self) -> &ExpectedUsage {
        &self.manifest.usage
    }
    fn tool_usage(&self) -> &[ExpectedUsage; 2] {
        &self.manifest.tool_usage
    }
}

/// Qualifies one complete fixture family with the common verifier.
pub async fn qualify(path: &Path) -> Result<Vec<&'static str>> {
    verify(&load(path)?).await
}

/// Fresh adapter for the published suite and benchmark over hash-checked captures.
pub fn captured(path: &Path) -> Result<impl Backend + use<>> {
    load(path)
}

struct Local {
    captured: Captured,
    base_url: String,
    rate_url: String,
}

impl Backend for Local {
    fn session(&self, scenario: Scenario) -> Result<Session> {
        if matches!(scenario, Scenario::Replay) {
            return self.captured.session(scenario);
        }
        let active = Arc::new(AtomicUsize::new(0));
        let transport = Arc::new(TrackedTransport {
            inner: acyclic_harness_providers::ReqwestTransport::new()?,
            active: active.clone(),
        });
        let keys = if matches!(scenario, Scenario::Authentication) {
            BTreeMap::new()
        } else {
            BTreeMap::from([("local".into(), "conformance-public-test-key".into())])
        };
        let mut registry = Registry::with_transport(Credentials::explicit(keys), transport);
        let url = if matches!(scenario, Scenario::RateLimit) {
            &self.rate_url
        } else {
            &self.base_url
        };
        let mut spec = acyclic_harness_providers::endpoints::local(url)?;
        spec.retry.attempts = 0;
        let manifest = &self.captured.manifest;
        spec.models.insert(
            manifest.model.clone(),
            ModelSpec {
                max_context_tokens: None,
                capacity: ModelContextCapacity {
                    context_tokens: 2048,
                    output_tokens: Some(128),
                },
                features: ModelFeatures {
                    tools: true,
                    images: false,
                },
            },
        );
        registry.register("local".into(), spec)?;
        let provider = registry.resolve_with_options(
            &format!("local/{}", manifest.model),
            manifest.options.clone(),
        )?;
        let (request, dispatch) = prepare(&provider, scenario, &manifest.context_input)?;
        let provider = counted(provider);
        let requests = provider.ledger();
        Ok(Session {
            provider: Arc::new(provider),
            requests,
            request,
            dispatch,
            released: Arc::new(move || active.load(Ordering::SeqCst) == 0),
        })
    }
    fn usage(&self) -> &ExpectedUsage {
        self.captured.usage()
    }
    fn tool_usage(&self) -> &[ExpectedUsage; 2] {
        self.captured.tool_usage()
    }
}

/// Runs the complete common verifier against a real local server, an actual rate
/// limiter and that server's hash-checked bytes for deterministic replay.
pub async fn local_live(path: &Path, base_url: &str, rate_url: &str) -> Result<Vec<&'static str>> {
    verify(&Local {
        captured: load(path)?,
        base_url: base_url.into(),
        rate_url: rate_url.into(),
    })
    .await
}
