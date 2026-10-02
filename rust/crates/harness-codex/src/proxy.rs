//! The metered Responses proxy Codex calls instead of `OpenAI` (task A2).
//!
//! Contract, fixed by `tests/proxy.rs` and the 0.155.1 fixtures:
//! - Serves `POST /v1/responses` on `127.0.0.1:0`; other paths get a 404 JSON error.
//! - Only answers callers presenting the per-turn [`ResponsesProxy::client_key`].
//! - Forwards with the real key (Codex only holds a dummy) and merges `extra_body`.
//! - Streams SSE through frame by frame and meters `response.completed` usage.
//! - Once the meter or the step cap says stop, answers `429` with
//!   `{"error":{"type":"insufficient_quota"}}`. Codex 0.155.1 retries a 402
//!   six times but ends the turn on this after one request.
//! - Upstream 4xx/5xx pass through unchanged and cost nothing.

use crate::{
    executor::Upstream,
    meter::{MeterVerdict, ResponsesUsage, UsageMeter},
};
use acyclic_harness::{Error, Result};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::post,
};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::{sync::mpsc, task::JoinHandle};

/// Codex request bodies carry the whole conversation; allow long threads.
const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Request headers worth passing upstream. Everything else, notably Codex's
/// dummy `authorization`, is dropped.
const FORWARDED_HEADERS: [&str; 4] = ["accept", "session-id", "thread-id", "x-client-request-id"];

/// Why the proxy stopped forwarding, reported in the executor's error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyStop {
    /// The meter refused a call.
    Budget(String),
    /// The turn's model-step cap was reached.
    StepLimit(u32),
}

/// One proxied model call, as the executor journals it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyCall {
    /// A call was admitted and forwarded as step `step` (1-based).
    Started {
        /// The step number.
        step: u32,
        /// BLAKE3 of the request body as forwarded.
        request_digest: [u8; 32],
    },
    /// The call's response completed with this usage.
    Completed {
        /// The step number.
        step: u32,
        /// Metered usage.
        usage: ResponsesUsage,
    },
}

struct Shared {
    upstream: Upstream,
    /// The per-turn key Codex must present; anything else on the machine
    /// could otherwise spend the real upstream key through this port.
    client_key: String,
    meter: Arc<dyn UsageMeter>,
    max_steps: u32,
    client: reqwest::Client,
    ledger: Mutex<Ledger>,
    calls: Option<mpsc::UnboundedSender<ProxyCall>>,
}

#[derive(Default)]
struct Ledger {
    steps: u32,
    stopped: Option<ProxyStop>,
}

impl Shared {
    fn ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn stop(&self, stop: ProxyStop) {
        let mut ledger = self.ledger();
        if ledger.stopped.is_none() {
            ledger.stopped = Some(stop);
        }
    }

    fn report(&self, call: ProxyCall) {
        if let Some(calls) = &self.calls {
            let _ = calls.send(call);
        }
    }
}

/// A running proxy for one turn. Dropping it stops the server.
pub struct ResponsesProxy {
    base_url: String,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for ResponsesProxy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponsesProxy")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl ResponsesProxy {
    /// Starts the proxy. `max_steps` caps forwarded model calls for the turn.
    ///
    /// # Errors
    /// When the listener cannot bind or the HTTP client cannot be built.
    pub async fn start(
        upstream: Upstream,
        meter: Arc<dyn UsageMeter>,
        max_steps: u32,
    ) -> Result<Self> {
        Self::launch(upstream, meter, max_steps, None).await
    }

    /// Starts the proxy and reports every call on the returned channel.
    ///
    /// # Errors
    /// As [`Self::start`].
    pub async fn start_reporting(
        upstream: Upstream,
        meter: Arc<dyn UsageMeter>,
        max_steps: u32,
    ) -> Result<(Self, mpsc::UnboundedReceiver<ProxyCall>)> {
        let (sender, receiver) = mpsc::unbounded_channel();
        Ok((
            Self::launch(upstream, meter, max_steps, Some(sender)).await?,
            receiver,
        ))
    }

    async fn launch(
        upstream: Upstream,
        meter: Arc<dyn UsageMeter>,
        max_steps: u32,
        calls: Option<mpsc::UnboundedSender<ProxyCall>>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|error| Error::Unsupported(format!("codex proxy client: {error}")))?;
        let shared = Arc::new(Shared {
            upstream,
            client_key: uuid::Uuid::new_v4().simple().to_string(),
            meter,
            max_steps,
            client,
            ledger: Mutex::new(Ledger::default()),
            calls,
        });
        let app = Router::new()
            .route("/v1/responses", post(responses))
            .fallback(not_served)
            .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
            .with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| Error::Unsupported(format!("codex proxy bind: {error}")))?;
        let address = listener
            .local_addr()
            .map_err(|error| Error::Unsupported(format!("codex proxy address: {error}")))?;
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            base_url: format!("http://{address}/v1"),
            shared,
            task,
        })
    }

    /// The base URL Codex is configured with, ending in `/v1`.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The bearer key Codex must send (`env_key` in its provider config).
    #[must_use]
    pub fn client_key(&self) -> &str {
        &self.shared.client_key
    }

    /// Model calls forwarded and answered so far: the turn's step count.
    #[must_use]
    pub fn steps(&self) -> u32 {
        self.shared.ledger().steps
    }

    /// Why forwarding stopped, if it has.
    #[must_use]
    pub fn stopped(&self) -> Option<ProxyStop> {
        self.shared.ledger().stopped.clone()
    }
}

impl Drop for ResponsesProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn refuse(reason: &str) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        axum::Json(json!({"error": {
            "message": reason,
            "type": "insufficient_quota",
            "code": "insufficient_quota",
        }})),
    )
        .into_response()
}

fn stop_reason(stop: &ProxyStop) -> String {
    match stop {
        ProxyStop::Budget(reason) => format!("budget exhausted: {reason}"),
        ProxyStop::StepLimit(limit) => format!("step limit of {limit} model calls reached"),
    }
}

async fn not_served(method: Method, uri: Uri) -> Response {
    tracing::warn!(%method, path = uri.path(), "codex called a path the proxy does not serve");
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({"error": {
            "message": format!(
                "the acyclic codex proxy only serves POST /v1/responses, not {method} {}",
                uri.path()
            ),
            "type": "not_found",
        }})),
    )
        .into_response()
}

/// Admits the call as the next step, or says why not.
fn admit(shared: &Shared, model: &str) -> std::result::Result<u32, ProxyStop> {
    let mut ledger = shared.ledger();
    if let Some(stop) = &ledger.stopped {
        return Err(stop.clone());
    }
    if ledger.steps >= shared.max_steps {
        let stop = ProxyStop::StepLimit(shared.max_steps);
        ledger.stopped = Some(stop.clone());
        return Err(stop);
    }
    if let MeterVerdict::Stop { reason } = shared.meter.admit(model) {
        let stop = ProxyStop::Budget(reason);
        ledger.stopped = Some(stop.clone());
        return Err(stop);
    }
    ledger.steps += 1;
    Ok(ledger.steps)
}

fn release(shared: &Shared) {
    let mut ledger = shared.ledger();
    ledger.steps = ledger.steps.saturating_sub(1);
}

async fn responses(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Bytes) -> Response {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if presented != Some(shared.client_key.as_str()) {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": {"message": "missing or wrong proxy key", "type": "invalid_api_key"}})),
        )
            .into_response();
    }
    let mut request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": {"message": format!("request body is not JSON: {error}"), "type": "invalid_request_error"}})),
            )
                .into_response();
        }
    };
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let step = match admit(&shared, &model) {
        Ok(step) => step,
        Err(stop) => return refuse(&stop_reason(&stop)),
    };
    merge(&mut request, &shared.upstream.extra_body);
    let forwarded = request.to_string();
    shared.report(ProxyCall::Started {
        step,
        request_digest: *blake3::hash(forwarded.as_bytes()).as_bytes(),
    });

    let mut outgoing = shared
        .client
        .post(format!(
            "{}/responses",
            shared.upstream.base_url.trim_end_matches('/')
        ))
        .bearer_auth(&shared.upstream.api_key)
        .header(header::CONTENT_TYPE, "application/json")
        .body(forwarded);
    for name in FORWARDED_HEADERS {
        if let Some(value) = headers.get(name) {
            outgoing = outgoing.header(name, value);
        }
    }
    let upstream = match outgoing.send().await {
        Ok(response) => response,
        Err(error) => {
            release(&shared);
            return (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({"error": {"message": format!("upstream unreachable: {error}"), "type": "upstream_error"}})),
            )
                .into_response();
        }
    };
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_owned();
    if !status.is_success() {
        release(&shared);
        let bytes = upstream.bytes().await.unwrap_or_default();
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::from(bytes))
            .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
    }

    let mut scanner = SseScanner::default();
    let metered = shared.clone();
    let stream = upstream.bytes_stream().map(move |chunk| {
        if let Ok(bytes) = &chunk {
            for usage in scanner.push(bytes) {
                metered.report(ProxyCall::Completed { step, usage });
                if let MeterVerdict::Stop { reason } = metered.meter.record(&model, &usage) {
                    metered.stop(ProxyStop::Budget(reason));
                }
            }
        }
        chunk
    });
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

/// Deep-merges `extra` into `target`: objects merge, anything else replaces.
fn merge(target: &mut Value, extra: &Value) {
    match (target, extra) {
        (Value::Object(target), Value::Object(extra)) => {
            for (key, value) in extra {
                match target.get_mut(key) {
                    Some(existing) if existing.is_object() && value.is_object() => {
                        merge(existing, value);
                    }
                    _ => {
                        target.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (target, extra) if !extra.is_null() => *target = extra.clone(),
        _ => {}
    }
}

/// Finds `response.completed` usage in an SSE byte stream split anywhere.
#[derive(Default)]
struct SseScanner {
    pending: Vec<u8>,
}

impl SseScanner {
    fn push(&mut self, bytes: &[u8]) -> Vec<ResponsesUsage> {
        self.pending.extend_from_slice(bytes);
        let mut found = Vec::new();
        while let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=end).collect();
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let Ok(event) = serde_json::from_slice::<Value>(data.trim_ascii()) else {
                continue;
            };
            if event.get("type").and_then(Value::as_str) == Some("response.completed")
                && let Some(usage) = event.pointer("/response/usage")
            {
                found.push(usage_of(usage));
            }
        }
        found
    }
}

fn usage_of(usage: &Value) -> ResponsesUsage {
    let count = |pointer: &str| usage.pointer(pointer).and_then(Value::as_u64).unwrap_or(0);
    ResponsesUsage {
        input_tokens: count("/input_tokens"),
        cached_input_tokens: count("/input_tokens_details/cached_tokens"),
        output_tokens: count("/output_tokens"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extra_body_merges_objects_and_replaces_scalars() {
        let mut body = json!({"reasoning": {"summary": "auto"}, "store": false, "model": "m"});
        merge(
            &mut body,
            &json!({"reasoning": {"effort": "low"}, "store": true, "tier": "flex"}),
        );
        assert_eq!(
            body,
            json!({"reasoning": {"summary": "auto", "effort": "low"}, "store": true, "model": "m", "tier": "flex"})
        );
        merge(&mut body, &Value::Null);
        assert_eq!(body["tier"], "flex");
    }

    #[test]
    fn usage_is_found_across_arbitrary_chunk_boundaries() {
        let stream = concat!(
            "event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":",
            "{\"input_tokens\":100,\"input_tokens_details\":{\"cached_tokens\":40},\"output_tokens\":7}}}\n\n"
        )
        .as_bytes();
        for split in 1..stream.len() {
            let mut scanner = SseScanner::default();
            let (head, tail) = stream.split_at(split);
            let mut found = scanner.push(head);
            found.extend(scanner.push(tail));
            assert_eq!(
                found,
                vec![ResponsesUsage {
                    input_tokens: 100,
                    cached_input_tokens: 40,
                    output_tokens: 7
                }],
                "split at {split}"
            );
        }
    }
}
