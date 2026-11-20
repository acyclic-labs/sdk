use super::provider_case::{Protocol, ProviderCase};
use acyclic_harness_core::{Error, Result, context::ModelContextCapacity};
use acyclic_harness_provider_core::fixtures::ScriptedHttp;
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, HttpRequest, HttpResponse, ModelFeatures, ModelSpec,
    ProviderSpec, Registry, ResolvedModel, TokenCountField, TokenCounterSpec,
};
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub(super) struct Sent {
    pub url: String,
    pub body: Bytes,
}
struct State {
    next: usize,
    sent: Vec<Sent>,
    /// Exchanges refused for want of a connection.
    refused: usize,
}
pub(super) struct Recorded {
    // Test recording only: one lock, never nested or held across a poll/await.
    state: Mutex<State>,
    width: usize,
    replies: Vec<(u16, Bytes, bool)>,
    unreachable: usize,
    /// The virtual clock advances when a model backoff or selected request deadline fires.
    clock: Arc<AtomicU64>,
}
impl Recorded {
    pub fn new(case: &ProviderCase) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                next: 0,
                sent: Vec::new(),
                refused: 0,
            }),
            width: case.chunk_bytes.get(),
            unreachable: case.unreachable.into(),
            clock: Arc::new(AtomicU64::new(0)),
            replies: case
                .responses
                .iter()
                .enumerate()
                .map(|(index, parts)| {
                    (
                        case.status(index),
                        Bytes::from(parts.concat()),
                        case.stalls.contains(&index),
                    )
                })
                .collect(),
        })
    }
    pub fn take_requests(&self) -> (Vec<Sent>, usize) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        (
            std::mem::take(&mut state.sent),
            self.replies.len() - state.next,
        )
    }
    /// Exchanges refused so far.
    pub fn refused(&self) -> usize {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).refused
    }
    pub fn restart(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.next != self.replies.len() || !state.sent.is_empty() {
            return Err(Error::Invalid("restarted incomplete HTTP recording".into()));
        }
        state.next = 0;
        Ok(())
    }
    fn reply(&self, request: HttpRequest, native: bool) -> Result<(u16, Bytes, bool)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.refused < self.unreachable {
            state.refused += 1;
            // Both platform paths retain the semantic Connect class. Native
            // failures omit the excerpt until diagnostic storage is admitted.
            return Err(Error::ModelUnavailable {
                failure: acyclic_harness_core::ModelTransportFailure::Connect,
                excerpt: if native {
                    String::new()
                } else {
                    "Connection refused".into()
                },
            });
        }
        if request.url.ends_with("/count") {
            return Ok((200, Bytes::from_static(br#"{"input_tokens":32}"#), false));
        }
        state.sent.push(Sent {
            url: request.url,
            body: request.body,
        });
        let bytes = self
            .replies
            .get(state.next)
            .cloned()
            .ok_or_else(|| Error::Invalid("recorded HTTP exhausted".into()))?;
        state.next += 1;
        Ok(bytes)
    }
    /// The reply in `width`-byte chunks; a stalled reply then stays silent forever.
    fn chunks(
        &self,
        bytes: Bytes,
        stall: bool,
    ) -> impl futures::Stream<Item = Result<Bytes>> + Send + Unpin + 'static + use<> {
        let width = self.width;
        let length = bytes.len();
        let silence = if stall {
            stream::pending().left_stream()
        } else {
            stream::empty().right_stream()
        };
        stream::iter(
            (0..length)
                .step_by(width)
                .map(move |start| Ok(bytes.slice(start..(start + width).min(length)))),
        )
        .chain(silence)
    }
}
impl Recorded {
    pub(super) fn transport(self: Arc<Self>) -> Arc<ScriptedHttp> {
        let ordinary = self.clone();
        let monotonic = self.clock.clone();
        let delay = self.clock.clone();
        let native_delay = self.clock.clone();
        Arc::new(
            ScriptedHttp::respond_owned(move |request| {
                let ordinary = ordinary.clone();
                Box::pin(async move {
                    let (status, bytes, stall) = ordinary.reply(request, false)?;
                    Ok(HttpResponse {
                        request_id: None,
                        retry_after: None,
                        status,
                        body: Box::pin(ordinary.chunks(bytes, stall)),
                    })
                })
            })
            .with_monotonic_clock(move || {
                Duration::from_nanos(monotonic.load(Ordering::SeqCst).saturating_mul(1_000_000))
            })
            .with_delay(move |wait| {
                let clock = delay.clone();
                Box::pin(async move {
                    elapse(clock, wait).await;
                    Ok(())
                })
            })
            .with_native_delay(move |wait, work| {
                let clock = native_delay.clone();
                acyclic_fs::NativeFutureBundle::new(work, move || async move {
                    elapse(clock, wait).await;
                    Ok(())
                })
            })
            .with_native(true, move |request, work| {
                let native = self.clone();
                acyclic_fs::NativeFutureBundle::new(work, move || async move {
                    let (status, bytes, stall) = native.reply(request, true)?;
                    HttpResponse::with_native_body(status, native.chunks(bytes, stall), work)
                })
            }),
        )
    }
}
/// Advances the caller's logical clock by the chosen wait.
async fn elapse(clock: Arc<AtomicU64>, wait: Duration) {
    let millis = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
    clock.fetch_add(millis, Ordering::SeqCst);
}
pub(super) fn resolve(case: &ProviderCase, transport: Arc<Recorded>) -> Result<ResolvedModel> {
    let adapter = match case.protocol {
        Protocol::Anthropic => Adapter::Anthropic,
        Protocol::Responses => Adapter::Responses,
        Protocol::Chat => Adapter::Chat,
        Protocol::Gemini => Adapter::Gemini,
    };
    let mut registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::new()),
        transport.transport(),
    );
    registry.register(
        "recorded".into(),
        ProviderSpec {
            adapter,
            authentication: Authentication::Optional,
            base_url: "https://recorded.invalid/v1".into(),
            allow_http_host: None,
            headers: BTreeMap::new(),
            models: BTreeMap::from([(
                "model".into(),
                ModelSpec {
                    tool_profile: None,
                    thinking_levels: false,
                    max_context_tokens: None,
                    capacity: ModelContextCapacity {
                        context_tokens: 8192,
                        output_tokens: Some(1024),
                        output_shares_context: false,
                    },
                    features: ModelFeatures {
                        tools: true,
                        images: false,
                    },
                },
            )]),
            token_counter: Some(TokenCounterSpec {
                path: "count".into(),
                adapter,
                response_field: TokenCountField::InputTokens,
                framing_tokens: 0,
            }),
            max_event_bytes: Some(4096),
            retry: acyclic_harness_providers::RetrySpec::DEFAULT,
            timeouts: acyclic_harness_providers::TimeoutSpec {
                total_ms: Some(100),
            },
            prompt_cache: acyclic_harness_providers::PromptCache::Native,
        },
    )?;
    registry.resolve("recorded/model")
}
