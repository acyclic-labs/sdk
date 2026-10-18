use super::provider_case::{Protocol, ProviderCase};
use acyclic_harness::{Error, NativeFutureBundle, Result, context::ModelContextCapacity};
use acyclic_harness_providers::{
    Adapter, Authentication, Credentials, HttpRequest, HttpResponse, HttpTransport, ModelFeatures,
    ModelSpec, ProviderSpec, Registry, ResolvedModel, TokenCountField, TokenCounterSpec,
};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use futures::stream;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
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
    replies: Vec<(u16, Bytes)>,
    unreachable: usize,
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
            replies: case
                .responses
                .iter()
                .enumerate()
                .map(|(index, parts)| (case.status(index), Bytes::from(parts.concat())))
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
    fn reply(&self, request: HttpRequest, native: bool) -> Result<(u16, Bytes)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.refused < self.unreachable {
            state.refused += 1;
            // As the platform transport reports a refused connection on each path.
            return Err(if native {
                Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unavailable,
                    reason: "model provider connection failed",
                }
            } else {
                Error::ModelUnavailable {
                    failure: acyclic_harness::ModelTransportFailure::Connect,
                    excerpt: "Connection refused (os error 111)".into(),
                }
            });
        }
        if request.url.ends_with("/count") {
            return Ok((200, Bytes::from_static(br#"{"input_tokens":32}"#)));
        }
        let bytes = self
            .replies
            .get(state.next)
            .cloned()
            .ok_or_else(|| Error::Invalid("recorded HTTP exhausted".into()))?;
        state.next += 1;
        state.sent.push(Sent {
            url: request.url,
            body: request.body,
        });
        Ok(bytes)
    }
    fn chunks(
        &self,
        bytes: Bytes,
    ) -> impl futures::Stream<Item = Result<Bytes>> + Send + Unpin + 'static + use<> {
        let width = self.width;
        let length = bytes.len();
        stream::iter(
            (0..length)
                .step_by(width)
                .map(move |start| Ok(bytes.slice(start..(start + width).min(length)))),
        )
    }
}
impl HttpTransport for Recorded {
    fn delay(&self, _: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        Box::pin(async {})
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let (status, bytes) = self.reply(request, false)?;
            Ok(HttpResponse {
                retry_after: None,
                status,
                body: Box::pin(self.chunks(bytes)),
            })
        })
    }
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse>> {
        NativeFutureBundle::new(work, || async move {
            let (status, bytes) = self.reply(request, true)?;
            HttpResponse::with_native_body(status, self.chunks(bytes), work)
        })
    }
}
pub(super) fn resolve(case: &ProviderCase, transport: Arc<Recorded>) -> Result<ResolvedModel> {
    let adapter = match case.protocol {
        Protocol::Anthropic => Adapter::Anthropic,
        Protocol::Responses => Adapter::Responses,
        Protocol::Chat => Adapter::Chat,
        Protocol::Gemini => Adapter::Gemini,
    };
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport);
    registry.register(
        "recorded".into(),
        ProviderSpec {
            adapter,
            authentication: Authentication::Optional,
            base_url: "https://recorded.invalid/v1".into(),
            headers: BTreeMap::new(),
            models: BTreeMap::from([(
                "model".into(),
                ModelSpec {
                    capacity: ModelContextCapacity {
                        context_tokens: 8192,
                        output_tokens: 1024,
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
            max_event_bytes: 4096,
            retry: acyclic_harness_providers::RetrySpec::DEFAULT,
        },
    )?;
    registry.resolve("recorded/model")
}
