use crate::{Error, HttpRequest, HttpResponse, HttpTransport, Result};
use acyclic_fs::{NativeFutureBundle, NativeMaterializationWork};
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

type Clock = dyn Fn() -> Duration + Send + Sync;
type WallClock = dyn Fn() -> Result<u64> + Send + Sync;
type Delay = dyn Fn(Duration) -> BoxProviderFuture<'static, Result<()>> + Send + Sync;
type NativeDelay = dyn Fn(Duration, &NativeMaterializationWork) -> Result<NativeFutureBundle<'static, (), Error>>
    + Send
    + Sync;

type RecordedExchange = dyn FnMut(&HttpRequest) -> Result<HttpResponse> + Send;

type OwnedExchange =
    dyn Fn(HttpRequest) -> BoxProviderFuture<'static, Result<HttpResponse>> + Send + Sync;

enum Exchange {
    Recorded(Mutex<Box<RecordedExchange>>),
    Owned(Box<OwnedExchange>),
    Forwarded {
        transport: Arc<dyn HttpTransport>,
        observe: Box<dyn Fn() + Send + Sync>,
    },
}

type NativeExchange = dyn for<'a> Fn(
        HttpRequest,
        &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse, Error>>
    + Send
    + Sync;

/// An ordered script of HTTP exchanges with a transcript of requests.
pub struct ScriptedHttp {
    exchange: Exchange,
    requests: Mutex<Vec<HttpRequest>>,
    native_exchange: Option<Box<NativeExchange>>,
    native_supported: bool,
    delay: Box<Delay>,
    clock: Box<Clock>,
    wall_clock: Box<WallClock>,
    native_delay: Option<Box<NativeDelay>>,
}

impl ScriptedHttp {
    /// Scripts one outcome per request. Error outcomes fail the exchange before its body.
    pub fn new(script: impl IntoIterator<Item = Result<HttpResponse>>) -> Self {
        let mut script: VecDeque<_> = script.into_iter().collect();
        Self::respond(move |_| {
            script
                .pop_front()
                .unwrap_or_else(|| Err(Error::Invalid("unexpected HTTP request".into())))
        })
    }

    /// Scripts a response based on the request, optionally using captured state.
    pub fn respond(
        exchange: impl FnMut(&HttpRequest) -> Result<HttpResponse> + Send + 'static,
    ) -> Self {
        Self::from_exchange(Exchange::Recorded(Mutex::new(Box::new(exchange))))
    }

    /// Gives a scenario custody of the request, including any recording it needs.
    /// The callback can hold the response head pending on an explicit event.
    pub fn respond_owned(
        exchange: impl Fn(HttpRequest) -> BoxProviderFuture<'static, Result<HttpResponse>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::from_exchange(Exchange::Owned(Box::new(exchange)))
    }

    /// Observes dispatch and forwards the original exchange, including its native ledger.
    pub fn forward(
        transport: Arc<dyn HttpTransport>,
        observe: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self::from_exchange(Exchange::Forwarded {
            transport,
            observe: Box::new(observe),
        })
    }

    fn from_exchange(exchange: Exchange) -> Self {
        Self {
            exchange,
            requests: Mutex::new(Vec::new()),
            native_exchange: None,
            native_supported: false,
            delay: Box::new(super::delay),
            clock: Box::new(super::now),
            wall_clock: Box::new(|| {
                let elapsed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| Error::Invalid("HTTP wall clock unavailable".into()))?;
                u64::try_from(elapsed.as_millis())
                    .map_err(|_| Error::Invalid("HTTP clock overflow".into()))
            }),
            native_delay: None,
        }
    }

    /// Supplies a ledger-aware exchange. Its script owns request custody and any
    /// recording it needs, so allocation and cancellation tests control both.
    pub fn with_native(
        mut self,
        supported: bool,
        exchange: impl for<'a> Fn(
            HttpRequest,
            &'a NativeMaterializationWork,
        ) -> Result<NativeFutureBundle<'a, HttpResponse, Error>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.native_exchange = Some(Box::new(exchange));
        self.native_supported = supported;
        self
    }

    /// Scripts the monotonic clock and the wall clock independently.
    pub fn with_clock(
        mut self,
        now: impl Fn() -> Duration + Send + Sync + 'static,
        unix_ms: impl Fn() -> Result<u64> + Send + Sync + 'static,
    ) -> Self {
        self.clock = Box::new(now);
        self.wall_clock = Box::new(unix_ms);
        self
    }

    /// Scripts monotonic time while keeping the default wall clock.
    pub fn with_monotonic_clock(
        mut self,
        now: impl Fn() -> Duration + Send + Sync + 'static,
    ) -> Self {
        self.clock = Box::new(now);
        self
    }

    /// Scripts ordinary timer waits.
    pub fn with_delay(
        mut self,
        delay: impl Fn(Duration) -> BoxProviderFuture<'static, Result<()>> + Send + Sync + 'static,
    ) -> Self {
        self.delay = Box::new(delay);
        self
    }

    /// Scripts timer admission and execution on the native ledger.
    pub fn with_native_delay(
        mut self,
        delay: impl Fn(
            Duration,
            &NativeMaterializationWork,
        ) -> Result<NativeFutureBundle<'static, (), Error>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.native_delay = Some(Box::new(delay));
        self
    }

    /// Builds a response with a single body chunk.
    pub fn reply(status: u16, body: impl Into<Bytes>) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status,
            body: Box::pin(futures::stream::iter([Ok(body.into())])),
            request_id: None,
            retry_after: None,
        })
    }

    /// Requests in script order, including attempts after the script runs out.
    ///
    /// Returns an error if a previous exchange poisoned the transcript.
    pub fn requests(&self) -> Result<MutexGuard<'_, Vec<HttpRequest>>> {
        self.requests
            .lock()
            .map_err(|_| Error::Invalid("HTTP fixture transcript poisoned".into()))
    }
}

impl HttpTransport for ScriptedHttp {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        let exchange = match &self.exchange {
            Exchange::Recorded(exchange) => exchange,
            Exchange::Owned(exchange) => return exchange(request),
            Exchange::Forwarded { transport, observe } => {
                observe();
                return transport.send(request);
            }
        };
        Box::pin(async move {
            // Hold the transcript lock through the exchange to preserve script order.
            let mut requests = self.requests()?;
            let mut exchange = exchange
                .lock()
                .map_err(|_| Error::Invalid("HTTP fixture script poisoned".into()))?;
            let response = exchange(&request);
            requests.push(request);
            response
        })
    }

    fn delay(&self, wait: Duration) -> BoxProviderFuture<'static, Result<()>> {
        (self.delay)(wait)
    }

    fn now(&self) -> Duration {
        (self.clock)()
    }

    fn unix_ms(&self) -> Result<u64> {
        (self.wall_clock)()
    }

    fn supports_native(&self, work: &NativeMaterializationWork) -> Result<bool> {
        if let Exchange::Forwarded { transport, .. } = &self.exchange {
            return transport.supports_native(work);
        }
        work.remaining()?;
        Ok(self.native_supported)
    }

    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse, Error>> {
        if let Exchange::Forwarded { transport, observe } = &self.exchange {
            observe();
            return transport.send_with_native_work(request, work);
        }
        work.remaining()?;
        let exchange = self.native_exchange.as_ref().ok_or(Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native producer",
        })?;
        exchange(request, work)
    }

    fn delay_with_native_work(
        &self,
        wait: Duration,
        work: &NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'static, (), Error>> {
        if let Some(delay) = &self.native_delay {
            return delay(wait, work);
        }
        if self.native_supported {
            return super::native_delay(wait, work);
        }
        work.remaining()?;
        Err(Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native timer",
        })
    }
}
