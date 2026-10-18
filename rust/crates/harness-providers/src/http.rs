use acyclic_harness::Result;
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, ProviderPlatform};
use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use futures::StreamExt as _;
use std::collections::BTreeMap;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod owned_bytes;

/// One streaming JSON request. Headers may contain credentials; deliberately not Debug.
pub struct HttpRequest {
    /// Fully resolved endpoint.
    pub url: String,
    /// Protocol and authentication headers.
    pub headers: BTreeMap<String, String>,
    /// Exact encoded JSON.
    pub body: Bytes,
}

/// Response whose body remains lazy until polled.
pub struct HttpResponse {
    /// HTTP status.
    pub status: u16,
    /// Incremental body. Dropping it cancels I/O.
    pub body: BoxProviderStream<'static, Result<Bytes>>,
    /// The response's `Retry-After` delay, when it gave one in seconds.
    pub retry_after: Option<std::time::Duration>,
}

impl HttpResponse {
    /// Records the response's `Retry-After` delay.
    #[must_use]
    pub const fn with_retry_after(mut self, retry_after: Option<std::time::Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl HttpResponse {
    /// Admits the concrete body stream before boxing it on the issued ledger.
    /// Charges received bytes once; downstream codecs charge their own copies/decoding.
    pub fn with_native_body<S>(
        status: u16,
        stream: S,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self>
    where
        S: futures::Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        Ok(Self {
            status,
            body: native::body(stream, Some(work))?,
            retry_after: None,
        })
    }
}

/// Platform HTTP boundary, also used by scripted fixtures.
pub trait HttpTransport: ProviderPlatform {
    /// Sends exactly one request without retries.
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>>;
    /// Waits `duration` on the platform timer: a redelivery backoff, or a deadline's wake-up.
    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, ()>;
    /// Monotonic time on the platform clock that `delay` waits on, from a fixed origin.
    /// Deadlines are measured on it; timers only wake their measurement.
    fn now(&self) -> std::time::Duration;
    /// `delay` admitted on the original invocation ledger before it exists.
    #[cfg(not(target_arch = "wasm32"))]
    fn delay_with_native_work(
        &self,
        _: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'static, ()>> {
        work.remaining()?;
        Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native timer",
        })
    }
    /// Whether this transport funds its native callback frames before allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }
    /// Issues one HTTP exchange on the original invocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self,
        _: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        work.remaining()?;
        Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native producer",
        })
    }
}

#[cfg(target_arch = "wasm32")]
mod browser;
#[cfg(target_arch = "wasm32")]
pub use browser::BrowserTransport;

pub(crate) fn platform() -> Result<std::sync::Arc<dyn HttpTransport>> {
    #[cfg(not(target_arch = "wasm32"))]
    let transport = ReqwestTransport::new()?;
    #[cfg(target_arch = "wasm32")]
    let transport = BrowserTransport::global()?;
    Ok(std::sync::Arc::new(transport))
}

/// Tokio's monotonic clock (which a paused test runtime drives) from a process-wide origin.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_now() -> std::time::Duration {
    static ORIGIN: std::sync::OnceLock<tokio::time::Instant> = std::sync::OnceLock::new();
    let origin = *ORIGIN.get_or_init(tokio::time::Instant::now);
    tokio::time::Instant::now().saturating_duration_since(origin)
}

/// Pooled native HTTP with redirects disabled.
#[cfg(not(target_arch = "wasm32"))]
pub struct ReqwestTransport(reqwest::Client);

#[cfg(not(target_arch = "wasm32"))]
impl ReqwestTransport {
    async fn exchange(
        &self,
        request: HttpRequest,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<HttpResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let mut builder = self
            .0
            .post(request.url)
            .header("content-type", "application/json");
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .body(request.body)
            .send()
            .await
            .map_err(|error| reqwest_failure(native, &error))?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(crate::retry::retry_after);
        let body = response
            .bytes_stream()
            .map(move |chunk| chunk.map_err(|error| reqwest_failure(native, &error)));
        #[cfg(not(target_arch = "wasm32"))]
        let body = native::body(body, work)?;
        #[cfg(target_arch = "wasm32")]
        let body = Box::pin(body);
        Ok(HttpResponse {
            status,
            body,
            retry_after,
        })
    }
    /// Creates the platform transport. Credentials are never forwarded across redirects.
    pub fn new() -> Result<Self> {
        let builder = reqwest::Client::builder();
        #[cfg(not(target_arch = "wasm32"))]
        let builder = builder
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
        Ok(Self(builder.build().map_err(|_| {
            super::invalid("HTTP client initialization failed")
        })?))
    }
}

/// Classifies a send or body error by its source chain, keeping only the root cause's text: the
/// outer layers name the URL, which can carry credentials.
#[cfg(not(target_arch = "wasm32"))]
fn reqwest_failure(native: bool, error: &reqwest::Error) -> acyclic_harness::Error {
    use acyclic_harness::ModelTransportFailure;
    let mut root: &(dyn std::error::Error + 'static) = error;
    let mut tls = false;
    loop {
        tls |= root.is::<rustls::Error>();
        // An `io::Error`'s `source` skips the error it wraps, and rustls reports through nested
        // ones, so descend into the wrapped error first.
        let next = match root
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            Some(wrapped) => Some(wrapped as &(dyn std::error::Error + 'static)),
            None => root.source(),
        };
        match next {
            Some(next) => root = next,
            None => break,
        }
    }
    let failure = if tls {
        ModelTransportFailure::Tls
    } else if error.is_timeout() {
        ModelTransportFailure::Timeout
    } else if error.is_connect() {
        ModelTransportFailure::Connect
    } else {
        ModelTransportFailure::Interrupted
    };
    crate::transport_failure(native, failure, root)
}

#[cfg(not(target_arch = "wasm32"))]
impl HttpTransport for ReqwestTransport {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(self.exchange(
            request,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        ))
    }

    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, ()> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn now(&self) -> std::time::Duration {
        native_now()
    }

    fn delay_with_native_work(
        &self,
        duration: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'static, ()>> {
        acyclic_harness::NativeFutureBundle::new(work, move || async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, HttpResponse>> {
        acyclic_harness::NativeFutureBundle::new(work, || self.exchange(request, Some(work)))
    }
}
