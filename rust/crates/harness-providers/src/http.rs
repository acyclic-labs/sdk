use acyclic_harness_core::Result;
use acyclic_stream::BoxProviderFuture;
#[cfg(not(target_arch = "wasm32"))]
use futures::StreamExt as _;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use acyclic_harness_core::http::owned_bytes;
pub use acyclic_harness_core::http::{HttpRequest, HttpResponse, HttpTransport};
pub(crate) mod bounded;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod refusal_tests;

/// Issue one exchange through the selected platform adapter. Native work never
/// falls back to an unfunded producer, including when admission is refused.
pub(crate) fn send<'a>(
    transport: &'a dyn HttpTransport,
    request: HttpRequest,
    work: acyclic_fs::NativeWork<'a>,
) -> Result<
    impl std::future::Future<Output = Result<HttpResponse>> + acyclic_stream::ProviderTask + 'a,
> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        if let Some(work) = work {
            return Ok(futures::future::Either::Right(
                transport.send_with_native_work(request, work)?,
            ));
        }
        Ok(futures::future::Either::Left(transport.send(request)))
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = work;
        Ok(transport.send(request))
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) mod browser;
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
        let mut builder = self.0.request(request.method, request.url);
        if !request
            .headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("content-type"))
        {
            builder = builder.header("content-type", "application/json");
        }
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let mut response = builder
            .body(request.body)
            .send()
            .await
            .map_err(|error| reqwest_failure(native, &error))?;
        let status = response.status().as_u16();
        let retry_after = acyclic_harness_core::http::response_retry_after(
            response
                .headers()
                .iter()
                .filter_map(|(name, value)| Some((name.as_str(), value.to_str().ok()?))),
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(self.unix_ms()?),
        );
        let request_id = if acyclic_harness_core::model::RESEARCH_CAPTURE_ENABLED {
            response
                .headers_mut()
                .remove("x-request-id")
                .or_else(|| response.headers_mut().remove("request-id"))
        } else {
            None
        };
        let body = response
            .bytes_stream()
            .map(move |chunk| chunk.map_err(|error| reqwest_failure(native, &error)));
        #[cfg(not(target_arch = "wasm32"))]
        let body = match work {
            Some(work) => HttpResponse::with_native_body(status, body, work)?.body,
            None => Box::pin(body),
        };
        #[cfg(target_arch = "wasm32")]
        let body = Box::pin(body);
        Ok(HttpResponse {
            status,
            body,
            request_id,
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
fn reqwest_failure(native: bool, error: &reqwest::Error) -> acyclic_harness_core::Error {
    use acyclic_harness_core::ModelTransportFailure;
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
    let failure = if error.is_timeout() {
        ModelTransportFailure::Timeout
    } else if tls {
        ModelTransportFailure::Tls
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

    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        Box::pin(async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }

    fn now(&self) -> std::time::Duration {
        native_now()
    }

    fn delay_with_native_work(
        &self,
        duration: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'static, (), acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, move || async move {
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
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, HttpResponse, acyclic_harness_core::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || self.exchange(request, Some(work)))
    }
}
