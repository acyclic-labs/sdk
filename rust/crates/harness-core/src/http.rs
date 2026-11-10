//! Shared credential-free HTTP exchange boundary for model and machine adapters.
use crate::Result;
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, ProviderPlatform};
use bytes::Bytes;
use std::collections::BTreeMap;
// Native work has the same availability as NativeFutureBundle and NativeWork.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
/// Native response payload custody shared by protocol codecs.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub mod owned_bytes;
/// One streaming HTTP request. Headers may contain credentials; deliberately not Debug.
pub struct HttpRequest {
    /// Fully resolved endpoint.
    pub url: String,
    /// HTTP method; transports preserve it without redirects or retries.
    pub method: http::Method,
    /// Protocol and authentication headers.
    pub headers: BTreeMap<String, String>,
    /// Exact request body.
    pub body: Bytes,
}

/// Response whose body remains lazy until polled.
pub struct HttpResponse {
    /// HTTP status.
    pub status: u16,
    /// Incremental body. Dropping it cancels I/O.
    pub body: BoxProviderStream<'static, Result<Bytes>>,
    /// Allow-listed provider request identifier. Header storage moves from the
    /// transport and is copied once into admitted completion metadata.
    pub request_id: Option<::http::HeaderValue>,
    /// The response's `Retry-After` delay, when it gave one in seconds.
    pub retry_after: Option<std::time::Duration>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl HttpResponse {
    /// Admits the concrete body stream before boxing it on the issued ledger.
    /// Charges received bytes once; downstream codecs charge their own copies/decoding.
    /// Its first error or EOF is terminal; later polls return EOF without polling
    /// the original source or checking its work again.
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
            request_id: None,
            retry_after: None,
        })
    }
}

/// Platform HTTP boundary, also used by scripted fixtures.
pub trait HttpTransport: ProviderPlatform {
    /// Sends exactly one request without retries.
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>>;
    /// Waits `duration` on the platform timer: a redelivery backoff, or a deadline's wake-up.
    /// A lost or failed timer producer returns an error, never an elapsed wait.
    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, Result<()>>;
    /// Original wall clock for absolute guest deadlines, not a readiness probe.
    fn unix_ms(&self) -> Result<u64> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| crate::Error::Invalid("HTTP wall clock unavailable".into()))?;
            u64::try_from(time.as_millis())
                .map_err(|_| crate::Error::Invalid("HTTP clock overflow".into()))
        }
        #[cfg(target_arch = "wasm32")]
        {
            let time = js_sys::Date::now();
            if !time.is_finite() || time < 0.0 {
                return Err(crate::Error::Invalid("HTTP wall clock unavailable".into()));
            }
            Ok(time as u64)
        }
    }
    /// Monotonic time on the platform clock that `delay` waits on, from a fixed origin.
    /// Deadlines are measured on it; timers only wake their measurement.
    fn now(&self) -> std::time::Duration;
    /// `delay` admitted on the original invocation ledger before it exists.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn delay_with_native_work(
        &self,
        _: std::time::Duration,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'static, ()>> {
        work.remaining()?;
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native timer",
        })
    }
    /// Whether this transport funds its native callback frames before allocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }
    /// Issues one HTTP exchange on the original invocation ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn send_with_native_work<'a>(
        &'a self,
        _: HttpRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, HttpResponse>> {
        work.remaining()?;
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "HTTP transport has no native producer",
        })
    }
}

mod credentials;
pub use credentials::{
    AuthorizationScheme, CredentialBinding, CredentialResolver, CredentialSource,
};

#[cfg(target_arch = "wasm32")]
fn invalid(reason: impl Into<String>) -> crate::Error {
    crate::Error::Invalid(reason.into())
}
#[cfg(target_arch = "wasm32")]
/// Shared browser exchange implementation.
pub mod browser;
#[cfg(target_arch = "wasm32")]
pub use browser::BrowserTransport;
#[cfg(target_arch = "wasm32")]
mod browser_credentials;
#[cfg(target_arch = "wasm32")]
pub use browser_credentials::BrowserCredentialResolver;

/// Both HTTP forms of `Retry-After`, relative to the platform clock.
pub fn retry_after(value: &str) -> Option<std::time::Duration> {
    retry_after_at(value, || {
        #[cfg(not(target_arch = "wasm32"))]
        let now = std::time::SystemTime::now();
        #[cfg(target_arch = "wasm32")]
        let now = std::time::UNIX_EPOCH
            + std::time::Duration::from_secs_f64(js_sys::Date::now() / 1_000.0);
        now
    })
}

/// Decodes a delay hint relative to the supplied original clock.
pub fn retry_after_at(
    value: &str,
    now: impl FnOnce() -> std::time::SystemTime,
) -> Option<std::time::Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // Overflow is still a delay hint, always beyond our u32-millisecond retry budget.
        // Treating it as absent would retry before the endpoint's requested time.
        return Some(std::time::Duration::from_secs(
            value.parse().unwrap_or(u64::MAX),
        ));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    Some(
        date.duration_since(now())
            .unwrap_or(std::time::Duration::ZERO),
    )
}
