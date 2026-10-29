use super::{HttpRequest, HttpResponse, HttpTransport};
use crate::invalid;
use acyclic_harness_core::Result;
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use js_sys::{Function, Promise, Reflect, Uint8Array};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    AbortController, Headers, ReadableStreamDefaultReader, RequestCredentials, RequestInit,
    RequestRedirect, Response,
};
#[path = "browser_delay.rs"]
mod timer;

/// Browser HTTP with an explicitly owned fetch function.
pub struct BrowserTransport(Function);

struct Abort(AbortController);
impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Body {
    reader: ReadableStreamDefaultReader,
    _abort: Abort,
}

impl BrowserTransport {
    /// Injects platform I/O without replacing any protocol implementation.
    pub fn new(fetch: Function) -> Self {
        Self(fetch)
    }

    /// Selects fetch from the current browser or worker realm.
    pub fn global() -> Result<Self> {
        let fetch = Reflect::get(&js_sys::global(), &JsValue::from_str("fetch"))
            .map_err(|_| invalid("browser fetch is unavailable"))?
            .dyn_into::<Function>()
            .map_err(|_| invalid("browser fetch must be a function"))?;
        Ok(Self::new(fetch))
    }
}

impl HttpTransport for BrowserTransport {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let abort = Abort(
                AbortController::new()
                    .map_err(|_| invalid("browser cancellation is unavailable"))?,
            );
            let init = RequestInit::new();
            init.set_method("POST");
            init.set_credentials(RequestCredentials::Omit);
            init.set_redirect(RequestRedirect::Error);
            init.set_signal(Some(&abort.0.signal()));
            let body = Uint8Array::from(request.body.as_ref());
            init.set_body(body.as_ref());
            let headers = Headers::new().map_err(|_| invalid("browser headers are unavailable"))?;
            headers
                .set("content-type", "application/json")
                .map_err(|_| invalid("invalid model content type"))?;
            for (name, value) in request.headers {
                headers
                    .set(&name, &value)
                    .map_err(|_| invalid("invalid model HTTP header"))?;
            }
            init.set_headers(headers.as_ref());
            let response = self
                .0
                .call2(
                    &js_sys::global(),
                    &JsValue::from_str(&request.url),
                    init.as_ref(),
                )
                .map_err(|cause| failure(cause, "browser model dispatch failed"))?;
            let response = JsFuture::from(Promise::resolve(&response))
                .await
                .map_err(|cause| failure(cause, "browser model exchange failed"))?
                .dyn_into::<Response>()
                .map_err(|_| invalid("fetch did not return a Response"))?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get("retry-after")
                .ok()
                .flatten()
                .and_then(|value| crate::retry::retry_after(&value));
            let Some(body) = response.body() else {
                return Ok(HttpResponse {
                    status,
                    body: Box::pin(futures::stream::empty()),
                    retry_after,
                });
            };
            let reader = body
                .get_reader()
                .dyn_into::<ReadableStreamDefaultReader>()
                .map_err(|_| invalid("browser response stream has no reader"))?;
            let body = futures::stream::try_unfold(
                Body {
                    reader,
                    _abort: abort,
                },
                |body| async move {
                    let chunk = JsFuture::from(body.reader.read())
                        .await
                        .map_err(|cause| failure(cause, "browser model body interrupted"))?;
                    let done = Reflect::get(&chunk, &JsValue::from_str("done"))
                        .map_err(|_| invalid("invalid browser stream result"))?;
                    if done.as_bool() == Some(true) {
                        return Ok(None);
                    }
                    let value = Reflect::get(&chunk, &JsValue::from_str("value"))
                        .map_err(|_| invalid("invalid browser stream chunk"))?
                        .dyn_into::<Uint8Array>()
                        .map_err(|_| invalid("browser stream chunk must be bytes"))?;
                    Ok(Some((Bytes::from(value.to_vec()), body)))
                },
            );
            Ok(HttpResponse {
                status,
                body: Box::pin(body),
                retry_after,
            })
        })
    }

    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, ()> {
        Box::pin(timer::delay(duration))
    }

    fn now(&self) -> std::time::Duration {
        timer::now()
    }
}

pub(crate) fn failure(cause: JsValue, message: &'static str) -> acyclic_harness_core::Error {
    use acyclic_harness_core::{Error, wire::ErrorCode};
    let received = Reflect::get(&cause, &JsValue::from_str("code"))
        .ok()
        .and_then(|value| value.as_f64());
    let code = [
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::Unauthorized,
        ErrorCode::Cancelled,
        ErrorCode::Unsupported,
        ErrorCode::Invalid,
        ErrorCode::Storage,
        ErrorCode::Indeterminate,
        ErrorCode::InteractionDeclined,
        ErrorCode::InteractionCancelled,
        ErrorCode::InteractionExpired,
        ErrorCode::InteractionDenied,
        ErrorCode::Unavailable,
    ]
    .into_iter()
    .find(|code| received == Some(f64::from(*code as i32)));
    if let Some(code) = code {
        return Error::ModelDiagnostic {
            code,
            message: message.into(),
            custody: None,
        };
    }
    crate::transport_failure(
        false,
        acyclic_harness_core::ModelTransportFailure::Interrupted,
        message,
    )
}

#[cfg(test)]
#[path = "browser_tests.rs"]
mod tests;
