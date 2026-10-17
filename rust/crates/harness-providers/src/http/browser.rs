use super::{HttpRequest, HttpResponse, HttpTransport};
use crate::invalid;
use acyclic_harness::Result;
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use js_sys::{Function, Promise, Reflect, Uint8Array};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    AbortController, Headers, ReadableStreamDefaultReader, RequestCredentials, RequestInit,
    RequestRedirect, Response,
};

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
                .map_err(|_| {
                    acyclic_harness::Error::Storage("browser model dispatch failed".into())
                })?;
            let response = JsFuture::from(Promise::resolve(&response))
                .await
                .map_err(|_| {
                    acyclic_harness::Error::Storage("browser model exchange failed".into())
                })?
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
                    let chunk = JsFuture::from(body.reader.read()).await.map_err(|_| {
                        acyclic_harness::Error::Storage("browser model body interrupted".into())
                    })?;
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
        Box::pin(async move {
            let milliseconds = JsValue::from_f64(duration.as_secs_f64() * 1_000.0);
            let promise = Promise::new(&mut |resolve, _| {
                let global = js_sys::global();
                let scheduled = Reflect::get(&global, &JsValue::from_str("setTimeout"))
                    .ok()
                    .and_then(|set| set.dyn_into::<Function>().ok())
                    .is_some_and(|set| set.call2(&global, &resolve, &milliseconds).is_ok());
                if !scheduled {
                    let _ = resolve.call0(&JsValue::UNDEFINED);
                }
            });
            let _ = JsFuture::from(promise).await;
        })
    }
}

#[cfg(test)]
#[path = "browser_tests.rs"]
mod tests;
