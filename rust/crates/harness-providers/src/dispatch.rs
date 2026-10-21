use crate::{
    Adapter, HttpRequest, HttpResponse, HttpTransport as _, ResolvedModel, invalid, projection,
};
use acyclic_harness_core::{
    Result,
    model::{ModelDispatch, ModelEvent, PreparedModelRequest},
};
use acyclic_stream::BoxProviderStream;
use futures::StreamExt as _;
use std::{borrow::Cow, collections::BTreeMap};

pub(crate) fn generate(
    provider: &ResolvedModel,
    request: PreparedModelRequest,
    dispatch: ModelDispatch,
) -> BoxProviderStream<'_, Result<ModelEvent>> {
    Box::pin(async_stream::try_stream! {
        provider.validate(&request.request().model)?;
        if request.request_digest() != dispatch.request_digest {
            Err(invalid("model dispatch digest differs from prepared request"))?;
        }
        let names = crate::tool_names::Names::new(request.request());
        let auth = authenticate(provider, #[cfg(not(target_arch = "wasm32"))] None).await?;
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let body = bytes::Bytes::from(encode(provider,&request,&names,None,&origin).await?);
        let mut events = generate_encoded(provider, &request, dispatch, body, auth, names);
        while let Some(event) = events.next().await { yield event?; }
    })
}

/// Caller has admitted model identity and bound dispatch to the immutable request.
pub(crate) fn generate_encoded<'a>(
    provider: &'a ResolvedModel,
    request: &'a PreparedModelRequest,
    dispatch: ModelDispatch,
    body: bytes::Bytes,
    auth: (Option<Cow<'a, str>>, Option<String>),
    names: crate::tool_names::Names<'a>,
) -> BoxProviderStream<'a, Result<ModelEvent>> {
    Box::pin(async_stream::try_stream! {
        let input_bytes = body.len();
        #[cfg(not(target_arch = "wasm32"))]
        let mut auth = auth;
        #[cfg(not(target_arch = "wasm32"))]
        let mut recovery = Recovery::default();
        let mut redelivery = crate::retry::Redelivery::new(provider.config.retry, dispatch);
        let mut output = false;
        loop {
            let (failure, retry_after) = match send(provider, body.clone(), dispatch, auth.0.as_deref(), auth.1.as_deref()).await {
                Err(error) => (Some(error), None),
                Ok(response) if !(200..300).contains(&response.status) => {
                    let retry_after = response.retry_after;
                    (Some(crate::http_failure(response).await), retry_after)
                }
                Ok(response) => {
                    let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
                    let mut events = crate::stream::events_named(provider.config.adapter, response.body,
                        provider.config.max_event_bytes, dispatch, Some(&names), Some(origin));
                    let mut failure = None;
                    while let Some(event) = events.next().await {
                        match event {
                            Ok(event) => {
                                output = true;
                                if let ModelEvent::Completed { metadata } = &event {
                                    provider.observe(request, input_bytes, metadata)?;
                                }
                                yield event;
                            }
                            Err(error) => { failure = Some(error); break; }
                        }
                    }
                    (failure, None)
                }
            };
            let Some(error) = failure else { break; };
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(login) = &provider.codex_login {
                recovery.output = output;
                if recovery.retry(&error) {
                    drop(error);
                    let (key, account) = login.bearer(provider.transport.as_ref(), auth.0.as_deref(), None).await?;
                    auth = (Some(Cow::Owned(key)), account);
                    continue;
                }
            }
            if let Some(wait) = redelivery.next(&error, retry_after, output) {
                drop(error);
                crate::deadline::timer(provider.transport.as_ref(), wait, None)?.await?;
                continue;
            }
            Err(error)?;
        }

    })
}

pub(crate) async fn authenticate<'a>(
    provider: &'a ResolvedModel,
    #[cfg(not(target_arch = "wasm32"))] custody: Option<&mut acyclic_fs::NativeAllocation>,
) -> Result<(Option<Cow<'a, str>>, Option<String>)> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(login) = &provider.codex_login {
        let (key, account) = login
            .bearer(provider.transport.as_ref(), None, custody)
            .await?;
        return Ok((Some(Cow::Owned(key)), account));
    }
    Ok((provider.key.as_deref().map(Cow::Borrowed), None))
}

pub(crate) async fn send(
    provider: &ResolvedModel,
    body: bytes::Bytes,
    dispatch: ModelDispatch,
    key: Option<&str>,
    account: Option<&str>,
) -> Result<HttpResponse> {
    let response = provider
        .transport
        .send(HttpRequest {
            url: projection::endpoint(provider)?,
            headers: request_headers(provider, dispatch, key, account),
            body,
        })
        .await?;
    Ok(response)
}

pub(crate) async fn encode(
    provider: &ResolvedModel,
    request: &PreparedModelRequest,
    names: &crate::tool_names::Names<'_>,
    io: Option<&acyclic_harness_core::conversation::ContentIo>,
    origin: &acyclic_harness_core::model::ReasoningOrigin,
) -> Result<Vec<u8>> {
    let mut files = crate::files::resolve(&provider.files, request.request(), io).await?;
    let body = projection::body_named(provider, request.request(), names, origin, &mut files)?;
    serde_json::to_vec(&body).map_err(|_| invalid("model request encoding failed"))
}

pub(crate) fn request_headers(
    provider: &ResolvedModel,
    dispatch: ModelDispatch,
    key: Option<&str>,
    account: Option<&str>,
) -> BTreeMap<String, String> {
    let headers = headers(provider, key, account, true);
    #[cfg(not(target_arch = "wasm32"))]
    let headers = {
        let mut headers = headers;
        if provider.codex_login.is_some() {
            headers.insert("session-id".into(), dispatch.operation_id.to_string());
            headers.insert("thread-id".into(), dispatch.operation_id.to_string());
            headers.insert(
                "x-client-request-id".into(),
                format!("{}-{}", dispatch.operation_id, dispatch.step),
            );
        }
        headers
    };
    let _ = dispatch;
    headers
}

pub(crate) fn headers(
    provider: &ResolvedModel,
    key: Option<&str>,
    account: Option<&str>,
    streaming: bool,
) -> BTreeMap<String, String> {
    let mut headers = provider.config.headers.clone();
    headers.insert(
        "accept".into(),
        if streaming {
            "text/event-stream"
        } else {
            "application/json"
        }
        .into(),
    );
    if let Some(key) = key {
        let (name, value) = match provider.config.adapter {
            Adapter::Anthropic => ("x-api-key", key.into()),
            Adapter::Gemini => ("x-goog-api-key", key.into()),
            _ => ("authorization", format!("Bearer {key}")),
        };
        headers.insert(name.into(), value);
    }
    if provider.config.adapter == Adapter::Anthropic {
        headers.insert("anthropic-version".into(), "2023-06-01".into());
        #[cfg(target_arch = "wasm32")]
        headers.insert(
            "anthropic-dangerous-direct-browser-access".into(),
            "true".into(),
        );
    }
    if let Some(account) = account {
        headers.insert("chatgpt-account-id".into(), account.into());
    }
    #[cfg(not(target_arch = "wasm32"))]
    if provider.codex_login.is_some() {
        headers.insert("originator".into(), "acyclic_harness".into());
        headers.insert(
            "user-agent".into(),
            concat!("acyclic-harness/", env!("CARGO_PKG_VERSION")).into(),
        );
    }
    headers
}

/// One budget for both HTTP and SSE authentication failures. Once an event has
/// escaped, replay could repeat a caller's side effects and is forbidden.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct Recovery {
    pub(crate) output: bool,
    retried: bool,
}
#[cfg(not(target_arch = "wasm32"))]
impl Recovery {
    pub(crate) fn retry(&mut self, error: &acyclic_harness_core::Error) -> bool {
        if error.code() != acyclic_harness_core::wire::ErrorCode::Unauthorized
            || self.output
            || self.retried
        {
            return false;
        }
        self.retried = true;
        true
    }
}
