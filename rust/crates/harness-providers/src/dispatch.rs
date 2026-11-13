use crate::{Adapter, HttpRequest, HttpResponse, ResolvedModel, invalid, projection};
use acyclic_harness_core::{
    Result,
    model::{ModelDispatch, ModelEvent, PreparedModelRequest},
};
use acyclic_stream::BoxProviderStream;
use futures::StreamExt as _;
use std::{borrow::Cow, collections::BTreeMap};

pub(crate) fn generate<'a>(
    provider: &'a ResolvedModel,
    request: &'a PreparedModelRequest,
    dispatch: ModelDispatch,
) -> BoxProviderStream<'a, Result<ModelEvent>> {
    Box::pin(async_stream::try_stream! {
        let current = provider.current_credentials()?;
        let provider = &current;
        provider.validate(&request.request().model)?;
        if request.request_digest() != dispatch.request_digest {
            Err(invalid("model dispatch digest differs from prepared request"))?;
        }
        let names = crate::tool_names::Names::new(request.request());
        let auth = authenticate(provider, #[cfg(not(target_arch = "wasm32"))] None).await?;
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let encoded = encode(provider,request,&names,None,&origin).await?;
        let events = generate_encoded(provider, request, dispatch, encoded.bytes.into(), encoded.cache, auth, names);
        let mut events = core::pin::pin!(events);
        while let Some(event) = events.next().await { yield event?; }
    })
}

/// Caller has admitted model identity and bound dispatch to the immutable request.
pub(crate) fn generate_encoded<'a>(
    provider: &'a ResolvedModel,
    request: &'a PreparedModelRequest,
    dispatch: ModelDispatch,
    body: bytes::Bytes,
    cache: Option<acyclic_harness_core::executor::ModelCacheObservation>,
    auth: (Option<Cow<'a, str>>, Option<String>),
    names: crate::tool_names::Names<'a>,
) -> impl futures::Stream<Item = Result<ModelEvent>> + acyclic_stream::ProviderTask + 'a {
    async_stream::try_stream! {
        let input_bytes = body.len();
        let mut auth = auth;
        let mut attempt = GenerationAttempt::new(provider.config.retry, dispatch);
        loop {
            let response_body = {
                let exchange = attempt.exchange(provider, &body, dispatch, &mut auth,
                    #[cfg(not(target_arch = "wasm32"))] None, None);
                let mut exchange = core::pin::pin!(exchange);
                loop {
                    match exchange.next().await {
                        Some(Ok(Exchange::Observation(event))) => yield event,
                        Some(Ok(Exchange::Accepted(response))) => break response,
                        Some(Err(error)) => Err(error)?,
                        None => Err(invalid("model exchange ended without an accepted response"))?,
                    }
                }
            };
            let retry_after = response_body.retry_after;
            let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
            let cache = cache.map(|cache| acyclic_harness_core::executor::ModelCacheObservation {
                credential_scope: origin.credential_scope, ..cache
            });
            let mut failure = None;
            {
                let events = crate::stream::events_named(provider.config.adapter, response_body.body,
                    provider.config.max_event_bytes, dispatch, Some(&names), Some(origin), response_body.request_id);
                let mut events = core::pin::pin!(events);
                while let Some(event) = events.next().await {
                    let mut event = match event { Ok(event) => event, Err(error) => { failure = Some(crate::diagnostic::rejection(error)); break; } };
                    if let ModelEvent::Completed { metadata } = &mut event {
                        projection::cache::attach(metadata, cache)?;
                        provider.observe(request, input_bytes, metadata)?;
                        // The provider has finished serving this request.
                        attempt.held = None;
                    }
                    yield event;
                }
            }
            attempt.held = None;
            let Some(error) = failure else {break;};
            if error.code() == acyclic_harness_core::wire::ErrorCode::Unavailable { yield ModelEvent::Restarted; }
            {
                let recovery = attempt.recover(provider, &mut auth, error, retry_after,
                    #[cfg(not(target_arch = "wasm32"))] None, None);
                let mut recovery = core::pin::pin!(recovery);
                while let Some(event) = recovery.next().await { yield event?; }
            }
        }
    }
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
    if provider.key.is_none()
        && provider.credential_scope.is_none()
        && provider.config.authentication != crate::Authentication::Optional
    {
        return Err(invalid("model provider API key is unavailable"));
    }
    Ok((provider.key.as_deref().map(Cow::Borrowed), None))
}

pub(crate) fn send<'a>(
    provider: &'a ResolvedModel,
    body: bytes::Bytes,
    dispatch: ModelDispatch,
    key: Option<&'a str>,
    account: Option<&'a str>,
    work: acyclic_fs::NativeWork<'a>,
    held: &'a mut Option<crate::admission::Permit>,
) -> impl futures::Stream<Item = Result<crate::admission::Event<HttpResponse>>>
+ acyclic_stream::ProviderTask
+ 'a {
    async_stream::try_stream! {
    #[cfg(not(target_arch = "wasm32"))]
    let mut allocation = work
        .map(|work| {
            let bytes = provider.config.base_url.len()
                + provider
                    .config
                    .headers
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
                + key.map_or(0, str::len)
                + account.map_or(0, str::len);
            crate::native::reserve(
                work,
                bytes as u64,
                provider.config.headers.len() as u64 + 16,
                0,
            )
        })
        .transpose()?;
    let url = projection::endpoint(provider);
    #[cfg(not(target_arch = "wasm32"))]
    let url = match allocation.as_mut() {
        Some(allocation) => crate::native::diagnostic(url, allocation),
        None => url,
    };
    let request = HttpRequest {
        method: http::Method::POST,
        url: url?,
        headers: request_headers(provider, dispatch, key, account),
        body,
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(allocation) = allocation.as_mut() {
        let retained = std::mem::size_of::<HttpRequest>()
            + request.url.capacity()
            + request.headers.len() * std::mem::size_of::<(String, String)>()
            + request
                .headers
                .iter()
                .map(|(name, value)| name.capacity() + value.capacity())
                .sum::<usize>();
        crate::native::retain(allocation, retained as u64)?;
    }
    let events = crate::admission::send(provider, request, work, held);
    let mut events = core::pin::pin!(events);
    while let Some(event) = events.next().await { yield event?; }
    }
}

pub(crate) async fn encode(
    provider: &ResolvedModel,
    request: &PreparedModelRequest,
    names: &crate::tool_names::Names<'_>,
    io: Option<&acyclic_harness_core::conversation::ContentIo>,
    origin: &acyclic_harness_core::model::ReasoningOrigin,
) -> Result<projection::cache::Encoded> {
    let mut files = crate::files::resolve(&provider.files, request, io).await?;
    let body = projection::body_named(provider, request.request(), names, origin, &mut files)?;
    projection::cache::encode(&body, provider, origin)
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
        if streaming {
            let feature = "model-context-window-exceeded-2025-08-26";
            if let Some((_, value)) = headers
                .iter_mut()
                .find(|(name, _)| name.eq_ignore_ascii_case("anthropic-beta"))
            {
                if !value.split(',').any(|part| part.trim() == feature) {
                    if !value.is_empty() {
                        value.push(',');
                    }
                    value.push_str(feature);
                }
            } else {
                headers.insert("anthropic-beta".into(), feature.into());
            }
        }
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

/// One generation's recovery owner, retained across every transport attempt.
pub(crate) struct GenerationAttempt {
    attempt: crate::retry::Attempt,
    /// The endpoint admission of the accepted response being read.
    pub(crate) held: Option<crate::admission::Permit>,
}

/// One shared exchange's original observations, followed by its accepted body.
pub(crate) enum Exchange {
    Observation(ModelEvent),
    Accepted(HttpResponse),
}

impl GenerationAttempt {
    pub(crate) fn new(spec: crate::RetrySpec, dispatch: ModelDispatch) -> Self {
        Self {
            attempt: crate::retry::Attempt::generation(spec, dispatch),
            held: None,
        }
    }

    /// Finish the original first send or its permitted pre-acceptance recovery.
    /// Accepted bodies leave this loop once, before their parser is polled.
    pub(crate) fn exchange<'a, 'auth>(
        &'a mut self,
        provider: &'a ResolvedModel,
        body: &'a bytes::Bytes,
        dispatch: ModelDispatch,
        auth: &'a mut (Option<Cow<'auth, str>>, Option<String>),
        #[cfg(not(target_arch = "wasm32"))] mut custody: Option<
            &'a mut acyclic_fs::NativeAllocation,
        >,
        work: acyclic_fs::NativeWork<'a>,
    ) -> impl futures::Stream<Item = Result<Exchange>> + acyclic_stream::ProviderTask + 'a {
        async_stream::try_stream! {
        loop {
            let response = {
                let events = send(provider, body.clone(), dispatch,
                    auth.0.as_deref(), auth.1.as_deref(), work, &mut self.held);
                let mut events = core::pin::pin!(events);
                loop {
                    match events.next().await {
                        Some(Ok(crate::admission::Event::Wait(wait))) =>
                            yield Exchange::Observation(ModelEvent::Wait(wait)),
                        Some(Ok(crate::admission::Event::Ready(response))) => break Ok(response),
                        Some(Err(error)) => break Err(error),
                        None => break Err(invalid("model send ended without a response")),
                    }
                }
            };
            let (error, retry_after) = match response {
                Ok(response) if (200..300).contains(&response.status) => {
                    self.attempt.observe(response.status);
                    yield Exchange::Accepted(response);
                    break;
                }
                Ok(response) => {
                    // Preserve refusal before its diagnostic body can fail.
                    self.attempt.observe(response.status);
                    let retry_after = response.retry_after;
                    let error =
                        crate::recovery_failure(response, provider.config.max_event_bytes, work)
                            .await;
                    (crate::diagnostic::rejection(error), retry_after)
                }
                Err(error) => (error, None),
            };
            {
            let recovery = self.recover(
                provider,
                auth,
                error,
                retry_after,
                #[cfg(not(target_arch = "wasm32"))]
                custody.as_deref_mut(),
                work,
            );
            let mut recovery = core::pin::pin!(recovery);
            while let Some(event) = recovery.next().await {
                yield Exchange::Observation(event?);
            }
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = work {
                work.remaining()?;
            }
        }
        }
    }

    /// Complete recovery before the caller may send another model request.
    /// Refresh and timer failures are final causes, never model retry inputs.
    pub(crate) fn recover<'a, 'auth>(
        &'a mut self,
        provider: &'a ResolvedModel,
        auth: &'a mut (Option<Cow<'auth, str>>, Option<String>),
        error: acyclic_harness_core::Error,
        retry_after: Option<std::time::Duration>,
        #[cfg(not(target_arch = "wasm32"))] custody: Option<&'a mut acyclic_fs::NativeAllocation>,
        work: acyclic_fs::NativeWork<'a>,
    ) -> impl futures::Stream<Item = Result<ModelEvent>> + acyclic_stream::ProviderTask + 'a {
        async_stream::try_stream! {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(login) = &provider.codex_login
            && self.attempt.refresh(&error)
        {
            drop(error);
            let (key, account) = login
                .bearer(provider.transport.as_ref(), auth.0.as_deref(), custody)
                .await?;
            *auth = (Some(Cow::Owned(key)), account);
            return;
        }
        let _ = auth;
        if let Some(wait) = self.attempt.next(&error, retry_after) {
            let events = crate::retry::events(provider.transport.as_ref(), error, wait, retry_after.is_some(), work);
            let mut events = core::pin::pin!(events);
            while let Some(event) = events.next().await { yield event?; }
            return;
        }
        Err(error)?;
        }
    }
}
