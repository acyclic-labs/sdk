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
        provider.validate(&request.request().model)?;
        if request.request_digest() != dispatch.request_digest {
            Err(invalid("model dispatch digest differs from prepared request"))?;
        }
        let names = crate::tool_names::Names::new(request.request());
        let auth = authenticate(provider, #[cfg(not(target_arch = "wasm32"))] None).await?;
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let body = bytes::Bytes::from(encode(provider,request,&names,None,&origin).await?);
        let events = generate_encoded(provider, request, dispatch, body, auth, names);
        futures::pin_mut!(events);
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
) -> impl futures::Stream<Item = Result<ModelEvent>> + acyclic_stream::ProviderTask + 'a {
    async_stream::try_stream! {
        let input_bytes = body.len();
        let mut auth = auth;
        let mut attempt = GenerationAttempt::new(provider.config.retry, dispatch);
        let response = send(provider, body.clone(), dispatch, auth.0.as_deref(), auth.1.as_deref(), None).await;
        let response = attempt.exchange(provider, &body, dispatch, &mut auth, response,
            #[cfg(not(target_arch = "wasm32"))] None, None).await?;
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let events = crate::stream::events_named(provider.config.adapter, response.body,
            provider.config.max_event_bytes, dispatch, Some(&names), Some(origin));
        futures::pin_mut!(events);
        while let Some(event) = events.next().await {
            let event = event.map_err(crate::diagnostic::rejection)?;
            if let ModelEvent::Completed { metadata } = &event {
                provider.observe(request, input_bytes, metadata)?;
            }
            yield event;
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
    Ok((provider.key.as_deref().map(Cow::Borrowed), None))
}

pub(crate) async fn send(
    provider: &ResolvedModel,
    body: bytes::Bytes,
    dispatch: ModelDispatch,
    key: Option<&str>,
    account: Option<&str>,
    work: acyclic_harness_core::NativeWork<'_>,
) -> Result<HttpResponse> {
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        return provider
            .transport
            .send_with_native_work(request, work)?
            .await;
    }
    let _ = work;
    provider.transport.send(request).await
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

/// One generation's recovery budget. Successful HTTP admission is final even
/// before the first event: a lost body cannot prove the model did not execute.
pub(crate) struct GenerationAttempt {
    admission: Admission,
    redelivery: crate::retry::Redelivery,
    #[cfg(not(target_arch = "wasm32"))]
    retried: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Admission {
    Unknown,
    Refused(u16),
    Accepted,
}

impl GenerationAttempt {
    pub(crate) fn new(spec: crate::RetrySpec, dispatch: ModelDispatch) -> Self {
        Self {
            admission: Admission::Unknown,
            redelivery: crate::retry::Redelivery::new(spec, dispatch),
            #[cfg(not(target_arch = "wasm32"))]
            retried: false,
        }
    }

    /// Finish the original first send or its permitted pre-acceptance recovery.
    /// Accepted bodies leave this loop once, before their parser is polled.
    pub(crate) async fn exchange(
        &mut self,
        provider: &ResolvedModel,
        body: &bytes::Bytes,
        dispatch: ModelDispatch,
        auth: &mut (Option<Cow<'_, str>>, Option<String>),
        mut response: Result<HttpResponse>,
        #[cfg(not(target_arch = "wasm32"))] mut custody: Option<&mut acyclic_fs::NativeAllocation>,
        work: acyclic_harness_core::NativeWork<'_>,
    ) -> Result<HttpResponse> {
        loop {
            let (error, retry_after) = match response {
                Ok(response) if (200..300).contains(&response.status) => {
                    self.accept();
                    return Ok(response);
                }
                Ok(response) => {
                    // Preserve refusal before its diagnostic body can fail.
                    self.refuse(response.status);
                    let retry_after = response.retry_after;
                    let error =
                        crate::http_failure(response, provider.config.max_event_bytes, work).await;
                    (crate::diagnostic::rejection(error), retry_after)
                }
                Err(error) => (error, None),
            };
            self.recover(
                provider,
                auth,
                error,
                retry_after,
                #[cfg(not(target_arch = "wasm32"))]
                custody.as_deref_mut(),
                work,
            )
            .await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = work {
                work.remaining()?;
            }
            response = send(
                provider,
                body.clone(),
                dispatch,
                auth.0.as_deref(),
                auth.1.as_deref(),
                work,
            )
            .await;
        }
    }

    pub(crate) fn accept(&mut self) {
        self.admission = Admission::Accepted;
    }

    pub(crate) fn refuse(&mut self, status: u16) {
        if self.admission != Admission::Accepted {
            self.admission = Admission::Refused(status);
        }
    }

    /// Complete recovery before the caller may send another model request.
    /// Refresh and timer failures are final causes, never model retry inputs.
    async fn recover(
        &mut self,
        provider: &ResolvedModel,
        auth: &mut (Option<Cow<'_, str>>, Option<String>),
        error: acyclic_harness_core::Error,
        retry_after: Option<std::time::Duration>,
        #[cfg(not(target_arch = "wasm32"))] custody: Option<&mut acyclic_fs::NativeAllocation>,
        work: acyclic_harness_core::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(login) = &provider.codex_login
            && self.refresh(&error)
        {
            drop(error);
            let (key, account) = login
                .bearer(provider.transport.as_ref(), auth.0.as_deref(), custody)
                .await?;
            *auth = (Some(Cow::Owned(key)), account);
            return Ok(());
        }
        let _ = auth;
        if let Some(wait) = self.redeliver(&error, retry_after) {
            drop(error);
            crate::deadline::timer(provider.transport.as_ref(), wait, work)?.await?;
            return Ok(());
        }
        Err(error)
    }

    pub(crate) fn redeliver(
        &mut self,
        error: &acyclic_harness_core::Error,
        retry_after: Option<std::time::Duration>,
    ) -> Option<std::time::Duration> {
        // A timeout or disconnect before headers can still lose an accepted
        // request's reply. Only a failed connection or explicit HTTP refusal
        // proves there was no successful response to preserve. A failed error
        // body cannot turn a final HTTP status into a retryable refusal.
        let retryable = match self.admission {
            Admission::Unknown => matches!(
                error,
                acyclic_harness_core::Error::ModelUnavailable {
                    failure: acyclic_harness_core::ModelTransportFailure::Connect,
                    ..
                }
            ),
            Admission::Refused(status) => crate::retry::retryable_status(status),
            Admission::Accepted => false,
        };
        if !retryable {
            return None;
        }
        let wait = self.redelivery.next(error, retry_after, false);
        if wait.is_some() {
            self.admission = Admission::Unknown;
        }
        wait
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn refresh(&mut self, error: &acyclic_harness_core::Error) -> bool {
        if error.code() != acyclic_harness_core::wire::ErrorCode::Unauthorized
            || self.admission != Admission::Refused(401)
            || self.retried
        {
            return false;
        }
        self.retried = true;
        self.admission = Admission::Unknown;
        true
    }
}
