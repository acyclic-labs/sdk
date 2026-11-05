//! Verified provider admission, independent of usage-calibrated planning weights.
pub(crate) mod wire;
use crate::{HttpRequest, ResolvedModel, dispatch, projection};
use acyclic_harness_core::{Error, NativeCustody, NativeWork, Result, context::ModelTokenCount};

fn boundary(work: NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    crate::invalid(reason)
}

pub(crate) fn specification<'a>(
    config: &'a crate::ProviderSpec,
    work: NativeWork<'_>,
) -> Result<&'a wire::TokenCounterSpec> {
    let spec = config.token_counter.as_ref().ok_or_else(|| {
        let reason = "configure a verified input token counter for this endpoint";
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_some() {
            return Error::NativeBoundary {
                code: acyclic_harness_core::wire::ErrorCode::Unsupported,
                reason,
            };
        }
        Error::Unsupported(reason.into())
    })?;
    if spec.adapter != config.adapter {
        return Err(boundary(
            work,
            "token counter dialect differs from generation",
        ));
    }
    Ok(spec)
}

/// Admission for one request: a locally proven upper bound when it is within
/// `ceiling`, so no count request precedes generation, otherwise the
/// endpoint's verified count. The only guarantee admission buys is that the
/// caller compacts before the window overflows, and a bound within the ceiling
/// already proves the caller need not compact.
pub(crate) async fn count(
    provider: &ResolvedModel,
    request: &acyclic_harness_core::model::PreparedModelRequest,
    ceiling: u64,
    work: NativeWork<'_>,
) -> Result<(ModelTokenCount, NativeCustody)> {
    if request.request().model != provider.model {
        return Err(boundary(
            work,
            "request model differs from resolved selection",
        ));
    }
    let framing = u64::from(
        provider
            .config
            .token_counter
            .as_ref()
            .map_or(0, |spec| spec.framing_tokens),
    );
    if let Some(bound) = provider
        .accounting
        .usage
        .bound(request)
        .map_err(|_| boundary(work, "usage bound encoding failed"))?
        .and_then(|tokens| tokens.checked_add(framing))
        .filter(|tokens| *tokens <= ceiling)
    {
        return local(provider, request, bound, work);
    }
    #[cfg(not(target_arch = "wasm32"))]
    if provider.codex_login.is_some() {
        let tokens = provider.accounting.usage.codex_estimate(request)?;
        return local(provider, request, tokens, work);
    }
    let spec = specification(&provider.config, work)?;
    #[cfg(not(target_arch = "wasm32"))]
    let mut auth_custody = work
        .map(|work| work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED))
        .transpose()?;
    let auth = dispatch::authenticate(
        provider,
        #[cfg(not(target_arch = "wasm32"))]
        auth_custody.as_mut(),
    )
    .await?;
    #[cfg(not(target_arch = "wasm32"))]
    let mut admission = work
        .map(|work| {
            let (mut bytes, nodes) = request.projection_shape();
            for message in &request.request().messages {
                bytes = bytes
                    .checked_add(crate::files::extra_bytes(
                        &message.content,
                        provider.files.maximum_bytes,
                    )? as u64)
                    .ok_or_else(|| boundary(Some(work), "model file size overflow"))?;
            }
            crate::native::reserve(
                work,
                bytes,
                nodes,
                auth.0.as_ref().map_or(0, |key| key.len() as u64)
                    + auth.1.as_ref().map_or(0, |account| account.len() as u64),
            )
        })
        .transpose()?;
    #[cfg(not(target_arch = "wasm32"))]
    let content_io = work
        .map(acyclic_harness_core::filesystem::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(target_arch = "wasm32")]
    let content_io = None;
    let prepared = async {
        // Retain table custody until after its projected generation is encoded.
        let _name_custody;
        let mut files =
            crate::files::resolve(&provider.files, request.request(), content_io.as_ref()).await?;
        let names = {
            let (names, custody) = crate::tool_names::Names::funded(request.request(), work)?;
            _name_custody = custody;
            names
        };
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let generation =
            projection::body_named(provider, request.request(), &names, &origin, &mut files)?;
        let bytes = wire::body(generation, &provider.model.name, spec.adapter)?;
        let headers = dispatch::headers(provider, auth.0.as_deref(), auth.1.as_deref(), false);
        let url = wire::endpoint(&provider.config.base_url, &provider.model.name, spec)?;
        let plan = projection::plan(provider, request, 0)?;
        Ok::<_, Error>((url, headers, bytes, plan))
    }
    .await;
    #[cfg(not(target_arch = "wasm32"))]
    let prepared = match admission.as_mut() {
        Some(hold) => crate::native::diagnostic(prepared, hold),
        None => prepared,
    };
    // Declare custody before the count so cancellation drops the DTO first.
    #[cfg(not(target_arch = "wasm32"))]
    let count_custody;
    #[cfg(target_arch = "wasm32")]
    let count_custody = None;
    let (url, headers, bytes, mut count) = prepared?;
    drop(auth);
    #[cfg(not(target_arch = "wasm32"))]
    drop(auth_custody);
    #[cfg(not(target_arch = "wasm32"))]
    {
        count_custody = admission
            .as_mut()
            .map(|hold| {
                hold.split_off(
                    (std::mem::size_of::<ModelTokenCount>() + count.message_tokens.capacity() * 4)
                        as u64,
                )
            })
            .transpose()?;
    }
    // For text input, every counted token spans at least one body byte, and
    // `framing` bounds added input. Compressed media has no such byte bound.
    let has_native_media = request.input_has_native_media()?;
    if let Some(bound) = (bytes.len() as u64)
        .checked_add(framing)
        .filter(|tokens| *tokens <= ceiling && !has_native_media)
    {
        count.input_tokens = bound;
        return Ok((count, count_custody));
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(hold) = admission.as_mut() {
        let retained = std::mem::size_of::<HttpRequest>()
            + url.capacity()
            + bytes.capacity()
            + headers.len() * std::mem::size_of::<(String, String)>()
            + headers
                .iter()
                .map(|(key, value)| key.capacity() + value.capacity())
                .sum::<usize>();
        crate::native::retain(hold, retained as u64)?;
    }
    #[cfg(target_arch = "wasm32")]
    let body = bytes.into();
    #[cfg(not(target_arch = "wasm32"))]
    let body = match admission {
        Some(custody) => crate::http::owned_bytes::owned(bytes, custody)?,
        None => bytes.into(),
    };
    let exchange = HttpRequest { url, headers, body };
    send_count(provider, exchange, count, count_custody, work).await
}

/// A count proven without a request is charged like the DTO it returns.
fn local(
    provider: &ResolvedModel,
    request: &acyclic_harness_core::model::PreparedModelRequest,
    tokens: u64,
    work: NativeWork<'_>,
) -> Result<(ModelTokenCount, NativeCustody)> {
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = work
        .map(|work| {
            work.claim(
                (std::mem::size_of::<ModelTokenCount>() + request.request().messages.len() * 4)
                    as u64,
                1,
                acyclic_fs::WorkCounters {
                    bytes_encoded: request.bytes().len() as u64,
                    items_examined: request.request().messages.len() as u64,
                    ..acyclic_fs::WorkCounters::UNCHARGED
                },
            )
        })
        .transpose()?;
    #[cfg(target_arch = "wasm32")]
    let custody: NativeCustody = {
        let _ = work;
        None
    };
    let result = projection::plan(provider, request, tokens);
    #[cfg(not(target_arch = "wasm32"))]
    let result = match custody.as_mut() {
        Some(hold) => crate::native::diagnostic(result, hold),
        None => result,
    };
    Ok((result?, custody))
}

pub(crate) async fn send_count(
    provider: &ResolvedModel,
    exchange: HttpRequest,
    mut count: ModelTokenCount,
    count_custody: NativeCustody,
    work: NativeWork<'_>,
) -> Result<(ModelTokenCount, NativeCustody)> {
    count.input_tokens = exchange_count(provider, exchange, count.request_digest, work).await?;
    Ok((count, count_custody))
}

async fn read_count(
    provider: &ResolvedModel,
    response: crate::HttpResponse,
    work: NativeWork<'_>,
) -> Result<u64> {
    let spec = provider
        .config
        .token_counter
        .as_ref()
        .ok_or_else(|| crate::invalid("missing token counter"))?;
    let body =
        crate::http::bounded::read(response.body, provider.config.max_event_bytes, work).await?;
    #[cfg(not(target_arch = "wasm32"))]
    let mut body = body;
    let verified = wire::response(&body.bytes, spec);
    #[cfg(not(target_arch = "wasm32"))]
    let verified = match body.custody.as_mut() {
        Some(hold) => crate::native::diagnostic(verified, hold),
        None => verified,
    };
    verified
}

/// A count has no caller-visible output until its whole body is validated. All retryable
/// failures, including a cut or stalled successful body, consume this one attempt budget.
async fn exchange_count(
    provider: &ResolvedModel,
    exchange: HttpRequest,
    request_digest: [u8; 32],
    work: NativeWork<'_>,
) -> Result<u64> {
    let mut recovery = crate::retry::Attempt::count(provider.config.retry, request_digest);
    // Each attempt sends a copy, so the copy in flight is funded alongside the retained request.
    #[cfg(not(target_arch = "wasm32"))]
    let _copy = work
        .map(|work| {
            let bytes = exchange.url.capacity()
                + exchange
                    .headers
                    .iter()
                    .map(|(key, value)| {
                        std::mem::size_of::<(String, String)>() + key.capacity() + value.capacity()
                    })
                    .sum::<usize>();
            work.claim(bytes as u64, 1, acyclic_fs::WorkCounters::UNCHARGED)
        })
        .transpose()?;
    loop {
        let attempt = HttpRequest {
            url: exchange.url.clone(),
            headers: exchange.headers.clone(),
            body: exchange.body.clone(),
        };
        let sent = crate::http::send(provider.transport.as_ref(), attempt, work)?.await;
        if let Ok(response) = &sent {
            recovery.observe(response.status);
        }
        let (failure, retry_after) = match sent {
            Ok(response) if (200..300).contains(&response.status) => {
                match read_count(provider, response, work).await {
                    Ok(count) => return Ok(count),
                    Err(failure) => (failure, None),
                }
            }
            Ok(response) => {
                let retry_after = response.retry_after;
                let failure =
                    crate::http_failure(response, provider.config.max_event_bytes, work).await;
                (failure, retry_after)
            }
            Err(failure) => (failure, None),
        };
        let Some(wait) = recovery.next(&failure, retry_after) else {
            return Err(failure);
        };
        drop(failure);
        crate::deadline::timer(provider.transport.as_ref(), wait, work)?.await?;
    }
}
