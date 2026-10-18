//! Verified provider admission, independent of usage-calibrated planning weights.
pub(crate) mod wire;
use crate::{HttpRequest, HttpTransport as _, ResolvedModel, dispatch, projection};
use acyclic_harness::{Error, NativeCustody, NativeWork, Result, context::ModelTokenCount};
use futures::StreamExt as _;

struct Body {
    bytes: Vec<u8>,
    _custody: NativeCustody,
}
impl AsRef<[u8]> for Body {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn boundary(work: NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    crate::invalid(reason)
}

pub(crate) async fn count(
    provider: &ResolvedModel,
    request: &acyclic_harness::model::PreparedModelRequest,
    work: NativeWork<'_>,
) -> Result<(ModelTokenCount, NativeCustody)> {
    if request.request().model != provider.model {
        return Err(boundary(
            work,
            "request model differs from resolved selection",
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    if provider.codex_login.is_some() {
        let custody = work
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
        let result = provider
            .codex_count
            .count(request)
            .and_then(|tokens| projection::plan(provider, request, tokens));
        let mut custody = custody;
        let result = match custody.as_mut() {
            Some(hold) => crate::native::diagnostic(result, hold),
            None => result,
        };
        return Ok((result?, custody));
    }
    let spec = provider.config.token_counter.as_ref().ok_or_else(|| {
        let reason = "configure a verified input token counter for this endpoint";
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_some() {
            return Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unsupported,
                reason,
            };
        }
        Error::Unsupported(reason.into())
    })?;
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
            let (mut bytes, nodes) = crate::native::shape(request.bytes());
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
        .map(acyclic_harness::filesystem::PreparationIo::from_native_work)
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
        let body = wire::body(&generation, &provider.model.name, spec.adapter);
        let bytes =
            serde_json::to_vec(&body).map_err(|_| crate::invalid("token count encoding failed"))?;
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
    let (url, headers, bytes, count) = prepared?;
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(hold) = admission.as_mut() {
        let retained = std::mem::size_of::<HttpRequest>()
            + std::mem::size_of::<Body>()
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
    let admission = None;
    let body = bytes::Bytes::from_owner(Body {
        bytes,
        _custody: admission,
    });
    let exchange = HttpRequest { url, headers, body };
    send_count(provider, exchange, count, count_custody, work).await
}

pub(crate) async fn send_count(
    provider: &ResolvedModel,
    exchange: HttpRequest,
    mut count: ModelTokenCount,
    count_custody: NativeCustody,
    work: NativeWork<'_>,
) -> Result<(ModelTokenCount, NativeCustody)> {
    count.input_tokens = exchange_count(provider, exchange, work).await?;
    Ok((count, count_custody))
}

async fn read_count(
    provider: &ResolvedModel,
    mut response: crate::HttpResponse,
    work: NativeWork<'_>,
) -> Result<u64> {
    let spec = provider
        .config
        .token_counter
        .as_ref()
        .ok_or_else(|| crate::invalid("missing token counter"))?;
    let maximum = provider.config.max_event_bytes;
    #[cfg(not(target_arch = "wasm32"))]
    let mut response_custody = work
        .map(|work| work.claim(64, 1, acyclic_fs::WorkCounters::UNCHARGED))
        .transpose()?;
    let mut bytes = Vec::new();
    let mut chunks = 0usize;
    while let Some(chunk) = response.body.next().await {
        let chunk = chunk?;
        chunks = chunks
            .checked_add(1)
            .filter(|count| *count <= maximum)
            .ok_or_else(|| boundary(work, "token count response exceeds item limit"))?;
        let next = bytes
            .len()
            .checked_add(chunk.len())
            .filter(|n| *n <= maximum)
            .ok_or_else(|| boundary(work, "token count response exceeds byte limit"))?;
        if next > bytes.capacity() {
            let capacity = next
                .checked_next_power_of_two()
                .unwrap_or(maximum)
                .min(maximum);
            #[cfg(not(target_arch = "wasm32"))]
            if let (Some(work), Some(hold)) = (work, response_custody.as_mut()) {
                let mut growth = work.claim(
                    (capacity - bytes.capacity()) as u64,
                    1,
                    acyclic_fs::WorkCounters {
                        bytes_copied: bytes.len() as u64,
                        ..Default::default()
                    },
                )?;
                hold.absorb(&mut growth)?;
            }
            bytes.reserve_exact(capacity - bytes.len());
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                bytes_copied: chunk.len() as u64,
                items_examined: 1,
                ..Default::default()
            })?;
        }
        bytes.extend_from_slice(&chunk);
    }
    let verified = wire::response(&bytes, spec);
    #[cfg(not(target_arch = "wasm32"))]
    let verified = match response_custody.as_mut() {
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
    work: NativeWork<'_>,
) -> Result<u64> {
    let mut redelivery =
        crate::retry::Redelivery::for_request(provider.config.retry, &exchange.body);
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
        #[cfg(not(target_arch = "wasm32"))]
        let sent = match work {
            Some(work) => {
                provider
                    .transport
                    .send_with_native_work(attempt, work)?
                    .await
            }
            None => provider.transport.send(attempt).await,
        };
        #[cfg(target_arch = "wasm32")]
        let sent = {
            let _ = work;
            provider.transport.send(attempt).await
        };
        let (failure, retry_after) = match sent {
            Ok(response) if (200..300).contains(&response.status) => {
                match read_count(provider, response, work).await {
                    Ok(count) => return Ok(count),
                    Err(failure) => (failure, None),
                }
            }
            Ok(response) => {
                let retry_after = response.retry_after;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = work {
                    // The bounded prefix, its lossy decoding and the excerpt.
                    let mut hold = work.claim(
                        4 * crate::EXCERPT_BYTES as u64 + 64,
                        4,
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )?;
                    let failure = crate::http_failure(response).await;
                    match crate::native::diagnostic::<()>(Err(failure), &mut hold) {
                        Err(failure) => (failure, retry_after),
                        Ok(()) => return Err(boundary(Some(work), "token count refusal lost")),
                    }
                } else {
                    (crate::http_failure(response).await, retry_after)
                }
                #[cfg(target_arch = "wasm32")]
                (crate::http_failure(response).await, retry_after)
            }
            Err(failure) => (failure, None),
        };
        let Some(wait) = redelivery.next(&failure, retry_after, false) else {
            return Err(failure);
        };
        drop(failure);
        let delay = provider.transport.delay(wait);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            let cancelled = work.cancellation_wait()?;
            futures::pin_mut!(cancelled, delay);
            if let futures::future::Either::Left(_) =
                futures::future::select(cancelled, delay).await
            {
                return Err(Error::from(
                    acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ));
            }
            continue;
        }
        delay.await;
    }
}
