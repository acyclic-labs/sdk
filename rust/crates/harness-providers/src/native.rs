//! Original-ledger admission for the shared wire projections and parsers.
use crate::{Adapter, ResolvedModel, dispatch, sse, stream};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_harness::{
    Error, NativeFutureBundle, Result,
    model::{ModelDispatch, ModelEvent, ModelProvider, NativeModelStream, PreparedModelRequest},
    wire::ErrorCode,
};
use futures::StreamExt as _;

#[cfg(test)]
#[path = "native_accounting_tests.rs"]
mod accounting_tests;

struct Parser<'work> {
    state: stream::State<'work>,
    allocation: NativeAllocation,
}

struct RequestBody {
    bytes: Vec<u8>,
    _custody: NativeAllocation,
}

impl AsRef<[u8]> for RequestBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

async fn send(
    provider: &ResolvedModel,
    request: &PreparedModelRequest,
    names: &crate::tool_names::Names<'_>,
    dispatch_id: ModelDispatch,
    auth: (Option<&str>, Option<&str>),
    work: &NativeMaterializationWork,
) -> Result<(crate::HttpResponse, usize)> {
    let (key, account) = auth;
    let (bytes, nodes) = shape(request.bytes());
    let mut allocation = reserve(
        work,
        bytes,
        nodes,
        key.map_or(0, |key| key.len() as u64) + account.map_or(0, |account| account.len() as u64),
    )?;
    let (url, headers, body) = diagnostic(
        dispatch::request(provider, request, names, dispatch_id, key, account),
        &mut allocation,
    )?;
    let input_bytes = body.len();
    let retained = (std::mem::size_of::<crate::HttpRequest>()
        + std::mem::size_of::<RequestBody>()
        + url.capacity()
        + body.capacity()
        + headers.len() * std::mem::size_of::<(String, String)>()
        + headers
            .iter()
            .map(|(name, value)| name.capacity() + value.capacity())
            .sum::<usize>()) as u64;
    retain(&mut allocation, retained)?;
    let body = bytes::Bytes::from_owner(RequestBody {
        bytes: body,
        _custody: allocation,
    });
    let response = provider
        .transport
        .send_with_native_work(crate::HttpRequest { url, headers, body }, work)?
        .await?;
    Ok((response, input_bytes))
}

fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: ErrorCode::Invalid,
        reason,
    }
}

pub(crate) fn diagnostic<T>(result: Result<T>, allocation: &mut NativeAllocation) -> Result<T> {
    result.map_err(|error| {
        let (code, message) = match error {
            Error::Invalid(message) => (ErrorCode::Invalid, message),
            Error::Unauthorized(message) => (ErrorCode::Unauthorized, message),
            Error::Storage(message) => (ErrorCode::Storage, message),
            error => return error,
        };
        match allocation.split_off(message.capacity() as u64) {
            Ok(custody) => Error::NativeDiagnostic {
                code,
                message,
                custody,
            },
            Err(cause) => {
                drop(message);
                cause.into()
            }
        }
    })
}

// Count structural tokens without constructing a second JSON graph. Quoted
// punctuation and escaped quotes are payload, not allocation-bearing nodes.
pub(crate) fn shape(bytes: &[u8]) -> (u64, u64) {
    let mut nodes = 1;
    let mut quoted = false;
    let mut escape = false;
    for &byte in bytes {
        if escape {
            escape = false;
        } else if quoted && byte == b'\\' {
            escape = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && matches!(byte, b'{' | b'[' | b',' | b':') {
            nodes += 1;
        }
    }
    (bytes.len() as u64, nodes)
}

// Admission includes the temporary decoded graph, copies retained by the
// protocol state, geometric buffer growth and SDK event slots. It precedes the
// producer; only surviving capacity is transferred to the executor afterwards.
pub(crate) fn reserve(
    work: &NativeMaterializationWork,
    bytes: u64,
    nodes: u64,
    prior: u64,
) -> Result<NativeAllocation> {
    let capacity = bytes
        .checked_mul(8)
        .and_then(|n| {
            nodes
                .checked_mul(1024)
                .and_then(|slots| n.checked_add(slots))
        })
        .and_then(|n| prior.checked_mul(2).and_then(|prior| n.checked_add(prior)))
        .and_then(|n| n.checked_add(8192))
        .ok_or_else(|| invalid("provider allocation bound overflow"))?;
    let operations = nodes
        .checked_mul(16)
        .and_then(|n| n.checked_add(128))
        .ok_or_else(|| invalid("provider allocation operation bound overflow"))?;
    Ok(work.claim(
        capacity,
        operations,
        WorkCounters {
            bytes_copied: bytes.saturating_mul(8),
            items_examined: nodes,
            ..Default::default()
        },
    )?)
}

pub(crate) fn retain(allocation: &mut NativeAllocation, bytes: u64) -> Result<()> {
    if bytes > allocation.bytes() {
        return Err(invalid("provider exceeded admitted allocation"));
    }
    allocation.resize(bytes)?;
    Ok(())
}

pub(crate) fn generate<'a>(
    provider: &'a ResolvedModel,
    request: &'a PreparedModelRequest,
    dispatch_id: ModelDispatch,
    work: &'a NativeMaterializationWork,
) -> Result<NativeFutureBundle<'a, NativeModelStream<'a>>> {
    work.remaining()?;
    provider.context_capacity_with_native_work(&request.request().model, work)?;
    if request.request_digest() != dispatch_id.request_digest {
        return Err(invalid(
            "model dispatch digest differs from prepared request",
        ));
    }
    let cancelled = work.cancellation_wait()?;
    NativeFutureBundle::new(work, || async move {
        let exchange = async move {
            let (names, name_custody) = crate::tool_names::Names::native(request.request(), work)?;
            let auth = dispatch::authenticate(provider).await?;
            work.remaining()?;
            let (response, input_bytes) = send(
                provider,
                request,
                &names,
                dispatch_id,
                (auth.0.as_deref(), auth.1.as_deref()),
                work,
            )
            .await?;
            let origin_bytes =
                crate::stream::family(provider.config.adapter).len() + provider.model.name.len();
            let origin_custody = work.claim(origin_bytes as u64, 2, WorkCounters::UNCHARGED)?;
            let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
            drop(auth);
            if !(200..300).contains(&response.status) {
                let mut allocation = work.claim(64, 1, WorkCounters::UNCHARGED)?;
                return diagnostic(Err(crate::http_error(response.status)), &mut allocation);
            }
            let maximum = provider.config.max_event_bytes;
            let adapter = provider.config.adapter;
            NativeModelStream::new(work, || {
                async_stream::try_stream! {
                    let _name_custody = name_custody;
                    let mut parser = Parser { state: stream::State::native(maximum, work), allocation: work.claim(std::mem::size_of::<stream::State>() as u64, 0, WorkCounters::UNCHARGED)? };
                    parser.allocation.absorb(&mut { origin_custody })?;
                    parser.state.set_origin(Some(origin))?;
                    let frames = sse::frames(response.body, maximum, sse::Decoder::native(work)?);
                    let cancelled = work.cancellation_wait()?;
                    futures::pin_mut!(frames, cancelled);
                    loop {
                        let frame = match futures::future::select(cancelled.as_mut(), frames.next()).await {
                            futures::future::Either::Left(_) => Err(Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?,
                            futures::future::Either::Right((frame, _)) => frame,
                        };
                        let Some(frame) = frame else { break; };
                        work.remaining()?;
                        let frame = frame?;
                        let (bytes, nodes) = shape(frame.data.as_bytes());
                        let mut admission = reserve(work, bytes, nodes, parser.allocation.bytes())?;
                        parser.allocation.absorb(&mut admission)?;
                        work.charge(WorkCounters { source_bytes_read: bytes, ..WorkCounters::UNCHARGED })?;
                        let events = diagnostic(stream::parse(&mut parser.state, adapter, &frame.data), &mut parser.allocation)?;
                        let terminal = parser.state.terminal && (frame.data == "[DONE]" || matches!(adapter, Adapter::Anthropic | Adapter::Responses));
                        drop(frame);
                        let slots = (events.capacity() * std::mem::size_of::<ModelEvent>()) as u64;
                        let retained = events.iter().try_fold(parser.state.logical_owned_bytes()?.checked_add(slots).ok_or_else(|| invalid("provider retained capacity overflow"))?, |bytes, event| {
                            bytes.checked_add(event.logical_owned_bytes()?).ok_or_else(|| invalid("provider retained capacity overflow"))
                        })?;
                        retain(&mut parser.allocation, retained)?;
                        for event in events {
                            let custody = parser.allocation.split_off(event.logical_owned_bytes()?)?;
                            yield (event, custody);
                        }
                        parser.allocation.release(slots)?;
                        if terminal { break; }
                    }
                    let (bytes, nodes) = parser.state.arguments().map(|args| shape(args.as_bytes())).try_fold((0u64, 0u64), |(bytes, nodes), (b, n)| {
                        Ok::<_, Error>((bytes.checked_add(b).ok_or_else(|| invalid("tool batch size overflow"))?, nodes.checked_add(n).ok_or_else(|| invalid("tool batch size overflow"))?))
                    })?;
                    let (reasoning_bytes, reasoning_nodes) = parser.state.reasoning_shape()?;
                    let mut admission = reserve(work, bytes.checked_add(reasoning_bytes).ok_or_else(|| invalid("reasoning size overflow"))?, nodes.checked_add(reasoning_nodes).ok_or_else(|| invalid("reasoning size overflow"))?, parser.allocation.bytes())?;
                    parser.allocation.absorb(&mut admission)?;
                    let restored = parser.state.restore_names(&names, |length| {
                        let mut custody = work.claim(length as u64, 1, WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED })?;
                        parser.allocation.absorb(&mut custody)?;
                        Ok(())
                    });
                    diagnostic(restored, &mut parser.allocation)?;
                    let Parser { state, mut allocation } = parser;
                    let events = diagnostic(stream::finish(state, adapter, dispatch_id), &mut allocation)?;
                    let slots = (events.capacity() * std::mem::size_of::<ModelEvent>()) as u64;
                    let retained = events.iter().try_fold(slots, |bytes, event| {
                        bytes.checked_add(event.logical_owned_bytes()?).ok_or_else(|| invalid("provider retained capacity overflow"))
                    })?;
                    retain(&mut allocation, retained)?;
                    for event in events {
                        if let ModelEvent::Completed { metadata } = &event { provider.estimator.observe(input_bytes, metadata); }
                        let custody = allocation.split_off(event.logical_owned_bytes()?)?;
                        yield (event, custody);
                    }
                }
            })
        };
        futures::pin_mut!(exchange, cancelled);
        match futures::future::select(cancelled, exchange).await {
            futures::future::Either::Left(_) => {
                Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into())
            }
            futures::future::Either::Right((result, _)) => result,
        }
    })
}
