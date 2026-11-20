//! Original-ledger admission for the shared wire projections and parsers.
use crate::allocation::{
    diagnostic, invalid, model_diagnostic, reserve, reserve_request, retain, shape,
};
use crate::{Adapter, ResolvedModel, dispatch, sse};
use acyclic_harness_core::{
    Error, Result,
    model::{ModelDispatch, ModelEvent, ModelProvider, NativeModelStream, PreparedModelRequest},
};
use acyclic_harness_provider_response::stream;
use futures::StreamExt as _;
use {
    acyclic_fs::{NativeAllocation, NativeMaterializationWork},
    acyclic_fs_work::performance::WorkCounters,
};

struct Parser<'work> {
    state: stream::State<'work>,
    allocation: NativeAllocation,
}

async fn encode(
    provider: &ResolvedModel,
    request: &PreparedModelRequest,
    names: &crate::tool_names::Names<'_>,
    auth: (Option<&str>, Option<&str>),
    work: &NativeMaterializationWork,
) -> Result<(
    bytes::Bytes,
    Option<acyclic_harness_core::executor::ModelCacheObservation>,
)> {
    let (key, account) = auth;
    let mut allocation = reserve_request(provider.files.maximum_bytes, request, auth, work)?;
    let io = acyclic_harness_core::filesystem::PreparationIo::from_native_work(work)?;
    let origin = provider.reasoning_origin(key, account);
    let body = diagnostic(
        dispatch::encode(provider, request, names, Some(&io), &origin).await,
        &mut allocation,
    )?;
    retain(&mut allocation, body.bytes.capacity() as u64)?;
    Ok((
        crate::http::owned_bytes::owned(body.bytes, allocation)?,
        body.cache,
    ))
}

pub(crate) fn generate<'a>(
    provider: &'a ResolvedModel,
    request: &'a PreparedModelRequest,
    dispatch_id: ModelDispatch,
    work: &'a NativeMaterializationWork,
) -> Result<acyclic_fs::NativeFutureBundle<'a, NativeModelStream<'a>, acyclic_harness_core::Error>>
{
    work.remaining()?;
    provider.context_capacity_with_native_work(&request.request().model, work)?;
    if request.request_digest() != dispatch_id.request_digest {
        return Err(invalid(
            "model dispatch digest differs from prepared request",
        ));
    }
    let cancelled = work.cancellation_wait()?;
    acyclic_fs::NativeFutureBundle::new(work, || async move {
        let exchange = async move {
            let current = provider.current_credentials()?;
            let provider = &current;
            let (names, name_custody) =
                crate::tool_names::Names::funded(request.request(), Some(work))?;
            let mut auth_custody = work.claim(0, 0, WorkCounters::UNCHARGED)?;
            let auth = dispatch::authenticate(provider, Some(&mut auth_custody)).await?;
            let mut auth = (
                auth.0.map(|key| std::borrow::Cow::Owned(key.into_owned())),
                auth.1,
            );
            let (body, cache) = encode(
                provider,
                request,
                &names,
                (auth.0.as_deref(), auth.1.as_deref()),
                work,
            )
            .await?;
            let input_bytes = body.len();
            if cache.is_some() {
                work.charge(WorkCounters {
                    bytes_hashed: crate::projection::cache::hashed_bytes(provider, input_bytes),
                    ..WorkCounters::UNCHARGED
                })?;
            } else {
                work.remaining()?;
            }
            NativeModelStream::new(work, || {
                async_stream::try_stream! {
                    let provider = &current;
                    let _name_custody = name_custody;
                    let mut attempt = dispatch::GenerationAttempt::new(provider.config.retry, dispatch_id);
                    loop {
                    let (response_body, traces) = {
                        let exchange = attempt.exchange(provider, &body, dispatch_id, &mut auth,
                            Some(&mut auth_custody), Some(work));
                        let mut exchange = core::pin::pin!(exchange);
                        loop {
                            match exchange.next().await {
                                Some(Ok(dispatch::Exchange::Observation(event))) => {
                                    let custody = work.claim(event.logical_owned_bytes()?, 1, WorkCounters::UNCHARGED)?;
                                    yield (event, custody);
                                }
                                Some(Ok(dispatch::Exchange::Accepted(response, traces))) => break (response, traces),
                                Some(Err(error)) => Err(error)?,
                                None => Err(crate::invalid("model exchange ended without an accepted response"))?,
                            }
                        }
                    };
                    let retry_after = response_body.retry_after;
                    let mut traces = traces.into_iter();
                    let mut traces_yielded = false;
                    let origin_bytes = stream::family(provider.config.adapter).len() + provider.model.name.len();
                    let origin_custody = work.claim(origin_bytes as u64, 2, WorkCounters::UNCHARGED)?;
                    let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
                    let cache = cache.map(|cache| acyclic_harness_core::executor::ModelCacheObservation {
                        credential_scope: origin.credential_scope,
                        ..cache
                    });
                    let events = parse_response(provider, request, &names, dispatch_id, work, response_body,
                        (input_bytes, cache), origin, origin_custody);
                    let mut failure = None;
                    {
                        let mut events = core::pin::pin!(events);
                        while let Some(event) = events.next().await {
                            match event {
                                Ok((event, custody)) => {
                                    if matches!(event, ModelEvent::Completed { .. }) && !traces_yielded {
                                        for record in traces.by_ref() {
                                            let event = ModelEvent::Trace { record };
                                            let custody = work.claim(event.logical_owned_bytes()?, 1, WorkCounters::UNCHARGED)?;
                                            yield (event, custody);
                                        }
                                        traces_yielded = true;
                                    }
                                    // The provider has finished serving this request.
                                    if matches!(event, ModelEvent::Completed { .. }) { attempt.held = None; }
                                    yield (event, custody)
                                }
                                Err(error) => {failure = Some(error); break;}
                            }
                        }
                    }
                    attempt.held = None;
                    let Some(error) = failure else {break;};
                    if error.code() == acyclic_harness_core::wire::ErrorCode::Unavailable {
                        let event = ModelEvent::Restarted;
                        let custody = work.claim(event.logical_owned_bytes()?, 1, WorkCounters::UNCHARGED)?;
                        yield (event, custody);
                    }
                    {
                        let recovery = attempt.recover(provider, &mut auth, error, retry_after, Some(&mut auth_custody), Some(work));
                        let mut recovery = core::pin::pin!(recovery);
                        while let Some(event) = recovery.next().await {
                            let event = event?;
                            let custody = work.claim(event.logical_owned_bytes()?, 1, WorkCounters::UNCHARGED)?;
                            yield (event, custody);
                        }
                    }
                    }
                }
            }).map(|stream| stream.with_delivery(acyclic_harness_core::model::ModelDelivery::Live))
        };
        let exchange = core::pin::pin!(exchange);
        let cancelled = core::pin::pin!(cancelled);
        match futures::future::select(cancelled, exchange).await {
            futures::future::Either::Left(_) => {
                Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into())
            }
            futures::future::Either::Right((result, _)) => result,
        }
    })
}

fn parse_response<'a>(
    provider: &'a ResolvedModel,
    request: &'a PreparedModelRequest,
    names: &'a crate::tool_names::Names<'a>,
    dispatch_id: ModelDispatch,
    work: &'a NativeMaterializationWork,
    response: crate::HttpResponse,
    (input_bytes, cache): (
        usize,
        Option<acyclic_harness_core::executor::ModelCacheObservation>,
    ),
    origin: acyclic_harness_core::model::ReasoningOrigin,
    origin_custody: NativeAllocation,
) -> impl futures::Stream<Item = Result<(ModelEvent, NativeAllocation)>> + Send + 'a {
    let maximum = provider.config.max_event_bytes;
    let adapter = provider.config.adapter;
    async_stream::try_stream! {
            let mut parser = Parser { state: stream::State::native(maximum, work), allocation: work.claim(std::mem::size_of::<stream::State>() as u64, 0, WorkCounters::UNCHARGED)? };
            parser.allocation.absorb(&mut { origin_custody })?;
            parser.state.set_origin(Some(origin))?;
            if let Some(id) = response.request_id.as_ref().and_then(|value| value.to_str().ok()) {
                let mut custody = reserve(work, id.len() as u64 + 64, 2, parser.allocation.bytes())?;
                parser.allocation.absorb(&mut custody)?;
                parser.state.request_id(Some(id))?;
                retain(&mut parser.allocation, parser.state.logical_owned_bytes()?)?;
            }
            drop(response.request_id);
            let frames = sse::frames(response.body, maximum, sse::decoder(adapter, sse::Decoder::native(work)?));
            let mut frames = core::pin::pin!(frames);
            while let Some(frame) = frames.next().await {
                work.remaining()?;
                let frame = frame?;
                let (bytes, nodes) = shape(frame.data().as_bytes());
                let mut admission = reserve(work, bytes, nodes, parser.allocation.bytes())?;
                parser.allocation.absorb(&mut admission)?;
                let events = model_diagnostic(stream::parse(&mut parser.state, adapter, frame.data()), &mut parser.allocation)?;
                let terminal = parser.state.is_terminal() && (frame.data() == "[DONE]" || matches!(adapter, Adapter::Anthropic | Adapter::Responses));
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
            let restored = parser.state.restore_names(|wire| names.canonical(wire), |length| {
                let mut custody = work.claim(length as u64, 1, WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED })?;
                parser.allocation.absorb(&mut custody)?;
                Ok(())
            });
            diagnostic(restored, &mut parser.allocation)?;
            let Parser { state, mut allocation } = parser;
            let mut events = model_diagnostic(stream::finish(state, adapter, dispatch_id), &mut allocation)?;
            if acyclic_harness_core::model::RESEARCH_CAPTURE_ENABLED {
                let metadata = match events.last_mut() {
                    Some(ModelEvent::Completed { metadata }) => metadata,
                    _ => Err(invalid("model completion omitted its terminal observation"))?,
                };
                diagnostic(crate::projection::cache::attach(metadata, cache), &mut allocation)?;
            }
            let slots = (events.capacity() * std::mem::size_of::<ModelEvent>()) as u64;
            let retained = events.iter().try_fold(slots, |bytes, event| {
                bytes.checked_add(event.logical_owned_bytes()?).ok_or_else(|| invalid("provider retained capacity overflow"))
            })?;
            retain(&mut allocation, retained)?;
            for event in events {
                if let ModelEvent::Completed { metadata } = &event { work.charge(WorkCounters { bytes_encoded: request.bytes().len() as u64, items_examined: request.request().messages.len() as u64, ..WorkCounters::UNCHARGED })?;
                    provider.observe(request, input_bytes, metadata)?; }
                let custody = allocation.split_off(event.logical_owned_bytes()?)?;
                yield (event, custody);
            }
    }
}
