//! Drives admitted response frames through the provider event parser.

use crate::{Adapter, sse};
use acyclic_harness_core::{
    Result,
    model::{ModelDispatch, ModelEvent},
};
use acyclic_harness_provider_response::stream::{State, finish, parse};
use acyclic_stream::BoxProviderStream;
use bytes::Bytes;
use futures::StreamExt as _;

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
/// Drives BODY through the parser with no tool-name restoration.
pub fn events<'a>(
    adapter: Adapter,
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: impl Into<Option<usize>>,
    dispatch: ModelDispatch,
) -> BoxProviderStream<'a, Result<ModelEvent>> {
    Box::pin(events_named(
        adapter, body, maximum, dispatch, None, None, None,
    ))
}

/// Drives BODY through the parser, restoring NAMES and attributing ORIGIN and REQUEST_ID.
pub fn events_named<'a>(
    adapter: Adapter,
    body: BoxProviderStream<'a, Result<Bytes>>,
    maximum: impl Into<Option<usize>>,
    dispatch: ModelDispatch,
    names: Option<&'a crate::tool_names::Names<'a>>,
    origin: Option<acyclic_harness_core::model::ReasoningOrigin>,
    request_id: Option<::http::HeaderValue>,
) -> impl futures::Stream<Item = Result<ModelEvent>> + acyclic_stream::ProviderTask + 'a {
    let maximum = maximum.into();
    async_stream::try_stream! {
        let input = sse::frames(body, maximum, sse::decoder(adapter, sse::Decoder::default()));
        let mut input = core::pin::pin!(input);
        let mut state = State::new(maximum);
        state.set_origin(origin)?;
        state.request_id(request_id.as_ref().and_then(|value| value.to_str().ok()))?;
        while let Some(data) = input.next().await {
            let data = data?;
            let data = data.data();
            let deltas = parse(&mut state, adapter, data)?;
            for event in deltas { yield event; }
            if state.is_terminal() && (data == "[DONE]" || matches!(adapter, Adapter::Anthropic | Adapter::Responses)) { break; }
        }
        if let Some(names) = names { state.restore_names(|wire| names.canonical(wire), |_| Ok(()))?; }
        let events = finish(state, adapter, dispatch)?;
        for event in events { yield event; }
    }
}
