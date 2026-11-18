//! Pull-based platform I/O for the canonical authenticated session server.
use super::*;
use crate::session::{self, CallProvider, ResourceProvider};
use std::sync::Arc;

struct Input {
    next: js_sys::Function,
    abort: web_sys::AbortController,
    decoder: Option<wire_codec::session::FrameDecoder>,
    chunk: Vec<u8>,
    offset: usize,
}

impl Drop for Input {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// The account providers and limits used by a WASM session.
pub struct WasmSessionService {
    /// Account-scoped Stream operations.
    pub provider: Arc<dyn StreamProvider>,
    /// Credential verification and revocation source.
    pub authority: Arc<session::Authority>,
    /// Optional account resource observations.
    pub resources: Option<Arc<dyn ResourceProvider>>,
    /// Account-scoped call dispatch.
    pub calls: Arc<dyn CallProvider>,
    /// Node identity and negotiated session limits.
    pub spec: session::SessionSpec,
}

/// Serves canonical frames or gRPC envelopes through the same session engine.
/// `next(signal)` returns null at EOF and must cancel pending I/O on abort.
/// Closing the cursor aborts input and drops session-owned calls and follows.
pub fn serve_wasm_session(
    service: WasmSessionService,
    next: js_sys::Function,
    shutdown: Option<crate::BoxProviderFuture<'static, ()>>,
    grpc: bool,
) -> Result<WasmFollow, JsValue> {
    let input = Input {
        next,
        abort: web_sys::AbortController::new()?,
        decoder: grpc.then(wire_codec::session::FrameDecoder::grpc),
        chunk: Vec::new(),
        offset: 0,
    };
    let incoming = futures::stream::try_unfold(input, |mut input| async move {
        loop {
            if let Some(decoder) = input.decoder.as_mut()
                && let Some(frame) = read_chunk_frame(decoder, &input.chunk, &mut input.offset)?
            {
                return Ok(Some((frame, input)));
            }
            let value = input
                .next
                .call1(&JsValue::UNDEFINED, &input.abort.signal())
                .map_err(|_| StreamError::Unavailable)?;
            let value = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&value))
                .await
                .map_err(|_| StreamError::Unavailable)?;
            if value.is_null() || value.is_undefined() {
                if let Some(decoder) = &input.decoder {
                    decoder.finish()?;
                }
                return Ok(None);
            }
            let view = value
                .dyn_into::<Uint8Array>()
                .map_err(|_| StreamError::InvalidArgument)?;
            if input.decoder.is_some() {
                input.chunk = view.to_vec();
                input.offset = 0;
                continue;
            }
            if view.length() as usize > MAX_COMMAND_BYTES {
                return Err(StreamError::LimitExceeded);
            }
            let frame = wire_codec::session::decode_frame(&view.to_vec())?;
            return Ok(Some((frame, input)));
        }
    });
    let responses = session::serve_with_products(
        service.provider,
        service.authority,
        service.resources,
        service.calls,
        service.spec,
        Box::pin(incoming),
        shutdown,
    )
    .map_err(js_error)?;
    let frames = responses
        .map(move |frame| {
            frame.and_then(|frame| {
                if grpc {
                    wire_codec::session::encode_grpc_frame(frame.wire())
                } else {
                    Ok(frame.wire().encode_to_vec())
                }
            })
        })
        .boxed_local();
    Ok(WasmFollow {
        state: Rc::new(FollowState::new(frames, StreamError::Unavailable)),
    })
}
