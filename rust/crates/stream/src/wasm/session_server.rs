//! Pull-based platform I/O for the canonical authenticated session server.
use super::*;
use crate::session::{self, CallProvider, ResourceProvider};
use std::sync::Arc;

struct Input {
    next: js_sys::Function,
    abort: web_sys::AbortController,
}

impl Drop for Input {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// Serves canonical protobuf frames with the existing session engine and cursor.
/// `next(signal)` pulls one frame, returns null at EOF, and must cancel pending
/// platform I/O when its signal aborts. Closing the returned cursor aborts input
/// and drops all session-owned calls and follows; no writer or queue is detached.
pub fn serve_wasm_session(
    provider: Arc<dyn StreamProvider>,
    authority: Arc<session::Authority>,
    resources: Option<Arc<dyn ResourceProvider>>,
    calls: Arc<dyn CallProvider>,
    node: String,
    limits: session::Limits,
    next: js_sys::Function,
    shutdown: Option<crate::BoxProviderFuture<'static, ()>>,
) -> Result<WasmFollow, JsValue> {
    let input = Input {
        next,
        abort: web_sys::AbortController::new()?,
    };
    let incoming = futures::stream::try_unfold(input, |input| async move {
        let value = input
            .next
            .call1(&JsValue::UNDEFINED, &input.abort.signal())
            .map_err(|_| StreamError::Unavailable)?;
        let value = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&value))
            .await
            .map_err(|_| StreamError::Unavailable)?;
        if value.is_null() || value.is_undefined() {
            return Ok(None);
        }
        let view = value
            .dyn_into::<Uint8Array>()
            .map_err(|_| StreamError::InvalidArgument)?;
        if view.length() as usize > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        let frame = wire_codec::session::decode_frame(&view.to_vec())?;
        Ok(Some((frame, input)))
    });
    let responses = session::serve_with_products(
        provider,
        authority,
        resources,
        calls,
        node,
        limits,
        Box::pin(incoming),
        shutdown,
    )
    .map_err(js_error)?;
    let frames = responses
        .map(|frame| frame.map(|frame| frame.encode_to_vec()))
        .boxed_local();
    Ok(WasmFollow {
        state: Rc::new(FollowState::new(frames)),
    })
}
