use super::{error, to_js_json};
use crate::ResolvedModel;
use acyclic_harness::model::{ModelDispatch, ModelEvent};
use acyclic_stream::BoxProviderStream;
use futures::StreamExt as _;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
    task::Poll,
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = r#"
export function providerIterator(stream, signal) {
  let pending = false;
  let closed = false;
  let listening = false;
  const dispose = () => {
    if (!closed || pending || !stream) return;
    signal?.removeEventListener('abort', abort);
    stream.free();
    stream = undefined;
  };
  const abort = () => { closed = true; stream?.cancel(); dispose(); };
  return {
    [Symbol.asyncIterator]() { return this; },
    async next() {
      if (pending) throw new Error('concurrent model iterator reads');
      if (closed) { signal?.throwIfAborted(); return { done: true }; }
      if (!listening) {
        listening = true;
        signal?.addEventListener('abort', abort, { once: true });
        if (signal?.aborted) { abort(); signal.throwIfAborted(); }
      }
      pending = true;
      try {
        const result = await stream.next();
        signal?.throwIfAborted();
        if (closed) return { done: true };
        if (result.done) closed = true;
        return result;
      } catch (error) { closed = true; throw error; }
      finally { pending = false; dispose(); }
    },
    async return() { abort(); return { done: true }; },
    async throw(error) { abort(); throw error; },
  };
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = providerIterator)]
    fn provider_iterator(stream: WasmModelStream, signal: JsValue) -> JsValue;
}

struct State {
    stream: RefCell<Option<BoxProviderStream<'static, acyclic_harness::Result<ModelEvent>>>>,
    busy: Cell<bool>,
    cancellation: crate::cancellation::Cancellation,
}

/// Stream handle owned by the generated async iterator, including pending fetch IO.
#[wasm_bindgen]
pub struct WasmModelStream(Rc<State>);

pub(super) fn iterator(
    provider: Arc<ResolvedModel>,
    prepared: Rc<crate::prepared::PreparedInput>,
    dispatch: ModelDispatch,
    signal: JsValue,
) -> JsValue {
    let stream = Box::pin(async_stream::try_stream! {
        let auth = crate::dispatch::authenticate(&provider).await?;
        let mut events = crate::dispatch::generate_encoded(&provider, &prepared.request, dispatch, prepared.generation.clone(), auth, crate::tool_names::Names::new(prepared.request.request()));
        while let Some(event) = events.next().await { yield event?; }
    });
    provider_iterator(
        WasmModelStream(Rc::new(State {
            stream: RefCell::new(Some(stream)),
            busy: Cell::new(false),
            cancellation: Default::default(),
        })),
        signal,
    )
}

#[wasm_bindgen]
impl WasmModelStream {
    pub fn cancel(&self) {
        self.0.cancellation.cancel();
        if let Ok(mut stream) = self.0.stream.try_borrow_mut() {
            stream.take();
        }
    }

    pub fn next(&self) -> js_sys::Promise {
        let state = self.0.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            if state.busy.replace(true) {
                return Err(error("concurrent model stream reads"));
            }
            let event = futures::future::poll_fn(|cx| {
                let mut stream = state.stream.borrow_mut();
                if state.cancellation.poll(cx).is_ready() {
                    stream.take();
                    return Poll::Ready(None);
                }
                match stream.as_mut() {
                    Some(stream) => stream.as_mut().poll_next(cx),
                    None => Poll::Ready(None),
                }
            })
            .await;
            state.busy.set(false);
            let result = js_sys::Object::new();
            match event {
                Some(Ok(event)) => {
                    let tool_call = matches!(event, ModelEvent::ToolCall(_));
                    let event = to_js_json(&event)?;
                    // The public facade uses callId; protocol and durable Rust DTOs use call_id.
                    if tool_call {
                        let id = js_sys::Reflect::get(&event, &"call_id".into())?;
                        js_sys::Reflect::set(&event, &"callId".into(), &id)?;
                        js_sys::Reflect::delete_property(
                            &js_sys::Object::from(event.clone()),
                            &"call_id".into(),
                        )?;
                    }
                    js_sys::Reflect::set(&result, &"done".into(), &JsValue::FALSE)?;
                    js_sys::Reflect::set(&result, &"value".into(), &event)?;
                }
                Some(Err(cause)) => {
                    state.stream.borrow_mut().take();
                    return Err(error(cause));
                }
                None => {
                    state.stream.borrow_mut().take();
                    js_sys::Reflect::set(&result, &"done".into(), &JsValue::TRUE)?;
                }
            }
            Ok(result.into())
        })
    }
}
