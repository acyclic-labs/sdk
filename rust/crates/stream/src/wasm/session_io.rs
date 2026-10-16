//! Binary platform callbacks for the canonical session client, also on Node/Bun.
use super::*;
use crate::session::{Client, runtime::PlatformExecutor, transport};
use futures::{
    Stream,
    future::{AbortHandle, Abortable},
};
use js_sys::{Function, Promise};
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Weak,
    sync::Arc,
    task::{Context, Poll},
};
use wasm_bindgen_futures::JsFuture;

struct Endpoint {
    connect: Function,
    node: Option<wire::SessionNode>,
    writing: Rc<RefCell<Weak<RefCell<Option<Writing>>>>>,
}
struct Writing {
    channel: wire::SessionChannel,
    subscription: bool,
    controller: web_sys::AbortController,
    abort: AbortHandle,
}
impl Writing {
    fn cancel(self) {
        self.controller.abort();
        self.abort.abort();
    }
}
fn owner(frame: &wire::SessionFrame) -> wire::SessionChannel {
    use wire::session_frame::Body;
    if frame.channel_id == 0
        && let Some(Body::Interest(interest)) = &frame.body
        && interest.upsert.len() == 1
        && interest.remove.is_empty()
        && let Some(channel) = interest.upsert[0].channel
    {
        return channel;
    }
    wire::SessionChannel {
        channel_id: frame.channel_id,
        generation: frame.generation,
    }
}
struct Connection {
    controller: web_sys::AbortController,
    io: JsValue,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.controller.abort();
        if let Ok(close) = method(&self.io, "close") {
            let _ = close.call0(&self.io);
        }
    }
}
fn method(object: &JsValue, name: &str) -> Result<Function, StreamError> {
    Reflect::get(object, &JsValue::from_str(name))
        .and_then(|value| value.dyn_into::<Function>())
        .map_err(|_| StreamError::Unavailable)
}
fn failure(error: JsValue) -> StreamError {
    Reflect::get(&error, &JsValue::from_str("code"))
        .ok()
        .and_then(|code| code.as_string())
        .as_deref()
        .and_then(StreamError::from_code)
        .unwrap_or(StreamError::Unavailable)
}
impl Connection {
    fn call(&self, method_name: &str, value: &JsValue) -> Result<JsFuture, StreamError> {
        let value = method(&self.io, method_name)?
            .call1(&self.io, value)
            .map_err(failure)?;
        Ok(JsFuture::from(Promise::resolve(&value)))
    }
}

// The response owns both directions. A blocked JS write never blocks reading;
// dropping the response aborts setup/read/write immediately, with no detached
// writer, shared lock, recovery state or channel queue in this adapter.
struct Responses {
    connection: Connection,
    source: transport::Requests,
    read: Option<JsFuture>,
    write: Option<Abortable<JsFuture>>,
    writing: Rc<RefCell<Option<Writing>>>,
    ended: bool,
}
impl Responses {
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<wire::SessionFrame, StreamError>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        // Bound per-poll I/O, preserving read progress beside ready controls.
        for _ in 0..2 {
            if let Some(write) = &mut self.write {
                match Pin::new(write).poll(cx) {
                    Poll::Pending => break,
                    Poll::Ready(result) => {
                        let writing = self.writing.borrow_mut().take();
                        self.write = None;
                        if let Ok(Err(error)) = result {
                            let error = failure(error);
                            if let Some(writing) = writing.filter(|writing| writing.subscription) {
                                // A rejected interest never installed its channel. Surface
                                // that local failure through the engine's terminal path;
                                // sibling interests keep their physical connection.
                                return Poll::Ready(Some(Ok(wire::SessionFrame {
                                    channel_id: writing.channel.channel_id,
                                    generation: writing.channel.generation,
                                    body: Some(wire::session_frame::Body::Closed(
                                        wire::SessionClosed {
                                            code: error.code().into(),
                                            ..Default::default()
                                        },
                                    )),
                                })));
                            }
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
            }
            match self.source.as_mut().poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(frame)) => {
                    let bytes = Uint8Array::from(frame.encode_to_vec().as_slice());
                    let controller = web_sys::AbortController::new().map_err(failure)?;
                    let signal = controller.signal();
                    let (abort, registration) = AbortHandle::new_pair();
                    *self.writing.borrow_mut() = Some(Writing {
                        channel: owner(&frame),
                        subscription: matches!(&frame.body, Some(wire::session_frame::Body::Interest(interest)) if interest.upsert.len() == 1 && interest.remove.is_empty()),
                        controller,
                        abort,
                    });
                    let value = method(&self.connection.io, "send")?.call2(
                        &self.connection.io,
                        &bytes,
                        &signal,
                    );
                    let promise = match value {
                        Ok(value) => Promise::resolve(&value),
                        Err(error) => Promise::reject(&error),
                    };
                    self.write = Some(Abortable::new(JsFuture::from(promise), registration));
                    cx.waker().wake_by_ref();
                }
            }
        }
        if self.read.is_none() {
            self.read = Some(self.connection.call("next", &JsValue::UNDEFINED)?);
        }
        match Pin::new(self.read.as_mut().ok_or(StreamError::Unavailable)?).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.read = None;
                let value = result.map_err(failure)?;
                if value.is_null() || value.is_undefined() {
                    return Poll::Ready(None);
                }
                if !value.is_instance_of::<Uint8Array>() {
                    return Poll::Ready(Some(Err(StreamError::Unavailable)));
                }
                let bytes = Uint8Array::new(&value);
                if bytes.length() as usize > MAX_COMMAND_BYTES {
                    return Poll::Ready(Some(Err(StreamError::LimitExceeded)));
                }
                Poll::Ready(Some(wire_codec::session::decode_frame(&bytes.to_vec())))
            }
        }
    }
}
impl Drop for Responses {
    fn drop(&mut self) {
        // Never hold the RefCell across a JS abort callback or a waker.
        let writing = self.writing.borrow_mut().take();
        if let Some(writing) = writing {
            writing.cancel();
        }
    }
}
impl Stream for Responses {
    type Item = Result<wire::SessionFrame, StreamError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.poll(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.ended = true;
        }
        result
    }
}
impl transport::Transport for Endpoint {
    fn cancel_channel(&self, channel: wire::SessionChannel) {
        let current = self.writing.borrow().upgrade();
        let writing = current.and_then(|current| {
            let mut slot = current.borrow_mut();
            if slot
                .as_ref()
                .is_some_and(|writing| writing.channel == channel)
            {
                slot.take()
            } else {
                None
            }
        });
        if let Some(writing) = writing {
            writing.cancel();
        }
    }
    fn session(
        &self,
        source: transport::Requests,
    ) -> crate::BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            let mut connection = Connection {
                controller: web_sys::AbortController::new().map_err(failure)?,
                io: JsValue::UNDEFINED,
            };
            let node = self.node.as_ref().map_or(JsValue::NULL, |node| {
                Uint8Array::from(node.encode_to_vec().as_slice()).into()
            });
            let opening = self
                .connect
                .call2(&JsValue::UNDEFINED, &node, &connection.controller.signal())
                .map_err(failure)?;
            connection.io = JsFuture::from(Promise::resolve(&opening))
                .await
                .map_err(failure)?;
            let writing = Rc::new(RefCell::new(None));
            *self.writing.borrow_mut() = Rc::downgrade(&writing);
            Ok(Box::pin(Responses {
                writing,
                connection,
                source,
                read: None,
                write: None,
                ended: false,
            }) as transport::Responses)
        })
    }
}
pub(super) fn client(connect: Function) -> Client {
    let home = Arc::new(Endpoint {
        connect: connect.clone(),
        node: None,
        writing: Rc::new(RefCell::new(Weak::new())),
    });
    let moved: Arc<transport::Connect> = Arc::new(move |node| {
        let endpoint: transport::Endpoint = Arc::new(Endpoint {
            connect: connect.clone(),
            node: Some(node),
            writing: Rc::new(RefCell::new(Weak::new())),
        });
        Box::pin(async move { Ok(endpoint) })
    });
    Client::with_transport(home, moved, Arc::new(PlatformExecutor))
}
