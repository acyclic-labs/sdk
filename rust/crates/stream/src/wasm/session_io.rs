//! Binary platform callbacks for the canonical session client, also on Node/Bun.
use super::*;
use crate::runtime::PlatformExecutor;
use crate::session::{Client, Frame, transport};
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
    let interest = match &frame.body {
        Some(Body::Hello(hello)) => hello.interest.as_ref(),
        Some(Body::Interest(interest)) => Some(interest),
        _ => None,
    };
    if frame.channel_id == 0
        && let Some(interest) = interest
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
fn failure(error: JsValue) -> transport::Failure {
    let transient = Reflect::get(&error, &JsValue::from_str("transient"))
        .ok()
        .and_then(|value| value.as_bool())
        == Some(true);
    let error = match Reflect::get(&error, &"refusal".into()) {
        Ok(refusal) if refusal.is_undefined() => Reflect::get(&error, &"code".into())
            .ok()
            .and_then(|code| code.as_string())
            .as_deref()
            .and_then(StreamError::from_code)
            .unwrap_or(StreamError::Unavailable),
        Ok(refusal) => acyclic_auth::refusal::javascript::parts(&refusal, |view| {
            crate::javascript_bytes::snapshot(view, |_| Ok(())).map(|(bytes, ())| bytes)
        })
        .map_or(StreamError::Unavailable, wire_codec::status::status_owned),
        Err(_) => StreamError::Unavailable,
    };
    transport::Failure { error, transient }
}
impl Connection {
    fn call(&self, method_name: &str, value: &JsValue) -> Result<JsFuture, transport::Failure> {
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
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame, transport::Failure>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        // Bound per-poll I/O, preserving read progress beside ready controls.
        for _ in 0..2 {
            if let Some(write) = &mut self.write {
                match Pin::new(write).poll(cx) {
                    Poll::Pending => break,
                    Poll::Ready(result) => {
                        self.writing.borrow_mut().take();
                        self.write = None;
                        if let Ok(Err(error)) = result {
                            return Poll::Ready(Some(Err(failure(error))));
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
                    return Poll::Ready(Some(Err(StreamError::InvalidResponse.into())));
                }
                let bytes = Uint8Array::new(&value);
                if bytes.length() as usize > MAX_COMMAND_BYTES {
                    return Poll::Ready(Some(Err(StreamError::LimitExceeded.into())));
                }
                Poll::Ready(Some(
                    wire_codec::session::decode_frame(&bytes.to_vec())
                        .map(Into::into)
                        .map_err(Into::into),
                ))
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
    type Item = Result<crate::session::Frame, transport::Failure>;
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
    ) -> crate::BoxProviderFuture<
        '_,
        Result<transport::Responses<transport::Failure>, transport::Failure>,
    > {
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
            }) as transport::Responses<transport::Failure>)
        })
    }
}
pub(super) fn client(connect: Function, scope: Option<crate::RevisionScope>) -> Client {
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
    let revisions = Arc::default();
    let revisions = match scope {
        Some(scope) => crate::RevisionTracker::scoped(&revisions, scope),
        None => revisions,
    };
    Client::tracking(home, moved, Arc::new(PlatformExecutor), revisions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::transport::Transport as _;

    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn javascript_transport_failures_preserve_interruption_disposition() {
        // Derive adversarial diagnostics from the production status writer.
        // These are not captures of a deployed member's attribution contract.
        let canonical = wire_codec::status::error_status(&StreamError::Unavailable);
        let mut metadata = canonical.metadata().clone();
        metadata.append("acyclic-original", canonical.message().parse().unwrap());
        metadata.append("acyclic-original", canonical.message().parse().unwrap());
        let status = tonic::Status::with_details_and_metadata(
            tonic::Code::Unknown,
            canonical.message(),
            canonical.message().as_bytes().to_vec().into(),
            metadata,
        );
        let reported = wire_codec::status::status(&status);
        for write in [false, true] {
            for (error, transient) in [
                (StreamError::Unavailable, true),
                (StreamError::Unavailable, false),
                (StreamError::AccessDenied, false),
                (StreamError::InvalidResponse, false),
                (reported.clone(), true),
                (reported.clone(), false),
            ] {
                let original = super::super::js_error(error.clone());
                Reflect::set(&original, &"transient".into(), &transient.into()).unwrap();
                let io = Object::new();
                let next = Closure::<dyn Fn() -> Promise>::new({
                    let original = original.clone();
                    move || {
                        if write {
                            Promise::new(&mut |_, _| {})
                        } else {
                            Promise::reject(&original)
                        }
                    }
                });
                let send = Closure::<dyn Fn() -> Promise>::new(move || Promise::reject(&original));
                let close = Closure::<dyn Fn()>::new(|| {});
                for (name, method) in [
                    ("next", next.as_ref()),
                    ("send", send.as_ref()),
                    ("close", close.as_ref()),
                ] {
                    Reflect::set(&io, &name.into(), method).unwrap();
                }
                let connect = Closure::<dyn Fn() -> JsValue>::new(move || io.clone().into());
                let endpoint = Endpoint {
                    connect: connect.as_ref().unchecked_ref::<Function>().clone(),
                    node: None,
                    writing: Rc::new(RefCell::new(Weak::new())),
                };
                let source: transport::Requests = if write {
                    Box::pin(
                        futures::stream::once(async {
                            wire::SessionFrame {
                                body: Some(wire::session_frame::Body::Interest(
                                    wire::SessionInterest {
                                        upsert: vec![wire::SessionSubscription {
                                            channel: Some(wire::SessionChannel {
                                                channel_id: 1,
                                                generation: 1,
                                            }),
                                            ..Default::default()
                                        }],
                                        ..Default::default()
                                    },
                                )),
                                ..Default::default()
                            }
                        })
                        .chain(futures::stream::pending()),
                    )
                } else {
                    Box::pin(futures::stream::pending())
                };
                let mut responses = endpoint.session(source).await.unwrap();
                match responses
                    .next()
                    .await
                    .expect("original callback must complete")
                {
                    Err(failure) => {
                        assert_eq!(failure.error, error);
                        assert_eq!(failure.transient, transient);
                    }
                    Ok(_) => panic!("a rejected callback must fail the physical connection"),
                }
                drop(responses);
            }
        }

        // Exercise the shipped session pool and public ABI as well as its
        // physical callback boundary. A peer refusal must finish this request.
        let connect = Closure::<dyn Fn() -> Promise>::new({
            let error = super::super::js_error(reported.clone());
            move || Promise::reject(&error)
        });
        let session = super::super::WasmSession::new(
            connect.as_ref().unchecked_ref::<Function>().clone(),
            true,
            None,
        );
        let request = wire_codec::heads_request_to_wire(crate::HeadsRequest {
            retained: Default::default(),
            durable_through: None,
            projection: crate::HeadProjection::Tail,
            paths: Default::default(),
            children: None,
            limit: 1,
            at_least: Default::default(),
        });
        let actual = session
            .heads(&request.encode_to_vec(), 0)
            .await
            .unwrap_err();
        assert_eq!(failure(actual).error, reported, "public session refusal");
    }
}
