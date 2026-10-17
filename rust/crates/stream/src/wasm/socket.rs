//! Browser physical I/O for the same session pool used by native clients.
use super::*;
use crate::session::transport::{Requests, Responses, Transport};
use futures::{
    channel::oneshot,
    future::{Either, select},
};
use std::{cell::RefCell, sync::Arc};
use tokio::sync::mpsc;

#[derive(Clone)]
pub(super) struct Endpoint {
    pub(super) url: String,
    pub(super) credential: js_sys::Function,
}
impl Endpoint {
    pub(super) fn new(url: &str, credential: js_sys::Function) -> Result<Self, StreamError> {
        let parsed = web_sys::Url::new(url).map_err(|_| StreamError::InvalidArgument)?;
        let local = matches!(
            parsed.hostname().as_str(),
            "localhost" | "127.0.0.1" | "[::1]"
        );
        if (parsed.protocol() != "https:" && !(local && parsed.protocol() == "http:"))
            || !parsed.username().is_empty()
            || !parsed.password().is_empty()
            || !parsed.search().is_empty()
            || !parsed.hash().is_empty()
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            url: parsed.href().trim_end_matches('/').to_owned(),
            credential,
        })
    }
    pub(super) async fn token(&self) -> Result<String, StreamError> {
        let result = self
            .credential
            .call0(&JsValue::UNDEFINED)
            .map_err(|_| StreamError::AccessDenied)?;
        let token = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&result))
            .await
            .map_err(|_| StreamError::AccessDenied)?
            .as_string()
            .ok_or(StreamError::AccessDenied)?;
        if !acyclic_auth::credential::valid_credential(&token) {
            return Err(StreamError::AccessDenied);
        }
        Ok(token)
    }
}

struct Incoming {
    sender: Option<mpsc::Sender<Result<wire::SessionFrame, StreamError>>>,
    terminal: Option<StreamError>,
}
impl Incoming {
    fn fail(&mut self, error: StreamError) {
        if self.sender.take().is_some() {
            self.terminal = Some(error);
        }
    }
}
struct Socket {
    socket: web_sys::WebSocket,
    _open: Closure<dyn FnMut(web_sys::Event)>,
    _message: Closure<dyn FnMut(web_sys::MessageEvent)>,
    _error: Closure<dyn FnMut(web_sys::Event)>,
    _close: Closure<dyn FnMut(web_sys::Event)>,
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.socket.set_onopen(None);
        self.socket.set_onmessage(None);
        self.socket.set_onerror(None);
        self.socket.set_onclose(None);
        let _ = self.socket.close();
    }
}
impl Transport for Endpoint {
    fn session(
        &self,
        source: Requests,
    ) -> crate::BoxProviderFuture<'_, Result<Responses, StreamError>> {
        Box::pin(async move {
            use base64::Engine as _;
            let token =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.token().await?);
            let url = format!("{}/v1/stream/session", self.url).replacen("http", "ws", 1);
            let protocols = Array::of2(
                &"acyclic.session.v1".into(),
                &format!("acyclic.bearer.{token}").into(),
            );
            let socket = web_sys::WebSocket::new_with_str_sequence(&url, &protocols)
                .map_err(|_| StreamError::Unavailable)?;
            socket.set_binary_type(web_sys::BinaryType::Arraybuffer);
            let (sender, receiver) = mpsc::channel(crate::MAX_ITEMS);
            let incoming = Rc::new(RefCell::new(Incoming {
                sender: Some(sender),
                terminal: None,
            }));
            let (ready, opened) = oneshot::channel();
            let ready = Rc::new(RefCell::new(Some(ready)));
            let open_socket = socket.clone();
            let open_ready = ready.clone();
            let open = Closure::new(move |_: web_sys::Event| {
                if let Some(sender) = open_ready.borrow_mut().take() {
                    let result = if open_socket.protocol() == "acyclic.session.v1" {
                        Ok(())
                    } else {
                        Err(StreamError::InvalidArgument)
                    };
                    let _ = sender.send(result);
                }
            });
            let messages = incoming.clone();
            let message_socket = socket.clone();
            let message = Closure::new(move |event: web_sys::MessageEvent| {
                let result = decode_frame(event.data());
                let mut incoming = messages.borrow_mut();
                match result {
                    Ok(frame) => {
                        if incoming
                            .sender
                            .as_ref()
                            .is_some_and(|sender| sender.try_send(Ok(frame)).is_err())
                        {
                            incoming.fail(StreamError::Capacity);
                            let _ = message_socket.close();
                        }
                    }
                    Err(error) => {
                        incoming.fail(error);
                        let _ = message_socket.close();
                    }
                }
            });
            let failure = incoming.clone();
            let error_ready = ready.clone();
            let error_socket = socket.clone();
            let error = Closure::new(move |_: web_sys::Event| {
                failure.borrow_mut().fail(StreamError::Unavailable);
                if let Some(sender) = error_ready.borrow_mut().take() {
                    let _ = sender.send(Err(StreamError::Unavailable));
                }
                let _ = error_socket.close();
            });
            let closed = incoming.clone();
            let close_ready = ready;
            let close = Closure::new(move |_: web_sys::Event| {
                closed.borrow_mut().fail(StreamError::Unavailable);
                if let Some(sender) = close_ready.borrow_mut().take() {
                    let _ = sender.send(Err(StreamError::Unavailable));
                }
            });
            socket.set_onopen(Some(open.as_ref().unchecked_ref()));
            socket.set_onmessage(Some(message.as_ref().unchecked_ref()));
            socket.set_onerror(Some(error.as_ref().unchecked_ref()));
            socket.set_onclose(Some(close.as_ref().unchecked_ref()));
            let guard = Socket {
                socket,
                _open: open,
                _message: message,
                _error: error,
                _close: close,
            };
            opened.await.map_err(|_| StreamError::Unavailable)??;
            let response = futures::stream::try_unfold(
                (guard, receiver, source, incoming),
                |(guard, mut receiver, mut source, incoming)| async move {
                    loop {
                        let event = {
                            let received = receiver.recv();
                            futures::pin_mut!(received);
                            match select(received, source.next()).await {
                                Either::Left((frame, _)) => Either::Left(frame),
                                Either::Right((frame, _)) => Either::Right(frame),
                            }
                        };
                        match event {
                            Either::Left(Some(frame)) => {
                                return Ok(Some((frame?, (guard, receiver, source, incoming))));
                            }
                            Either::Left(None) => {
                                return Err(incoming
                                    .borrow_mut()
                                    .terminal
                                    .take()
                                    .unwrap_or(StreamError::Unavailable));
                            }
                            Either::Right(Some(frame)) => {
                                let bytes = frame.encode_to_vec();
                                if bytes.len() > MAX_COMMAND_BYTES
                                    || guard.socket.buffered_amount() as usize + bytes.len()
                                        > MAX_COMMAND_BYTES
                                {
                                    return Err(StreamError::Capacity);
                                }
                                guard
                                    .socket
                                    .send_with_u8_array(&bytes)
                                    .map_err(|_| StreamError::Unavailable)?;
                            }
                            Either::Right(None) => return Ok(None),
                        }
                    }
                },
            );
            Ok(Box::pin(response) as Responses)
        })
    }
}
fn decode_frame(data: JsValue) -> Result<wire::SessionFrame, StreamError> {
    if !data.is_instance_of::<js_sys::ArrayBuffer>() {
        return Err(StreamError::InvalidArgument);
    }
    let bytes = Uint8Array::new(&data);
    if bytes.length() as usize > MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    wire_codec::session::decode_frame(&bytes.to_vec())
}

pub(super) fn sessions(endpoint: Endpoint) -> crate::session::Client {
    let moved = endpoint.clone();
    crate::session::Client::with_transport(
        Arc::new(endpoint),
        Arc::new(move |node| {
            let credential = moved.credential.clone();
            Box::pin(async move {
                Ok(Arc::new(Endpoint::new(&node.endpoint, credential)?)
                    as crate::session::transport::Endpoint)
            })
        }),
        Arc::new(crate::session::runtime::PlatformExecutor),
    )
}
