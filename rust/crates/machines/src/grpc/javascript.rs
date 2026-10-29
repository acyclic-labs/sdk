//! Thread-affine projection of the public protobuf service onto Connect Transport.

use super::{ProviderError, transport};
use acyclic_stream::pull::PullCursor;
use futures::{Future, Stream, StreamExt as _};
use js_sys::{Function, Promise, Reflect, Uint8Array};
use prost::Message;
use std::{
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};
use tonic::{Request, Response, Status};
use wasm_bindgen::{JsCast as _, JsValue, closure::Closure};
use wasm_bindgen_futures::{JsFuture, future_to_promise};

type Budget<D> = dyn Fn(Option<Duration>, Option<u64>) -> Result<D, ProviderError>;

/// Retains an authenticated transport and the embedding platform's clock owner.
/// The callback is `createMachinesRpcTransport(originalTransport)`. The budget
/// factory binds one original stop/expiry event, including credential admission.
pub struct Endpoint<D> {
    open: Function,
    budget: Rc<Budget<D>>,
}

impl<D> Clone for Endpoint<D> {
    fn clone(&self) -> Self {
        Self {
            open: self.open.clone(),
            budget: self.budget.clone(),
        }
    }
}

impl<D: transport::Deadline> Endpoint<D> {
    /// No credential, endpoint, caller scope or selected volume is serialized here.
    pub fn new(
        open: Function,
        budget: impl Fn(Option<Duration>, Option<u64>) -> Result<D, ProviderError> + 'static,
    ) -> Self {
        Self {
            open,
            budget: Rc::new(budget),
        }
    }
}

impl<D: transport::Deadline> transport::Endpoint for Endpoint<D> {
    type Client = Client;
    type Deadline = D;
    async fn client(&self) -> Result<Client, ProviderError> {
        Ok(Client(self.open.clone()))
    }
    fn deadline(&self, timeout: Duration, expiry: Option<u64>) -> Result<D, ProviderError> {
        (self.budget)(Some(timeout), expiry)
    }
    fn absolute_deadline(&self, expiry: u64) -> Result<D, ProviderError> {
        (self.budget)(None, Some(expiry))
    }
}

/// Physical RPC methods are generated from the same service descriptor as Tonic.
pub struct Client(Function);

impl Client {
    pub(super) async fn stream<I, O, S>(
        &self,
        method: &str,
        request: Request<S>,
    ) -> Result<Response<Output<O>>, Status>
    where
        I: Message + 'static,
        O: Message + Default,
        S: Stream<Item = I> + 'static,
    {
        let timeout = request
            .metadata()
            .get("grpc-timeout")
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| Status::invalid_argument("original RPC deadline metadata is invalid"))?;
        let input = Rc::new(PullCursor::new(
            request
                .into_inner()
                .map(|message| {
                    Ok(JsValue::from(Uint8Array::from(
                        message.encode_to_vec().as_slice(),
                    )))
                })
                .boxed_local(),
            Status::unavailable("original request producer lost"),
        ));
        let source = input.clone();
        let pull = Closure::<dyn FnMut() -> Promise>::new(move || {
            let source = source.clone();
            future_to_promise(async move {
                source
                    .next()
                    .await
                    .map(|value| value.unwrap_or(JsValue::UNDEFINED))
                    .map_err(|error| js_sys::Error::new(error.message()).into())
            })
        });
        let opened = self
            .0
            .call3(
                &JsValue::UNDEFINED,
                &JsValue::from_str(method),
                pull.as_ref(),
                &timeout.map(JsValue::from).unwrap_or(JsValue::UNDEFINED),
            )
            .map_err(status)?;
        let output = Output {
            owner: opened,
            input,
            _pull: pull,
            pending: None,
            ended: false,
            message: PhantomData,
        };
        // Own cancellation before waiting for headers. Dropping this future ends
        // both directions even if credentials/connect have not produced headers.
        let ready = property(&output.owner, "ready")?
            .dyn_into::<Promise>()
            .map_err(status)?;
        JsFuture::from(ready).await.map_err(status)?;
        Ok(Response::new(output))
    }

    pub(super) async fn unary<I, O, S>(
        &self,
        method: &str,
        request: Request<S>,
    ) -> Result<Response<O>, Status>
    where
        I: Message + 'static,
        O: Message + Default,
        S: Stream<Item = I> + 'static,
    {
        let mut output = self.stream(method, request).await?.into_inner();
        let message = output
            .next()
            .await
            .transpose()?
            .ok_or_else(|| Status::data_loss("unary RPC omitted its original response"))?;
        Ok(Response::new(message))
    }
}

/// One original response cursor; it never pulls until its consumer does.
pub struct Output<T> {
    owner: JsValue,
    input: Rc<PullCursor<JsValue, Status>>,
    _pull: Closure<dyn FnMut() -> Promise>,
    pending: Option<JsFuture>,
    ended: bool,
    message: PhantomData<fn() -> T>,
}

impl<T: Message + Default> Stream for Output<T> {
    type Item = Result<T, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if this.pending.is_none() {
            let next = function(&this.owner, "next")
                .and_then(|next| next.call0(&this.owner).map_err(status))
                .and_then(|value| value.dyn_into::<Promise>().map_err(status));
            match next {
                Ok(next) => this.pending = Some(JsFuture::from(next)),
                Err(error) => {
                    this.close();
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
        let result = match this.pending.as_mut() {
            Some(next) => match Pin::new(next).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            },
            None => return Poll::Ready(None),
        };
        this.pending = None;
        match result {
            Ok(value) if value.is_undefined() => {
                this.close();
                Poll::Ready(None)
            }
            Ok(value) => {
                let decoded = value
                    .dyn_into::<Uint8Array>()
                    .map_err(status)
                    .and_then(|bytes| {
                        T::decode(bytes.to_vec().as_slice())
                            .map_err(|error| Status::data_loss(error.to_string()))
                    });
                if decoded.is_err() {
                    this.close();
                }
                Poll::Ready(Some(decoded))
            }
            Err(error) => {
                this.close();
                Poll::Ready(Some(Err(status(error))))
            }
        }
    }
}

impl<T> Output<T> {
    fn close(&mut self) {
        if !self.ended {
            self.ended = true;
            self.input.close();
            if let Ok(cancel) = function(&self.owner, "cancel") {
                let _ = cancel.call0(&self.owner);
            }
        }
    }
}
impl<T> Drop for Output<T> {
    fn drop(&mut self) {
        self.close();
    }
}

fn property(owner: &JsValue, name: &str) -> Result<JsValue, Status> {
    Reflect::get(owner, &JsValue::from_str(name)).map_err(status)
}
fn function(owner: &JsValue, name: &str) -> Result<Function, Status> {
    property(owner, name)?
        .dyn_into::<Function>()
        .map_err(status)
}
fn status(error: JsValue) -> Status {
    let code = Reflect::get(&error, &JsValue::from_str("code"))
        .ok()
        .and_then(|code| code.as_f64())
        .filter(|code| code.fract() == 0.0 && (1.0..=16.0).contains(code))
        .map_or(tonic::Code::Unknown, |code| {
            tonic::Code::from_i32(code as i32)
        });
    let message = Reflect::get(&error, &JsValue::from_str("rawMessage"))
        .ok()
        .and_then(|message| message.as_string())
        .or_else(|| {
            Reflect::get(&error, &JsValue::from_str("message"))
                .ok()
                .and_then(|message| message.as_string())
        })
        .unwrap_or_else(|| {
            "public Machines transport failed; recovery: retain the original operation identity"
                .into()
        });
    Status::new(code, message)
}
