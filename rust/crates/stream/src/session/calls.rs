//! Owning Actors dispatch on the same authority and credit boundaries as follows.
use super::*;
use futures::TryStreamExt as _;

/// Canonical product target decoded by the owning call provider before dispatch.
/// It names the requested operation; only the original session credential can
/// authorize it. It is never read from a peer-selected authority header.
pub struct CallTarget {
    pub(super) resource: String,
    pub(super) action: &'static str,
}

impl CallTarget {
    /// Validate one exact resource/action pair at the product ingress boundary.
    /// The resource and declared action must name the same product.
    pub fn new(resource: String, action: &'static str) -> Result<Self, StreamError> {
        if !acyclic_auth::valid_resource(&resource)
            || !acyclic_auth::valid_action(action)
            || resource.split_once(":/").map(|(product, _)| product)
                != action.split_once('/').map(|(product, _)| product)
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self { resource, action })
    }
}

/// Verified admission for one canonical product invocation. Only session
/// admission constructs this value. It contains no caller-supplied identity.
#[derive(Clone)]
pub struct CallAuthorization(pub(super) ResourceAuthorization);

impl CallAuthorization {
    /// Authorize an account-owned target using the same current verified
    /// credential for both the certified account and resource grant checks.
    /// A namespace grant cannot confer access to another account's target.
    pub fn authorize_in_account(
        &self,
        account: &[u8],
        resource: &str,
        keys: &[Vec<u8>],
        attributes: &acyclic_auth::contract::RecordAttributes,
        policy: acyclic_auth::authority::PolicyDecision,
    ) -> Result<(), StreamError> {
        if resource != self.0.resource.name {
            return Err(StreamError::AccessDenied);
        }
        self.0
            .authorize_account(resource, keys, attributes, policy, Some(account))
    }

    /// Issuer-attested identity used by the owning host's input staging/control.
    pub fn principal(&self) -> Result<acyclic_auth::contract::Principal, StreamError> {
        self.0.principal()
    }

    /// Recheck current credential/trust and the owner's current product policy at
    /// dispatch. The canonical resource must remain within the admitted target.
    pub fn authorize(
        &self,
        resource: &str,
        keys: &[Vec<u8>],
        attributes: &acyclic_auth::contract::RecordAttributes,
        policy: acyclic_auth::authority::PolicyDecision,
    ) -> Result<(), StreamError> {
        if resource != self.0.resource.name {
            return Err(StreamError::AccessDenied);
        }
        self.0.authorize(resource, keys, attributes, policy)
    }
}

/// Product-owned decoding/routing, without another session or transport loop.
pub trait CallProvider: crate::ProviderPlatform + 'static {
    /// Validate/decode the canonical invocation and derive its exact product
    /// resource and declared action. This phase MUST NOT perform I/O or dispatch.
    fn target(&self, invocation: &[u8]) -> Result<CallTarget, StreamError>;

    /// Dispatch once after credential admission. The owner derives identity from
    /// `authorization`, checks current actor policy before staging/mutation, and
    /// retains any uncertain commit until its encoded result has been exported.
    fn call(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
    ) -> crate::BoxProviderFuture<'static, Result<Bytes, StreamError>>;

    /// Dispatch a canonical request header before its input reaches EOF.
    /// The same verified authorization and original input owner reach the
    /// product. A product without streaming input refuses before dispatch;
    /// it must never collect the input and silently call the buffered API.
    fn call_input(
        &self,
        _invocation: Bytes,
        _authorization: CallAuthorization,
        _input: CallInput,
    ) -> crate::BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        Box::pin(future::ready(Err(StreamError::Unsupported)))
    }
}

pub(super) enum InputReleased {
    Credit {
        identity: wire::SessionChannel,
        bytes: u64,
    },
    Stopped(wire::SessionChannel),
}

/// One original admitted call's input. Data retains its original receive
/// credit; EOF is successful only after the explicit input terminal. Losing
/// the session or its producer returns the original typed cause, or
/// `Unavailable` when the producer disappeared without a terminal.
pub struct CallInput {
    data: tokio::sync::mpsc::Receiver<crate::body::BodyChunk<signal::Guard<InputReleased>>>,
    terminal: Option<oneshot::Receiver<Result<wire::SessionInputEnd, StreamError>>>,
    end: Option<wire::SessionInputEnd>,
    failed: bool,
    identity: wire::SessionChannel,
    released: sync::Arc<signal::Signal<InputReleased>>,
}

impl Drop for CallInput {
    fn drop(&mut self) {
        if !self.failed && self.end.is_none() {
            // Consumer loss must wake an upload blocked on escaped data
            // owners. It cannot wait for those owners to return their credit.
            let _ = self.released.send(InputReleased::Stopped(self.identity));
        }
    }
}

impl CallInput {
    /// Consume one original input fragment without returning its capacity.
    pub async fn next_chunk(
        &mut self,
    ) -> Result<Option<crate::body::BodyChunk<impl Sized + use<>>>, StreamError> {
        future::poll_fn(|cx| self.poll_chunk(cx)).await
    }

    /// Poll the SAME original input owner from a downstream body adapter.
    /// Registers both original producer events before waiting; it does not
    /// collect, allocate a future or introduce another credit/terminal loop.
    /// Failure wins over queued data and is returned once. Successful terminal
    /// metadata remains available while the original queued data drains.
    pub fn poll_chunk(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<crate::body::BodyChunk<impl Sized + use<>>>, StreamError>>
    {
        use std::{future::Future as _, pin::Pin, task::Poll};
        if self.failed {
            return Poll::Ready(Ok(None));
        }
        if let Some(terminal) = self.terminal.as_mut()
            && let Poll::Ready(result) = Pin::new(terminal).poll(cx)
        {
            self.terminal = None;
            match result.unwrap_or(Err(StreamError::Unavailable)) {
                Ok(end) => self.end = Some(end),
                Err(error) => {
                    self.failed = true;
                    self.data.close();
                    // Only this call's fixed receive window can be queued.
                    while self.data.try_recv().is_ok() {}
                    return Poll::Ready(Err(error));
                }
            }
        }
        match self.data.poll_recv(cx) {
            // The SAME producer closes data and settles terminal. Its terminal
            // waker was installed above, including a concurrent final drop.
            Poll::Ready(None) if self.terminal.is_some() => Poll::Pending,
            data => data.map(Ok),
        }
    }
    /// Ordered metadata once the original input terminal has been observed.
    #[must_use]
    pub fn trailers(&self) -> Option<&[wire::SessionHeader]> {
        self.end.as_ref().map(|end| end.trailers.as_slice())
    }
}

pub(super) struct InputProducer {
    sender: Option<tokio::sync::mpsc::Sender<crate::body::BodyChunk<signal::Guard<InputReleased>>>>,
    terminal: Option<oneshot::Sender<Result<wire::SessionInputEnd, StreamError>>>,
    pub credit: wire::SessionWindow,
    pub next: u64,
}

impl InputProducer {
    pub fn new(
        identity: wire::SessionChannel,
        released: sync::Arc<signal::Signal<InputReleased>>,
    ) -> (Self, CallInput) {
        let (sender, data) = tokio::sync::mpsc::channel(RECEIVE_FRAMES);
        let (terminal, receiver) = oneshot::channel();
        (
            Self {
                sender: Some(sender),
                terminal: Some(terminal),
                credit: wire::SessionWindow {
                    frames: RECEIVE_FRAMES as u32,
                    bytes: RECEIVE_FRAMES as u64 * DATA_FRAME_BYTES as u64,
                },
                next: 0,
            },
            CallInput {
                data,
                terminal: Some(receiver),
                end: None,
                failed: false,
                identity,
                released,
            },
        )
    }

    pub fn receive(
        &mut self,
        identity: wire::SessionChannel,
        input: wire::SessionData,
        length: u64,
        released: sync::Arc<signal::Signal<InputReleased>>,
    ) -> Result<(), StreamError> {
        let credit = signal::Guard::new(
            released,
            InputReleased::Credit {
                identity,
                bytes: length,
            },
        );
        let next = wire_codec::session::input_sequence(self.next, &input)?;
        let Some(Item::ActorCall(bytes)) = input.item else {
            return Err(StreamError::InvalidArgument);
        };
        let chunk = crate::body::BodyChunk::new(bytes, credit);
        self.sender
            .as_ref()
            .ok_or(StreamError::InvalidArgument)?
            .try_send(chunk)
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => StreamError::Capacity,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => StreamError::Unavailable,
            })?;
        self.next = next;
        Ok(())
    }

    pub fn finish(
        &mut self,
        result: Result<wire::SessionInputEnd, StreamError>,
    ) -> Result<(), StreamError> {
        let terminal = self.terminal.take().ok_or(StreamError::InvalidArgument)?;
        self.sender.take();
        let _ = terminal.send(result);
        Ok(())
    }
    pub fn is_open(&self) -> bool {
        self.terminal.is_some()
    }
}

pub(super) fn source(
    provider: Arc<dyn CallProvider>,
    invocation: wire::SessionCall,
    authorization: CallAuthorization,
    identity: wire::SessionChannel,
    input: Option<CallInput>,
) -> Source {
    reply(
        identity,
        Item::ActorCall,
        Box::pin(async move {
            match input {
                Some(input) => {
                    provider
                        .call_input(invocation.invocation, authorization, input)
                        .await
                }
                None => provider.call(invocation.invocation, authorization).await,
            }
        }),
    )
}

/// One finite one-shot reply (an Actors call or a children page) on a channel.
pub(super) fn reply(
    identity: wire::SessionChannel,
    item: fn(Bytes) -> Item,
    reply: crate::BoxProviderFuture<'static, Result<Bytes, StreamError>>,
) -> Source {
    // Opening acknowledges admission, not completion. Dispatch itself starts
    // only when the shared scheduler admits the next source poll under credit.
    let opened = stream::once(future::ready(Ok(Event::Opened(None, None, None))));
    // One result allocation is retained while chunks are exposed. Fragmentation
    // allocates no payload copies and the common session queue enforces credit.
    let result = stream::once(async move {
        let reply = reply.await?;
        if reply.is_empty() || reply.len() > wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok::<_, StreamError>(reply)
    })
    .map_ok(move |reply| {
        let total = reply.len();
        stream::iter(
            (0..reply.len())
                .step_by(crate::MAX_RECORD_BYTES)
                .enumerate()
                .map(move |(index, start)| {
                    let end = (start + crate::MAX_RECORD_BYTES).min(reply.len());
                    Ok(Event::CallChunk {
                        index: index as u64,
                        total: total as u64,
                        bytes: reply.slice(start..end),
                        item,
                    })
                }),
        )
    })
    .try_flatten();
    Box::pin(opened.chain(result).map(move |event| (identity, event)))
}
