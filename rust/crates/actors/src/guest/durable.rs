//! Host adapters for Durable Object state. Socket ownership stays with the host:
//! Cloud's and the local runtime's (`local::Sockets`).
use super::{GuestError, SocketPayload};
use crate::addressing::ActorReference;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc};
use tokio::sync::Mutex;
use ts_rs::TS;

/// Runtime-owned hibernatable socket identity, unrelated to the actor's identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TS)]
pub struct WebSocketId(pub String);
/// A request/response pair answered by the host without waking the object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TS)]
pub struct WebSocketAutoResponse {
    /// Exact incoming text frame.
    pub request: String,
    /// Exact outgoing text frame.
    pub response: String,
}
/// Original socket failure retained across native executor threads.
#[cfg(not(target_arch = "wasm32"))]
pub type SocketTerminalError = dyn std::error::Error + Send + Sync;
/// Original socket failure retaining browser event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type SocketTerminalError = dyn std::error::Error;

/// A retained observation of one original host socket's close producer.
/// Dropping an observer never cancels that producer. The original decided
/// failure remains reachable after the host retires its live socket entry.
pub trait SocketTerminal: acyclic_stream::ProviderPlatform {
    /// Joins the original producer and retains its decided failure after retirement.
    fn completion(
        self: Arc<Self>,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<(), Arc<SocketTerminalError>>>;
}

/// Hibernatable socket effects. A host keeps sockets, tags, attachments and the
/// auto-response pair outside the actor heap, so a hibernated actor holding
/// sockets costs it nothing; implementations check the actor on every call.
/// Like every host trait it follows the platform boundary: thread-safe on
/// native targets, and free to own thread-affine handles in a browser.
pub trait SocketHost: acyclic_stream::ProviderPlatform {
    /// Registers against this actor's exact live socket before dispatch.
    /// A missing socket refuses registration; absence is no close certificate.
    fn web_socket_terminal(
        &self,
        _actor: &ActorReference,
        _socket: &WebSocketId,
    ) -> Result<Arc<dyn SocketTerminal>, GuestError> {
        Err(GuestError::Handler(
            "original socket terminal is unavailable".into(),
        ))
    }

    /// Transfer a pending socket to host ownership, retaining tags across hibernation.
    fn accept_web_socket(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        tags: &[String],
    ) -> Result<(), GuestError>;
    /// All retained sockets, optionally selected by an exact tag.
    fn get_web_sockets(
        &self,
        actor: &ActorReference,
        tag: Option<&str>,
    ) -> Result<Vec<WebSocketId>, GuestError>;
    /// Send one frame on an actor-owned socket.
    fn web_socket_send(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        payload: SocketPayload,
    ) -> Result<(), GuestError>;
    /// Close an actor-owned socket.
    fn web_socket_close(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        code: u16,
        reason: &str,
    ) -> Result<(), GuestError>;
    /// Persist opaque attachment bytes with the socket, across heap loss.
    fn serialize_attachment(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        bytes: Bytes,
    ) -> Result<(), GuestError>;
    /// Read retained attachment bytes.
    fn deserialize_attachment(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Option<Bytes>, GuestError>;
    /// Replace or disable the host auto-response pair for this actor.
    fn set_web_socket_auto_response(
        &self,
        actor: &ActorReference,
        value: Option<WebSocketAutoResponse>,
    ) -> Result<(), GuestError>;
    /// Read the active auto-response pair.
    fn get_web_socket_auto_response(
        &self,
        actor: &ActorReference,
    ) -> Result<Option<WebSocketAutoResponse>, GuestError>;
    /// Timestamp of the latest auto-response for an actor-owned socket.
    fn get_web_socket_auto_response_timestamp(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Option<u64>, GuestError>;
    /// Read the socket's retained tags.
    fn get_tags(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Vec<String>, GuestError>;
}
/// One host attempt's opaque request handle. It names a live binding in the
/// host's [`Requests`]; by itself it grants nothing.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RequestId(u64);
impl RequestId {
    /// The handle with this wire value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    /// Its wire value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The host-side capability binding `request -> (actor, epoch)`, local to one
/// host process: the only authority a host capability call carries. The host
/// opens one binding per handler invocation (`on_record`, then `on_failure`,
/// each its own) and the binding closes when that invocation returns, is
/// cancelled, times out or is abandoned, so a late call from it is refused
/// as [`GuestError::Revoked`] without any provider read. Co-resident actors of one process
/// are one principal; distinct principals need distinct processes. It does not
/// stop a partitioned old owner's in-flight or already issued external call;
/// that needs resource-side fencing.
#[derive(Clone, Debug, Default)]
pub struct Requests(Arc<std::sync::Mutex<Bindings>>);
#[derive(Debug, Default)]
struct Bindings {
    next: u64,
    live: std::collections::HashMap<u64, BoundRequest>,
}
#[derive(Debug)]
struct BoundRequest {
    actor: ActorReference,
    epoch: u64,
    state: Option<super::ActorStateHost>,
}
/// One open binding; dropping it closes the request.
#[derive(Debug)]
pub struct Request {
    id: RequestId,
    requests: Requests,
}
impl Request {
    /// The handle the attempt's guest receives.
    #[must_use]
    pub const fn id(&self) -> RequestId {
        self.id
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        self.requests.bindings().live.remove(&self.id.0);
    }
}
impl Requests {
    fn bindings(&self) -> std::sync::MutexGuard<'_, Bindings> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Bind a new request to `actor` at placement `epoch`.
    #[must_use]
    pub fn open(&self, actor: &ActorReference, epoch: u64) -> Request {
        let mut bindings = self.bindings();
        bindings.next = bindings.next.wrapping_add(1);
        let id = bindings.next;
        bindings.live.insert(
            id,
            BoundRequest {
                actor: actor.clone(),
                epoch,
                state: None,
            },
        );
        Request {
            id: RequestId(id),
            requests: self.clone(),
        }
    }
    /// Native state of this live request, when its manifest declares it.
    pub fn state(&self, request: RequestId) -> Result<super::ActorStateHost, GuestError> {
        self.bindings()
            .live
            .get(&request.0)
            .and_then(|binding| binding.state.clone())
            .ok_or(GuestError::Revoked)
    }

    pub(crate) fn bind_state(&self, request: RequestId, state: super::ActorStateHost) {
        let mut bindings = self.bindings();
        if let Some(binding) = bindings.live.get_mut(&request.0) {
            binding.state = Some(state);
        }
    }

    /// The actor and epoch a live request is bound to; a closed or unknown
    /// request is refused.
    pub fn admit(&self, request: RequestId) -> Result<(ActorReference, u64), GuestError> {
        self.bindings()
            .live
            .get(&request.0)
            .map(|binding| (binding.actor.clone(), binding.epoch))
            .ok_or(GuestError::Revoked)
    }
}

/// Scoped runtime effects; implementations must check the actor reference on every call.
pub trait DurableHost: SocketHost {
    /// Atomically clear actor storage and alarm; the address remains usable.
    fn delete_all(
        &self,
        actor: &ActorReference,
    ) -> impl Future<Output = Result<(), GuestError>> + Send;
}
/// Scoped state for one host request. Every call resolves the actor through
/// the host's live binding, so it acts only while that request is live.
pub struct DurableObjectState<H> {
    request: RequestId,
    requests: Requests,
    host: Arc<H>,
}
impl<H: DurableHost> DurableObjectState<H> {
    /// Clear state and alarm without deleting its address or namespace deployment.
    pub async fn delete_all(&self) -> Result<(), GuestError> {
        let actor = self.id()?;
        self.host.delete_all(&actor).await
    }
}
impl<H: SocketHost> DurableObjectState<H> {
    /// Bind to one delivered request of the host's `requests`.
    pub const fn new(request: RequestId, requests: Requests, host: Arc<H>) -> Self {
        Self {
            request,
            requests,
            host,
        }
    }
    /// The actor this live request is bound to.
    pub fn id(&self) -> Result<ActorReference, GuestError> {
        self.requests.admit(self.request).map(|(actor, _)| actor)
    }
    /// Retain a socket and its tags across hibernation.
    pub fn accept_web_socket(
        &self,
        socket: &WebSocketId,
        tags: &[String],
    ) -> Result<(), GuestError> {
        self.host.accept_web_socket(&self.id()?, socket, tags)
    }
    /// Query retained sockets, optionally by exact tag.
    pub fn get_web_sockets(&self, tag: Option<&str>) -> Result<Vec<WebSocketId>, GuestError> {
        self.host.get_web_sockets(&self.id()?, tag)
    }
    /// Send on a retained socket.
    pub fn web_socket_send(
        &self,
        socket: &WebSocketId,
        payload: SocketPayload,
    ) -> Result<(), GuestError> {
        self.host.web_socket_send(&self.id()?, socket, payload)
    }
    /// Close a retained socket.
    pub fn web_socket_close(
        &self,
        socket: &WebSocketId,
        code: u16,
        reason: &str,
    ) -> Result<(), GuestError> {
        self.host
            .web_socket_close(&self.id()?, socket, code, reason)
    }
    /// Persist a socket attachment.
    pub fn serialize_attachment(
        &self,
        socket: &WebSocketId,
        value: Bytes,
    ) -> Result<(), GuestError> {
        self.host.serialize_attachment(&self.id()?, socket, value)
    }
    /// Recover a retained socket attachment.
    pub fn deserialize_attachment(
        &self,
        socket: &WebSocketId,
    ) -> Result<Option<Bytes>, GuestError> {
        self.host.deserialize_attachment(&self.id()?, socket)
    }
    /// Replace or disable the auto-response pair.
    pub fn set_web_socket_auto_response(
        &self,
        value: Option<WebSocketAutoResponse>,
    ) -> Result<(), GuestError> {
        self.host.set_web_socket_auto_response(&self.id()?, value)
    }
    /// Query the active auto-response pair.
    pub fn get_web_socket_auto_response(
        &self,
    ) -> Result<Option<WebSocketAutoResponse>, GuestError> {
        self.host.get_web_socket_auto_response(&self.id()?)
    }
    /// Query the latest automatic response time.
    pub fn get_web_socket_auto_response_timestamp(
        &self,
        socket: &WebSocketId,
    ) -> Result<Option<u64>, GuestError> {
        self.host
            .get_web_socket_auto_response_timestamp(&self.id()?, socket)
    }
    /// Query a socket's retained tags.
    pub fn get_tags(&self, socket: &WebSocketId) -> Result<Vec<String>, GuestError> {
        self.host.get_tags(&self.id()?, socket)
    }
}

type Initializer<S> = acyclic_stream::BoxProviderFuture<'static, Result<S, GuestError>>;
enum ActivationState<S> {
    Pending(Initializer<S>),
    Ready(Arc<S>),
    Failed,
}
/// Initialization gates every delivery. Failure or cancellation permanently fences this heap.
pub struct Activation<S> {
    state: Mutex<ActivationState<S>>,
}
impl<S: Send + Sync + 'static> Activation<S> {
    /// The constructor future runs once before any event is delivered.
    pub fn new(
        initialize: impl Future<Output = Result<S, GuestError>> + acyclic_stream::ProviderTask + 'static,
    ) -> Self {
        Self {
            state: Mutex::new(ActivationState::Pending(Box::pin(initialize))),
        }
    }
    /// Block admission while initialization or an exclusive callback is running.
    pub async fn block_concurrency_while<R>(
        &self,
        callback: impl AsyncFnOnce(Arc<S>) -> Result<R, GuestError>,
    ) -> Result<R, GuestError> {
        let mut state = self.state.lock().await;
        let previous = std::mem::replace(&mut *state, ActivationState::Failed);
        let value = match previous {
            ActivationState::Pending(initialize) => Arc::new(initialize.await?),
            ActivationState::Ready(value) => value,
            ActivationState::Failed => {
                return Err(GuestError::Handler(
                    "activation failed; host must discard heap".into(),
                ));
            }
        };
        let result = callback(value.clone()).await?;
        *state = ActivationState::Ready(value);
        Ok(result)
    }
    /// Every host-delivered fetch, RPC, alarm or socket handler passes this gate.
    /// After admission handlers may interleave; cross-stream commits are unchanged.
    pub async fn deliver<R>(
        &self,
        handler: impl AsyncFnOnce(Arc<S>) -> Result<R, GuestError>,
    ) -> Result<R, GuestError> {
        let value = self
            .block_concurrency_while(async |state| Ok(state))
            .await?;
        handler(value).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn guest_socket_metadata_round_trips_without_logical_caps() -> Result<(), GuestError> {
        let result = super::super::GuestEventResult::SocketAccept {
            subprotocol: None,
            attachment: Bytes::from(vec![7; 2049]),
            tags: vec!["λ".repeat(257); 11],
        };
        let raw =
            serde_json::to_value(&result).map_err(|error| GuestError::Encode(error.to_string()))?;
        let restored: super::super::GuestEventResult = serde_json::from_value(raw.clone())
            .map_err(|error| GuestError::Encode(error.to_string()))?;
        assert_eq!(restored, result);
        let mut malformed = raw;
        let fields = malformed
            .as_object_mut()
            .ok_or_else(|| GuestError::Encode("socket result is not an object".into()))?;
        fields.insert("tags".into(), serde_json::json!([null]));
        assert!(serde_json::from_value::<super::super::GuestEventResult>(malformed).is_err());
        Ok(())
    }
    #[tokio::test]
    async fn initialization_is_once_before_all_delivery_and_failed_heap_stays_fenced()
    -> Result<(), GuestError> {
        let count = Arc::new(AtomicUsize::new(0));
        let initialized = count.clone();
        let activation = Activation::new(async move {
            initialized.fetch_add(1, Ordering::SeqCst);
            Ok(42)
        });
        let (a, b) = futures::join!(
            activation.deliver(async |state| Ok(*state)),
            activation.deliver(async |state| Ok(*state))
        );
        assert_eq!((a?, b?), (42, 42));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let failed = activation
            .block_concurrency_while(async |_| {
                Err::<(), _>(GuestError::Handler("failed initializer".into()))
            })
            .await;
        assert!(failed.is_err());
        assert!(activation.deliver(async |_| Ok(())).await.is_err());
        // Negative control: bypassing the admission gate would execute the event.
        let ungated = async {
            count.fetch_add(1, Ordering::SeqCst);
            Ok::<_, GuestError>(())
        };
        ungated.await?;
        assert_eq!(count.load(Ordering::SeqCst), 2);
        Ok(())
    }
    #[tokio::test]
    async fn cancellation_during_initialization_fences_the_heap() {
        let activation = Activation::<u32>::new(std::future::pending());
        let mut delivery = Box::pin(activation.deliver(async |_| Ok(())));
        assert!(futures::poll!(delivery.as_mut()).is_pending());
        drop(delivery);
        assert!(activation.deliver(async |_| Ok(())).await.is_err());
    }
}
