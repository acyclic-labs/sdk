//! Host adapters for Durable Object state. Storage and socket ownership stay in Cloud.
use super::{GuestError, SocketPayload};
use crate::addressing::ActorReference;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc};
use tokio::sync::Mutex;
use ts_rs::TS;

/// Durable alarm delivery metadata. Delivery retries are owned by the host.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", try_from = "RawAlarmInfo")]
pub struct AlarmInfo {
    /// Zero for first delivery, increasing on each retry.
    retry_count: u32,
    /// Whether this delivery follows a failed attempt.
    is_retry: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawAlarmInfo {
    retry_count: u32,
    is_retry: bool,
}
impl TryFrom<RawAlarmInfo> for AlarmInfo {
    type Error = String;
    fn try_from(raw: RawAlarmInfo) -> Result<Self, Self::Error> {
        if raw.is_retry != (raw.retry_count != 0) {
            return Err("contradictory alarm retry metadata".into());
        }
        Ok(Self::new(raw.retry_count))
    }
}
impl AlarmInfo {
    /// Number of failed delivery attempts before this call.
    pub const fn retry_count(self) -> u32 {
        self.retry_count
    }
    /// Derived from `retry_count`; never independent state.
    pub const fn is_retry(self) -> bool {
        self.is_retry
    }
    /// Derive both fields from one counter so contradictory metadata is impossible.
    pub const fn new(retry_count: u32) -> Self {
        Self {
            retry_count,
            is_retry: retry_count != 0,
        }
    }
}
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
/// Scoped runtime effects; implementations must check the actor reference on every call.
pub trait DurableHost: Send + Sync {
    /// Atomically clear actor storage and alarm; the address remains usable.
    fn delete_all(
        &self,
        actor: &ActorReference,
    ) -> impl Future<Output = Result<(), GuestError>> + Send;
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
/// Scoped state supplied by the host at activation; the reference is never authority.
pub struct DurableObjectState<H> {
    reference: ActorReference,
    host: Arc<H>,
}
impl<H: DurableHost> DurableObjectState<H> {
    /// Materialize only after host admission of the namespace binding.
    pub const fn new(reference: ActorReference, host: Arc<H>) -> Self {
        Self { reference, host }
    }
    /// Transferable address without the host adapter.
    pub const fn id(&self) -> &ActorReference {
        &self.reference
    }
    /// Clear state and alarm without deleting its address or namespace deployment.
    pub async fn delete_all(&self) -> Result<(), GuestError> {
        self.host.delete_all(&self.reference).await
    }
    /// Retain a socket and its tags across hibernation.
    pub fn accept_web_socket(
        &self,
        socket: &WebSocketId,
        tags: &[String],
    ) -> Result<(), GuestError> {
        self.host.accept_web_socket(&self.reference, socket, tags)
    }
    /// Query retained sockets, optionally by exact tag.
    pub fn get_web_sockets(&self, tag: Option<&str>) -> Result<Vec<WebSocketId>, GuestError> {
        self.host.get_web_sockets(&self.reference, tag)
    }
    /// Send on a retained socket.
    pub fn web_socket_send(
        &self,
        socket: &WebSocketId,
        payload: SocketPayload,
    ) -> Result<(), GuestError> {
        self.host.web_socket_send(&self.reference, socket, payload)
    }
    /// Close a retained socket.
    pub fn web_socket_close(
        &self,
        socket: &WebSocketId,
        code: u16,
        reason: &str,
    ) -> Result<(), GuestError> {
        self.host
            .web_socket_close(&self.reference, socket, code, reason)
    }
    /// Persist a socket attachment.
    pub fn serialize_attachment(
        &self,
        socket: &WebSocketId,
        value: Bytes,
    ) -> Result<(), GuestError> {
        self.host
            .serialize_attachment(&self.reference, socket, value)
    }
    /// Recover a retained socket attachment.
    pub fn deserialize_attachment(
        &self,
        socket: &WebSocketId,
    ) -> Result<Option<Bytes>, GuestError> {
        self.host.deserialize_attachment(&self.reference, socket)
    }
    /// Replace or disable the auto-response pair.
    pub fn set_web_socket_auto_response(
        &self,
        value: Option<WebSocketAutoResponse>,
    ) -> Result<(), GuestError> {
        self.host
            .set_web_socket_auto_response(&self.reference, value)
    }
    /// Query the active auto-response pair.
    pub fn get_web_socket_auto_response(
        &self,
    ) -> Result<Option<WebSocketAutoResponse>, GuestError> {
        self.host.get_web_socket_auto_response(&self.reference)
    }
    /// Query the latest automatic response time.
    pub fn get_web_socket_auto_response_timestamp(
        &self,
        socket: &WebSocketId,
    ) -> Result<Option<u64>, GuestError> {
        self.host
            .get_web_socket_auto_response_timestamp(&self.reference, socket)
    }
    /// Query a socket's retained tags.
    pub fn get_tags(&self, socket: &WebSocketId) -> Result<Vec<String>, GuestError> {
        self.host.get_tags(&self.reference, socket)
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
    fn alarm_metadata_rejects_contradictory_retry_state() -> Result<(), GuestError> {
        for retry_count in [0, 1, u32::MAX] {
            let info = AlarmInfo::new(retry_count);
            assert_eq!(
                serde_json::from_value::<AlarmInfo>(
                    serde_json::json!({"retryCount":retry_count,"isRetry":retry_count != 0})
                )
                .map_err(|e| GuestError::Decode(e.to_string()))?,
                info
            );
            assert!(
                serde_json::from_value::<AlarmInfo>(
                    serde_json::json!({"retryCount":retry_count,"isRetry":retry_count == 0})
                )
                .is_err()
            );
        }
        Ok(())
    }
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
