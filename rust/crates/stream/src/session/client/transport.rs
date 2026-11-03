//! Platform I/O for the shared session pool, without another session protocol.
use crate::{BoxProviderFuture, BoxProviderStream, ProviderPlatform, StreamError, wire};
use std::sync::Arc;

/// Canonical inbound frames, with the owning boundary's error contract.
pub type Responses<E = StreamError> = BoxProviderStream<'static, Result<super::super::Frame, E>>;

/// A physical connection failure, separate from the caller's semantic error.
/// A definite refusal never authorizes reconnecting. Only an interrupted I/O
/// operation may request backoff, and the pool still checks replay safety.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct Failure {
    /// The original caller-visible error, independent of retry classification.
    #[source]
    pub error: StreamError,
    /// Whether the I/O owner observed a transient transport interruption.
    pub transient: bool,
}

impl From<StreamError> for Failure {
    fn from(error: StreamError) -> Self {
        Self {
            error,
            transient: false,
        }
    }
}
/// Canonical wire frames, without process-local backing metadata.
pub type WireResponses = BoxProviderStream<'static, Result<wire::SessionFrame, StreamError>>;
/// Canonical outbound frames; dropping this source cancels the connection.
pub type Requests = BoxProviderStream<'static, wire::SessionFrame>;

/// Retain one registered source event across the physical connection's waits.
/// A ready change/refusal wins over transport data; no task or timer is added.
#[cfg(any(feature = "grpc", feature = "wasm"))]
pub(crate) async fn credential_event<T>(
    changed: &mut Option<acyclic_auth::credential::CredentialWatch>,
    operation: impl std::future::Future<Output = T>,
) -> Result<T, StreamError> {
    use futures::future::{Either, select};
    futures::pin_mut!(operation);
    if let Some(changed) = changed {
        match select(changed, operation).await {
            Either::Left(_) => Err(StreamError::AccessDenied),
            Either::Right((result, _)) => Ok(result),
        }
    } else {
        Ok(operation.await)
    }
}

/// An authenticated endpoint. The pool owns all protocol, credit and recovery.
pub trait Transport: ProviderPlatform {
    /// The original endpoint owner's terminal event, when the endpoint cannot
    /// reconnect independently of that owner. The pool waits once for this
    /// event and ends pending opens and live interests with its original error.
    /// A missing producer must complete this future too.
    fn termination(&self) -> Option<BoxProviderFuture<'static, StreamError>> {
        None
    }
    /// Opens one bidirectional platform connection from the canonical frame source.
    /// The I/O owner classifies interruptions explicitly; an `Unavailable`
    /// error alone never requests another physical connection.
    fn session(
        &self,
        source: Requests,
    ) -> BoxProviderFuture<'_, Result<Responses<Failure>, Failure>>;
    /// Withdraws an unadmitted write owned wholly by this logical channel.
    /// Synchronous socket writes need no hook; asynchronous platform queues
    /// cancel that write without closing the shared physical connection.
    fn cancel_channel(&self, _channel: wire::SessionChannel) {}
}

/// Shared endpoint handle retained across reconnects.
pub type Endpoint = Arc<dyn Transport>;
#[cfg(not(target_arch = "wasm32"))]
/// Resolves a moved node to its authenticated endpoint, without opening a session.
pub type Connect = dyn Fn(wire::SessionNode) -> BoxProviderFuture<'static, Result<Endpoint, Failure>>
    + Send
    + Sync;
#[cfg(target_arch = "wasm32")]
/// Resolves a moved node using the browser's platform I/O.
pub type Connect =
    dyn Fn(wire::SessionNode) -> BoxProviderFuture<'static, Result<Endpoint, Failure>>;
