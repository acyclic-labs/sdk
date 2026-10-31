//! Platform I/O for the shared session pool, without another session protocol.
use crate::{BoxProviderFuture, BoxProviderStream, ProviderPlatform, StreamError, wire};
use std::sync::Arc;

/// Canonical inbound frames from one physical platform connection.
pub type Responses = BoxProviderStream<'static, Result<super::super::Frame, StreamError>>;
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
    /// Opens one bidirectional platform connection from the canonical frame source.
    fn session(&self, source: Requests) -> BoxProviderFuture<'_, Result<Responses, StreamError>>;
    /// Withdraws an unadmitted write owned wholly by this logical channel.
    /// Synchronous socket writes need no hook; asynchronous platform queues
    /// cancel that write without closing the shared physical connection.
    fn cancel_channel(&self, _channel: wire::SessionChannel) {}
}

/// Shared endpoint handle retained across reconnects.
pub type Endpoint = Arc<dyn Transport>;
#[cfg(not(target_arch = "wasm32"))]
/// Resolves a moved node to its authenticated endpoint, without opening a session.
pub type Connect = dyn Fn(wire::SessionNode) -> BoxProviderFuture<'static, Result<Endpoint, StreamError>>
    + Send
    + Sync;
#[cfg(target_arch = "wasm32")]
/// Resolves a moved node using the browser's platform I/O.
pub type Connect =
    dyn Fn(wire::SessionNode) -> BoxProviderFuture<'static, Result<Endpoint, StreamError>>;
