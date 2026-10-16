//! Platform I/O for the shared session pool, without another session protocol.
use crate::{BoxProviderFuture, BoxProviderStream, ProviderPlatform, StreamError, wire};
use std::sync::Arc;

/// Canonical inbound frames from one physical platform connection.
pub type Responses = BoxProviderStream<'static, Result<wire::SessionFrame, StreamError>>;
/// Canonical outbound frames; dropping this source cancels the connection.
pub type Requests = BoxProviderStream<'static, wire::SessionFrame>;

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
