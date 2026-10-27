//! The same public [`AgentHost`](acyclic_harness_core::agent::AgentHost) vectors
//! for native, browser and hosted providers.
//!
//! A backend supplies disposable storage that can be closed and reopened. All
//! fault injection and assertions belong here, never in a backend adapter.

mod admission;
mod approvals;
pub mod fixtures;
mod identity;
mod observe;
mod publication;
mod roots;
mod vectors;
mod wire;

use acyclic_actors::guest::Handler;
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, StreamProvider};
use std::sync::Arc;

/// A vector failure, including the provider's original diagnostic.
#[cfg(not(target_arch = "wasm32"))]
pub type Failure = Box<dyn std::error::Error + Send + Sync>;
/// A vector failure retaining the guest's event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type Failure = Box<dyn std::error::Error>;
/// A conformance operation's result.
pub type Result<T = ()> = std::result::Result<T, Failure>;

/// A fresh consistency domain. Every `open` must reopen the same durable data;
/// no provider session or host survives a crash vector.
pub trait Storage: ProviderPlatform {
    /// Open a new session on this vector's retained storage.
    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>>;
}

/// Backend setup only. It must not interpret or replace any vector.
pub trait Backend: ProviderPlatform {
    /// A fresh, disposable domain for one vector.
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>>;
    /// Install the host's guest boundary. Native hosts can return `handler`;
    /// guest hosts must pass it through their real codec/transport.
    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>>;
}

/// Use the canonical guest wire boundary around a handler. The closure retains
/// the guest until its call completes, and dropping a call drops its turn.
pub fn remote(handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
    wire::remote(handler)
}

/// Execute every portable vector against fresh backend storage.
pub async fn verify(backend: &dyn Backend) -> Result {
    identified("controls", vectors::controls(backend).await)?;
    identified("spawn", vectors::spawn_cut(backend).await)?;
    identified("publication", publication::verify(backend).await)?;
    identified("admission", admission::verify(backend).await)?;
    identified("identity", identity::verify(backend).await)?;
    identified("roots", roots::verify(backend).await)?;
    identified("approvals", approvals::verify(backend).await)?;
    identified("guest wire", wire::verify(backend).await)?;
    identified("restart", vectors::restart(backend).await)?;
    Ok(())
}

/// Additional real-issuer control, using the same host fixture and production
/// admission as the portable identity vectors. The expected subject is supplied
/// by the trusted test configuration, independently of verification output.
pub async fn verify_oidc(
    backend: &dyn Backend,
    verifier: &acyclic_harness_core::oidc::Verifier,
    token: &str,
    expected_subject: &str,
    now_unix_seconds: u64,
) -> Result {
    identified(
        "real OIDC identity",
        identity::external(backend, verifier, token, expected_subject, now_unix_seconds).await,
    )
}

fn identified(vector: &str, result: Result) -> Result {
    result.map_err(|error| format!("agent-host {vector}: {error}").into())
}
