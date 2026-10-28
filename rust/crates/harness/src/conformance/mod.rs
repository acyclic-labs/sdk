//! The same public [`AgentHost`](acyclic_harness_core::agent::AgentHost) vectors
//! for native, browser and hosted providers.
//!
//! A backend supplies disposable storage that can be closed and reopened. All
//! fault injection and assertions belong here, never in a backend adapter.

mod admission;
mod approvals;
mod dispatcher;
pub mod fixtures;
mod identity;
mod interrupt;
mod observe;
mod performance;
pub mod protocol;
mod publication;
mod reply;
mod roots;
mod vectors;
mod wire;

use acyclic_actors::guest::Handler;
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, StreamProvider};
use std::sync::Arc;

pub use performance::{Measurement, Scale, ScaleMeasurement, bench, scale};

/// Historical package marker inventory retained for existing receipt producers.
/// It does not establish execution of the current AgentHost vectors.
pub const LEGACY_MARKER_INVENTORY: &[u8] = include_bytes!("../../conformance/legacy-harness.json");

/// Register the same AgentHost suite for an async factory returning a backend.
/// Use `suite!(local, Local::fresh)` to register several backends in one file.
pub use crate::agent_conformance_suite as suite;

/// Implementation of [`suite!`]; factories provide disposable backend storage.
#[macro_export]
macro_rules! agent_conformance_suite {
    ($factory:path) => {
        $crate::agent_conformance_suite!(agent_conformance, $factory);
    };
    ($name:ident, $factory:path) => {
        #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
        #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
        async fn $name() -> $crate::conformance::Result {
            let backend = $factory().await?;
            $crate::conformance::verify(&backend).await
        }
    };
}

/// A vector failure, including the provider's original diagnostic.
#[cfg(not(target_arch = "wasm32"))]
pub type Failure = Box<dyn std::error::Error + Send + Sync>;
/// A vector failure retaining the guest's event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type Failure = Box<dyn std::error::Error>;
/// A conformance operation's result.
pub type Result<T = ()> = std::result::Result<T, Failure>;

/// Object primitives used by the filesystem-backed host vectors.
pub struct Objects {
    /// An empty blob account belonging only to this vector.
    pub provider: Arc<dyn acyclic_objects::Blobs>,
    /// The provider's declared durability.
    pub capabilities: acyclic_fs::EmbeddedCapabilities,
}

type CachedObjects =
    acyclic_fs::CachedObjectStore<acyclic_fs::LogicalObjectStore<dyn acyclic_objects::Blobs>>;
/// The canonical native publication staging and immutable cache.
#[cfg(not(target_arch = "wasm32"))]
pub type ObjectStore = acyclic_fs::staged_objects::StagedObjects<CachedObjects>;
/// The portable immutable cache for guest fixtures.
#[cfg(target_arch = "wasm32")]
pub type ObjectStore = CachedObjects;

impl Objects {
    /// Use the shared filesystem composition; native writes stage until publication.
    pub fn into_store(self, storage: &dyn Storage) -> Result<ObjectStore> {
        let cached = acyclic_fs::CachedObjectStore::new(
            acyclic_fs::LogicalObjectStore::new(self.provider),
            acyclic_fs::ObjectCacheOptions::default(),
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        return Ok(acyclic_fs::staged_objects::StagedObjects::open(
            cached,
            storage.staging_directory().to_owned(),
        ));
        #[cfg(target_arch = "wasm32")]
        {
            let _ = storage;
            Ok(cached)
        }
    }

    /// The original memory object fixture, for stream-only qualification.
    pub fn memory() -> Self {
        Self {
            provider: Arc::new(acyclic_objects::MemoryObjects::memory()),
            capabilities: acyclic_fs::EmbeddedCapabilities::MEMORY,
        }
    }
}

/// A fresh consistency domain. Every `open` must reopen the same durable data;
/// no provider session or host survives a crash vector.
pub trait Storage: ProviderPlatform {
    /// Existing private spill directory, retained while this fixture is in use.
    #[cfg(not(target_arch = "wasm32"))]
    fn staging_directory(&self) -> &std::path::Path;
    /// Open a new session on this vector's retained storage.
    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>>;
    /// Provision an empty object domain for the filesystem-backed vectors.
    fn objects(&self) -> BoxProviderFuture<'_, Result<Objects>>;
}

/// Backend setup only. It must not interpret or replace any vector.
pub trait Backend: ProviderPlatform {
    /// A fresh, disposable domain for one vector.
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>>;
    /// Install the host's guest boundary. Native hosts can return `handler`;
    /// guest hosts must pass it through their real codec/transport.
    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>>;
    /// A real hosted endpoint using the suite's shared model/tool fixture and
    /// `policy`. It must enter through the backend's production composition.
    fn serving(
        &self,
        policy: acyclic_harness_core::approvals::ApprovalPolicy,
    ) -> BoxProviderFuture<'_, Result<Box<dyn protocol::Endpoint>>>;
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
    identified("reply", reply::verify(backend).await)?;
    identified("publication", publication::verify(backend).await)?;
    identified("admission", admission::verify(backend).await)?;
    identified("identity", identity::verify(backend).await)?;
    identified("roots", roots::verify(backend).await)?;
    identified("approvals", approvals::verify(backend).await)?;
    identified("guest wire", wire::verify(backend).await)?;
    identified("runtime interrupt (G41)", interrupt::verify(backend).await)?;
    identified(
        "activation checkpoint (G40)",
        dispatcher::verify(backend).await,
    )?;
    identified("restart", vectors::restart(backend).await)?;
    identified("serving protocol", protocol::verify(backend).await)?;
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
