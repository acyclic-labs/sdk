#![doc = include_str!("../README.md")]

/// Portable public `AgentHost` vectors, also built independently for browser wasm.
pub use acyclic_agent_host_conformance as agent_host;

/// Production actor-client cases shared by local and hosted session adapters.
#[cfg(all(feature = "client-session", not(target_arch = "wasm32")))]
pub mod client;
/// Complete filesystem workload taxonomy, selectors, and portable vectors.
pub mod filesystem;
/// Provider-neutral inference assertions over the public streaming model seam.
pub mod inference;
/// Machine-readable cross-language conformance reports and qualification receipts.
pub mod runner;

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs};
use acyclic_objects::Blobs;
#[cfg(test)]
use acyclic_stream::StreamProvider;

/// Canonical language-neutral Objects conformance inventory.
pub const OBJECTS_SUITE: &[u8] = acyclic_objects::conformance::SUITE;

/// Canonical language-neutral harness conformance inventory.
pub const HARNESS_SUITE: &[u8] = acyclic_harness::conformance::LEGACY_MARKER_INVENTORY;

/// Canonical language-neutral Machines conformance inventory.
pub const MACHINES_SUITE: &[u8] = acyclic_machines::conformance::SUITE;
const _: () = assert!(!MACHINES_SUITE.is_empty(), "conformance inventory is empty");

/// Canonical language-neutral Stream conformance inventory.
pub const STREAM_SUITE: &[u8] = acyclic_stream::conformance::SUITE;

/// Exercises the minimum customer-level filesystem semantics.
pub async fn filesystem_smoke<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    provider: &Fs<A, O>,
) -> Result<(), String> {
    let workspace = provider
        .create_workspace(
            "conformance",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(|error| error.to_string())?;
    workspace
        .write("/answer", bytes::Bytes::from_static(b"42"))
        .await
        .map_err(|error| error.to_string())?;
    let observed = workspace
        .read(
            "/answer",
            2,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(|error| error.to_string())?;
    (observed.as_ref() == b"42")
        .then_some(())
        .ok_or_else(|| "written value missing".into())
}

/// Runs the canonical Stream suite against a fresh, disposable provider.
pub async fn stream(backend: &dyn acyclic_stream::conformance::Backend) -> Result<(), String> {
    acyclic_stream::conformance::verify(backend)
        .await
        .map_err(|error| error.to_string())
}

/// Runs the canonical Objects suite against two disposable accounts of one service.
///
/// The suite keeps its content under the `conformance` namespace.
pub async fn objects(first: &dyn Blobs, second: &dyn Blobs) -> Result<(), String> {
    acyclic_objects::conformance::verify(first, second, "conformance")
        .await
        .map_err(|error| error.to_string())
}

/// Runs the shared public Machines suite against a fresh provider.
pub async fn machines(backend: &dyn acyclic_machines::conformance::Backend) -> Result<(), String> {
    acyclic_machines::conformance::verify(backend).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_machines::SimulatedMachines;
    use acyclic_stream::MemoryStream;
    use std::sync::Arc;

    #[tokio::test]
    async fn family_memory_providers_conform() -> Result<(), String> {
        let stream_provider = MemoryStream::default();
        let store = acyclic_objects::Store::new(acyclic_objects::MemoryMedium::default());
        let objects_provider = Arc::new(
            store
                .account(b"filesystem")
                .map_err(|error| error.to_string())?,
        );
        let filesystem = acyclic_fs::Fs::from_memory_providers(
            Arc::new(stream_provider.clone()),
            Arc::clone(&objects_provider),
        );
        filesystem_smoke(&filesystem).await?;

        let stream_children = stream_provider
            .children_page(acyclic_stream::ChildrenPageRequest {
                tail_stream: None,
                parent: None,
                limit: 8,
                after: None,
                hierarchy_version: None,
            })
            .await
            .map_err(|error| error.to_string())?
            .children;
        if !stream_children
            .iter()
            .any(|child| child.path.as_str() == "fs")
        {
            return Err("filesystem did not publish through the profile's public Stream".into());
        }
        let filesystem_objects = objects_provider
            .list(None, 128)
            .await
            .map_err(|error| error.to_string())?;
        if filesystem_objects.digests.is_empty() {
            return Err(
                "filesystem did not admit objects through the profile's public Objects".into(),
            );
        }

        stream(&acyclic_stream::conformance::Fixture::new(
            stream_provider.clone(),
        ))
        .await?;
        objects(
            &*objects_provider,
            &store
                .account(b"second")
                .map_err(|error| error.to_string())?,
        )
        .await?;
        machines(&acyclic_machines::conformance::Fixture::new(
            SimulatedMachines::default(),
        ))
        .await?;
        Ok(())
    }
}
