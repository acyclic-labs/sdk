//! Provider-neutral identities for other SDK resources.

use crate::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize};

mod provider;
pub use provider::ProviderRef;
mod resource;
pub use resource::{ResourceKind, ResourceRef};
mod typed;
pub use typed::{
    ArtifactRef, CheckpointRef, ContextRef, RunRef, SandboxRef, StreamRef, WorkspaceRef,
};
mod generation;
pub use generation::GenerationRef;
#[cfg(test)]
mod tests;

/// Copies a known metadata field immediately after its own allocation admission.
#[cfg(feature = "filesystem")]
pub(crate) fn copy_metadata_string(
    source: &str,
    io: &crate::filesystem::PreparationIo,
) -> Result<String> {
    let copying = acyclic_fs::WorkCounters {
        bytes_copied: source.len() as u64,
        ..Default::default()
    };
    io.preflight(copying)?;
    io.allocation_attempt(source.len() as u64)?;
    let value = source.to_owned();
    io.charge(copying)?;
    Ok(value)
}

fn resource_validation_error(
    reason: &'static str,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_ref()
        .is_some_and(|work| work.original_native_work().is_some())
    {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    Error::Invalid(reason.into())
}
