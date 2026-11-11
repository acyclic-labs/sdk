//! Bounded, versioned access to unresolved filesystem sources.

use crate::cancellation::CancellationToken;
use crate::kernel::{NamespacePath, NamespacePathError};
#[cfg(not(target_arch = "wasm32"))]
use crate::performance::WorkBudget;
use crate::performance::{OperationFailure, OperationReceipt, WorkCounters};
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;

/// One identity for an unresolved source node and its authored captures.
pub(crate) fn source_file_id(
    namespace: &[u8; 16],
    identity: &[u8; 32],
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<crate::FileId, crate::FsError> {
    let domain = b"acyclic-fs-lazy-source-file-id-v1\0";
    accounting.charge(WorkCounters {
        bytes_hashed: domain.len() as u64 + 48,
        ..WorkCounters::UNCHARGED
    })?;
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(namespace);
    hash.update(identity);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok(crate::FileId::from_bytes(bytes))
}

mod model;
pub use model::*;
mod cursor;
pub use cursor::SourceCursor;
mod page;
pub use page::*;
mod contracts;
pub use contracts::*;
mod computed;
mod shared;
pub use computed::*;
mod fold;
pub use fold::*;
mod empty;
pub use empty::Empty;
mod routed;
pub use routed::Routed;
#[cfg(test)]
mod computed_tests;

/// Native provider backed by a held, no-follow directory capability.
#[cfg(not(target_arch = "wasm32"))]
pub mod native;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
mod accounting;

#[cfg(test)]
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod scope_tests;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod write_probe;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use write_probe::WriteProbe;

#[cfg(not(target_arch = "wasm32"))]
mod admissions;

#[cfg(not(target_arch = "wasm32"))]
mod reservations;

#[cfg(not(target_arch = "wasm32"))]
mod context;

#[cfg(not(target_arch = "wasm32"))]
mod ownership;

#[cfg(not(target_arch = "wasm32"))]
mod source_work;
#[cfg(not(target_arch = "wasm32"))]
pub use accounting::DemandWork;
#[cfg(not(target_arch = "wasm32"))]
use admissions::Admissions;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use admissions::NativeAllocationAccounting;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use admissions::ViewReadCompletion;
#[cfg(target_os = "macos")]
pub(crate) use context::native_demand_read_error;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use context::{CaptureTransientCustody, NativeDemandReadMeter, native_demand_error};
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::NativeMountAllocationCustody;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use ownership::SourcePayloadOwnership;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use source_work::SourceDemandReadWork;

#[cfg(not(target_arch = "wasm32"))]
mod paths;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use paths::*;

#[cfg(test)]
#[cfg(all(loom, not(target_arch = "wasm32")))]
mod context_loom_tests;
