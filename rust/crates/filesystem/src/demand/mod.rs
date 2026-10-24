//! Bounded, versioned access to unresolved filesystem sources.

use crate::cancellation::CancellationToken;
use crate::kernel::{NamespacePath, NamespacePathError};
#[cfg(not(target_arch = "wasm32"))]
use crate::performance::WorkBudget;
use crate::performance::{OperationFailure, OperationReceipt, WorkCounters};
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;

mod model;
pub use model::*;
mod cursor;
pub use cursor::SourceCursor;
mod page;
pub use page::*;
mod contracts;
pub use contracts::*;

/// Native provider backed by a held, no-follow directory capability.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native;
#[cfg(test)]
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
mod accounting;

#[cfg(test)]
#[cfg(all(not(acyclic_fs_loom), not(target_arch = "wasm32")))]
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
pub(crate) use admissions::NativeProofAccounting;
#[cfg(target_os = "macos")]
pub(crate) use context::native_demand_read_error;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use context::{CaptureTransientCustody, NativeDemandReadMeter, native_demand_error};
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::NativeMountAllocationCustody;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) use ownership::SourcePayloadOwnership;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use source_work::SourceDemandReadWork;

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
mod paths;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub(crate) use paths::*;

#[cfg(test)]
#[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
mod context_loom_tests;
