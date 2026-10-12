//! Bounded, versioned access to unresolved filesystem sources.

use crate::cancellation::CancellationToken;
use crate::kernel::{NamespacePath, NamespacePathError};
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;

mod cursor;
mod model;
pub use cursor::SourceCursor;
mod contracts;
mod filtered;
pub use contracts::*;
pub use filtered::FilteredDemandSource;
pub use model::*;
/// Native provider backed by a held, no-follow directory capability.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native;
#[cfg(all(test, feature = "native-watch", not(target_arch = "wasm32")))]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
mod source_work;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use source_work::SourceDemandReadWork;

#[cfg(not(target_arch = "wasm32"))]
mod accounting;
#[cfg(not(target_arch = "wasm32"))]
pub use accounting::DemandWork;

#[cfg(not(target_arch = "wasm32"))]
mod reservations;

#[cfg(not(target_arch = "wasm32"))]
mod admissions;
#[cfg(not(target_arch = "wasm32"))]
use admissions::Admissions;

#[cfg(not(target_arch = "wasm32"))]
mod context;
#[cfg(target_os = "macos")]
pub(crate) use context::native_demand_read_error;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use context::{CaptureTransientCustody, NativeDemandReadMeter, native_demand_error};

#[cfg(not(target_arch = "wasm32"))]
mod ownership;
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::NativeMountAllocationCustody;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use ownership::SourcePayloadOwnership;

#[cfg(not(target_arch = "wasm32"))]
mod paths;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use paths::*;
