//! Bounded, versioned access to unresolved filesystem sources.

use crate::cancellation::CancellationToken;

use crate::kernel::{NamespacePath, NamespacePathError};

use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters};

#[cfg(any(feature = "native-watch", target_arch = "wasm32"))]
use crate::WorkError;

use serde::{Deserialize, Serialize};

use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::NativeOperationKind;

mod contracts;
mod cursor;
mod filtered;
mod model;
/// Held native filesystem source.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native;
mod ownership;
mod page;
mod paths;
mod publication;
#[cfg(all(test, feature = "native-watch", not(target_arch = "wasm32")))]
mod tests;
mod watch;
#[cfg(not(target_arch = "wasm32"))]
mod work;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub use contracts::DemandDirectoryObserver;
pub(crate) use contracts::*;
pub use contracts::{
    DemandError, DemandFile, DemandResult, DemandSource, SourceChange, SourceChangeSink, read_file,
};
pub use cursor::SourceCursor;
pub use filtered::FilteredDemandSource;
pub use model::*;
#[cfg(not(target_arch = "wasm32"))]
pub use ownership::NativeMountAllocationCustody;
pub(crate) use ownership::*;
pub(crate) use page::*;
pub use page::{SourceDirectoryEntries, SourceDirectoryPage};
pub(crate) use paths::*;
pub use publication::{SourceDirectoryPublication, abort_publication, accept_publication};
pub(crate) use watch::*;
pub use watch::{SourceWatch, SourceWatchAllocationCustody, fence_watch, stop_watch};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use work::*;
#[cfg(not(target_arch = "wasm32"))]
pub use work::{DemandWork, NativeSourceWork};
