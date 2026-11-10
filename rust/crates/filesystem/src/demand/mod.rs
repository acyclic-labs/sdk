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

mod empty;
pub use empty::Empty;
mod routed;
pub use routed::Routed;
mod computed;
pub use computed::{
    Computed, PinnedView, View, ViewPage, ViewProvider, ViewRecordSink, ViewRequest,
};

// Keep delegation on the caller's original native ledger. Standalone callers
// receive and merge the same receipt, including work completed on failure.
macro_rules! source_call {
    ($ctx:expr, $cancel:expr, |$work:ident| $call:expr) => {{
        async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some($work) = $ctx.native_work() {
                return $call.await;
            }
            let $work = $ctx.remaining().map_err(DemandError::from)?;
            contracts::accept_source_result($call.await, $ctx)
        }
        .await
    }};
}
pub(crate) use source_call;

macro_rules! source_sync {
    ($ctx:expr, |$work:ident| $call:expr) => {{
        #[cfg(not(target_arch = "wasm32"))]
        let result = if let Some($work) = $ctx.native_work() {
            $call
        } else {
            let $work = $ctx.remaining().map_err(DemandError::from)?;
            contracts::accept_source_result($call, $ctx)
        };
        #[cfg(target_arch = "wasm32")]
        let result = {
            let $work = $ctx.remaining().map_err(DemandError::from)?;
            contracts::accept_source_result($call, $ctx)
        };
        result
    }};
}
pub(crate) use source_sync;

/// Native provider backed by a held, no-follow directory capability.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub mod native;
#[cfg(test)]
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
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
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub(crate) use admissions::ViewReadCompletion;
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
#[cfg(all(loom, not(target_arch = "wasm32")))]
mod context_loom_tests;
