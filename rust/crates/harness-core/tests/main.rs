//! Original ordinary and instrumented suites share their real root bindings.
#![cfg(test)]

acyclic_test_support::counting_allocator!();

#[cfg(all(not(loom), feature = "test-support"))]
use acyclic_harness_control_checks as agent_control_mapping;
#[cfg(not(loom))]
mod agent_model;

pub use acyclic_harness_core::Error;
#[cfg(all(loom, feature = "model-checking"))]
pub use acyclic_harness_core::{ProcessError, Result};
#[cfg(all(loom, feature = "model-checking"))]
#[path = "../src/sync/owned.rs"]
pub(crate) mod owned;
#[cfg(all(loom, feature = "model-checking"))]
pub(crate) mod sync {
    include!("../src/sync/arc.rs");
    include!("../src/sync/atomic.rs");
    pub(crate) use crate::owned;
}
#[cfg(all(loom, feature = "model-checking"))]
mod mcp_loom;
#[cfg(all(loom, feature = "model-checking"))]
use mcp_loom::mcp;
#[cfg(all(loom, feature = "model-checking"))]
use sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[cfg(all(loom, feature = "model-checking"))]
mod skill_materializer_loom;

#[path = "computed_ui_records.rs"]
mod computed_ui_records;
#[path = "contract_error_cases.rs"]
mod contract_error_cases;
#[path = "conversation_contract.rs"]
mod conversation_contract;
#[path = "model_usage_cost.rs"]
mod model_usage_cost;
#[path = "projection_selection.rs"]
mod projection_selection;
