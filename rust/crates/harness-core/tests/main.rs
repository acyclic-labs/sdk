//! Instrumented (loom) suites share their real root bindings.
#![cfg(test)]

acyclic_test_support::counting_allocator!();

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
