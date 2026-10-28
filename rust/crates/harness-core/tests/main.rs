//! Original ordinary and instrumented suites share their real root bindings.
#[cfg(not(loom))]
mod agent_model;
#[cfg(not(loom))]
mod platform_shell;
#[cfg(all(not(loom), feature = "filesystem-local", feature = "native-execution"))]
mod git_executor;

#[cfg(all(loom, feature = "model-checking"))]
pub use acyclic_harness_core::{Error, ProcessError, Result};
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
mod skill_materializer_loom;
