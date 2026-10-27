//! The dedicated model command sets RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh.
//! Both instrumented modules below are production source.
#![cfg(loom)]

pub use acyclic_harness_core::{Error, ProcessError, Result};

pub(crate) mod sync {
    include!("../src/sync/arc.rs");
    include!("../src/sync/atomic.rs");
}
mod mcp {
    pub(crate) use crate::sync;
}
#[path = "../src/mcp/install/cancellation.rs"]
mod cancellation;
#[path = "../src/mcp/stdio/native/codec.rs"]
mod codec;
