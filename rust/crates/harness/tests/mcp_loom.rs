//! The dedicated model command sets RUSTFLAGS="--cfg acyclic_harness_loom".
//! Both instrumented modules below are production source.
#![cfg(acyclic_harness_loom)]

pub use acyclic_harness::{Error, Result};

#[path = "../src/sync/primitives.rs"]
pub(crate) mod sync;
mod mcp {
    pub(crate) use crate::sync;
}
#[path = "../src/mcp/install/cancellation.rs"]
mod cancellation;
#[path = "../src/mcp/stdio/native/codec.rs"]
mod codec;
