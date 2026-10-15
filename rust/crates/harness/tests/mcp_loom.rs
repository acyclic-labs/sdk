//! `cargo rustc --test mcp-loom ... -- --cfg loom` instruments only this target,
//! leaving dependencies' runtimes unchanged. Both modules are production source.
#![cfg(loom)]

pub use acyclic_harness::{Error, Result};

#[path = "../src/mcp/sync.rs"]
pub(crate) mod sync;
mod mcp {
    pub(crate) use crate::sync;
}
#[path = "../src/mcp/install/cancellation.rs"]
mod cancellation;
#[path = "../src/mcp/stdio/native/codec.rs"]
mod codec;
