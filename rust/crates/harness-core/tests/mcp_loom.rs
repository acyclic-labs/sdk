//! The dedicated model command sets ACYCLIC_LOOM=1.
//! Both instrumented modules below are production source.
#![cfg(loom)]

pub(crate) mod mcp {
    pub(crate) use crate::sync;
}
#[path = "../src/mcp/install/cancellation.rs"]
mod cancellation;
#[path = "../../mcp/src/codec.rs"]
mod codec;
