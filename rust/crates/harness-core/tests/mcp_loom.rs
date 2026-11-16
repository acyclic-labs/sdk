//! The dedicated model command sets ACYCLIC_LOOM=1.
//! Both instrumented modules below are production source.
#![cfg(loom)]

#[path = "../../harness-mcp/src/install/cancellation.rs"]
mod cancellation;
#[path = "../../mcp/src/codec.rs"]
mod codec;
