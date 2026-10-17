//! Production and model synchronization share these exact imports.
#[cfg(all(
    acyclic_harness_loom,
    any(
        test,
        all(
            not(target_arch = "wasm32"),
            any(feature = "mcp-stdio", feature = "mcp-http")
        )
    )
))]
pub(crate) use loom::sync::atomic;
#[cfg(acyclic_harness_loom)]
pub(crate) use loom::sync::{Arc, Mutex};
#[cfg(all(
    not(acyclic_harness_loom),
    any(
        test,
        all(
            not(target_arch = "wasm32"),
            any(feature = "mcp-stdio", feature = "mcp-http")
        )
    )
))]
pub(crate) use std::sync::atomic;
#[cfg(not(acyclic_harness_loom))]
pub(crate) use std::sync::{Arc, Mutex};
