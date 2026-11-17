#![deny(unsafe_code)]
#![cfg_attr(
    target_arch = "wasm32",
    allow(
        clippy::arc_with_non_send_sync,
        reason = "public shared handles retain uniform ownership while browser providers stay on their event loop"
    )
)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::cognitive_complexity,
        reason = "unit tests run ordered scenarios over fixtures they built"
    )
)]

//! The durable native host behind the `acyclic-harness` facade.

#[cfg(test)]
acyclic_test_support::counting_allocator!();

// The core's items keep their `crate::` paths here, as in the facade.
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_client as client;
#[cfg(any(not(target_arch = "wasm32"), test, feature = "test-support"))]
use acyclic_harness_core::*;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_hosting as hosting;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_search as search;

#[cfg(not(target_arch = "wasm32"))]
pub mod local;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(not(target_arch = "wasm32"))]
pub use local::{LocalHarness, LocalHarnessConfig};

#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_experiments as experiments;

#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_input_watch as input_watch;

#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_mcp as mcp;

#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_native_settings as native_settings;
