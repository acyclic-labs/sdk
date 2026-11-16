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

// The core's items keep their `crate::` paths here, as in the facade.
use acyclic_harness_core::*;
use acyclic_harness_client as client;
use acyclic_harness_hosting as hosting;

#[cfg(not(target_arch = "wasm32"))]
pub mod local;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use local::{LocalHarness, LocalHarnessConfig};
