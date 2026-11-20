//! Local harness scenarios driven through the public API.
//!
//! The prelude mirrors `acyclic-harness-local`'s own crate root, so the
//! scenarios keep the `crate::` paths they were written with.
#![cfg(not(target_arch = "wasm32"))]

pub use acyclic_harness_client as client;
pub use acyclic_harness_core::*;
pub use acyclic_harness_hosting as hosting;
pub use acyclic_harness_local::{LocalHarness, LocalHarnessConfig, local, server, test_support};
pub use acyclic_harness_search as search;

mod local_tests;
