#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::cognitive_complexity,
        reason = "unit tests run ordered scenarios over fixtures they built"
    )
)]
#![doc = include_str!("../README.md")]
#[cfg(test)]
acyclic_test_support::counting_allocator!();

// Everything below the client and host compositions lives in
// `acyclic-harness-core`; its public items keep their paths here.
pub use acyclic_harness_core::*;
/// Provider-neutral web search, with credentials confined to the host.
pub use acyclic_harness_search as search;
/// Credential-free, bounded HTTP source retrieval.
pub use acyclic_harness_search::url_fetch;

pub use acyclic_harness_hosting as hosting;

pub mod client;
#[cfg(not(target_arch = "wasm32"))]
pub mod completion;
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_harness_local::local;
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_harness_local::server;
#[cfg(any(test, feature = "test-support"))]
pub use acyclic_harness_local::test_support;
#[cfg(feature = "conformance")]
pub use acyclic_harness_suite::conformance;
#[cfg(not(target_arch = "wasm32"))]
pub mod tui;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use acyclic_harness_browser::BrowserContentReader;
#[cfg(all(feature = "browser", target_arch = "wasm32"))]
pub use acyclic_harness_browser::{
    WasmAgentProviders, WasmAgentRegistry, WasmMachinesWorkspace, WasmObjects,
};

#[cfg(not(target_arch = "wasm32"))]
pub use local::{LocalHarness, LocalHarnessConfig};

#[cfg(not(target_arch = "wasm32"))]
pub mod host_hooks;
#[cfg(not(target_arch = "wasm32"))]
pub use local::host_workspace;
