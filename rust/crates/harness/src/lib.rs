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
#![doc = include_str!("../README.md")]
#[cfg(test)]
acyclic_test_support::counting_allocator!();

// Everything below the client and host compositions lives in
// `acyclic-harness-core`; its public items keep their paths here.
pub use acyclic_harness_core::*;
#[cfg(not(target_arch = "wasm32"))]
/// Attached devices: a user's machine runs the `shell` commands agents dispatch to it.
pub use acyclic_harness_devices as devices;
/// Experiment planning and analysis.
pub use acyclic_harness_experiments as experiments;
/// Shared host HTTP boundary.
pub use acyclic_harness_http as http;
#[cfg(not(target_arch = "wasm32"))]
/// Host input invalidation.
pub use acyclic_harness_input_watch as input_watch;
/// MCP clients over the ordinary tool, execution and interaction owners.
pub use acyclic_harness_mcp as mcp;
#[cfg(not(target_arch = "wasm32"))]
/// Native settings admission.
pub use acyclic_harness_native_settings as native_settings;
/// Object content stores.
pub use acyclic_harness_objects as objects;
/// Cached OIDC verification.
pub use acyclic_harness_oidc as oidc;
/// Provider-neutral web search, with credentials confined to the host.
pub use acyclic_harness_search as search;
/// Credential-free, bounded HTTP source retrieval.
pub use acyclic_harness_search::url_fetch;

pub mod hosting;

pub mod client;
#[cfg(not(target_arch = "wasm32"))]
pub mod completion;
#[cfg(feature = "conformance")]
pub mod conformance;
#[cfg(not(target_arch = "wasm32"))]
pub mod local;
pub mod preset;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
#[cfg(not(target_arch = "wasm32"))]
pub mod tui;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::BrowserContentReader;
#[cfg(all(feature = "browser", target_arch = "wasm32"))]
pub use wasm::{WasmAgentProviders, WasmAgentRegistry, WasmMachinesWorkspace, WasmObjects};

#[cfg(not(target_arch = "wasm32"))]
pub use local::{LocalHarness, LocalHarnessConfig};

#[cfg(not(target_arch = "wasm32"))]
pub mod host_hooks;
#[cfg(not(target_arch = "wasm32"))]
pub use local::host_workspace;
