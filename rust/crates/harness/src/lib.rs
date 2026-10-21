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
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "unit tests run ordered scenarios over fixtures they built"
    )
)]
#![doc = include_str!("../README.md")]

// Everything below the client and host compositions lives in
// `acyclic-harness-core`; its public items keep their paths here.
pub use acyclic_harness_core::*;
/// Provider-neutral web search, with credentials confined to the host.
pub use acyclic_harness_search as search;

/// Cargo-provided build metadata, isolated from ambient user environment discovery.
#[doc(hidden)]
pub mod build_metadata;

pub mod client;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub mod local;
#[cfg(all(test, feature = "hooks", not(target_arch = "wasm32")))]
mod mcp_identity_tests;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
#[cfg(feature = "tui")]
pub mod tui;
#[cfg(feature = "tui")]
pub mod tui_config;
#[cfg(feature = "tui")]
pub mod tui_files;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::BrowserContentReader;

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local::{LocalHarness, LocalHarnessConfig};
