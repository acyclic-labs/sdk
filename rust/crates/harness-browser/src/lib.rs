//! Browser bindings for the shared agent runtime and authority.
#![cfg(target_arch = "wasm32")]
#![deny(unsafe_code)]
#![allow(
    clippy::arc_with_non_send_sync,
    reason = "public shared handles retain uniform ownership while browser providers stay on their event loop"
)]

use acyclic_harness_core::*;
use acyclic_harness_hosting as hosting;

mod wasm;

pub use wasm::BrowserContentReader;
#[cfg(feature = "browser")]
pub use wasm::{WasmAgentProviders, WasmAgentRegistry, WasmMachinesWorkspace, WasmObjects};
