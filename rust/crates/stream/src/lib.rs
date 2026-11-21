#![doc = include_str!("../README.md")]

extern crate self as acyclic_stream;

pub use acyclic_stream_contract::*;

use acyclic_stream_wire::wire;
#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod browser;
pub mod conformance;
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
#[cfg(not(target_arch = "wasm32"))]
pub mod http;
#[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
mod journal_provider;
#[cfg(not(target_arch = "wasm32"))]
pub mod postgres;
#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub mod session;
#[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
pub use journal_provider::{JournalStream, JournalStreamLimits};
#[cfg(not(target_arch = "wasm32"))]
mod local;
mod memory;
mod observation_lock;
mod replicated;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use browser::{BrowserStream, BrowserStreamLimits};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::js_error as wasm_error;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::{
    InjectedStream, WasmFollow, WasmProviderRequest, WasmSessionAuthority, WasmSessionService,
    WasmStream, serve_wasm_session,
};
#[cfg(any(feature = "test-support", all(test, not(target_arch = "wasm32"))))]
#[doc(hidden)]
pub mod codec_fuzz;
#[cfg(test)]
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod follow_tests;
#[cfg(test)]
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod head_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[doc(hidden)]
pub use acyclic_stream_contract_bytes::test_meter;
#[cfg(test)]
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod test_trace;

#[cfg(test)]
#[cfg(all(loom, not(target_arch = "wasm32")))]
#[path = "memory/recovery_model.rs"]
/// Bounded schedule verification of production query concurrency paths.
pub mod verification;
#[cfg(test)]
#[cfg(all(loom, not(target_arch = "wasm32")))]
#[doc(hidden)]
pub use local::replay_model::{
    negative_control_acknowledges_an_unflushed_original, replay_and_flush_preserve_durability,
};
#[cfg(test)]
#[cfg(all(loom, not(target_arch = "wasm32")))]
#[doc(hidden)]
pub use memory::browser_follow_model as browser_follow_verification;
#[cfg(test)]
#[cfg(all(loom, not(target_arch = "wasm32")))]
mod departure_model;
#[cfg(test)]
#[cfg(loom)]
mod loom_tests;

#[cfg(all(not(target_arch = "wasm32"), feature = "test-support"))]
pub use local::{CutSimulationError, PersistenceCut, fail_cut_truncation};
#[cfg(not(target_arch = "wasm32"))]
pub use local::{
    LocalDurability, LocalStream, LocalStreamError, LocalStreamFailure, LocalStreamLimits,
    deferring_durability, deferring_provider_durability, requiring_durability,
};
pub use memory::{CommitAdmission, MemoryLimits, MemoryStream, admit_commit};

// Contract unit tests that exercise the contract through the providers that build on it.
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "changes_tests.rs"]
mod changes_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "follow_filter_tests.rs"]
mod follow_filter_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "journal_tests.rs"]
mod journal_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "lib_tests.rs"]
mod lib_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "pages_tests.rs"]
mod pages_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "pending_tests.rs"]
mod pending_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "persistence_tests.rs"]
mod persistence_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "provider_cursor_tests.rs"]
mod provider_cursor_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "query_tests.rs"]
mod query_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "receipt_tests.rs"]
mod receipt_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "request_tests.rs"]
mod request_tests;
#[cfg(test)]
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[path = "search_tests.rs"]
mod search_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "session_wire_tests.rs"]
mod session_wire_tests;
#[cfg(test)]
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[path = "sources_tests.rs"]
mod sources_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "testing_tests.rs"]
mod testing_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "trace_tests.rs"]
mod trace_tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();
