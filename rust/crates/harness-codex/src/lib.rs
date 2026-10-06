//! An [`acyclic_harness::executor::Executor`] that hands the whole turn to the
//! `OpenAI` Codex CLI (`codex exec --json`).
//!
//! Codex owns context, compaction and work splitting. This crate supplies what
//! a consumer of the stock executor already relies on: metered model calls
//! through a local Responses proxy, the consumer's tool registry over MCP, a
//! private `CODEX_HOME`, step and deadline limits, and journal records.
//!
//! The supported Codex version is pinned in [`CODEX_VERSION`]; the recorded
//! fixtures under `fixtures/codex-<version>` are the contract the parser is
//! tested against. See `DESIGN.md` for the full plan and phase gates.

// Codex runs inside Linux sandboxes; process control uses Unix process groups
// at runtime. Keep the public API visible to rustdoc on every target so the
// Rust-owned reference bundle can describe this crate even when the selected
// documentation host is Windows. Non-Unix builds retain the same API and use
// child-level termination in the private process helper.
#![cfg(any(unix, doc))]
#![cfg_attr(test, allow(clippy::panic, clippy::indexing_slicing))]

pub mod config;
pub mod events;
pub mod executor;
pub mod mcp;
pub mod meter;
mod process;
pub mod proxy;

pub use config::HomeConfig;
pub use events::{CodexEvent, CodexItem, CodexUsage, ItemKind, Transcript};
pub use executor::{CodexConfig, CodexExecutor, CodexObserver, Upstream};
pub use meter::{MeterVerdict, UsageMeter};

/// The only Codex CLI version this crate is qualified against.
pub const CODEX_VERSION: &str = "0.155.1";

/// The executor identity recorded in `Started` digests and turn metadata.
pub const EXECUTOR_ID: &str = "acyclic.codex.v1";
