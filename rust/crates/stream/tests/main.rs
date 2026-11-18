//! Original Stream contract and model suites under one integration root.

#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod browser_follow_model;

#[cfg(all(loom, feature = "model-checking"))]
mod client_pool_loom;

#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod client_session_loom;

#[cfg(not(loom))]
mod http_response_core;

#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod local_follow_loom;

#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod local_replay_model;

#[cfg(not(loom))]
mod record_formats;

#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod retry_recovery_loom;

#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod wave1_loom;

#[cfg(all(loom, feature = "model-checking"))]
mod revision_frontier_loom;

#[cfg(all(not(loom), target_arch = "wasm32", feature = "wasm"))]
mod credential_javascript;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod trust_anchor;
