//! The Loom model suites. They run in this crate's own test target, beside
//! the models they drive, so no model code reaches a library build.
#[cfg(not(target_arch = "wasm32"))]
mod browser_follow_model;
mod client_pool_loom;
#[cfg(not(target_arch = "wasm32"))]
mod client_session_loom;
#[cfg(not(target_arch = "wasm32"))]
mod local_follow_loom;
#[cfg(not(target_arch = "wasm32"))]
mod local_replay_model;
#[cfg(not(target_arch = "wasm32"))]
mod retry_recovery_loom;
mod session_sources;
#[cfg(not(target_arch = "wasm32"))]
mod wave1_loom;
