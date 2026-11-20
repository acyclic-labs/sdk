//! Original Stream contract suites under one integration root. The Loom
//! models run in the library's own test target.
#![cfg(not(loom))]

#[cfg(not(loom))]
mod http_response_core;

#[cfg(not(loom))]
mod record_formats;

#[cfg(all(not(loom), target_arch = "wasm32", feature = "wasm"))]
mod credential_javascript;

#[cfg(all(not(loom), not(target_arch = "wasm32"), feature = "test-support"))]
mod trust_credentials;

#[path = "follow_contracts.rs"]
mod follow_contracts;
#[cfg(feature = "conformance")]
#[path = "provider_contracts.rs"]
mod provider_contracts;
#[cfg(all(feature = "wasm", feature = "conformance"))]
#[path = "read_selectors.rs"]
mod read_selectors;
#[path = "session_contracts.rs"]
mod session_contracts;
