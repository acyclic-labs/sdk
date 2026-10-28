//! Original Stream contract and model suites under one integration root.

#[cfg(all(loom, feature = "local", feature = "model-checking"))]
mod browser_follow_model;

#[cfg(not(loom))]
mod checked_session_contract;

#[cfg(all(loom, feature = "grpc", feature = "model-checking"))]
mod client_pool_loom;

#[cfg(all(loom, feature = "local", feature = "model-checking"))]
mod client_session_loom;

#[cfg(not(loom))]
mod follow_data_contract;

#[cfg(not(loom))]
mod http_response_core;

#[cfg(all(loom, feature = "local", feature = "model-checking"))]
mod local_follow_loom;

#[cfg(all(loom, feature = "local", feature = "model-checking"))]
mod local_replay_model;

#[cfg(not(loom))]
mod record_formats;

#[cfg(not(loom))]
mod session_properties;

#[cfg(all(not(loom), feature = "conformance"))]
mod wave1_coverage;

#[cfg(all(not(loom), feature = "conformance"))]
mod wave1_keys;

#[cfg(all(loom, feature = "local", feature = "model-checking"))]
mod wave1_loom;

#[cfg(all(not(loom), feature = "local", feature = "conformance"))]
mod wave1_recovery;
