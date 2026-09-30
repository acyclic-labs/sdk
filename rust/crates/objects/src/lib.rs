#![doc = include_str!("../README.md")]
mod body;
pub mod v2;
/// Canonical logical Objects v2 public contracts and providers.
pub use v2::*;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod local_options;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod physical;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use local_options::{LocalDurability, LocalObjectsGarbageCollection, LocalObjectsLimits};
#[cfg(feature = "grpc")]
pub use v2::grpc::{ConnectError, GrpcObjects};
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub use v2::http::HttpObjects;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use v2::local::{LocalObjects, LocalOpenError};
