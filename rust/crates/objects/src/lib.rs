#![doc = include_str!("../README.md")]
mod body;
mod obs;
pub mod v1;
/// Canonical logical Objects v1 public contracts and providers.
pub use v1::*;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod local_options;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod physical;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use local_options::{LocalDurability, LocalObjectsGarbageCollection, LocalObjectsLimits};
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub use v1::grpc::{ConnectError, GrpcObjects};
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub use v1::http::HttpObjects;
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub use v1::local::{LocalObjects, LocalOpenError};
