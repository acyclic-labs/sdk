#![doc = include_str!("../README.md")]

/// Cloud stateless Worker deployment through Objects and Actors.
#[cfg(not(target_arch = "wasm32"))]
pub mod deployment;
