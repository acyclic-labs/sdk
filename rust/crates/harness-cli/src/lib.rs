//! Native CLI composition: process completion custody, host hook manifests and
//! the shared usage footer. Hosts retain their tools, permissions and loop.
#![deny(unsafe_code)]
#![doc = include_str!("../README.md")]

#[cfg(not(target_arch = "wasm32"))]
pub mod completion;
#[cfg(not(target_arch = "wasm32"))]
pub mod host_hooks;
#[cfg(not(target_arch = "wasm32"))]
pub mod usage;
