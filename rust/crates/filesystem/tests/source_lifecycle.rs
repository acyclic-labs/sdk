//! Native attached-source lifecycle through the public workspace API.

#![cfg(not(target_arch = "wasm32"))]

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

#[path = "source_lifecycle/source/mod.rs"]
mod source;
