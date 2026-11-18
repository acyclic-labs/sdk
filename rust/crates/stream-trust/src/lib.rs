//! Signed trust records delivered through a Stream provider.
#![cfg(not(target_arch = "wasm32"))]

pub mod trust_feed;

#[cfg(all(test, not(loom)))]
mod trust_anchor;
