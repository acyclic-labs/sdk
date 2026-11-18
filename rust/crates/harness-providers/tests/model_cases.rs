//! Provider concurrency models over the production implementations.

#![cfg(not(target_arch = "wasm32"))]

use acyclic_model_render::Family as Adapter;

#[cfg(loom)]
#[path = "../src/sync.rs"]
mod sync;
#[cfg(not(loom))]
#[path = "../src/sync/loom.rs"]
mod sync;
mod verification_models;
