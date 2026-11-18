//! Hosted filesystem RPCs over real SDK providers.

#![cfg(not(target_arch = "wasm32"))]

#[path = "wire_rpc/wire_service/mod.rs"]
mod wire_service;
