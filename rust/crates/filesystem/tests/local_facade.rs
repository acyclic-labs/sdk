//! Public durable filesystem reopening and shared object-cache contracts.
#![cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]

#[path = "support/checkout_modes.rs"]
mod checkout_modes;
#[path = "local_facade/facade/mod.rs"]
mod facade;
