//! Public durable filesystem reopening and shared object-cache contracts.
#![cfg(not(target_arch = "wasm32"))]

#[path = "support/allow_publication.rs"]
mod allow_publication;
#[path = "support/checkout_fixture.rs"]
mod checkout_fixture;
#[path = "local_facade/facade/mod.rs"]
mod facade;
