//! Public durable filesystem reopening and shared object-cache contracts.
#![cfg(not(target_arch = "wasm32"))]

use crate::allow_publication;
use crate::checkout_fixture;
#[path = "local_facade/facade/mod.rs"]
mod facade;
