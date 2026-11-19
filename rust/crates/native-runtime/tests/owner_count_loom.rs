//! Check the production final-owner election with instrumented atomics.
#![cfg(loom)]

#[path = "../src/health/owners.rs"]
mod owners;
