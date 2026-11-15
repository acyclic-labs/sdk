//! `ACYCLIC_LOOM=1 cargo test -p acyclic-native-runtime --test device_flush_loom --features model-checking`
#![cfg(loom)]
#[path = "../src/device_flush.rs"]
mod device_flush;
#[path = "../src/sync/locks.rs"]
pub mod sync;
