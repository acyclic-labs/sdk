//! `ACYCLIC_LOOM=1 cargo test -p acyclic-native-runtime --test main --features model-checking device_flush_loom::`
#![cfg(loom)]
#[path = "../src/device_flush.rs"]
mod device_flush;
