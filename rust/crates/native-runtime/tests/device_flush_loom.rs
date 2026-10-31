//! `cargo rustc -p acyclic-native-runtime --test device_flush_loom --features model-checking -- --cfg loom`
#![cfg(loom)]
#[path = "../src/device_flush.rs"]
mod device_flush;
