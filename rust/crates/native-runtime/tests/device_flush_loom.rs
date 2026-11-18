//! `bazel test --@rules_rust//rust/settings:per_crate_rustc_flag=//rust/crates/native-runtime:@--cfg=loom //rust/crates/native-runtime:device_flush_loom`
#![cfg(loom)]
#[path = "../src/device_flush.rs"]
mod device_flush;
#[path = "../src/sync/locks.rs"]
pub mod sync;
