//! Native receipt and host hook integration tests.
#![cfg(not(loom))]

acyclic_test_support::counting_allocator!();

#[path = "../../harness/tests/native_receipt.rs"]
mod native_receipt;

#[cfg(not(target_arch = "wasm32"))]
#[path = "../../harness/tests/host_hooks.rs"]
mod host_hooks;
