//! Native host hook installation contracts.

#![cfg(all(not(loom), not(target_arch = "wasm32")))]

acyclic_test_support::counting_allocator!();

mod host_hooks;
