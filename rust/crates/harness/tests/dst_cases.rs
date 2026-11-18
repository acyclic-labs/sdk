//! Deterministic simulation of the harness across the seeded worlds.

#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(all(not(loom), not(target_arch = "wasm32"), feature = "test-support"))]
#[path = "dst/main.rs"]
mod dst;
