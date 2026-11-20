//! Deterministic simulation of the native harness.

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod dst_cases;
